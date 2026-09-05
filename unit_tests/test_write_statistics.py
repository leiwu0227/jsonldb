"""Observable compatibility and publication boundaries for writer statistics."""
import builtins
from datetime import datetime
import os

import orjson
import pandas as pd
import pytest

from jsonldb import FolderDB, jsonlfile as jf, jsonldf as jdf
import jsonldb.folderdb as folder_module


def payload(rows, kind):
    return pd.DataFrame.from_dict(rows, orient='index') if kind == 'df' else rows


def assert_disk_metadata(db, name):
    path = db._get_file_path(name)
    index = orjson.loads(open(path + '.idx', 'rb').read())
    keys = sorted(index)
    entry = db.get_dbmeta()[name]
    assert entry == dict(name=name, path=path, count=len(keys),
                         min_index=keys[0] if keys else None,
                         max_index=keys[-1] if keys else None,
                         size=os.path.getsize(path), linted=False, lint_time='')
    with open(path, 'rb') as stream:
        for key, offset in index.items():
            stream.seek(offset)
            assert key in orjson.loads(stream.readline())


@pytest.mark.parametrize('kind', ['dict', 'df'])
@pytest.mark.parametrize('operation', ['overwrite', 'upsert'])
@pytest.mark.parametrize('slot', [False, True])
@pytest.mark.parametrize('hierarchy', [None, 1])
def test_writes_match_published_index(tmp_path, kind, operation, slot, hierarchy):
    db = FolderDB(str(tmp_path), hierarchy_depth=hierarchy)
    if slot:
        db.set_meta_slot_bytes(256)
    write = getattr(db, operation + '_' + kind)
    name = 'region.table'
    for rows in ({'z': {'v': 1}, 'm': {'v': 2}},
                 {'zz': {'v': 3}, 'a': {'v': 'a longer appended row'}}, {}):
        assert write(name + '.jsonl', payload(rows, kind),
                     meta={'generation': 1} if slot else None) is None
        assert_disk_metadata(db, name)
        if slot:
            assert db.read_meta(name) == {'generation': 1}


@pytest.mark.parametrize('kind', ['dict', 'df'])
@pytest.mark.parametrize('timespec', ['seconds', 'microseconds'])
def test_precision_and_normalization_collisions(tmp_path, kind, timespec):
    jf.save_jsonl(str(tmp_path / 'config.meta'), {'config': {'timespec': timespec}})
    db = FolderDB(str(tmp_path))
    stamp = datetime(2026, 9, 5, 1, 2, 3, 123456)
    key = stamp.isoformat(timespec=timespec)
    # Object index prevents pandas from coercing the string into a timestamp.
    rows = {stamp: {'v': 1}, key: {'v': 2}, 'a': {'v': 3}}
    data = (pd.DataFrame(list(rows.values()), index=pd.Index(list(rows), dtype=object))
            if kind == 'df' else rows)
    for operation in ('overwrite', 'upsert'):
        assert getattr(db, operation + '_' + kind)('table', data) is None
        assert_disk_metadata(db, 'table')
        assert db.get_dbmeta()['table']['count'] == 2


@pytest.mark.parametrize('kind', ['dict', 'df'])
@pytest.mark.parametrize('operation,existing', [('overwrite', False), ('overwrite', True),
                                              ('upsert', False), ('upsert', True)])
def test_no_post_publication_table_reads(tmp_path, monkeypatch, kind, operation, existing):
    db = FolderDB(str(tmp_path))
    path = db._get_file_path('table')
    if existing:
        db.overwrite_dict('table', {'m': {'v': 1}})
    published = False
    reads = []
    original_open, original_write = builtins.open, jf._write_index

    def guarded_open(file, mode='r', *args, **kwargs):
        if isinstance(file, (str, os.PathLike)) and os.fspath(file) in (path, path + '.idx'):
            if 'r' in mode:
                assert not published, 'metadata upkeep reopened the published table/index'
                reads.append(os.fspath(file))
        return original_open(file, mode, *args, **kwargs)

    def publish(file, index):
        nonlocal published
        original_write(file, index)
        if file == path:
            published = True

    with monkeypatch.context() as patch:
        patch.setattr(builtins, 'open', guarded_open)
        patch.setattr(jf, '_write_index', publish)
        getattr(db, operation + '_' + kind)('table', payload({'a': {'v': 2}}, kind))
    assert published
    if operation == 'upsert' and existing:
        assert path + '.idx' in reads  # the required initial writer load remains
    assert_disk_metadata(db, 'table')


@pytest.mark.parametrize('kind', ['dict', 'df'])
@pytest.mark.parametrize('operation', ['overwrite', 'upsert'])
@pytest.mark.parametrize('failure', ['row', 'index', 'metadata'])
def test_failure_boundaries(tmp_path, monkeypatch, kind, operation, failure):
    db = FolderDB(str(tmp_path))
    db.overwrite_dict('table', {'old': {'v': 1}})
    path = db._get_file_path('table')
    jf.select_line_jsonl(path, 'old')  # populate the read cache before failure
    before_meta = open(db.dbmeta_path, 'rb').read()
    original_dump, original_write, original_update = jf._fast_dumps, jf._write_index, folder_module.update_jsonl

    def dump(row):
        if failure == 'row' and 'new' in row:
            raise OSError('injected row failure')
        return original_dump(row)

    def publish(file, index):
        if failure == 'index' and file == path:
            raise OSError('injected index failure')
        return original_write(file, index)

    def update(file, *args, **kwargs):
        if failure == 'metadata' and file == db.dbmeta_path:
            raise OSError('injected metadata failure')
        return original_update(file, *args, **kwargs)

    monkeypatch.setattr(jf, '_fast_dumps', dump)
    monkeypatch.setattr(jf, '_write_index', publish)
    monkeypatch.setattr(folder_module, 'update_jsonl', update)
    with pytest.raises(OSError, match='injected'):
        getattr(db, operation + '_' + kind)('table', payload({'new': {'v': 2}}, kind))
    assert open(db.dbmeta_path, 'rb').read() == before_meta
    assert os.path.abspath(path) not in jf._INDEX_CACHE
    if failure == 'metadata':
        assert jf.select_line_jsonl(path, 'new') == {'new': {'v': 2}}


@pytest.mark.parametrize('kind', ['dict', 'df'])
@pytest.mark.parametrize('operation', ['overwrite', 'upsert'])
def test_plural_completion_and_partial_failure(tmp_path, monkeypatch, kind, operation):
    db = FolderDB(str(tmp_path))
    write = getattr(db, operation + '_' + kind + 's')
    assert write({n: payload({'z': {'v': 1}}, kind) for n in ('first', 'second')}) is None
    original = jf._write_index

    def publish(path, index):
        if path == db._get_file_path('second'):
            assert db.get_dbmeta()['first']['min_index'] == 'a'
            raise OSError('second publication failed')
        return original(path, index)

    monkeypatch.setattr(jf, '_write_index', publish)
    with pytest.raises(OSError, match='second publication'):
        write({n: payload({'a': {'v': 2}}, kind) for n in ('first', 'second', 'third')})
    assert_disk_metadata(db, 'first')
    assert db.get_dbmeta()['second']['min_index'] == 'z'
    assert not os.path.exists(db._get_file_path('third'))


@pytest.mark.parametrize('operation', ['refresh', 'rebuild', 'lint'])
def test_disk_fallback_recovers_corrupt_index(tmp_path, operation):
    db = FolderDB(str(tmp_path))
    db.overwrite_dict('table', {'a': {'v': 1}, 'z': {'v': 2}})
    path = db._get_file_path('table')
    with open(path + '.idx', 'wb') as stream:
        stream.write(b'{broken')
    if operation == 'refresh':
        db.update_dbmeta('table.jsonl')
    elif operation == 'rebuild':
        db.build_dbmeta()
    else:
        db.lint_db(force=True)
    entry = db.get_dbmeta()['table']
    assert (entry['count'], entry['min_index'], entry['max_index']) == (2, 'a', 'z')
    assert entry['linted'] is (operation == 'lint')
    assert bool(entry['lint_time']) is (operation == 'lint')


def test_public_writers_skip_summary_and_keep_none(tmp_path, monkeypatch):
    def unexpected(*args):
        pytest.fail('ordinary low-level writes must not derive statistics')
    monkeypatch.setattr(jf, '_index_stats', unexpected)
    path = tmp_path / 'file.jsonl'
    assert jf.save_jsonl(path, {}) is None
    assert jf.update_jsonl(str(path), {'a': {'v': 1}}) is None
    assert jdf.save_jsonldf(path, pd.DataFrame({'v': [1]}, index=['z'])) is None
    assert jdf.update_jsonldf(str(path), pd.DataFrame({'v': [2]}, index=['a'])) is None


def test_validation_does_not_publish_metadata(tmp_path):
    db = FolderDB(str(tmp_path))
    before = open(db.dbmeta_path, 'rb').read()
    with pytest.raises(ValueError, match='unique'):
        db.overwrite_df('duplicate', pd.DataFrame({'v': [1, 2]}, index=['a', 'a']))
    for method in (db.overwrite_dict, db.upsert_dict):
        with pytest.raises(ValueError):
            method('reserved', {'_meta': {'v': 1}})
    assert open(db.dbmeta_path, 'rb').read() == before


@pytest.mark.parametrize('hierarchy', [None, 1])
@pytest.mark.parametrize('slot', [False, True])
@pytest.mark.parametrize('rows', [{}, {'z': {'v': 1}, 'a': {'v': 2}}])
def test_explicit_refresh_rebuilds_missing_index(tmp_path, hierarchy, slot, rows):
    db = FolderDB(str(tmp_path), hierarchy_depth=hierarchy)
    if slot:
        db.set_meta_slot_bytes(256)
    name = 'region.table'
    db.overwrite_dict(name, rows, meta={'owner': 'keep'} if slot else None)
    path = db._get_file_path(name)
    with open(path, 'rb') as stream:
        table_bytes = stream.read()
    with open(path + '.idx', 'rb') as stream:
        index_bytes = stream.read()
    if rows:
        jf.select_line_jsonl(path, 'a')  # Retain a cached index before external removal.
    os.remove(path + '.idx')

    db.update_dbmeta(name + '.jsonl')

    assert os.path.exists(path + '.idx'), 'refresh must rebuild before measuring'
    with open(path + '.idx', 'rb') as stream:
        assert stream.read() == index_bytes
    with open(path, 'rb') as stream:
        assert stream.read() == table_bytes
    assert_disk_metadata(db, name)
    assert db.read_meta(name) == ({'owner': 'keep'} if slot else None)
    if rows:
        assert jf.select_line_jsonl(path, 'a') == {'a': {'v': 2}}
