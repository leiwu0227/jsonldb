"""Opt-in error-style deletion reuses indexes and retries physical cleanup."""
from pathlib import Path

import pytest

from jsonldb import jsonlfile
from jsonldb.folderdb import FolderDB


@pytest.fixture
def table(tmp_path):
    db = FolderDB(str(tmp_path), hierarchy_depth=1)
    db.set_meta_slot_bytes(256)
    db.overwrite_dict('err.sample', {'2026-01-01': {'error': 'a'},
                                    '2026-01-02': {'error': 'b'},
                                    '2026-01-03': {'error': 'c'}})
    return db, Path(db._get_file_path('err.sample'))


@pytest.mark.parametrize('ranged', [False, True])
def test_single_index_and_no_empty_rewrite(table, monkeypatch, ranged):
    db, path = table
    calls = []
    original = jsonlfile.load_index
    def load(file):
        if str(file) == str(path):
            calls.append(file)
        return original(file)
    monkeypatch.setattr(jsonlfile, 'load_index', load)
    def forbidden(*args, **kwargs):
        raise AssertionError('No full-history scan or empty table rewrite')
    monkeypatch.setattr(jsonlfile, 'load_jsonl', forbidden)
    monkeypatch.setattr(jsonlfile, 'build_jsonl_index', forbidden)
    publish = jsonlfile._write_index
    def write(file, index):
        assert str(file) != str(path), 'Final deletion must not publish an empty index'
        return publish(file, index)
    monkeypatch.setattr(jsonlfile, '_write_index', write)
    if ranged:
        db.delete_file_range('err.sample', '2026-01-01', '2026-01-03', remove_if_empty=True)
    else:
        db.delete_file_keys('err.sample', ['2026-01-01', '2026-01-02', '2026-01-03'], remove_if_empty=True)
    assert len(calls) == 1
    assert not path.exists()
    assert not Path(str(path) + '.idx').exists()
    assert jsonlfile.select_line_jsonl(db.dbmeta_path, 'err.sample') == {}


def test_partial_batch_preserves_gaps_and_updates_stats_once(table, monkeypatch):
    db, path = table
    calls = []
    original = jsonlfile.load_index
    def load(file):
        if str(file) == str(path):
            calls.append(file)
        return original(file)
    monkeypatch.setattr(jsonlfile, 'load_index', load)
    db.delete_file_keys('err.sample', ['2026-01-01', '2026-01-03'], remove_if_empty=True)
    assert len(calls) == 1
    assert db.get_dict(['err.sample']) == {'err.sample': {'2026-01-02': {'error': 'b'}}}
    assert db.get_dbmeta()['err.sample']['count'] == 1


@pytest.mark.parametrize('failure', ['data', 'index', 'metadata'])
def test_retry_partial_unlink(table, monkeypatch, failure):
    db, path = table
    remove = __import__('os').remove
    delete_meta = db.delete_dbmeta
    def fail_remove(file):
        if str(file) == str(path) + ('.idx' if failure == 'index' else ''):
            raise OSError('injected unlink')
        return remove(file)
    def fail_meta(name):
        raise OSError('injected metadata')
    if failure == 'metadata':
        monkeypatch.setattr(db, 'delete_dbmeta', fail_meta)
    else:
        monkeypatch.setattr('jsonldb.folderdb.os.remove', fail_remove)
    with pytest.raises(OSError, match='injected'):
        db.delete_file_range('err.sample', '2026-01-01', '2026-01-03', remove_if_empty=True)
    monkeypatch.setattr('jsonldb.folderdb.os.remove', remove)
    monkeypatch.setattr(db, 'delete_dbmeta', delete_meta)
    db.delete_file_range('err.sample', '2026-01-01', '2026-01-03', remove_if_empty=True)
    assert not path.exists()
    assert not Path(str(path) + '.idx').exists()
    assert 'err.sample' not in db.get_dbmeta()
    before = {p: p.read_bytes() for p in Path(db.folder_path).rglob('*') if p.is_file()}
    db.delete_file_keys('err.sample', ['2026-01-01'], remove_if_empty=True)
    assert before == {p: p.read_bytes() for p in Path(db.folder_path).rglob('*') if p.is_file()}


def test_default_deletion_preserves_metadata_and_opt_in_noop(table):
    db, path = table
    db.overwrite_dict('err.sample', {'2026-01-01': {'error': 'a'}}, meta={'keep': True})
    db.delete_file_keys('err.sample', ['2026-01-01'])
    assert path.exists()
    assert db.read_meta('err.sample') == {'keep': True}
    before = path.read_bytes()
    db.delete_file_keys('err.sample', ['2026-01-01'], remove_if_empty=True)
    assert path.read_bytes() == before
