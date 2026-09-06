"""Maximum-depth placement and compatibility across interrupted layout changes."""
import os
from pathlib import Path

import pandas as pd
import pytest

from jsonldb import FolderDB, jsonlfile


def _seed(root, relative, value=1):
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    jsonlfile.save_jsonl(str(path), {'row': {'value': value}},
                        meta={'owner': value}, slot_bytes=128)
    return path


def _controls(root, depth, delimiter='.'):
    jsonlfile.save_jsonl_atomic(str(root / 'h.meta'), {'hierarchy': {
        'use_hierarchy': True, 'hierarchy_depth': depth, 'delimiter': delimiter}})


def _files(root):
    return {str(p.relative_to(root)): p.read_bytes() for p in root.rglob('*')
            if p.is_file() and '.jsonldb' not in p.parts}


@pytest.mark.parametrize('writer', ['overwrite_dict', 'upsert_dict', 'overwrite_df', 'upsert_df'])
@pytest.mark.parametrize('suffix', ['', '.jsonl'])
def test_maximum_six_mixed_names_round_trip(tmp_path, writer, suffix):
    db = FolderDB(str(tmp_path), hierarchy_depth=6)
    paths = {'a': 'a.jsonl', 'a.b.c': 'a/b/a.b.c.jsonl',
             'a.b.c.d.e.f': 'a/b/c/d/e/a.b.c.d.e.f.jsonl',
             'a.b.c.d.e.f.g.h': 'a/b/c/d/e/f/a.b.c.d.e.f.g.h.jsonl'}
    rows = {'row': {'value': 1}}
    content = pd.DataFrame.from_dict(rows, orient='index') if writer.endswith('_df') else rows
    for name, relative in paths.items():
        getattr(db, writer)(name + suffix, content)
        assert (tmp_path / relative).is_file()
    db = FolderDB(str(tmp_path))
    assert db.hierarchy_depth == 6
    assert set(db.get_file_list()) == set(paths)
    for name in paths:
        assert db.get_dict(name) == {name: rows}
        assert db.get_df([name])[name].loc['row', 'value'] == 1
        db.delete_file_keys(name, ['row'])
        assert db.get_dict(name) == {name: {}}
        db.delete_file(name)
    assert db.get_file_list() == []
    assert not (tmp_path / 'a').exists()


@pytest.mark.parametrize('depth', [1, 3])
@pytest.mark.parametrize('explicit', [False, True])
def test_legacy_layout_migrates_without_changing_maximum(tmp_path, depth, explicit):
    name = 'single' if depth == 1 else 'a.b.c'
    relative = 'single/single.jsonl' if depth == 1 else 'a/b/c/a.b.c.jsonl'
    path = _seed(tmp_path, relative)
    data = path.read_bytes(), path.stat().st_ino, path.stat().st_mtime_ns
    index = Path(str(path) + '.idx').read_bytes()
    _controls(tmp_path, depth)
    db = FolderDB(str(tmp_path), **({'hierarchy_depth': depth} if explicit else {}))
    target = tmp_path / ('single.jsonl' if depth == 1 else 'a/b/a.b.c.jsonl')
    assert not path.exists()
    assert (target.read_bytes(), target.stat().st_ino, target.stat().st_mtime_ns) == data
    assert Path(str(target) + '.idx').read_bytes() == index
    assert db.read_meta(name) == {'owner': 1}
    assert db.get_dbmeta()[name]['path'] == str(target)
    before = {p: (p.read_bytes(), p.stat().st_mtime_ns) for p in tmp_path.rglob('*')
              if p.is_file() and '.jsonldb' not in p.parts
              and not p.name.startswith('db.meta')}
    assert FolderDB(str(tmp_path)).get_dict(name) == {name: {'row': {'value': 1}}}
    assert all((p.read_bytes(), p.stat().st_mtime_ns) == value for p, value in before.items())


@pytest.mark.parametrize('control', [None, b'{"hierarchy":'])
def test_lost_controls_recover_mixed_root_and_nested_layout(tmp_path, control):
    _seed(tmp_path, 'short.jsonl')
    _seed(tmp_path, 'a/b/a-b-c.jsonl', 2)
    _seed(tmp_path, 'other-name.jsonl', 3)
    if control is not None:
        (tmp_path / 'h.meta').write_bytes(control)
    db = FolderDB(str(tmp_path))
    assert (db.hierarchy_depth, db.delimiter) == (2, '-')
    assert set(db.get_dict()) == {'short', 'a-b-c', 'other-name'}
    assert (tmp_path / 'short.jsonl').is_file()
    assert (tmp_path / 'other' / 'other-name.jsonl').is_file()
    assert 'maximum=inferred (original unknown)' in (tmp_path / '.jsonldb/integrity.log').read_text()


@pytest.mark.parametrize('failure', ['first_index', 'second_table', 'control', 'metadata'])
@pytest.mark.parametrize('retry', ['reopen', 'maintenance'])
def test_reorganization_retries_original_target_after_interruption(tmp_path, monkeypatch, failure, retry):
    db = FolderDB(str(tmp_path), hierarchy_depth=1)
    paths = [_seed(tmp_path, 'a/a.b.c.d.jsonl'), _seed(tmp_path, 'x/x.y.z.w.jsonl', 2)]
    _seed(tmp_path, 'short.jsonl', 3)
    original = {p.name: p.read_bytes() for p in paths}
    old_control = (tmp_path / 'h.meta').read_bytes()
    rename, replace = os.rename, os.replace
    build_dbmeta = FolderDB.build_dbmeta

    def fail_rename(old, new):
        if ((failure == 'first_index' and str(old) == str(paths[0]) + '.idx')
                or (failure == 'second_table' and str(old) == str(paths[1]))):
            raise OSError('injected move interruption')
        return rename(old, new)

    def fail_replace(old, new):
        if failure == 'control' and str(new) == str(tmp_path / 'h.meta'):
            raise OSError('injected control interruption')
        return replace(old, new)

    def fail_metadata(self):
        if failure == 'metadata':
            raise OSError('injected metadata interruption')
        return build_dbmeta(self)

    with monkeypatch.context() as patch:
        patch.setattr(os, 'rename', fail_rename)
        patch.setattr(os, 'replace', fail_replace)
        patch.setattr(FolderDB, 'build_dbmeta', fail_metadata)
        with pytest.raises(OSError, match='injected'):
            db.lint_hierarchy(3)
    if failure != 'metadata':
        assert (tmp_path / 'h.meta').read_bytes() == old_control
    assert (tmp_path / '.hierarchy.pending').is_file()
    if retry == 'maintenance':
        db.lint_hierarchy(3)
    db = FolderDB(str(tmp_path))
    assert db.hierarchy_depth == 3
    assert set(db.get_dict()) == {'a.b.c.d', 'x.y.z.w', 'short'}
    for name, relative, owner in [('a.b.c.d', 'a/b/c/a.b.c.d.jsonl', 1),
                                  ('x.y.z.w', 'x/y/z/x.y.z.w.jsonl', 2)]:
        target = tmp_path / relative
        assert target.read_bytes() == original[name + '.jsonl']
        assert Path(str(target) + '.idx').is_file()
        assert db.read_meta(name) == {'owner': owner}
        assert len(list(tmp_path.rglob(name + '.jsonl'))) == 1
    assert not (tmp_path / '.hierarchy.pending').exists()
    db.lint_hierarchy(1)
    assert set(db.get_dict()) == {'a.b.c.d', 'x.y.z.w', 'short'}


@pytest.mark.parametrize('obstacle', ['table', 'index', 'directory', 'symlink'])
def test_reorganization_preflights_every_move(tmp_path, obstacle):
    db = FolderDB(str(tmp_path), hierarchy_depth=1)
    _seed(tmp_path, 'a/a.b.c.jsonl')
    _seed(tmp_path, 'x/x.y.z.jsonl', 2)
    target = tmp_path / 'x/y/x.y.z.jsonl'
    target.parent.mkdir()
    if obstacle == 'table':
        _seed(tmp_path, 'x/y/x.y.z.jsonl', 99)
    elif obstacle == 'index':
        Path(str(target) + '.idx').write_bytes(b'preserve')
    elif obstacle == 'directory':
        target.mkdir()
    else:
        target.parent.rmdir()
        target.parent.symlink_to(tmp_path / 'a', target_is_directory=True)
    before = _files(tmp_path)
    with pytest.raises(ValueError, match='collision|not a regular file'):
        db.lint_hierarchy(2)
    assert _files(tmp_path) == before
    assert db.hierarchy_depth == 1
    assert not (tmp_path / '.hierarchy.pending').exists()


@pytest.mark.parametrize('missing_index', [False, True])
def test_explicit_quarantine_restore_preserves_safe_short_names(tmp_path, missing_index):
    db = FolderDB(str(tmp_path), hierarchy_depth=6)
    path = _seed(tmp_path, '.invalid_tickers/short.jsonl')
    unsafe = _seed(tmp_path, '.invalid_tickers/bad name.jsonl', 2)
    raw = path.read_bytes()
    if missing_index:
        Path(str(path) + '.idx').unlink()
    db = FolderDB(str(tmp_path))
    assert db.get_file_list() == []
    db.reprocess_invalid_tickers()
    assert (tmp_path / 'short.jsonl').read_bytes() == raw
    assert db.read_meta('short') == {'owner': 1}
    assert db.get_dbmeta()['short']['count'] == 1
    assert unsafe.is_file()
    assert not path.exists()


def test_quarantine_collision_preserves_all_sources(tmp_path):
    db = FolderDB(str(tmp_path), hierarchy_depth=6)
    _seed(tmp_path, 'short.jsonl')
    _seed(tmp_path, '.invalid_tickers/short.jsonl', 2)
    _seed(tmp_path, '.invalid_tickers/another.jsonl', 3)
    before = _files(tmp_path)
    with pytest.raises(ValueError, match='collision'):
        db.reprocess_invalid_tickers()
    assert _files(tmp_path) == before


def test_quarantine_index_move_interruption_resumes_on_open(tmp_path, monkeypatch):
    db = FolderDB(str(tmp_path), hierarchy_depth=6)
    source = _seed(tmp_path, '.invalid_tickers/a.b.jsonl')
    raw = source.read_bytes()
    rename = os.rename

    def interrupt(old, new):
        if str(old).endswith('.idx'):
            raise OSError('injected')
        return rename(old, new)

    with monkeypatch.context() as patch:
        patch.setattr(os, 'rename', interrupt)
        with pytest.raises(OSError, match='injected'):
            db.reprocess_invalid_tickers()
    db = FolderDB(str(tmp_path))
    assert (tmp_path / 'a/a.b.jsonl').read_bytes() == raw
    assert not Path(str(source) + '.idx').exists()
    assert db.get_dict('a.b') == {'a.b': {'row': {'value': 1}}}


@pytest.mark.parametrize('depth', [0, -1, True, 2.5, '2'])
def test_invalid_maximum_rejected_before_moving_data(tmp_path, depth):
    _seed(tmp_path, 'short.jsonl')
    before = _files(tmp_path)
    with pytest.raises(ValueError, match='positive integer'):
        FolderDB(str(tmp_path), hierarchy_depth=depth)
    assert _files(tmp_path) == before
