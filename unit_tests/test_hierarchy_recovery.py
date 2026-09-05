import os

import pytest

from jsonldb import FolderDB, jsonlfile


TORN_CONTROL = b'{"hierarchy":'


def _table(root, relative, value=1):
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    jsonlfile.save_jsonl(str(path), {'row': {'value': value}},
                        meta={'owner': value}, slot_bytes=128)
    return path


def _report(root):
    return (root / '.jsonldb' / 'integrity.log').read_text()


@pytest.mark.parametrize('control', [
    None, b'', TORN_CONTROL, b'[]\n', b'{"hierarchy":{}}\n',
    b'{"hierarchy":{"use_hierarchy":true,"delimiter":"-","hierarchy_depth":"2"}}\n',
    b'{"hierarchy":{"use_hierarchy":1,"delimiter":"-","hierarchy_depth":2}}\n',
])
def test_recovers_missing_or_invalid_controls_without_rewriting_tables(tmp_path, control):
    path = _table(tmp_path, 'a/b/a-b.jsonl')
    before = path.read_bytes(), path.stat().st_ino, path.stat().st_mtime_ns
    if control is not None:
        (tmp_path / 'h.meta').write_bytes(control)

    db = FolderDB(str(tmp_path))

    assert (db.use_hierarchy, db.hierarchy_depth, db.delimiter) == (True, 2, '-')
    assert db.get_dict('a-b') == {'a-b': {'row': {'value': 1}}}
    assert db.read_meta('a-b') == {'owner': 1}
    assert (path.read_bytes(), path.stat().st_ino, path.stat().st_mtime_ns) == before
    assert db.get_dbmeta()['a-b']['path'] == str(path)
    assert jsonlfile.load_jsonl(str(tmp_path / 'h.meta')) == {
        'hierarchy': {'use_hierarchy': True, 'delimiter': '-', 'hierarchy_depth': 2}}
    assert (tmp_path / 'h.meta.idx').is_file()
    report = _report(tmp_path)
    assert 'kind=hierarchy_recovered' in report
    assert 'observed_depths=[2] depth=2' in report
    assert "delimiter='-'" in report

    reopened = FolderDB(str(tmp_path))
    assert reopened.get_dict() == db.get_dict()
    assert 'hierarchy_recovered' not in _report(tmp_path)
    reopened.upsert_dict('a-b.jsonl', {'next': {'value': 2}})
    assert reopened.get_dbmeta()['a-b']['count'] == 2


@pytest.mark.parametrize('delimiter', ['.', '-', '::'])
def test_infers_delimiter_from_directory_and_filename_prefixes(tmp_path, delimiter):
    name = delimiter.join(['region', 'us', 'prices'])
    _table(tmp_path, 'region/us/' + name + '.jsonl')
    (tmp_path / 'h.meta').write_bytes(TORN_CONTROL)

    db = FolderDB(str(tmp_path))

    assert (db.hierarchy_depth, db.delimiter) == (2, delimiter)
    assert db.get_dict(name) == {name: {'row': {'value': 1}}}


def test_mixed_depths_choose_shallowest_and_preserve_hidden_trees(tmp_path):
    shallow = _table(tmp_path, 'a/a-x.jsonl')
    deep = _table(tmp_path, 'b/y/b-y-z.jsonl', 2)
    before = deep.read_bytes()
    index_before = deep.with_suffix('.jsonl.idx').read_bytes()
    for relative in ('.invalid_tickers/duplicate.jsonl', '.external/duplicate.jsonl'):
        _table(tmp_path, relative, 99)
    hidden_empty = tmp_path / 'b' / 'y' / '.external' / 'empty'
    hidden_empty.mkdir(parents=True)
    (tmp_path / 'h.meta').write_bytes(TORN_CONTROL)
    jsonlfile.save_jsonl(str(tmp_path / 'db.meta'), {'stale': {'count': 999}})

    db = FolderDB(str(tmp_path) + os.sep)

    assert (db.hierarchy_depth, db.delimiter) == (1, '-')
    moved = tmp_path / 'b' / 'b-y-z.jsonl'
    assert shallow.is_file() and not deep.exists()
    assert moved.read_bytes() == before
    assert moved.with_suffix('.jsonl.idx').read_bytes() == index_before
    assert sorted(db.get_file_list()) == ['a-x', 'b-y-z']
    assert set(db.get_dbmeta()) == {'a-x', 'b-y-z'}
    assert db.read_meta('b-y-z') == {'owner': 2}
    assert hidden_empty.is_dir()
    assert (tmp_path / '.invalid_tickers' / 'duplicate.jsonl').is_file()
    assert (tmp_path / '.external' / 'duplicate.jsonl').is_file()
    assert 'observed_depths=[1, 2] depth=1' in _report(tmp_path)
    assert FolderDB(str(tmp_path)).get_dict() == db.get_dict()


@pytest.mark.parametrize('layout', ['empty', 'root', 'mixed'])
def test_recovers_flat_layout_and_removes_invalid_hierarchy_control(tmp_path, layout):
    expected = {}
    if layout != 'empty':
        _table(tmp_path, 'root.jsonl')
        expected['root'] = {'row': {'value': 1}}
    if layout == 'mixed':
        _table(tmp_path, 'a/b/a-b.jsonl', 2)
        expected['a-b'] = {'row': {'value': 2}}
    (tmp_path / 'h.meta').write_bytes(TORN_CONTROL)
    (tmp_path / 'h.meta.idx').write_bytes(b'{"hierarchy":0}')

    db = FolderDB(str(tmp_path))

    assert db.use_hierarchy is False and db.hierarchy_depth == 0
    assert db.get_dict() == expected
    assert not (tmp_path / 'h.meta').exists()
    assert not (tmp_path / 'h.meta.idx').exists()
    assert 'depth=0' in _report(tmp_path)
    assert FolderDB(str(tmp_path)).get_dict() == expected


@pytest.mark.parametrize('conflict', ['duplicate', 'index', 'directory', 'prefix'])
def test_recovery_preflights_conflicts_before_moving_any_table(tmp_path, conflict):
    _table(tmp_path, 'a/a-x.jsonl')
    deep = _table(tmp_path, 'b/y/b-y-z.jsonl', 2)
    if conflict == 'duplicate':
        _table(tmp_path, 'b/b-y-z.jsonl', 3)
    elif conflict == 'index':
        (tmp_path / 'b' / 'b-y-z.jsonl.idx').write_bytes(b'keep this index')
    elif conflict == 'directory':
        # Root data forces flattening, where this directory blocks a table.
        _table(tmp_path, 'root.jsonl')
        (tmp_path / 'b-y-z.jsonl').mkdir()
    else:
        _table(tmp_path, 'wrong/place/other-name.jsonl', 3)
    (tmp_path / 'h.meta').write_bytes(TORN_CONTROL)
    before = {p: p.read_bytes() for p in tmp_path.rglob('*') if p.is_file()}

    with pytest.raises(ValueError, match='collision|contradictory'):
        FolderDB(str(tmp_path))

    assert deep.is_file()
    assert all(p.read_bytes() == content for p, content in before.items())
    assert 'kind=hierarchy_recovery_failed' in _report(tmp_path)


def test_explicit_depth_is_honored_without_losing_inferred_delimiter(tmp_path):
    _table(tmp_path, 'a/b/a-b-c.jsonl')
    (tmp_path / 'h.meta').write_bytes(TORN_CONTROL)

    db = FolderDB(str(tmp_path), hierarchy_depth=1)

    assert (db.hierarchy_depth, db.delimiter) == (1, '-')
    assert (tmp_path / 'a' / 'a-b-c.jsonl').is_file()
    assert db.get_dict('a-b-c')


@pytest.mark.parametrize('failure', ['index_move', 'control_publish'])
def test_interrupted_recovery_can_be_retried(tmp_path, monkeypatch, failure):
    _table(tmp_path, 'a/a-x.jsonl')
    source = _table(tmp_path, 'b/y/b-y-z.jsonl', 2)
    raw = source.read_bytes()
    (tmp_path / 'h.meta').write_bytes(TORN_CONTROL)
    real_rename, real_replace = os.rename, os.replace

    def interrupt(old, new):
        if ((failure == 'index_move' and str(old) == str(source) + '.idx')
                or (failure == 'control_publish' and str(new) == str(tmp_path / 'h.meta'))):
            raise OSError('injected recovery interruption')
        return (real_rename if failure == 'index_move' else real_replace)(old, new)

    with monkeypatch.context() as patch:
        patch.setattr(os, 'rename' if failure == 'index_move' else 'replace', interrupt)
        with pytest.raises(OSError, match='injected recovery interruption'):
            FolderDB(str(tmp_path))
    assert (tmp_path / 'h.meta').read_bytes() == TORN_CONTROL

    db = FolderDB(str(tmp_path))

    assert (tmp_path / 'b' / 'b-y-z.jsonl').read_bytes() == raw
    assert db.get_dict('b-y-z') == {'b-y-z': {'row': {'value': 2}}}
    assert db.read_meta('b-y-z') == {'owner': 2}
    assert set(db.get_dbmeta()) == {'a-x', 'b-y-z'}


def test_valid_controls_do_not_trigger_layout_inference(tmp_path, monkeypatch):
    db = FolderDB(str(tmp_path), hierarchy_depth=2)
    db.overwrite_dict('a.b', {'row': {'value': 1}})
    control = (tmp_path / 'h.meta').read_bytes()

    def unexpected(*args):
        pytest.fail('valid configuration must not enter recovery')

    monkeypatch.setattr(FolderDB, '_recover_hierarchy', unexpected)
    reopened = FolderDB(str(tmp_path))

    assert reopened.get_dict('a.b') == db.get_dict('a.b')
    assert (tmp_path / 'h.meta').read_bytes() == control


def test_explicit_hierarchy_creation_retains_quarantine_behavior(tmp_path):
    _table(tmp_path, 'a.b.jsonl')
    invalid = _table(tmp_path, 'short.jsonl', 2)
    before = invalid.read_bytes()

    db = FolderDB(str(tmp_path), hierarchy_depth=2)

    assert db.get_file_list() == ['a.b']
    assert (tmp_path / '.invalid_tickers' / 'short.jsonl').read_bytes() == before


def test_unreadable_directory_aborts_recovery_before_publication(tmp_path, monkeypatch):
    (tmp_path / 'h.meta').write_bytes(TORN_CONTROL)

    def unreadable(*args, onerror, **kwargs):
        onerror(PermissionError('injected unreadable directory'))
        return iter(())

    monkeypatch.setattr(os, 'walk', unreadable)
    with pytest.raises(PermissionError, match='unreadable directory'):
        FolderDB(str(tmp_path))

    assert (tmp_path / 'h.meta').read_bytes() == TORN_CONTROL
    assert 'hierarchy_recovery_failed' in _report(tmp_path)


@pytest.mark.parametrize('index_kind', ['directory', 'symlink'])
def test_non_file_indexes_are_preserved_before_table_moves(tmp_path, index_kind):
    _table(tmp_path, 'a/a-x.jsonl')
    source = _table(tmp_path, 'b/y/b-y-z.jsonl')
    index = source.with_suffix('.jsonl.idx')
    index.unlink()
    if index_kind == 'directory':
        index.mkdir()
    else:
        index.symlink_to(tmp_path / 'a' / 'a-x.jsonl.idx')
    (tmp_path / 'h.meta').write_bytes(TORN_CONTROL)

    with pytest.raises(ValueError, match='index is not a regular file'):
        FolderDB(str(tmp_path))

    assert source.is_file()
    assert index.is_symlink() if index_kind == 'symlink' else index.is_dir()
    assert (tmp_path / 'h.meta').read_bytes() == TORN_CONTROL
