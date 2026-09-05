from datetime import datetime

import pytest

from jsonldb import FolderDB, jsonlfile, metaslot


def _configured_db(root, hierarchy, slot_bytes):
    settings = {'timespec': 'microseconds', 'consumer_setting': {'keep': True}}
    if slot_bytes is not None:
        settings['meta_slot_bytes'] = slot_bytes
    jsonlfile.save_jsonl_atomic(str(root / 'config.meta'), {'config': settings})
    if hierarchy:
        jsonlfile.save_jsonl_atomic(str(root / 'h.meta'), {'hierarchy': {
            'use_hierarchy': True, 'delimiter': '-', 'hierarchy_depth': 2,
        }})
    return FolderDB(str(root))


def _settings(db):
    return (db.use_hierarchy, db.hierarchy_depth, db.delimiter,
            db.timespec, db.meta_slot_bytes)


@pytest.mark.parametrize('hierarchy', [False, True])
@pytest.mark.parametrize('slot_bytes', [None, 160])
def test_clear_preserves_configuration_for_live_and_reopened_instances(
        tmp_path, hierarchy, slot_bytes):
    db = _configured_db(tmp_path, hierarchy, slot_bytes)
    key = datetime(2026, 9, 5, 12, 0, 0, 123456)
    meta = {'owner': 'consumer'} if slot_bytes is not None else None
    db.overwrite_dict('old-table', {key: {'value': 1}}, meta=meta)
    extra = tmp_path / 'external' / 'deep' / 'extra.jsonl'
    extra.parent.mkdir(parents=True)
    jsonlfile.save_jsonl(str(extra), {'row': {'value': 2}})
    orphan_index = tmp_path / 'orphan.jsonl.idx'
    orphan_index.write_bytes(b'{}')
    control_names = ['config.meta', 'config.meta.idx']
    if hierarchy:
        control_names += ['h.meta', 'h.meta.idx']
    controls = {tmp_path / name: ((tmp_path / name).read_bytes(),
                                (tmp_path / name).stat().st_mtime_ns)
                for name in control_names}
    settings = _settings(db)

    db.clear_folder(force=True)

    assert _settings(db) == settings
    assert (tmp_path / 'db.meta').read_bytes() == b''
    assert (tmp_path / 'db.meta.idx').read_bytes() == b'{}'
    assert not list(tmp_path.rglob('*.jsonl'))
    assert not list(tmp_path.rglob('*.jsonl.idx'))
    assert not (tmp_path / 'external').exists()
    assert all((path.read_bytes(), path.stat().st_mtime_ns) == before
               for path, before in controls.items())

    reopened = FolderDB(str(tmp_path))
    assert _settings(reopened) == settings
    assert reopened._config_meta['consumer_setting'] == {'keep': True}
    assert (tmp_path / 'h.meta').exists() is hierarchy
    for instance, name in ((db, 'from-live'), (reopened, 'from-reopen')):
        instance.overwrite_dict(name, {key: {'value': 3}}, meta=meta)
        path = instance._get_file_path(name)
        assert list(jsonlfile.load_index(path)) == [key.isoformat(timespec='microseconds')]
        info = metaslot.inspect_file(path)
        assert info.is_slot is (slot_bytes is not None)
        assert info.record == meta
        if slot_bytes is not None:
            assert info.width == slot_bytes
    final = FolderDB(str(tmp_path))
    assert _settings(final) == settings
    assert final.get_dict() == {
        'from-live': {key: {'value': 3}}, 'from-reopen': {key: {'value': 3}},
    }


def test_clear_without_force_changes_no_files(tmp_path, caplog):
    db = _configured_db(tmp_path, hierarchy=True, slot_bytes=160)
    db.overwrite_dict('kept-table', {'row': {'value': 1}}, meta={'kept': True})
    before = {path: path.read_bytes() for path in tmp_path.rglob('*') if path.is_file()}
    settings = _settings(db)
    caplog.set_level('WARNING', logger='jsonldb.folderdb')
    caplog.clear()

    db.clear_folder()

    assert {path: path.read_bytes() for path in tmp_path.rglob('*') if path.is_file()} == before
    assert _settings(db) == settings
    assert any('force=True' in message for message in caplog.messages)


def test_clear_preserves_unrelated_files_and_is_repeatable(tmp_path):
    db = _configured_db(tmp_path, hierarchy=False, slot_bytes=None)
    unrelated = tmp_path / 'external'
    unrelated.mkdir()
    for name in ('notes.meta', 'notes.idx', 'notes.txt'):
        (unrelated / name).write_bytes(b'keep')
    db.overwrite_dict('removed', {'row': {'value': 1}})

    db.clear_folder(force=True)
    db.clear_folder(force=True)

    assert all((unrelated / name).read_bytes() == b'keep'
               for name in ('notes.meta', 'notes.idx', 'notes.txt'))
    assert (tmp_path / 'db.meta').read_bytes() == b''
    assert (tmp_path / 'db.meta.idx').read_bytes() == b'{}'
    assert _settings(FolderDB(str(tmp_path))) == _settings(db)
