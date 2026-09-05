import logging

import orjson
import pytest

from jsonldb import FolderDB, jsonlfile, metaslot


ENVELOPE = (b' {"_meta": {"v":99,"data":{"owner":"opaque-marker","v":7},'
            b'"future":[true,3]}}')
ROWS = {'a': {'v': 99, '_meta': {'v': 123}}, 'z': {'value': 2}}


def _unknown_table(path, width=160, compact=True):
    slot = ENVELOPE + b' ' * (width - len(ENVELOPE) - 1) + b'\n'
    keys = ['a', 'z'] if compact else ['z', 'a']
    rows = b''.join(orjson.dumps({key: ROWS[key]}) + b'\n' for key in keys)
    path.write_bytes(slot + rows + (b'' if compact else b'   \n'))
    jsonlfile.build_jsonl_index(str(path))
    return slot


@pytest.mark.parametrize('force', [False, True])
@pytest.mark.parametrize('slot_bytes', [None, 160])
@pytest.mark.parametrize('compact', [False, True])
def test_lint_preserves_unknown_envelope_through_compaction(
        tmp_path, caplog, force, slot_bytes, compact):
    path = tmp_path / 'table.jsonl'
    slot = _unknown_table(path, compact=compact)
    before = path.stat()
    caplog.set_level(logging.WARNING, logger='jsonldb.jsonlfile')

    assert jsonlfile.lint_jsonl(str(path), force=force, slot_bytes=slot_bytes)

    assert path.read_bytes().splitlines(keepends=True)[0] == slot
    assert jsonlfile.read_jsonl_meta(str(path)) is None
    assert jsonlfile.load_jsonl(str(path)) == ROWS
    index = jsonlfile.load_index(str(path))
    assert list(index) == ['a', 'z'] and index['a'] == len(slot)
    assert path.read_bytes().count(b'\n') == 3
    assert not any('opaque-marker' in str(getattr(record, 'jsonldb_removed', ''))
                   for record in caplog.records)
    if compact:
        assert path.stat().st_ino == before.st_ino
        assert path.stat().st_mtime_ns == before.st_mtime_ns
        assert caplog.messages == []
    canonical = path.read_bytes()
    assert jsonlfile.lint_jsonl(str(path), force=force, slot_bytes=slot_bytes)
    assert path.read_bytes() == canonical


@pytest.mark.parametrize('width', [128, 256])
def test_folder_lint_repads_unknown_envelope_without_reserializing_it(tmp_path, width):
    db = FolderDB(str(tmp_path))
    db.set_meta_slot_bytes(width)
    path = tmp_path / 'table.jsonl'
    _unknown_table(path)
    db.build_dbmeta()

    db.lint_db()

    info = metaslot.inspect_file(str(path))
    assert info.width == width and info.version == 99 and info.record is None
    assert info.raw_line == ENVELOPE + b' ' * (width - len(ENVELOPE) - 1) + b'\n'
    assert db.get_dict_with_meta('table').meta is None
    assert db.get_dict_with_meta('table').rows == ROWS
    assert jsonlfile.load_index(str(path))['a'] == width
    assert db.get_dbmeta()['table']['count'] == 2
    report = (tmp_path / '.jsonldb' / 'lint.log').read_text()
    assert 'kind=layout_repaired' in report
    assert 'opaque-marker' not in report


@pytest.mark.parametrize('force', [False, True])
def test_unknown_envelope_that_cannot_fit_is_refused_before_table_mutation(tmp_path, force):
    path = tmp_path / 'table.jsonl'
    _unknown_table(path, compact=False)
    index_path = tmp_path / 'table.jsonl.idx'
    before = path.read_bytes(), index_path.read_bytes()

    with pytest.raises(ValueError, match=r'envelope requires \d+ bytes but slot is 64 bytes'):
        jsonlfile.lint_jsonl(str(path), force=force, slot_bytes=64)

    assert (path.read_bytes(), index_path.read_bytes()) == before


@pytest.mark.parametrize('force', [False, True])
def test_metadata_only_unknown_envelope_missing_newline_is_preserved(tmp_path, force):
    path = tmp_path / 'only-meta.jsonl'
    path.write_bytes(ENVELOPE + b'   ')
    jsonlfile.build_jsonl_index(str(path))

    assert jsonlfile.lint_jsonl(str(path), force=force)

    assert path.read_bytes() == ENVELOPE + b'   \n'
    assert jsonlfile.read_jsonl_meta(str(path)) is None
    assert jsonlfile.load_index(str(path)) == {}
    assert jsonlfile.load_jsonl(str(path)) == {}


@pytest.mark.parametrize('malformed', [
    b'{"_meta": {"v":"99"}}', b'{"_meta": {"v":true}}',
    b'{"_meta": {}}', b'{"_meta": []}', b'{"_meta": {"v":99',
    b'{"_meta": {"v":99}, "extra":{}}',
])
def test_malformed_envelopes_are_still_repaired(tmp_path, malformed):
    path = tmp_path / 'malformed.jsonl'
    path.write_bytes(malformed + b' ' * (159 - len(malformed)) + b'\n'
                     + orjson.dumps({'row': {'v': 99}}) + b'\n')
    jsonlfile.build_jsonl_index(str(path))

    assert jsonlfile.lint_jsonl(str(path), force=True)

    assert metaslot.inspect_file(str(path)).version == 1
    assert jsonlfile.read_jsonl_meta(str(path)) is None
    assert jsonlfile.load_jsonl(str(path)) == {'row': {'v': 99}}


def test_consumer_versions_and_other_keys_are_not_envelope_versions(tmp_path):
    path = tmp_path / 'consumer.jsonl'
    rows = {'metadata': {'v': 99}, 'row': {'_meta': {'v': 99}}}
    meta = {'v': 99, '_meta': {'v': 100}}
    jsonlfile.save_jsonl(str(path), rows, meta=meta, slot_bytes=160)

    assert jsonlfile.lint_jsonl(str(path), force=True)
    assert jsonlfile.read_jsonl_meta(str(path)) == meta
    assert jsonlfile.load_jsonl(str(path)) == rows

    legacy = tmp_path / 'legacy.jsonl'
    jsonlfile.save_jsonl(str(legacy), rows)
    assert jsonlfile.lint_jsonl(str(legacy), force=True)
    assert metaslot.inspect_file(str(legacy)).is_slot is False
    assert jsonlfile.load_jsonl(str(legacy)) == rows
