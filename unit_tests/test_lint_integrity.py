import logging
import os

import orjson
import pytest

from jsonldb import FolderDB, jsonlfile, metaslot


def _assert_canonical(path, rows, slot_bytes=None, meta=None):
    raw = path.read_bytes()
    index = jsonlfile.load_index(str(path))
    assert list(index) == sorted(index)
    assert jsonlfile.load_jsonl(str(path)) == rows
    expected_offset = slot_bytes or 0
    if slot_bytes is not None:
        info = metaslot.inspect_file(str(path))
        assert info.width == slot_bytes and info.version == 1
        assert info.record == meta
        assert len(info.raw_line) == slot_bytes
    with path.open('rb') as source:
        for key in sorted(rows):
            assert index[key] == expected_offset
            source.seek(expected_offset)
            line = source.readline()
            assert orjson.loads(line) == {key: rows[key]}
            expected_offset += len(line)
    assert expected_offset == len(raw)
    assert raw.count(b'\n') == len(rows) + int(slot_bytes is not None)


@pytest.mark.parametrize('force', [False, True])
@pytest.mark.parametrize('index_bytes', [
    None, b'', b'{', b'[]', b'{"ghost":0}', b'{}',
], ids=['missing', 'empty', 'malformed', 'non-object', 'phantom', 'canonical'])
def test_empty_table_lint_repairs_index_without_rewriting_data(
        tmp_path, force, index_bytes):
    path = tmp_path / 'empty.jsonl'
    path.write_bytes(b'')
    original = path.stat()
    index_path = tmp_path / 'empty.jsonl.idx'
    if index_bytes is not None:
        index_path.write_bytes(index_bytes)
        fresh = original.st_mtime_ns + 1_000_000_000
        os.utime(index_path, ns=(fresh, fresh))

    assert jsonlfile.lint_jsonl(str(path), force=force) is True

    assert index_path.read_bytes() == b'{}'
    assert path.read_bytes() == b''
    assert path.stat().st_ino == original.st_ino
    assert path.stat().st_mtime_ns == original.st_mtime_ns


@pytest.mark.parametrize('force', [False, True])
def test_standalone_lint_converges_metadata_only_missing_newline(
        tmp_path, caplog, force):
    path = tmp_path / 'metadata-only.jsonl'
    meta = {'owner': 'consumer'}
    canonical = metaslot.encode_slot(meta, 128)
    path.write_bytes(canonical[:-1])
    path.with_suffix('.jsonl.idx').write_bytes(b'{}')
    os.utime(str(path) + '.idx')
    caplog.set_level(logging.WARNING, logger='jsonldb.jsonlfile')

    assert jsonlfile.lint_jsonl(str(path), force=force) is True

    assert path.read_bytes() == canonical
    assert metaslot.read_slot(str(path)) == meta
    assert jsonlfile.load_index(str(path)) == {}
    assert sum(
        record.jsonldb_kind == 'layout_repaired'
        for record in caplog.records
        if hasattr(record, 'jsonldb_kind')) == 1

    caplog.clear()
    assert jsonlfile.lint_jsonl(str(path), force=force) is True
    assert path.read_bytes() == canonical
    assert not any(
        getattr(record, 'jsonldb_kind', None) == 'layout_repaired'
        for record in caplog.records)


@pytest.mark.parametrize('force', [False, True])
def test_standalone_lint_keeps_canonical_metadata_only_and_legacy_behavior(
        tmp_path, caplog, force):
    meta = {'owner': 'consumer'}
    slot_path = tmp_path / 'canonical-slot.jsonl'
    slot_bytes = metaslot.encode_slot(meta, 128)
    slot_path.write_bytes(slot_bytes)
    slot_path.with_suffix('.jsonl.idx').write_bytes(b'{}')
    os.utime(str(slot_path) + '.idx')
    caplog.set_level(logging.WARNING, logger='jsonldb.jsonlfile')

    assert jsonlfile.lint_jsonl(str(slot_path), force=force) is True
    assert slot_path.read_bytes() == slot_bytes
    assert jsonlfile.load_index(str(slot_path)) == {}
    assert not any(
        getattr(record, 'jsonldb_kind', None) == 'layout_repaired'
        for record in caplog.records)

    legacy_path = tmp_path / 'legacy.jsonl'
    legacy_path.write_bytes(b'{"row":{"value":1}}')
    legacy_path.with_suffix('.jsonl.idx').write_bytes(b'{"row":0}')
    os.utime(str(legacy_path) + '.idx')
    assert jsonlfile.lint_jsonl(str(legacy_path), force=force) is True
    assert legacy_path.read_bytes() == b'{"row":{"value":1}}\n'
    assert metaslot.inspect_file(str(legacy_path)).is_slot is False
    assert jsonlfile.load_index(str(legacy_path)) == {'row': 0}


@pytest.mark.parametrize('force', [False, True])
@pytest.mark.parametrize('slot_bytes', [None, 128])
def test_lint_canonicalizes_legacy_and_slotted_files(
        tmp_path, force, slot_bytes):
    path = tmp_path / 'table.jsonl'
    rows = {'c': {'value': 3}, 'a': {'value': 1}, 'b': {'value': 2}}
    meta = {'owner': 'consumer'} if slot_bytes else None
    jsonlfile.save_jsonl(
        str(path), rows, meta=meta, slot_bytes=slot_bytes)
    jsonlfile.delete_jsonl(str(path), ['b'])

    assert jsonlfile.lint_jsonl(
        str(path), force=force, slot_bytes=slot_bytes) is True

    _assert_canonical(
        path, {'a': {'value': 1}, 'c': {'value': 3}}, slot_bytes, meta)


def test_folder_lint_repairs_missing_wrong_and_malformed_slots(tmp_path):
    db = FolderDB(str(tmp_path))
    db.set_meta_slot_bytes(128)
    rows = {'row': {'value': 1}}
    db.overwrite_dict('missing', rows)
    db.overwrite_dict('wrong', rows, meta={'kept': True})
    db.overwrite_dict('malformed', rows, meta={'lost': 'invalid'})

    missing = tmp_path / 'missing.jsonl'
    wrong = tmp_path / 'wrong.jsonl'
    malformed = tmp_path / 'malformed.jsonl'
    jsonlfile.save_jsonl(str(missing), rows)
    jsonlfile.save_jsonl(
        str(wrong), rows, meta={'kept': True}, slot_bytes=160)
    bad_prefix = b'{"_meta":{"v":1,"data":'
    bad = bad_prefix + b' ' * (127 - len(bad_prefix)) + b'\n'
    assert len(bad) == 128
    malformed.write_bytes(bad + malformed.read_bytes()[128:])
    for path in (missing, wrong, malformed):
        os.utime(str(path) + '.idx')

    db.lint_db()

    _assert_canonical(missing, rows, 128, None)
    _assert_canonical(wrong, rows, 128, {'kept': True})
    _assert_canonical(malformed, rows, 128, None)
    assert metaslot.inspect_file(str(tmp_path / 'db.meta')).is_slot is False
    assert metaslot.inspect_file(str(tmp_path / 'config.meta')).is_slot is False


@pytest.mark.parametrize('force', [False, True])
def test_lint_repairs_torn_tail_and_missing_newline_without_payload(
        tmp_path, caplog, force):
    path = tmp_path / 'damage.jsonl'
    rows = {'a': {'value': 1}, 'b': {'value': 2}}
    jsonlfile.save_jsonl(str(path), rows)
    secret = b'{"secret":{"payload":"DO-NOT-LOG"}'
    with path.open('ab') as destination:
        destination.write(secret)
    os.utime(str(path) + '.idx')
    caplog.set_level(logging.WARNING, logger='jsonldb.jsonlfile')

    jsonlfile.lint_jsonl(str(path), force=force)

    _assert_canonical(path, rows)
    messages = [record.getMessage() for record in caplog.records]
    removed = [message for message in messages if message.startswith('lint removed')]
    assert removed and all(str(path) in message for message in removed)
    assert all('bytes' in message and 'at byte' in message for message in removed)
    assert all('DO-NOT-LOG' not in message for message in messages)

    path.write_bytes(path.read_bytes()[:-1])
    os.utime(str(path) + '.idx')
    jsonlfile.lint_jsonl(str(path))
    _assert_canonical(path, rows)


def test_force_removes_torn_indexed_row_and_default_drops_fresh_orphan(
        tmp_path):
    indexed = tmp_path / 'indexed.jsonl'
    jsonlfile.save_jsonl(
        str(indexed), {'a': {'value': 1}, 'b': {'value': 2}})
    offset = jsonlfile.load_index(str(indexed))['b']
    indexed.write_bytes(indexed.read_bytes()[:offset] + b'{"b":{"value":')
    os.utime(str(indexed) + '.idx')
    jsonlfile.lint_jsonl(str(indexed), force=True)
    _assert_canonical(indexed, {'a': {'value': 1}})

    orphan = tmp_path / 'orphan.jsonl'
    rows = {'a': {'value': 1}}
    jsonlfile.save_jsonl(str(orphan), rows)
    with orphan.open('ab') as destination:
        destination.write(b'{"z":{"value":9}}\n')
    os.utime(str(orphan) + '.idx')
    jsonlfile.lint_jsonl(str(orphan))
    _assert_canonical(orphan, rows)


def test_clean_fast_path_skips_full_checks_but_force_runs_them(
        tmp_path, monkeypatch):
    path = tmp_path / 'fast.jsonl'
    rows = {key: {'value': key} for key in ('a', 'b', 'c', 'd')}
    jsonlfile.save_jsonl(str(path), rows)
    calls = []
    real_counts = jsonlfile._lint_counts
    real_valid = jsonlfile._lint_index_valid

    def recording_counts(file_path, full):
        calls.append(('counts', full))
        return real_counts(file_path, full)

    def recording_valid(file_path, index, force):
        calls.append(('rows', force))
        return real_valid(file_path, index, force)

    monkeypatch.setattr(jsonlfile, '_lint_counts', recording_counts)
    monkeypatch.setattr(jsonlfile, '_lint_index_valid', recording_valid)
    jsonlfile.lint_jsonl(str(path))
    assert calls == [('rows', False), ('counts', False)]

    calls.clear()
    jsonlfile.lint_jsonl(str(path), force=True)
    assert calls == [('rows', True), ('counts', True)]


def test_lint_atomic_replacement_and_index_publication_order(
        tmp_path, monkeypatch):
    path = tmp_path / 'atomic.jsonl'
    jsonlfile.save_jsonl(
        str(path), {'a': {'value': 1}, 'b': {'value': 2}})
    jsonlfile.delete_jsonl(str(path), ['a'])
    old_data = path.read_bytes()
    old_index = (tmp_path / 'atomic.jsonl.idx').read_bytes()
    real_replace = os.replace

    def fail_data_replace(source, destination):
        if str(destination) == str(path):
            raise OSError('injected data replacement failure')
        return real_replace(source, destination)

    with monkeypatch.context() as patch:
        patch.setattr(jsonlfile.os, 'replace', fail_data_replace)
        with pytest.raises(OSError, match='data replacement failure'):
            jsonlfile.lint_jsonl(str(path))
    assert path.read_bytes() == old_data
    assert (tmp_path / 'atomic.jsonl.idx').read_bytes() == old_index
    assert not list(tmp_path.glob('.atomic.jsonl.*.tmp'))

    destinations = []

    def record_replaces(source, destination):
        destinations.append(str(destination))
        return real_replace(source, destination)

    monkeypatch.setattr(jsonlfile.os, 'replace', record_replaces)
    jsonlfile.lint_jsonl(str(path))
    assert destinations[-2:] == [str(path), str(path) + '.idx']
    _assert_canonical(path, {'b': {'value': 2}})

    jsonlfile.update_jsonl(str(path), {'c': {'value': 3}})
    jsonlfile.delete_jsonl(str(path), ['b'])
    stale_index = (tmp_path / 'atomic.jsonl.idx').read_bytes()
    real_write_index = jsonlfile._write_index

    def fail_index_publish(file_path, index):
        if file_path == str(path):
            raise OSError('injected index publication failure')
        return real_write_index(file_path, index)

    with monkeypatch.context() as patch:
        patch.setattr(jsonlfile, '_write_index', fail_index_publish)
        with pytest.raises(OSError, match='index publication failure'):
            jsonlfile.lint_jsonl(str(path))
    assert path.read_bytes() == b'{"c":{"value":3}}\n'
    assert (tmp_path / 'atomic.jsonl.idx').read_bytes() == stale_index
    assert orjson.loads(stale_index)['c'] > 0

    jsonlfile.lint_jsonl(str(path))
    _assert_canonical(path, {'c': {'value': 3}})
