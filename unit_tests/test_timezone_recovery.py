"""Timezone preservation, refusal and publication boundaries."""
from pathlib import Path

import orjson
import pytest

from jsonldb import jsonlfile as jf, metaslot
from test_table_timezone import table, stamp, snapshot


DAMAGED = [
    b'{"_meta":{"v":1,"timezone":null}}',
    b'{"_meta":{"v":1,"timezone":123}}',
    b'{"_meta":{"v":1,"timezone":"+24:00"}}',
    b'{"_meta":{"v":1,"timezone":"Asia/Hong_Kong"}}',
    b'{"_meta":{"v":1,"timezone":',
    b'{"_meta":{"v":true,"timezone":"+08:00"}}',
]


@pytest.mark.parametrize('raw', DAMAGED)
@pytest.mark.parametrize('operation', ['save', 'update', 'delete', 'meta', 'direct_meta',
                                      'lint', 'resize', 'folder_resize', 'atomic'])
def test_malformed_timezone_refused_without_discarding_bytes(tmp_path, raw, operation):
    db, path = table(tmp_path)
    with open(path, 'wb') as stream:
        stream.write(raw + b' '*(191-len(raw)) + b'\n{"a":{"v":1}}\n')
    Path(path+'.idx').unlink()
    before = snapshot(tmp_path)
    with pytest.raises(ValueError):
        if operation == 'save':
            jf.save_jsonl(path, {'b': {'v': 2}})
        elif operation == 'update':
            jf.update_jsonl(path, {'b': {'v': 2}})
        elif operation == 'delete':
            jf.delete_jsonl(path, ['a'])
        elif operation == 'meta':
            jf.write_jsonl_meta(path, {'v': 2})
        elif operation == 'direct_meta':
            metaslot.write_slot(path, {'v': 2})
        elif operation == 'lint':
            jf.lint_jsonl(path, force=True)
        elif operation == 'resize':
            jf.migrate_jsonl_slot(path, 256)
        elif operation == 'folder_resize':
            db.set_meta_slot_bytes(256)
        else:
            jf.save_jsonl_atomic(path, {})
    assert snapshot(tmp_path) == before


@pytest.mark.parametrize('phase', ['during_rows', 'before_index'])
def test_overwrite_failure_keeps_timezone_with_completed_rows(tmp_path, monkeypatch, phase):
    db, path = table(tmp_path)
    db.overwrite_dict('table', {stamp(): {'v': 'old'}}, meta={'old': True})
    real_dumps = jf._fast_dumps
    calls = []

    def broken_dumps(row):
        calls.append(row)
        if len(calls) == 2:
            raise RuntimeError('injected row serialization failure')
        return real_dumps(row)

    def broken_index(*args):
        raise RuntimeError('injected index publication failure')

    with monkeypatch.context() as patch:
        patch.setattr(jf, '_fast_dumps' if phase == 'during_rows' else '_write_index',
                      broken_dumps if phase == 'during_rows' else broken_index)
        with pytest.raises(RuntimeError, match='injected'):
            jf.save_jsonl(path, {stamp(): {'v': 'new'}, 'plain': {'v': 2}}, meta={'new': True})
    assert jf.read_jsonl_timezone(path) == '+08:00'
    assert jf.load_jsonl(path)[stamp().replace(tzinfo=None)] == {'v': 'new'}
    assert jf.read_jsonl_meta(path) == (None if phase == 'during_rows' else {'new': True})
    jf.lint_jsonl(path, force=True)
    assert jf.read_jsonl_timezone(path) == '+08:00'
    assert jf.select_line_jsonl(path, stamp())


def test_upsert_failure_before_data_publication_keeps_interpretation(tmp_path, monkeypatch):
    db, path = table(tmp_path)
    db.overwrite_dict('table', {stamp(): {'v': 'old'}}, meta={'old': True})

    def broken_blank(*args):
        assert jf.read_jsonl_timezone(path) == '+08:00'
        assert jf.read_jsonl_meta(path) == {'new': True}
        assert jf.load_jsonl(path)[stamp().replace(tzinfo=None)] == {'v': 'longer replacement'}
        raise RuntimeError('injected tombstone failure')

    with monkeypatch.context() as patch:
        patch.setattr(jf, '_blank_old_lines', broken_blank)
        with pytest.raises(RuntimeError, match='injected'):
            jf.update_jsonl(path, {stamp(): {'v': 'longer replacement'}}, meta={'new': True})
    jf.lint_jsonl(path, force=True)
    assert jf.read_jsonl_timezone(path) == '+08:00'
    assert jf.select_line_jsonl(path, stamp()) == {stamp().replace(tzinfo=None): {'v': 'longer replacement'}}


def test_atomic_control_writer_cannot_erase_declaration(tmp_path):
    db, path = table(tmp_path)
    before = snapshot(tmp_path)
    with pytest.raises(ValueError, match='atomic control-file'):
        jf.save_jsonl_atomic(path, {'a': {'v': 1}})
    assert snapshot(tmp_path) == before


@pytest.mark.parametrize('operation', ['save', 'lint', 'migrate', 'meta'])
def test_complete_slot_fit_before_missing_index_rebuild(tmp_path, operation):
    db, path = table(tmp_path)
    Path(path+'.idx').unlink()
    before = snapshot(tmp_path)
    with pytest.raises(ValueError, match='requires'):
        if operation == 'save':
            jf.save_jsonl(path, {}, slot_bytes=32)
        elif operation == 'lint':
            jf.lint_jsonl(path, slot_bytes=32)
        elif operation == 'migrate':
            jf.migrate_jsonl_slot(path, 32)
        else:
            jf.write_jsonl_meta(path, {'too_large': 'x'*256})
    assert snapshot(tmp_path) == before


def test_consumer_timezone_field_has_no_library_meaning(tmp_path):
    db, path = table(tmp_path)
    db.set_timezone('table', None)
    db.overwrite_dict('table', {stamp(): {'v': 1}}, meta={'timezone': '+09:00'})
    assert jf.read_jsonl_timezone(path) is None
    assert jf.load_jsonl(path, False) == {stamp().isoformat(): {'v': 1}}
    assert jf.read_jsonl_meta(path) == {'timezone': '+09:00'}


def test_external_slot_change_is_seen_after_cached_lookup(tmp_path):
    db, path = table(tmp_path)
    db.overwrite_dict('table', {stamp(): {'v': 1}})
    assert jf.select_line_jsonl(path, stamp())
    with open(path, 'rb+') as stream:
        stream.write(metaslot._encode_slot(None, 192, '+09:00'))
    with pytest.raises(ValueError, match='conflicts'):
        jf.select_line_jsonl(path, stamp())
    assert jf.select_line_jsonl(path, stamp('+09:00'))
