"""Unknown slots survive row operations and reject explicit metadata edits."""
from contextlib import nullcontext

import pandas as pd
import pytest

from jsonldb import FolderDB, jsonlfile as jf, jsonldf as jdf, metaslot


ENVELOPE = b' {"_meta": {"v":99,"data":{"owner":"keep-me","v":7},"future":[true,3]}}'
ROWS = {'a': {'v': 1}, 'z': {'v': 2}}


def seed(db, name='table'):
    path = db.folder_path + '/' + name + '.jsonl'
    slot = ENVELOPE + b' '*(159-len(ENVELOPE)) + b'\n'
    with open(path, 'wb') as stream:
        stream.write(slot + b'{"a":{"v":1}}\n{"z":{"v":2}}\n')
    jf.build_jsonl_index(path)
    db.build_dbmeta()
    jf.select_line_jsonl(path, 'a')  # Exercise invalidation of a retained index.
    return path, slot


def snapshot(root):
    return {str(p.relative_to(root)): p.read_bytes() for p in root.rglob('*') if p.is_file()}


@pytest.mark.parametrize('operation', ['save_jsonl', 'update_jsonl', 'save_jsonldf',
    'update_jsonldf', 'overwrite_dict', 'upsert_dict', 'overwrite_df', 'upsert_df'])
@pytest.mark.parametrize('meta', [None, {}, {'v': 99, 'owner': 'replacement'}])
def test_row_writes_preserve_unknown_and_reject_explicit_metadata(tmp_path, operation, meta):
    db = FolderDB(str(tmp_path))
    db.set_meta_slot_bytes(160)
    path, slot = seed(db)
    data = {'b': {'v': 3}}
    if operation.endswith(('ldf', '_df')):
        data = pd.DataFrame({'v': [3]}, index=['b'])
    before = snapshot(tmp_path)
    expected_error = (nullcontext() if meta is None else
                      pytest.raises(ValueError, match='unsupported envelope version 99'))
    with expected_error:
        if operation in ('save_jsonl', 'save_jsonldf'):
            module = jdf if operation.endswith('ldf') else jf
            getattr(module, operation)(path, data, meta=meta, slot_bytes=160)
        elif operation in ('update_jsonl', 'update_jsonldf'):
            module = jdf if operation.endswith('ldf') else jf
            getattr(module, operation)(path, data, meta=meta)
        else:
            getattr(db, operation)('table', data, meta=meta)
    if meta is not None:
        assert snapshot(tmp_path) == before
        return
    info = metaslot.inspect_file(path)
    assert info.version == 99 and info.raw_line == slot and info.record is None
    expected = {'b': {'v': 3}}
    if operation.startswith(('upsert', 'update')):
        expected = {**ROWS, **expected}
    assert jf.load_jsonl(path) == expected
    assert jf.select_line_jsonl(path, 'b') == {'b': {'v': 3}}
    assert all(offset >= 160 for offset in jf.load_index(path).values())


@pytest.mark.parametrize('width', [128, 160, 256])
@pytest.mark.parametrize('folder_operation', [False, True])
def test_resize_preserves_opaque_envelope_rows_and_offsets(tmp_path, width, folder_operation):
    db = FolderDB(str(tmp_path))
    db.set_meta_slot_bytes(160)
    path, slot = seed(db)
    before = (tmp_path/'table.jsonl').read_bytes()[160:]
    if folder_operation:
        db.set_meta_slot_bytes(width)
        assert FolderDB(str(tmp_path)).meta_slot_bytes == width
    else:
        jf.migrate_jsonl_slot(path, width)
    info = metaslot.inspect_file(path)
    assert info.version == 99 and info.record is None and info.width == width
    assert info.raw_line == ENVELOPE + b' '*(width-len(ENVELOPE)-1) + b'\n'
    assert (tmp_path/'table.jsonl').read_bytes()[width:] == before
    assert jf.load_index(path)['a'] == width
    assert jf.select_line_jsonl(path, 'a') == {'a': {'v': 1}}


@pytest.mark.parametrize('operation', ['resize_folder', 'resize_file', 'save_file'])
def test_unsafe_shrink_preserves_all_files_and_live_configuration(tmp_path, operation):
    db = FolderDB(str(tmp_path))
    db.set_meta_slot_bytes(160)
    path, _ = seed(db)
    before = snapshot(tmp_path)
    with pytest.raises(ValueError) as error:
        if operation == 'resize_folder':
            db.set_meta_slot_bytes(48)
        elif operation == 'resize_file':
            jf.migrate_jsonl_slot(path, 48)
        else:
            jf.save_jsonl(path, {'new': {}}, slot_bytes=48)
    assert ('table' in str(error.value) if operation == 'resize_folder'
            else 'envelope requires' in str(error.value))
    assert snapshot(tmp_path) == before
    assert db.meta_slot_bytes == 160


@pytest.mark.parametrize('operation', ['clear_meta', 'write_jsonl_meta', 'write_slot'])
@pytest.mark.parametrize('index_state', ['valid', 'missing', 'corrupt'])
def test_metadata_edits_reject_unknown_before_index_recovery(tmp_path, operation, index_state):
    db = FolderDB(str(tmp_path))
    db.set_meta_slot_bytes(160)
    path, _ = seed(db)
    index = tmp_path / 'table.jsonl.idx'
    if index_state == 'missing':
        index.unlink()
    elif index_state == 'corrupt':
        index.write_bytes(b'broken index')
    before = snapshot(tmp_path)
    records = (None,) if operation == 'clear_meta' else (None, {}, {'owner': 'replacement'})
    for meta in records:
        with pytest.raises(ValueError, match='unsupported envelope version 99'):
            if operation == 'clear_meta':
                db.clear_meta('table')
            elif operation == 'write_jsonl_meta':
                jf.write_jsonl_meta(path, meta)
            else:
                metaslot.write_slot(path, meta)
        assert snapshot(tmp_path) == before
