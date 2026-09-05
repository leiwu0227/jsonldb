"""Table-wide fixed offsets are independent of consumer metadata and row values."""
import datetime as dt
from pathlib import Path

import orjson
import pandas as pd
import pytest

from jsonldb import FolderDB, jsonlfile as jf, jsonldf as jdf, metaslot


def table(root, zone='+08:00', spec='seconds', name='table'):
    jf.save_jsonl(str(root/'config.meta'), {'config': {'timespec': spec}})
    db = FolderDB(str(root))
    db.set_meta_slot_bytes(192)
    db.overwrite_dict(name, {}, meta={'timezone': 'consumer-owned'})
    db.set_timezone(name, zone)
    return db, str(root/(name+'.jsonl'))


def snapshot(root):
    return {str(p.relative_to(root)): p.read_bytes() for p in root.rglob('*') if p.is_file()}


def stamp(zone='+08:00', spec='seconds'):
    offset = int(zone[1:3])*60 + int(zone[4:])
    tz = dt.timezone(dt.timedelta(minutes=offset if zone[0] == '+' else -offset))
    return dt.datetime(2026, 9, 5, 10, 30, 15, 123456 if spec == 'microseconds' else 0, tz)


@pytest.mark.parametrize('value, expected', [('+8:00', '+08:00'), ('-3:30', '-03:30'),
    ('UTC', '+00:00'), ('-0:00', '+00:00'), ('+00:00', '+00:00'),
    ('+23:59', '+23:59'), ('-23:59', '-23:59')])
@pytest.mark.parametrize('low_level', [False, True])
def test_configuration_normalization_and_independent_data(tmp_path, value, expected, low_level):
    db, path = table(tmp_path, None)
    if low_level:
        jf.write_jsonl_timezone(path, value)
    else:
        db.set_timezone('table', value)
    assert jf.read_jsonl_timezone(path) == db.read_timezone('table') == expected
    envelope = orjson.loads(Path(path).read_bytes().splitlines()[0])['_meta']
    assert envelope == {'v': 1, 'timezone': expected, 'data': {'timezone': 'consumer-owned'}}
    assert FolderDB(str(tmp_path)).read_timezone('table') == expected
    db.set_timezone('table', None)
    assert db.read_timezone('table') is None
    assert db.read_meta('table') == {'timezone': 'consumer-owned'}


@pytest.mark.parametrize('value', ['', '8:00', '+24:00', '-24:00', '+8:60', '+008:00',
    '+08:0', '+08:00:01', 'Asia/Hong_Kong', 'Z', 'utc', ' +08:00', 8, {}, True])
def test_invalid_configuration_never_mutates(tmp_path, value):
    db, path = table(tmp_path)
    before = snapshot(tmp_path)
    with pytest.raises(ValueError if isinstance(value, str) else TypeError):
        db.set_timezone('table', value)
    assert snapshot(tmp_path) == before


@pytest.mark.parametrize('kind', ['missing', 'unslotted', 'unknown', 'malformed', 'too_small'])
def test_setter_requires_known_fitting_slot(tmp_path, kind):
    path = str(tmp_path/'table.jsonl')
    if kind == 'unslotted':
        jf.save_jsonl(path, {})
    elif kind in ('unknown', 'malformed'):
        Path(path).write_bytes((b'{"_meta":{"v":99}}\n' if kind == 'unknown'
                              else b'{"_meta":{"v":1,"timezone":null}}\n'))
    elif kind == 'too_small':
        jf.save_jsonl(path, {}, slot_bytes=24)
    before = snapshot(tmp_path)
    with pytest.raises(FileNotFoundError if kind == 'missing' else ValueError):
        jf.write_jsonl_timezone(path, '+8:00')
    assert snapshot(tmp_path) == before
    if kind in ('missing', 'unslotted', 'unknown', 'too_small'):
        assert jf.read_jsonl_timezone(path) is None
    else:
        with pytest.raises(ValueError):
            jf.read_jsonl_timezone(path)


def test_populated_configuration_uses_rows_not_stale_index(tmp_path):
    db, path = table(tmp_path)
    db.overwrite_dict('table', {stamp(): {'v': 1}})
    Path(path+'.idx').write_bytes(b'{}')
    before = snapshot(tmp_path)
    db.set_timezone('table', '+8:00')
    assert snapshot(tmp_path) == before
    for value in (None, 'UTC'):
        with pytest.raises(ValueError, match='populated'):
            db.set_timezone('table', value)
        assert snapshot(tmp_path) == before
    jf.build_jsonl_index(path)  # Repair the deliberately falsified fresh index.
    jf.delete_jsonl(path, [stamp()])
    db.set_timezone('table', None)  # Tombstones are not populated rows.
    assert db.read_timezone('table') is None


OPERATIONS = ['save_jsonl', 'update_jsonl', 'save_jsonldf', 'update_jsonldf',
              'overwrite_dict', 'upsert_dict', 'overwrite_df', 'upsert_df']


def write(db, path, operation, data, spec, meta=None):
    if operation.endswith(('ldf', '_df')):
        data = pd.DataFrame(list(data.values()), index=pd.Index(list(data), dtype=object))
    if operation.endswith(('jsonl', 'jsonldf')):
        owner = jdf if operation.endswith('ldf') else jf
        getattr(owner, operation)(path, data, timespec=spec, meta=meta)
    else:
        getattr(db, operation)('table', data, meta=meta)


@pytest.mark.parametrize('operation', OPERATIONS)
@pytest.mark.parametrize('spec', ['seconds', 'microseconds'])
@pytest.mark.parametrize('zone', ['+08:00', '-03:30', '+00:00'])
def test_writes_queries_deletes_and_restart(tmp_path, operation, spec, zone):
    db, path = table(tmp_path, zone, spec)
    key = stamp(zone, spec)
    text = key.replace(tzinfo=None).isoformat(timespec=spec)
    rows = {key: {'timezone': 'row value'}, 'plain': {'timezone': 'other'}}
    write(db, path, operation, rows, spec, meta={'replacement': True})
    assert list(rows)[0] is key and key.tzinfo is not None
    assert jf.load_jsonl(path, False, spec) == {text: {'timezone': 'row value'}, 'plain': {'timezone': 'other'}}
    assert jf.select_line_jsonl(path, key, timespec=spec) == {key.replace(tzinfo=None): rows[key]}
    assert jf.select_line_jsonl(path, key.isoformat(), False, spec) == {text: rows[key]}
    for lower, upper in [(key, key), (key, key.isoformat()), (None, key), (key, None)]:
        selected = jf.select_jsonl(path, lower, upper, False, spec)
        assert selected[text] == rows[key]
    assert db.get_dict(['table'], key, key, False)['table'] == {text: rows[key]}
    assert isinstance(db.get_df(['table'], key, key)['table'].index, pd.DatetimeIndex)
    assert db.get_df(['table'], key, key)['table'].index.tz is None
    assert db.get_dict_with_meta('table', key, key).meta == {'replacement': True}
    assert db.get_df_with_meta('table', key, key).meta == {'replacement': True}
    assert FolderDB(str(tmp_path)).read_timezone('table') == zone
    db.delete_file_range('table', key, key.isoformat())
    assert jf.load_jsonl(path, False, spec) == {'plain': {'timezone': 'other'}}
    assert db.read_timezone('table') == zone


@pytest.mark.parametrize('operation', OPERATIONS)
@pytest.mark.parametrize('bad', ['conflicting_datetime', 'conflicting_string', 'bad_suffix', 'record'])
def test_write_errors_preflight_before_index_recovery(tmp_path, operation, bad):
    db, path = table(tmp_path)
    bad_key = stamp('+09:00') if bad == 'conflicting_datetime' else (
        stamp('+09:00').isoformat() if bad == 'conflicting_string' else
        '2026-09-05T10:30:15+broken' if bad == 'bad_suffix' else 'plain')
    data = {stamp(): {'v': 1}, bad_key: {'v': 2}}
    if bad == 'record':
        # DataFrame conversion owns record shape; use an invalid reserved key there.
        data = {stamp(): {'v': 1}, '_meta': {'v': 2}}
    Path(path+'.idx').unlink()
    before = snapshot(tmp_path)
    with pytest.raises(ValueError):
        write(db, path, operation, data, 'seconds')
    assert snapshot(tmp_path) == before


@pytest.mark.parametrize('operation', ['point', 'range', 'equal_range', 'delete', 'folder_range'])
def test_conflicting_bounds_refused_before_index_recovery(tmp_path, operation):
    db, path = table(tmp_path)
    db.overwrite_dict('table', {stamp(): {'v': 1}})
    Path(path+'.idx').unlink()
    before = snapshot(tmp_path)
    bad = stamp('+09:00')
    with pytest.raises(ValueError):
        if operation == 'point':
            jf.select_line_jsonl(path, bad)
        elif operation in ('range', 'equal_range'):
            jf.select_jsonl(path, bad, bad if operation == 'equal_range' else None)
        elif operation == 'delete':
            jf.delete_jsonl(path, [stamp(), bad])
        else:
            db.delete_file_range('table', stamp(), bad)
    assert snapshot(tmp_path) == before


@pytest.mark.parametrize('operation', ['overwrite_dicts', 'upsert_dicts', 'overwrite_dfs', 'upsert_dfs'])
def test_plural_keeps_per_table_completion(tmp_path, operation):
    db, path = table(tmp_path)
    db.overwrite_dict('second', {})
    db.set_timezone('second', 'UTC')
    first = {stamp(): {'v': 1}}
    second = {stamp(): {'v': 2}}
    if operation.endswith('dfs'):
        first, second = [pd.DataFrame(list(d.values()), index=list(d)) for d in (first, second)]
    before_second = (tmp_path/'second.jsonl').read_bytes()
    with pytest.raises(ValueError):
        getattr(db, operation)({'table': first, 'second': second})
    assert jf.load_jsonl(path)
    assert (tmp_path/'second.jsonl').read_bytes() == before_second
    assert db.get_dbmeta()['table']['count'] == 1


@pytest.mark.parametrize('operation', ['clear', 'replace', 'overwrite', 'upsert', 'lint', 'resize'])
def test_preservation_and_complete_envelope_fit(tmp_path, operation):
    db, path = table(tmp_path)
    db.overwrite_dict('table', {stamp(): {'v': 1}})
    if operation == 'clear':
        db.clear_meta('table')
    elif operation == 'replace':
        jf.write_jsonl_meta(path, {'timezone': 'still consumer data'})
    elif operation == 'overwrite':
        db.overwrite_dict('table', {stamp(): {'v': 2}})
    elif operation == 'upsert':
        db.upsert_dict('table', {stamp(): {'v': 2}})
    elif operation == 'lint':
        jf.lint_jsonl(path, force=True)
    else:
        db.set_meta_slot_bytes(256)
    assert db.read_timezone('table') == '+08:00'
    assert jf.select_line_jsonl(path, stamp())
    before = snapshot(tmp_path)
    with pytest.raises(ValueError):
        db.set_meta_slot_bytes(32)  # An empty v1 envelope fits; its timezone does not.
    assert snapshot(tmp_path) == before


def test_collision_rows_are_not_precollapsed(tmp_path):
    db, path = table(tmp_path)
    key = stamp()
    text = key.replace(tzinfo=None).isoformat()
    rows = {key: {'v': 1}, key.isoformat(): {'v': 2}, text: {'v': 3}}
    db.overwrite_dict('table', rows)
    raw_rows = Path(path).read_bytes().splitlines()[1:]
    assert [orjson.loads(raw)[text]['v'] for raw in raw_rows] == [1, 2, 3]
    assert jf.load_jsonl(path, False) == {text: {'v': 3}}
    assert jf.select_line_jsonl(path, key, False) == {text: {'v': 3}}


@pytest.mark.parametrize('key, zone', [('2026-09-05T10:30:15+08:60', '+09:00'),
    ('2026-09-05T10:30:15+08:00:60', '+08:01'),
    ('2026-09-05T10:30:15-08:60', '-09:00'),
    ('2026-09-05T10:30:15.123456+08:60', '+09:00')])
@pytest.mark.parametrize('operation', ['save', 'update', 'point', 'range', 'delete'])
def test_invalid_iso_components_are_not_silently_normalized(tmp_path, key, zone, operation):
    db, path = table(tmp_path, zone)
    Path(path+'.idx').unlink()
    before = snapshot(tmp_path)
    with pytest.raises(ValueError, match='invalid timezone-bearing'):
        if operation == 'save':
            jf.save_jsonl(path, {key: {'v': 1}})
        elif operation == 'update':
            jf.update_jsonl(path, {key: {'v': 1}})
        elif operation == 'point':
            jf.select_line_jsonl(path, key)
        elif operation == 'range':
            jf.select_jsonl(path, key, None)
        else:
            jf.delete_jsonl(path, [key])
    assert snapshot(tmp_path) == before


@pytest.mark.parametrize('tail', ['_batch-7', '_zoneZ', '_batch+8', '.123456_label-7'])
def test_timestamp_like_labels_remain_arbitrary_strings(tmp_path, tail):
    db, path = table(tmp_path)
    key = '2026-09-05T10:30:15'+tail
    db.overwrite_dict('table', {key: {'v': 1}})
    assert jf.select_line_jsonl(path, key) == {key: {'v': 1}}
    jf.delete_jsonl(path, [key])
    assert jf.load_jsonl(path) == {}


@pytest.mark.parametrize('suffix', ['Z', '+00', '+0000', '+00:00', '+00:00:00', '+000000'])
def test_valid_iso_zero_offsets(tmp_path, suffix):
    db, path = table(tmp_path, 'UTC')
    key = '2026-09-05T10:30:15'+suffix
    db.overwrite_dict('table', {key: {'v': 1}})
    assert jf.load_jsonl(path, False) == {'2026-09-05T10:30:15': {'v': 1}}


@pytest.mark.parametrize('suffix', ['+00:00:00.5', '+00:00:00.0000001', '-00:00:00.5'])
def test_fractional_zero_offsets_are_not_discarded(tmp_path, suffix):
    db, path = table(tmp_path, 'UTC')
    before = snapshot(tmp_path)
    with pytest.raises(ValueError, match='conflicts'):
        db.overwrite_dict('table', {'2026-09-05T10:30:15'+suffix: {'v': 1}})
    assert snapshot(tmp_path) == before
