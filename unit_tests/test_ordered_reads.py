"""Logical read ordering is independent of physical layout and strictness."""
import datetime as dt
from pathlib import Path

import orjson
import pandas as pd
import pytest

from jsonldb import FolderDB, jsonlfile as jf, jsonldf as jdf


@pytest.mark.parametrize('strict', [False, True])
@pytest.mark.parametrize('bounds', [(None, None), ('2026-06-05', None),
    (None, '2026-06-07'), ('2026-06-05', '2026-06-07')])
@pytest.mark.parametrize('method', ['get_dict', 'get_df', 'get_dict_with_meta', 'get_df_with_meta'])
def test_full_and_bounded_wrappers_share_order_and_preserve_data(tmp_path, strict, bounds, method):
    db = FolderDB(str(tmp_path))
    db.set_meta_slot_bytes(128)
    meta = {'owner': 'consumer'}
    db.overwrite_dict('table', {'2026-06-06': {'v': 1}, '2026-06-07': {'v': None}}, meta=meta)
    db.upsert_dict('table', {'2026-06-05': {'v': 2}})
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir() if p.is_file()}
    names = 'table' if method.endswith('_with_meta') else ['table']
    result = getattr(db, method)(names, *bounds, False, strict=strict)
    if method.endswith('_with_meta'):
        assert result.meta == meta
        rows = result.rows
    else:
        rows = result['table']
    keys = list(rows.index) if isinstance(rows, pd.DataFrame) else list(rows)
    assert keys == ['2026-06-05', '2026-06-06', '2026-06-07']
    if not isinstance(rows, pd.DataFrame):
        assert rows == {'2026-06-05': {'v': 2}, '2026-06-06': {'v': 1}, '2026-06-07': {'v': None}}
    assert {p.name: p.read_bytes() for p in tmp_path.iterdir() if p.is_file()} == before


def test_duplicates_and_record_fields_preserved_before_ordering(tmp_path):
    path = str(tmp_path / 'rows.jsonl')
    Path(path).write_bytes(b'{"z":{"v":1}}\n{"a":{"nullable":null,"nested":{"x":[]}}}\n'
                          b'{"z":{"new":2}}\n{"z":null}\n')
    rows = jf.load_jsonl(path, False)
    assert list(rows) == ['a', 'z']
    assert rows == {'a': {'nullable': None, 'nested': {'x': []}}, 'z': {'new': 2}}
    with pytest.raises(ValueError):
        jf.load_jsonl(path, False, strict=True)


@pytest.mark.parametrize('timespec, first, second', [
    ('seconds', '2026-06-07T09:00:00', '2026-06-05T09:00:00'),
    ('microseconds', '2026-06-07T09:00:00.000001', '2026-06-05T09:00:00.000001'),
])
def test_serialized_comparison_with_datetime_and_string_fallbacks(tmp_path, timespec, first, second):
    path = str(tmp_path / 'mixed.jsonl')
    keys = [first, 'invalid-date', '2', second, '10']
    jf.save_jsonl(path, {key: {'v': i} for i, key in enumerate(keys)})
    raw = jf.load_jsonl(path, False, timespec)
    converted = jf.load_jsonl(path, True, timespec)
    expected = [dt.datetime.fromisoformat(k) if k in (first, second) else k for k in sorted(keys)]
    assert list(raw) == sorted(keys)
    assert list(converted) == expected
    assert list(jdf.load_jsonldf(path, timespec, True).index) == expected


@pytest.mark.parametrize('reverse', [False, True])
def test_converted_aliases_keep_last_physical_winner(tmp_path, reverse):
    aliases = ['2026-06-05T09:00:00.000001', '2026-06-05T09:00:00,000001']
    if reverse:
        aliases.reverse()
    path = tmp_path / 'aliases.jsonl'
    pairs = [(aliases[0], {'v': 1}), (aliases[1], {'v': 2}), (aliases[0], {'v': 3})]
    marker = '2026-06-05T09:00:00-'  # Lexically between comma and period spellings.
    path.write_bytes(orjson.dumps({marker: {}}) + b'\n' +
                     b''.join(orjson.dumps({key: row}) + b'\n' for key, row in pairs))
    rows = jf.load_jsonl(str(path), True, 'microseconds')
    stamp = dt.datetime(2026, 6, 5, 9, 0, 0, 1)
    assert rows == {stamp: {'v': 3}, marker: {}}
    assert list(rows) == ([stamp, marker] if reverse else [marker, stamp])
    raw = jf.load_jsonl(str(path), False, 'microseconds')
    assert list(raw) == sorted(aliases + [marker])
    assert raw[aliases[0]] == {'v': 3} and raw[aliases[1]] == {'v': 2}


def test_full_strict_still_finds_damage_excluded_from_index(tmp_path, monkeypatch):
    path = str(tmp_path / 'damaged.jsonl')
    Path(path).write_bytes(b'{"z":{}}\n{broken\n{"a":{}}\n')
    jf.build_jsonl_index(path)
    jf.select_jsonl(path, 'a', 'z')  # Warm the permissive index.
    before = Path(path).read_bytes()
    with pytest.raises(ValueError, match='byte 9'):
        jf.load_jsonl(path, strict=True)
    assert list(jf.load_jsonl(path)) == ['a', 'z']
    def forbidden(*args, **kwargs):
        pytest.fail('indexed read introduced a full-read ordering pass')
    monkeypatch.setattr(jf, '_ordered_rows', forbidden)
    for bounds in [('a', None), (None, 'z'), ('a', 'z'), ('a', 'a')]:
        assert jf.select_jsonl(path, *bounds, strict=True)
    assert jf.select_line_jsonl(path, 'a', strict=True) == {'a': {}}
    assert Path(path).read_bytes() == before


def test_ordered_fast_path_preserves_dictionary_and_does_not_sort(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail('already ordered rows were sorted')
    monkeypatch.setattr(jf, 'sorted', forbidden, raising=False)
    for rows in ({}, {'a': {}}, {'a': {}, 'z': {}}):
        assert jf._ordered_rows(rows, {}) is rows
    timestamp = dt.datetime(2026, 6, 5)
    rows = {'10': {}, timestamp: {}, 'z': {}}
    assert jf._ordered_rows(rows, {timestamp: '2026-06-05T00:00:00'}) is rows


def test_reordering_stops_at_inversion_and_shares_record_values():
    class Keys(dict):
        def __init__(self):
            super().__init__()
            self.calls = []
        def get(self, key, default):
            self.calls.append(key)
            return default
        def __bool__(self):
            return True
    keys = Keys()
    rows = {'b': {'v': []}, 'a': {'v': []}, 'z': {'v': []}}
    ordered = jf._ordered_rows(rows, keys)
    assert list(ordered) == ['a', 'b', 'z']
    assert all(ordered[key] is rows[key] for key in rows)
    assert keys.calls == ['b', 'a', 'b', 'a', 'z']  # Two checks, then sort-key extraction.


def test_empty_missing_and_multiple_tables(tmp_path):
    db = FolderDB(str(tmp_path))
    db.overwrite_dict('ztable', {})
    db.overwrite_dict('atable', {'z': {}, 'a': {}})
    assert db.get_dict(['missing']) == {}
    assert db.get_dict_with_meta('missing') == (None, {})
    assert db.get_df_with_meta('ztable').rows.empty
    rows = db.get_dict(['ztable', 'atable'])
    assert list(rows) == ['ztable', 'atable']
    assert rows['ztable'] == {} and list(rows['atable']) == ['a', 'z']
