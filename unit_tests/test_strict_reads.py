"""Strictness concerns encountered observations, not index completeness."""
import datetime as dt
import inspect
import os
from pathlib import Path

import orjson
import pandas as pd
import pytest

from jsonldb import FolderDB, jsonlfile as jf, jsonldf as jdf, metaslot


FIRST = b'{"a":{"v":1}}\n'
LAST = b'{"z":{"v":3}}\n'


def indexed_table(tmp_path, middle):
    path = tmp_path / 'table.jsonl'
    path.write_bytes(FIRST + middle + LAST)
    index = {'a': 0, 'b': len(FIRST), 'z': len(FIRST) + len(middle)}
    write_index(path, index)
    return str(path)


def write_index(path, index):
    sidecar = Path(str(path) + '.idx')
    sidecar.write_bytes(orjson.dumps(index, option=orjson.OPT_SORT_KEYS))
    stamp = Path(path).stat().st_mtime_ns + 1_000_000_000
    os.utime(sidecar, ns=(stamp, stamp))
    jf._invalidate_index_cache(str(path))


def read(path, mode, **options):
    if mode == 'full':
        return jf.load_jsonl(path, False, None, **options)
    if mode == 'point':
        return jf.select_line_jsonl(path, 'b', False, None, **options)
    bounds = {'range': ('a', 'z'), 'equal': ('b', 'b'), 'all': (None, None)}
    return jf.select_jsonl(path, *bounds[mode], False, None, **options)


@pytest.mark.parametrize('mode', ['full', 'range', 'point', 'equal', 'all'])
@pytest.mark.parametrize('middle', [b'{broken\n', b'[]\n', b'{}\n',
    b'{"b":null}\n', b'{"b":{},"c":{}}\n', b'\xff\n'])
def test_encountered_damage_raises_with_location_and_preserves_default(tmp_path, mode, middle):
    path = indexed_table(tmp_path, middle)
    before = Path(path).read_bytes()
    assert read(path, mode) == read(path, mode, strict=False)
    with pytest.raises(ValueError) as error:
        read(path, mode, strict=True)
    assert path in str(error.value)
    assert f'byte {len(FIRST)}:' in str(error.value)
    assert str(error.value).split(': ', 1)[1]
    assert Path(path).read_bytes() == before


@pytest.mark.parametrize('mode', ['range', 'point', 'equal'])
@pytest.mark.parametrize('damage', ['mismatch', 'blank', 'eof', 'negative', 'text', 'huge'])
def test_bad_selected_observation_and_offset_fail(tmp_path, mode, damage):
    middle = {'mismatch': b'{"c":{}}\n', 'blank': b'    \n'}.get(damage, b'{"b":{}}\n')
    path = indexed_table(tmp_path, middle)
    offset = {'eof': Path(path).stat().st_size, 'negative': -1,
              'text': 'offset', 'huge': 2**63}.get(damage, len(FIRST))
    write_index(path, {'a': 0, 'b': offset})
    before = Path(path).read_bytes()
    with pytest.raises(ValueError, match='invalid observation') as error:
        read(path, mode, strict=True)
    assert path in str(error.value) and 'byte' in str(error.value)
    if damage == 'mismatch':
        assert 'key mismatch' in str(error.value)
    assert Path(path).read_bytes() == before


@pytest.mark.parametrize('mode', ['range', 'point', 'equal'])
@pytest.mark.parametrize('index_state', ['existing', 'missing', 'rebuilt_warm'])
def test_omitted_damage_is_outside_indexed_guarantee(tmp_path, mode, index_state, monkeypatch):
    path = indexed_table(tmp_path, b'{broken\n')
    if index_state == 'missing':
        Path(path + '.idx').unlink()
    else:
        jf.build_jsonl_index(path)  # Permissively excludes the damaged observation.
    if index_state == 'rebuilt_warm':
        read(path, mode)
        assert path in jf._INDEX_CACHE
    before = Path(path).read_bytes()
    result = read(path, mode, strict=True)
    assert result == ({'a': {'v': 1}, 'z': {'v': 3}} if mode == 'range' else {})
    # A warm strict selection must not stream the table or rebuild its index.
    def forbidden(*args, **kwargs):
        pytest.fail('strict indexed read introduced a scan or index reload')
    monkeypatch.setattr(jf, 'load_jsonl', forbidden)
    monkeypatch.setattr(jf, 'build_jsonl_index', forbidden)
    monkeypatch.setattr(jf, '_read_index_snapshot', forbidden)
    parsed = []
    parse = jf._parse_row
    def counted(line):
        parsed.append(line)
        return parse(line)
    monkeypatch.setattr(jf, '_parse_row', counted)
    assert read(path, mode, strict=True) == result
    assert len(parsed) == (2 if mode == 'range' else 0)
    assert Path(path).read_bytes() == before


def test_unselected_damage_is_not_inspected_and_explicit_repair_allows_full_read(tmp_path):
    path = indexed_table(tmp_path, b'{broken\n')
    assert jf.select_jsonl(path, 'a', 'az', strict=True) == {'a': {'v': 1}}
    assert jf.select_line_jsonl(path, 'a', strict=True) == {'a': {'v': 1}}
    with pytest.raises(ValueError):
        jf.load_jsonl(path, strict=True)
    jf.lint_jsonl(path, force=True)
    assert jf.load_jsonl(path, strict=True) == {'a': {'v': 1}, 'z': {'v': 3}}


@pytest.mark.parametrize('slot', [b'', metaslot.encode_slot({'owner': 'x'}, 128),
    b'{"_meta":{"v":99}}\n', b'{"_meta":broken\n'])
def test_slots_padding_tombstones_and_empty_records_keep_existing_meaning(tmp_path, slot):
    path = tmp_path / 'normal.jsonl'
    path.write_bytes(slot + b'   \n{"z":{}}   \n\n{"a":{"v":1}}\n')
    jf.build_jsonl_index(str(path))
    before = path.read_bytes()
    assert list(jf.load_jsonl(str(path), False, strict=True)) == ['z', 'a']
    assert list(jf.select_jsonl(str(path), 'a', 'z', False, strict=True)) == ['a', 'z']
    assert jf.select_line_jsonl(str(path), 'missing', strict=True) == {}
    assert path.read_bytes() == before


@pytest.mark.parametrize('method', ['get_dict', 'get_df', 'get_dict_with_meta',
                                   'get_df_with_meta', 'load_jsonldf', 'select_jsonldf'])
@pytest.mark.parametrize('bounded', [False, True])
def test_wrappers_propagate_and_do_not_return_partial_results(tmp_path, method, bounded):
    db = FolderDB(str(tmp_path))
    path = indexed_table(tmp_path, b'{broken\n')
    bounds = ('a', 'z') if bounded else (None, None)
    if method == 'load_jsonldf':
        call = lambda **kw: jdf.load_jsonldf(path, None, False, **kw)
    elif method == 'select_jsonldf':
        call = lambda **kw: jdf.select_jsonldf(path, *bounds, False, None, **kw)
    else:
        name = 'table' if method.endswith('_with_meta') else ['table']
        call = lambda **kw: getattr(db, method)(name, *bounds, False, **kw)
    call()  # Positional arguments and permissive defaults still work.
    with pytest.raises(ValueError, match='invalid observation'):
        call(strict=True)


@pytest.mark.parametrize('method', ['get_dict', 'get_df'])
def test_multitable_failure_propagates(tmp_path, method):
    db = FolderDB(str(tmp_path))
    db.overwrite_dict('good', {'a': {}})
    indexed_table(tmp_path, b'{broken\n')
    with pytest.raises(ValueError, match='table.jsonl'):
        getattr(db, method)(['good', 'table'], strict=True)
    assert 'good' in getattr(db, method)(['good'], strict=True)


def test_missing_empty_and_datetime_results(tmp_path):
    db = FolderDB(str(tmp_path))
    assert db.get_dict(['missing'], strict=True) == {}
    assert db.get_df(['missing'], strict=True) == {}
    assert db.get_dict_with_meta('missing', strict=True) == (None, {})
    assert db.get_df_with_meta('missing', strict=True).rows.empty
    path = str(tmp_path / 'missing.jsonl')
    for mode in ('full', 'range', 'point'):
        expected = OSError if mode == 'range' else FileNotFoundError
        with pytest.raises(expected):
            read(path, mode, strict=True)
    jf.save_jsonl(path, {})
    assert jf.load_jsonl(path, strict=True) == {}
    assert jdf.load_jsonldf(path, strict=True).empty
    assert jf.select_jsonl(path, 'a', 'z', strict=True) == {}
    key = dt.datetime(2026, 9, 7)
    jf.save_jsonl(path, {key: {'v': 1}})
    assert jf.select_jsonl(path, key, key, strict=True) == {key: {'v': 1}}
    assert list(jdf.load_jsonldf(path, strict=True).index) == [pd.Timestamp(key)]


@pytest.mark.parametrize('function', [jf.load_jsonl, jf.select_jsonl, jf.select_line_jsonl,
    jdf.load_jsonldf, jdf.select_jsonldf, FolderDB.get_dict, FolderDB.get_df,
    FolderDB.get_dict_with_meta, FolderDB.get_df_with_meta])
def test_strict_is_optional_keyword_only(function):
    parameter = inspect.signature(function).parameters['strict']
    assert parameter.kind == inspect.Parameter.KEYWORD_ONLY and parameter.default is False
