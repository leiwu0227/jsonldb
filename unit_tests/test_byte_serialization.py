"""Compatibility with the former serializer's bytes and failure boundaries."""
from datetime import datetime
import os

import numpy as np
import orjson
import pytest

from jsonldb import jsonlfile as jf


def former_bytes(row):
    return (orjson.dumps(row, option=orjson.OPT_SERIALIZE_NUMPY).decode('utf-8') + '\n').encode('utf-8')


WRITERS = [jf.save_jsonl, jf.save_jsonl_atomic, jf.update_jsonl]
ROWS = [
    {},
    {'雪\n😀': {'text': 'é\n\r\t\\"', 'nested': {'空': [None, True, False]}}},
    {'numpy': {'integer': np.int64(7), 'float': np.float32(1.25),
               'array': np.array([1, 2, 3]), 'date': np.datetime64('2026-09-05')}},
    {datetime(2026, 9, 5, 1, 2, 3, 123456): {'v': 1},
     '2026-09-05T01:02:03.123456': {'v': 2}, 'a': {'v': -0.0}},
]


@pytest.mark.parametrize('writer', WRITERS)
@pytest.mark.parametrize('rows', ROWS)
def test_published_bytes_and_offsets_match_former_serializer(tmp_path, writer, rows):
    path = tmp_path / 'rows.jsonl'
    if writer is jf.update_jsonl:
        jf.save_jsonl(str(path), {})
    data, index = b'', {}
    for key, value in rows.items():
        key = jf.serialize_linekey(key, 'microseconds')
        index[key] = len(data)
        data += former_bytes({key: value})
    assert writer(str(path), rows, timespec='microseconds') is None
    assert path.read_bytes() == data
    assert (tmp_path / 'rows.jsonl.idx').read_bytes() == orjson.dumps(index, option=orjson.OPT_SORT_KEYS)
    for row in rows.values():
        assert isinstance(jf._fast_dumps({'key': row}), bytes)
        assert jf._fast_dumps({'key': row}) == former_bytes({'key': row})


def test_unicode_padding_growth_and_slot_keep_byte_boundaries(tmp_path):
    path = tmp_path / 'rows.jsonl'
    name = str(path)
    jf.save_jsonl(name, {'a': {'text': '雪' * 20}, 'z': {'v': 1}},
                  slot_bytes=256, meta={'owner': '雪'})
    before_index = jf.load_index(name)
    before = path.read_bytes()
    jf.update_jsonl(name, {'a': {'text': '😀'}})
    expected = former_bytes({'a': {'text': '😀'}})
    old_length = before_index['z'] - before_index['a']
    assert path.read_bytes()[256:before_index['z']] == expected[:-1] + b' ' * (old_length - len(expected)) + b'\n'
    assert jf.load_index(name) == before_index
    assert jf.read_jsonl_meta(name) == {'owner': '雪'}
    grown = {'a': {'text': '😀' * 100}}
    jf.update_jsonl(name, grown)
    assert jf.load_index(name)['a'] == len(before)
    assert path.read_bytes()[len(before):] == former_bytes(grown)
    assert path.read_bytes()[:256] == before[:256]


@pytest.mark.parametrize('writer', WRITERS)
@pytest.mark.parametrize('bad', [object(), '\ud800', np.arange(10)[::2]])
def test_payload_errors_preserve_exception_and_mutation_timing(tmp_path, writer, bad):
    path = tmp_path / 'rows.jsonl'
    name = str(path)
    jf.save_jsonl(name, {'old': {'v': 1}})
    jf.select_line_jsonl(name, 'old')
    before, old_index = path.read_bytes(), (tmp_path / 'rows.jsonl.idx').read_bytes()
    rows = {'good': {'v': 2}, 'bad': {'v': bad}}
    with pytest.raises(TypeError) as expected:
        former_bytes({'bad': rows['bad']})
    with pytest.raises(TypeError) as actual:
        writer(name, rows)
    assert str(actual.value) == str(expected.value)
    assert path.read_bytes() == (former_bytes({'good': {'v': 2}}) if writer is jf.save_jsonl else before)
    assert (tmp_path / 'rows.jsonl.idx').read_bytes() == old_index
    assert os.path.abspath(name) not in jf._INDEX_CACHE
    assert not list(tmp_path.glob('.rows.jsonl.*.tmp'))


@pytest.mark.parametrize('writer', WRITERS)
def test_reserved_key_rejected_before_mutation(tmp_path, writer):
    path = tmp_path / 'rows.jsonl'
    jf.save_jsonl(str(path), {'old': {'v': 1}})
    before = path.read_bytes()
    with pytest.raises(ValueError):
        writer(str(path), {'good': {'v': 2}, '_meta': {'v': 3}})
    assert path.read_bytes() == before
