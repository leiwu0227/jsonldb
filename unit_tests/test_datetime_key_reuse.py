"""Baseline byte/error comparisons and observable custom-key call ordering."""
import datetime as dt
import warnings

import pandas as pd
import pytest

from jsonldb import FolderDB, jsonldf as jdf, jsonlfile as jf


def original_validation(rows, timespec=None, *, reuse=False):
    for key, record in rows.items():
        if not isinstance(record, dict):
            raise TypeError('JSONL record values must be dictionaries')
        if jf.serialize_linekey(key, timespec) == '_meta':
            raise ValueError("'_meta' is reserved for table metadata")
    if reuse:
        return lambda: rows.items()


class ChangingKey:
    def __str__(self):
        self.calls = getattr(self, 'calls', 0) + 1
        return f'custom-{self.calls}'


class ChangingDatetime(dt.datetime):
    def isoformat(self, **kwargs):
        self.calls = getattr(self, 'calls', 0) + 1
        return f'date-{self.calls}'


class ChangingMapping(dict):
    def items(self):
        self.calls = getattr(self, 'calls', 0) + 1
        return {f'pass-{self.calls}': {'v': self.calls}}.items()


class CustomTimezone(dt.tzinfo):
    def utcoffset(self, value):
        self.calls = getattr(self, 'calls', 0) + 1
        return dt.timedelta(hours=1)
    def dst(self, value):
        return dt.timedelta(0)


def rows(case):
    t = dt.datetime(2026, 1, 1)
    cases = {
        'naive': lambda: {t: {'v': 1}, t+dt.timedelta(seconds=1): {'v': 2}},
        'collision': lambda: {t.replace(microsecond=1): {'v': 1}, t.replace(microsecond=2): {'v': 2}},
        'nanoseconds': lambda: {pd.Timestamp(t)+pd.Timedelta(1, 'ns'): {'v': 1}, pd.Timestamp(t)+pd.Timedelta(2, 'ns'): {'v': 2}},
        'fixed': lambda: {t.replace(tzinfo=dt.timezone(dt.timedelta(hours=8))): {'v': 1}},
        'named': lambda: {k: {'v': i} for i, k in enumerate(pd.date_range('2026-03-08 01:59', periods=3, freq='min', tz='America/New_York'))},
        'mixed_collision': lambda: {t: {'v': 1}, '2026-01-01T00:00:00': {'v': 2}},
        'strings': lambda: {'雪': {'v': 1}, 'a': {'v': 2}},
        'empty': dict,
        'invalid': lambda: {t: {'v': 1}, 'bad': 1},
        'reserved': lambda: {t: {'v': 1}, '_meta': {}},
        'bad_value': lambda: {t: {'v': 1}, t+dt.timedelta(seconds=1): {'v': object()}},
        'nat': lambda: {pd.NaT: {'v': 1}},
        'custom': lambda: {ChangingKey(): {'v': 1}},
        'mixed_custom': lambda: {t: {'v': 1}, ChangingKey(): {'v': 2}},
        'subclass': lambda: {ChangingDatetime(2026, 1, 1): {'v': 1}},
        'mapping': lambda: ChangingMapping(a={'v': 1}),
        'custom_timezone': lambda: {t.replace(tzinfo=CustomTimezone()): {'v': 1}},
    }
    return cases[case]()


@pytest.mark.parametrize('writer', ['save_jsonl', 'save_jsonl_atomic', 'update_jsonl'])
@pytest.mark.parametrize('timespec', ['seconds', 'microseconds', 'invalid'])
@pytest.mark.parametrize('case', ['naive', 'collision', 'nanoseconds', 'fixed', 'named',
    'mixed_collision', 'strings', 'empty', 'invalid', 'reserved', 'bad_value', 'nat',
    'custom', 'mixed_custom', 'subclass', 'mapping', 'custom_timezone'])
def test_writers_match_original_passes(tmp_path, monkeypatch, writer, timespec, case):
    path = str(tmp_path/'table.jsonl')
    def observe():
        jf.save_jsonl(path, {'2026-01-01T00:00:00': {'v': 'initial long text'}, 'old': {'v': 0}})
        data = rows(case)
        key_order = list(data)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            try:
                result = getattr(jf, writer)(path, data, timespec)
            except Exception as error:
                result = type(error).__name__, str(error)
        assert list(data) == key_order
        return (result, [(type(w.message).__name__, str(w.message)) for w in caught],
                {p.name: p.read_bytes() for p in tmp_path.iterdir()},
                [getattr(k, 'calls', None) for k in data], getattr(data, 'calls', None),
                [getattr(getattr(k, 'tzinfo', None), 'calls', None) for k in data])
    with monkeypatch.context() as patch:
        patch.setattr(jf, '_validate_row_keys', original_validation)
        expected = observe()
    assert observe() == expected


@pytest.mark.parametrize('operation', ['save_jsonldf', 'update_jsonldf', 'overwrite_df', 'upsert_df'])
@pytest.mark.parametrize('spec', ['seconds', 'microseconds'])
@pytest.mark.parametrize('shape', ['short', 'long', 'empty', 'reserved'])
def test_dataframe_slot_metadata_and_cache_match(tmp_path, monkeypatch, operation, spec, shape):
    db = FolderDB(str(tmp_path))
    db.timespec = spec
    db.set_meta_slot_bytes(256)
    keys = pd.date_range('2026-01-01', periods=2, freq='us')
    frame = pd.DataFrame({'v': ['x', 'y'] if shape == 'short' else ['long '*40]*2}, index=keys)
    if shape == 'empty':
        frame = pd.DataFrame(index=keys)
    elif shape == 'reserved':
        frame = pd.DataFrame({'v': [1, 2]}, index=[keys[0], '_meta'])
    before = frame.copy(deep=True)
    def observe():
        db.overwrite_dict('table', {'2026-01-01T00:00:00': {'v': 'initial long text'}, 'old': {'v': 0}}, meta={'old': 1})
        path = db._get_file_path('table')
        jf.select_line_jsonl(path, 'old')
        try:
            result = (getattr(jdf, operation)(path, frame, timespec=spec, meta={'new': 1})
                      if operation.endswith('jsonldf') else getattr(db, operation)('table', frame, meta={'new': 1}))
        except Exception as error:
            result = type(error).__name__, str(error)
        return result, {p.name: p.read_bytes() for p in tmp_path.iterdir() if p.is_file()}, jf.select_line_jsonl(path, 'old')
    with monkeypatch.context() as patch:
        patch.setattr(jf, '_validate_row_keys', original_validation)
        expected = observe()
    assert observe() == expected
    pd.testing.assert_frame_equal(frame, before)


@pytest.mark.parametrize('operation', ['overwrite_dicts', 'upsert_dicts', 'overwrite_dfs', 'upsert_dfs'])
def test_plural_validation_failure_keeps_prior_completion(tmp_path, operation):
    db = FolderDB(str(tmp_path))
    batch = {'first': rows('naive'), 'second': rows('reserved'), 'third': rows('naive')}
    if operation.endswith('dfs'):
        batch = {name: pd.DataFrame(list(data.values()), index=list(data)) for name, data in batch.items()}
    with pytest.raises(ValueError, match='reserved'):
        getattr(db, operation)(batch)
    assert db.get_dbmeta()['first']['count'] == 2
    assert not (tmp_path/'second.jsonl').exists()
    assert not (tmp_path/'third.jsonl').exists()


@pytest.mark.parametrize('case, calls', [('naive', 2), ('nanoseconds', 2), ('fixed', 1), ('mixed_custom', 2), ('named', 6)])
def test_stable_datetime_formatting_occurs_once(tmp_path, monkeypatch, case, calls):
    original = jf.serialize_linekey
    datetimes = []
    def observed(key, timespec=None):
        if isinstance(key, dt.datetime):
            datetimes.append(key)
        return original(key, timespec)
    monkeypatch.setattr(jf, 'serialize_linekey', observed)
    jf.save_jsonl(str(tmp_path/'table.jsonl'), rows(case))
    assert len(datetimes) == calls


def test_custom_mapping_items_lookup_stays_inside_open_file(tmp_path):
    path = tmp_path/'table.jsonl'
    class LookupMapping(dict):
        lookups = 0
        def __getattribute__(self, name):
            if name == 'items':
                self.lookups += 1
                if self.lookups == 2:
                    assert path.exists() and path.stat().st_size == 0
            return super().__getattribute__(name)
    jf.save_jsonl(str(path), LookupMapping(a={'v': 1}))
