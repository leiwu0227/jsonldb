"""Differential conversion and storage checks against index-oriented pandas."""
from datetime import datetime
import warnings

import numpy as np
import pandas as pd
import pytest

from jsonldb import FolderDB, jsonldf as jdf, jsonlfile as jf


class CustomFrame(pd.DataFrame):
    def to_dict(self, orient='dict', **kwargs):
        if orient != 'index':
            raise AssertionError('custom conversion must retain index orientation')
        return super().to_dict(orient=orient, **kwargs)


def frame(case):
    keys = ['a', 'z']
    cases = {
        'numeric': lambda: pd.DataFrame({'i': [1, 2], 'f': [1.5, float('nan')], 'b': [True, False]}, index=keys),
        'mixed': lambda: pd.DataFrame({'i': [1, 2], 'text': ['雪', 'é\n']}, index=keys),
        'nullable': lambda: pd.DataFrame({'i': pd.array([1, None], dtype='Int64'), 'b': pd.array([True, None], dtype='boolean'), 'f': pd.array([1.5, None], dtype='Float64')}, index=keys),
        'strings': lambda: pd.DataFrame({'v': pd.array(['雪', None], dtype='string')}, index=keys),
        'objects': lambda: pd.DataFrame({'v': np.array([np.int64(3), pd.NA], dtype=object)}, index=keys),
        'nested': lambda: pd.DataFrame({'v': [{'x': [1, None]}, ['雪', True]]}, index=keys),
        'categorical': lambda: pd.DataFrame({'v': pd.Categorical(['a', None])}, index=keys),
        'timestamps': lambda: pd.DataFrame({'v': pd.to_datetime(['2026-01-01', None])}, index=keys),
        'datetime_index': lambda: pd.DataFrame({'v': [1, 2]}, index=pd.date_range('2026-01-01', periods=2, tz='UTC')),
        'multi_index': lambda: pd.DataFrame({'v': [1, 2]}, index=pd.MultiIndex.from_tuples([('a', 1), ('b', 2)])),
        'range_index': lambda: pd.DataFrame({'v': [1, 2]}),
        'mixed_index': lambda: pd.DataFrame({'v': [1, 2]}, index=pd.Index([1, '1'], dtype=object)),
        'object_index': lambda: pd.DataFrame({'v': [1, 2]}, index=pd.Index(keys, dtype=object)),
        'missing_index': lambda: pd.DataFrame({'v': [1, 2]}, index=pd.Index([None, 'x'], dtype='string')),
        'duplicate_index': lambda: pd.DataFrame({'v': [1, 2]}, index=['a', 'a']),
        'duplicate_columns': lambda: pd.DataFrame([[1, 2], [3, 4]], columns=['v', 'v'], index=keys),
        'duplicate_both': lambda: pd.DataFrame([[1, 2], [3, 4]], columns=['v', 'v'], index=['a', 'a']),
        'empty_rows': lambda: pd.DataFrame({'v': pd.Series([], dtype='int64')}),
        'empty_columns': lambda: pd.DataFrame(index=keys),
        'integer_columns': lambda: pd.DataFrame({1: [1, 2]}, index=keys),
        'subclass': lambda: CustomFrame({'v': [1, 2]}, index=keys),
        'unsupported': lambda: pd.DataFrame({'v': [1, complex(1, 2)]}, index=keys),
    }
    return cases[case]()


CASES = ['numeric', 'mixed', 'nullable', 'strings', 'objects', 'nested', 'categorical',
         'timestamps', 'datetime_index', 'multi_index', 'range_index', 'mixed_index',
         'object_index', 'missing_index', 'duplicate_index', 'duplicate_columns',
         'duplicate_both', 'empty_rows', 'empty_columns', 'integer_columns', 'subclass', 'unsupported']


def observe(convert, df):
    result = {'keys': [], 'bytes': []}
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        try:
            rows = convert(df)
            result['keys'] = [(type(key).__name__, repr(key)) for key in rows]
            for key, value in rows.items():
                result['bytes'].append(jf._fast_dumps({jf.serialize_linekey(key): value}))
        except Exception as error:
            result['error'] = (type(error).__name__, str(error))
    result['warnings'] = [(type(w.message).__name__, str(w.message)) for w in caught]
    return result


@pytest.mark.parametrize('case', CASES)
@pytest.mark.parametrize('overwrite', [False, True])
def test_conversion_matches_existing_pandas_semantics(case, overwrite):
    df = frame(case)
    before = df.copy(deep=True)
    def original(value):
        if overwrite and not value.index.is_unique:
            raise ValueError('DataFrame index must be unique')
        return value.to_dict('index')
    candidate = jdf._df_records if overwrite else jdf._convert_df
    assert observe(candidate, df) == observe(original, df)
    pd.testing.assert_frame_equal(df, before)


@pytest.mark.parametrize('case', ['mixed', 'empty_columns', 'duplicate_columns', 'datetime_index', 'subclass', 'missing_index'])
def test_conversion_route_uses_guarded_fast_path(monkeypatch, case):
    df = frame(case)
    orientations = []
    original = pd.DataFrame.to_dict
    def record(self, orient='dict', **kwargs):
        orientations.append(orient)
        return original(self, orient=orient, **kwargs)
    monkeypatch.setattr(pd.DataFrame, 'to_dict', record)
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        jdf._convert_df(df)
    expected = 'records' if case == 'mixed' and pd.__version__.startswith('3.0.') else 'index'
    assert orientations == [expected]


@pytest.mark.parametrize('version', ['1.3.0', '2.0.3', '2.3.3', '3.1.0', '4.0.0'])
def test_unvalidated_version_branches_keep_original_conversion(monkeypatch, version):
    df = frame('mixed')
    monkeypatch.setattr(pd, '__version__', version)
    original = pd.DataFrame.to_dict
    orientations = []
    def record(self, orient='dict', **kwargs):
        orientations.append(orient)
        return original(self, orient=orient, **kwargs)
    monkeypatch.setattr(pd.DataFrame, 'to_dict', record)
    jdf._convert_df(df)
    assert orientations == ['index']


@pytest.mark.parametrize('operation', ['save_jsonldf', 'update_jsonldf', 'overwrite_df', 'upsert_df'])
@pytest.mark.parametrize('case', ['mixed', 'nullable', 'empty_columns', 'datetime_index', 'mixed_index'])
def test_storage_matches_original_conversion(tmp_path, monkeypatch, operation, case):
    db = FolderDB(str(tmp_path))
    db.set_meta_slot_bytes(256)
    df = frame(case)
    def prepare():
        db.overwrite_dict('table', {'old': {'v': 0}})
    def write():
        try:
            if operation.endswith('jsonldf'):
                return getattr(jdf, operation)(db._get_file_path('table'), df, meta={'v': 1})
            return getattr(db, operation)('table', df, meta={'v': 1})
        except Exception as error:
            return type(error).__name__, str(error)
    def snapshot():
        return {p.name: p.read_bytes() for p in tmp_path.iterdir() if p.is_file()}
    prepare()
    with monkeypatch.context() as patch:
        patch.setattr(jdf, '_convert_df', lambda value: value.to_dict('index'))
        expected_result = write()
        expected = snapshot()
    prepare()
    assert write() == expected_result
    assert snapshot() == expected


@pytest.mark.parametrize('operation', ['save_jsonldf', 'update_jsonldf', 'overwrite_df', 'upsert_df'])
def test_conversion_error_precedes_file_mutation(tmp_path, operation):
    class FailingFrame(pd.DataFrame):
        def to_dict(self, *args, **kwargs):
            raise RuntimeError('conversion failed')
    db = FolderDB(str(tmp_path))
    db.overwrite_dict('table', {'old': {'v': 1}})
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir() if p.is_file()}
    data = FailingFrame({'v': [1]}, index=['a'])
    with pytest.raises(RuntimeError, match='conversion failed'):
        if operation.endswith('jsonldf'):
            getattr(jdf, operation)(db._get_file_path('table'), data)
        else:
            getattr(db, operation)('table', data)
    assert {p.name: p.read_bytes() for p in tmp_path.iterdir() if p.is_file()} == before


@pytest.mark.parametrize('operation', ['overwrite_dfs', 'upsert_dfs'])
def test_plural_conversion_failure_retains_prior_completion(tmp_path, operation):
    db = FolderDB(str(tmp_path))
    data = {'first': frame('mixed'), 'second': frame('duplicate_index'), 'third': frame('numeric')}
    with pytest.raises(ValueError, match='unique'):
        getattr(db, operation)(data)
    assert db.get_dbmeta()['first']['count'] == 2
    assert not (tmp_path / 'second.jsonl').exists()
    assert not (tmp_path / 'third.jsonl').exists()
