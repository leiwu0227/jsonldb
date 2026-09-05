"""Existing pre-metadata call patterns remain opt-in and migration-free."""
import datetime as dt
from pathlib import Path

import pandas as pd
import pytest

from jsonldb import FolderDB, jsonlfile as jf


@pytest.mark.parametrize('hierarchy', [0, 1])
@pytest.mark.parametrize('spec', ['seconds', 'microseconds'])
def test_historical_unslotted_files_and_existing_folder_calls(tmp_path, hierarchy, spec):
    jf.save_jsonl(str(tmp_path/'config.meta'), {'config': {'timespec': spec}})
    name = 'region.table' if hierarchy else 'table'
    path = tmp_path/'region'/'region.table.jsonl' if hierarchy else tmp_path/'table.jsonl'
    path.parent.mkdir(exist_ok=True)
    text = '2020-01-01T00:00:00' + ('.123456' if spec == 'microseconds' else '')
    path.write_bytes(('{' + '"'+text+'":{"v":1}}\n{"plain":{"v":2}}\n').encode())
    original = path.read_bytes()
    db = FolderDB(str(tmp_path), hierarchy_depth=hierarchy or None)
    assert db.meta_slot_bytes is None
    assert db.read_timezone(name) is None
    assert path.read_bytes() == original
    assert db.get_dict([name], auto_deserialize=False)[name] == {text: {'v': 1}, 'plain': {'v': 2}}
    key = dt.datetime.fromisoformat(text)
    assert db.get_dict([name], key, key)[name] == {key: {'v': 1}}
    assert list(db.get_df([name], key, key)[name].index) == [pd.Timestamp(key)]
    with pytest.raises(ValueError):
        db.set_timezone(name, '+8:00')
    assert path.read_bytes() == original
    db.upsert_dict(name, {'new': {'v': 3}})
    db.upsert_df(name, pd.DataFrame({'v': [4]}, index=['frame']))
    db.delete_file_keys(name, ['plain'])
    db.delete_file_range(name, key, key)
    db.lint_db(force=True)
    assert db.get_dict([name], auto_deserialize=False)[name] == {'frame': {'v': 4}, 'new': {'v': 3}}
    reopened = FolderDB(str(tmp_path))
    assert reopened.meta_slot_bytes is None and reopened.read_timezone(name) is None
    reopened.overwrite_df(name, pd.DataFrame({'v': [5]}, index=['replacement']))
    assert b'"_meta"' not in path.read_bytes()
    assert b'"timezone"' not in (tmp_path/'config.meta').read_bytes()


@pytest.mark.parametrize('operation', ['overwrite_dicts', 'upsert_dicts', 'overwrite_dfs', 'upsert_dfs'])
def test_historical_plural_calls_do_not_opt_in(tmp_path, operation):
    db = FolderDB(str(tmp_path))
    key = dt.datetime(2020, 1, 1, tzinfo=dt.timezone(dt.timedelta(hours=8)))
    records = {key: {'v': 1}}
    data = pd.DataFrame({'v': [1]}, index=[key]) if operation.endswith('dfs') else records
    getattr(db, operation)({'a': data, 'b': data})
    for name in ('a', 'b'):
        assert db.get_dict([name])[name] == {key.isoformat(): {'v': 1}}
        assert db.read_timezone(name) is None
        assert Path(tmp_path/(name+'.jsonl')).read_bytes().startswith(b'{"2020-')
