"""Creation policy and maintenance of historical table identifiers."""
from pathlib import Path

import pandas as pd
import pytest

from jsonldb import FolderDB, jsonlfile


WRITERS = ('overwrite_dict', 'upsert_dict', 'overwrite_df', 'upsert_df')


def _write(db, writer, name, rows=None):
    rows = {'row': {'value': 1}} if rows is None else rows
    content = pd.DataFrame.from_dict(rows, orient='index') if writer.endswith('_df') else rows
    getattr(db, writer)(name, content)


def _snapshot(root):
    return {str(path.relative_to(root)): path.read_bytes() if path.is_file() else None
            for path in root.rglob('*')}


@pytest.mark.parametrize('depth', [None, 1])
@pytest.mark.parametrize('writer', WRITERS)
@pytest.mark.parametrize('name', [
    '', '.jsonl', '.', '..', '.hidden', 'trailing.', 'a..b',
    'with space', 'a/b', 'a\\b', '../outside', '/absolute', 'bad\nname',
    '2024-01-01T00:00:00', 'a:b', 'a*', 'a?', 'a|b', 'café',
    '_meta', '_META.jsonl', 'CON', 'nul.jsonl', 'PrN.log', 'AUX', 'COM1', 'lpt9',
])
def test_new_invalid_names_fail_before_any_mutation(tmp_path, depth, writer, name):
    db = FolderDB(str(tmp_path), hierarchy_depth=depth)
    before = _snapshot(tmp_path)
    with pytest.raises(ValueError, match='Invalid new table name'):
        _write(db, writer, name)
    assert _snapshot(tmp_path) == before


@pytest.mark.parametrize('depth', [None, 2])
@pytest.mark.parametrize('writer', WRITERS)
@pytest.mark.parametrize('name', ['prices.2024-01-01', '_group.table-1.jsonl', 'COM10.records'])
def test_valid_new_names_round_trip(tmp_path, depth, writer, name):
    db = FolderDB(str(tmp_path), hierarchy_depth=depth)
    _write(db, writer, name)
    assert db.get_dict([name])[name] == {'row': {'value': 1}}
    db.lint_db()
    reopened = FolderDB(str(tmp_path))
    assert reopened.get_dict([name])[name] == {'row': {'value': 1}}


@pytest.mark.parametrize('name', ['safe-CON-leaf', 'safe--leaf', '-safe-leaf', 'safe-leaf-'])
def test_custom_hierarchy_rejects_unsafe_directory_segments(tmp_path, name):
    db = FolderDB(str(tmp_path), hierarchy_depth=2)
    db.delimiter = '-'
    db.build_hmeta()
    before = _snapshot(tmp_path)
    with pytest.raises(ValueError, match='Invalid new table name'):
        db.overwrite_dict(name, {'row': {}})
    assert _snapshot(tmp_path) == before


@pytest.mark.parametrize('depth', [None, 1])
@pytest.mark.parametrize('name', ['2024-01-01T00:00:00', 'with space', 'CON', 'a..b'])
def test_historical_names_survive_writes_reopen_lint_and_delete(tmp_path, depth, name):
    db = FolderDB(str(tmp_path), hierarchy_depth=depth)
    # Seed a historical file through the path-based storage API.
    path = Path(db._get_file_path(name))
    path.parent.mkdir(parents=True, exist_ok=True)
    jsonlfile.save_jsonl(str(path), {'row': {'value': 0}})
    db.build_dbmeta()
    db = FolderDB(str(tmp_path))
    assert list(db.get_dbmeta()) == [name]
    assert name + ':' in str(db)
    for writer in WRITERS:
        _write(db, writer, name + '.jsonl')
        assert db.get_dict([name])[name] == {'row': {'value': 1}}
    db.lint_db(force=True)
    db.lint_db()
    assert db.get_dbmeta()[name]['linted'] is True
    db = FolderDB(str(tmp_path))
    assert list(db.get_dbmeta()) == [name]
    assert db.get_df([name])[name].loc['row', 'value'] == 1
    db.delete_file(name)
    assert db.get_dbmeta() == {}
    with pytest.raises(ValueError, match='Invalid new table name'):
        db.upsert_dict(name, {'row': {}})


def test_timestamp_table_names_do_not_change_row_datetime_semantics(tmp_path):
    name = '2024-01-01T00:00:00'
    path = tmp_path / (name + '.jsonl')
    jsonlfile.save_jsonl(str(path), {name: {'value': 1}})
    db = FolderDB(str(tmp_path))
    assert list(db.get_dbmeta()) == [name]
    assert list(db.get_dict([name])[name]) == [pd.Timestamp(name).to_pydatetime()]
    assert list(db.get_dict([name], auto_deserialize=False)[name]) == [name]
    db.lint_db()
    assert list(db.get_dbmeta()) == [name]


@pytest.mark.parametrize('writer', ['overwrite_dicts', 'upsert_dicts', 'overwrite_dfs', 'upsert_dfs'])
def test_plural_writes_validate_each_creation_and_keep_completed_tables(tmp_path, writer):
    db = FolderDB(str(tmp_path))
    rows = {'row': {'value': 1}}
    content = pd.DataFrame.from_dict(rows, orient='index') if writer.endswith('_dfs') else rows
    with pytest.raises(ValueError, match='Invalid new table name'):
        getattr(db, writer)({'valid': content, 'bad:name': content, 'later': content})
    assert db.get_file_list() == ['valid']
    assert db.get_dict(['valid']) == {'valid': rows}
    assert list(db.get_dbmeta()) == ['valid']
