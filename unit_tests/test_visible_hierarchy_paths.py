"""Visible path names must not be mistaken for the hidden quarantine directory."""
import pytest

from jsonldb import FolderDB, jsonlfile


@pytest.mark.parametrize('placement', ['root', 'ancestor', 'child'])
@pytest.mark.parametrize('operation', ['discovery', 'reorganization'])
def test_visible_invalid_tickers_substring_and_hidden_exclusion(tmp_path, placement, operation):
    visible = 'backup.invalid_tickers_2026'
    root = (tmp_path/visible if placement == 'root' else
            tmp_path/visible/'database' if placement == 'ancestor' else tmp_path/'database')
    root.mkdir(parents=True)
    db = FolderDB(str(root), hierarchy_depth=1)
    incoming = root/(visible if placement == 'child' else 'incoming')
    incoming.mkdir()
    source = incoming/'region.table.jsonl'
    rows = {'a': {'value': 1}}
    jsonlfile.save_jsonl(str(source), rows)
    table_bytes = source.read_bytes()
    hidden = {}
    for relative in ('.invalid_tickers', '.external/nested', 'incoming/.hidden'):
        directory = root/relative
        directory.mkdir(parents=True, exist_ok=True)
        path = directory/'hidden.table.jsonl'
        jsonlfile.save_jsonl(str(path), {'hidden': {'value': 2}})
        hidden[path] = path.read_bytes()
        index = path.with_name(path.name+'.idx')
        hidden[index] = index.read_bytes()
    if operation == 'discovery':
        assert db.get_file_list() == ['region.table']
    else:
        db.lint_hierarchy(2)
        target = root/'region'/'region.table.jsonl'
        assert target.read_bytes() == table_bytes
        assert not source.exists()
        assert db.get_file_list() == ['region.table']
        assert db.get_dict() == {'region.table': rows}
        assert jsonlfile.select_line_jsonl(str(target), 'a') == rows
    assert all(path.read_bytes() == content for path, content in hidden.items())
