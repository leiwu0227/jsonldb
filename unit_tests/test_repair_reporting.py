"""Every repaired configuration and missing table index leaves a finding."""
from pathlib import Path

import orjson
import pytest

from jsonldb import FolderDB, jsonlfile


@pytest.mark.parametrize('relative', [False, True])
@pytest.mark.parametrize('content, expected, reason', [
    (b'', {'timespec': 'seconds'}, 'missing timespec'),
    (b'\n', {'timespec': 'seconds'}, 'missing timespec'),
    (b'{torn', {'timespec': 'seconds'}, 'missing timespec'),
    (b'{"config":{}}\n', {'timespec': 'seconds'}, 'missing timespec'),
    (b'{"config":{"timespec":null,"consumer_setting":{"keep":true}}}\n',
     {'timespec': 'seconds', 'consumer_setting': {'keep': True}}, 'missing timespec'),
    (b'{"config":{"meta_slot_bytes":256,"consumer_setting":{"keep":true}}}\n',
     {'timespec': 'seconds', 'meta_slot_bytes': 256, 'consumer_setting': {'keep': True}},
     'missing timespec'),
    (b'{"timespec":"microseconds"}\n', {'timespec': 'microseconds'}, 'noncanonical format'),
])
def test_open_reports_configuration_regeneration_and_preserves_recovered_settings(
        tmp_path, monkeypatch, relative, content, expected, reason):
    db = FolderDB(str(tmp_path))
    db.overwrite_dict('table', {'a': {'value': 1}})
    table = tmp_path / 'table.jsonl'
    before = table.read_bytes()
    (tmp_path / 'config.meta').write_bytes(content)
    if relative:
        monkeypatch.chdir(tmp_path)
    argument = '.' if relative else str(tmp_path)
    reopened = FolderDB(argument)
    assert reopened.timespec == expected['timespec']
    assert orjson.loads((tmp_path / 'config.meta').read_bytes()) == {'config': expected}
    assert jsonlfile.load_index(str(tmp_path / 'config.meta')) == {'config': 0}
    assert table.read_bytes() == before
    report = (tmp_path / '.jsonldb/integrity.log').read_text()
    findings = [line for line in report.splitlines() if 'kind=control_regenerated' in line]
    assert len(findings) == 1
    assert 'config.meta' in findings[0]
    assert reason in findings[0]
    FolderDB(argument)
    assert len((tmp_path / '.jsonldb/integrity.log').read_text().splitlines()) == 1


@pytest.mark.parametrize('operation', ['open', 'lint', 'rebuild'])
@pytest.mark.parametrize('depth', [None, 2])
@pytest.mark.parametrize('slotted', [False, True])
@pytest.mark.parametrize('rows', [{}, {'a': {'value': 1}, 'z': {'value': 2}}])
def test_metadata_rebuild_reports_missing_index_once_without_rewriting_table(
        tmp_path, caplog, operation, depth, slotted, rows):
    db = FolderDB(str(tmp_path), hierarchy_depth=depth)
    if slotted:
        db.set_meta_slot_bytes(256)
    db.overwrite_dict('region.table', rows, meta={'keep': True} if slotted else None)
    table = Path(db._get_file_path('region.table'))
    before = table.read_bytes()
    Path(str(table) + '.idx').unlink()
    (tmp_path / 'db.meta').unlink()
    prior_report = (tmp_path / '.jsonldb/integrity.log').read_bytes()
    caplog.set_level('WARNING', logger='jsonldb')
    caplog.clear()
    if operation == 'open':
        db = FolderDB(str(tmp_path))
    elif operation == 'lint':
        db.lint_db()
    else:
        db.build_dbmeta()
    missing = [message for message in caplog.messages
               if 'rebuilt missing index ' + str(table) + '.idx' in message]
    assert len(missing) == 1
    assert list(jsonlfile.load_index(str(table))) == list(rows)
    entry = db.get_dbmeta()['region.table']
    assert (entry['count'], entry['min_index'], entry['max_index']) == (
        len(rows), min(rows, default=None), max(rows, default=None))
    assert table.read_bytes() == before
    if operation == 'rebuild':
        assert (tmp_path / '.jsonldb/integrity.log').read_bytes() == prior_report
    else:
        filename = 'integrity.log' if operation == 'open' else 'lint.log'
        findings = (tmp_path / '.jsonldb' / filename).read_text().splitlines()
        missing = [line for line in findings if 'kind=index_rebuilt' in line
                   and str(table) + '.idx' in line and 'detail=missing' in line]
        assert len(missing) == 1
