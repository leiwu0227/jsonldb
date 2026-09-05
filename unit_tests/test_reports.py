import logging
import os

import orjson
import pytest

from jsonldb import FolderDB, jsonlfile
from jsonldb import reports


def _lines(path):
    return path.read_text(encoding="utf-8").splitlines()


@pytest.mark.parametrize('operation', ['open', 'lint'])
@pytest.mark.parametrize('index_bytes, reason', [
    (b'{}', 'stale'), (b'', 'empty'), (b'not-json', 'corrupt'), (b'[]', 'corrupt'),
])
def test_index_rebuild_reports_distinguish_staleness_from_corruption(
        tmp_path, caplog, operation, index_bytes, reason):
    db = FolderDB(str(tmp_path))
    rows = {'a': {'value': 1}, 'b': {'value': 2}}
    db.overwrite_dict('table', rows)
    path = tmp_path / 'table.jsonl'
    before = path.read_bytes()
    index_path = tmp_path / 'table.jsonl.idx'
    index_path.write_bytes(index_bytes)
    timestamp = 0 if reason == 'stale' else path.stat().st_mtime_ns + 1_000_000_000
    os.utime(index_path, ns=(timestamp, timestamp))
    caplog.set_level(logging.WARNING, logger='jsonldb')
    caplog.clear()

    if operation == 'open':
        os.utime(tmp_path / 'db.meta', ns=(0, 0))
        FolderDB(str(tmp_path))
    else:
        db.lint_db()

    report_path = tmp_path / '.jsonldb' / ('integrity.log' if operation == 'open' else 'lint.log')
    assert list(orjson.loads(index_path.read_bytes())) == ['a', 'b']
    assert index_path.stat().st_mtime_ns >= path.stat().st_mtime_ns
    assert path.read_bytes() == before
    assert not any('rebuilt stale index' in message for message in caplog.messages)
    if reason == 'stale':
        assert caplog.messages == []
        assert len(_lines(report_path)) == 1
    else:
        assert 'kind=index_rebuilt' in report_path.read_text()
        assert 'detail=' + reason in report_path.read_text()


@pytest.mark.parametrize('operation', ['open', 'lint'])
def test_stale_index_rebuild_still_reports_damaged_rows(tmp_path, caplog, operation):
    db = FolderDB(str(tmp_path))
    db.overwrite_dict('table', {'a': {'value': 1}})
    path = tmp_path / 'table.jsonl'
    clean = path.read_bytes()
    path.write_bytes(clean + b'broken-row\n')
    os.utime(tmp_path / 'table.jsonl.idx', ns=(0, 0))
    caplog.set_level(logging.WARNING, logger='jsonldb')
    caplog.clear()

    if operation == 'open':
        os.utime(tmp_path / 'db.meta', ns=(0, 0))
        FolderDB(str(tmp_path))
        report = tmp_path / '.jsonldb' / 'integrity.log'
        assert 'kind=invalid_json' in report.read_text()
        assert path.read_bytes() == clean + b'broken-row\n'
    else:
        db.lint_db()
        report = tmp_path / '.jsonldb' / 'lint.log'
        assert 'kind=lint_removed' in report.read_text()
        assert path.read_bytes() == clean
    assert not any('rebuilt stale index' in message for message in caplog.messages)
    assert 'detail=stale' not in report.read_text()


def test_clean_reports_replace_and_ordinary_operations_are_silent(tmp_path):
    db = FolderDB(str(tmp_path))
    integrity = tmp_path / ".jsonldb" / "integrity.log"
    integrity.write_text("stale open report\n", encoding="utf-8")

    db = FolderDB(str(tmp_path))
    assert len(_lines(integrity)) == 1
    assert "operation=open" in _lines(integrity)[0]
    clean_open = integrity.read_bytes()

    db.overwrite_dict("table", {"a": {"value": 1}})
    db.get_dict("table")
    db.upsert_dict("table", {"b": {"value": 2}})
    assert integrity.read_bytes() == clean_open
    assert not (tmp_path / ".jsonldb" / "lint.log").exists()

    db.lint_db()
    lint = tmp_path / ".jsonldb" / "lint.log"
    assert len(_lines(lint)) == 1
    assert "operation=lint_db" in _lines(lint)[0]
    lint.write_text("stale lint report\n", encoding="utf-8")
    db.lint_db()
    assert len(_lines(lint)) == 1
    assert "stale" not in lint.read_text(encoding="utf-8")


def test_open_reports_torn_row_without_repair_and_capture_is_scoped(
        tmp_path, monkeypatch):
    db = FolderDB(str(tmp_path))
    db.overwrite_dict("table", {"a": {"value": 1}})
    table = tmp_path / "table.jsonl"
    torn_offset = table.stat().st_size
    with table.open("ab") as stream:
        stream.write(b'{"secret":{"payload":"failed at byte 7: leaked"}')
    os.remove(tmp_path / "table.jsonl.idx")
    os.remove(tmp_path / "db.meta")
    before = table.read_bytes()
    other = tmp_path.parent / "other-db" / "foreign.jsonl.idx"
    original = FolderDB._detect_data_timespec

    def log_other_folder(self):
        logging.getLogger("jsonldb.jsonlfile").warning(
            "rebuilt corrupt index %s", other)
        return original(self)

    monkeypatch.setattr(FolderDB, "_detect_data_timespec", log_other_folder)
    FolderDB(str(tmp_path))

    report = (tmp_path / ".jsonldb" / "integrity.log").read_text(
        encoding="utf-8")
    assert "kind=invalid_json" in report
    assert ("file=%s | offset=%d | detail=skipped" %
            (table, torn_offset)) in report
    assert "secret" not in report and "payload" not in report
    assert "failed" not in report and "leaked" not in report
    assert str(other) not in report
    assert table.read_bytes() == before

    prior = report
    logging.getLogger("jsonldb.jsonlfile").warning(
        "invalid JSON line in %s at byte 0: after-open", table)
    assert (tmp_path / ".jsonldb" / "integrity.log").read_text(
        encoding="utf-8") == prior


def test_lint_report_caps_removed_content_and_total_findings(tmp_path):
    db = FolderDB(str(tmp_path))
    rows = {
        "%03d" % number: {"payload": "x" * 300}
        for number in range(203)
    }
    db.overwrite_dict("damage", rows)
    deleted = ["%03d" % number for number in range(1, 203, 2)]
    jsonlfile.delete_jsonl(str(tmp_path / "damage.jsonl"), deleted)

    db.lint_db()

    lines = _lines(tmp_path / ".jsonldb" / "lint.log")
    findings = [line for line in lines if line.startswith("kind=")]
    assert len(findings) == reports.MAX_FINDINGS
    assert lines[-1] == "omitted=2"
    assert all(str(tmp_path / "damage.jsonl") in line for line in findings)
    for line in findings:
        removed = line.split(" | removed=", 1)[1]
        assert len(removed) <= reports.MAX_REMOVED_CHARS
    assert jsonlfile.load_jsonl(str(tmp_path / "damage.jsonl")) == {
        key: value for key, value in rows.items() if key not in deleted
    }


def test_report_directory_is_hidden_from_data_operations(tmp_path):
    db = FolderDB(str(tmp_path))
    db.overwrite_dict("A.B.table", {"a": {"value": 1}})
    report_dir = tmp_path / ".jsonldb"
    sentinel = report_dir / "hidden.jsonl"
    sentinel.write_text('{"hidden":{"value":1}}\n', encoding="utf-8")

    assert "hidden" not in db.get_file_list()
    db.lint_hierarchy(2)
    assert sentinel.exists()
    assert "hidden" not in db.get_file_list()
    db.clear_folder(force=True)
    assert sentinel.exists()


def test_converted_folder_diagnostic_uses_logging(tmp_path, caplog, capsys):
    db = FolderDB(str(tmp_path))
    caplog.set_level(logging.WARNING, logger="jsonldb.folderdb")

    assert db.get_df(["missing"]) == {}

    assert any(str(tmp_path / "missing.jsonl") in record.getMessage()
               for record in caplog.records)
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""
