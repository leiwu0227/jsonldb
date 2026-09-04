import logging
import os

from jsonldb import FolderDB
from jsonldb import jsonlfile


def test_jsonlfile_warnings_use_logging(tmp_path, caplog, capsys):
    caplog.set_level(logging.WARNING, logger="jsonldb.jsonlfile")
    path = tmp_path / "ticker.jsonl"
    path.write_bytes(b"not-json\n")

    jsonlfile.build_jsonl_index(str(path))
    jsonlfile.load_jsonl(str(path))

    jsonlfile.save_jsonl(str(path), {"key": {"value": 1}})
    index_path = tmp_path / "ticker.jsonl.idx"
    index_path.write_bytes(b"")
    jsonlfile.load_index(str(path))
    index_path.write_bytes(b"not-json")
    os.utime(index_path, None)
    jsonlfile.load_index(str(path))

    messages = [record.getMessage() for record in caplog.records]
    assert sum(message.startswith("invalid JSON line") for message in messages) == 2
    assert sum(message.startswith("rebuilt empty index") for message in messages) == 1
    assert sum(message.startswith("rebuilt corrupt index") for message in messages) == 1
    assert all(record.levelno == logging.WARNING for record in caplog.records)
    assert capsys.readouterr().out == ""


def test_folderdb_warnings_use_logging(tmp_path, monkeypatch, caplog, capsys):
    caplog.set_level(logging.WARNING, logger="jsonldb.folderdb")
    monkeypatch.setattr(FolderDB, "_detect_data_timespec", lambda self: "microseconds")
    monkeypatch.setattr(
        FolderDB,
        "_scan_index_timespecs",
        lambda self: {"microseconds"},
    )
    corrected_path = tmp_path / "corrected"
    corrected_path.mkdir()
    corrected = FolderDB(str(corrected_path))
    corrected.clear_folder()

    mixed_path = tmp_path / "mixed"
    mixed_path.mkdir()
    monkeypatch.setattr(
        FolderDB,
        "_scan_index_timespecs",
        lambda self: {"seconds", "microseconds"},
    )
    FolderDB(str(mixed_path))

    messages = [record.getMessage() for record in caplog.records]
    assert any("auto-correcting config.meta" in message for message in messages)
    assert any("Call clear_folder with force=True" in message for message in messages)
    assert any("mixed datetime key precisions" in message for message in messages)
    assert all(record.levelno == logging.WARNING for record in caplog.records)
    assert capsys.readouterr().out == ""
