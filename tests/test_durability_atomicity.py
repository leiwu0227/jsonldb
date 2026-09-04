import logging
import os

import orjson
import pytest

from jsonldb.folderdb import FolderDB
from jsonldb import jsonlfile


def _index_bytes(index):
    return orjson.dumps(index, option=orjson.OPT_SORT_KEYS)


def test_all_row_read_paths_skip_and_log_torn_rows_with_paths(tmp_path, caplog):
    path = tmp_path / "records.jsonl"
    first = b'{"a":{"value":1}}\n'
    torn = b'{"b":{"value":\n'
    last = b'{"c":{"value":3}}\n'
    path.write_bytes(first + torn + last)
    torn_offset = len(first)
    last_offset = torn_offset + len(torn)
    caplog.set_level(logging.WARNING, logger="jsonldb.jsonlfile")

    jsonlfile.build_jsonl_index(str(path))
    assert jsonlfile.load_index(str(path)) == {"a": 0, "c": last_offset}
    assert jsonlfile.load_jsonl(str(path)) == {
        "a": {"value": 1},
        "c": {"value": 3},
    }

    index_path = tmp_path / "records.jsonl.idx"
    index_path.write_bytes(_index_bytes({"b": torn_offset}))
    assert jsonlfile.select_line_jsonl(str(path), "b") == {}

    index_path.write_bytes(_index_bytes({
        "a": 0,
        "b": torn_offset,
        "c": last_offset,
    }))
    assert jsonlfile.select_jsonl(str(path), "a", "c") == {
        "a": {"value": 1},
        "c": {"value": 3},
    }

    warnings = [record.getMessage() for record in caplog.records]
    skipped = [message for message in warnings if message.startswith("invalid JSON line")]
    assert len(skipped) == 4
    assert all(str(path) in message for message in skipped)
    assert all("at byte " in message for message in skipped)


def test_update_heals_missing_terminal_newline_before_append(tmp_path):
    path = tmp_path / "records.jsonl"
    first = b'{"a":{"value":1}}'
    path.write_bytes(first)
    (tmp_path / "records.jsonl.idx").write_bytes(_index_bytes({"a": 0}))

    jsonlfile.update_jsonl(str(path), {"b": {"value": 2}})

    assert path.read_bytes() == first + b'\n{"b":{"value":2}}\n'
    assert jsonlfile.load_jsonl(str(path)) == {
        "a": {"value": 1},
        "b": {"value": 2},
    }


def test_every_index_writer_uses_atomic_replace_and_compact_sorted_bytes(
        tmp_path, monkeypatch):
    path = tmp_path / "records.jsonl"
    index_path = str(path) + ".idx"
    real_replace = os.replace
    destinations = []

    def recording_replace(source, destination):
        destinations.append(str(destination))
        return real_replace(source, destination)

    monkeypatch.setattr(jsonlfile.os, "replace", recording_replace)

    jsonlfile.save_jsonl(str(path), {"b": {"value": 2}, "a": {"value": 1}})
    jsonlfile.update_jsonl(str(path), {"c": {"value": 3}})
    jsonlfile.delete_jsonl(str(path), ["a"])
    jsonlfile.build_jsonl_index(str(path))

    assert destinations.count(index_path) == 4
    parsed = orjson.loads((tmp_path / "records.jsonl.idx").read_bytes())
    assert list(parsed) == sorted(parsed)
    assert (tmp_path / "records.jsonl.idx").read_bytes() == _index_bytes(parsed)


def test_index_serialization_and_replacement_failures_keep_old_index_complete(
        tmp_path, monkeypatch):
    path = tmp_path / "records.jsonl"
    jsonlfile.save_jsonl(str(path), {"a": {"value": "old"}})
    index_path = tmp_path / "records.jsonl.idx"
    old_index = index_path.read_bytes()

    with monkeypatch.context() as patch:
        def fail_serialization(index):
            raise RuntimeError("injected serialization interruption")

        patch.setattr(jsonlfile, "_serialize_index", fail_serialization)
        with pytest.raises(RuntimeError, match="serialization interruption"):
            jsonlfile.update_jsonl(str(path), {"a": {"value": "new"}})
    assert index_path.read_bytes() == old_index
    assert orjson.loads(index_path.read_bytes()) == {"a": 0}

    real_replace = os.replace
    with monkeypatch.context() as patch:
        def fail_index_replace(source, destination):
            if str(destination) == str(index_path):
                raise OSError("injected replacement interruption")
            return real_replace(source, destination)

        patch.setattr(jsonlfile.os, "replace", fail_index_replace)
        with pytest.raises(OSError, match="replacement interruption"):
            jsonlfile._write_index(str(path), {"replacement": 99})
    assert index_path.read_bytes() == old_index
    assert list(tmp_path.glob(".records.jsonl.idx.*.tmp")) == []


def test_grown_upsert_interruption_after_append_keeps_old_record_available(
        tmp_path, monkeypatch):
    path = tmp_path / "records.jsonl"
    old_value = {"value": "old"}
    new_value = {"value": "expanded-" + "x" * 128}
    jsonlfile.save_jsonl(str(path), {"key": old_value})

    def interrupt_before_blank(f, old_lines):
        raise RuntimeError("injected append-to-blank interruption")

    monkeypatch.setattr(jsonlfile, "_blank_old_lines", interrupt_before_blank)
    with pytest.raises(RuntimeError, match="append-to-blank interruption"):
        jsonlfile.update_jsonl(str(path), {"key": new_value})

    recovered = jsonlfile.select_line_jsonl(str(path), "key")
    assert recovered in ({"key": old_value}, {"key": new_value})
    assert orjson.dumps({"key": new_value}) + b"\n" in path.read_bytes()
    assert orjson.dumps({"key": old_value}) + b"\n" in path.read_bytes()


def test_grown_upsert_publishes_index_after_append_and_blank(tmp_path, monkeypatch):
    path = tmp_path / "records.jsonl"
    new_value = {"value": "expanded-" + "x" * 128}
    jsonlfile.save_jsonl(str(path), {"key": {"value": "old"}})
    old_line_length = len(path.read_bytes())
    real_write_index = jsonlfile._write_index
    observed = []

    def inspect_then_write_index(jsonl_path, index):
        data = path.read_bytes()
        observed.append(data)
        assert data[:old_line_length] == b" " * (old_line_length - 1) + b"\n"
        assert orjson.dumps({"key": new_value}) + b"\n" in data
        real_write_index(jsonl_path, index)

    monkeypatch.setattr(jsonlfile, "_write_index", inspect_then_write_index)
    jsonlfile.update_jsonl(str(path), {"key": new_value})

    assert len(observed) == 1
    assert jsonlfile.select_line_jsonl(str(path), "key") == {"key": new_value}
    parsed = orjson.loads((tmp_path / "records.jsonl.idx").read_bytes())
    assert (tmp_path / "records.jsonl.idx").read_bytes() == _index_bytes(parsed)


def test_grown_upsert_interruption_before_index_keeps_new_record_rebuildable(
        tmp_path, monkeypatch):
    path = tmp_path / "records.jsonl"
    new_value = {"value": "expanded-" + "x" * 128}
    jsonlfile.save_jsonl(str(path), {"key": {"value": "old"}})

    def interrupt_index(jsonl_path, index):
        raise RuntimeError("injected pre-index interruption")

    monkeypatch.setattr(jsonlfile, "_write_index", interrupt_index)
    with pytest.raises(RuntimeError, match="pre-index interruption"):
        jsonlfile.update_jsonl(str(path), {"key": new_value})

    assert jsonlfile.load_jsonl(str(path)) == {"key": new_value}
    monkeypatch.undo()
    jsonlfile.build_jsonl_index(str(path))
    assert jsonlfile.select_line_jsonl(str(path), "key") == {"key": new_value}


def test_protected_control_files_replace_atomically_but_dbmeta_does_not(
        tmp_path, monkeypatch):
    db = FolderDB(str(tmp_path))
    real_replace = os.replace
    destinations = []

    def recording_replace(source, destination):
        destinations.append(str(destination))
        return real_replace(source, destination)

    monkeypatch.setattr(jsonlfile.os, "replace", recording_replace)
    db.build_configmeta()
    jsonlfile.save_jsonl(str(tmp_path / "table.jsonl"), {"a": {"value": 1}})
    db.build_dbmeta()
    db.use_hierarchy = True
    db.delimiter = "."
    db.hierarchy_depth = 2
    db.build_hmeta()

    assert str(tmp_path / "config.meta") in destinations
    assert str(tmp_path / "h.meta") in destinations
    assert str(tmp_path / "db.meta") not in destinations
    assert str(tmp_path / "db.meta.idx") in destinations


@pytest.mark.parametrize("control_name", ["config.meta", "h.meta"])
def test_control_file_replace_failure_preserves_complete_old_file(
        tmp_path, monkeypatch, control_name):
    db = FolderDB(str(tmp_path))
    db.use_hierarchy = True
    db.delimiter = "."
    db.hierarchy_depth = 1
    db.build_hmeta()
    control_path = tmp_path / control_name
    old_bytes = control_path.read_bytes()
    old_index = (tmp_path / (control_name + ".idx")).read_bytes()

    if control_name == "config.meta":
        db.timespec = "microseconds"
        publish = db.build_configmeta
    else:
        db.hierarchy_depth = 2
        publish = db.build_hmeta

    real_replace = os.replace

    def fail_control_replace(source, destination):
        if str(destination) == str(control_path):
            raise OSError("injected control replacement interruption")
        return real_replace(source, destination)

    monkeypatch.setattr(jsonlfile.os, "replace", fail_control_replace)
    with pytest.raises(OSError, match="control replacement interruption"):
        publish()

    assert control_path.read_bytes() == old_bytes
    assert (tmp_path / (control_name + ".idx")).read_bytes() == old_index
    assert jsonlfile.load_jsonl(str(control_path))
    assert list(tmp_path.glob("." + control_name + ".*.tmp")) == []
