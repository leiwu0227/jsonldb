import datetime as dt
import logging

import orjson
import pandas as pd
import pytest

import jsonldb
from jsonldb import FolderDB, jsonldf, jsonlfile, metaslot


@pytest.mark.parametrize("value", [None, 1, [], "record"])
@pytest.mark.parametrize("writer", ["save_jsonl", "save_jsonl_atomic", "update_jsonl"])
def test_writers_reject_non_dictionary_values_before_mutation(
        tmp_path, writer, value):
    path = tmp_path / "rows.jsonl"
    jsonlfile.save_jsonl(str(path), {"kept": {"value": 1}})
    before = path.read_bytes(), (tmp_path / "rows.jsonl.idx").read_bytes()

    with pytest.raises(TypeError, match="must be dictionaries"):
        getattr(jsonlfile, writer)(str(path), {"bad": value})

    assert (path.read_bytes(), (tmp_path / "rows.jsonl.idx").read_bytes()) == before


def test_invalid_disk_values_are_skipped_unindexed_and_removed(tmp_path, caplog):
    path = tmp_path / "damaged.jsonl"
    rows = [
        {"a": {"value": 1}},
        {"bad_scalar": 2},
        {"bad_list": []},
        {"bad_null": None},
        {"z": {}},
    ]
    path.write_bytes(b"".join(orjson.dumps(row) + b"\n" for row in rows))
    caplog.set_level(logging.WARNING, logger="jsonldb.jsonlfile")

    jsonlfile.build_jsonl_index(str(path))
    expected = {"a": {"value": 1}, "z": {}}
    assert list(jsonlfile.load_index(str(path))) == ["a", "z"]
    assert jsonlfile.load_jsonl(str(path), auto_deserialize=False) == expected
    assert jsonlfile.select_jsonl(
        str(path), "a", "z", auto_deserialize=False) == expected
    assert jsonlfile.select_line_jsonl(
        str(path), "bad_scalar", auto_deserialize=False) == {}

    caplog.clear()
    assert jsonlfile.lint_jsonl(str(path), force=True) is True
    assert jsonlfile.load_jsonl(str(path), auto_deserialize=False) == expected
    assert path.read_bytes() == b"".join(
        orjson.dumps({key: value}) + b"\n" for key, value in expected.items())
    assert any(
        str(path) in record.getMessage()
        for record in caplog.records
        if record.getMessage().startswith("lint removed"))


def test_datetime_single_and_equal_bound_lookup_ignore_output_mode(tmp_path):
    path = tmp_path / "dated.jsonl"
    key = dt.datetime(2026, 9, 5, 12, 34, 56)
    record = {"value": 1}
    jsonlfile.save_jsonl(str(path), {key: record}, timespec="seconds")
    stored_key = key.isoformat(timespec="seconds")

    assert jsonlfile.select_line_jsonl(
        str(path), key, auto_deserialize=False, timespec="seconds") == {
            stored_key: record}
    assert jsonlfile.select_jsonl(
        str(path), key, key, auto_deserialize=False, timespec="seconds") == {
            stored_key: record}
    assert jsonlfile.select_line_jsonl(
        str(path), key, auto_deserialize=True, timespec="seconds") == {
            key: record}


@pytest.mark.parametrize("width", [64, None])
def test_standalone_lint_repairs_or_removes_malformed_slot(
        tmp_path, caplog, width):
    path = tmp_path / "slot.jsonl"
    prefix = b'{"_meta":{}}'
    malformed = (prefix + b" " * (width - len(prefix) - 1) + b"\n"
                 if width is not None else prefix + b"\n")
    row = orjson.dumps({"row": {"value": 1}}) + b"\n"
    path.write_bytes(malformed + row)
    jsonlfile.build_jsonl_index(str(path), warn_invalid=False)
    caplog.set_level(logging.WARNING, logger="jsonldb.jsonlfile")

    assert jsonlfile.lint_jsonl(str(path), force=True) is True
    assert jsonlfile.load_jsonl(str(path)) == {"row": {"value": 1}}
    index = jsonlfile.load_index(str(path))
    if width is None:
        assert path.read_bytes() == row
        assert index == {"row": 0}
    else:
        info = metaslot.inspect_file(str(path))
        assert info.version == metaslot.CURRENT_VERSION
        assert info.width == width
        assert info.record is None
        assert index == {"row": width}
    assert any(str(path) in record.getMessage() for record in caplog.records)


def test_dataframe_load_and_lint_forwarding_remain_positional(tmp_path, monkeypatch):
    path = tmp_path / "frame.jsonl"
    key = dt.datetime(2026, 9, 5, 1, 2, 3)
    jsonldf.save_jsonldf(
        str(path), pd.DataFrame({"value": [1]}, index=[key]), "seconds")

    assert list(jsonldf.load_jsonldf(str(path), "seconds").index) == [key]
    assert list(jsonldf.load_jsonldf(
        str(path), "seconds", False).index) == [key.isoformat()]

    calls = []

    def fake_lint(file_path, force=False, slot_bytes=None):
        calls.append((file_path, force, slot_bytes))
        return False

    monkeypatch.setattr(jsonldf, "lint_jsonl", fake_lint)
    assert jsonldf.lint_jsonldf(str(path), True, 96) is False
    assert calls == [(str(path), True, 96)]


def test_package_inventory_lists_metaslot_without_optional_imports():
    assert "metaslot" in jsonldb.__all__
    assert jsonldb.metaslot is metaslot


def test_folder_open_migrates_legacy_scalar_control_rows(tmp_path):
    controls = {
        "config.meta": {
            "timespec": "microseconds",
            "meta_slot_bytes": 96,
            "consumer_setting": "kept",
        },
        "h.meta": {
            "use_hierarchy": True,
            "delimiter": ".",
            "hierarchy_depth": 1,
        },
    }
    for name, values in controls.items():
        path = tmp_path / name
        path.write_bytes(b"".join(
            orjson.dumps({key: value}) + b"\n"
            for key, value in values.items()))
        jsonlfile.build_jsonl_index(str(path), warn_invalid=False)

    db = FolderDB(str(tmp_path))

    assert db.timespec == "microseconds"
    assert db.meta_slot_bytes == 96
    assert db._config_meta["consumer_setting"] == "kept"
    assert (db.use_hierarchy, db.delimiter, db.hierarchy_depth) == (
        True, ".", 1)
    assert jsonlfile.load_jsonl(str(tmp_path / "config.meta")) == {
        "config": controls["config.meta"]}
    assert jsonlfile.load_jsonl(str(tmp_path / "h.meta")) == {
        "hierarchy": controls["h.meta"]}
