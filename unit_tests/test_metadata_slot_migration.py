import os

import orjson
import pandas as pd
import pytest

from jsonldb import FolderDB
from jsonldb import jsonlfile, metaslot


def _first_line(path):
    return path.read_bytes().splitlines(keepends=True)[0]


def _file_bytes(root):
    return {
        str(path.relative_to(root)): path.read_bytes()
        for path in root.rglob("*") if path.is_file()
    }


def test_enable_resize_and_configured_table_creation(tmp_path, monkeypatch):
    jsonlfile.save_jsonl_atomic(
        str(tmp_path / "config.meta"),
        {"config": {"timespec": "seconds", "consumer_setting": "kept"}},
    )
    legacy = tmp_path / "legacy.jsonl"
    slotted = tmp_path / "slotted.jsonl"
    rows = {"b": {"value": 2}, "a": {"value": 1}}
    jsonlfile.save_jsonl(str(legacy), rows)
    jsonlfile.save_jsonl(
        str(slotted), rows, meta={"owner": "consumer"}, slot_bytes=160,
    )
    legacy_rows = legacy.read_bytes()
    slotted_rows = slotted.read_bytes()[160:]

    db = FolderDB(str(tmp_path))
    replaced = []
    real_replace = os.replace

    def recording_replace(source, destination):
        replaced.append(str(destination))
        return real_replace(source, destination)

    monkeypatch.setattr(jsonlfile.os, "replace", recording_replace)
    db.set_meta_slot_bytes()

    config = jsonlfile.select_jsonl(str(tmp_path / "config.meta"))["config"]
    assert config == {
        "timespec": "seconds",
        "consumer_setting": "kept",
        "meta_slot_bytes": 4096,
    }
    assert db.meta_slot_bytes == 4096
    assert metaslot.inspect_file(str(legacy)).width == 4096
    assert metaslot.inspect_file(str(slotted)).width == 4096
    assert metaslot.read_slot(str(slotted)) == {"owner": "consumer"}
    assert legacy.read_bytes()[4096:] == legacy_rows
    assert slotted.read_bytes()[4096:] == slotted_rows
    assert jsonlfile.load_jsonl(str(legacy)) == rows
    assert jsonlfile.load_jsonl(str(slotted)) == rows
    assert min(jsonlfile.load_index(str(legacy)).values()) >= 4096

    db.set_meta_slot_bytes(192)
    assert metaslot.inspect_file(str(legacy)).width == 192
    assert metaslot.inspect_file(str(slotted)).width == 192
    assert metaslot.read_slot(str(slotted)) == {"owner": "consumer"}
    assert legacy.read_bytes()[192:] == legacy_rows
    assert slotted.read_bytes()[192:] == slotted_rows

    db = FolderDB(str(tmp_path))
    assert db.meta_slot_bytes == 192
    db.overwrite_dict("new_dict", {"x": {"value": 3}})
    db.upsert_df("new_df", pd.DataFrame({"value": [4]}, index=["y"]))
    assert metaslot.inspect_file(str(tmp_path / "new_dict.jsonl")).width == 192
    assert metaslot.inspect_file(str(tmp_path / "new_df.jsonl")).width == 192

    replaced.clear()
    db.set_meta_slot_bytes(192)
    table_paths = {
        str(legacy), str(slotted), str(tmp_path / "new_dict.jsonl"),
        str(tmp_path / "new_df.jsonl"),
    }
    assert table_paths.isdisjoint(replaced)


def test_unsafe_shrink_names_all_blockers_and_changes_no_bytes(tmp_path):
    db = FolderDB(str(tmp_path))
    db.set_meta_slot_bytes(256)
    for name in ("zeta", "alpha"):
        db.overwrite_dict(
            name, {"row": {"value": name}}, meta={"payload": "x" * 100},
        )
    db.overwrite_dict("fits", {"row": {"value": "small"}}, meta={"x": 1})
    before = _file_bytes(tmp_path)

    with pytest.raises(ValueError) as error:
        db.set_meta_slot_bytes(80)

    assert str(error.value).endswith("alpha, zeta")
    assert _file_bytes(tmp_path) == before
    assert db.meta_slot_bytes == 256


def test_config_replace_failure_precedes_table_mutation_and_retry_converges(
        tmp_path, monkeypatch):
    db = FolderDB(str(tmp_path))
    db.overwrite_dict("table", {"row": {"value": 1}})
    table = tmp_path / "table.jsonl"
    before_table = table.read_bytes()
    before_config = (tmp_path / "config.meta").read_bytes()
    real_replace = os.replace

    def fail_config(source, destination):
        if str(destination) == str(tmp_path / "config.meta"):
            raise OSError("injected config replacement interruption")
        return real_replace(source, destination)

    with monkeypatch.context() as patch:
        patch.setattr(jsonlfile.os, "replace", fail_config)
        with pytest.raises(OSError, match="config replacement interruption"):
            db.set_meta_slot_bytes(192)

    assert table.read_bytes() == before_table
    assert (tmp_path / "config.meta").read_bytes() == before_config
    assert db.meta_slot_bytes is None
    assert list(tmp_path.glob(".config.meta.*.tmp")) == []

    db.set_meta_slot_bytes(192)
    assert metaslot.inspect_file(str(table)).width == 192
    assert jsonlfile.load_jsonl(str(table)) == {"row": {"value": 1}}


def test_table_replace_failure_leaves_whole_files_and_retry_skips_completed(
        tmp_path, monkeypatch):
    db = FolderDB(str(tmp_path))
    db.set_meta_slot_bytes(160)
    for name in ("alpha", "beta"):
        db.overwrite_dict(
            name, {"row": {"value": name}}, meta={"table": name},
        )
    alpha = tmp_path / "alpha.jsonl"
    beta = tmp_path / "beta.jsonl"
    old_alpha = alpha.read_bytes()
    old_beta = beta.read_bytes()
    real_replace = os.replace

    def fail_beta(source, destination):
        if str(destination) == str(beta):
            raise OSError("injected table replacement interruption")
        return real_replace(source, destination)

    with monkeypatch.context() as patch:
        patch.setattr(jsonlfile.os, "replace", fail_beta)
        with pytest.raises(OSError, match="table replacement interruption"):
            db.set_meta_slot_bytes(192)

    assert metaslot.inspect_file(str(alpha)).width == 192
    assert beta.read_bytes() == old_beta
    assert alpha.read_bytes() != old_alpha
    assert metaslot.read_slot(str(alpha)) == {"table": "alpha"}
    assert jsonlfile.load_jsonl(str(alpha)) == {"row": {"value": "alpha"}}
    assert list(tmp_path.glob(".beta.jsonl.*.tmp")) == []
    assert jsonlfile.select_jsonl(str(tmp_path / "config.meta"))["config"][
        "meta_slot_bytes"
    ] == 192

    destinations = []

    def recording_replace(source, destination):
        destinations.append(str(destination))
        return real_replace(source, destination)

    with monkeypatch.context() as patch:
        patch.setattr(jsonlfile.os, "replace", recording_replace)
        db.set_meta_slot_bytes(192)

    assert str(alpha) not in destinations
    assert str(beta) in destinations
    for path, name in ((alpha, "alpha"), (beta, "beta")):
        assert metaslot.inspect_file(str(path)).width == 192
        assert metaslot.read_slot(str(path)) == {"table": name}
        assert jsonlfile.load_jsonl(str(path)) == {"row": {"value": name}}
        assert jsonlfile.load_index(str(path)) == {"row": 192}


def test_config_index_failure_leaves_new_config_and_retry_migrates_tables(
        tmp_path, monkeypatch):
    db = FolderDB(str(tmp_path))
    db.overwrite_dict("table", {"row": {"value": 1}})
    table = tmp_path / "table.jsonl"
    old_table = table.read_bytes()
    old_index = (tmp_path / "config.meta.idx").read_bytes()
    real_write_index = jsonlfile._write_index

    def fail_config_index(file_path, index):
        if file_path == str(tmp_path / "config.meta"):
            raise OSError("injected config index interruption")
        return real_write_index(file_path, index)

    with monkeypatch.context() as patch:
        patch.setattr(jsonlfile, "_write_index", fail_config_index)
        with pytest.raises(OSError, match="config index interruption"):
            db.set_meta_slot_bytes(208)

    assert table.read_bytes() == old_table
    assert (tmp_path / "config.meta.idx").read_bytes() == old_index
    assert jsonlfile.load_jsonl(str(tmp_path / "config.meta"))["config"][
        "meta_slot_bytes"
    ] == 208
    assert db.meta_slot_bytes == 208

    db.set_meta_slot_bytes(208)
    assert metaslot.inspect_file(str(table)).width == 208
    assert jsonlfile.load_index(str(table)) == {"row": 208}


def test_index_publish_failure_is_repaired_without_rewriting_table(
        tmp_path, monkeypatch):
    db = FolderDB(str(tmp_path))
    db.set_meta_slot_bytes(160)
    db.overwrite_dict("table", {"row": {"value": 1}}, meta={"v": 1})
    table = tmp_path / "table.jsonl"
    real_write_index = jsonlfile._write_index

    def fail_table_index(file_path, index):
        if file_path == str(table):
            raise OSError("injected table index interruption")
        return real_write_index(file_path, index)

    with monkeypatch.context() as patch:
        patch.setattr(jsonlfile, "_write_index", fail_table_index)
        with pytest.raises(OSError, match="table index interruption"):
            db.set_meta_slot_bytes(224)

    migrated_bytes = table.read_bytes()
    assert metaslot.inspect_file(str(table)).width == 224
    assert metaslot.read_slot(str(table)) == {"v": 1}

    replaced = []
    real_replace = os.replace

    def recording_replace(source, destination):
        replaced.append(str(destination))
        return real_replace(source, destination)

    monkeypatch.setattr(jsonlfile.os, "replace", recording_replace)
    db.set_meta_slot_bytes(224)
    assert str(table) not in replaced
    assert table.read_bytes() == migrated_bytes
    assert jsonlfile.load_index(str(table)) == {"row": 224}
    assert orjson.loads((tmp_path / "table.jsonl.idx").read_bytes()) == {
        "row": 224
    }
