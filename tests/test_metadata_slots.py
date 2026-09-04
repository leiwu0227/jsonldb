import os

import orjson
import pandas as pd
import pytest

import jsonldb.folderdb as folderdb_module
from jsonldb import FolderDB
from jsonldb import jsonlfile, metaslot


def _make_slotted_table(tmp_path, name="table", rows=None, meta=None, width=160):
    path = tmp_path / (name + ".jsonl")
    jsonlfile.save_jsonl(
        str(path), rows or {"a": {"value": 1}}, meta=meta,
        slot_bytes=width,
    )
    return path


def test_slot_classification_rows_offsets_and_reserved_key(tmp_path):
    legacy = b'{"a":{"value":1}}\n'
    assert metaslot.classify_line(legacy).is_slot is False

    valid = metaslot.encode_slot({"owner": "consumer"}, 96)
    valid_info = metaslot.classify_line(valid)
    assert valid_info.is_slot is True
    assert valid_info.width == 96
    assert valid_info.version == 1
    assert valid_info.record == {"owner": "consumer"}
    assert len(valid) == 96
    assert orjson.loads(valid) == {
        "_meta": {"v": 1, "data": {"owner": "consumer"}}
    }

    malformed = b'{"_meta":{"v":1,"data":\n'
    unknown = b'{"_meta":{"v":2,"data":{"x":1}}}\n'
    extra_key = b'{"_meta":{"v":1,"data":{"x":1}},"row":{}}\n'
    assert metaslot.classify_line(malformed).is_slot is True
    assert metaslot.classify_line(malformed).record is None
    assert metaslot.classify_line(unknown).is_slot is True
    assert metaslot.classify_line(unknown).record is None
    assert metaslot.classify_line(extra_key).is_slot is True
    assert metaslot.classify_line(extra_key).record is None

    path = _make_slotted_table(
        tmp_path,
        rows={"a": {"value": 1}, "b": {"value": 2}},
        meta={"opaque": {"nested": [1, 2]}},
        width=128,
    )
    assert jsonlfile.load_jsonl(str(path)) == {
        "a": {"value": 1}, "b": {"value": 2}
    }
    index = jsonlfile.load_index(str(path))
    assert index["a"] == 128
    with path.open("rb") as f:
        f.seek(index["b"])
        assert orjson.loads(f.readline()) == {"b": {"value": 2}}

    for operation in (
        lambda: jsonlfile.save_jsonl(str(path), {"_meta": {"bad": True}}),
        lambda: jsonlfile.update_jsonl(str(path), {"_meta": {"bad": True}}),
        lambda: jsonlfile.delete_jsonl(str(path), ["_meta"]),
    ):
        with pytest.raises(ValueError, match="reserved"):
            operation()


def test_folderdb_metadata_api_and_existing_calls(tmp_path):
    db = FolderDB(str(tmp_path))
    path = _make_slotted_table(
        tmp_path, rows={"a": {"value": 1}}, meta={"generation": 1}
    )
    db.build_dbmeta()

    assert db.read_meta("table") == {"generation": 1}
    paired = db.get_dict_with_meta("table")
    assert paired.meta == {"generation": 1}
    assert paired.rows == {"a": {"value": 1}}

    db.overwrite_dict("table", {"b": {"value": 2}}, meta={"generation": 2})
    db.upsert_dict("table", {"c": {"value": 3}}, meta={"generation": 3})
    assert db.read_meta("table") == {"generation": 3}
    assert db.get_dict("table")["table"] == {
        "b": {"value": 2}, "c": {"value": 3}
    }

    frame = pd.DataFrame({"value": [4]}, index=["d"])
    db.overwrite_df("table", frame, meta={"generation": 4})
    db.upsert_df(
        "table", pd.DataFrame({"value": [5]}, index=["e"]),
        meta={"generation": 5},
    )
    paired_df = db.get_df_with_meta("table")
    assert paired_df.meta == {"generation": 5}
    assert paired_df.rows.to_dict("index") == {
        "d": {"value": 4}, "e": {"value": 5}
    }

    # The existing no-meta call preserves the record on a slotted table.
    db.upsert_dict("table", {"f": {"value": 6}})
    assert db.read_meta("table") == {"generation": 5}

    db.clear_meta("table")
    assert db.read_meta("table") is None
    assert jsonlfile.load_jsonl(str(path))["f"] == {"value": 6}

    missing_dict = db.get_dict_with_meta("missing")
    assert missing_dict.meta is None and missing_dict.rows == {}
    missing_df = db.get_df_with_meta("missing")
    assert missing_df.meta is None and missing_df.rows.empty
    assert db.read_meta("missing") is None
    db.clear_meta("missing")

    with pytest.raises(ValueError, match="not enabled"):
        db.overwrite_dict("legacy-new", {"a": {"value": 1}}, meta={"x": 1})
    assert not (tmp_path / "legacy-new.jsonl").exists()


def test_record_first_reads_and_rows_first_writes(tmp_path, monkeypatch):
    db = FolderDB(str(tmp_path))
    path = _make_slotted_table(
        tmp_path, rows={"key": {"value": "old"}}, meta={"state": "old"}
    )
    db.build_dbmeta()

    read_events = []
    real_read_meta = db.read_meta
    real_select = folderdb_module.select_jsonl

    def ordered_meta(name):
        read_events.append("meta")
        return real_read_meta(name)

    def ordered_rows(*args, **kwargs):
        read_events.append("rows")
        return real_select(*args, **kwargs)

    monkeypatch.setattr(db, "read_meta", ordered_meta)
    monkeypatch.setattr(folderdb_module, "select_jsonl", ordered_rows)
    db.get_dict_with_meta("table")
    assert read_events == ["meta", "rows"]

    new_value = "expanded-" + "x" * 256
    observed = []
    real_blank = jsonlfile._blank_old_lines

    def inspect_before_blank(file_obj, old_lines):
        if file_obj.name == str(path):
            data = path.read_bytes()
            observed.append(data)
            assert metaslot.classify_line(data.splitlines(keepends=True)[0]).record == {
                "state": "new"
            }
            assert orjson.dumps({"key": {"value": new_value}}) + b"\n" in data
        real_blank(file_obj, old_lines)

    monkeypatch.setattr(jsonlfile, "_blank_old_lines", inspect_before_blank)
    db.upsert_dict(
        "table", {"key": {"value": new_value}}, meta={"state": "new"}
    )
    assert len(observed) == 1


def test_fit_refusal_and_slot_only_index_publication(tmp_path, monkeypatch):
    path = _make_slotted_table(
        tmp_path,
        rows={"a": {"value": 1}, "b": {"value": 2}},
        meta={"state": "old"},
        width=96,
    )
    before_data = path.read_bytes()
    before_index = (tmp_path / "table.jsonl.idx").read_bytes()

    with pytest.raises(ValueError, match=r"requires \d+ bytes but slot is 96 bytes"):
        jsonlfile.update_jsonl(
            str(path), {"c": {"value": 3}}, meta={"payload": "x" * 256}
        )
    assert path.read_bytes() == before_data
    assert (tmp_path / "table.jsonl.idx").read_bytes() == before_index

    events = []
    real_write_slot = metaslot.write_slot
    real_utime = os.utime

    def ordered_slot(file_path, record):
        events.append("slot")
        real_write_slot(file_path, record)

    def ordered_utime(file_path, *args, **kwargs):
        events.append("index")
        real_utime(file_path, *args, **kwargs)

    monkeypatch.setattr(metaslot, "write_slot", ordered_slot)
    monkeypatch.setattr(jsonlfile.os, "utime", ordered_utime)

    row_bytes = before_data[96:]
    jsonlfile.write_jsonl_meta(str(path), {"state": "slot-only"})

    assert events == ["slot", "index"]
    assert path.read_bytes()[96:] == row_bytes
    assert (tmp_path / "table.jsonl.idx").read_bytes() == before_index
    assert jsonlfile.read_jsonl_meta(str(path)) == {"state": "slot-only"}
