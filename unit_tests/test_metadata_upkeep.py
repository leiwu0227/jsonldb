import os

from jsonldb import FolderDB
from jsonldb.jsonlfile import delete_jsonl, load_jsonl, save_jsonl


def test_file_and_range_deletion_refresh_metadata(tmp_path):
    db = FolderDB(str(tmp_path))
    db.upsert_dict("ticker", {
        "a": {"value": 1},
        "b": {"value": 2},
        "c": {"value": 3},
    })

    db.delete_file_range("ticker", "b", "c")

    entry = db.get_dbmeta()["ticker"]
    assert entry["count"] == 1
    assert entry["min_index"] == "a"
    assert entry["max_index"] == "a"

    db.delete_file("ticker")

    assert "ticker" not in db.get_dbmeta()


def test_hierarchical_reopen_detects_child_directory_change(tmp_path):
    save_jsonl(str(tmp_path / "h.meta"), {
        "hierarchy": {
            "use_hierarchy": True,
            "delimiter": ".",
            "hierarchy_depth": 1,
        },
    })
    db = FolderDB(str(tmp_path))
    db.upsert_dict("region.first", {"a": {"value": 1}})

    child = tmp_path / "region"
    second_path = child / "region.second.jsonl"
    save_jsonl(
        str(second_path),
        {"b": {"value": 2}},
    )
    newer = os.stat(db.dbmeta_path).st_mtime_ns + 1_000_000_000
    os.utime(child, ns=(newer, newer))

    reopened = FolderDB(str(tmp_path))

    assert set(reopened.get_dbmeta()) == {"region.first", "region.second"}

    second_path.unlink()
    (child / "region.second.jsonl.idx").unlink()
    newer = os.stat(reopened.dbmeta_path).st_mtime_ns + 1_000_000_000
    os.utime(child, ns=(newer, newer))

    reopened_after_removal = FolderDB(str(tmp_path))

    assert set(reopened_after_removal.get_dbmeta()) == {"region.first"}


def test_lint_includes_on_disk_table_missing_from_metadata(tmp_path):
    db = FolderDB(str(tmp_path))
    path = tmp_path / "external.jsonl"
    save_jsonl(str(path), {
        "a": {"value": 1},
        "b": {"value": 2},
        "c": {"value": 3},
    })
    delete_jsonl(str(path), ["b"])
    size_with_tombstone = path.stat().st_size

    assert "external" not in load_jsonl(db.dbmeta_path)
    db.lint_db()

    entry = db.get_dbmeta()["external"]
    assert entry["count"] == 2
    assert entry["linted"] is True
    assert path.stat().st_size < size_with_tombstone
