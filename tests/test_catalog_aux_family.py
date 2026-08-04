import hashlib
import importlib
import os
import sys
import types
from dataclasses import FrozenInstanceError

import orjson
import pytest

from jsonldb import (
    CatalogChangedError,
    FolderCatalogEntry,
    FolderDB,
    TickerFamilyRead,
)
from jsonldb import jsonlfile
from jsonldb.catalog import invalidate_snapshot


def _digest(payload):
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _rewrite_as_v1(db):
    path = db.catalog_path
    raw = orjson.loads(open(path, "rb").read())
    raw["version"] = 1
    for entry in raw["entries"].values():
        entry.pop("aux_present")
        entry.pop("aux_size")
        entry.pop("aux_sha256")
    with open(path, "wb") as stream:
        stream.write(orjson.dumps(raw, option=orjson.OPT_SORT_KEYS))
    invalidate_snapshot(db._catalog_root)
    return raw


def test_v2_identity_and_backward_compatible_entry_defaults(tmp_path):
    legacy = FolderCatalogEntry(None, None, 0, 0, False, "")
    assert (legacy.aux_present, legacy.aux_size, legacy.aux_sha256) == (
        False, None, None,
    )

    db = FolderDB(str(tmp_path))
    db.upsert_dict("ticker", {"a": {"value": 1}})
    assert db.load_catalog_snapshot().version == 2
    db.write_aux("ticker", b"")
    empty = db.load_catalog_snapshot().entries["ticker"]
    assert empty.aux_present is True
    assert empty.aux_size == 0
    assert empty.aux_sha256 == _digest(b"")

    db.write_aux("ticker", b"opaque\x00bytes")
    present = db.load_catalog_snapshot().entries["ticker"]
    assert present.aux_size == 12
    assert present.aux_sha256 == _digest(b"opaque\x00bytes")
    db.upsert_dict("ticker", {"b": {"value": 2}})
    absent = db.load_catalog_snapshot().entries["ticker"]
    assert (absent.aux_present, absent.aux_size, absent.aux_sha256) == (
        False, None, None,
    )


def test_v1_migration_hashes_only_cataloged_companions_without_owner_work(
    monkeypatch, tmp_path,
):
    db = FolderDB(str(tmp_path))
    db.upsert_dicts({
        "alpha": {"a": {"value": 1}},
        "beta": {"b": {"value": 2}},
    })
    db.write_aux("alpha", b"derived")
    v1 = _rewrite_as_v1(db)

    monkeypatch.setattr(
        FolderDB, "get_file_list",
        lambda self: (_ for _ in ()).throw(AssertionError("owner walk")),
    )
    monkeypatch.setattr(
        jsonlfile, "load_index",
        lambda path: (_ for _ in ()).throw(AssertionError("index load")),
    )
    migrated = FolderDB(str(tmp_path)).load_catalog_snapshot()
    assert migrated.version == 2
    assert migrated.catalog_id == v1["catalog_id"]
    assert migrated.revision == v1["revision"] + 1
    assert migrated.entries["alpha"].aux_sha256 == _digest(b"derived")
    assert migrated.entries["beta"].aux_present is False
    assert not os.path.exists(db.pending_path)


@pytest.mark.parametrize("field,value", [
    ("aux_present", 1),
    ("aux_size", -1),
    ("aux_sha256", "sha256:" + "A" * 64),
])
def test_invalid_v2_aux_identity_is_all_or_nothing(field, value, tmp_path):
    db = FolderDB(str(tmp_path))
    db.upsert_dict("ticker", {"a": {"value": 1}})
    raw = orjson.loads(open(db.catalog_path, "rb").read())
    raw["entries"]["ticker"].update({
        "aux_present": True,
        "aux_size": 0,
        "aux_sha256": _digest(b""),
    })
    raw["entries"]["ticker"][field] = value
    from jsonldb.catalog import validate_catalog_envelope
    with pytest.raises(ValueError):
        validate_catalog_envelope(raw, (db.catalog_path,), db._catalog_name_valid)


def test_family_read_returns_exact_range_aux_and_generation(tmp_path):
    db = FolderDB(str(tmp_path))
    db.upsert_dict("ticker", {
        "a": {"value": 1}, "b": {"value": 2}, "c": {"value": 3},
    })
    db.write_aux("ticker", b"opaque")
    snapshot = db.load_catalog_snapshot()
    result = db.read_family("ticker", snapshot, "b", "c")

    assert isinstance(result, TickerFamilyRead)
    assert result.name == "ticker"
    assert result.data == {"b": {"value": 2}, "c": {"value": 3}}
    assert result.aux == b"opaque"
    assert (result.catalog_id, result.revision) == (
        snapshot.catalog_id, snapshot.revision,
    )
    assert result.aux_sha256 == _digest(b"opaque")
    with pytest.raises(FrozenInstanceError):
        result.revision = 0


def test_family_read_rejects_stale_foreign_pending_and_aux_mismatch(tmp_path):
    root_a = tmp_path / "a"
    root_b = tmp_path / "b"
    root_a.mkdir()
    root_b.mkdir()
    first = FolderDB(str(root_a))
    second = FolderDB(str(root_b))
    first.upsert_dict("ticker", {"a": {"value": 1}})
    first.write_aux("ticker", b"old")
    second.upsert_dict("ticker", {"a": {"value": 1}})
    stale = first.load_catalog_snapshot()
    foreign = second.load_catalog_snapshot()

    first.write_aux("ticker", b"new")
    with pytest.raises(CatalogChangedError):
        first.read_family("ticker", stale)
    with pytest.raises(CatalogChangedError):
        first.read_family("ticker", foreign)

    current = first.load_catalog_snapshot()
    with open(first.pending_path, "wb") as stream:
        stream.write(b"pending")
    with pytest.raises(CatalogChangedError):
        first.read_family("ticker", current)
    os.remove(first.pending_path)

    with open(first.get_aux_path("ticker"), "wb") as stream:
        stream.write(b"NEW")
    with pytest.raises(CatalogChangedError):
        first.read_family("ticker", current)

    os.remove(first.get_aux_path("ticker"))
    with pytest.raises(CatalogChangedError):
        first.read_family("ticker", current)


def test_family_read_retries_unbound_but_never_substitutes_bound_snapshot(
    monkeypatch, tmp_path,
):
    db = FolderDB(str(tmp_path))
    db.upsert_dict("ticker", {"a": {"value": 1}})
    db.upsert_dict("other", {"a": {"value": 1}})
    supplied = db.load_catalog_snapshot()
    folderdb_module = importlib.import_module("jsonldb.folderdb")
    real_select = folderdb_module.select_jsonl
    changed = {"done": False}

    def change_generation(path, *args, **kwargs):
        value = real_select(path, *args, **kwargs)
        if path.endswith("ticker.jsonl") and not changed["done"]:
            changed["done"] = True
            db.upsert_dict("other", {"b": {"value": 2}})
        return value

    monkeypatch.setattr(folderdb_module, "select_jsonl", change_generation)
    with pytest.raises(CatalogChangedError):
        db.read_family("ticker", supplied)

    changed["done"] = False
    result = db.read_family("ticker")
    assert result.data == {"a": {"value": 1}}
    assert result.revision == db.load_catalog_snapshot().revision


def test_healthy_family_read_takes_no_writer_lock_or_unrelated_reads(
    monkeypatch, tmp_path,
):
    db = FolderDB(str(tmp_path))
    db.upsert_dict("ticker", {"a": {"value": 1}})
    db.upsert_dict("other", {"x": {"value": 9}})
    db.write_aux("ticker", b"payload")
    snapshot = db.load_catalog_snapshot()
    folderdb_module = importlib.import_module("jsonldb.folderdb")
    real_select = folderdb_module.select_jsonl
    selected = []

    class ForbiddenLock:
        def __init__(self, *args, **kwargs):
            raise AssertionError("writer lock acquired")

    def record_select(path, *args, **kwargs):
        selected.append(os.path.basename(path))
        return real_select(path, *args, **kwargs)

    monkeypatch.setattr(folderdb_module, "WriterLock", ForbiddenLock)
    monkeypatch.setattr(folderdb_module, "select_jsonl", record_select)
    result = db.read_family("ticker", snapshot)
    assert result.aux == b"payload"
    assert selected == ["ticker.jsonl"]


def test_missing_family_owner_raises_file_not_found(tmp_path):
    db = FolderDB(str(tmp_path))
    db.load_catalog_snapshot()
    with pytest.raises(FileNotFoundError):
        db.read_family("missing")


def test_aux_recovery_and_full_reconciliation_publish_final_identity(
    monkeypatch, tmp_path,
):
    db = FolderDB(str(tmp_path))
    db.upsert_dict("ticker", {"a": {"value": 1}})
    before = db.load_catalog_snapshot()
    real_publish = FolderDB._publish_catalog

    def fail_publish(self, *args, **kwargs):
        raise OSError("injected catalog interruption")

    monkeypatch.setattr(FolderDB, "_publish_catalog", fail_publish)
    with pytest.raises(OSError, match="interruption"):
        db.write_aux("ticker", b"recovered")
    monkeypatch.setattr(FolderDB, "_publish_catalog", real_publish)
    recovered = FolderDB(str(tmp_path)).load_catalog_snapshot()
    assert recovered.revision == before.revision + 1
    assert recovered.entries["ticker"].aux_sha256 == _digest(b"recovered")

    with open(db.get_aux_path("ticker"), "wb") as stream:
        stream.write(b"RECONCILE")
    db.get_dbmeta()
    reconciled = db.load_catalog_snapshot()
    assert reconciled.revision == recovered.revision + 1
    assert reconciled.entries["ticker"].aux_sha256 == _digest(b"RECONCILE")


def test_hierarchy_move_preserves_cataloged_family_identity(tmp_path):
    db = FolderDB(str(tmp_path))
    db.upsert_dict("region.ticker", {"a": {"value": 1}})
    db.write_aux("region.ticker", b"hierarchical")
    db.lint_hierarchy(2)

    snapshot = db.load_catalog_snapshot()
    entry = snapshot.entries["region.ticker"]
    assert entry.aux_sha256 == _digest(b"hierarchical")
    assert db.read_family("region.ticker", snapshot).aux == b"hierarchical"


def test_managed_commit_migrates_v1_before_git_publication(monkeypatch, tmp_path):
    db = FolderDB(str(tmp_path))
    db.upsert_dict("ticker", {"a": {"value": 1}})
    db.write_aux("ticker", b"committed")
    v1 = _rewrite_as_v1(db)
    observed = []
    fake = types.ModuleType("jsonldb.vercontrol")
    fake.is_versioned = lambda path: True
    fake.init_folder = lambda path: None

    def commit(path, message):
        raw = orjson.loads(open(db.catalog_path, "rb").read())
        observed.append((path, message, raw))

    fake.commit = commit
    monkeypatch.setitem(sys.modules, "jsonldb.vercontrol", fake)
    db.commit("v2")

    raw = observed[0][2]
    assert raw["version"] == 2
    assert raw["catalog_id"] == v1["catalog_id"]
    assert raw["revision"] == v1["revision"] + 1
    assert raw["entries"]["ticker"]["aux_sha256"] == _digest(b"committed")
