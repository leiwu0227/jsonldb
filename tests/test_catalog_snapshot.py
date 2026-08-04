import inspect
import multiprocessing
import os
import sys
import types
import uuid
from dataclasses import FrozenInstanceError

import orjson
import pytest

from jsonldb import (
    CatalogBusyError,
    CatalogFilesystemError,
    FolderCatalogEntry,
    FolderCatalogSnapshot,
    FolderDB,
    UnsupportedCatalogVersionError,
)
from jsonldb import jsonlfile
from jsonldb.catalog import (
    PendingState,
    WriterLock,
    atomic_write,
    pending_envelope,
)


def _hold_writer_process(lock_path, pending_path, envelope, ready, release):
    with WriterLock(lock_path, 2.0):
        atomic_write(pending_path, envelope)
        ready.put(True)
        release.wait(2.0)


def test_constructor_defers_owner_discovery_and_snapshot_is_shared(monkeypatch, tmp_path):
    owner = tmp_path / "ticker.jsonl"
    jsonlfile.save_jsonl(str(owner), {"a": {"value": 1}})
    migrated = FolderDB(str(tmp_path)).load_catalog_snapshot()
    calls = {"walks": 0, "indexes": 0}
    real_walk = FolderDB.get_file_list
    real_index = jsonlfile.load_index

    def count_walk(self):
        calls["walks"] += 1
        return real_walk(self)

    def count_index(path):
        if path.endswith(".jsonl"):
            calls["indexes"] += 1
        return real_index(path)

    monkeypatch.setattr(FolderDB, "get_file_list", count_walk)
    monkeypatch.setattr(jsonlfile, "load_index", count_index)
    first_db = FolderDB(str(tmp_path))
    assert calls == {"walks": 0, "indexes": 0}
    from jsonldb.catalog import invalidate_snapshot
    invalidate_snapshot(first_db._catalog_root)
    first = first_db.load_catalog_snapshot()
    second = FolderDB(str(tmp_path)).load_catalog_snapshot()

    assert first is second
    assert first.catalog_id == migrated.catalog_id
    assert first.revision == migrated.revision
    assert calls == {"walks": 0, "indexes": 0}
    assert isinstance(first, FolderCatalogSnapshot)
    assert isinstance(first.entries["ticker"], FolderCatalogEntry)
    with pytest.raises(TypeError):
        first.entries["other"] = first.entries["ticker"]
    with pytest.raises(FrozenInstanceError):
        first.revision = 99
    assert (tmp_path / ".jsonldb" / ".gitignore").read_bytes() == (
        b"pending.json\nwriter.lock\n*.tmp\n"
    )


def test_batch_and_delete_publish_exactly_once(tmp_path):
    db = FolderDB(str(tmp_path))
    initial = db.load_catalog_snapshot()
    db.upsert_dicts({
        "alpha": {"a": {"value": 1}},
        "beta": {"b": {"value": 2}},
    })
    batch = db.load_catalog_snapshot()
    assert batch.revision == initial.revision + 1
    assert set(batch.entries) == {"alpha", "beta"}

    db.delete_range(["alpha", "beta"], "a", "z")
    deleted_range = db.load_catalog_snapshot()
    assert deleted_range.revision == batch.revision + 1
    assert deleted_range.entries["alpha"].count == 0
    assert deleted_range.entries["beta"].count == 0

    db.delete_file("alpha")
    removed = db.load_catalog_snapshot()
    assert removed.revision == deleted_range.revision + 1
    assert set(removed.entries) == {"beta"}

    db.delete_dbmeta("beta")
    metadata_deleted = db.load_catalog_snapshot()
    assert metadata_deleted.revision == removed.revision + 1
    assert dict(metadata_deleted.entries) == {}
    assert db.get_dbmeta()["beta"]["count"] == 0


def test_abandoned_ticker_pending_repairs_only_declared_ticker(monkeypatch, tmp_path):
    db = FolderDB(str(tmp_path))
    db.upsert_dicts({
        "alpha": {"a": {"value": 1}},
        "beta": {"b": {"value": 2}},
    })
    base = db.load_catalog_snapshot()
    state = PendingState(
        str(uuid.uuid4()), base.catalog_id, base.revision,
        base.revision + 1, "tickers", ("alpha",),
    )
    atomic_write(db.pending_path, pending_envelope(state))
    jsonlfile.update_jsonl(db._get_file_path("alpha"), {"c": {"value": 3}})

    loaded = []
    real_load = jsonlfile.load_index

    def record(path):
        loaded.append(os.path.basename(path))
        return real_load(path)

    monkeypatch.setattr(jsonlfile, "load_index", record)
    recovered = FolderDB(str(tmp_path)).load_catalog_snapshot()
    assert recovered.revision == base.revision + 1
    assert recovered.last_transaction_id == state.transaction_id
    assert recovered.entries["alpha"].count == 2
    assert loaded == ["alpha.jsonl"]
    assert not os.path.exists(db.pending_path)


def test_committed_pending_is_cleanup_only(tmp_path):
    db = FolderDB(str(tmp_path))
    db.upsert_dict("ticker", {"a": {"value": 1}})
    committed = db.load_catalog_snapshot()
    state = PendingState(
        committed.last_transaction_id, committed.catalog_id,
        committed.revision - 1, committed.revision, "tickers", ("ticker",),
    )
    atomic_write(db.pending_path, pending_envelope(state))
    recovered = FolderDB(str(tmp_path)).load_catalog_snapshot()
    assert recovered.catalog_id == committed.catalog_id
    assert recovered.revision == committed.revision
    assert recovered.entries == committed.entries
    assert not os.path.exists(db.pending_path)


def test_failure_before_catalog_commit_recovers_affected_owner(monkeypatch, tmp_path):
    db = FolderDB(str(tmp_path))
    db.upsert_dict("ticker", {"a": {"value": 1}})
    before = db.load_catalog_snapshot()
    real_publish = FolderDB._publish_catalog

    def fail_publish(self, *args, **kwargs):
        raise OSError("injected pre-commit failure")

    monkeypatch.setattr(FolderDB, "_publish_catalog", fail_publish)
    with pytest.raises(OSError, match="pre-commit"):
        db.upsert_dict("ticker", {"b": {"value": 2}})
    assert os.path.exists(db.pending_path)
    monkeypatch.setattr(FolderDB, "_publish_catalog", real_publish)

    recovered = FolderDB(str(tmp_path)).load_catalog_snapshot()
    assert recovered.catalog_id == before.catalog_id
    assert recovered.revision == before.revision + 1
    assert recovered.entries["ticker"].count == 2
    assert not os.path.exists(db.pending_path)


def test_failure_after_catalog_commit_is_cleanup_only(monkeypatch, tmp_path):
    import jsonldb.folderdb as folderdb_module

    db = FolderDB(str(tmp_path))
    db.upsert_dict("ticker", {"a": {"value": 1}})
    before = db.load_catalog_snapshot()
    real_clear = folderdb_module.clear_pending

    def fail_clear(path):
        raise CatalogFilesystemError("injected cleanup failure")

    monkeypatch.setattr(folderdb_module, "clear_pending", fail_clear)
    with pytest.raises(CatalogFilesystemError, match="cleanup"):
        db.upsert_dict("ticker", {"b": {"value": 2}})
    assert os.path.exists(db.pending_path)
    monkeypatch.setattr(folderdb_module, "clear_pending", real_clear)

    recovered = FolderDB(str(tmp_path)).load_catalog_snapshot()
    assert recovered.catalog_id == before.catalog_id
    assert recovered.revision == before.revision + 1
    assert recovered.entries["ticker"].count == 2
    assert not os.path.exists(db.pending_path)


def test_malformed_pending_forces_full_reconciliation(monkeypatch, tmp_path):
    db = FolderDB(str(tmp_path))
    db.upsert_dicts({
        "alpha": {"a": {"value": 1}},
        "beta": {"b": {"value": 2}},
    })
    before = db.load_catalog_snapshot()
    (tmp_path / ".jsonldb" / "pending.json").write_bytes(b"not-json")
    walks = []
    real_walk = FolderDB.get_file_list

    def record(self):
        walks.append(True)
        return real_walk(self)

    monkeypatch.setattr(FolderDB, "get_file_list", record)
    recovered = FolderDB(str(tmp_path)).load_catalog_snapshot()
    assert walks == [True]
    assert recovered.catalog_id != before.catalog_id
    assert recovered.revision == 1


def test_newer_catalog_is_preserved_fail_closed(tmp_path):
    db = FolderDB(str(tmp_path))
    db.load_catalog_snapshot()
    path = tmp_path / ".jsonldb" / "catalog.json"
    raw = orjson.loads(path.read_bytes())
    raw["version"] = 2
    path.write_bytes(orjson.dumps(raw, option=orjson.OPT_SORT_KEYS))
    before = path.read_bytes()

    with pytest.raises(UnsupportedCatalogVersionError):
        FolderDB(str(tmp_path)).load_catalog_snapshot()
    assert path.read_bytes() == before


def test_pending_with_live_writer_has_bounded_contention(tmp_path):
    db = FolderDB(str(tmp_path))
    base = db.load_catalog_snapshot()
    state = PendingState(
        str(uuid.uuid4()), base.catalog_id, base.revision,
        base.revision + 1, "full", (),
    )
    atomic_write(db.pending_path, pending_envelope(state))
    with WriterLock(db.writer_lock_path, 0.1):
        with pytest.raises(CatalogBusyError):
            FolderDB(str(tmp_path)).load_catalog_snapshot(timeout_seconds=0.02)


def test_cross_process_writer_contention_and_abandonment_recovery(tmp_path):
    db = FolderDB(str(tmp_path))
    base = db.load_catalog_snapshot()
    state = PendingState(
        str(uuid.uuid4()), base.catalog_id, base.revision,
        base.revision + 1, "full", (),
    )
    context = multiprocessing.get_context("spawn")
    ready = context.Queue()
    release = context.Event()
    process = context.Process(target=_hold_writer_process, args=(
        db.writer_lock_path, db.pending_path, pending_envelope(state), ready, release,
    ))
    process.start()
    try:
        assert ready.get(timeout=3.0) is True
        with pytest.raises(CatalogBusyError):
            FolderDB(str(tmp_path)).load_catalog_snapshot(timeout_seconds=0.03)
    finally:
        release.set()
        process.join(timeout=3.0)
        if process.is_alive():
            process.terminate()
            process.join(timeout=1.0)
    assert process.exitcode == 0

    recovered = FolderDB(str(tmp_path)).load_catalog_snapshot()
    assert recovered.catalog_id == base.catalog_id
    assert recovered.revision == base.revision + 1
    assert not os.path.exists(db.pending_path)


def test_clear_preserves_control_namespace_and_publishes_empty_revision(tmp_path):
    db = FolderDB(str(tmp_path))
    db.upsert_dict("ticker", {"a": {"value": 1}})
    before = db.load_catalog_snapshot()
    db.clear_folder(force=True)
    after = db.load_catalog_snapshot()

    assert after.catalog_id == before.catalog_id
    assert after.revision == before.revision + 1
    assert dict(after.entries) == {}
    assert os.path.isfile(db.catalog_path)
    assert os.path.isfile(db.control_ignore_path)
    assert os.path.isfile(db.dbmeta_path)


def test_managed_commit_recovers_without_incrementing_revision(monkeypatch, tmp_path):
    db = FolderDB(str(tmp_path))
    db.upsert_dict("ticker", {"a": {"value": 1}})
    before = db.load_catalog_snapshot()
    observed = []
    fake = types.ModuleType("jsonldb.vercontrol")
    fake.is_versioned = lambda path: True
    fake.init_folder = lambda path: None

    def commit(path, message):
        observed.append((path, message, os.path.exists(db.pending_path)))

    fake.commit = commit
    monkeypatch.setitem(sys.modules, "jsonldb.vercontrol", fake)
    db.commit("stable")
    after = db.load_catalog_snapshot()

    assert observed == [(str(tmp_path), "stable", False)]
    assert after.catalog_id == before.catalog_id
    assert after.revision == before.revision


def test_managed_revert_preserves_matching_historical_catalog(monkeypatch, tmp_path):
    db = FolderDB(str(tmp_path))
    db.upsert_dict("ticker", {"a": {"value": 1}})
    historical = db.load_catalog_snapshot()
    owner = db._get_file_path("ticker")
    restored_bytes = {
        owner: open(owner, "rb").read(),
        owner + ".idx": open(owner + ".idx", "rb").read(),
        db.catalog_path: open(db.catalog_path, "rb").read(),
    }
    db.upsert_dict("ticker", {"b": {"value": 2}})
    db.write_aux("ticker", b"derived")

    fake = types.ModuleType("jsonldb.vercontrol")

    def revert(path, revision):
        assert path == str(tmp_path)
        assert revision == "historical"
        assert not os.path.exists(owner + ".aux")
        for target, payload in restored_bytes.items():
            with open(target, "wb") as stream:
                stream.write(payload)

    fake.revert = revert
    monkeypatch.setitem(sys.modules, "jsonldb.vercontrol", fake)
    db.revert("historical")
    restored = db.load_catalog_snapshot()

    assert restored.catalog_id == historical.catalog_id
    assert restored.revision == historical.revision
    assert restored.entries["ticker"].count == 1
    assert not os.path.exists(db.pending_path)


def test_public_signatures_are_additive_and_legacy_projection_is_mutable(tmp_path):
    assert str(inspect.signature(FolderDB.__init__)) == (
        "(self, folder_path: str, hierarchy_depth: int = None)"
    )
    assert str(inspect.signature(FolderDB.load_catalog_snapshot)) == (
        "(self, timeout_seconds: float = 5.0) -> jsonldb.catalog.FolderCatalogSnapshot"
    )
    db = FolderDB(str(tmp_path))
    db.upsert_dict("ticker", {"a": {"value": 1}})
    metadata = db.get_dbmeta()
    metadata["local-only"] = {}
    assert "local-only" in metadata
    assert "local-only" not in db.load_catalog_snapshot().entries


def test_raw_owner_edit_requires_explicit_reconciliation(tmp_path):
    db = FolderDB(str(tmp_path))
    db.upsert_dict("ticker", {"a": {"value": 1}})
    managed = db.load_catalog_snapshot()
    jsonlfile.update_jsonl(db._get_file_path("ticker"), {"b": {"value": 2}})

    unchanged = db.load_catalog_snapshot()
    assert unchanged is managed
    assert unchanged.entries["ticker"].count == 1

    metadata = db.get_dbmeta()
    reconciled = db.load_catalog_snapshot()
    assert metadata["ticker"]["count"] == 2
    assert reconciled.entries["ticker"].count == 2
    assert reconciled.revision == managed.revision + 1
