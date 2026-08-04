#!/usr/bin/env python
"""Synthetic baseline for the proposed immutable FolderDB catalog.

This benchmark does not implement or call a production snapshot API. It compares
current reconciliation/update boundaries with a compact-envelope measurement
model and reports structural call counts alongside descriptive wall time.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass
import json
import os
from pathlib import Path
import tempfile
import time
from types import MappingProxyType
from typing import Any, Iterator, Mapping, Optional
from unittest import mock
import uuid

import orjson

from jsonldb.folderdb import FolderDB
from jsonldb import jsonlfile


DEFAULT_TICKERS = 256
DEFAULT_RECORDS_PER_TICKER = 4
DEFAULT_REPEATS = 1_000


@dataclass
class StructuralCounts:
    owner_tree_walks: int = 0
    ticker_index_loads: int = 0
    catalog_file_loads: int = 0
    cache_hits: int = 0


@dataclass(frozen=True)
class BaselineEntry:
    min_index: Optional[str]
    max_index: Optional[str]
    count: int
    size: int
    linted: bool
    lint_time: str


@dataclass(frozen=True)
class BaselineSnapshot:
    catalog_id: str
    revision: int
    file_identity: tuple[int, int, int, int, int]
    entries: Mapping[str, BaselineEntry]


class CompactCatalogReader:
    """Small measurement model for stable-file identity and immutable reuse."""

    def __init__(self, catalog_path: Path, counts: StructuralCounts):
        self.catalog_path = catalog_path
        self.counts = counts
        self._snapshot: Optional[BaselineSnapshot] = None

    @staticmethod
    def _identity(stat_result: os.stat_result) -> tuple[int, int, int, int, int]:
        return (
            stat_result.st_dev,
            stat_result.st_ino,
            stat_result.st_size,
            stat_result.st_mtime_ns,
            stat_result.st_ctime_ns,
        )

    def load(self) -> BaselineSnapshot:
        identity = self._identity(self.catalog_path.stat())
        if self._snapshot is not None and self._snapshot.file_identity == identity:
            self.counts.cache_hits += 1
            return self._snapshot

        self.counts.catalog_file_loads += 1
        payload = orjson.loads(self.catalog_path.read_bytes())
        after = self._identity(self.catalog_path.stat())
        if after != identity:
            raise RuntimeError("synthetic catalog changed during benchmark read")

        entries = {
            name: BaselineEntry(**entry)
            for name, entry in payload["entries"].items()
        }
        snapshot = BaselineSnapshot(
            catalog_id=payload["catalog_id"],
            revision=payload["revision"],
            file_identity=identity,
            entries=MappingProxyType(entries),
        )
        self._snapshot = snapshot
        return snapshot


@contextmanager
def instrument_current_paths(counts: StructuralCounts) -> Iterator[None]:
    """Count current owner discovery and per-ticker index-load boundaries."""

    original_get_file_list = FolderDB.get_file_list
    original_load_index = jsonlfile.load_index

    def counted_get_file_list(self: FolderDB) -> list[str]:
        counts.owner_tree_walks += 1
        return original_get_file_list(self)

    def counted_load_index(owner_path: str) -> dict[str, Any]:
        if os.fspath(owner_path).endswith(".jsonl"):
            counts.ticker_index_loads += 1
        return original_load_index(owner_path)

    with mock.patch.object(FolderDB, "get_file_list", counted_get_file_list), \
            mock.patch.object(jsonlfile, "load_index", counted_load_index):
        yield


def _elapsed(operation) -> tuple[float, Any]:
    started = time.perf_counter()
    value = operation()
    return time.perf_counter() - started, value


def _operation_result(elapsed: float, counts: StructuralCounts, **extra: Any) -> dict[str, Any]:
    return {
        "elapsed_seconds": elapsed,
        "owner_tree_walks": counts.owner_tree_walks,
        "ticker_index_loads": counts.ticker_index_loads,
        "catalog_file_loads": counts.catalog_file_loads,
        "cache_hits": counts.cache_hits,
        **extra,
    }


def _make_records(ticker_number: int, count: int) -> dict[str, dict[str, int]]:
    return {
        f"key_{record_number:08d}": {
            "ticker_number": ticker_number,
            "value": record_number,
        }
        for record_number in range(count)
    }


def build_fixture(root: Path, tickers: int, records_per_ticker: int) -> tuple[FolderDB, Path]:
    """Build deterministic owners, current metadata, and a compact envelope."""

    db = FolderDB(str(root))
    for ticker_number in range(tickers):
        owner = root / f"ticker_{ticker_number:06d}.jsonl"
        jsonlfile.save_jsonl(
            str(owner),
            _make_records(ticker_number, records_per_ticker),
        )
    db.build_dbmeta()

    current = jsonlfile.load_jsonl(db.dbmeta_path, auto_deserialize=False)
    entries = {
        name: {
            "min_index": entry["min_index"],
            "max_index": entry["max_index"],
            "count": entry["count"],
            "size": entry["size"],
            "linted": entry["linted"],
            "lint_time": entry["lint_time"],
        }
        for name, entry in sorted(current.items())
    }
    control = root / ".jsonldb"
    control.mkdir(exist_ok=True)
    catalog_path = control / "catalog.json"
    catalog_path.write_bytes(orjson.dumps({
        "schema": "jsonldb.folder-catalog",
        "version": 1,
        "catalog_id": str(uuid.uuid4()),
        "revision": 1,
        "last_transaction_id": str(uuid.uuid4()),
        "timespec": db.timespec,
        "entries": entries,
    }, option=orjson.OPT_SORT_KEYS))
    return db, catalog_path


def run_benchmark(
    *,
    tickers: int = DEFAULT_TICKERS,
    records_per_ticker: int = DEFAULT_RECORDS_PER_TICKER,
    repeats: int = DEFAULT_REPEATS,
) -> dict[str, Any]:
    if tickers < 2:
        raise ValueError("tickers must be at least 2 to expose scaling")
    if records_per_ticker < 1:
        raise ValueError("records_per_ticker must be positive")
    if repeats < 2:
        raise ValueError("repeats must be at least 2 to measure reuse")

    with tempfile.TemporaryDirectory(prefix="jsonldb-catalog-benchmark-") as tmp:
        db, catalog_path = build_fixture(
            Path(tmp), tickers, records_per_ticker
        )

        compact_counts = StructuralCounts()
        compact_reader = CompactCatalogReader(catalog_path, compact_counts)
        compact_elapsed, compact_snapshot = _elapsed(compact_reader.load)

        reconcile_counts = StructuralCounts()
        with instrument_current_paths(reconcile_counts):
            reconcile_elapsed, reconciled = _elapsed(db.get_dbmeta)

        update_counts = StructuralCounts()
        with instrument_current_paths(update_counts):
            update_elapsed, _ = _elapsed(
                lambda: db.update_dbmeta("ticker_000000")
            )

        repeated_counts = StructuralCounts()
        repeated_reader = CompactCatalogReader(catalog_path, repeated_counts)

        def repeated_loads() -> tuple[BaselineSnapshot, BaselineSnapshot]:
            first = repeated_reader.load()
            last = first
            for _ in range(repeats - 1):
                last = repeated_reader.load()
            return first, last

        repeated_elapsed, (first, last) = _elapsed(repeated_loads)

        return {
            "fixture": {
                "tickers": tickers,
                "records_per_ticker": records_per_ticker,
                "repeated_reads": repeats,
            },
            "operations": {
                "compact_catalog_load": _operation_result(
                    compact_elapsed,
                    compact_counts,
                    entries=len(compact_snapshot.entries),
                ),
                "full_reconciliation": _operation_result(
                    reconcile_elapsed,
                    reconcile_counts,
                    entries=len(reconciled),
                ),
                "one_entry_metadata_update": _operation_result(
                    update_elapsed,
                    update_counts,
                    entries_updated=1,
                ),
                "repeated_unchanged_catalog_reads": _operation_result(
                    repeated_elapsed,
                    repeated_counts,
                    reads=repeats,
                    snapshot_reused=first is last,
                ),
            },
            "structural_target": {
                "healthy_snapshot_owner_tree_walks": 0,
                "healthy_snapshot_ticker_index_loads": 0,
            },
        }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tickers", type=int, default=DEFAULT_TICKERS)
    parser.add_argument(
        "--records-per-ticker",
        type=int,
        default=DEFAULT_RECORDS_PER_TICKER,
    )
    parser.add_argument("--repeats", type=int, default=DEFAULT_REPEATS)
    args = parser.parse_args()
    print(json.dumps(run_benchmark(
        tickers=args.tickers,
        records_per_ticker=args.records_per_ticker,
        repeats=args.repeats,
    ), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
