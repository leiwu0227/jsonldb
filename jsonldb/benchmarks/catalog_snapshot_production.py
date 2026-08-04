#!/usr/bin/env python3
"""Synthetic production benchmark for FolderDB's revisioned catalog."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import importlib
import json
import os
from pathlib import Path
from statistics import median
import tempfile
import time
from typing import Any, Dict, Iterable, Iterator, List, Set, Tuple
from unittest import mock

import orjson

from jsonldb import FolderDB, jsonlfile
from jsonldb.catalog import invalidate_snapshot


DEFAULT_SIZES = (16, 64, 256)
DEFAULT_RECORDS_PER_TICKER = 4
DEFAULT_REPEATS = 100
DEFAULT_TRIALS = 5


def _records(ticker: int, count: int) -> Dict[str, Dict[str, int]]:
    return {
        "key_{:08d}".format(number): {"ticker": ticker, "value": number}
        for number in range(count)
    }


def build_fixture(root: Path, tickers: int, records_per_ticker: int) -> FolderDB:
    db = FolderDB(str(root))
    for number in range(tickers):
        jsonlfile.save_jsonl(
            str(root / "ticker_{:06d}.jsonl".format(number)),
            _records(number, records_per_ticker),
        )
    db.load_catalog_snapshot()
    return db


def _empty_counts() -> Dict[str, int]:
    return {
        "catalog_decodes": 0,
        "catalog_file_loads": 0,
        "cache_hits": 0,
        "owner_tree_walks": 0,
        "affected_ticker_index_loads": 0,
        "unaffected_ticker_index_loads": 0,
        "catalog_publications": 0,
        "pending_publications": 0,
        "projection_updates": 0,
    }


@contextmanager
def instrument(counts: Dict[str, int], affected: Set[str]) -> Iterator[None]:
    folderdb_module = importlib.import_module("jsonldb.folderdb")
    original_walk = FolderDB.get_file_list
    original_index = jsonlfile.load_index
    original_read = folderdb_module.read_object
    original_cached = folderdb_module.cached_snapshot
    original_atomic = folderdb_module.atomic_write
    original_projection = FolderDB._write_projection

    def counted_walk(self):
        counts["owner_tree_walks"] += 1
        return original_walk(self)

    def counted_index(owner_path):
        path = os.fspath(owner_path)
        if path.endswith(".jsonl"):
            name = os.path.basename(path)[:-6]
            key = (
                "affected_ticker_index_loads"
                if name in affected else "unaffected_ticker_index_loads"
            )
            counts[key] += 1
        return original_index(owner_path)

    def counted_read(path):
        if os.fspath(path).endswith(os.path.join(".jsonldb", "catalog.json")):
            counts["catalog_file_loads"] += 1
            counts["catalog_decodes"] += 1
        return original_read(path)

    def counted_cached(root, identity):
        value = original_cached(root, identity)
        if value is not None:
            counts["cache_hits"] += 1
        return value

    def counted_atomic(path, value):
        target = os.fspath(path)
        if target.endswith(os.path.join(".jsonldb", "catalog.json")):
            counts["catalog_publications"] += 1
        elif target.endswith(os.path.join(".jsonldb", "pending.json")):
            counts["pending_publications"] += 1
        return original_atomic(path, value)

    def counted_projection(self, entries):
        counts["projection_updates"] += 1
        return original_projection(self, entries)

    with mock.patch.object(FolderDB, "get_file_list", counted_walk), \
            mock.patch.object(jsonlfile, "load_index", counted_index), \
            mock.patch.object(folderdb_module, "read_object", counted_read), \
            mock.patch.object(folderdb_module, "cached_snapshot", counted_cached), \
            mock.patch.object(folderdb_module, "atomic_write", counted_atomic), \
            mock.patch.object(FolderDB, "_write_projection", counted_projection):
        yield


def _trial(db: FolderDB, operation, affected: Set[str]) -> Dict[str, Any]:
    counts = _empty_counts()
    with open(db.catalog_path, "rb") as stream:
        before = orjson.loads(stream.read())["revision"]
    started = time.perf_counter()
    with instrument(counts, affected):
        value = operation()
    elapsed = time.perf_counter() - started
    with open(db.catalog_path, "rb") as stream:
        after = orjson.loads(stream.read())["revision"]
    return {
        "elapsed_seconds": elapsed,
        "revision_before": before,
        "revision_after": after,
        "revision_delta": after - before,
        **counts,
        "value": value,
    }


def _summarize(trials: List[Dict[str, Any]]) -> Dict[str, Any]:
    result = {
        "median_seconds": median(item["elapsed_seconds"] for item in trials),
        "trials": len(trials),
    }
    for key in _empty_counts():
        values = [item[key] for item in trials]
        result[key] = values[0] if len(set(values)) == 1 else values
    deltas = [item["revision_delta"] for item in trials]
    result["revision_delta"] = deltas[0] if len(set(deltas)) == 1 else deltas
    result["revision_range"] = [
        trials[0]["revision_before"], trials[-1]["revision_after"]
    ]
    return result


def benchmark_size(
    tickers: int,
    records_per_ticker: int = DEFAULT_RECORDS_PER_TICKER,
    repeats: int = DEFAULT_REPEATS,
    trials: int = DEFAULT_TRIALS,
) -> Dict[str, Any]:
    if tickers < 2 or records_per_ticker < 1 or repeats < 2 or trials < 1:
        raise ValueError("benchmark dimensions must be tickers>=2, records>=1, repeats>=2, trials>=1")
    with tempfile.TemporaryDirectory(prefix="jsonldb-catalog-production-") as tmp:
        db = build_fixture(Path(tmp), tickers, records_per_ticker)

        cold = []
        for _ in range(trials):
            invalidate_snapshot(db._catalog_root)
            cold.append(_trial(db, db.load_catalog_snapshot, set()))

        reuse = []
        for _ in range(trials):
            invalidate_snapshot(db._catalog_root)

            def repeated():
                first = db.load_catalog_snapshot()
                last = first
                for _ in range(repeats - 1):
                    last = db.load_catalog_snapshot()
                return first is last

            reuse.append(_trial(db, repeated, set()))

        reconciliation = [
            _trial(db, db.get_dbmeta, set()) for _ in range(trials)
        ]

        one_ticker = []
        for trial_number in range(trials):
            key = "mutation_{:08d}".format(trial_number)
            one_ticker.append(_trial(
                db,
                lambda key=key: db.upsert_dict(
                    "ticker_000000", {key: {"value": trial_number}}
                ),
                {"ticker_000000"},
            ))

        batch = []
        for trial_number in range(trials):
            key = "batch_{:08d}".format(trial_number)
            batch.append(_trial(
                db,
                lambda key=key: db.upsert_dicts({
                    "ticker_000000": {key: {"value": trial_number}},
                    "ticker_000001": {key: {"value": trial_number}},
                }),
                {"ticker_000000", "ticker_000001"},
            ))

        operations = {
            "production_cold_snapshot": _summarize(cold),
            "unchanged_snapshot_reuse": _summarize(reuse),
            "full_reconciliation": _summarize(reconciliation),
            "one_ticker_mutation": _summarize(one_ticker),
            "batch_mutation": _summarize(batch),
        }
        operations["unchanged_snapshot_reuse"]["reads_per_trial"] = repeats
        speedup = (
            operations["full_reconciliation"]["median_seconds"]
            / operations["production_cold_snapshot"]["median_seconds"]
        )
        return {
            "tickers": tickers,
            "records_per_ticker": records_per_ticker,
            "operations": operations,
            "cold_vs_reconciliation_speedup": speedup,
        }


def run_scaling_benchmark(
    sizes: Iterable[int] = DEFAULT_SIZES,
    records_per_ticker: int = DEFAULT_RECORDS_PER_TICKER,
    repeats: int = DEFAULT_REPEATS,
    trials: int = DEFAULT_TRIALS,
) -> Dict[str, Any]:
    results = [
        benchmark_size(size, records_per_ticker, repeats, trials)
        for size in sizes
    ]
    reference = next((item for item in results if item["tickers"] == 256), None)
    return {
        "version": 1,
        "sizes": results,
        "reference_256_gate": None if reference is None else {
            "minimum_speedup": 5.0,
            "measured_speedup": reference["cold_vs_reconciliation_speedup"],
            "passed": reference["cold_vs_reconciliation_speedup"] >= 5.0,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sizes", type=int, nargs="+", default=list(DEFAULT_SIZES))
    parser.add_argument("--records-per-ticker", type=int, default=DEFAULT_RECORDS_PER_TICKER)
    parser.add_argument("--repeats", type=int, default=DEFAULT_REPEATS)
    parser.add_argument("--trials", type=int, default=DEFAULT_TRIALS)
    args = parser.parse_args()
    print(json.dumps(run_scaling_benchmark(
        args.sizes, args.records_per_ticker, args.repeats, args.trials
    ), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
