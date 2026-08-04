from pathlib import Path
import tempfile

import pytest

from jsonldb.benchmarks.catalog_snapshot_baseline import (
    CompactCatalogReader,
    DEFAULT_RECORDS_PER_TICKER,
    DEFAULT_REPEATS,
    DEFAULT_TICKERS,
    StructuralCounts,
    build_fixture,
    run_benchmark,
)
from jsonldb.folderdb import FolderDB
from jsonldb import jsonlfile


def test_default_fixture_is_large_and_reproducible():
    assert DEFAULT_TICKERS >= 256
    assert DEFAULT_RECORDS_PER_TICKER > 0
    assert DEFAULT_REPEATS >= 100


def test_all_phases_report_timing_and_expected_structural_counts():
    original_get_file_list = FolderDB.get_file_list
    original_load_index = jsonlfile.load_index

    result = run_benchmark(tickers=12, records_per_ticker=3, repeats=7)

    assert result["fixture"] == {
        "tickers": 12,
        "records_per_ticker": 3,
        "repeated_reads": 7,
    }
    operations = result["operations"]
    assert set(operations) == {
        "compact_catalog_load",
        "full_reconciliation",
        "one_entry_metadata_update",
        "repeated_unchanged_catalog_reads",
    }
    assert all(operation["elapsed_seconds"] >= 0 for operation in operations.values())

    compact = operations["compact_catalog_load"]
    assert compact["entries"] == 12
    assert compact["owner_tree_walks"] == 0
    assert compact["ticker_index_loads"] == 0
    assert compact["catalog_file_loads"] == 1

    reconciliation = operations["full_reconciliation"]
    assert reconciliation["entries"] == 12
    assert reconciliation["owner_tree_walks"] == 1
    assert reconciliation["ticker_index_loads"] == 12

    update = operations["one_entry_metadata_update"]
    assert update["entries_updated"] == 1
    assert update["owner_tree_walks"] == 0
    assert update["ticker_index_loads"] == 1

    repeated = operations["repeated_unchanged_catalog_reads"]
    assert repeated["reads"] == 7
    assert repeated["owner_tree_walks"] == 0
    assert repeated["ticker_index_loads"] == 0
    assert repeated["catalog_file_loads"] == 1
    assert repeated["cache_hits"] == 6
    assert repeated["snapshot_reused"] is True
    assert result["structural_target"] == {
        "healthy_snapshot_owner_tree_walks": 0,
        "healthy_snapshot_ticker_index_loads": 0,
    }

    assert FolderDB.get_file_list is original_get_file_list
    assert jsonlfile.load_index is original_load_index


def test_measurement_snapshot_entries_are_immutable_and_reused():
    with tempfile.TemporaryDirectory(prefix="jsonldb-catalog-test-") as tmp:
        _, catalog_path = build_fixture(Path(tmp), tickers=2, records_per_ticker=1)
        counts = StructuralCounts()
        reader = CompactCatalogReader(catalog_path, counts)

        first = reader.load()
        second = reader.load()

        assert first is second
        assert counts.catalog_file_loads == 1
        assert counts.cache_hits == 1
        with pytest.raises(TypeError):
            first.entries["new"] = first.entries["ticker_000000"]
