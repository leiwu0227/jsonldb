from jsonldb.benchmarks.catalog_snapshot_production import (
    benchmark_size,
    run_scaling_benchmark,
)


def test_production_benchmark_reports_structural_mutation_and_cache_counts():
    result = benchmark_size(8, records_per_ticker=2, repeats=5, trials=2)
    operations = result["operations"]
    cold = operations["production_cold_snapshot"]
    assert cold["catalog_file_loads"] == 1
    assert cold["catalog_decodes"] == 1
    assert cold["owner_tree_walks"] == 0
    assert cold["affected_ticker_index_loads"] == 0
    assert cold["unaffected_ticker_index_loads"] == 0
    assert cold["affected_companion_identity_loads"] == 0
    assert cold["unaffected_companion_identity_loads"] == 0

    reuse = operations["unchanged_snapshot_reuse"]
    assert reuse["catalog_file_loads"] == 1
    assert reuse["catalog_decodes"] == 1
    assert reuse["cache_hits"] >= 4
    assert reuse["owner_tree_walks"] == 0

    reconciliation = operations["full_reconciliation"]
    assert reconciliation["owner_tree_walks"] == 1
    assert reconciliation["unaffected_ticker_index_loads"] == 8
    assert reconciliation["unaffected_companion_identity_loads"] == 8
    assert reconciliation["catalog_publications"] == 1
    assert reconciliation["revision_delta"] == 1

    mutation = operations["one_ticker_mutation"]
    assert mutation["owner_tree_walks"] == 0
    assert mutation["affected_ticker_index_loads"] >= 1
    assert mutation["unaffected_ticker_index_loads"] == 0
    assert mutation["affected_companion_identity_loads"] == 1
    assert mutation["unaffected_companion_identity_loads"] == 0
    assert mutation["catalog_publications"] == 1
    assert mutation["pending_publications"] == 1
    assert mutation["projection_updates"] == 1
    assert mutation["revision_delta"] == 1

    batch = operations["batch_mutation"]
    assert batch["owner_tree_walks"] == 0
    assert batch["unaffected_ticker_index_loads"] == 0
    assert batch["catalog_publications"] == 1
    assert batch["pending_publications"] == 1
    assert batch["projection_updates"] == 1
    assert batch["revision_delta"] == 1
    assert batch["affected_companion_identity_loads"] == 2
    assert batch["unaffected_companion_identity_loads"] == 0

    aux = operations["one_ticker_aux_publication"]
    assert aux["affected_companion_identity_loads"] == 1
    assert aux["unaffected_companion_identity_loads"] == 0
    assert aux["unaffected_ticker_index_loads"] == 0
    assert aux["revision_delta"] == 1


def test_scaling_report_covers_requested_sizes():
    result = run_scaling_benchmark((4, 8), records_per_ticker=1, repeats=3, trials=1)
    assert [item["tickers"] for item in result["sizes"]] == [4, 8]
    assert result["reference_256_gate"] is None
    assert set(result["aux_hash_costs"]) == {"absent", "empty", "small", "larger"}
    assert result["aux_hash_costs"]["empty"]["identity_size"] == 0
    assert result["aux_hash_costs"]["larger"]["identity_size"] == 1024 * 1024
