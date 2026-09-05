# Implementation plan

**Implementation Guides:** [api-security]
**Review Guides:** [api-security]

## Tasks

## T-1 — Baseline and bounded implementation (AC-1, AC-2, AC-3)

Capture repeated point/range, cold, mixed-write and memory-pressure baselines before product changes with profile_test/benchmark_index_cache.py. Implement a private LRU in jsonldb/jsonlfile.py, a shared snapshot-producing loader, per-hit fingerprints, guarded admission and pre-mutation invalidation. Retain private mutable ownership and lazy immutable range keys. Shorten repetitive docstrings to keep the existing 950-line cap without compressing executable logic. No FolderDB cache hooks or new product module.

## T-2 — Behavioral and failure evidence (AC-1, AC-2, AC-3)

Add focused unit_tests/test_index_cache.py cases for reuse, public dictionary isolation, normal and damaged files, slot/datatype behavior, mutations and interrupted writes, stable recovery, replacement/invalidation races, LRU accounting, oversized entries and concurrent readers. Exercise FolderDB/DataFrame callers and folder deletion/movement. Run the relevant existing JSONL, slot, lint, report and integration tests; never the whole suite.

## T-3 — Performance and delivery (AC-4)

Expose the repeatable benchmark through profile_test/benchmark.py --index-cache. Compare baseline and final medians, preserve samples in ignored cache, and write durable summarized evidence. Calibrate the default using owned-object accounting and cold/pressure results; return to the user for any unresolved material tradeoff. Document current behavior in README, write progress/outcome/result and request required independent implementation review through specdev implement.

## Context selection

Published index-cache/integrity notes and approved contract are authoritative. D00003's exploratory results motivate the benchmark but do not substitute for final evidence. Historical lint-performance and WSL filesystem notes are leads only: this host is macOS, and current code already distinguishes ordinary and forced lint. Preserve coarse-time compatibility, recovery and lint checks. Frontend guidance is inapplicable; API-security guidance applies narrowly to mutable ownership and filesystem trust, with no dependency changes.
