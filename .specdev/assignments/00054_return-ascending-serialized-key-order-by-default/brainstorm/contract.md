# Assignment contract

Kind: change

## Objective and context

Implement the published [Ordered Reads](../../../project_notes/roadmap/designs/jsonl_file_store/ordered_reads.md) design from commit `a55e844`: every table's row results use ascending serialized-key order by default, independently of physical layout and supplied bounds.

The published strict-read contract and Assignment 00053's delivered behavior remain authoritative for encountered-error handling. The earlier ordered-read prototype in `.specdev/cache/ordered-read-experiment/` is supporting performance context only; implementation measurements must exercise the final code.

## Scope and non-goals

Change full-read result ordering and preserve it through single-file dictionary/DataFrame reads, FolderDB getters, and metadata-plus-rows variants. Update public documentation and affected regression expectations for this intentional default change.

No ordering flag, physical-order mode, persistence format, sorted-state cache, automatic lint, index-completeness verification, dependency change, or OceanData implementation. Do not impose ordering on table names, record fields, or DataFrame columns.

## Expected behavior

Full reads, reached when both bounds are omitted, continue scanning every physical observation and applying the existing strict/permissive policy. Reconstruct logical observations before ordering; the last valid physical occurrence of a duplicate serialized key still wins. Preserve existing selected values, field presence, nulls, metadata ordering, key conversion, missing/empty results, and filesystem error behavior.

Compare logical keys using serialized lexical order, including when results contain deserialized datetime keys or string fallbacks. Already ascending results use an O(N) adjacent-key check with O(1) additional checking space and require no sorting or reconstruction solely for ordering. At the first inversion, stop checking and order the logical result, reusing record values without deep copies.

Any supplied bound retains the indexed path. Start-only and end-only ranges remain inclusive and ascending; equal bounds and point reads remain single-key lookups. These paths add no whole-table ordering check, integrity scan, or redundant result sort. Wrappers inherit ordering without sorting independently.

## Important decisions

Consistent default ordering intentionally replaces physical encounter order for full reads; public documentation must identify that change. There is no opt-out flag. Numeric-looking strings retain lexical order, so "10" precedes "2".

A full strict read must still fail on malformed physical observations excluded from an existing index. Never obtain ordered full results by substituting an indexed selection. Indexed strict reads retain their selected-observation guarantee and accepted omission boundary.

## Constraints and invariants

Reads do not change observation bytes or invoke lint. Existing index loading, recovery, and cache behavior remain unchanged. Preserve Python 3.8 compatibility and source caps: `jsonlfile.py` 950, `jsonldf.py` 150, `folderdb.py` 1250 total lines. Bounded simplification within existing modules is allowed to meet caps. Published designs and unrelated working-tree artifacts remain untouched.

## Delegated and reserved authority

- Delegated after approval: implementation, bounded simplification, documentation, focused tests and performance measurements, required independent implementation review, in-scope repairs, and final delivery commit.
- Reserved: changing the approved ordering/strictness guarantees, adding public options or dependencies, raising source caps, editing published designs, or running the full suite.

## Risks and assumptions

Disordered full results require general-case O(N log N) key comparisons and O(N) additional references/dictionary entries. Ordered results still incur the linear check. Actual elapsed time and peak memory depend on layout, row size, key conversion, and runtime; the published design sets no universal threshold. The prototype used date strings without deserialization, so final evidence must also cover key-conversion behavior. Neither ordering nor strictness adds a concurrent-read snapshot guarantee.

## Verification authority

Approval authorizes focused ordering, strict-read, cache, dictionary/DataFrame, FolderDB, metadata, and key-conversion regressions; static source-cap/Python 3.8/diff checks; and reproducible local before/after full-read measurements for already sorted, lightly backfilled, and shuffled representative tables. Measure elapsed time and peak memory, disclose dataset sizes/runtime/cache conditions, and preserve raw measurements plus a concise comparison. No full-suite authorization is implied. Independent implementation review is required using the configured reviewer.

## Acceptance criteria

- AC-1: Full and equivalent bounded/one-sided reads return identical ascending serialized-key order through dictionary, DataFrame, and metadata-plus-rows APIs. Duplicate winners, key conversion, values/nulls/field presence, metadata, missing/empty/single-row behavior, and per-table results remain correct.
- AC-2: Full strict reads still encounter and reject physical damage omitted from an index; permissive behavior and indexed strict scope remain unchanged. Reads preserve observation bytes, and indexed/wrapper paths add no full-table ordering checks or redundant result sorting.
- AC-3: Already ordered full results bypass sorting/reconstruction solely for ordering; disordered results sort logical keys without deep-copying values. Focused evidence verifies both paths, and a reproducible final-code comparison reports full-read elapsed time and peak memory against the current implementation for sorted, lightly backfilled, and shuffled layouts. Public docs describe the new default and its costs.
