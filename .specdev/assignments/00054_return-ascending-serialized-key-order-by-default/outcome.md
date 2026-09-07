# Outcome

## Delivered behavior

Full reads now return ascending serialized-key order by default. They still scan all physical rows, apply strict/permissive error handling, and preserve last valid physical values. Original spellings retained for converted datetime keys preserve lexical ordering across mixed key types and conversion collisions. An adjacent-key check returns the existing dictionary when ordered; disordered results reuse record values in a reordered dictionary.

Indexed point, bounded, and one-sided reads remain unchanged. FolderDB, DataFrame, and metadata-plus-rows wrappers inherit the result order. Public documentation identifies the intentional default ordering change and its costs. The former physical-order assertion in strict-read tests now expects logical order.

## Deviations

None. Bounded comment/docstring simplification offsets new code within the existing 950-line file-store cap; no published design or dependency changed.

## Unresolved risks

Sorting costs remain layout-dependent. One-million-row date-key medians increased by approximately 7% sorted, 31% backfilled, and 68% shuffled. Datetime conversion requires retaining original serialized spellings, increasing peak memory even on ordered results; the adjacent-key check itself uses constant extra space. See implementation/performance.md for dataset/runtime limits and measured peaks. Full reads deliberately stop exposing physical encounter order. Existing indexed strict omission and concurrency limitations remain. Distinct serialized spellings that deserialize to one datetime retain existing full-versus-indexed collision differences: full reads keep the physical last winner and its spelling, whereas indexed traversal collapses in lexical order. Usage documentation discloses this boundary.

| Acceptance | Evidence | Result |
| --- | --- | --- |
| AC-1 | 558 focused tests passed; 41 ordering cases passed again after adding stronger mixed-type alias-spelling assertions. Coverage includes full/bounded/one-sided wrapper parity, duplicate and conversion-collision winners, fields/nulls/nesting, metadata, missing/empty/single-row behavior, and preserved table-name order. | Passed |
| AC-2 | Strict tests prove full reads reject physical damage omitted from warm indexes, while indexed point/range/either one-sided bound bypass the full-read ordering helper. Table byte snapshots remain unchanged. Existing strict/cache/metadata/timezone cases pass. | Passed |
| AC-3 | Fast-path identity/no-sort checks and inversion/value-sharing tests pass. Sixty fresh-process baseline/final trials record elapsed time and peak RSS for sorted/backfilled/shuffled date and datetime workloads, validate returned values and order, and preserve table hashes. Public docs and static cap/Python 3.8/diff checks pass. | Passed |
