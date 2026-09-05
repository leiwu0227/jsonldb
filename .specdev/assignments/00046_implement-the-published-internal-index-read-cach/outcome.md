# Outcome

## Delivered behavior

Private process-local index reuse accelerates point/range reads while preserving public ownership, recovery, slot and lint behavior. Files are validated on every hit; stable loads share the authoritative recovery path, writers invalidate before mutation, and concurrent admission cannot overwrite invalidation. Lazy keys and LRU entries share a conservative 64 MiB retained-object budget. Oversized/malformed indexes bypass retention. Cache state resets after fork. jsonlfile.py remains 945 lines after shortening repetitive documentation.

## Deviations

None. Conservative admission accounting replaces an initially expensive object-by-object sizing approach within the approved authority. No product module, public setting, interface or dependency was added.

## Unresolved risks

No unresolved implementation blocker. Per the approved design, filesystem fingerprints do not supply transactional snapshots or detect edits with indistinguishable metadata. Cold admission has a small cost; a six-table cycle exceeding the budget is about 16% slower. Full-table and oversized-index reads receive no cache speedup. The budget bounds retained accounting, not process RSS. See implementation/performance.md for complete measured tradeoffs.

| Acceptance | Evidence | Result |
| --- | --- | --- |
| AC-1 | 40 cache cases plus affected JSONL, DataFrame and FolderDB suites within 246 passing focused tests; public signatures unchanged. | Passed |
| AC-2 | Mutation/recovery, partial failure, unstable admission, alias/rename/delete, slot/lint and diagnostics cases; all 246 focused tests pass. | Passed |
| AC-3 | LRU, oversized and lazy-key limits, conservative Unicode/integer accounting, concurrent-reader/invalidation cases; benchmark pressure retained about 58 MiB under 64 MiB. | Passed |
| AC-4 | Same-host nine-sample medians: 100k warm point about 63x and 100-row range about 53x; cold/write/oversized/pressure costs recorded in implementation/performance.md and benchmark-results.json. | Passed |
