# Outcome

## Delivered behavior

Ordinary save, atomic save and upsert now write serialized row bytes directly,
avoiding an intermediate text conversion. NumPy support, record bytes, byte offsets,
public APIs, validation, publication and failure behavior are preserved. Higher-level
callers inherit the optimization. The file store remains at 950 lines.

## Deviations

None. No new product module, dependency, public setting, streaming/buffering change
or roadmap publication. The unapproved design draft remains outside ownership.

## Unresolved risks

No unresolved implementation blocker. Gains depend on workload: 100k full saves
improve 1.212x while isolated serialization improves 1.640x. One-row large-table
upserts and atomic control writes remain near baseline. Small timings vary; all
negative results and dense follow-ups are retained in implementation/performance.md.
The existing single-writer durability model is unchanged. No full suite was run.

| Acceptance | Evidence | Result |
| --- | --- | --- |
| AC-1 | 330 focused tests; 25 new cases verify former-serializer bytes/indexes, Unicode, NumPy, collisions, slots and update offsets. Baseline comparison confirms 66 public signatures and 40 byte-identical storage snapshots. | Passed |
| AC-2 | Unsupported payload/invalid-key tests preserve exception and mutation timing; existing durability, atomic control-file, metadata, cache and recovery suites pass. | Passed |
| AC-3 | Three callers consume bytes without text conversion. Four alternating benchmark pairs show 100k full save 39.009 to 32.195 ms (1.212x); serialization-only 15.850 to 9.663 ms (1.640x). All small/large/atomic costs, dense follow-ups and fresh-snapshot benchmark evidence are preserved. | Passed |
