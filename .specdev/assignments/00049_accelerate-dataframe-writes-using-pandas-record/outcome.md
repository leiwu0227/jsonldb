# Outcome

## Delivered behavior

Eligible pandas 3.0 DataFrames use record-oriented conversion and bulk string
index keys before writing. Other inputs retain the original conversion, including
older/future pandas branches, missing or unusual indexes, subclasses, duplicate
labels and empty axes. Conversion remains eager; public interfaces, bytes and
entry-point-specific validation/warnings remain intact. The adapter is 144 lines.

## Deviations

None. Pandas 1.3.5 lacked a usable local wheel; the original conversion remains
in place for that branch, and runtime compatibility was checked on 1.5.3, 2.0.3,
2.3.3, 3.0.0 and 3.0.5. No support-floor, dependency, architecture or design change.

## Unresolved risks

No unresolved implementation blocker. The measured fast path improves large
string-indexed numeric/mixed overwrites 1.14–1.31x. Fallback workloads and single-row
upserts remain near baseline. Temporary lists add about 0.54 MB to conversion
peak at 20k rows; full-write peak is essentially unchanged. Timings and allocation
are workload-dependent. Full-suite execution and pandas 1.3 runtime testing were
not performed; detailed limitations and all negative samples are retained in
implementation/performance.md. The unpublished roadmap draft remains separate.

| Acceptance | Evidence | Result |
| --- | --- | --- |
| AC-1 | 411 main-runtime focused tests, 142 tests on each of four additional pandas versions; 66 public signatures and 40 baseline storage snapshots unchanged. New differential cases cover values, labels, indexes, subclasses, fallbacks and caller-frame preservation. | Passed |
| AC-2 | Conversion failures precede writer mutation; distinct validation/warnings and plural completion are checked. Existing durability, metadata, slot, cache and byte-serialization suites pass. | Passed |
| AC-3 | Three alternating benchmark pairs show 100k overwrites improve 1.220x numeric-one-column, 1.139x numeric-eight-column and 1.309x mixed. Conversion/write/upsert, fallback, small, datetime and allocation evidence is recorded separately; a fresh-snapshot benchmark run passes. | Passed |
