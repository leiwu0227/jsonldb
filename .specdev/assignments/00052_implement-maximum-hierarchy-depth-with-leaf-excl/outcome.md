# Outcome

## Delivered behavior

Hierarchy now caps directory depth and excludes the final name segment. Short
names coexist with nested tables. Open migrates older layouts without a changed
depth argument. Missing controls use an explicit maximum or the deepest observed
depth, reported as inferred. Shared collision preflight and a hidden atomic
pending-move record support retries for migration, reorganization, and explicit
safe quarantine restoration, including separated indexes. Public docs and the
portable-dataset example describe the new behavior.

## Deviations

None. The preceding user-requested big-picture correction is included as
supporting documentation; the pending-implementation sentence is now removed.

## Unresolved risks

As approved: no concurrent writers, no downgrade compatibility, and inferred
settings may differ from lost settings. Preserve `.hierarchy.pending` with an
interrupted database until reopen completes its operation. No full suite run.

| Acceptance | Evidence | Result |
| --- | --- | --- |
| AC-1 | Mixed-depth dictionary/DataFrame, suffix, custom delimiter, discovery/deletion, historical-name and read-no-directory tests in the 552-test focused run. | Passed |
| AC-2 | Legacy unchanged/omitted-depth migration checks preserve bytes, inode, indexes, embedded metadata and paths; explicit depth increase/decrease and stable reopen covered. | Passed |
| AC-3 | Updated hierarchy recovery and mixed-root/nested tests cover explicit and inferred maximum, custom delimiter, empty/flat fallback, hidden exclusions and failure reports. | Passed |
| AC-4 | Collision preflight and injected index/table/control/metadata failures cover retries through open and maintenance; safe quarantine restoration and missing-index rebuilding covered. | Passed |
| AC-5 | API, usage, file-format and portability docs plus notebook updated; check_evidence.py passes with 1095 source lines, Python 3.8 and notebook syntax. | Passed |
