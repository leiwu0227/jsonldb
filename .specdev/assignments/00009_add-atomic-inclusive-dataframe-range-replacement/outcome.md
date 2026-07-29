# Outcome

Delivered `FolderDB.replace_df_range` for exact inclusive replacement of a
bounded interval in an existing ticker. The low-level operation validates and
syncs the complete staged owner and index before invalidating `.aux`, publishes
the owner atomically before its derived index, and leaves stale companions
absent after publication begins.

Indexed range and single-key reads now use bounded owner-generation retries.
Index rebuilding is atomic and generation-fresh, while `get_dbmeta()` repairs
metadata left stale by an interrupted publication for both current and reopened
`FolderDB` instances. Focused tracked tests cover semantics, validation,
failure boundaries, recovery, races, and compatibility.

Deviations: none.

Unresolved risks: filesystem atomicity remains platform-dependent, and the
approved existing single-writer boundary remains unchanged. No additional
bounded Assignment is required.

| Acceptance | Evidence | Result |
| --- | --- | --- |
| AC-1 | Tracked parameterized semantics and pre-publication validation tests; 26-test focused module passed. | Passed |
| AC-2 | Tracked staging/order and failure-injection tests prove old-or-complete-new owners, absent-not-stale `.aux`, and current/reopened index plus metadata recovery. | Passed |
| AC-3 | Tracked indexed-reader race tests and selected existing storage, DataFrame, discovery, mutation, lint, hierarchy, repair, and timespec regressions; 25 compatibility tests passed. | Passed |
