# Outcome

Delivered strict catalog version 2 with exact opaque companion presence, size,
and SHA-256 identity; durable zero-owner-work migration from valid v1 catalogs;
and identity-correct targeted/full recovery, reconciliation, hierarchy, clear,
and managed Git behavior. Added and exported frozen `TickerFamilyRead`, retryable
`CatalogChangedError`, and bounded optimistic `FolderDB.read_family()` reads
that bind selected dictionary data and optional AUX bytes to one catalog
generation without taking the writer lock.

Deviations: none. No unresolved implementation risks were found. The full suite
was not run because the approved contract explicitly withheld authorization;
56 focused tests and the required synthetic scaling benchmark passed. Catalog
v2's documented incompatibility with pre-v2 binaries is intentional and
approved.

| Acceptance | Evidence | Result |
|---|---|---|
| AC-1 | Strict v1/v2 validation and immutable fields; v1 same-lineage migration; focused mutation, recovery, reconciliation, hierarchy, clear, newer-version, pending-v1, and Git checks. | Passed |
| AC-2 | Focused exact range/AUX reads, frozen result, foreign/stale/pending/mismatch failures, supplied-snapshot non-substitution, unbound retry, and no-lock/unrelated-read checks. | Passed |
| AC-3 | Existing focused compatibility inventory plus additive export/signature, unchanged mutable `db.meta`, Python 3.9 compilation/construction, and documented old-binary limitation; 56 tests passed. | Passed |
| AC-4 | Reproducible 16/64/256 benchmark recorded one decode and zero walks/index/AUX loads for cold reads, cache reuse, affected-only AUX work, all four AUX sizes, and an 18.28x 256-ticker speedup against the 5x gate. | Passed |
