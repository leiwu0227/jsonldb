# Implementation plan

**Implementation Guides:** [api-security]
**Review Guides:** []

## Tasks

1. **T-1 — Catalog v2 and managed AUX identity (AC-1, AC-3):** extend the immutable catalog entry and exact envelope validation/serialization for strict v1/v2 support; add structural AUX hashing; migrate valid v1 catalogs under writer ownership as one next revision without owner/index discovery; and carry final identities through targeted/full reconciliation, recovery, hierarchy, clear, and Git lifecycle paths while keeping pending v1 and `db.meta` unchanged.
2. **T-2 — Snapshot-consistent family read (AC-2, AC-3):** add and export the frozen `TickerFamilyRead` value and retryable `CatalogChangedError`, then implement bounded optimistic `read_family()` reads that enforce canonical snapshot ownership, generation stability, pending-state exclusion, owner selection, and exact AUX presence/size/digest checks without a reader lock or unrelated-ticker work.
3. **T-3 — Focused compatibility, failure, and performance evidence (AC-1, AC-2, AC-3, AC-4):** add focused tests for strict validation, v1 migration, mutation/recovery/Git/hierarchy behavior, public compatibility, generation races, and opaque AUX mismatch handling; extend the reproducible production benchmark for 16/64/256 tickers and representative AUX sizes; run only the authorized focused checks and record receipts.
4. **T-4 — Delivery receipts (AC-1, AC-2, AC-3, AC-4):** record completed task status, exact verification commands and working-tree revision, any deviations or unresolved risks, and the acceptance evidence table in the required Assignment artifacts.
