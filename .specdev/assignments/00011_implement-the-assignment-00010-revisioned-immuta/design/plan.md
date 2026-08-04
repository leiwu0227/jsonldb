# Implementation plan

**Implementation Guides:** [api-security]

**Review Guides:** []

## Tasks

1. **T-1 — Implement the immutable catalog core (AC-1, AC-3).** Add the
   public frozen snapshot and entry values and catalog exceptions; implement the
   exact version-1 catalog/pending validation boundary, root `.jsonldb/` layout,
   durable same-directory publication, bounded platform writer locking, stable
   reader, and one-generation-per-root process cache.

2. **T-2 — Integrate recovery and managed mutation publication (AC-2, AC-3).**
   Add re-entrant folder writer transactions and deterministic pending recovery,
   then route all enumerated single and batch CRUD, deletion, auxiliary,
   configuration, hierarchy, lint, clear, reconciliation, commit, and revert
   responsibilities through one appropriate ticker/full transaction and catalog
   commit point while preserving `.jsonldb/` during discovery and cleanup.

3. **T-3 — Preserve and prove supported compatibility (AC-4).** Keep existing
   public signatures, legacy mutable `get_dbmeta()` projection and healing,
   constructor behavior that does not force owner scans, formats, CRUD/lint/
   hierarchy/timespec/companion/Git semantics, and document the complete
   supported-compatibility inventory with focused regression coverage.

4. **T-4 — Add structural, interruption, and concurrency evidence (AC-1, AC-2,
   AC-3).** Add focused tests for strict validation and immutable shared loads;
   exact single/batch revision behavior; cache invalidation; lock contention;
   targeted/full and before/after-commit recovery; unsupported-version
   preservation; clear and Git lifecycle; and failure boundaries without
   weakening existing tests.

5. **T-5 — Produce reproducible performance evidence and receipts (AC-5).**
   Extend the synthetic benchmark to compare production cold/reuse snapshot,
   reconciliation, one-ticker mutation, and batch mutation at multiple catalog
   sizes with timing and structural counters; run only focused verification,
   enforce the 256-ticker relative/structural gates, inspect the narrow diff,
   and finalize progress and outcome artifacts.
