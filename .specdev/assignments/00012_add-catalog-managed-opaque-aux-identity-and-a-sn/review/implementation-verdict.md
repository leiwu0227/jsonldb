---
verdict: approved
material_divergence: false
---

## Findings

No blocking findings. All four contract acceptance criteria have final passing results with reusable receipts recorded at `working-tree@6a93a28`, matching the current HEAD and working tree.

- AC-1: Strict v1/v2 envelope validation is exact and all-or-nothing (`jsonldb/catalog.py` — absent identity must be null/null, present identity requires non-negative int size and the exact 71-character lowercase `sha256:` digest; `_is_int` excludes bool). Versions newer than 2 raise `UnsupportedCatalogVersionError` fail-closed, and the existing fail-closed test was correctly retargeted from version 2 to 3 (equivalent semantics, not weakened). V1 migration publishes one next revision in the same lineage under writer ownership via a durable pending record (`_migrate_catalog_locked`), and the focused test proves zero owner-tree walks and zero index loads during migration by making both fatal. Recovery, `get_dbmeta()` reconciliation, hierarchy, Git commit, and interrupted-publish paths were verified to publish final companion identities. Pending protocol remains strict version 1 (`PENDING_VERSION`).
- AC-2: `read_family()` enforces canonical-root snapshot ownership, checks generation identity plus pending absence before/after owner and AUX reads, verifies exact byte length and SHA-256 (the test includes a same-size `b"new"` → `b"NEW"` replacement caught by digest), never substitutes a newer generation for a supplied snapshot, retries bounded when unbound, and takes no writer lock and reads no unrelated ticker (verified with a fatal `WriterLock` and select-path recording). Missing owners raise `FileNotFoundError`.
- AC-3: Additive exports only (`TickerFamilyRead`, `CatalogChangedError`); six-argument `FolderCatalogEntry` construction retains backward-compatible defaults and is tested; `db.meta` projection code is untouched; `hashlib` is stdlib so no dependency evidence is required; the README documents the old-binary limitation. Python 3.8 was unavailable on the host, so 3.9 compilation was used and reported rather than weakened, per contract.
- AC-4: The receipt-embedded benchmark assertions match the contract gates: one catalog decode, zero tree walks and zero companion identity loads on cold v2 load, affected-only companion work for one-ticker AUX publication, all four AUX payload classes reported, and 18.28x measured against the 5x 256-ticker gate.
- The full suite is correctly recorded as skipped because the contract explicitly withheld authorization.

Non-blocking observation: the implemented `read_family` branch that rejects an unexpected companion when the entry records absence (`jsonldb/folderdb.py`, the `elif os.path.lexists(aux_path)` arm) has no dedicated focused test; the opposite presence-mismatch direction is tested. Worth a one-line test in a future change; it does not leave any acceptance criterion without a final result.
