# Implementation Plan

## Context

- Approved authority: `brainstorm/contract.md`, inheriting the parent Mission and the published metadata/timespec and metadata-slot Roadmap designs.
- Fresh knowledge search: `specdev knowledge search "atomic folder metadata slot width migration"` (precise mode). Relevant parent-selected paths read: `knowledge/workflow/adhoc-history.md`, `knowledge/_index.md`, and `knowledge/workflow/wsl2-filesystem.md`. The search returned prerequisite/history leads but no newer migration-specific rule; current source and approved design remain authoritative. Verification will prefer bytes/readability over mtime-only assertions.
- Existing work: prerequisite Assignments 00034 and 00035 are delivered; this Assignment had no plan or progress receipt and only a pending outcome. Existing Mission-controller state is preserved.
- Scope boundaries: change only the approved metadata-slot/configuration paths and focused tracked tests; do not alter immutable `tests/legacy/`, add dependencies, run lifecycle commands, or run the Mission-only full suite.

**Implementation Guides:** [api-security]

**Review Guides:** []

## Tasks

1. **T-1 — Add the atomic table slot-width replacement primitive (AC-1, AC-2, AC-3).** Validate the requested width and every preserved metadata record before mutation; rewrite only mismatched tables through same-directory temporary files, atomically replace the table, publish a rebuilt absolute-offset index last, clean temporary files on failure, and make retries/no-ops observable and convergent.
2. **T-2 — Implement folder-wide configuration and configured writers (AC-1, AC-2, AC-3).** Load and preserve `meta_slot_bytes` alongside `timespec`, add `FolderDB.set_meta_slot_bytes(width=4096)`, deterministically discover/preflight all tables before any write, publish the target configuration before per-table migrations, and pass the configured width only when creating new dictionary or DataFrame tables. Preserve control-file slot exclusion and unrelated configuration.
3. **T-3 — Add focused behavioral and failure-boundary coverage (AC-1, AC-2, AC-3).** Cover default enablement, resize/no-op selection, new-table creation in both writer families, rows/metadata/index preservation, deterministic all-table shrink refusal with byte-for-byte immutability, configuration/table replacement interruptions, cleanup, and successful retry without rewriting conforming tables.
4. **T-4 — Qualify the bounded delivery and write receipts (AC-1, AC-2, AC-3).** Run only focused metadata-slot/durability regressions plus the seven-file logical-line inventory and Python 3.8 syntax/API audit authorized by the contract. Record exact working-tree revision evidence in `implementation/progress.json` and summarize each acceptance result in `outcome.md`.

## Verification Plan

- Focused new migration tests are authoritative acceptance evidence for AC-1 through AC-3.
- Existing metadata-slot and durability tests are qualification evidence for compatibility with prerequisites.
- A seven-file logical-line inventory enforces inherited source caps, treating the not-yet-created `reports.py` as zero lines.
- An AST parse using Python 3.8 grammar plus a targeted API-floor scan audits declared Python 3.8 compatibility without claiming execution on an unavailable interpreter.
