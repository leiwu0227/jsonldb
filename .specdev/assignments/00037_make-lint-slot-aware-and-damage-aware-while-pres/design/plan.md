# Implementation plan

## Knowledge search

- Fresh bounded search: `specdev knowledge search "lint slot damage fast path"`.
- Relevant current/project guidance read: `.specdev/project_notes/big_picture.md`, `.specdev/project_notes/lint_db_performance.md`, `.specdev/assignments/00035_implement-metadata-slots-and-the-folderdb-metada/outcome.md`, and `.specdev/assignments/00036_implement-atomic-folder-wide-metadata-slot-width/outcome.md`.
- Relevant parent-selected living knowledge read: `.specdev/knowledge/workflow/wsl2-filesystem.md`; its coarse-mtime warning requires content/call-path assertions instead of mtime-only tests. The parent-selected knowledge index and Adhoc history contain no lint implementation constraint and were not used.
- Current authority and design read: the approved Assignment and parent Mission contracts plus `project_notes/roadmap/designs/jsonl_file_store/index_integrity_and_lint.md` and `project_notes/roadmap/designs/folder_database/metadata_and_timespec.md`.
- One symptom-focused search, `specdev knowledge search "orjson pytest environment"`, followed the ambient-interpreter collection failure; it confirmed `orjson` as required, and prior durable delivery receipts identified the existing pinned Python 3.12 test runtime used for verification.

**Implementation Guides:** []

**Review Guides:** []

Neither catalog guide applies to this internal Python file-integrity change: it has no browser-facing behavior or new trust/authorization boundary.

## Tasks

1. **T-1 — Make file lint slot- and damage-aware (AC-1, AC-2).** Extend `lint_jsonl` with optional expected slot width, slot-aware cardinality and exact layout checks, default/forced damage detection, payload-free removal diagnostics, and an atomic canonical rewrite that publishes the rebuilt index last. Preserve standalone legacy behavior and the fresh-index fast path.
2. **T-2 — Propagate folder slot policy without slotting controls (AC-1, AC-2).** Pass `FolderDB.meta_slot_bytes` only when linting table files; keep `db.meta` and `h.meta` on unslotted standalone lint defaults and preserve public return/default shapes.
3. **T-3 — Add focused fault and behavior coverage (AC-1, AC-2).** Add tracked non-legacy tests for legacy/slotted default and forced lint, slot preservation/repair, torn indexed and unindexed rows, missing newline/dead space, absolute indexes, diagnostics, fast-path observation, and atomic data/index failure seams.
4. **T-4 — Qualify the bounded delivery (AC-1, AC-2).** Run only the focused lint and directly related durability/slot regressions, then the mandated seven-file logical-line inventory and Python 3.8 syntax/API audit. Record exact receipts and finalize progress/outcome artifacts.
