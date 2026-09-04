# Implementation Plan

## Context and guides

- Approved authority: `.specdev/assignments/00035_implement-metadata-slots-and-the-folderdb-metada/brainstorm/contract.md`.
- Fresh knowledge search: `specdev knowledge search 'metadata slot FolderDB fixed-width ordering'` (precise mode). It returned only partial project-note matches, so no second broad search was needed.
- Parent-selected knowledge read for this plan: `.specdev/knowledge/workflow/wsl2-filesystem.md` (avoid sub-second mtime assertions), `.specdev/knowledge/_index.md` (knowledge organization and authority), and `.specdev/knowledge/workflow/adhoc-history.md` (historical receipts are leads, not current guidance; no relevant Adhoc receipt was needed).
- Relevant designs: `.specdev/project_notes/roadmap/designs/jsonl_file_store/metadata_slot.md`, `.specdev/project_notes/roadmap/designs/folder_database.md`, `.specdev/project_notes/roadmap/designs/folder_database/metadata_and_timespec.md`, and `.specdev/project_notes/roadmap/designs/jsonl_file_store/index_integrity_and_lint.md`.

**Implementation Guides:** [api-security]

**Review Guides:** []

## Tasks

1. **T-1 — Implement the version-1 metadata-slot primitive and integrate it with JSONL row/index paths (AC-1, AC-2).** Add the package-independent envelope registry, classification, byte-width encoding, read, and in-place write helpers; reject `_meta` as a row key; skip classified line-one slots in sequential reads and index builds; preserve absolute row offsets; and add fit-checked metadata-aware save/update plus slot-only publication with the index freshness update last.
2. **T-2 — Add the bounded `FolderDB` metadata API and DataFrame propagation (AC-1, AC-2).** Add `meta=` to the four singular overwrite/upsert calls, preserve existing call signatures and defaults, provide record-first `read_meta`, `get_dict_with_meta`, and `get_df_with_meta`, provide `clear_meta`, and return `None` plus an empty row container for missing tables. Keep folder-wide slot enablement/resizing reserved for the next Mission child.
3. **T-3 — Add focused acceptance tests and run bounded qualification (AC-1, AC-2).** Cover legacy/valid/malformed slot classification, reserved-key validation, opaque records, missing tables, read/write ordering, oversized-record refusal without mutation, absolute offsets, row-byte preservation during slot-only writes, index publication last, and existing-call compatibility. Run only the focused tests, then the required Python 3.8 syntax audit and seven-file logical-line inventory.
