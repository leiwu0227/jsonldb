# Implementation plan

## Context and constraints

- Authority: the approved Assignment contract, inherited Mission contract, and
  published `folder_database/integrity_logs.md`, source-layout, index/lint,
  hierarchy, metadata, version-control, and visualization Roadmap designs.
- Fresh knowledge search: `specdev knowledge search "runtime logging integrity
  lint reports"` (precise mode) was run before planning. The parent-selected
  `.specdev/knowledge/workflow/wsl2-filesystem.md` is relevant and was read;
  report replacement tests will compare content rather than mtimes. The
  parent-selected `.specdev/knowledge/_index.md` and
  `.specdev/knowledge/workflow/adhoc-history.md` were screened for objective
  terms and contained no applicable implementation guidance, so they are not
  relied upon. After the default interpreter lacked `orjson`, the permitted
  symptom search `orjson missing dependency test environment` found no relevant
  environment guidance; prior verified Assignment receipts identified the
  existing Python 3.12 dependency runtime. No stale or superseded guidance is
  used.
- Preserve Python 3.8 compatibility and existing public return shapes. Keep
  report formatting/capture in package-independent `jsonldb/reports.py`, with
  only open/lint hook points in `FolderDB`. Respect all seven source caps.
- Fixed delegated bounds: capture at most 100 findings per run and at most 160
  decoded replacement characters per removed entry; record the omitted count.
- No available catalog guide applies to this internal Python logging and local
  report-file change.

**Implementation Guides:** []

**Review Guides:** []

## Tasks

1. **T-1 — Replace package runtime prints with path-bearing logging (AC-1).**
   Convert executable package `print` calls in `folderdb`, `vercontrol`, and
   `visual` to module loggers at informational or warning severity while
   preserving control flow, return values, exceptions, and other non-transport
   behavior. Add focused checks that representative diagnostics emit records
   and leave stdout/stderr silent.
2. **T-2 — Add bounded report capture and FolderDB hooks (AC-2, AC-1).**
   Add the dependency-leaf `reports` module with scoped logger capture,
   one-line sanitization, timestamped replacement writers, per-removal and
   total-finding bounds, and omitted counts. Attach capture only around
   `FolderDB` open and `lint_db`; filter findings to the opened folder, always
   replace `.jsonldb/integrity.log` or `.jsonldb/lint.log`, keep clean reports
   header-only, preserve open as observational for table bytes, and retain the
   hidden directory's exclusion from discovery, hierarchy, and clearing.
3. **T-3 — Qualify behavior and hard constraints (AC-1, AC-2).**
   Add focused tests for clean/anomalous replacement, bounded removed details,
   omitted counts, folder scoping, handler cleanup, open non-repair, ordinary
   operation silence, and hidden-directory behavior. Run only those focused
   tests plus the package AST print/stream-write inventory, Python 3.8
   syntax/API-floor audit, immutable legacy diff check, and seven-file logical
   line inventory; record commands and results in `progress.json` and summarize
   acceptance evidence in `outcome.md`.
