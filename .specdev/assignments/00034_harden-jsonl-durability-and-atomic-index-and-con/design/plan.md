# Implementation plan

## Context and knowledge

- Authority: the approved Assignment contract, parent Mission contract, repository instructions, and the published JSONL file-store/index-integrity and metadata-control-file designs.
- Fresh search: `specdev knowledge search "jsonl durability atomic index concurrency"` (precise mode) returned only partial matches. Relevant parent-selected living knowledge read for planning: `.specdev/knowledge/_index.md`, `.specdev/knowledge/workflow/wsl2-filesystem.md`, and `.specdev/knowledge/workflow/adhoc-history.md`. The WSL2 note requires content-based assertions rather than sub-second mtime assertions; the vault/index and Adhoc-history notes supplied workflow context but no product rule that supersedes current designs.
- Current state: row append newline healing and some torn-row guards already exist. Range reads lack a guard, single-key skips are silent, warnings lack paths, grown updates blank before append, and index/control-file writes still target their final paths directly.

**Implementation Guides:** []

**Review Guides:** []

## Tasks

1. **T-1 (AC-1): Torn-row reads and append boundary.** Centralize path-and-offset-bearing torn-row logging, apply it to full loads, index builds, single-key lookups, and range reads while preserving intact rows and public return shapes, correct index-scan offset advancement after malformed lines, and cover missing-terminal-newline healing with focused tests.
2. **T-2 (AC-2): Atomic indexes and grown-upsert ordering.** Add same-directory temporary-file atomic index publication for build, save, update, and delete; append and flush grown replacements before blanking old rows; keep the index publication last; add focused normal-path and injected serialization/replacement/interruption tests proving complete compact sorted indexes and old-or-new row availability.
3. **T-3 (AC-3): Atomic protected control files.** Add a narrow atomic whole-JSONL save path and use it only for `config.meta` and `h.meta`, leaving `db.meta` data publication in place; test replacement failures and destination tracing to prove complete old/new protected files and the unchanged `db.meta` policy.
4. **T-4 (AC-1, AC-2, AC-3): Qualification.** Run only the focused durability test module plus directly affected warning/legacy JSONL tests, then run the parent-required Python 3.8 syntax/API audit and seven-file logical-line cap inventory. Record exact commands, working-tree revision, durations, and results in `implementation/progress.json` and summarize acceptance evidence in `outcome.md`.

## Verification strategy

- Behavioral tests use file bytes and parsed contents, never sub-second mtime comparisons.
- Failure injection targets bounded internal serialization, append-before-blank, and `os.replace` seams; assertions inspect durable on-disk old/new states after the injected exception.
- The full tracked suite is not run because it is reserved for final Mission integration.
