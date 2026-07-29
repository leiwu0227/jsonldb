# Implementation plan

## Guides

**Implementation Guides:** [api-security]

**Review Guides:** []

## Tasks

### T-1 — Add atomic low-level range publication and race-safe indexed reads

Acceptance: AC-1, AC-2, AC-3

- Add an internal JSONL range-replacement primitive that serializes bounds and
  replacement keys with the configured timespec, rejects invalid or colliding
  serialized keys, and completely stages and syncs the replacement owner and
  index beside the destination.
- Publish in the required order: remove `.aux`, atomically replace the owner,
  then atomically replace its derived index. Ensure an interrupted owner/index
  window is recognizable and the index can be deterministically rebuilt.
- Make indexed range and single-key reads retry a bounded number of times when
  the owner changes after an index snapshot or indexed offsets do not identify
  their expected keys.

### T-2 — Expose `FolderDB.replace_df_range` and recover metadata

Acceptance: AC-1, AC-2

- Add the DataFrame-specific public API for existing owners only, including
  DataFrame type, unique index, inclusive bound, range membership, serialized
  key uniqueness, and complete payload serialization validation before
  publication.
- Refresh `db.meta` after successful owner/index publication and make metadata
  reads repair stale entries from the current owner/index, covering both the
  current `FolderDB` instance and a reopened instance after interruption.

### T-3 — Add tracked focused regression and failure-injection evidence

Acceptance: AC-1, AC-2, AC-3

- Add version-controlled focused tests covering middle, edge, whole, empty,
  insertion, correction, timespec, and all specified pre-publication failures.
- Inject failures at staging, companion invalidation, owner publication, index
  publication, and metadata update boundaries; assert unchanged-old or
  complete-new owner behavior, never-stale `.aux`, and index/metadata recovery.
- Exercise old-index/new-owner retries for range and single-key reads and run
  focused compatibility checks for existing storage, discovery, mutation,
  lint, hierarchy, repair, and timespec behavior.

## Verification

- Run only the new tracked focused test module while iterating.
- Run the existing focused `jsonlfile`, `jsonldf`, and `folderdb` modules once
  for compatibility after the new tests pass; do not run the full suite.
- Record exact commands, durations, and the dirty candidate revision in
  `implementation/progress.json`.
