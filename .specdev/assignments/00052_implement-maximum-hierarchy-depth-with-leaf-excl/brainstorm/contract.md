# Assignment contract

Kind: change

## Objective and context

Implement the maximum hierarchy depth design published in commit `c2dbf85`.
The authoritative design notes are:

- `.specdev/project_notes/roadmap/designs/folder_database/hierarchical_layout.md`
- `.specdev/project_notes/roadmap/designs/folder_database.md`
- `.specdev/project_notes/roadmap/designs/core_concepts.md`

The hierarchy note governs placement; the older big-picture path example is not
authority for this change. The bounded knowledge search for `hierarchy` returned
only `.specdev/project_notes/big_picture.md`, which was read for project context.

## Scope and non-goals

Implement maximum-depth naming, ordinary table access, reorganization, legacy
layout migration, missing/damaged hierarchy-control recovery, and explicit
restoration of formerly quarantined short names. Update affected public API and
usage documentation and examples with focused regression coverage.

No public parameter rename, arbitrary per-table path overrides, compatibility
with older library readers after migration, concurrent-writer support, new
transaction guarantees, unrelated storage changes, or roadmap edits.

## Expected behavior

Keep `hierarchy_depth` as the positive maximum directory depth. Strip the optional
`.jsonl` suffix, split on the configured delimiter, exclude the final segment,
and use at most the configured number of remaining segments as directories.
The filename retains the complete name. At maximum six, `a` lives at `a.jsonl`,
`a.b.c` at `a/b/a.b.c.jsonl`, and `a.b.c.d.e.f.g.h` at
`a/b/c/d/e/f/a.b.c.d.e.f.g.h.jsonl`. Short names remain valid. Flat mode remains
available and does not gain implicit hierarchy merely from dotted filenames.

## Important decisions

- Valid saved hierarchy controls determine the delimiter and maximum unless an
  explicit maximum overrides the latter. On open, automatically reconcile older
  hierarchy layouts with the new mapping, including when the argument is omitted
  or equals the saved depth. A successful reopen is stable.
- When hierarchy controls are missing or invalid, honor an explicit positive
  maximum. Otherwise use the deepest observed visible table directory depth as
  the inferred maximum if any nested tables exist. If none exist, fall back to
  flat mode. Infer the delimiter from consistent directory/name prefixes using
  the existing recovery convention, defaulting to a dot when no prefix provides
  evidence. Reject contradictory prefixes before moving tables. Report inferred
  settings without claiming to recover the original configured maximum.
- Mixed root-level and nested tables are supported. Recovery may reorganize them
  to the inferred or explicitly requested canonical layout. Previously valid
  historical names retain access under existing compatibility rules.
- Before migration, reorganization, or quarantine restoration, detect destination
  collisions and unsafe paths without overwriting data. Preserve table bytes and
  embedded metadata; move usable companion indexes and rebuild missing indexes
  where necessary. Report errors without claiming successful completion.
- Interrupted moves are retryable through reopen or the same maintenance call.
  Publish completed layout settings only after successful moves; interrupted
  data/index separation must not lose table access after a successful retry.
  Restore quarantined short names only through the explicit reprocess operation.

## Constraints and invariants

Preserve portable-name checks for new tables, safe directory components, hidden
subtree exclusions, existing metadata/configuration behavior, and directory-free
reads of missing tables. Refresh table locations and statistics after moves.
Retain the published maximum of 1250 total lines for `jsonldb/folderdb.py` through
bounded refactoring if needed. Independent Discussion state is outside ownership.

## Delegated and reserved authority

- Delegated: implementation structure, internal migration bookkeeping if needed,
  bounded refactoring, focused test selection, documentation updates, required
  implementation review and repairs, and the final Assignment delivery commit.
- Reserved for the user: changing these behaviors or recovery policy, expanding
  scope, raising the source line cap, waiving implementation review, or running
  the full test suite.

## Risks and assumptions

Layout changes move existing files. A single owner operates the database during
migration. Lost settings cannot always be reconstructed: an all-root layout may
have been flat or a hierarchy containing only single-segment names. The stated
fallback is deliberate. Older library versions may reject or misplace tables
under the new layout; downgrade compatibility is not promised.

## Verification authority

Focused tests for hierarchy, table names, recovery, and affected folder operations
are authorized, including failure injection for interrupted moves and collision
handling. Do not run the full suite without explicit user approval. Brainstorm
review is optional; implementation review is required.

## Acceptance criteria

- AC-1: Short, exact-limit, and longer names round-trip through dictionary and
  DataFrame operations under the specified mapping, including optional suffixes,
  custom delimiters, root-level names, discovery, deletion, and reopen. Unsafe
  new names remain rejected and missing-table reads create no directories.
- AC-2: Existing fixed-depth layouts migrate on open with unchanged or omitted
  depth; explicit maximum changes reorganize all visible tables without
  quarantining short names. Data, embedded metadata, usable indexes, and database
  metadata remain consistent, and a second open makes no further layout changes.
- AC-3: Missing/damaged hierarchy controls follow the stated explicit/inferred
  recovery policy for nested, mixed, root-only, and empty layouts. Recovery
  reports inference, preserves hidden trees, and restores access to all tables.
- AC-4: Migration, reorganization, and quarantine restoration preserve source
  data on detected collisions or unsafe destinations. Injected interruptions are
  recoverable by retry without table loss or duplicate logical tables. Explicit
  restoration accepts safe short names without overwriting existing tables.
- AC-5: Affected public documentation and examples describe maximum directory
  depth and final-segment exclusion, including migration and recovery behavior;
  the published source line cap is met.
