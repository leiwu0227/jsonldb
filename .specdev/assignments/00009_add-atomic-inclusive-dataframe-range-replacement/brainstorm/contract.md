# Assignment contract

Kind: change

<!-- Keep this proportional: reference existing project context, state only change-specific decisions, and use the fewest independent observable acceptance criteria (normally 1-5). -->

## Objective and context

Add a public atomic bounded DataFrame replacement operation for one existing
JSONL ticker, following the architecture in
`.specdev/project_notes/thoughts/20260729_atomic_replace_range_for_explicit_restatement.md`.
The operation must preserve JSONLDB's opaque `.aux` contract and remain safe for
the repository's current single-writer, concurrent-reader consumers.

## Scope and non-goals

- In scope: `FolderDB.replace_df_range(name, lower_key, upper_key, df)`; the
  low-level range-replacement primitive; complete pre-publication validation and
  staging; `.aux`, index, and metadata ordering and recovery; indexed-read retry
  across the owner/index publication window; version-controlled focused tests.
- Non-goals: consumer-specific refresh, payload, or companion semantics;
  consumer cache adapters; a public dictionary-range API; arbitrary companions;
  multi-writer or cross-process transactions; or changing routine
  upsert/delete/overwrite semantics.

## Expected behavior

For an existing ticker `E`, inclusive bounds `[L, U]`, and replacement frame
`R`, the committed owner contains all rows of `E` below `L`, exactly `R` within
the interval, and all rows of `E` above `U`. Old interval rows absent from `R`
are removed; an empty frame clears the interval; rows outside it remain
logically equivalent under canonical serialization. A missing owner is rejected
without creating one.

Bounds, uniqueness, range membership, key serialization, and payload
serialization are validated by successfully staging the complete new owner and
index before the existing owner, index, metadata, or companion can change. The
old `.aux` is removed immediately before owner publication. Success returns
with a complete new owner, matching index and metadata, and `.aux` absent.

After interruption, indexed readers either observe a complete old or complete
new logical ticker; an old-index/new-owner race is detected and retried rather
than returning wrong, empty, or partial data. Index and `db.meta` state left
behind after owner publication are deterministically recoverable by the current
`FolderDB` instance and by a newly opened instance.

## Important decisions

- The public API is named `replace_df_range`, matching existing `upsert_df` and
  `overwrite_df` naming. Internal helper names and factoring are delegated.
- Bounds are inclusive and serialized with the owning `FolderDB` timespec before
  comparison.
- JSONLDB retains its existing single-writer assumption; this change supports
  concurrent readers but does not introduce a new locking or transaction system.
- Regression tests are committed under a tracked path; ignored `unit_tests/`
  evidence alone is insufficient.

## Constraints and invariants

- Before companion invalidation, every failure leaves the old owner, index,
  metadata, and `.aux` unchanged.
- After companion invalidation, `.aux` stays absent unless a caller explicitly
  republishes it; stale companion bytes must never attach to changed data.
- Owner publication is atomic where the platform permits. Index and metadata are
  derived/recoverable state and must be correct before success is reported.
- Existing datasets require no migration, and existing public signatures plus
  `.jsonl`, `.idx`, `.meta`, `.aux`, discovery, upsert, delete, overwrite, lint,
  hierarchy, and repair behavior remain backward compatible.

## Delegated and reserved authority

- Delegated: streaming versus bounded in-memory construction, temporary-file and
  fsync details, internal helper names, bounded indexed-read retry design,
  metadata freshness mechanism, and the focused test decomposition.
- Reserved for the user: the public API name, inclusive interval semantics,
  existing-owner requirement, single-writer boundary, `.aux` invalidation order,
  backward-compatibility guarantee, or expansion to other public payload types.

## Risks and assumptions

- JSONL owner, index, metadata, and companion cannot be replaced as one
  filesystem transaction. Safe ordering, retry, and deterministic recovery are
  therefore part of the observable behavior.
- The current indexed read path can load an old index immediately before owner
  replacement; the implementation must close this race without silently
  weakening read results.
- The handoff note is a pre-existing untracked project file and must be
  checkpointed separately or explicitly adopted at the implementation Git gate.

## Verification authority

- Focused tests for changed modules: allowed after repository instructions are satisfied
- Full suite: requires explicit user approval unless already authorized here

## Acceptance criteria

- AC-1: `FolderDB.replace_df_range` implements exact inclusive replacement for
  middle, edge, whole, empty, insertion, and correction cases; invalid owners,
  bounds, duplicate keys, out-of-range keys, or serialization leave all existing
  artifacts unchanged.
- AC-2: Focused failure injection demonstrates stage-first publication, `.aux`
  invalidation immediately before owner replacement, complete old/new owners,
  absent-not-stale companion outcomes, and deterministic index plus same-instance
  and reopened-instance metadata recovery.
- AC-3: Indexed range and single-key reads detect and retry the owner/index race,
  while version-controlled focused tests demonstrate that existing storage,
  discovery, mutation, lint, hierarchy, repair, and timespec behavior remains
  compatible.
