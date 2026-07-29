# Assignment contract

Kind: change

<!-- Keep this proportional: reference existing project context, state only change-specific decisions, and use the fewest independent observable acceptance criteria (normally 1-5). -->

## Objective and context

Add one lifecycle-managed opaque companion slot at `<ticker>.jsonl.aux` for each
JSONL ticker. JSONLDB owns the file association and lifecycle; consumers own the
payload and its meaning.

## Scope and non-goals

- In scope: the canonical path and public operations for the single companion;
  atomic companion replacement; invalidation, deletion, clearing, movement, lint,
  and repair behavior wherever JSONLDB manages the owning ticker.
- Non-goals: consumer-specific schemas or semantics, arbitrary companion suffixes,
  multiple companions per ticker, synthesizing companion contents from JSONL data,
  or changing external archive/transport policy.

## Expected behavior

Each ticker has one canonical optional companion path formed by appending `.aux` to
its `.jsonl` path. Consumers can read, replace, or remove opaque companion content
through JSONLDB. Ticker discovery never exposes the companion as a ticker.

JSONLDB keeps an existing companion with its ticker during supported moves and
removes it during ticker deletion or database/folder clearing. Before JSONL data
can diverge from an existing companion, JSONLDB invalidates the companion; a later
consumer write publishes its replacement atomically. Interrupted work may leave
the companion absent but must not leave stale companion content attached to changed
data.

## Important decisions

- The suffix is fixed as `.aux`, producing `<ticker>.jsonl.aux`.
- A ticker has one companion slot only; there is no caller-defined suffix, kind,
  registry, or collection of arbitrary sidecar files.
- Companion presence is optional. JSONLDB treats its contents as opaque and never
  parses, validates, interprets, or manufactures them.
- Orphan companions are reported conservatively and are not silently promoted,
  attached, or used to recreate data.

## Constraints and invariants

- Existing `.jsonl`, `.idx`, and `.meta` discovery and lifecycle behavior remains
  compatible.
- Filesystem ordering must prefer a missing companion over a stale companion after
  interruption.
- The companion stays adjacent to and derives its identity solely from its owning
  `.jsonl` path.

## Delegated and reserved authority

- Delegated: API naming below the fixed public concept, internal factoring,
  platform-appropriate atomic replacement, conservative orphan reporting details,
  and the focused verification matrix.
- Reserved for the user: changing the `.aux` suffix, companion cardinality,
  optional-presence rule, opacity boundary, or invalidation guarantee.

## Risks and assumptions

- The worktree already contains substantial unrelated changes. Before
  implementation, those changes must be inspected and explicitly kept separate,
  checkpointed, or adopted according to the workflow's dirty-tree gate.
- Cross-file atomic rename is unavailable on common filesystems; safety therefore
  depends on invalidating stale companion state before changing data and publishing
  replacement companion state last.

## Verification authority

- Focused tests for changed modules: allowed after repository instructions are satisfied
- Full suite: requires explicit user approval unless already authorized here

## Acceptance criteria

- AC-1: Public companion operations map every JSONL ticker to the sole canonical
  `<ticker>.jsonl.aux` path, preserve opaque payload bytes across read/write, replace
  atomically where the platform permits, and never expose `.aux` as a ticker.
- AC-2: All supported ticker deletion, clearing, movement, reorganization, and
  content-mutation paths manage or invalidate the companion so changed data cannot
  retain stale companion content.
- AC-3: Lint/repair reports orphan companions and handles them conservatively
  without inventing payloads, while focused normal and interruption-path tests
  demonstrate compatibility with existing JSONL, index, and metadata behavior.
