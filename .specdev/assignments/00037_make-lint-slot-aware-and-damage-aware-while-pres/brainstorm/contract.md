# Assignment contract

Kind: change

<!-- Keep this proportional: reference existing project context, state only change-specific decisions, and use the fewest independent observable acceptance criteria (normally 1-5). -->

## Objective and context

Implement the lint child delegated by
`.specdev/missions/M00001_implement-the-five-dependency-ordered-roadmap-wo/brainstorm/contract.md`
at approved hash `7e0824300f897190603ccb138e56aaf5254389c0b40f69967908352465ceac4f`.
This delta inherits that Mission contract and builds on integrated outcomes
`00034`, `00035`, and `00036` to make lint slot-aware and damage-aware without
losing its default fast path.

## Scope and non-goals

- In scope: JSONL lint fidelity and canonical-layout checks and repairs,
  `FolderDB` propagation of configured slot width to table lint, damage
  diagnostics, and focused tests. Non-goals are inherited from the Mission;
  specifically, persistent report capture, package-wide print replacement, and
  unrelated open-time diagnostics remain with child `00038`.

## Expected behavior

Default and forced lint leave legacy tables unslotted, make enabled-folder
tables use an exact configured-width version-1 slot while preserving any valid
record, and produce compact sorted rows with a faithful absolute-offset index.
Lint repairs wrong or malformed slots, torn tails, missing terminal newlines,
dead space, and—under `force`—torn indexed rows; slot lines are excluded from
row cardinality and included in canonical-layout boundaries.

## Important decisions

`FolderDB` is the authority for expected table-slot width; control files are
linted without slot enablement, while standalone lint must not invent a slot for
a legacy file. The normal path skips the full mmap cardinality pass when the
index is at least as new as the data file but still performs the required spot
and exact layout checks; only `force` parses every indexed row. Repairs publish
a temporary data file atomically, with slot first and normalized indexed rows,
then rebuild the index last.

## Constraints and invariants

Inherit all Mission constraints and invariants, including Python 3.8, existing
public defaults and return shapes, absolute offsets, compact sorted indexes,
the lint fast/forced split, control files without slots, immutable legacy tests,
and all seven source caps. Damage diagnostics identify the path, offset or
region, and removed-byte count without embedding removed payload bytes.

## Delegated and reserved authority

- Delegated: compatible internal width plumbing, lint helper factoring,
  detection order, and focused fault-injection/test organization within the
  Mission's module and line-cap boundaries. Reserved authority is unchanged
  from the Mission, including any weakening of fast-path, repair, slot-record
  preservation, atomic publication, or compatibility requirements.

## Risks and assumptions

The integrated prerequisite outcomes are assumed authoritative and complete;
lint must consume their slot classification, migration, and index-publication
behavior rather than replace it. `jsonldb/jsonlfile.py` currently has little
room below its Mission cap, and coarse filesystem mtimes can compare equal, so
implementation may require consolidation and tests must not use mtime change
alone as proof of repair or non-repair.

## Verification authority

- Implement the behavior and run focused legacy/slotted default/forced lint
  tests, including clean-file fast-path observation, malformed and missing
  slots, torn indexed and unindexed rows, tail/newline/dead-space repair,
  record preservation, diagnostics, and atomic replacement failure seams. Also
  run the Mission-mandated seven-file logical-line inventory and Python 3.8
  syntax/API audit; the full suite remains reserved for final Mission
  verification, and evidence-only completion is not allowed.

## Acceptance criteria

- AC-1: For both legacy and slot-enabled folders, default and forced lint return
  files with faithful absolute-offset indexes and canonical layout, preserve
  valid slot records, and repair the delegated slot, torn-line, terminal-newline,
  indexed-damage, and dead-space cases with atomic data replacement and the
  index published last.
- AC-2: A fresh index keeps default lint off the full mmap cardinality and
  all-indexed-row parse paths while required spot/layout checks still run;
  `force` performs both full checks, and each removal emits a path-bearing,
  payload-free removed-byte diagnostic suitable for child `00038` to capture.
