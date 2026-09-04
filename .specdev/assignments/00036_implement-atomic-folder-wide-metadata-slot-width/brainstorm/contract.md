# Assignment contract

Kind: feature

<!-- Keep this proportional: reference existing project context, state only change-specific decisions, and use the fewest independent observable acceptance criteria (normally 1-5). -->

## Objective and context

Implement the Mission child “Implement atomic folder-wide metadata-slot width
migration” under
`.specdev/missions/M00001_implement-the-five-dependency-ordered-roadmap-wo/brainstorm/contract.md`
(approved SHA-256
`7e0824300f897190603ccb138e56aaf5254389c0b40f69967908352465ceac4f`).
This delta follows completed prerequisites `00034` and `00035` and inherits all
unchanged Mission constraints, non-goals, behavior, and reserved authority.

## Scope and non-goals

- In scope: `FolderDB.set_meta_slot_bytes`, persisted folder width, complete
  preflight of unsafe shrinkage, atomic replacement of only tables needing the
  requested width, and configured-slot creation for new tables.
- Non-goals: slot-envelope or metadata-API redesign, lint repair, stronger
  crash guarantees, and every other exclusion inherited from the Mission.

## Expected behavior

Calling `set_meta_slot_bytes` with its 4096-byte default enables a legacy folder
or changes an enabled folder's width. The target is recorded as
`meta_slot_bytes` without losing other configuration, every mismatched table is
rewritten with the same rows and metadata, and later-created tables start with
that width. Before shrinking, all existing metadata records are fit-checked;
failure names every blocking table and changes neither configuration nor tables.

## Important decisions

- Persist the target folder width before per-table migration so an interruption
  leaves an explicit repair target; each table replacement is atomic, and a
  retry of the same operation converges mixed complete widths.
- Readers continue to measure line one, while configured writers use the folder
  width. Control files never receive slots, and already-conforming tables are
  not rewritten.

## Constraints and invariants

Preserve `timespec` and all unrelated configuration, table rows and metadata,
absolute index offsets, and atomic-last index publication. No table or config
bytes may change before an unsafe-shrink preflight succeeds. All inherited
compatibility, Python 3.8, immutable-legacy-test, and seven-file line-cap
invariants remain binding.

## Delegated and reserved authority

- Delegated: migration helper factoring, same-directory temporary naming and
  cleanup, deterministic blocker ordering, and focused fault-injection seams
  within the Mission's published module boundaries.
- Reserved for the user: all authority reserved by the Mission, including any
  weaker publish, refusal, compatibility, or recovery rule and any scope beyond
  this width operation.

## Risks and assumptions

Prerequisites
`.specdev/assignments/00034_harden-jsonl-durability-and-atomic-index-and-con/outcome.md`
and
`.specdev/assignments/00035_implement-metadata-slots-and-the-folderdb-metada/outcome.md`
are assumed integrated. A process interruption can leave different tables at
old and target widths, but never a partially rewritten table or lost rows or
metadata; recovery must be retryable. WSL2 mtime resolution makes content and
readability evidence preferable to mtime-only assertions.

## Verification authority

Focused tests may cover enablement, resize/no-op selection, configured new-table
creation, fit refusal, metadata/row/index preservation, and injected failures at
configuration and table publication boundaries. Run the narrow relevant
durability and metadata-slot regressions, the required seven-file logical-line
inventory, and a Python-3.8 syntax/API audit; this child does not authorize the
Mission's final full suite.

## Acceptance criteria

- AC-1: Enabling or resizing records the requested width, atomically rewrites
  only nonconforming tables while preserving their rows and metadata, maintains
  valid absolute indexes, and gives every subsequently created table that slot;
  omitting the argument selects 4096 bytes.
- AC-2: An unsafe shrink refuses with every blocking table named and leaves the
  prior configuration and every table byte-for-byte unchanged.
- AC-3: Injected interruption at each publication boundary leaves whole old or
  new configuration/table files with no row or metadata loss, and rerunning the
  operation completes the migration without rewriting conforming tables.
