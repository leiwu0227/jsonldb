# Assignment contract

Kind: feature

## Objective and context

Implement the logging and persistent-report child delegated by
`.specdev/missions/M00001_implement-the-five-dependency-ordered-roadmap-wo/brainstorm/contract.md`
at approved SHA-256 `7e0824300f897190603ccb138e56aaf5254389c0b40f69967908352465ceac4f`.
This delta builds on the completed outcomes for Assignments `00034`, `00035`,
`00036`, and `00037`, whose path-bearing durability and lint diagnostics supply
the report findings.

## Scope and non-goals

- In scope: replace package runtime standard-output diagnostics with logging and
  add bounded replacement integrity and lint reports for database open and
  `lint_db`; add focused evidence for those behaviors.
- Non-goals: new damage detection or repair, changes to the diagnostic-triggering
  behavior delivered by prerequisites, or any expansion beyond the parent
  Mission; inherit all other Mission non-goals.

## Expected behavior

Every open replaces `.jsonldb/integrity.log` and every database lint replaces
`.jsonldb/lint.log` with a timestamped operation header and bounded, path-bearing
findings; a clean run contains only the header. Capture is limited to the opened
database, open remains observational for table files, ordinary reads and writes
do not touch reports, and runtime diagnostics use logging instead of standard
output.

## Important decisions

- Complete the runtime logging conversion before wiring logger capture; keep
  capture and report formatting in a package-independent `reports` leaf, with
  `FolderDB` owning only the open and lint hook points.
- Replace each report as one run-scoped artifact, cap both per-entry removed
  content and total findings, and record an omitted count when the total cap is
  exceeded.

## Constraints and invariants

Inherit the approved Mission constraints, behavior, and Roadmap rules unchanged,
including Python 3.8 compatibility and all seven source caps. Findings expose no
payload beyond capped removed bytes decoded with replacement; capture must not
leak records across folders or invocations, and `.jsonldb/` remains invisible to
table discovery, hierarchy operations, and clearing.

## Delegated and reserved authority

- Delegated: choose fixed entry/count bounds, precise report formatting, logger
  levels, internal helpers, and focused test organization within the parent
  Mission's delegated authority.
- Reserved for the user: all authority reserved by the approved Mission,
  including any change to scope, open-versus-lint responsibilities, public
  behavior, compatibility, or source caps.

## Risks and assumptions

The prerequisite outcomes are accepted inputs and provide the anomaly signals
this child captures. Handler lifetime and path filtering can otherwise duplicate
or cross-contaminate reports, and report creation can accidentally enter table
enumeration; tests must use content rather than mtime for replacement evidence
because the documented WSL2 filesystem may have one-second timestamp resolution.

## Verification authority

Run focused report/logging tests covering clean and anomalous runs, replacement,
bounds, folder scoping, handler cleanup, open non-repair, ordinary-operation
silence, and hidden-directory behavior; run the package AST print/stream-write
inventory, Python 3.8 syntax/API-floor audit, and required seven-file logical-line
inventory. The full tracked suite remains reserved for the Mission's final
integrated verification.

## Acceptance criteria

- AC-1: Package runtime diagnostics no longer write to standard output or error;
  equivalent logger records identify the relevant file path without changing the
  operation's non-transport behavior.
- AC-2: Every database open and `lint_db` run replaces its respective bounded
  `.jsonldb/` report; clean runs contain only the header, anomalous entries carry
  the required path and bounded detail/removal data with an omitted count when
  capped, capture excludes other folders, open does not repair tables, ordinary
  operations do not write reports, and the report directory is absent from
  discovery, hierarchy, and clearing behavior.
