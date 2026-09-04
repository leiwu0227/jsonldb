# Assignment contract

Kind: bugfix

<!-- Keep this proportional: reference existing project context, state only change-specific decisions, and use the fewest independent observable acceptance criteria (normally 1-5). -->

## Objective and context

Resolve Mission gap `gap-f4323203e2f7d1e0` by making standalone lint converge
when a metadata-only slot line lacks its terminal newline. This is a narrow
delta to the approved Mission contract at
`.specdev/missions/M00001_implement-the-five-dependency-ordered-roadmap-wo/brainstorm/contract.md`
(SHA-256 `7e0824300f897190603ccb138e56aaf5254389c0b40f69967908352465ceac4f`),
building on the completed outcomes for Assignments `00034`, `00035`, `00036`,
`00037`, and `00038` at their respective `.specdev/assignments/<id>_*/outcome.md`
paths; `00037` is the gap evidence source.

## Scope and non-goals

- In scope: normalize the missing terminal newline on a recognized version-1
  metadata-only slot during standalone `lint_jsonl`, preserve its metadata, and
  add focused convergence coverage. Broader lint, slot-width, folder, reporting,
  and prerequisite behavior is out of scope; inherit every other Mission
  non-goal.

## Expected behavior

Default and forced standalone lint turn the affected metadata-only file into a
canonical newline-terminated slot with an empty faithful index. A subsequent
lint leaves that result unchanged instead of rewriting it again.

## Important decisions

Standalone lint may normalize the terminator of an already recognized slot but
must neither invent a slot for a legacy file nor infer or impose a configured
folder width. Convergence is judged by content and repair observations, not
filesystem timestamp changes.

## Constraints and invariants

Inherit the approved Mission constraints, behavior, and Roadmap rules unchanged,
including metadata-record preservation, atomic data replacement with index
publication last, the fast/forced split, Python 3.8 compatibility, public-call
defaults and return shapes, and all seven source caps.

## Delegated and reserved authority

- Delegated: choose the smallest internal normalization and focused test
  structure within the parent Mission's module and line-cap boundaries.
- Reserved for the user: all authority reserved by the approved Mission,
  including changes to slot format or width policy, repair ordering, compatibility,
  source caps, or scope beyond this gap.

## Risks and assumptions

The gap is limited to standalone lint of a recognized metadata-only slot: the
folder-enabled path already converges. The WSL2 filesystem guidance warns that
sub-second mtimes are unreliable, and the capped core module has little line
budget, so evidence must compare bytes/diagnostics and any implementation must
remain within the inherited cap.

## Verification authority

Run focused default/forced standalone-lint tests for the affected metadata-only
file, a second-pass convergence check, and directly adjacent canonical-slot and
legacy regressions, plus the Mission-required Python 3.8 syntax/API-floor audit
and seven-file logical-line inventory. The full tracked suite remains reserved
for final Mission integration, and evidence-only completion is not allowed.

## Acceptance criteria

- AC-1: Default and forced standalone lint each preserve the version-1 metadata
  record, add exactly one terminal newline, and publish a faithful empty index;
  rerunning lint is byte-stable and emits no further layout-repair observation,
  while canonical metadata-only slots and legacy standalone files retain their
  inherited behavior.
