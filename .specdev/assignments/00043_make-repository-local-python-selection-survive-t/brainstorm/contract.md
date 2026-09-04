# Assignment contract

Kind: change

<!-- Keep this proportional: reference existing project context, state only change-specific decisions, and use the fewest independent observable acceptance criteria (normally 1-5). -->

## Objective and context

Resolve Mission gap `gap-cdf3d8d11b0a6762` by making the repository-local
Python runtime provisioned by prerequisite `00042` remain selected when the
Mission controller launches the frozen final verification through its login
shell execution boundary. This is a delta under
`.specdev/missions/M00001_implement-the-five-dependency-ordered-roadmap-wo/brainstorm/contract.md`
(approved SHA-256
`7e0824300f897190603ccb138e56aaf5254389c0b40f69967908352465ceac4f`)
and follows the completed outcomes for Assignments `00034` through `00042`
referenced by the Mission's `design/assignments.yaml`, most directly
`.specdev/assignments/00042_establish-a-dependency-complete-python-runtime-f/outcome.md`.

## Scope and non-goals

- In scope: repository-scoped handoff or launcher wiring that carries the
  `00042` runtime selection into the independent `/bin/sh -lc` process used by
  final verification.
- Non-goals: changing product or test code, reprovisioning dependencies,
  changing the Mission contract or frozen command, modifying system or user
  shell startup, or altering SpecDev behavior outside this repository.

## Expected behavior

After the recorded repository-local handoff is used to launch the Mission,
every unqualified `python3` in the unchanged final command resolves inside the
repository runtime even after the verifier crosses its login-shell boundary.

## Important decisions

Fix the boundary itself with a repository-local shell-environment handoff; an
additional activation instruction or a PATH change that exists only before
`/bin/sh -lc` is insufficient.

## Constraints and invariants

Inherit all unchanged Mission constraints, non-goals, behavior, and invariants.
Keep the runtime and handoff ignored and repository-scoped, preserve the
Python-3.8-or-newer floor, use one dependency-complete interpreter for every
command segment, and make an implementation change rather than completing from
evidence alone.

## Delegated and reserved authority

- Delegated: the repository-local environment-hook and launcher details needed
  to preserve `00042`'s runtime selection across the current verifier boundary,
  plus focused probes of that boundary.
- Reserved for the user: all unchanged authority reserved by the approved
  Mission, including changes to its command, contract, executor class or route,
  bypass policy, dependency declarations, and product or test tree.

## Risks and assumptions

The second failed receipt
`.specdev/missions/M00001_implement-the-five-dependency-ordered-roadmap-wo/review/final-verification.json`
shows that ordinary PATH inheritance is reset at the login-shell boundary. The
repository runtime from `00042` is assumed intact; if the platform shell cannot
consume a repository-local hook, stop rather than changing user-global shell
configuration or weakening verification.

## Verification authority

Focused probes may cross the same `/bin/sh -lc` boundary to assert interpreter
identity and imports, and may run tracked-suite collection through that
boundary. The full tracked suite and frozen final command remain reserved to
the parent Mission's integrated verification.

## Acceptance criteria

- AC-1: Launching through the recorded repository handoff causes a fresh
  `/bin/sh -lc` verifier process to resolve unqualified `python3` to the
  repository-local Python-3.8-or-newer runtime and import every declared runtime
  dependency without system or user-global changes.
- AC-2: Through that same boundary, the unchanged tracked suite collects
  successfully, including its real GitPython, Bokeh, and Matplotlib regression
  nodes, so the parent can execute the frozen final command verbatim.
