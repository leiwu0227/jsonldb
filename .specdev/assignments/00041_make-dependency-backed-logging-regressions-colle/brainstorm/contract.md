# Assignment contract

Kind: bugfix

## Objective and context

Close durable Mission gap `gap-a0276bb3f8ae5eca` by making the dependency-backed
logging regressions added by `00040` collect safely when a declared runtime is
unavailable. This child is delegated by
`.specdev/missions/M00001_implement-the-five-dependency-ordered-roadmap-wo/brainstorm/contract.md`
at approved SHA-256
`7e0824300f897190603ccb138e56aaf5254389c0b40f69967908352465ceac4f` and builds
on the integrated prerequisite outcomes `00034` through `00040`, with source
evidence in
`.specdev/assignments/00040_add-dependency-backed-runtime-coverage-for-versi/outcome.md`.

## Scope and non-goals

- In scope: test-boundary availability handling for the focused GitPython,
  Bokeh, and Matplotlib logging regressions; non-goals are product code, public
  APIs, dependency declarations or ranges, unrelated tests, and weaker runtime
  assertions. Inherit all other Mission scope and non-goals unchanged.

## Expected behavior

The focused module collects without import errors when one or more declared
runtimes are absent, reports the affected regression as skipped, and still runs
each regression unchanged when its real dependencies are available.

## Important decisions

- Gate GitPython and visualization availability independently at the test
  boundary. Completion requires an implemented tracked regression fix; it may
  neither fake dependencies nor replace real-runtime evidence with static
  inspection.

## Constraints and invariants

Inherit the approved Mission constraints, invariants, behavior, and Roadmap
rules unchanged. Preserve Python 3.8 compatibility, headless plotting, isolated
Git setup, and the existing logging, stdio-silence, return-value, and concrete
runtime-result assertions; collection safety does not make product dependencies
optional.

## Delegated and reserved authority

- Delegated: exact pytest availability checks, import placement, skip reasons,
  and focused fixtures within the parent Mission's test authority. Reserved for
  the user: the approved Mission's reserved authority remains unchanged.

## Risks and assumptions

Interpreter environments expose different dependency subsets, and eager or
transitive imports can still fail before a skip is registered. The `00040`
outcome's real-runtime passes remain the behavioral baseline; missing-runtime
evidence must not supersede them.

## Verification authority

Run focused collection and execution in a dependency-incomplete runtime, then
run the same regressions under the available real dependencies; also run the
Mission-required Python 3.8 syntax/API-floor audit and seven-file logical-line
inventory. The full tracked suite remains reserved for final Mission
verification.

## Acceptance criteria

- AC-1: Absence of GitPython, Bokeh, or Matplotlib does not produce a collection
  error in the focused logging regressions; each affected regression is
  explicitly skipped without unnecessarily disabling an independently runnable
  regression.
- AC-2: With the real declared dependencies available, both regressions execute
  and retain their established path-bearing logging, silent stdio, return/state,
  and concrete visualization-result assertions.
