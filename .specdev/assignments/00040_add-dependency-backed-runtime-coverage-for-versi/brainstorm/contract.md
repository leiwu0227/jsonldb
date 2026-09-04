# Assignment contract

Kind: change

## Objective and context

Close durable Mission gap `gap-a0276bb3f8ae5eca` by adding tracked,
dependency-backed runtime regression coverage for version-control and
visualization logging. This child is delegated by
`.specdev/missions/M00001_implement-the-five-dependency-ordered-roadmap-wo/brainstorm/contract.md`
at approved SHA-256
`7e0824300f897190603ccb138e56aaf5254389c0b40f69967908352465ceac4f` and builds
on completed Assignment outcomes `00034`, `00035`, `00036`, `00037`, `00038`,
and `00039`; `00038`'s authoritative gap evidence is
`.specdev/assignments/00038_replace-runtime-prints-with-logging-and-add-boun/outcome.md`.

## Scope and non-goals

- In scope: tracked focused tests that exercise the converted `vercontrol` and
  `visual` diagnostics through the declared GitPython, Bokeh, and Matplotlib
  runtimes; non-goals are production-behavior, public-API, dependency-range,
  report, or unrelated test changes. Inherit all other Mission scope and
  non-goals unchanged.

## Expected behavior

Representative version-control success and visualization discovery/empty-data
paths emit path-bearing logger records while standard output and error remain
silent under the real declared dependencies; their existing operational and
return behavior is unchanged.

## Important decisions

- Completion requires tracked regression-test implementation, not an
  evidence-only rerun. Tests may isolate unrelated setup, but must not replace
  GitPython, Bokeh, or Matplotlib with fake modules or reduce visualization
  evidence to static inspection.

## Constraints and invariants

Inherit the approved Mission constraints, invariants, behavior, and Roadmap
rules unchanged. Keep visualization tests headless, avoid reliance on global
Git identity or configuration, and use content rather than mtime where
filesystem replacement evidence is relevant.

## Delegated and reserved authority

- Delegated: focused fixtures, temporary-repository setup, backend-safe cleanup,
  and exact log assertions within the parent Mission's test authority. Reserved
  for the user: the approved Mission's reserved authority remains unchanged.

## Risks and assumptions

The completed outcomes at
`.specdev/assignments/00034_harden-jsonl-durability-and-atomic-index-and-con/outcome.md`,
`.specdev/assignments/00035_implement-metadata-slots-and-the-folderdb-metada/outcome.md`,
`.specdev/assignments/00036_implement-atomic-folder-wide-metadata-slot-width/outcome.md`,
`.specdev/assignments/00037_make-lint-slot-aware-and-damage-aware-while-pres/outcome.md`,
and `.specdev/assignments/00039_make-standalone-lint-converge-when-a-metadata-on/outcome.md`
remain accepted inputs. Runtime coverage can otherwise be environment-sensitive
through Git configuration or GUI backends, so tests must use repository-local
setup and a non-interactive plotting backend.

## Verification authority

Run only focused version-control and visualization logging tests under the real
declared dependencies, plus the required Python 3.8 syntax/API-floor audit and
seven-file logical-line inventory. The full tracked suite remains reserved for
the Mission's final integrated verification.

## Acceptance criteria

- AC-1: A real GitPython-backed version-control operation produces the expected
  path-bearing logger record with no standard-output or standard-error
  diagnostic and preserves its established result.
- AC-2: Real Bokeh and Matplotlib visualization execution produces the expected
  path-bearing discovery and empty-data logger records with no standard-output
  or standard-error diagnostic and preserves the established plot results.
