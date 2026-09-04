# Assignment contract

Kind: change

<!-- Keep this proportional: reference existing project context, state only change-specific decisions, and use the fewest independent observable acceptance criteria (normally 1-5). -->

## Objective and context

Resolve Mission gap `gap-cdf3d8d11b0a6762` by provisioning a durable,
repository-scoped Python runtime that the Mission verifier can select as
`python3` for its frozen final command. This is a delta under
`.specdev/missions/M00001_implement-the-five-dependency-ordered-roadmap-wo/brainstorm/contract.md`
(approved SHA-256
`7e0824300f897190603ccb138e56aaf5254389c0b40f69967908352465ceac4f`).
It depends on the completed outcomes at
`.specdev/assignments/00034_harden-jsonl-durability-and-atomic-index-and-con/outcome.md`,
`00035_implement-metadata-slots-and-the-folderdb-metada/outcome.md`,
`00036_implement-atomic-folder-wide-metadata-slot-width/outcome.md`,
`00037_make-lint-slot-aware-and-damage-aware-while-pres/outcome.md`,
`00038_replace-runtime-prints-with-logging-and-add-boun/outcome.md`,
`00039_make-standalone-lint-converge-when-a-metadata-on/outcome.md`,
`00040_add-dependency-backed-runtime-coverage-for-versi/outcome.md`, and
`00041_make-dependency-backed-logging-regressions-colle/outcome.md`, all under
`.specdev/assignments/`.

## Scope and non-goals

- In scope: provision an isolated runtime within the repository's ignored
  workflow/runtime area, populate it with the dependencies declared by
  `setup.py`, and provide the executor handoff that makes the frozen command's
  unqualified `python3` use that runtime.
- Non-goals: changing product or test code, dependency declarations or
  lockfiles, the Mission contract or verification command, or any system-wide
  Python installation.

## Expected behavior

Using the recorded handoff, every `python3` segment of the unchanged final
verification command resolves to one supported interpreter with pandas,
orjson, NumPy, GitPython, Bokeh, and Matplotlib importable, so test collection
reaches the suite instead of failing on missing dependencies.

## Important decisions

Build a repository-scoped environment from locally available, declaration-
compatible distributions and a supported interpreter; do not depend on the
purgeable `/private/tmp/jsonldb-attempt14-runtime-py312` tree or mutate the
dependency-incomplete Homebrew Python 3.14 installation.

## Constraints and invariants

Inherit all unchanged Mission constraints, non-goals, behavior, and invariants.
The provisioned interpreter must be Python 3.8 or newer and all required
packages must resolve from the same selected environment. This implementation
child must create or refresh the runtime and its deterministic selection
handoff; evidence from a pre-existing temporary environment alone cannot
complete it.

## Delegated and reserved authority

- Delegated: the ignored repository-local runtime location, compatible package
  versions, installation mechanics, and PATH or activation handoff needed to
  select it for the current Mission executor.
- Reserved for the user: any change to the frozen command, dependency
  declarations, Mission execution policy, product or test tree, or any other
  authority reserved by the approved Mission contract.

## Risks and assumptions

The default `/opt/homebrew/bin/python3` is present but dependency-incomplete,
while prior outcomes `00040` and `00041` prove compatible distributions existed
locally for Python 3.12 without proving their continued availability. If a
complete repository-scoped runtime cannot be provisioned from available
artifacts, report a blocker rather than weakening dependency coverage or
falling back to the purgeable temporary tree. The WSL2 filesystem note is not
applicable to the current macOS executor.

## Verification authority

Focused runtime probes may confirm the selected interpreter and package
versions/imports, and `python3 -m pytest --collect-only -q tests` may confirm
dependency-complete collection and presence of the real GitPython/Bokeh/
Matplotlib regressions. The full tracked suite and complete frozen command
remain reserved to the parent Mission's final integrated verification.

## Acceptance criteria

- AC-1: The recorded repository-scoped handoff makes unqualified `python3`
  resolve to a Python 3.8-or-newer environment that imports every declared
  runtime dependency, without relying on `/private/tmp` or changing a global
  interpreter.
- AC-2: With that handoff, the unchanged tracked suite collects successfully
  without missing-dependency errors and includes the real GitPython, Bokeh, and
  Matplotlib regression tests, leaving the parent able to run its frozen final
  command verbatim.
