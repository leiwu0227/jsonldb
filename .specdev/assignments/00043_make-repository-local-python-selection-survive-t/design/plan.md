# Implementation plan

## Context

The fresh objective search `specdev knowledge search "repository local python
login shell environment handoff"` was run before planning. Relevant current
context was limited to:

- `.specdev/knowledge/workflow/adhoc-history.md`
- `.specdev/knowledge/_index.md`
- `.specdev/knowledge/workflow/wsl2-filesystem.md`
- `.specdev/assignments/00042_establish-a-dependency-complete-python-runtime-f/outcome.md`

The first three paths contain workflow-history and filesystem cautions but no
current shell-startup recipe. Assignment `00042` supplies the repository-local
runtime and launcher that this delta must preserve. Current probes confirmed
that macOS `/etc/profile` reorders inherited `PATH` ahead of that runtime in a
fresh `/bin/sh -lc`, while an exported Bash `python3` function survives the
same boundary and delegates to the repository-local interpreter.

**Implementation Guides:** api-security

**Review Guides:** none

## Tasks

1. **T-1 (AC-1):** Add an ignored repository-local environment launcher that
   validates the existing runtime, exports only its absolute interpreter path,
   and exports a narrowly scoped `python3` shell function before executing its
   command; make the repository-local SpecDev wrapper delegate through it.
   Keep the handoff repository-scoped and avoid
   product, test, user-profile, system-profile, dependency, contract, and
   Mission-command changes.
2. **T-2 (AC-1):** Through the recorded launcher environment and a fresh
   `/bin/sh -lc`, verify that unqualified `python3` is the exported function,
   resolves to Python 3.8 or newer, and imports every declared runtime
   dependency from the one repository-local environment.
3. **T-3 (AC-2):** Through the same boundary, run only tracked-suite collection
   and assert that the real GitPython and combined Bokeh/Matplotlib regression
   nodes are present. Record exact commands, revision, durations, and results
   in `implementation/progress.json`, then summarize both acceptance results in
   `outcome.md`.
