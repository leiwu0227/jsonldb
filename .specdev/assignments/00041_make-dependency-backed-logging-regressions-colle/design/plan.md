# Implementation plan

Fresh knowledge search: `dependency logging regression collection unavailable runtime`
(precise mode). The relevant result was the verified prerequisite outcome
`.specdev/assignments/00040_add-dependency-backed-runtime-coverage-for-versi/outcome.md`,
which establishes the real-runtime behavioral baseline and the default
interpreter's missing-GitPython symptom. The parent-selected living-knowledge
paths `knowledge/workflow/adhoc-history.md`, `knowledge/_index.md`, and
`knowledge/workflow/wsl2-filesystem.md` were not relevant to this test-boundary
import/skip fix and were therefore not loaded. The default interpreter then
failed earlier on missing core runtime `orjson`; the permitted symptom search
`orjson missing default interpreter dependency collection` returned the same
`00040` prerequisite outcome as the only relevant historical lead. Evidence
will therefore use an isolated Python 3.12 path that includes core and
visualization runtimes while genuinely omitting GitPython.

**Implementation Guides:** []

**Review Guides:** []

## Tasks

1. **T-1 (AC-1, AC-2) — Isolate dependency imports and skips.** Move GitPython
   and visualization imports behind their respective focused test boundaries,
   use explicit `pytest.importorskip` reasons before importing the dependent
   product modules, and preserve every existing runtime assertion unchanged.
2. **T-2 (AC-1) — Verify dependency-incomplete collection and execution.** Run
   only the focused module under a genuinely dependency-incomplete isolated
   interpreter path, confirming collection succeeds and the GitPython-dependent
   regression skips without disabling the independently runnable visualization
   regression.
3. **T-3 (AC-2) — Verify real runtimes and Mission invariants.** Run the same
   focused regressions with the locally available real GitPython, Bokeh, and
   Matplotlib distributions, then run the Python 3.8 syntax/API-floor audit and
   seven-file logical-line inventory. Record exact receipts and summarize both
   acceptance criteria in the required delivery artifacts.
