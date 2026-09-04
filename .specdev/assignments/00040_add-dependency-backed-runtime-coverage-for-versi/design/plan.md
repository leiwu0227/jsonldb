# Implementation plan

Fresh knowledge search: `dependency runtime coverage version control visualization logging tests` (precise mode). Relevant current guidance: `.specdev/knowledge/workflow/wsl2-filesystem.md`; assertions use content and runtime objects rather than mtime. The search also identified Assignment `00038`'s outcome as the verified source of the coverage gap. The other parent-selected knowledge paths were not relevant to this test-only change. After the default interpreter lacked GitPython, the allowed symptom search `GitPython missing dependency runtime Python interpreter environment` confirmed `00038`'s dependency-runtime limitation; acceptance therefore uses locally cached real distributions within the declared dependency ranges.

**Implementation Guides:** []

**Review Guides:** [frontend]

## Tasks

1. **T-1 — Add real GitPython runtime coverage (AC-1).** Add a tracked focused test that initializes a temporary repository through `jsonldb.vercontrol`, verifies the established return and repository state through real GitPython, and asserts a path-bearing logger record with silent stdout/stderr without relying on global Git identity or configuration.
2. **T-2 — Add real visualization runtime coverage (AC-2).** Add tracked headless coverage that executes both the Bokeh and Matplotlib folder visualizers against an empty JSONL table, verifies their established plot results, and asserts path-bearing discovery and empty-data logger records with silent stdout/stderr.
3. **T-3 — Run bounded acceptance and invariant checks (AC-1, AC-2).** Run only the new focused runtime tests, audit all package syntax against the Python 3.8 AST floor, and inventory logical lines in all seven governed source files against Mission caps.
