# Implementation plan

## Context

The approved contract and published Roadmap designs are authoritative. Current
inspection confirmed that Git and visualization remain in the package source,
the `FolderDB` facade, exports, direct dependencies, README, examples, and
capability-specific tests even though the Roadmap has removed those product
capabilities. NumPy remains a core serializer dependency. Historical SpecDev
records and legitimate repository/install references to Git are outside this
removal.

Relevant bounded context:

- `.specdev/project_notes/roadmap/designs/jsonl_file_store/index_integrity_and_lint.md`
- `.specdev/project_notes/roadmap/designs/folder_database/metadata_and_timespec.md`
- `.specdev/project_notes/roadmap/designs/source_code_folder_structure.md`
- `.specdev/project_notes/roadmap/forecast.md`

**Implementation Guides:** []

**Review Guides:** []

The catalogued frontend and API-security guides do not govern removal of these
Python package capabilities; the approved contract and Roadmap source, API,
dependency, testing, and line-cap rules provide the applicable constraints.

## Tasks

1. **T-1 (AC-1):** Delete the Git and visualization modules, remove the three
   `FolderDB` version-control methods, and remove both names and capability
   claims from the package initializer. Add focused negative-surface
   regressions for module absence, facade absence, and package inventory.
2. **T-2 (AC-2):** Remove the three capability-only direct dependencies while
   retaining NumPy, regenerate the universal Python 3.8 lock with the
   documented `uv pip compile` command, remove capability documentation and
   notebooks, and delete or rewrite only capability-specific maintained tests.
3. **T-3 (AC-1, AC-2, AC-3):** Run focused core and removal regressions, build
   and inspect an installed artifact in an isolated Python 3.12 environment
   without GitPython, Bokeh, or Matplotlib, then run the canonical full suite,
   Python 3.8 parse audit, all remaining Roadmap source caps, inventory sweeps,
   and diff hygiene.
4. **T-4 (AC-1, AC-2, AC-3):** Record exact receipts and any deviations in
   `implementation/progress.json`, summarize contract coverage in `outcome.md`,
   and complete the worker-result envelope for independent review.
