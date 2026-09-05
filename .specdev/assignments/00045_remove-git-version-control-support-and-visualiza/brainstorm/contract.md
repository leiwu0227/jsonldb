# Assignment contract

Kind: change

<!-- Keep this proportional: reference existing project context, state only change-specific decisions, and use the fewest independent observable acceptance criteria (normally 1-5). -->

## Objective and context

Remove Git version-control and plotting/visualization support completely from
JSONLDB, leaving a smaller package focused on JSONL storage, the DataFrame
adapter, folder databases, metadata slots, lint, and integrity reports.

The published Roadmap now excludes both capabilities and is the target design
for this change. `big_picture.md` still carries its older high-level inventory;
updating that separately governed living-knowledge note is a reserved follow-up,
not authority for retaining the removed product features.

## Scope and non-goals

- In scope: delete the `vercontrol` and `visual` package modules; remove the
  `FolderDB.commit`, `FolderDB.revert`, and `FolderDB.version` facade; remove the
  two submodule names and examples from the package inventory; remove GitPython,
  Bokeh, and Matplotlib from packaging and the generated development lock;
  remove or rewrite capability-specific tests; delete the version-control and
  visualization example notebooks; and remove their feature, usage, dependency,
  and package-description claims from the README and shipping metadata.
- In scope: add focused negative-surface regressions proving the removed modules,
  facade methods, inventory entries, and direct third-party dependencies are
  absent while the remaining package still imports and operates without those
  libraries installed.
- Non-goals: removing NumPy support or its dependency, because the core JSONL
  serializer still promises NumPy-scalar support; changing JSONL data formats;
  deleting or modifying any database's existing `.git` directory or history;
  retaining compatibility shims or deprecation stubs; rewriting historical
  SpecDev outcomes, contracts, or receipts; or fixing unrelated gaps from
  Discussion D00002.

## Expected behavior

`jsonldb.vercontrol` and `jsonldb.visual` no longer exist in the installed or
source package. `FolderDB` exposes no Git methods. Importing JSONLDB requires
none of GitPython, Bokeh, or Matplotlib, and package installation no longer
declares them. Existing database contents, including user-owned Git metadata,
are left untouched. Core storage, DataFrame, folder, metadata, lint, and report
behavior remains unchanged.

## Important decisions

- Removal is immediate rather than deprecated: old imports and facade calls
  fail through normal Python missing-module/missing-attribute behavior.
- Delete capability-only tests and examples rather than preserving dead
  demonstrations. Rewrite mixed-purpose tests so their remaining storage or
  reporting coverage survives.
- Retain NumPy as a declared dependency because its support is part of the
  current file-store contract independently of visualization.
- Regenerate `requirements-dev.lock` from `setup.py` and
  `requirements-dev.in`; do not hand-edit generated dependency closure.
- Do not treat every textual occurrence of Git as product support. The source
  repository, a Git-based installation URL, hidden-directory safety examples,
  and immutable historical records may still name Git where factually needed.

## Constraints and invariants

- Respect every remaining Roadmap design rule and source line cap. Removal may
  reduce capped files; it must not alter the limits.
- Do not edit published Roadmap design notes or `big_picture.md` under this
  Assignment unless their owning governance workflow explicitly authorizes it.
- Do not touch existing database directories, repositories, tables, indexes,
  controls, or reports as part of migration: this is a package/API removal only.
- Preserve Python 3.8 syntax compatibility and the current tracked
  `unit_tests/` layout for every remaining module.

## Delegated and reserved authority

- Delegated after approval: delete and revise the bounded package, packaging,
  lockfile, README, examples, and tests described above; choose exact test
  organization; run focused tests and the full canonical suite; and make the
  single Assignment delivery commit after required implementation review.
- Reserved for the user: any compatibility or deprecation period; removing
  NumPy; fixing unrelated audit gaps; changing data formats; changing or
  deleting user database contents; and separately curating `big_picture.md`.

## Risks and assumptions

- This is a breaking public-API change. Internal consumers using either module
  or the three `FolderDB` methods must migrate before upgrading.
- Lockfile regeneration may retain a package transitively for a remaining
  dependency; acceptance concerns direct declared dependencies and verified
  dependency provenance, not substring absence without provenance.
- Capability references in immutable historical workflow records are historical
  evidence and are not stale product documentation.

## Verification authority

- Focused tests for changed modules and negative dependency/import probes:
  authorized after repository instructions are satisfied.
- Full canonical suite: authorized by approval of this contract.
- Packaging evidence: regenerate the universal hashed lock through its documented
  command, inspect direct requirements, and install/import in an isolated
  dependency environment without GitPython, Bokeh, or Matplotlib.

## Acceptance criteria

- AC-1: The source and installed package contain no `vercontrol` or `visual`
  module, `FolderDB` has none of the three Git facade methods, `jsonldb.__all__`
  names neither capability, and focused regressions prove the intentional
  missing-module and missing-attribute surface.
- AC-2: `setup.py`, generated `requirements-dev.lock`, README, current examples,
  and current non-historical tests contain no JSONLDB-owned Git/version-control
  or plotting/visualization capability; GitPython, Bokeh, and Matplotlib are not
  direct package requirements, while NumPy remains declared for core value
  serialization. Legitimate repository transport and hidden-directory safety
  references are not removed merely for containing a capability name.
- AC-3: The remaining package installs and imports in an isolated environment
  without the removed libraries, focused core regressions pass, and the full
  canonical suite passes under Python 3.12 plus a Python 3.8 parse check, with
  all remaining Roadmap source caps satisfied.
