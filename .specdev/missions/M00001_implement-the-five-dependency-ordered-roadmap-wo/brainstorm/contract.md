# Mission contract

Kind: mission

<!-- Keep this proportional: reference existing project context, state only change-specific decisions, and use the fewest independent observable acceptance criteria (normally 1-5). -->

## Objective and context

Implement the five dependency-ordered work packages selected in
`.specdev/project_notes/roadmap/todo.md` and close all ten gaps in
`.specdev/project_notes/roadmap/forecast.md`. The published Roadmap designs are
the target state. Work starts from `master`; discarded Assignment `00008` and
the `post-fc22cba` branch are not implementation inputs.

## Scope and non-goals

- In scope: JSONL and index durability; atomic control files; the line-one
  metadata-slot format and `FolderDB` APIs; folder-wide slot-width migration;
  slot-aware and damage-aware lint; package logging; bounded integrity and lint
  reports; focused failure-path tests and integrated regressions.
- Non-goals: catalogs, range replacement, or any other code-only feature from
  `00008` or `post-fc22cba`; locking,
  multi-writer coordination, fsync or stronger power-loss guarantees; changing
  the established key model, hierarchy semantics, Git or visualization behavior
  beyond the required diagnostic-transport change, or DataFrame behavior beyond
  metadata propagation.

## Expected behavior

Legacy unslotted databases and existing public calls continue to work. Index
and control-file publication is atomic within the documented process-crash
model, reads skip torn rows, and grown upserts retain an old or new copy across
interruption.

Slot-enabled folders store one opaque version-1 metadata record in a fixed-width
line-one envelope. Row publication, metadata publication, migration, reads, and
lint obey the Roadmap ordering and repair rules. Explicit width changes are the
only normal full-folder migration.

Open and lint expose bounded persistent reports built from path-bearing logger
records. Open observes without rewriting tables; lint performs the documented
repairs. Ordinary reads and writes never touch the reports, and no package
runtime diagnostic uses standard-output `print`.

## Important decisions

- Execute the five packages in Todo order as five planned sequential Mission
  children. Their contracts are narrow deltas inheriting this Mission authority.
- Treat the published Roadmap designs as authoritative when Todo or Forecast is
  abbreviated. Existing code-only behavior does not expand scope.
- Every child must obey all applicable published Roadmap design rules and source
  targets, not only the abbreviated gap text. A child may not defer a line-cap
  violation to a later child or evade a cap by moving governed behavior into an
  unlisted module.
- Every child contract and integration must run a logical-line inventory for all
  seven governed source files, even when that child changes only a subset.
- Implement forecast item 1 first inside the final reporting child before that
  child adds logger capture and report files.

## Constraints and invariants

- Preserve Python 3.8 compatibility, current public-call defaults and return
  shapes, absolute index offsets, compact sorted indexes, inclusive range
  semantics, per-instance timespec, and the lint fast/forced split.
- Control files never carry metadata slots. `db.meta` keeps its existing
  non-atomic policy; only `config.meta` and `h.meta` gain atomic replacement.
- Treat every applicable Roadmap source cap as a hard child-integration and
  final-delivery invariant: `jsonldb/metaslot.py` at most 250 logical lines,
  `jsonldb/jsonlfile.py` at most 950, `jsonldb/reports.py` at most 150,
  `jsonldb/folderdb.py` at most 1250, `jsonldb/jsonldf.py` at most 150,
  `jsonldb/vercontrol.py` at most 160, and `jsonldb/visual.py` at most 500.
  Count an unterminated final line. Refactor duplication within the approved
  module boundaries as needed.
- Tracked delivery tests live under `tests/`. Existing `tests/legacy/` files are
  an immutable compatibility baseline; add focused tests outside that subtree.
  The ignored local `unit_tests/` tree is neither an implementation input nor a
  Mission output. This resolves the current test-tree mismatch without waiving
  any product source target, dependency rule, or line cap.
- Preserve unrelated user changes and SpecDev state. Mission work may mutate
  only this JSONLDB worktree and must not import changes from discarded branches
  or Assignments. Children must derive implementations from the Roadmap and the
  current tracked tree, not inspect discarded implementation artifacts.

## Delegated and reserved authority

- Delegated: internal helper names and factoring within the published module
  boundaries; focused test organization and fault-injection seams; concrete
  bounded report entry/count limits and formatting details consistent with the
  designs; concise source cleanup needed to satisfy the line caps.
- Reserved for the user: changing the five-child scope or order; weakening
  atomicity, publish order, repair, compatibility, or line-cap requirements;
  changing the version-1 envelope or 4096-byte default; adding locking/fsync;
  reviving or reusing `00008` or `post-fc22cba`; expanding into another
  repository.

## Risks and assumptions

The two capped core modules have limited remaining line budget, so later
children may need consolidation rather than additive implementation. Atomicity
and crash-order behavior require injected-failure evidence, not successful-path
tests alone. Lint changes must retain the mtime fast path; the fresh knowledge
search result `.specdev/project_notes/lint_db_performance.md` is a historical
lead for the lint child only and must be checked against current code.

The worktree contains pre-existing user and workflow changes. They must remain
separate from child ownership unless SpecDev explicitly checkpoints or adopts
them under the approved Mission.

## Verification authority

- Focused tests for changed modules and injected process-interruption boundaries
  are allowed throughout after repository instructions are satisfied.
- One full tracked pytest suite plus the immutable-legacy, source-cap,
  Python-3.8-syntax, and print-inventory checks is explicitly authorized only as
  the Mission's final integrated verification. Child Assignments run narrower
  behavioral evidence plus the required seven-file cap inventory.
- Child evidence must explicitly audit the declared Python 3.8 syntax and API
  floor; the final suite runs on the available Python 3.8-or-newer interpreter
  and is not by itself evidence for every supported minor version.

## Acceptance criteria

- AC-1: Every read path skips and logs torn rows; all index writes and the two
  protected control-file writes publish by atomic replacement; injected failures
  prove a grown upsert retains the old or new record rather than neither, and an
  append first heals a missing terminal newline.
- AC-2: Legacy and slotted files classify correctly, `_meta` is reserved, and
  the complete `FolderDB` metadata API preserves record-first/read and
  rows-first/write ordering, fit refusal, missing-table results, existing-call
  compatibility, absolute row offsets, and index-mtime publication after a
  slot-only write. Width enablement and resizing migrate atomically and refuse
  unsafe shrinkage without partial configuration.
- AC-3: Default and forced lint establish index fidelity and canonical layout
  for legacy and enabled folders, preserving valid slot records while repairing
  slot width, malformed slot envelopes, torn tails, terminal newlines, indexed
  damage, and dead space, with slot-aware cardinality and bounded removed-byte
  diagnostics.
- AC-4: All runtime prints are replaced by path-bearing logging, and every open
  and database lint replaces its bounded integrity or lint report;
  clean runs write only a header, scoped capture excludes other folders, open
  never repairs a table, ordinary operations never write reports, and the
  report directory is invisible to table discovery, hierarchy, and clearing.
- AC-5: The integrated tracked suite passes on the available supported Python
  runtime, child evidence audits the declared Python 3.8 syntax/API floor,
  existing unslotted databases and public calls remain compatible, no discarded
  code is present, every child has respected its applicable published Roadmap
  design rules, and all seven governed source files remain within their Roadmap
  line caps.

## Mission execution shape

- Initial child plan: planned
- Split reason: durability must land before ordered slot publication; the slot
  contract must land before folder-wide width migration; migration establishes
  the enabled-folder invariant consumed by lint; completed lint diagnostics are
  then consumed by the final logger-capture and report child. Each boundary has
  independent failure injection, review, and rollback value.

<!-- Use planned only for a concrete context, dependency, decision, or independent verification/rollback boundary. -->

## Mission execution policy

- Executor catalog: `.specdev/executors.yaml`
- Primary executor: managed-worker
- Alternate executors: none
- Required runtimes: none
- Required capabilities: none
- Required services: none
- Required platforms: current
- Required secrets: none
- Allowed bypasses: none
- Escalation: user-decision-required

## Final integrated verification

- Command: `python3 -m pytest -q tests && git diff --exit-code d5cd7b1ff3c462897667e5d19d17dfc092a6ecb5 -- tests/legacy && python3 -c 'from pathlib import Path; caps={"jsonldb/metaslot.py":250,"jsonldb/jsonlfile.py":950,"jsonldb/reports.py":150,"jsonldb/folderdb.py":1250,"jsonldb/jsonldf.py":150,"jsonldb/vercontrol.py":160,"jsonldb/visual.py":500}; counts={p:len(Path(p).read_text(encoding="utf-8").splitlines()) for p in caps}; excess={p:(counts[p],caps[p]) for p in caps if counts[p]>caps[p]}; assert not excess,excess' && python3 -c 'import ast,pathlib; trees={str(p):ast.parse(p.read_text(encoding="utf-8"),feature_version=(3,8)) for p in pathlib.Path("jsonldb").rglob("*.py")}; calls=[(p,n) for p,t in trees.items() for n in ast.walk(t) if isinstance(n,ast.Call)]; bad=[p for p,n in calls if (isinstance(n.func,ast.Name) and n.func.id=="print") or (isinstance(n.func,ast.Attribute) and isinstance(n.func.value,ast.Name) and n.func.value.id=="builtins" and n.func.attr=="print") or (isinstance(n.func,ast.Attribute) and n.func.attr in {"write","writelines"} and isinstance(n.func.value,ast.Attribute) and isinstance(n.func.value.value,ast.Name) and n.func.value.value.id=="sys" and n.func.value.attr in {"stdout","stderr"})]; assert not bad,bad'`
