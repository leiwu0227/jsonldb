# Assignment contract

Kind: change

## Objective and context

Close the non-Git, non-RippleGraph compliance gaps found by the 2026-09-04
read-only comparison of the package against the approved Roadmap design notes.
Preserve the behavior delivered by Mission M00001 while making the file-store
contracts, DataFrame adapter, package surface, canonical test layout, and
future-work roadmap agree.

Relevant context includes `.specdev/project_notes/big_picture.md`, the Roadmap
design notes, and the completed outcomes for Assignments 00035 and 00036 and
Mission M00001.

## Scope and non-goals

In scope:

- Enforce the designed row shape throughout JSONL writers, readers, index
  rebuilding, selection, and lint: a row value must be a dictionary. Invalid
  shapes are refused before a library write and are skipped, logged, excluded
  from indexes, and removed by lint when encountered on disk.
- Make single-key lookup serialize strings, datetimes, and coercible keys
  independently of whether returned keys are auto-deserialized.
- Make standalone lint replace a recognized malformed reserved first-line
  envelope with an empty valid version-1 envelope of the same width when that
  width can represent one. If it is too short, treat it as removable torn
  damage rather than raising or preserving malformed bytes. Folder lint still
  uses the configured folder width.
- Make the DataFrame adapter faithfully forward load auto-deserialization and
  lint `force`/slot-width options, preserving existing positional calls and
  returning the underlying lint existence result.
- List `metaslot` with the package submodules without eagerly importing Git or
  visualization dependencies.
- Make `unit_tests/` the sole canonical tracked suite required by the source
  structure design: remove its obsolete ignored Assignment 00008 contents,
  remove its ignore rule, relocate the maintained `tests/` suite into it, and
  point pytest at it. The preserved baseline tests become the exact top-level
  matches `test_jsonlfile.py`, `test_jsonldf.py`, and `test_folderdb.py` named
  by the design; additional cross-cutting regression files may supplement but
  not replace them. Add focused tracked regressions for every behavior changed
  here, and remove the old `tests/` tree including ignored generated residue.
- Refresh `forecast.md` to remove completed gaps and record only verified gaps
  that remain outside this Assignment; refresh `todo.md` so completed Mission
  work is no longer presented as future work. Keep all Roadmap documents within
  their format and word limits.

Non-goals:

- Changing Git snapshot, commit, revert, branch, or revision behavior.
- Editing any published file under `.specdev/project_notes/roadmap/designs/`;
  those notes are immutable implementation authority for this Assignment.
- Editing `.specdev/.ripplegraph/`, historical Assignment/Mission evidence, or
  the existing incomplete Discussion.
- Enforcing non-empty record dictionaries; empty dictionaries remain valid for
  compatibility. Schema validation inside a record remains out of scope.
- Changing metadata envelope versions, slot-width migration policy, report
  format, hierarchy behavior, or existing public defaults.

## Expected behavior

- Public writers fail before mutation when any supplied row value is not a
  dictionary. Every read form returns only valid dictionary-valued rows; index
  rebuild and lint make the same classification and path-log rejected rows.
- `select_line_jsonl(path, datetime_key, auto_deserialize=False)` finds the
  serialized record and returns its key as stored text. Equal-bound range
  selection inherits the same behavior.
- Standalone forced lint converges a malformed reserved first line and valid
  rows into a canonical readable file, with the repair represented in logging.
- Existing `load_jsonldf(path, timespec)` calls retain their meaning; callers
  may additionally disable key auto-deserialization. `lint_jsonldf` exposes
  and forwards the file-store maintenance options and result.
- `from jsonldb import metaslot` and the package submodule inventory agree,
  while importing `FolderDB` still does not load optional Git or plotting
  modules.
- The maintained suite has one authoritative tracked location, contains no
  discarded Assignment 00008 test implementation, includes the three designed
  top-level module test files, and collects successfully.
- Forecast and Todo describe future work only, not the completed M00001 plan.

## Important decisions

- The published source-structure design takes precedence over M00001's current
  test location. Relocate the maintained suite rather than changing, weakening,
  or reinterpreting the design note; discarded Assignment 00008 test code is
  not carried into the destination.
- Invalid scalar, list, null, or other non-dictionary row values are structural
  damage, not consumer schema. Empty dictionaries remain structurally valid.
- `auto_deserialize` controls output representation only; it never controls
  lookup-key serialization.
- Historical workflow records remain durable provenance even when they describe
  discarded work. Only obsolete executable/test files are removed.
- The refreshed Forecast may retain or add verified design-derived gaps that
  this Assignment explicitly excludes; it must not claim full compliance when
  out-of-scope gaps remain.

## Constraints and invariants

- Preserve all existing public calls and defaults. Add optional parameters only
  in positions that do not reinterpret existing positional arguments.
- Preserve the contents of revision
  `548a174b959b45b9395d87d5502568834dfc7b64`'s `tests/legacy/test_jsonlfile.py`,
  `test_jsonldf.py`, and `test_folderdb.py` byte-for-byte as their matching
  top-level files under `unit_tests/`.
- Preserve M00001 durability ordering, atomic index/control publication,
  metadata-slot opacity and absolute offsets, report bounds/privacy, and the
  fresh-index lint fast path.
- Respect every published completed-file cap: `metaslot.py` 250 lines,
  `jsonlfile.py` 950, `reports.py` 150, `folderdb.py` 1250, `jsonldf.py` 150,
  `vercontrol.py` 160, and `visual.py` 500. `jsonlfile.py` starts exactly at its
  cap, so its change must be line-neutral or reduce the file.
- Every published Roadmap design Markdown file remains byte-for-byte unchanged
  and below 800 words (maximum 799). Forecast/Todo numbered sections remain
  below 200 words when present.
- Maintain Python 3.8 syntax compatibility and do not introduce executable
  `print` or direct stdout/stderr writes in the package.
- Preserve the untracked `.specdev/discussions/` content outside Assignment
  ownership.

## Delegated and reserved authority

- Delegated: change the bounded package functions, tracked test layout,
  `.gitignore`, pytest configuration, `forecast.md`, and `todo.md` needed to
  satisfy this contract; simplify nearby code where necessary to remain within
  line caps.
- Reserved for the user: any Git-version implementation change, any
  RippleGraph or historical workflow-record edit, any new metadata envelope or
  schema rule, moving the canonical suite away from `unit_tests/`, weakening a
  line cap, changing legacy-test contents, or editing any published design note.

## Risks and assumptions

- Tight space in `jsonlfile.py` makes unnecessary refactoring risky; prefer one
  shared row-shape check and focused edits.
- Tightening row validation may expose callers that relied on undocumented
  invalid values. The approved designs and project data model make dictionary
  values authoritative, while empty dictionaries preserve the least disruptive
  compatibility boundary.
- Malformed first lines can be shorter than the smallest valid envelope; the
  explicit removal rule prevents lint from failing while avoiding fabricated
  widths.
- Replacing the ignored local `unit_tests/` tree is intentionally irreversible
  in this workspace. Its discarded Assignment 00008 files have no Git recovery
  source, their deletion is explicitly intended, and the relocated tracked
  suite must not be represented as their backup.

## Verification authority

- Focused tests for JSONL validation/selection/lint, the DataFrame adapter,
  imports, and roadmap structure are allowed during implementation and repair.
- The complete tracked suite and the exact final integrated command below are
  authorized after implementation.
- Final integrated verification:

```sh
python3 -m pytest -q unit_tests && test ! -e tests && test -z "$(git diff --name-only 548a174b959b45b9395d87d5502568834dfc7b64 -- .specdev/project_notes/roadmap/designs)" && test -z "$(git status --porcelain --untracked-files=all -- .specdev/project_notes/roadmap/designs)" && python3 -c 'from pathlib import Path; import subprocess; rev="548a174b959b45b9395d87d5502568834dfc7b64"; names=["test_folderdb.py","test_jsonldf.py","test_jsonlfile.py"]; assert all(Path("unit_tests",name).read_bytes()==subprocess.check_output(["git","show",f"{rev}:tests/legacy/{name}"]) for name in names)' && python3 -c 'from pathlib import Path; caps={"jsonldb/metaslot.py":250,"jsonldb/jsonlfile.py":950,"jsonldb/reports.py":150,"jsonldb/folderdb.py":1250,"jsonldb/jsonldf.py":150,"jsonldb/vercontrol.py":160,"jsonldb/visual.py":500}; counts={p:len(Path(p).read_text(encoding="utf-8").splitlines()) for p in caps}; excess={p:(counts[p],caps[p]) for p in caps if counts[p]>caps[p]}; assert not excess,excess' && python3 -c 'import ast,pathlib; trees={str(p):ast.parse(p.read_text(encoding="utf-8"),feature_version=(3,8)) for p in pathlib.Path("jsonldb").rglob("*.py")}; calls=[(p,n) for p,t in trees.items() for n in ast.walk(t) if isinstance(n,ast.Call)]; bad=[p for p,n in calls if (isinstance(n.func,ast.Name) and n.func.id=="print") or (isinstance(n.func,ast.Attribute) and isinstance(n.func.value,ast.Name) and n.func.value.id=="builtins" and n.func.attr=="print") or (isinstance(n.func,ast.Attribute) and n.func.attr in {"write","writelines"} and isinstance(n.func.value,ast.Attribute) and isinstance(n.func.value.value,ast.Name) and n.func.value.value.id=="sys" and n.func.value.attr in {"stdout","stderr"})]; assert not bad,bad' && python3 -c 'from pathlib import Path; files=list(Path(".specdev/project_notes/roadmap/designs").rglob("*.md")); excess={str(p):len(p.read_text(encoding="utf-8").split()) for p in files if len(p.read_text(encoding="utf-8").split())>799}; assert not excess,excess'
```

## Acceptance criteria

- AC-1: Focused tests prove non-dictionary rows are rejected before writes,
  skipped and excluded by every read/index path, and removed with path-bearing
  diagnostics by lint, while dictionary rows including empty dictionaries keep
  existing behavior.
- AC-2: Focused tests prove single/equal-bound datetime lookup works with
  auto-deserialization both enabled and disabled, and standalone lint repairs
  representable and too-short malformed reserved first lines without losing
  valid rows or violating absolute offsets.
- AC-3: Focused tests prove backward-compatible DataFrame load/lint forwarding,
  the `metaslot` package inventory, optional-import laziness, and collection of
  the sole maintained tracked `unit_tests/` tree with the three designed
  top-level module test files and no discarded Assignment 00008 implementation
  or residual `tests/` tree.
- AC-4: Published design notes remain byte-for-byte unchanged; Forecast contains
  only presently unimplemented design-derived work; Todo contains only presently
  selected future work; all document and source caps, relocated immutable legacy
  tests, Python 3.8 syntax, logging transport, and the full tracked suite pass
  the authorized final command.
