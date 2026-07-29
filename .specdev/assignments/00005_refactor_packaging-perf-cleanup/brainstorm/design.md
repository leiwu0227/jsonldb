# Design: Packaging, performance, and cleanup sweep

## Overview

Behavior-preserving sweep over four areas, all findings verified against the current code:

| Area | Items |
|------|-------|
| Packaging | unused `numba` import (breaks fresh installs); `setup.py` phantom `jsonldb=jsonldb.cli:main` entry point; matplotlib missing from deps while `visual.py` imports it; version `1.0.0` (setup.py) vs `0.1.0` (`__init__.py`); `build/`/`dist/` not gitignored |
| Import hygiene | `folderdb.py` eagerly imports `vercontrol` → gitpython loads on `from jsonldb import FolderDB`, contradicting the `__init__.py` modular-import docstring |
| Performance | double index sorting on every write (`dict(sorted(...))` + `OPT_SORT_KEYS`) in `build_jsonl_index`, `save_jsonl`, `update_jsonl`, `delete_jsonl`; O(n) `min`/`max` over already-sorted index keys in `select_jsonl`; `save_jsonl` buffers all encoded lines in a list before writing; `BUFFER_SIZE` comment says 10MB for a 50MB value |
| Dead code / dedup | `check_dict_format` never called; `deleted_line` computed and unused in `delete_jsonl`; three near-identical metadata-entry builders (`build_dbmeta`, `update_dbmeta`, `lint_db`); `validate_name` enforces `>=` but error text says "exactly"; `update_dbmeta` strips `.jsonl` with `str.replace` (removes the substring anywhere); redundant local `import orjson` in `lint_db` |

## Non-Goals

- **No vercontrol.py changes** — version-control bugs were explicitly excluded by the owner.
- **No visual.py changes** — plotting code is untested and low-traffic; its duplicate index
  reads and `.replace('.jsonl','')` calls stay as-is.
- **No print→logging migration** — that changes observable behavior and deserves its own
  decision.
- **No API changes** — all public signatures stay identical.

## Design

**Task A — packaging & import hygiene:**
- Delete `from numba import jit` (`jsonldf.py:8`). numba is unused everywhere.
- `setup.py`: remove the `entry_points` block (no `cli.py` exists); add `matplotlib>=3.0.0`
  to `install_requires` (visual.py imports it; bokeh is already listed); keep `setup.py`
  version `1.0.0` and set `__init__.py` `__version__ = "1.0.0"` to match the published wheel.
- `.gitignore`: add `build/` and `dist/` (a stale built copy of the lib sits in `build/`).
- `folderdb.py`: move the `from .vercontrol import ...` to inside the three version-control
  methods (`commit`, `revert`, `version`) so gitpython loads only when used. Update the
  `__init__.py` docstring example (`from jsonldb.visual import plot` → an actual function).
- New test (subprocess): `import jsonldb` then assert `'numba' not in sys.modules and
  'git' not in sys.modules` — pins the import contract.

**Task B — jsonlfile performance & dead code:**
- Index writes: keep `OPT_SORT_KEYS`, drop the Python-side `dict(sorted(...))` wrappers
  (4 sites). `build_jsonl_index` keeps a single sort path the same way.
- `select_jsonl`: replace `min(all_keys)`/`max(all_keys)` with `all_keys[0]`/`all_keys[-1]`
  (the index file is written key-sorted; orjson preserves object order on load).
- `save_jsonl`: write lines as they are encoded while tracking the byte offset, instead of
  accumulating the whole list first.
- Fix the `BUFFER_SIZE` comment (50MB).
- Delete `check_dict_format` (never called) and the unused `deleted_line` assignment.

**Task C — folderdb dedup & messages:**
- Extract `_make_meta_entry(name, file_path, linted=False, lint_time="") -> dict` that reads
  the `.idx` and returns the metadata dict; use it in `build_dbmeta`, `update_dbmeta`, and
  `lint_db` (removes ~50 duplicated lines).
- `validate_name` error text: "at least N delimiters" to match the `>=` check.
- `update_dbmeta`: strip the `.jsonl` suffix with `name[:-6] if name.endswith('.jsonl')`
  (consistent with `_get_file_name`; `removesuffix` is unavailable on the supported 3.8).
- Remove the redundant `import orjson` inside `lint_db`.

## Success Criteria

- New subprocess import test passes: core import loads neither `numba` nor `git`.
- Full suite (79 after the new test) passes unchanged — behavior-preserving guarantee.
- `pip`-fresh simulation: `python -c "import jsonldb"` works in an environment without numba
  (equivalently: `grep numba jsonldb/` returns nothing).
- `setup.py` has no `entry_points`, lists matplotlib, and `jsonldb.__version__ == "1.0.0"`.
- Index files produced after the change are byte-identical to before (same sorted-key JSON).

## Testing Approach

One new executable test (the subprocess import contract). Everything else is covered by the
existing suite, which exercises every touched function; `profile_test/benchmark.py` remains
available for manual perf comparison but is not part of the gate.
