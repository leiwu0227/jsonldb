# Packaging, Performance & Cleanup — Implementation Plan

> **For agent:** Implement this plan task-by-task. Match verification effort to task mode.

**Goal:** Behavior-preserving sweep: fix install-breaking packaging defects, defer heavy imports, remove redundant work in the hot write path, and delete verified dead code.

**Architecture:** Three independent slices: packaging/import hygiene (jsonldf, setup.py, __init__, folderdb imports, README), jsonlfile performance/dead code, folderdb dedup. See `brainstorm/design.md`. Reviewer notes: `.gitignore` already covers `build/` and `dist/` (no change needed); README.md also lists numba and must be updated.

**Tech Stack:** Python, orjson, pytest (`unit_tests/`, local-only per repo convention)

**Execution Mode:** inline

**Test Budget:** ≤ 1 new test across all tasks (behavior-preserving refactor; existing 78-test suite is the gate).

---

### Task 1: Packaging and import hygiene
**Mode:** standard
**Skills:** [test-driven-development]
**Files:** jsonldb/jsonldf.py, jsonldb/folderdb.py, jsonldb/__init__.py, setup.py, README.md, unit_tests/test_folderdb.py

**Work:**
- Delete `from numba import jit` from jsonldf.py; remove numba from README.md requirements.
- setup.py: remove `entry_points` block; add `matplotlib>=3.0.0` to install_requires.
- `__init__.py`: set `__version__ = "1.0.0"`; fix the docstring example for visual.
- folderdb.py: move the `.vercontrol` import inside `commit`/`revert`/`version` methods.

**Verify:**
- `python -m pytest unit_tests/test_folderdb.py -k import_contract -x -q`

**Test Budget:** +1 in unit_tests/test_folderdb.py; focused (<30s) — subprocess asserts `import jsonldb` loads neither `numba` nor `git`

**Test Pruning:**
- None needed.

**Commit:** `git commit -m "fix: remove unused numba import, defer gitpython, repair setup.py metadata"`

### Task 2: jsonlfile performance and dead code
**Mode:** standard
**Skills:** [test-driven-development]
**Files:** jsonldb/jsonlfile.py

**Work:**
- Drop Python-side `dict(sorted(...))` at the 4 index-write sites (keep `OPT_SORT_KEYS`).
- `select_jsonl`: `all_keys[0]`/`all_keys[-1]` instead of `min`/`max`.
- `save_jsonl`: stream lines to the file while building the index (no list accumulation).
- Fix `BUFFER_SIZE` comment; delete `check_dict_format` and the unused `deleted_line` line.

**Verify:**
- `python -m pytest unit_tests/ -q` (behavior-preserving; full suite is the contract, <2 min)

**Test Budget:** +0; covered by existing suite

**Test Pruning:**
- None needed (no test references check_dict_format).

**Commit:** `git commit -m "perf: single-sort index writes, O(1) range defaults, streaming save"`

### Task 3: folderdb metadata dedup and message fixes
**Mode:** standard
**Skills:** [test-driven-development]
**Files:** jsonldb/folderdb.py

**Work:**
- Extract `_make_meta_entry(name, file_path, linted=False, lint_time="")`; use in `build_dbmeta`, `update_dbmeta`, `lint_db`.
- `validate_name` error message: "at least" instead of "exactly".
- `update_dbmeta`: suffix-strip `.jsonl` via endswith/slice instead of `str.replace`.
- Remove redundant `import orjson` in `lint_db`.

**Verify:**
- `python -m pytest unit_tests/test_folderdb.py -q`
- Final: `python -m pytest unit_tests/ -q`

**Test Budget:** +0; covered by existing suite (build_dbmeta/update_dbmeta/lint_db all tested)

**Test Pruning:**
- None needed.

**Commit:** `git commit -m "refactor: dedupe metadata entry builder, fix validate_name message"`
