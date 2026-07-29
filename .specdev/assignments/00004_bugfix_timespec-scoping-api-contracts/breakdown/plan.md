# Timespec Scoping & API Contracts — Implementation Plan

> **For agent:** Implement this plan task-by-task. Match verification effort to task mode.

**Goal:** Make timespec per-database instance state (no module-global mutation), fix the `select_line_jsonl` contract, and deduplicate the datetime-key deserialization block.

**Architecture:** Bottom-up threading of an optional `timespec` keyword (default = module global, so existing callers are unchanged): `jsonlfile.py` first, then `jsonldf.py` pass-through, then `FolderDB` holds `self.timespec` and passes it. See `brainstorm/design.md`.

**Tech Stack:** Python, orjson, pytest (`unit_tests/`, local-only per repo convention)

**Execution Mode:** inline

**Test Budget:** ≤ 4 new tests across all tasks.

---

### Task 1: Thread timespec through jsonlfile and fix select_line_jsonl contract
**Mode:** standard
**Skills:** [test-driven-development]
**Files:** jsonldb/jsonlfile.py, unit_tests/test_jsonlfile.py

**Work:**
- Add `timespec: Optional[str] = None` to `serialize_linekey` and `_is_datetime_string`; resolve via `timespec or TIME_SPEC`.
- Add the same trailing keyword to `save_jsonl`, `load_jsonl`, `select_jsonl`, `select_line_jsonl`, `update_jsonl`, `delete_jsonl`; pass it to every internal `serialize_linekey`/`_is_datetime_string` call.
- Extract private helper `_store_with_key(result_dict, linekey, value, auto_deserialize, timespec)` and use it in `load_jsonl`, `select_jsonl`, `select_line_jsonl` (replaces 4 duplicated blocks).
- Fix `select_line_jsonl`: return annotation `DataDict`, docstring "single-record dict, `{}` if not found"; document the dual role of `auto_serialize`.

**Verify:**
- `python -m pytest unit_tests/test_jsonlfile.py -k timespec -x -q`

**Test Budget:** +1 in unit_tests/test_jsonlfile.py; focused (<30s)

**Test Pruning:**
- Inspect existing datetime-key tests for overlap before adding.

**Commit:** `git commit -m "refactor: thread optional timespec through jsonlfile, fix select_line_jsonl contract"`

### Task 2: Pass timespec through jsonldf wrappers
**Mode:** standard
**Skills:** [test-driven-development]
**Files:** jsonldb/jsonldf.py

**Work:**
- Add `timespec: Optional[str] = None` pass-through to `save_jsonldf`, `load_jsonldf`, `update_jsonldf`, `select_jsonldf`, `delete_jsonldf`.

**Verify:**
- `python -m pytest unit_tests/test_jsonldf.py -q`

**Test Budget:** +0; covered by Task 3 integration tests and existing suite (pure pass-through, no behavior change when omitted)

**Test Pruning:**
- None needed.

**Commit:** `git commit -m "refactor: pass timespec through jsonldf wrappers"`

### Task 3: FolderDB instance-scoped timespec
**Mode:** standard
**Skills:** [test-driven-development]
**Files:** jsonldb/folderdb.py, unit_tests/test_folderdb.py

**Work:**
- `self.timespec` from `config.meta` if present else `jsonlfile.TIME_SPEC`; delete the `jsonlfile.TIME_SPEC = ...` mutation; collapse the duplicated `config.meta` read in `__init__` into one block.
- `build_configmeta` writes `self.timespec`; rewrite-check compares against `self.timespec`.
- Pass `timespec=self.timespec` from every FolderDB call into jsonlfile/jsonldf read/write functions that serialize or deserialize keys.

**Verify:**
- `python -m pytest unit_tests/test_folderdb.py -k timespec -x -q`
- Final: `python -m pytest unit_tests/ -q` and `grep -n "TIME_SPEC = " jsonldb/folderdb.py` returns nothing

**Test Budget:** +2 in unit_tests/test_folderdb.py; focused (<30s) — one test for no-contamination (fresh DB after microseconds DB keeps `seconds` in config.meta and global untouched), one for two DBs with different timespecs round-tripping datetime keys concurrently; the scenarios cannot be combined without hiding which guarantee regressed

**Test Pruning:**
- Inspect `test_init_does_not_rewrite_configmeta_if_unchanged` for overlap; update if it asserts global-mutation behavior.

**Commit:** `git commit -m "fix: scope timespec per FolderDB instance instead of mutating module global"`
