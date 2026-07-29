# JSONL Write-Path Integrity Fixes — Implementation Plan

> **For agent:** Implement this plan task-by-task. Match verification effort to task mode.

**Goal:** Fix five verified data-integrity bugs in the JSONL write/delete path with no public API changes.

**Architecture:** Targeted fixes inside `jsonldb/jsonlfile.py` (`update_jsonl`) and `jsonldb/folderdb.py` (`delete_file_range`, `clear_folder`, `delete_file`/`delete_file_keys`). Each fix is driven by a failing test converted from the verification reproduction scripts. See `brainstorm/design.md` for full root-cause analysis and fix design.

**Tech Stack:** Python, orjson, pytest (`unit_tests/`)

**Execution Mode:** inline

**Test Budget:** ≤ 5 new tests across all tasks (one per bug).

---

### Task 1: Guard appends against missing trailing newline
**Mode:** standard
**Skills:** [test-driven-development]
**Files:** jsonldb/jsonlfile.py, unit_tests/test_jsonlfile.py

**Work:**
- In `update_jsonl`, after seeking to EOF: if file is non-empty and last byte is not `\n`, write `b'\n'` before computing `append_pos`.

**Verify:**
- `python -m pytest unit_tests/test_jsonlfile.py -k trailing_newline -x -q`

**Test Budget:** +1 in unit_tests/test_jsonlfile.py; focused (<30s)

**Test Pruning:**
- Check existing update_jsonl tests for overlap before adding.

**Commit:** `git commit -m "fix: heal missing trailing newline before update_jsonl appends"`

### Task 2: Fix shrink-in-place padding to preserve the line terminator
**Mode:** standard
**Skills:** [test-driven-development]
**Files:** jsonldb/jsonlfile.py, unit_tests/test_jsonlfile.py

**Work:**
- In `update_jsonl`, when the new line is shorter than the old, write `new_line[:-1] + b' ' * (old_len - new_len) + b'\n'` so the record region keeps its exact old length and trailing newline.

**Verify:**
- `python -m pytest unit_tests/test_jsonlfile.py -k shrink -x -q`

**Test Budget:** +1 in unit_tests/test_jsonlfile.py; focused (<30s)

**Test Pruning:**
- Check existing update tests for stale assertions about padding layout.

**Commit:** `git commit -m "fix: keep line terminator when shrinking records in place"`

### Task 3: Serialize datetime bounds in delete_file_range
**Mode:** standard
**Skills:** [test-driven-development]
**Files:** jsonldb/folderdb.py, unit_tests/test_folderdb.py

**Work:**
- Replace `str(lower_key)`/`str(upper_key)` with `serialize_linekey(...)` (import from `jsonldb.jsonlfile`).

**Verify:**
- `python -m pytest unit_tests/test_folderdb.py -k range -x -q`

**Test Budget:** +1 in unit_tests/test_folderdb.py; focused (<30s)

**Test Pruning:**
- Inspect existing delete_file_range tests; replace if they assert the broken boundary behavior.

**Commit:** `git commit -m "fix: use serialize_linekey for delete_file_range bounds"`

### Task 4: Make clear_folder clear hierarchy subfolders
**Mode:** standard
**Skills:** [test-driven-development]
**Files:** jsonldb/folderdb.py, unit_tests/test_folderdb.py

**Work:**
- Walk the tree with `os.walk`, skipping hidden directories; remove `.jsonl`/`.idx`/`.meta` files at any depth; then `delete_empty_folders()` and `build_dbmeta()`.

**Verify:**
- `python -m pytest unit_tests/test_folderdb.py -k clear_folder -x -q`

**Test Budget:** +1 in unit_tests/test_folderdb.py; focused (<30s)

**Test Pruning:**
- Check existing clear_folder tests for overlap.

**Commit:** `git commit -m "fix: clear_folder removes files in hierarchy subfolders"`

### Task 5: Make delete_file tolerate a missing index file
**Mode:** standard
**Skills:** [test-driven-development]
**Files:** jsonldb/folderdb.py, unit_tests/test_folderdb.py

**Work:**
- In `delete_file`, remove the `.idx` only if it exists.
- Switch `delete_file`, `delete_file_keys`, `delete_file_range` from `_get_or_create_file_path` to `_get_file_path` (deletes must not create folders).

**Verify:**
- `python -m pytest unit_tests/test_folderdb.py -k delete_file -x -q`
- Final: `python -m pytest unit_tests/ -q` (full suite, <2 min)

**Test Budget:** +1 in unit_tests/test_folderdb.py; focused (<30s)

**Test Pruning:**
- Check existing delete_file tests for overlap.

**Commit:** `git commit -m "fix: delete_file tolerates missing .idx and deletes stop creating folders"`
