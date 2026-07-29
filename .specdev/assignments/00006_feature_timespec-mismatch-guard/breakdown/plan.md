# Timespec Mismatch Guard — Implementation Plan

> **For agent:** Implement this plan task-by-task. Match verification effort to task mode.

**Goal:** Auto-detect and heal `config.meta` timespec values that disagree with the data's actual datetime-key precision, safely (full-scan confirmation before any rewrite).

**Architecture:** New `detect_timespec` helper in `jsonlfile.py`; two-stage guard in `FolderDB.__init__` (`_detect_data_timespec` boundary trigger from db.meta → `_scan_index_timespecs` full .idx confirmation → heal or warn). See `brainstorm/design.md`.

**Tech Stack:** Python, orjson, pytest (`unit_tests/`, local-only per repo convention)

**Execution Mode:** inline

**Test Budget:** ≤ 3 new tests (heal, mixed keep+warn, boundary-camouflage limitation — three distinct guarantees per the reviewed design).

---

### Task 1: detect_timespec helper in jsonlfile
**Mode:** standard
**Skills:** [test-driven-development]
**Files:** jsonldb/jsonlfile.py, unit_tests/test_jsonlfile.py

**Work:**
- Add `detect_timespec(linekey: str) -> Optional[str]`: returns 'seconds'/'microseconds' when `_is_datetime_string` shape-matches for that precision AND `datetime.fromisoformat` parses; None otherwise.

**Verify:**
- `python -m pytest unit_tests/test_jsonlfile.py -k detect_timespec -x -q`

**Test Budget:** +1 in unit_tests/test_jsonlfile.py; focused (<30s)

**Test Pruning:**
- None needed.

**Commit:** `git commit -m "feat: add detect_timespec helper for datetime-key precision detection"`

### Task 2: two-stage guard in FolderDB.__init__
**Mode:** standard
**Skills:** [test-driven-development]
**Files:** jsonldb/folderdb.py, unit_tests/test_folderdb.py

**Work:**
- `_detect_data_timespec()`: candidate precision from db.meta min/max keys; only returns a value when a boundary key disagrees with `self.timespec`.
- `_scan_index_timespecs()`: set of precisions across every key of every `.idx` file (reuse `get_file_list`/`_get_file_path`).
- `__init__` hook after the db.meta block: heal (`self.timespec = candidate; build_configmeta()`) only when scan == {candidate}; warn and keep when mixed; never touch healthy DBs.

**Verify:**
- `python -m pytest unit_tests/test_folderdb.py -k mismatch -x -q`
- Final: `python -m pytest unit_tests/ -q`

**Test Budget:** +2 in unit_tests/test_folderdb.py; focused (<30s) — heal test plus one test covering both non-heal paths (mixed keep+warn and boundary-camouflage limitation); they pin different guarantees of the same guard

**Test Pruning:**
- Existing `test_init_does_not_rewrite_configmeta_if_unchanged` already covers the healthy-DB no-rewrite path; keep it as-is.

**Commit:** `git commit -m "feat: auto-heal config.meta timespec when data precision disagrees"`
