# Design: Performance Optimization

## Overview

JSONLDB has several performance bottlenecks at scale (millions of lines per file, hundreds of files). Key issues:

1. **Inconsistent JSON library usage** — reads use stdlib `json` while writes use orjson (3-10x slower parsing)
2. **Suboptimal range selection** — O(n) Numba string scan instead of O(log n) bisect on sorted keys
3. **Bloated index files** — `.idx` files use `indent=2` JSON, wasting disk and parse time
4. **Linting loads entire file into memory** — problematic for million-line files; no skip path for already-clean files
5. **Redundant filesystem calls** — folder creation on reads, unconditional config rewrites, full `os.walk` per file list call, per-file `db.meta` updates during lint

The refactor unifies on orjson, replaces Numba string ops with bisect, compacts `.idx` files, introduces stream-based linting with a smart fast/slow path (line-count check to detect orphans), and eliminates redundant I/O in FolderDB.

## Non-Goals

- **No API changes** — all public methods on `FolderDB`, `jsonlfile`, and `jsonldf` keep the same signatures and behavior
- **No `.jsonl` format changes** — data files remain human-readable JSON Lines
- **No changes to `vercontrol.py` or `visual.py`** — out of scope
- **No new dependencies** — all optimizations use existing deps (orjson, numpy) or stdlib (bisect, mmap)
- **No parallelism/threading** — keeping single-threaded execution model; parallel file processing is a future consideration
- **No caching layer** — no in-memory LRU cache for indices or data; adds complexity and invalidation risk

## Design

### 1. Unify on orjson for all JSON operations (`jsonlfile.py`)

**Where:** `load_jsonl`, `select_jsonl`, `select_line_jsonl`, `build_jsonl_index`, `delete_jsonl`, all `.idx` reads/writes

**Change:**
- Replace `json.loads(line)` → `orjson.loads(line_bytes)` (avoids decode step too — orjson accepts bytes directly)
- Replace `json.load(f)` for index reads → `orjson.loads(f.read())`
- Replace `json.dump(index, f, indent=2)` for index writes → `f.write(orjson.dumps(index, option=orjson.OPT_SORT_KEYS))` — compact, no indentation
- Also fix `delete_jsonl` which already uses orjson but with `OPT_INDENT_2` — remove the indent flag for consistency
- Remove the `try/except ImportError` fallback in `_fast_dumps` since orjson is a required dependency

**`.idx` migration:** No explicit migration step needed. Old indented `.idx` files are read by `orjson.loads()` which handles both indented and compact JSON. On next write (any upsert/delete/lint/save), the `.idx` is rewritten in compact format. This is a lazy, transparent migration.

**Impact:** 3-10x faster parsing on every read path. Compact indices reduce disk I/O proportionally.

### 2. Replace Numba string operations with bisect (`jsonlfile.py`)

**Where:** `_select_keys_in_range`, `_fast_sort_records`, `_convert_key_to_sortable`

**Change:**
- Remove `_select_keys_in_range`, `_fast_sort_records`, `_convert_key_to_sortable`, `_sort_numeric_keys`
- `select_jsonl` range selection: use `bisect_left(keys, lower)` and `bisect_right(keys, upper)` on the already-sorted index key list → O(log n) to find bounds, then slice
- Sorting in `lint_jsonl` / `save_jsonl`: use Python's built-in `sorted()` on dict keys — C-implemented, correct for homogeneous key types. Note: each JSONL file has homogeneous keys (all strings or all datetimes after deserialization). As a safety net, use `sorted(keys, key=str)` to handle any unexpected mixed-type edge cases without raising `TypeError`.
- **bisect compatibility note:** Index keys are always serialized strings (ISO datetime strings sort lexicographically correctly for both `seconds` and `microseconds` timespec). `bisect` operates on these string keys directly, which is consistent with the `sorted(keys, key=str)` ordering.

**Impact:** O(log n) range selection instead of O(n) scan. Removes Numba JIT compilation overhead on first call. Correct string ordering (current float encoding is broken for long strings).

### 3. Batch sequential reads in `select_jsonl` (`jsonlfile.py`)

**Where:** `select_jsonl` after range keys are identified

**Change:** Sort the selected byte offsets ascending before reading, so the file is read sequentially. OS read-ahead and disk cache will serve consecutive reads much faster than random seeks. **Important:** read into a temporary dict keyed by offset, then rebuild the result dict in sorted key order to preserve the API contract that results are returned in key-sorted order.

**Impact:** Better I/O throughput for large range queries, especially on spinning disks. No change to output ordering.

### 4. Stream-based linting with smart path detection (`jsonlfile.py`)

**Where:** `lint_jsonl`

**Change — new flow:**
1. Read the `.idx` file (or rebuild if missing)
2. **Line-count check + spot verification:** mmap the `.jsonl` file and count non-blank lines. Compare to index entry count.
   - **Mismatch** → orphaned/corrupted lines exist. Rebuild index via `build_jsonl_index` first (already streams with mmap, no memory spike)
   - **Match** → additionally spot-check the first and last index entries: seek to the stored byte offset, read the line, verify the parsed key matches the index key. This catches offset corruption at O(2) cost. If spot-check fails → rebuild index.
   - **Both pass** → index is trustworthy
3. **Sorted + compaction check:** iterate index keys, verify they're in order. Check for dead space by comparing actual file size against `last_offset + length_of_last_line` (read one line at the last offset to get its length). If already sorted AND no dead space → skip, return `True` early.
4. **Stream-lint:** sort the index keys, iterate in sorted order, seek to each offset, read the line, write to a temp file. Atomic rename over the original. Rebuild index from the fresh file. Return `True`.
5. **File not found:** if the `.jsonl` file doesn't exist, return `False` (existing behavior preserved).

**Return value contract:** `lint_jsonl` returns `bool` — `True` if file exists (whether skipped or linted), `False` if file not found. This preserves the existing contract used by `lint_db` to decide whether to delete metadata.

**Memory:** Only one line + index in memory at a time, instead of the full file contents.

**Impact:** Handles million-line files without memory blowup. Skips already-clean files entirely. The line-count check is nearly free (single mmap scan).

### 5. Batch `db.meta` updates in `lint_db` (`folderdb.py`)

**Where:** `lint_db`

**Change:** Instead of calling `update_dbmeta(name)` per file (each call reads → modifies → writes `db.meta`), collect all metadata entries in a dict during the lint loop, then do a single `save_jsonl(self.dbmeta_path, metadata)` at the end.

**Impact:** For 200 files, goes from ~400 file I/O operations (read+write per file) to 1 write.

### 6. Eliminate redundant filesystem calls (`folderdb.py`)

**a) `_get_file_path` — stop creating folders on reads:**
- Split into `_get_file_path` (read path, no folder creation) and `_get_or_create_file_path` (write path, creates folder)
- Update write methods (`upsert_dict`, `overwrite_dict`, `upsert_df`, `overwrite_df`) to use the create variant
- Read methods (`get_dict`, `get_df`) use the plain variant

**b) `build_configmeta` — skip if unchanged:**
- In `__init__`, if `config.meta` exists, read it and check if `timespec` matches the current `TIME_SPEC`. Only rewrite if different or if the file doesn't exist. Currently `config.meta` only stores `timespec`, so the match check is a single key comparison.

**c) `get_file_list` — avoid redundant `os.walk` during `__init__`:**
- This is a local optimization within `__init__` only: when `build_dbmeta` runs, pass the file list it already computed to avoid a second `os.walk`. This is NOT an instance-level cache — `get_file_list()` always does a fresh walk when called by user code, ensuring correctness after writes/deletes.

**Impact:** Removes hundreds of unnecessary `os.path.exists`, `os.makedirs`, and file writes during normal read-heavy workloads.

### 7. Benchmark scripts

**New file:** `profile_test/benchmark.py`

**What it measures:**
- `load_jsonl` / `save_jsonl` — single file read/write at various sizes (1K, 10K, 100K, 1M lines)
- `select_jsonl` — range queries on large files
- `lint_jsonl` — lint on sorted, unsorted, and files-with-dead-lines
- `FolderDB.__init__` — initialization time with many files
- `lint_db` — full database lint
- Before/after comparison output

## Success Criteria

1. **All existing unit tests pass** — `test_jsonlfile.py`, `test_jsonldf.py`, `test_folderdb.py` must pass without modification. If any test explicitly checks for indented `.idx` format, update the assertion to expect compact format.
2. **Benchmark script exists** in `profile_test/benchmark.py` with before/after comparison
3. **Measurable speedup** on at least:
   - `load_jsonl` / `select_jsonl` — faster from orjson switch
   - `select_jsonl` range queries — faster from bisect vs O(n) scan
   - `lint_jsonl` — lower memory usage for large files, skip path for clean files
   - `lint_db` — faster from batched `db.meta` writes
   - `FolderDB.__init__` — faster from eliminating redundant filesystem calls
4. **`.jsonl` files remain human-readable** — no format change to data files
5. **No public API changes** — all existing callers work without modification

## Testing Approach

1. **Existing unit tests** — run `test_jsonlfile.py`, `test_jsonldf.py`, `test_folderdb.py` after each module change to catch regressions
2. **Benchmark script** (`profile_test/benchmark.py`) — generates synthetic data at various scales, runs before/after comparisons, outputs timing tables
3. **Manual verification** — spot-check that `.jsonl` files remain readable, `.idx` files are valid compact JSON, and stream-lint produces identical sorted output to the current `lint_jsonl`

## Risks

1. **orjson behavioral differences** — orjson is stricter than stdlib json (e.g., no `NaN`, no trailing commas). If existing data has edge cases, parsing could fail. Mitigation: existing tests will catch this; can add a stdlib fallback for malformed lines only.
2. **Stream-lint atomicity** — writing to a temp file then renaming. If the process crashes mid-write, the temp file is left behind but the original is untouched. Safe. The temp file approach adds negligible overhead (one extra `os.rename` call).
3. **Line-count heuristic** — comparing index entries vs non-blank lines in the file. Blank lines from deletes (spaces + `\n`) must be counted correctly as non-data lines. Mitigation: count only lines with actual content (strip whitespace before counting).

## Open Questions

None — all design decisions have been validated.
