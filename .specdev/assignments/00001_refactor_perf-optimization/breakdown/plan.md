# Performance Optimization Implementation Plan

> **For agent:** Implement this plan task-by-task using TDD discipline.

**Goal:** Optimize jsonlfile.py, jsonldf.py, and folderdb.py for speed — unify on orjson, replace Numba with bisect, compact index files, stream-based linting, and eliminate redundant I/O — without changing the public API or .jsonl format.

**Architecture:** Layered: `jsonlfile` (low-level CRUD) → `jsonldf` (DataFrame adapter) → `folderdb` (multi-table DB). All changes are internal to these three modules. Existing unit tests serve as the regression safety net.

**Tech Stack:** Python, orjson, bisect (stdlib), mmap (stdlib), pandas, pytest

---

### Task 1: Unify index reads on orjson and compact index writes
**Mode:** standard
**Skills:** test-driven-development
**Files:**
- Modify: `jsonldb/jsonlfile.py`
- Modify: `unit_tests/test_jsonlfile.py`

**Step 1: Write the failing test**
Add to `unit_tests/test_jsonlfile.py`:
```python
def test_index_file_is_compact(test_file, sample_data):
    """Test that .idx files are written in compact format (no indentation)"""
    save_jsonl(test_file, sample_data)
    with open(test_file + ".idx", "rb") as f:
        raw = f.read()
    # Compact format has no newlines within the JSON object
    decoded = raw.decode("utf-8")
    assert "\n" not in decoded.strip()
```

**Step 2: Run test to verify it fails**
Run: `cd /mnt/h/oceanwave/lib/jsonldb && python -m pytest unit_tests/test_jsonlfile.py::test_index_file_is_compact -x`
Expected: FAIL — current .idx files use `indent=2`

**Step 3: Write minimal implementation**
In `jsonldb/jsonlfile.py`:
- In `build_jsonl_index`: replace BOTH `json.dump` calls with orjson — the empty-file branch (`json.dump(index_dict, f, indent=2)`) AND the main branch. Use `f.write(orjson.dumps(dict(sorted(index_dict.items())), option=orjson.OPT_SORT_KEYS))`; open files in `'wb'` mode
- In `save_jsonl`: replace `json.dump(dict(sorted(index.items())), f, indent=2)` with `f.write(orjson.dumps(dict(sorted(index.items())), option=orjson.OPT_SORT_KEYS))`; open file in `'wb'` mode
- In `update_jsonl`: replace `json.dump(dict(sorted(index.items())), f, indent=2)` with writing `orjson.dumps(dict(sorted(index.items())), option=orjson.OPT_SORT_KEYS)`; open file in `'wb'` mode
- Replace all index reads (`json.load(f)`) with `orjson.loads(f.read())` — in `select_jsonl`, `select_line_jsonl`, `update_jsonl`, `delete_file_range` (in folderdb)
- In `delete_jsonl`: remove `OPT_INDENT_2` from the existing orjson.dumps call for index writes

**Step 4: Run test to verify it passes**
Run: `cd /mnt/h/oceanwave/lib/jsonldb && python -m pytest unit_tests/test_jsonlfile.py -x`
Expected: ALL PASS

**Step 5: Commit**
```bash
cd /mnt/h/oceanwave/lib/jsonldb
git add jsonldb/jsonlfile.py unit_tests/test_jsonlfile.py
git commit -m "perf: unify index I/O on orjson with compact format"
```

---

### Task 2: Switch all JSON line parsing to orjson
**Mode:** standard
**Skills:** test-driven-development
**Files:**
- Modify: `jsonldb/jsonlfile.py`

**Step 1: Write the failing test**
Add to `unit_tests/test_jsonlfile.py`:
```python
def test_load_jsonl_uses_orjson(test_file, sample_data, monkeypatch):
    """Test that load_jsonl works with orjson parsing (bytes input)"""
    save_jsonl(test_file, sample_data)
    # Monkeypatch json.loads to raise — proving orjson is used instead
    import json as json_mod
    original_loads = json_mod.loads
    def patched_loads(*args, **kwargs):
        raise AssertionError("json.loads should not be called during load_jsonl")
    monkeypatch.setattr(json_mod, "loads", patched_loads)
    try:
        loaded = load_jsonl(test_file)
        assert loaded == sample_data
    finally:
        monkeypatch.setattr(json_mod, "loads", original_loads)
```

**Step 2: Run test to verify it fails**
Run: `cd /mnt/h/oceanwave/lib/jsonldb && python -m pytest unit_tests/test_jsonlfile.py::test_load_jsonl_uses_orjson -x`
Expected: FAIL — `load_jsonl` currently calls `json.loads`

**Step 3: Write minimal implementation**
In `jsonldb/jsonlfile.py`:
- `load_jsonl`: open file in `'rb'` mode, iterate bytes lines, use `orjson.loads(line)` directly (no decode needed)
- `build_jsonl_index`: already uses mmap (bytes), replace `json.loads(line_str)` with `orjson.loads(line)` on the raw bytes (remove `.decode('utf-8')` and `.strip()` — just strip whitespace from bytes)
- `select_jsonl` line reads: open file in `'rb'` mode, use `orjson.loads(line)` after seek+readline
- `select_line_jsonl`: same pattern — `'rb'` mode, `orjson.loads`
- `_fast_dumps`: remove the `try/except ImportError` fallback, simplify to just `orjson.dumps(obj, option=orjson.OPT_SERIALIZE_NUMPY).decode('utf-8') + '\n'`
- Remove `import json` if no longer needed (keep if used elsewhere, e.g., in edge cases)

**Step 4: Run test to verify it passes**
Run: `cd /mnt/h/oceanwave/lib/jsonldb && python -m pytest unit_tests/test_jsonlfile.py -x`
Expected: ALL PASS

**Step 5: Commit**
```bash
cd /mnt/h/oceanwave/lib/jsonldb
git add jsonldb/jsonlfile.py unit_tests/test_jsonlfile.py
git commit -m "perf: switch all JSON line parsing to orjson"
```

---

### Task 3: Replace Numba range selection with bisect
**Mode:** standard
**Skills:** test-driven-development
**Files:**
- Modify: `jsonldb/jsonlfile.py`
- Modify: `unit_tests/test_jsonlfile.py`

**Step 1: Write the failing test**
Add to `unit_tests/test_jsonlfile.py`:
```python
def test_select_does_not_use_numpy(test_file, sample_data, monkeypatch):
    """Test that select_jsonl uses bisect, not numpy for range selection"""
    save_jsonl(test_file, sample_data)
    import numpy as np_mod
    original_array = np_mod.array
    def patched_array(*args, **kwargs):
        raise AssertionError("np.array should not be called during select_jsonl")
    monkeypatch.setattr(np_mod, "array", patched_array)
    try:
        selected = select_jsonl(test_file, lower_key="key1", upper_key="key2")
        assert len(selected) == 2
    finally:
        monkeypatch.setattr(np_mod, "array", original_array)
```

**Step 2: Run test to verify it fails**
Run: `cd /mnt/h/oceanwave/lib/jsonldb && python -m pytest unit_tests/test_jsonlfile.py::test_select_does_not_use_numpy -x`
Expected: FAIL — current `select_jsonl` creates numpy arrays

**Step 3: Write minimal implementation**
In `jsonldb/jsonlfile.py`:
- Add `from bisect import bisect_left, bisect_right` to imports
- Remove the `_select_keys_in_range` function
- Remove the `_fast_sort_records` function
- Remove the `_convert_key_to_sortable` function
- Remove the `_sort_numeric_keys` function
- Remove `from numba import jit` import
- In `select_jsonl`, replace the numpy range selection block with:
  ```python
  all_keys = list(index_dict.keys())  # already sorted from index
  lo = bisect_left(all_keys, lower_key)
  hi = bisect_right(all_keys, upper_key)
  selected_linekeys = all_keys[lo:hi]
  ```
- Sort selected byte offsets ascending for sequential I/O, then rebuild result dict in key order:
  ```python
  offset_key_pairs = sorted(
      [(index_dict[k], k) for k in selected_linekeys]
  )
  raw_results = {}
  with open(jsonl_file_path, 'rb', buffering=BUFFER_SIZE) as f:
      for offset, linekey in offset_key_pairs:
          f.seek(offset)
          line = f.readline()
          data = orjson.loads(line)
          raw_results[linekey] = data[linekey]
  # Rebuild in sorted key order
  result_dict = {}
  for linekey in selected_linekeys:
      # apply deserialization
      ...
  ```
- In `lint_jsonl`, replace `_fast_sort_records(records)` with `dict(sorted(records.items(), key=lambda x: str(x[0])))`

**Step 4: Run test to verify it passes**
Run: `cd /mnt/h/oceanwave/lib/jsonldb && python -m pytest unit_tests/test_jsonlfile.py -x`
Expected: ALL PASS

**Step 5: Commit**
```bash
cd /mnt/h/oceanwave/lib/jsonldb
git add jsonldb/jsonlfile.py unit_tests/test_jsonlfile.py
git commit -m "perf: replace Numba range selection with bisect O(log n)"
```

---

### Task 4: Stream-based linting with smart path detection
**Mode:** full
**Skills:** test-driven-development
**Files:**
- Modify: `jsonldb/jsonlfile.py`
- Modify: `unit_tests/test_jsonlfile.py`

**Step 1: Write the failing test**
Add to `unit_tests/test_jsonlfile.py`:
```python
def test_lint_skips_already_clean_file(test_file, sample_data):
    """Test that lint_jsonl skips rewrite for already sorted, compact files"""
    save_jsonl(test_file, sample_data)
    # File is already sorted and compact after save
    lint_jsonl(test_file)  # First lint to ensure clean state

    # Read file content after first lint
    with open(test_file, 'rb') as f:
        content_before = f.read()

    # Second lint should skip (file unchanged) — verify via content hash
    result = lint_jsonl(test_file)

    with open(test_file, 'rb') as f:
        content_after = f.read()

    assert result is True
    assert content_before == content_after  # File bytes unchanged — skip path taken

def test_lint_stream_compacts_dead_lines(test_file, sample_data):
    """Test that stream lint removes dead lines from delete operations"""
    save_jsonl(test_file, sample_data)
    delete_jsonl(test_file, ["key1"])

    size_before = os.path.getsize(test_file)
    lint_jsonl(test_file)
    size_after = os.path.getsize(test_file)

    # File should be smaller after compaction
    assert size_after < size_before

    # Data should still be correct
    loaded = load_jsonl(test_file)
    assert len(loaded) == 2
    assert "key1" not in loaded

def test_lint_returns_false_for_missing_file():
    """Test lint_jsonl returns False for non-existent file"""
    result = lint_jsonl("/nonexistent/path/test.jsonl")
    assert result is False
```

**Step 2: Run test to verify it fails**
Run: `cd /mnt/h/oceanwave/lib/jsonldb && python -m pytest unit_tests/test_jsonlfile.py::test_lint_skips_already_clean_file -x`
Expected: FAIL — current lint always rewrites

**Step 3: Write minimal implementation**
Rewrite `lint_jsonl` in `jsonldb/jsonlfile.py`:
```python
def lint_jsonl(jsonl_file_path: str) -> bool:
    if not os.path.exists(jsonl_file_path):
        return False

    if os.path.getsize(jsonl_file_path) == 0:
        ensure_index_exists(jsonl_file_path)
        return True

    # Step 1: Ensure index exists
    ensure_index_exists(jsonl_file_path)

    # Step 2: Line-count check
    with open(jsonl_file_path, 'rb') as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            non_blank_count = sum(
                1 for line in iter(mm.readline, b'')
                if line.strip()
            )

    with open(f"{jsonl_file_path}.idx", 'rb') as f:
        index_dict = orjson.loads(f.read())

    if non_blank_count != len(index_dict):
        # Orphaned/corrupted lines — rebuild index
        build_jsonl_index(jsonl_file_path)
        with open(f"{jsonl_file_path}.idx", 'rb') as f:
            index_dict = orjson.loads(f.read())
    else:
        # Spot-check first and last entries
        if index_dict:
            keys = list(index_dict.keys())
            with open(jsonl_file_path, 'rb', buffering=BUFFER_SIZE) as f:
                for check_key in [keys[0], keys[-1]]:
                    f.seek(index_dict[check_key])
                    line = f.readline()
                    data = orjson.loads(line)
                    parsed_key = next(iter(data))
                    if parsed_key != check_key:
                        build_jsonl_index(jsonl_file_path)
                        with open(f"{jsonl_file_path}.idx", 'rb') as f2:
                            index_dict = orjson.loads(f2.read())
                        break

    if not index_dict:
        return True

    # Step 3: Sorted + compaction check
    keys = list(index_dict.keys())
    is_sorted = all(keys[i] <= keys[i+1] for i in range(len(keys)-1))

    if is_sorted:
        # Check for dead space: compare file size vs last_offset + last_line_length
        with open(jsonl_file_path, 'rb', buffering=BUFFER_SIZE) as f:
            f.seek(index_dict[keys[-1]])
            last_line = f.readline()
            expected_end = index_dict[keys[-1]] + len(last_line)
            actual_size = os.path.getsize(jsonl_file_path)
            if expected_end == actual_size:
                return True  # Already clean — skip

    # Step 4: Stream-lint
    sorted_keys = sorted(keys, key=str)
    tmp_path = jsonl_file_path + '.tmp'

    with open(jsonl_file_path, 'rb', buffering=BUFFER_SIZE) as src:
        with open(tmp_path, 'wb', buffering=BUFFER_SIZE) as dst:
            for key in sorted_keys:
                src.seek(index_dict[key])
                line = src.readline()
                dst.write(line)

    os.replace(tmp_path, jsonl_file_path)
    build_jsonl_index(jsonl_file_path)
    return True
```

**Step 4: Run test to verify it passes**
Run: `cd /mnt/h/oceanwave/lib/jsonldb && python -m pytest unit_tests/test_jsonlfile.py -x`
Expected: ALL PASS

**Step 5: Commit**
```bash
cd /mnt/h/oceanwave/lib/jsonldb
git add jsonldb/jsonlfile.py unit_tests/test_jsonlfile.py
git commit -m "perf: stream-based linting with skip path for clean files"
```

---

### Task 5: FolderDB — split _get_file_path for reads vs writes
**Mode:** standard
**Skills:** test-driven-development
**Files:**
- Modify: `jsonldb/folderdb.py`
- Modify: `unit_tests/test_folderdb.py`

**Step 1: Write the failing test**
Add to `unit_tests/test_folderdb.py`:
```python
def test_get_dict_does_not_create_folders(db_folder, sample_data):
    """Test that reading via get_dict does not create new directories in hierarchy mode"""
    df, data_dict = sample_data
    db = FolderDB(db_folder, hierarchy_depth=2)
    db.upsert_dict("region.users", data_dict)

    # Count all directories recursively before read
    def count_dirs(path):
        count = 0
        for root, dirs, files in os.walk(path):
            count += len(dirs)
        return count

    dir_count_before = count_dirs(db_folder)

    # Try to read a non-existent hierarchical file — should NOT create its directory
    result = db.get_dict(["other.nonexistent"])

    dir_count_after = count_dirs(db_folder)
    assert dir_count_before == dir_count_after
```

**Step 2: Run test to verify it fails**
Run: `cd /mnt/h/oceanwave/lib/jsonldb && python -m pytest unit_tests/test_folderdb.py::test_get_dict_does_not_create_folders -x`
Expected: FAIL — current `_get_file_path` always calls `create_folder`

**Step 3: Write minimal implementation**
In `jsonldb/folderdb.py`:
- Rename existing `_get_file_path` to `_get_or_create_file_path` (keeps folder creation)
- Create new `_get_file_path` that only resolves the path without creating folders:
  ```python
  def _get_file_path(self, name: str) -> str:
      """Get the full path for a JSONL file (read-only, no folder creation)"""
      if self.use_hierarchy and not self.validate_name(name):
          raise ValueError(...)
      folder_path = self._get_hierarchy_path(name)
      if name.endswith('.jsonl'):
          return os.path.join(folder_path, name)
      return os.path.join(folder_path, f"{name}.jsonl")
  ```
- Update write methods to use `_get_or_create_file_path`: `upsert_dict`, `overwrite_dict`, `upsert_df`, `overwrite_df`, `delete_file`, `delete_file_keys`, `delete_file_range`
- Read methods keep using `_get_file_path`: `get_dict`, `get_df`, `__str__`, `build_dbmeta`, `update_dbmeta`

**Step 4: Run test to verify it passes**
Run: `cd /mnt/h/oceanwave/lib/jsonldb && python -m pytest unit_tests/test_folderdb.py -x`
Expected: ALL PASS

**Step 5: Commit**
```bash
cd /mnt/h/oceanwave/lib/jsonldb
git add jsonldb/folderdb.py unit_tests/test_folderdb.py
git commit -m "perf: split _get_file_path to avoid folder creation on reads"
```

---

### Task 6: FolderDB — skip build_configmeta if unchanged + optimize __init__
**Mode:** standard
**Skills:** test-driven-development
**Files:**
- Modify: `jsonldb/folderdb.py`
- Modify: `unit_tests/test_folderdb.py`

**Step 1: Write the failing test**
Add to `unit_tests/test_folderdb.py`:
```python
def test_init_does_not_rewrite_configmeta_if_unchanged(db_folder):
    """Test that __init__ skips config.meta rewrite when value unchanged"""
    db = FolderDB(db_folder)
    config_path = os.path.join(db_folder, "config.meta")

    # Read content after first init
    with open(config_path, 'rb') as f:
        content_before = f.read()
    size_before = os.path.getsize(config_path)

    # Re-init — should not rewrite (verify via content identity)
    db2 = FolderDB(db_folder)

    with open(config_path, 'rb') as f:
        content_after = f.read()

    assert content_before == content_after
```

**Step 2: Run test to verify it fails**
Run: `cd /mnt/h/oceanwave/lib/jsonldb && python -m pytest unit_tests/test_folderdb.py::test_init_does_not_rewrite_configmeta_if_unchanged -x`
Expected: FAIL — current __init__ always calls build_configmeta()

**Step 3: Write minimal implementation**
In `jsonldb/folderdb.py`, replace the `self.build_configmeta()` call at end of `__init__` with:
```python
# Only rewrite config.meta if it doesn't exist or timespec changed
# Note: config.meta stores {"timespec": value} as a flat JSONL record
# select_jsonl returns {"timespec": value} where "timespec" is the linekey
if os.path.exists(self.configmeta_path):
    config_meta = select_jsonl(self.configmeta_path)
    if not config_meta or config_meta.get("timespec") != jsonlfile.TIME_SPEC:
        self.build_configmeta()
else:
    self.build_configmeta()
```

**Step 4: Run test to verify it passes**
Run: `cd /mnt/h/oceanwave/lib/jsonldb && python -m pytest unit_tests/test_folderdb.py -x`
Expected: ALL PASS

**Step 5: Commit**
```bash
cd /mnt/h/oceanwave/lib/jsonldb
git add jsonldb/folderdb.py unit_tests/test_folderdb.py
git commit -m "perf: skip config.meta rewrite in __init__ when unchanged"
```

---

### Task 7: FolderDB — batch db.meta updates in lint_db
**Mode:** standard
**Skills:** test-driven-development
**Files:**
- Modify: `jsonldb/folderdb.py`
- Modify: `unit_tests/test_folderdb.py`

**Step 1: Write the failing test**
Add to `unit_tests/test_folderdb.py`:
```python
def test_lint_db_batches_meta_writes(db_folder, sample_data):
    """Test that lint_db writes db.meta only once at the end, not per file"""
    df, data_dict = sample_data
    db = FolderDB(db_folder)
    db.upsert_df("users", df)
    db.upsert_dict("products", data_dict)

    # Track calls to update_dbmeta
    call_count = 0
    original_update = db.update_dbmeta
    def counting_update(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        return original_update(*args, **kwargs)
    db.update_dbmeta = counting_update

    db.lint_db()

    # Should NOT call update_dbmeta per file anymore
    assert call_count == 0

    # But metadata should still be correct
    metadata = select_jsonl(os.path.join(db_folder, "db.meta"))
    assert metadata["users"]["linted"] is True
    assert metadata["products"]["linted"] is True
```

**Step 2: Run test to verify it fails**
Run: `cd /mnt/h/oceanwave/lib/jsonldb && python -m pytest unit_tests/test_folderdb.py::test_lint_db_batches_meta_writes -x`
Expected: FAIL — current lint_db calls update_dbmeta per file

**Step 3: Write minimal implementation**
Rewrite `lint_db` in `jsonldb/folderdb.py`:
```python
def lint_db(self) -> None:
    """Lint all JSONL files in the database."""
    meta_file = os.path.join(self.folder_path, "db.meta")
    if not os.path.exists(meta_file):
        self.build_dbmeta()

    metadata = select_jsonl(meta_file)
    print(f"Found {len(metadata)} JSONL files to lint.")

    all_meta = {}
    names_to_delete = []

    for name in metadata:
        print(f"Linting file: {name}")
        file_path = self._get_file_path(name)
        exist_flag = lint_jsonl(file_path)

        if not exist_flag:
            print(f"File {name} no longer exist, deleting metadata.")
            names_to_delete.append(name)
        else:
            # Build metadata entry inline
            index_file = file_path + ".idx"
            min_index = max_index = None
            count = 0
            if os.path.exists(index_file):
                with open(index_file, 'rb') as f:
                    index = orjson.loads(f.read())
                    if index:
                        keys = list(index.keys())
                        min_index, max_index = keys[0], keys[-1]
                        count = len(keys)

            all_meta[name] = {
                "name": name,
                "path": file_path,
                "min_index": min_index,
                "max_index": max_index,
                "size": os.path.getsize(file_path),
                "count": count,
                "lint_time": datetime.now().isoformat(),
                "linted": True
            }

    # Single write for all metadata
    save_jsonl(self.dbmeta_path, all_meta)
    lint_jsonl(self.dbmeta_path)

    if self.use_hierarchy:
        lint_jsonl(self.hmeta_path)
        self.delete_empty_folders()
```

**Step 4: Run test to verify it passes**
Run: `cd /mnt/h/oceanwave/lib/jsonldb && python -m pytest unit_tests/test_folderdb.py -x`
Expected: ALL PASS

**Step 5: Commit**
```bash
cd /mnt/h/oceanwave/lib/jsonldb
git add jsonldb/folderdb.py unit_tests/test_folderdb.py
git commit -m "perf: batch db.meta writes in lint_db"
```

---

### Task 8: Remove numba dependency
**Mode:** lightweight
**Files:**
- Modify: `jsonldb/jsonlfile.py`
- Modify: `setup.py`

**Step 1: (No test needed — dependency cleanup)**

**Step 2: Implementation**
- In `jsonldb/jsonlfile.py`: remove `import numpy as np` and `from numba import jit`. Keep `import mmap` (still used in `lint_jsonl` and `build_jsonl_index`). Note: `_fast_dumps` uses `orjson.OPT_SERIALIZE_NUMPY` which handles numpy types in user data without needing numpy imported in jsonlfile itself.
- In `setup.py`: remove `"numba>=0.54.0"` from `install_requires`. Keep `"numpy>=1.20.0"` since orjson's numpy serialization may handle numpy data passed by users.

**Step 3: Verify**
Run: `cd /mnt/h/oceanwave/lib/jsonldb && python -m pytest unit_tests/ -x`
Expected: ALL PASS

**Step 4: Commit**
```bash
cd /mnt/h/oceanwave/lib/jsonldb
git add jsonldb/jsonlfile.py setup.py
git commit -m "chore: remove numba dependency"
```

---

### Task 9: Create benchmark script
**Mode:** full
**Skills:** test-driven-development
**Files:**
- Create: `profile_test/benchmark.py`

**Step 1: Write the benchmark script**
Create `profile_test/benchmark.py` with:
- Timing utilities using `time.perf_counter`
- Test data generation at various scales (1K, 10K, 100K lines)
- Benchmarks for: `save_jsonl`, `load_jsonl`, `select_jsonl` (range query), `lint_jsonl` (sorted, unsorted, with dead lines), `FolderDB.__init__`, `lint_db`
- Tabulated output with operation, scale, and time
- Note: 1M lines benchmark is optional/configurable as it takes longer

**Step 2: Run the benchmark**
Run: `cd /mnt/h/oceanwave/lib/jsonldb && python profile_test/benchmark.py`
Expected: Outputs timing table for all operations

**Step 3: Commit**
```bash
cd /mnt/h/oceanwave/lib/jsonldb
git add profile_test/benchmark.py
git commit -m "test: add performance benchmark script"
```

---

### Task 10: Final integration test and cleanup
**Mode:** full
**Skills:** test-driven-development, verification-before-completion
**Files:**
- Test: `unit_tests/test_jsonlfile.py`
- Test: `unit_tests/test_jsonldf.py`
- Test: `unit_tests/test_folderdb.py`

**Step 1: Run full test suite**
Run: `cd /mnt/h/oceanwave/lib/jsonldb && python -m pytest unit_tests/ -v`
Expected: ALL PASS

**Step 2: Run benchmark and record results**
Run: `cd /mnt/h/oceanwave/lib/jsonldb && python profile_test/benchmark.py`
Expected: All benchmarks complete with timing output

**Step 3: Verify .jsonl files are human-readable**
Spot-check: after running tests, verify that any `.jsonl` file produced contains readable JSON lines (one JSON object per line).

**Step 4: Verify .idx files are compact**
Spot-check: after running tests, verify `.idx` files have no indentation/newlines.

**Step 5: Commit**
```bash
cd /mnt/h/oceanwave/lib/jsonldb
git add -A
git commit -m "perf: complete performance optimization — all tests pass"
```
