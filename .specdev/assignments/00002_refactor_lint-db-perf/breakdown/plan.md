# lint_db Performance Optimization — Implementation Plan

> **For agent:** Implement this plan task-by-task using TDD discipline.

**Goal:** Make `lint_jsonl()` near-instant for the common post-save case by skipping the O(file_size) mmap line-count scan when the index is fresh, and expose a `force` parameter for full verification when needed.

**Architecture:** Add an mtime-based fast path to `lint_jsonl()` that runs after `ensure_index_exists()`. When `.idx` mtime >= `.jsonl` mtime and `force=False`, skip the mmap scan and jump directly to spot check + sort/compaction. Extract the shared verification logic (spot check, sort/compaction, stream-lint) into a helper `_verify_and_compact()` used by both paths. Add `force` parameter to `FolderDB.lint_db()` passed through to `lint_jsonl()`.

**Tech Stack:** Python, orjson, mmap, os.path.getmtime, pytest

---

### Task 1: Extract `_verify_and_compact()` helper from `lint_jsonl()`
**Mode:** standard
**Skills:** test-driven-development
**Files:** Modify `jsonldb/jsonlfile.py`, Modify `unit_tests/test_jsonlfile.py`

**Step 1: Write the failing test**

Add to `unit_tests/test_jsonlfile.py`:

```python
def test_verify_and_compact_sorts_unsorted(test_file):
    """Test that _verify_and_compact sorts unsorted keys"""
    from jsonldb.jsonlfile import _verify_and_compact, build_jsonl_index
    import orjson
    # Write keys out of order
    data = {"c": {"v": 3}, "a": {"v": 1}, "b": {"v": 2}}
    with open(test_file, 'wb') as f:
        for k, v in data.items():
            f.write(orjson.dumps({k: v}) + b'\n')
    build_jsonl_index(test_file)

    with open(test_file + ".idx", 'rb') as f:
        index_dict = orjson.loads(f.read())

    _verify_and_compact(test_file, index_dict)

    # Verify file is now sorted
    with open(test_file, 'rb') as f:
        lines = f.readlines()
    keys = [next(iter(orjson.loads(line))) for line in lines]
    assert keys == ["a", "b", "c"]
```

**Step 2: Run test to verify it fails**
Run: `cd /mnt/h/oceanwave/lib/jsonldb && python -m pytest unit_tests/test_jsonlfile.py::test_verify_and_compact_sorts_unsorted -x`
Expected: FAIL with `ImportError: cannot import name '_verify_and_compact'`

**Step 3: Write minimal implementation**

In `jsonldb/jsonlfile.py`, extract lines 153-199 of the current `lint_jsonl()` into a new function. Insert it just before `lint_jsonl()`:

```python
def _verify_and_compact(jsonl_file_path: str, index_dict: dict) -> bool:
    """Spot-check, sort-verify, and compact a JSONL file using a pre-loaded index.

    Args:
        jsonl_file_path: Path to the JSONL file
        index_dict: Pre-loaded index dictionary (key -> byte offset)

    Returns:
        True if file exists and was processed, False if index is empty
    """
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

    keys = list(index_dict.keys())
    is_sorted = all(keys[i] <= keys[i+1] for i in range(len(keys)-1))

    if is_sorted:
        if index_dict[keys[0]] == 0:
            with open(jsonl_file_path, 'rb', buffering=BUFFER_SIZE) as f:
                f.seek(index_dict[keys[-1]])
                last_line = f.readline()
                expected_end = index_dict[keys[-1]] + len(last_line)
                actual_size = os.path.getsize(jsonl_file_path)
                if expected_end == actual_size:
                    return True

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

Then update `lint_jsonl()` to call `_verify_and_compact()` instead of inlining the logic:

```python
def lint_jsonl(jsonl_file_path: str) -> bool:
    if not os.path.exists(jsonl_file_path):
        return False

    if os.path.getsize(jsonl_file_path) == 0:
        ensure_index_exists(jsonl_file_path)
        return True

    ensure_index_exists(jsonl_file_path)

    # Full mmap line-count scan
    with open(jsonl_file_path, 'rb') as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            non_blank_count = sum(
                1 for line in iter(mm.readline, b'')
                if line.strip()
            )

    with open(f"{jsonl_file_path}.idx", 'rb') as f:
        index_dict = orjson.loads(f.read())

    if non_blank_count != len(index_dict):
        build_jsonl_index(jsonl_file_path)
        with open(f"{jsonl_file_path}.idx", 'rb') as f:
            index_dict = orjson.loads(f.read())

    return _verify_and_compact(jsonl_file_path, index_dict)
```

**Step 4: Run test to verify it passes**
Run: `cd /mnt/h/oceanwave/lib/jsonldb && python -m pytest unit_tests/test_jsonlfile.py::test_verify_and_compact_sorts_unsorted -x`
Expected: PASS

**Step 5: Run full test suite for regression**
Run: `cd /mnt/h/oceanwave/lib/jsonldb && python -m pytest unit_tests/test_jsonlfile.py -x`
Expected: All tests PASS

**Step 6: Commit**
```
git add jsonldb/jsonlfile.py unit_tests/test_jsonlfile.py
git commit -m "refactor: extract _verify_and_compact from lint_jsonl"
```

---

### Task 2: Add `force` parameter and mtime fast path to `lint_jsonl()`
**Mode:** full
**Skills:** test-driven-development
**Files:** Modify `jsonldb/jsonlfile.py`, Modify `unit_tests/test_jsonlfile.py`

**Step 1: Write the failing tests**

Add to `unit_tests/test_jsonlfile.py`:

```python
def test_lint_mtime_skip_path(test_file, sample_data):
    """Test that lint_jsonl skips mmap scan when index is fresh"""
    save_jsonl(test_file, sample_data)
    lint_jsonl(test_file, force=True)  # Ensure clean state

    # Get file state before
    mtime_before = os.path.getmtime(test_file)

    # Call lint without force — should use fast path (index is fresh)
    result = lint_jsonl(test_file)
    assert result is True

    # File should not have been rewritten (mtime unchanged)
    mtime_after = os.path.getmtime(test_file)
    assert mtime_before == mtime_after


def test_lint_force_runs_full_scan(test_file, sample_data):
    """Test that force=True bypasses mtime check and runs full mmap scan"""
    save_jsonl(test_file, sample_data)

    # force=True should work and return True
    result = lint_jsonl(test_file, force=True)
    assert result is True


def test_lint_stale_index_triggers_full_scan(test_file, sample_data):
    """Test that stale index (older than data file) triggers full scan"""
    save_jsonl(test_file, sample_data)

    # Make the index older than the data file by touching the data file
    import time
    time.sleep(1.1)  # Ensure mtime difference on WSL2
    with open(test_file, 'ab') as f:
        pass  # Touch the file to update mtime
    os.utime(test_file)  # Ensure mtime updates

    # lint should still work (falls through to full scan)
    result = lint_jsonl(test_file)
    assert result is True


def test_lint_corrupt_index_fast_path_recovery(test_file, sample_data):
    """Test that corrupt index on fast path triggers rebuild"""
    save_jsonl(test_file, sample_data)

    # Corrupt the index file
    with open(test_file + ".idx", 'wb') as f:
        f.write(b'not valid json')

    # Touch index to make it appear fresh
    os.utime(test_file + ".idx")

    # lint should recover via rebuild
    result = lint_jsonl(test_file)
    assert result is True

    # Data should still be readable
    loaded = load_jsonl(test_file)
    assert len(loaded) == len(sample_data)
```

**Step 2: Run tests to verify they fail**
Run: `cd /mnt/h/oceanwave/lib/jsonldb && python -m pytest unit_tests/test_jsonlfile.py::test_lint_mtime_skip_path -x`
Expected: FAIL with `TypeError: lint_jsonl() got an unexpected keyword argument 'force'`

**Step 3: Write minimal implementation**

Update `lint_jsonl()` in `jsonldb/jsonlfile.py`:

```python
def lint_jsonl(jsonl_file_path: str, force: bool = False) -> bool:
    """Clean and optimize a JSONL file.

    Uses stream-based approach to avoid loading entire file into memory.
    Skips rewrite if file is already sorted and compact.

    When force=False (default), skips the expensive mmap line-count scan
    if the index file is at least as recent as the data file.

    Args:
        jsonl_file_path: Path to the JSONL file to optimize
        force: If True, always run full mmap line-count verification

    Returns:
        bool: True if file exists (whether skipped or linted), False if not found
    """
    if not os.path.exists(jsonl_file_path):
        return False

    if os.path.getsize(jsonl_file_path) == 0:
        ensure_index_exists(jsonl_file_path)
        return True

    ensure_index_exists(jsonl_file_path)

    index_path = jsonl_file_path + ".idx"

    # Fast path: skip mmap scan when index is fresh
    if not force:
        idx_mtime = os.path.getmtime(index_path)
        data_mtime = os.path.getmtime(jsonl_file_path)
        if idx_mtime >= data_mtime:
            try:
                with open(index_path, 'rb') as f:
                    index_dict = orjson.loads(f.read())
            except (orjson.JSONDecodeError, OSError):
                build_jsonl_index(jsonl_file_path)
                with open(index_path, 'rb') as f:
                    index_dict = orjson.loads(f.read())
            return _verify_and_compact(jsonl_file_path, index_dict)

    # Full path: mmap line-count scan
    with open(jsonl_file_path, 'rb') as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            non_blank_count = sum(
                1 for line in iter(mm.readline, b'')
                if line.strip()
            )

    with open(index_path, 'rb') as f:
        index_dict = orjson.loads(f.read())

    if non_blank_count != len(index_dict):
        build_jsonl_index(jsonl_file_path)
        with open(index_path, 'rb') as f:
            index_dict = orjson.loads(f.read())

    return _verify_and_compact(jsonl_file_path, index_dict)
```

**Step 4: Run tests to verify they pass**
Run: `cd /mnt/h/oceanwave/lib/jsonldb && python -m pytest unit_tests/test_jsonlfile.py::test_lint_mtime_skip_path unit_tests/test_jsonlfile.py::test_lint_force_runs_full_scan unit_tests/test_jsonlfile.py::test_lint_stale_index_triggers_full_scan unit_tests/test_jsonlfile.py::test_lint_corrupt_index_fast_path_recovery -x`
Expected: All PASS

**Step 5: Run full test suite for regression**
Run: `cd /mnt/h/oceanwave/lib/jsonldb && python -m pytest unit_tests/test_jsonlfile.py -x`
Expected: All tests PASS

**Step 6: Commit**
```
git add jsonldb/jsonlfile.py unit_tests/test_jsonlfile.py
git commit -m "perf: add mtime-based fast path and force parameter to lint_jsonl"
```

---

### Task 3: Add `force` parameter to `FolderDB.lint_db()` and pass through
**Mode:** full
**Skills:** test-driven-development
**Files:** Modify `jsonldb/folderdb.py`, Modify `unit_tests/test_folderdb.py`

**Step 1: Write the failing test**

Add to `unit_tests/test_folderdb.py`:

```python
def test_lint_db_force_parameter(db, sample_data):
    """Test that lint_db accepts and passes through force parameter"""
    db.save_dict("test_table", sample_data)

    # Default (force=False) should work
    db.lint_db()

    # Explicit force=True should work
    db.lint_db(force=True)

    # Explicit force=False should work
    db.lint_db(force=False)

    # Data should still be intact after all lint modes
    result = db.get_dict("test_table")
    assert len(result) == len(sample_data)
```

**Step 2: Run test to verify it fails**
Run: `cd /mnt/h/oceanwave/lib/jsonldb && python -m pytest unit_tests/test_folderdb.py::test_lint_db_force_parameter -x`
Expected: FAIL with `TypeError: lint_db() got an unexpected keyword argument 'force'`

**Step 3: Write minimal implementation**

Update `lint_db()` in `jsonldb/folderdb.py`:

```python
    def lint_db(self, force: bool = False) -> None:
        """Lint all JSONL files in the database.

        Args:
            force: If True, run full mmap line-count verification on every file.
                   If False (default), skip the scan when the index is fresh.
        """
        import orjson
        meta_file = os.path.join(self.folder_path, "db.meta")
        if not os.path.exists(meta_file):
            self.build_dbmeta()

        metadata = select_jsonl(meta_file)
        print(f"Found {len(metadata)} JSONL files to lint.")

        all_meta = {}

        for name in metadata:
            print(f"Linting file: {name}")
            file_path = self._get_file_path(name)
            exist_flag = lint_jsonl(file_path, force=force)

            if not exist_flag:
                print(f"File {name} no longer exist, deleting metadata.")
            else:
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

        save_jsonl(self.dbmeta_path, all_meta)
        lint_jsonl(self.dbmeta_path, force=force)

        if self.use_hierarchy:
            lint_jsonl(self.hmeta_path, force=force)
            self.delete_empty_folders()
```

**Step 4: Run test to verify it passes**
Run: `cd /mnt/h/oceanwave/lib/jsonldb && python -m pytest unit_tests/test_folderdb.py::test_lint_db_force_parameter -x`
Expected: PASS

**Step 5: Run full test suite for regression**
Run: `cd /mnt/h/oceanwave/lib/jsonldb && python -m pytest unit_tests/ -x`
Expected: All tests PASS

**Step 6: Commit**
```
git add jsonldb/folderdb.py unit_tests/test_folderdb.py
git commit -m "perf: add force parameter to FolderDB.lint_db, pass through to lint_jsonl"
```
