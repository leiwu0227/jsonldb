import os
import sys
import pytest
from datetime import datetime, timedelta
import json

# Add parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jsonldb.jsonlfile import (
    save_jsonl, load_jsonl, update_jsonl, delete_jsonl, 
    lint_jsonl, build_jsonl_index, select_jsonl
)

# Test fixtures
@pytest.fixture
def test_file():
    filename = "test_data.jsonl"
    yield filename
    # Cleanup
    if os.path.exists(filename):
        os.remove(filename)
    if os.path.exists(filename + ".idx"):
        os.remove(filename + ".idx")

@pytest.fixture
def sample_data():
    return {
        "key1": {"value": 1, "name": "test1"},
        "key2": {"value": 2, "name": "test2"},
        "key3": {"value": 3, "name": "test3"}
    }

@pytest.fixture
def datetime_data():
    base_time = datetime(2024, 1, 1, 12, 0)
    return {
        base_time: {"temp": 20.5, "humidity": 45},
        base_time + timedelta(hours=1): {"temp": 21.5, "humidity": 46},
        base_time + timedelta(hours=2): {"temp": 22.5, "humidity": 47}
    }

# Update Tests
def test_update_existing_records(test_file, sample_data):
    """Test updating existing records with same size data"""
    save_jsonl(test_file, sample_data)
    
    # Update existing records
    updates = {
        "key1": {"value": 10, "name": "updated1"},
        "key2": {"value": 20, "name": "updated2"}
    }
    update_jsonl(test_file, updates)
    
    # Verify updates
    loaded_data = load_jsonl(test_file)
    assert loaded_data["key1"]["value"] == 10
    assert loaded_data["key2"]["name"] == "updated2"
    assert loaded_data["key3"] == sample_data["key3"]  # Unchanged

def test_update_with_larger_data(test_file, sample_data):
    """Test updating records with larger data that won't fit in place"""
    save_jsonl(test_file, sample_data)
    
    # Update with larger data
    updates = {
        "key1": {
            "value": 10,
            "name": "updated1",
            "extra": "this makes the record larger"
        }
    }
    update_jsonl(test_file, updates)
    
    # Verify updates
    loaded_data = load_jsonl(test_file)
    assert loaded_data["key1"]["extra"] == "this makes the record larger"
    assert loaded_data["key2"] == sample_data["key2"]  # Unchanged

def test_update_with_new_records(test_file, sample_data):
    """Test adding new records via update"""
    save_jsonl(test_file, sample_data)
    
    # Add new records
    updates = {
        "key4": {"value": 4, "name": "test4"},
        "key5": {"value": 5, "name": "test5"}
    }
    update_jsonl(test_file, updates)
    
    # Verify updates
    loaded_data = load_jsonl(test_file)
    assert len(loaded_data) == 5
    assert loaded_data["key4"]["value"] == 4
    assert loaded_data["key5"]["name"] == "test5"

def test_update_with_datetime_keys(test_file, datetime_data):
    """Test updating records with datetime keys"""
    save_jsonl(test_file, datetime_data)
    
    # Update existing and add new datetime records
    base_time = datetime(2024, 1, 1, 12, 0)
    updates = {
        base_time: {"temp": 25.0, "humidity": 50},  # Update existing
        base_time + timedelta(hours=3): {"temp": 23.5, "humidity": 48}  # New record
    }
    update_jsonl(test_file, updates)
    
    # Verify updates
    loaded_data = load_jsonl(test_file)
    assert loaded_data[base_time]["temp"] == 25.0
    assert base_time + timedelta(hours=3) in loaded_data
    assert len(loaded_data) == 4

# Delete Tests
def test_delete_single_record(test_file, sample_data):
    """Test deleting a single record"""
    save_jsonl(test_file, sample_data)
    
    # Delete one record
    delete_jsonl(test_file, ["key2"])
    
    # Verify deletion
    loaded_data = load_jsonl(test_file)
    assert "key2" not in loaded_data
    assert len(loaded_data) == 2
    assert loaded_data["key1"] == sample_data["key1"]
    assert loaded_data["key3"] == sample_data["key3"]

def test_delete_multiple_records(test_file, sample_data):
    """Test deleting multiple records"""
    save_jsonl(test_file, sample_data)
    
    # Delete multiple records
    delete_jsonl(test_file, ["key1", "key3"])
    
    # Verify deletions
    loaded_data = load_jsonl(test_file)
    assert len(loaded_data) == 1
    assert "key1" not in loaded_data
    assert "key3" not in loaded_data
    assert loaded_data["key2"] == sample_data["key2"]

def test_delete_with_datetime_keys(test_file, datetime_data):
    """Test deleting records with datetime keys"""
    save_jsonl(test_file, datetime_data)
    
    # Delete records with datetime keys
    base_time = datetime(2024, 1, 1, 12, 0)
    delete_jsonl(test_file, [base_time, base_time + timedelta(hours=1)])
    
    # Verify deletions
    loaded_data = load_jsonl(test_file)
    assert len(loaded_data) == 1
    assert base_time not in loaded_data
    assert base_time + timedelta(hours=1) not in loaded_data
    assert base_time + timedelta(hours=2) in loaded_data

def test_delete_nonexistent_records(test_file, sample_data):
    """Test deleting records that don't exist"""
    save_jsonl(test_file, sample_data)
    
    # Try to delete nonexistent records
    delete_jsonl(test_file, ["nonexistent1", "nonexistent2"])
    
    # Verify no changes
    loaded_data = load_jsonl(test_file)
    assert loaded_data == sample_data

# Lint Tests
def test_lint_sorts_string_keys(test_file):
    """Test that lint_jsonl properly sorts string keys"""
    # Save data in unsorted order
    data = {
        "key3": {"value": 3},
        "key1": {"value": 1},
        "key2": {"value": 2}
    }
    save_jsonl(test_file, data)
    
    # Lint the file
    lint_jsonl(test_file)
    
    # Verify sorting
    loaded_data = load_jsonl(test_file)
    keys = list(loaded_data.keys())
    assert keys == sorted(keys)
    assert keys == ["key1", "key2", "key3"]

def test_lint_sorts_datetime_keys(test_file):
    """Test that lint_jsonl properly sorts datetime keys"""
    # Save data in unsorted order
    base_time = datetime(2024, 1, 1, 12, 0)
    data = {
        base_time + timedelta(hours=2): {"value": 3},
        base_time: {"value": 1},
        base_time + timedelta(hours=1): {"value": 2}
    }
    save_jsonl(test_file, data)
    
    # Lint the file
    lint_jsonl(test_file)
    
    # Verify sorting
    loaded_data = load_jsonl(test_file)
    keys = list(loaded_data.keys())
    assert keys == sorted(keys)
    assert keys[0] == base_time

def test_lint_removes_deleted_records(test_file, sample_data):
    """Test that lint_jsonl removes space-marked deleted records"""
    save_jsonl(test_file, sample_data)
    
    # Delete some records
    delete_jsonl(test_file, ["key1", "key2"])
    
    # Lint the file
    lint_jsonl(test_file)
    
    # Verify deleted records are removed
    loaded_data = load_jsonl(test_file)
    assert len(loaded_data) == 1
    assert list(loaded_data.keys()) == ["key3"]

def test_lint_handles_empty_file(test_file):
    """Test that lint_jsonl handles empty files correctly"""
    # Create empty file
    save_jsonl(test_file, {})
    
    # Lint the empty file
    lint_jsonl(test_file)
    
    # Verify file remains empty
    loaded_data = load_jsonl(test_file)
    assert len(loaded_data) == 0

def test_lint_mixed_key_types(test_file):
    """Test that lint_jsonl handles mixed key types correctly"""
    # Create data with mixed key types
    base_time = datetime(2024, 1, 1, 12, 0)
    data = {
        "key2": {"value": 2},
        base_time: {"value": 1},
        "key1": {"value": 3}
    }
    save_jsonl(test_file, data)
    
    # Lint the file
    lint_jsonl(test_file)
    
    # Verify proper sorting
    loaded_data = load_jsonl(test_file)
    keys = list(loaded_data.keys())
    string_keys = [k for k in keys if isinstance(k, str)]
    datetime_keys = [k for k in keys if isinstance(k, datetime)]
    
    assert sorted(string_keys) == string_keys
    assert sorted(datetime_keys) == datetime_keys

# Select Tests
def test_select_all_records(test_file, sample_data):
    """Test selecting all records when no range is specified"""
    save_jsonl(test_file, sample_data)
    
    # Select all records
    selected = select_jsonl(test_file)
    
    # Verify all records are returned
    assert selected == sample_data
    assert len(selected) == 3

def test_select_with_lower_bound(test_file, sample_data):
    """Test selecting records with only lower bound specified"""
    save_jsonl(test_file, sample_data)
    
    # Select records from key2 onwards
    selected = select_jsonl(test_file, lower_key="key2")
    
    # Verify selection
    assert len(selected) == 2
    assert "key1" not in selected
    assert "key2" in selected
    assert "key3" in selected

def test_select_with_upper_bound(test_file, sample_data):
    """Test selecting records with only upper bound specified"""
    save_jsonl(test_file, sample_data)
    
    # Select records up to key2
    selected = select_jsonl(test_file, upper_key="key2")
    
    # Verify selection
    assert len(selected) == 2
    assert "key1" in selected
    assert "key2" in selected
    assert "key3" not in selected

def test_select_with_both_bounds(test_file, sample_data):
    """Test selecting records with both bounds specified"""
    save_jsonl(test_file, sample_data)
    
    # Select records between key1 and key2
    selected = select_jsonl(test_file, lower_key="key1", upper_key="key2")
    
    # Verify selection
    assert len(selected) == 2
    assert "key1" in selected
    assert "key2" in selected
    assert "key3" not in selected

def test_select_with_datetime_keys(test_file, datetime_data):
    """Test selecting records with datetime keys"""
    save_jsonl(test_file, datetime_data)
    
    # Select records between two times
    base_time = datetime(2024, 1, 1, 12, 0)
    selected = select_jsonl(
        test_file,
        lower_key=base_time,
        upper_key=base_time + timedelta(hours=1)
    )
    
    # Verify selection
    assert len(selected) == 2
    assert base_time in selected
    assert base_time + timedelta(hours=1) in selected
    assert base_time + timedelta(hours=2) not in selected

def test_select_empty_range(test_file, sample_data):
    """Test selecting records with a range that has no matches"""
    save_jsonl(test_file, sample_data)
    
    # Select records between non-existent keys
    selected = select_jsonl(test_file, lower_key="key4", upper_key="key5")
    
    # Verify empty result
    assert len(selected) == 0

def test_select_single_record(test_file, sample_data):
    """Test selecting a single record using same lower and upper bound"""
    save_jsonl(test_file, sample_data)

    # Select just key2
    selected = select_jsonl(test_file, lower_key="key2", upper_key="key2")

    # Verify selection
    assert len(selected) == 1
    assert "key2" in selected
    assert selected["key2"] == sample_data["key2"]

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


def test_lint_force_detects_orphan_lines(test_file, sample_data):
    """Test that force=True detects orphaned lines via mmap cardinality scan"""
    save_jsonl(test_file, sample_data)

    # Append an orphaned line (not in index)
    import orjson as _orjson
    with open(test_file, 'ab') as f:
        f.write(_orjson.dumps({"orphan": {"v": 99}}) + b'\n')

    # Touch index to appear fresh despite missing the orphan entry.
    # This prevents ensure_index_exists() from rebuilding.
    os.utime(test_file + ".idx")

    # force=True: mmap scan counts 4 lines, index has 3 → rebuilds index
    lint_jsonl(test_file, force=True)
    loaded = load_jsonl(test_file)
    assert "orphan" in loaded


def test_lint_default_compacts_orphan_lines(test_file, sample_data):
    """Test that force=False compacts away orphaned lines via sort/compaction"""
    save_jsonl(test_file, sample_data)

    # Append an orphaned line (not in index)
    import orjson as _orjson
    with open(test_file, 'ab') as f:
        f.write(_orjson.dumps({"orphan": {"v": 99}}) + b'\n')

    # Touch index to appear fresh despite missing the orphan entry
    os.utime(test_file + ".idx")

    # force=False: fast path loads old index, compaction removes orphan
    lint_jsonl(test_file, force=False)
    loaded = load_jsonl(test_file)
    assert "orphan" not in loaded
    assert len(loaded) == len(sample_data)


def test_lint_bad_index_offsets_recovery(test_file, sample_data):
    """Test that lint recovers when index has valid JSON but wrong offsets"""
    import orjson as _orjson
    save_jsonl(test_file, sample_data)

    # Write a valid JSON index with bad byte offsets
    bad_index = {"a": 1, "b": 999999}
    with open(test_file + ".idx", 'wb') as f:
        f.write(_orjson.dumps(bad_index))
    os.utime(test_file + ".idx")

    result = lint_jsonl(test_file)
    assert result is True

    loaded = load_jsonl(test_file)
    assert len(loaded) == len(sample_data)


def test_lint_non_integer_index_offsets_recovery(test_file, sample_data):
    """Test that lint recovers when index has non-integer offset values"""
    import orjson as _orjson
    save_jsonl(test_file, sample_data)

    bad_index = {"key1": "not-an-int", "key2": 0}
    with open(test_file + ".idx", 'wb') as f:
        f.write(_orjson.dumps(bad_index))
    os.utime(test_file + ".idx")

    result = lint_jsonl(test_file)
    assert result is True

    loaded = load_jsonl(test_file)
    assert len(loaded) == len(sample_data)


def test_lint_stale_index_recovers(test_file, sample_data):
    """Test that lint_jsonl handles a stale index gracefully"""
    save_jsonl(test_file, sample_data)

    # Make the index appear old — ensure_index_exists() will rebuild it,
    # then the mtime gate will see a fresh index and use the fast path.
    # Either way, data integrity is maintained.
    os.utime(test_file + ".idx", (0, 0))

    result = lint_jsonl(test_file)
    assert result is True

    # Verify data is intact after recovery
    loaded = load_jsonl(test_file)
    assert len(loaded) == len(sample_data)


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


def test_index_file_is_compact(test_file, sample_data):
    """Test that .idx files are written in compact format (no indentation)"""
    save_jsonl(test_file, sample_data)
    with open(test_file + ".idx", "rb") as f:
        raw = f.read()
    # Compact format has no newlines within the JSON object
    decoded = raw.decode("utf-8")
    assert "\n" not in decoded.strip()