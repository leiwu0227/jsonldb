import os
import sys
import pytest
from datetime import datetime, timedelta
import json

# Add parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jsonlfile import (
    save_jsonl, load_jsonl, update_jsonl, delete_jsonl, 
    lint_jsonl, build_jsonl_index
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