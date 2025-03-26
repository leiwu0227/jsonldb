import pytest
import pandas as pd
from datetime import datetime, timedelta
import os
import sys

# Add parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jsonldf import (
    save_jsonldf, load_jsonldf, update_jsonldf, 
    delete_jsonldf, select_jsonldf, lint_jsonldf
)

@pytest.fixture
def test_file(tmp_path):
    """Create a temporary test file"""
    return str(tmp_path / "test.jsonl")

@pytest.fixture
def sample_df():
    """Create a sample DataFrame with string keys"""
    return pd.DataFrame({
        'value': [1, 2, 3]
    }, index=['key1', 'key2', 'key3'])

@pytest.fixture
def datetime_df():
    """Create a sample DataFrame with datetime keys"""
    base_time = datetime(2024, 1, 1, 12, 0)
    return pd.DataFrame({
        'value': [1, 2, 3]
    }, index=[
        base_time,
        base_time + timedelta(hours=1),
        base_time + timedelta(hours=2)
    ])

@pytest.fixture
def duplicate_df():
    """Create a DataFrame with duplicate indices"""
    return pd.DataFrame({
        'value': [1, 2, 3]
    }, index=['key1', 'key1', 'key3'])

def test_save_and_load_jsonldf(test_file, sample_df):
    """Test saving and loading DataFrame to/from JSONL"""
    # Save DataFrame
    save_jsonldf(test_file, sample_df)
    
    # Load DataFrame back
    loaded_df = load_jsonldf(test_file)
    
    # Verify data
    pd.testing.assert_frame_equal(sample_df, loaded_df)
    
    # Cleanup
    os.remove(test_file)
    os.remove(test_file + '.idx')

def test_save_jsonldf_duplicate_index(test_file, duplicate_df):
    """Test saving DataFrame with duplicate indices"""
    with pytest.raises(ValueError, match="DataFrame index must be unique"):
        save_jsonldf(test_file, duplicate_df)
    
    # Verify no files were created
    assert not os.path.exists(test_file)
    assert not os.path.exists(test_file + '.idx')

def test_update_jsonldf(test_file, sample_df):
    """Test updating existing records"""
    # Save initial data
    save_jsonldf(test_file, sample_df)
    
    # Create update DataFrame
    update_df = pd.DataFrame({
        'value': [10, 20]
    }, index=['key1', 'key2'])
    
    # Update records
    update_jsonldf(test_file, update_df)
    
    # Load and verify
    loaded_df = load_jsonldf(test_file)
    assert loaded_df.loc['key1', 'value'] == 10
    assert loaded_df.loc['key2', 'value'] == 20
    assert loaded_df.loc['key3', 'value'] == 3
    
    # Cleanup
    os.remove(test_file)
    os.remove(test_file + '.idx')

def test_delete_jsonldf(test_file, sample_df):
    """Test deleting records"""
    # Save initial data
    save_jsonldf(test_file, sample_df)
    
    # Delete records
    delete_jsonldf(test_file, ['key1', 'key2'])
    
    # Load and verify
    loaded_df = load_jsonldf(test_file)
    assert len(loaded_df) == 1
    assert 'key3' in loaded_df.index
    
    # Cleanup
    os.remove(test_file)
    os.remove(test_file + '.idx')

def test_select_jsonldf_with_range(test_file, sample_df):
    """Test selecting records within a key range"""
    # Save initial data
    save_jsonldf(test_file, sample_df)
    
    # Select records between key1 and key2
    selected = select_jsonldf(test_file, lower_key="key1", upper_key="key2")
    
    # Verify selection
    assert len(selected) == 2
    assert "key1" in selected.index
    assert "key2" in selected.index
    assert "key3" not in selected.index
    
    # Cleanup
    os.remove(test_file)
    os.remove(test_file + '.idx')

def test_select_jsonldf_with_lower_bound(test_file, sample_df):
    """Test selecting records with only lower bound"""
    # Save initial data
    save_jsonldf(test_file, sample_df)
    
    # Select records from key2 onwards
    selected = select_jsonldf(test_file, lower_key="key2")
    
    # Verify selection
    assert len(selected) == 2
    assert "key1" not in selected.index
    assert "key2" in selected.index
    assert "key3" in selected.index
    
    # Cleanup
    os.remove(test_file)
    os.remove(test_file + '.idx')

def test_select_jsonldf_with_upper_bound(test_file, sample_df):
    """Test selecting records with only upper bound"""
    # Save initial data
    save_jsonldf(test_file, sample_df)
    
    # Select records up to key2
    selected = select_jsonldf(test_file, upper_key="key2")
    
    # Verify selection
    assert len(selected) == 2
    assert "key1" in selected.index
    assert "key2" in selected.index
    assert "key3" not in selected.index
    
    # Cleanup
    os.remove(test_file)
    os.remove(test_file + '.idx')

def test_select_jsonldf_all_records(test_file, sample_df):
    """Test selecting all records when no range specified"""
    # Save initial data
    save_jsonldf(test_file, sample_df)
    
    # Select all records
    selected = select_jsonldf(test_file)
    
    # Verify all records are returned
    assert len(selected) == 3
    assert all(key in selected.index for key in ["key1", "key2", "key3"])
    
    # Cleanup
    os.remove(test_file)
    os.remove(test_file + '.idx')

def test_select_jsonldf_empty_range(test_file, sample_df):
    """Test selecting records with a range that has no matches"""
    # Save initial data
    save_jsonldf(test_file, sample_df)
    
    # Select records between non-existent keys
    selected = select_jsonldf(test_file, lower_key="key4", upper_key="key5")
    
    # Verify empty result
    assert len(selected) == 0
    
    # Cleanup
    os.remove(test_file)
    os.remove(test_file + '.idx')

def test_select_jsonldf_single_record(test_file, sample_df):
    """Test selecting a single record using same lower and upper bound"""
    # Save initial data
    save_jsonldf(test_file, sample_df)
    
    # Select just key2
    selected = select_jsonldf(test_file, lower_key="key2", upper_key="key2")
    
    # Verify selection
    assert len(selected) == 1
    assert "key2" in selected.index
    assert selected.loc["key2", "value"] == sample_df.loc["key2", "value"]
    
    # Cleanup
    os.remove(test_file)
    os.remove(test_file + '.idx')

def test_select_jsonldf_with_datetime_keys(test_file, datetime_df):
    """Test selecting records with datetime keys"""
    # Save initial data
    save_jsonldf(test_file, datetime_df)
    
    # Select records between two times
    base_time = datetime(2024, 1, 1, 12, 0)
    selected = select_jsonldf(
        test_file,
        lower_key=base_time,
        upper_key=base_time + timedelta(hours=1)
    )
    
    # Verify selection
    assert len(selected) == 2
    assert base_time in selected.index
    assert base_time + timedelta(hours=1) in selected.index
    assert base_time + timedelta(hours=2) not in selected.index
    
    # Cleanup
    os.remove(test_file)
    os.remove(test_file + '.idx')

def test_lint_jsonldf(test_file, sample_df):
    """Test linting JSONL file"""
    # Save initial data
    save_jsonldf(test_file, sample_df)
    
    # Lint file
    result = lint_jsonldf(test_file)
    
    # Verify file still contains correct data
    loaded_df = load_jsonldf(test_file)
    pd.testing.assert_frame_equal(sample_df, loaded_df)
    
    # Cleanup
    os.remove(test_file)
    os.remove(test_file + '.idx') 