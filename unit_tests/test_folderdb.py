import pytest
import pandas as pd
import os
from datetime import datetime, timedelta
from folderdb import FolderDB

@pytest.fixture
def test_folder(tmp_path):
    """Create a temporary test folder"""
    return str(tmp_path)

@pytest.fixture
def sample_df():
    """Create a sample DataFrame"""
    return pd.DataFrame({
        'value': [1, 2, 3]
    }, index=['key1', 'key2', 'key3'])

@pytest.fixture
def datetime_df():
    """Create a DataFrame with datetime keys"""
    base_time = datetime(2024, 1, 1, 12, 0)
    return pd.DataFrame({
        'value': [1, 2, 3]
    }, index=[
        base_time,
        base_time + timedelta(hours=1),
        base_time + timedelta(hours=2)
    ])

@pytest.fixture
def sample_dict():
    """Create a sample dictionary"""
    return {
        'key1': {'value': 1},
        'key2': {'value': 2},
        'key3': {'value': 3}
    }

def test_init(test_folder):
    """Test FolderDB initialization"""
    # Test with existing folder
    db = FolderDB(test_folder)
    assert db.folder_path == test_folder
    
    # Test with non-existent folder
    with pytest.raises(FileNotFoundError):
        FolderDB(os.path.join(test_folder, "nonexistent"))

def test_upsert_df(test_folder, sample_df):
    """Test upserting DataFrame"""
    db = FolderDB(test_folder)
    
    # Test creating new file
    db.upsert_df("test1", sample_df)
    assert os.path.exists(os.path.join(test_folder, "test1.jsonl"))
    assert os.path.exists(os.path.join(test_folder, "test1.jsonl.idx"))
    
    # Test updating existing file
    updated_df = pd.DataFrame({
        'value': [10, 20]
    }, index=['key1', 'key2'])
    db.upsert_df("test1", updated_df)
    
    # Verify update
    loaded_df = db.get_df(["test1"])["test1"]
    assert loaded_df.loc['key1', 'value'] == 10
    assert loaded_df.loc['key2', 'value'] == 20
    assert loaded_df.loc['key3', 'value'] == 3

def test_upsert_dfs(test_folder, sample_df, datetime_df):
    """Test upserting multiple DataFrames"""
    db = FolderDB(test_folder)
    
    # Test creating multiple files
    dict_dfs = {
        "test1": sample_df,
        "test2": datetime_df
    }
    db.upsert_dfs(dict_dfs)
    
    # Verify files were created
    assert os.path.exists(os.path.join(test_folder, "test1.jsonl"))
    assert os.path.exists(os.path.join(test_folder, "test2.jsonl"))
    
    # Verify data
    loaded_dfs = db.get_df(["test1", "test2"])
    pd.testing.assert_frame_equal(loaded_dfs["test1"], sample_df)
    pd.testing.assert_frame_equal(loaded_dfs["test2"], datetime_df)

def test_upsert_dict(test_folder, sample_dict):
    """Test upserting dictionary"""
    db = FolderDB(test_folder)
    
    # Test creating new file
    db.upsert_dict("test1", sample_dict)
    assert os.path.exists(os.path.join(test_folder, "test1.jsonl"))
    assert os.path.exists(os.path.join(test_folder, "test1.jsonl.idx"))
    
    # Test updating existing file
    updated_dict = {
        'key1': {'value': 10},
        'key2': {'value': 20}
    }
    db.upsert_dict("test1", updated_dict)
    
    # Verify update
    loaded_dict = db.get_dict(["test1"])["test1"]
    assert loaded_dict['key1']['value'] == 10
    assert loaded_dict['key2']['value'] == 20
    assert loaded_dict['key3']['value'] == 3

def test_upsert_dicts(test_folder, sample_dict):
    """Test upserting multiple dictionaries"""
    db = FolderDB(test_folder)
    
    # Test creating multiple files
    dict_dicts = {
        "test1": sample_dict,
        "test2": {k: {'value': v['value'] * 2} for k, v in sample_dict.items()}
    }
    db.upsert_dicts(dict_dicts)
    
    # Verify files were created
    assert os.path.exists(os.path.join(test_folder, "test1.jsonl"))
    assert os.path.exists(os.path.join(test_folder, "test2.jsonl"))
    
    # Verify data
    loaded_dicts = db.get_dict(["test1", "test2"])
    assert loaded_dicts["test1"] == sample_dict
    assert loaded_dicts["test2"] == {k: {'value': v['value'] * 2} for k, v in sample_dict.items()}

def test_get_df(test_folder, sample_df, datetime_df):
    """Test getting DataFrames"""
    db = FolderDB(test_folder)
    
    # Setup test data
    db.upsert_df("test1", sample_df)
    db.upsert_df("test2", datetime_df)
    
    # Test getting all records
    result = db.get_df(["test1", "test2"])
    pd.testing.assert_frame_equal(result["test1"], sample_df)
    pd.testing.assert_frame_equal(result["test2"], datetime_df)
    
    # Test getting records in range
    base_time = datetime(2024, 1, 1, 12, 0)
    result = db.get_df(
        ["test2"],
        lower_key=base_time,
        upper_key=base_time + timedelta(hours=1)
    )
    assert len(result["test2"]) == 2
    assert base_time in result["test2"].index
    assert base_time + timedelta(hours=1) in result["test2"].index

def test_get_dict(test_folder, sample_dict):
    """Test getting dictionaries"""
    db = FolderDB(test_folder)
    
    # Setup test data
    db.upsert_dict("test1", sample_dict)
    
    # Test getting all records
    result = db.get_dict(["test1"])
    assert result["test1"] == sample_dict
    
    # Test getting records in range
    result = db.get_dict(
        ["test1"],
        lower_key="key1",
        upper_key="key2"
    )
    assert len(result["test1"]) == 2
    assert "key1" in result["test1"]
    assert "key2" in result["test1"]
    assert "key3" not in result["test1"]

def test_delete_file(test_folder, sample_dict):
    """Test deleting specific keys"""
    db = FolderDB(test_folder)
    
    # Setup test data
    db.upsert_dict("test1", sample_dict)
    
    # Delete specific keys
    db.delete_file("test1", ["key1", "key2"])
    
    # Verify deletion
    result = db.get_dict(["test1"])
    assert len(result["test1"]) == 1
    assert "key3" in result["test1"]
    assert "key1" not in result["test1"]
    assert "key2" not in result["test1"]

def test_delete_file_range(test_folder, sample_dict):
    """Test deleting keys in range"""
    db = FolderDB(test_folder)
    
    # Setup test data
    db.upsert_dict("test1", sample_dict)
    
    # Delete keys in range
    db.delete_file_range("test1", "key1", "key2")
    
    # Verify deletion
    result = db.get_dict(["test1"])
    assert len(result["test1"]) == 1
    assert "key3" in result["test1"]
    assert "key1" not in result["test1"]
    assert "key2" not in result["test1"]

def test_delete_range(test_folder, sample_dict):
    """Test deleting keys in range from multiple files"""
    db = FolderDB(test_folder)
    
    # Setup test data
    db.upsert_dict("test1", sample_dict)
    db.upsert_dict("test2", sample_dict)
    
    # Delete keys in range from both files
    db.delete_range(["test1", "test2"], "key1", "key2")
    
    # Verify deletion
    result = db.get_dict(["test1", "test2"])
    for name in ["test1", "test2"]:
        assert len(result[name]) == 1
        assert "key3" in result[name]
        assert "key1" not in result[name]
        assert "key2" not in result[name]

def test_str_representation(test_folder, sample_dict):
    """Test string representation"""
    db = FolderDB(test_folder)
    
    # Setup test data
    db.upsert_dict("test1", sample_dict)
    
    # Get string representation
    str_rep = str(db)
    
    # Verify content
    assert "FolderDB at" in str_rep
    assert "Found 1 JSONL files" in str_rep
    assert "test1.jsonl" in str_rep
    assert "Size:" in str_rep
    assert "Key range:" in str_rep
    assert "Count:" in str_rep 