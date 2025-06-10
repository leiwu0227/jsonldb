import pytest
import pandas as pd
import os
from datetime import datetime, timedelta
from jsonldb import FolderDB
import json
from jsonldb.jsonlfile import select_jsonl

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

@pytest.fixture
def sample_data():
    """Create sample data for testing."""
    # Create sample DataFrame
    df = pd.DataFrame({
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35],
        'city': ['New York', 'London', 'Paris']
    }, index=['user1', 'user2', 'user3'])
    
    # Create sample dictionary
    data_dict = {
        'prod1': {'name': 'Laptop', 'price': 1000},
        'prod2': {'name': 'Phone', 'price': 500}
    }
    
    return df, data_dict

@pytest.fixture
def db_folder(tmp_path):
    """Create a temporary folder for testing."""
    folder = tmp_path / "test_db"
    folder.mkdir()
    return str(folder)

@pytest.fixture
def db(db_folder):
    """Create a FolderDB instance for testing."""
    return FolderDB(db_folder)

def test_init(test_folder):
    """Test FolderDB initialization"""
    # Test with existing folder
    db = FolderDB(test_folder)
    assert db.folder_path == test_folder
    
    # Test with non-existent folder
    nonexistent_path = os.path.join(os.path.dirname(test_folder), "nonexistent_folder")
    if os.path.exists(nonexistent_path):
        os.rmdir(nonexistent_path)
    with pytest.raises(FileNotFoundError):
        FolderDB(nonexistent_path)

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
    db.delete_file_keys("test1", ["key1", "key2"])
    
    # Verify deletion
    result = db.get_dict(["test1"])
    assert len(result["test1"]) == 1
    assert "key3" in result["test1"]
    assert "key1" not in result["test1"]
    assert "key2" not in result["test1"]

def test__range(test_folder, sample_dict):
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
    assert "test1:" in str_rep  # Check for name without extension

def test_build_dbmeta(db, sample_data):
    """Test building the db.meta file."""
    df, data_dict = sample_data
    
    # Add some data to the database
    db.upsert_df("users", df)
    db.upsert_dict("products", data_dict)
    
    # Build metadata
    db.build_dbmeta()
    
    # Check if db.meta exists
    meta_file = os.path.join(db.folder_path, "db.meta")
    assert os.path.exists(meta_file)
    
    # Read and verify metadata
    metadata = select_jsonl(meta_file)
    
    # Verify metadata structure
    assert "users" in metadata
    assert "products" in metadata
    assert metadata["users"]["count"] == 3
    assert metadata["products"]["count"] == 2

def test_update_dbmeta(db, sample_data):
    """Test updating metadata for a specific file."""
    df, _ = sample_data
    
    # Add data and build initial metadata
    db.upsert_df("users", df)
    db.build_dbmeta()
    
    # Update metadata for users
    db.update_dbmeta("users", linted=True)
    
    # Verify the update
    meta_file = os.path.join(db.folder_path, "db.meta")
    metadata = select_jsonl(meta_file)
    users_meta = metadata["users"]
    assert users_meta["linted"] is True

def test_lint_db(db, sample_data):
    """Test linting database files and updating metadata."""
    df, data_dict = sample_data
    
    # Add data and build initial metadata
    db.upsert_df("users", df)
    db.upsert_dict("products", data_dict)
    db.build_dbmeta()
    
    # Lint the database
    db.lint_db()
    
    # Read and verify metadata
    meta_file = os.path.join(db.folder_path, "db.meta")
    metadata = select_jsonl(meta_file)
    
    # Verify both files are marked as linted
    assert metadata["users"]["linted"] is True
    assert metadata["products"]["linted"] is True

def test_metadata_persistence(db, sample_data):
    """Test that metadata persists between operations."""
    df, data_dict = sample_data
    
    # Initial setup
    db.upsert_df("users", df)
    db.upsert_dict("products", data_dict)
    db.build_dbmeta()
    
    # Create a new FolderDB instance
    new_db = FolderDB(db.folder_path)
    
    # Verify metadata is still accessible
    meta_file = os.path.join(db.folder_path, "db.meta")
    assert os.path.exists(meta_file)
    
    metadata = select_jsonl(meta_file)
    assert "users" in metadata
    assert "products" in metadata
    assert metadata["users"]["count"] == 3
    assert metadata["products"]["count"] == 2

def test_enable_hierarchy_mode(db):
    """Test enabling hierarchy mode and its settings."""
    # Test default settings
    db.enable_hierarchy_mode()
    assert db.use_hierarchy is True
    assert db.delimiter == '.'
    assert db.hierarchy_depth == 3

    # Test custom settings with force_build
    db.enable_hierarchy_mode(delimiter='/', hierarchy_depth=2, force_build=True)
    assert db.delimiter == '/'
    assert db.hierarchy_depth == 2

def test_hierarchical_file_operations(db, sample_data):
    """Test file operations in hierarchical mode."""
    df, data_dict = sample_data
    db.enable_hierarchy_mode(hierarchy_depth=3)

    # Test creating files in hierarchy
    db.upsert_df("region.north.users", df)  # 2 delimiters for depth=3
    db.upsert_dict("region.south.products", data_dict)

    # Verify files were created in correct folders
    assert os.path.exists(os.path.join(db.folder_path, "region", "north", "users", "region.north.users.jsonl"))
    assert os.path.exists(os.path.join(db.folder_path, "region", "south", "products", "region.south.products.jsonl"))

def test_hierarchical_file_deletion(db, sample_data):
    """Test file deletion in hierarchical mode."""
    df, _ = sample_data
    db.enable_hierarchy_mode(hierarchy_depth=3)

    # Create hierarchical data
    db.upsert_df("org.hr.employees", df)

    # Delete file
    db.delete_file("org.hr.employees")

    # Verify file and its index are deleted
    file_path = os.path.join(db.folder_path, "org", "hr", "employees", "org.hr.employees.jsonl")
    idx_path = os.path.join(db.folder_path, "org", "hr", "employees", "org.hr.employees.jsonl.idx")
    assert not os.path.exists(file_path)
    assert not os.path.exists(idx_path)

    # Verify empty folders are cleaned up
    assert not os.path.exists(os.path.join(db.folder_path, "org", "hr", "employees"))

def test_hierarchical_validation(db):
    """Test validation of hierarchical paths."""
    db.enable_hierarchy_mode(hierarchy_depth=3)

    # Test invalid path (not enough delimiters)
    with pytest.raises(ValueError, match=r"Name must contain exactly \d+ '.' delimiters"):
        db.upsert_df("invalid_no_hierarchy", pd.DataFrame())

    # Test invalid path (too few delimiters)
    with pytest.raises(ValueError, match=r"Name must contain exactly \d+ '.' delimiters"):
        db.upsert_df("only.one", pd.DataFrame())

    # Test valid path
    db.upsert_df("this.is.valid", pd.DataFrame())

def test_hierarchical_search(db, sample_data):
    """Test searching files in hierarchical mode."""
    df, data_dict = sample_data
    db.enable_hierarchy_mode()
    
    # Create test data in different hierarchies
    db.upsert_df("us.west.users", df)
    db.upsert_df("us.east.users", df)
    db.upsert_dict("eu.west.products", data_dict)
    
    # Test searching with regex
    us_files = db.search_file_list(r"^us\.")
    assert len(us_files) == 2
    assert "us.west.users" in us_files
    assert "us.east.users" in us_files
    
    west_files = db.search_file_list(r"\.west\.")
    assert len(west_files) == 2
    assert "us.west.users" in west_files
    assert "eu.west.products" in west_files 