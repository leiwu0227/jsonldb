import unittest
import os
import json
import datetime
import time
from jsonldb import (
    save_jsonl,
    load_jsonl,
    select_jsonl,
    update_jsonl,
    delete_jsonl,
    build_jsonl_index,
    lint_jsonl,
    serialize_linekey,
    deserialize_linekey
)

class TestJsonlDB(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.test_file = "test_data.jsonl"
        self.test_data = {
            "key1": {"value1": 10, "value2": 20},
            "key2": {"value1": 30, "value2": 40},
            "key3": {"value1": 50, "value2": 60}
        }
        # Clean up any existing test files
        self.cleanup_files()

    def tearDown(self):
        """Clean up after each test method."""
        self.cleanup_files()

    def cleanup_files(self):
        """Helper method to remove test files."""
        for file in [self.test_file, f"{self.test_file}.idx"]:
            if os.path.exists(file):
                try:
                    os.remove(file)
                except PermissionError:
                    # If file is locked, wait a bit and try again
                    time.sleep(0.1)
                    try:
                        os.remove(file)
                    except:
                        # If still can't delete, print warning but continue
                        print(f"Warning: Could not delete {file}")

    def print_file_contents(self, msg=""):
        """Helper method to print file contents for debugging."""
        print(f"\n=== File contents {msg} ===")
        try:
            if os.path.exists(self.test_file):
                with open(self.test_file, 'r') as f:
                    print(f.read())
            else:
                print("File does not exist")
        except Exception as e:
            print(f"Error reading file: {e}")
        print("=" * 40)

    def test_serialize_linekey(self):
        """Test linekey serialization."""
        # Test string linekey
        self.assertEqual(serialize_linekey("test"), "test")
        
        # Test datetime linekey
        dt = datetime.datetime(2024, 1, 1, 12, 0)
        self.assertEqual(serialize_linekey(dt), "2024-01-01T12:00:00")
        
        # Test other types
        self.assertEqual(serialize_linekey(123), "123")

    def test_deserialize_linekey(self):
        """Test linekey deserialization."""
        # Test datetime format
        dt_str = "2024-01-01T12:00:00"
        dt = deserialize_linekey(dt_str, default_format="datetime")
        self.assertIsInstance(dt, datetime.datetime)
        self.assertEqual(dt.year, 2024)
        self.assertEqual(dt.hour, 12)

        # Test default string format
        self.assertEqual(deserialize_linekey("test"), "test")

    def test_build_index(self):
        """Test index file creation."""
        # Create a test file
        save_jsonl(self.test_file, self.test_data)
        
        # Check if index file exists
        index_file = f"{self.test_file}.idx"
        self.assertTrue(os.path.exists(index_file))
        
        # Verify index content
        with open(index_file, 'r') as f:
            index = json.load(f)
            self.assertEqual(set(index.keys()), set(self.test_data.keys()))
            self.assertTrue(all(isinstance(pos, int) for pos in index.values()))

    def test_save_and_load(self):
        """Test basic save and load operations."""
        # Test saving
        save_jsonl(self.test_file, self.test_data)
        self.assertTrue(os.path.exists(self.test_file))
        
        # Test loading
        loaded_data = load_jsonl(self.test_file)
        self.assertEqual(loaded_data, self.test_data)
        
        # Test with empty dict
        save_jsonl(self.test_file, {})
        loaded_data = load_jsonl(self.test_file)
        self.assertEqual(loaded_data, {})

    def test_select(self):
        """Test range selection."""
        save_jsonl(self.test_file, self.test_data)
        
        # Test full range
        result = select_jsonl(self.test_file, ("key1", "key3"))
        self.assertEqual(result, self.test_data)
        
        # Test partial range
        result = select_jsonl(self.test_file, ("key1", "key2"))
        expected = {k: self.test_data[k] for k in ["key1", "key2"]}
        self.assertEqual(result, expected)
        
        # Test empty range
        result = select_jsonl(self.test_file, ("key9", "key99"))
        self.assertEqual(result, {})

    def test_update(self):
        """Test update operations."""
        save_jsonl(self.test_file, self.test_data)
        
        # Test updating existing record
        update_data = {"key1": {"value1": 100}}
        update_jsonl(self.test_file, update_data)
        loaded = load_jsonl(self.test_file)
        self.assertEqual(loaded["key1"], update_data["key1"])
        
        # Test inserting new record
        update_data = {"key4": {"value1": 70}}
        update_jsonl(self.test_file, update_data)
        loaded = load_jsonl(self.test_file)
        self.assertEqual(loaded["key4"], update_data["key4"])
        
        # Verify other records unchanged
        self.assertEqual(loaded["key2"], self.test_data["key2"])

    def test_delete(self):
        """Test delete operations."""
        save_jsonl(self.test_file, self.test_data)
        self.print_file_contents("after save")
        
        # Test deleting single record
        delete_jsonl(self.test_file, ["key1"])
        self.print_file_contents("after first delete")
        
        loaded = load_jsonl(self.test_file)
        print("Loaded data after first delete:", loaded)
        self.assertNotIn("key1", loaded)
        self.assertIn("key2", loaded)
        
        # Test deleting multiple records
        delete_jsonl(self.test_file, ["key2", "key3"])
        self.print_file_contents("after second delete")
        
        loaded = load_jsonl(self.test_file)
        print("Loaded data after second delete:", loaded)
        self.assertEqual(loaded, {})
        
        # Test deleting non-existent key
        delete_jsonl(self.test_file, ["nonexistent"])
        self.print_file_contents("after deleting non-existent key")
        
        loaded = load_jsonl(self.test_file)
        print("Final loaded data:", loaded)
        self.assertEqual(loaded, {})

    def test_lint(self):
        """Test linting functionality."""
        # Create unordered file with spaces
        with open(self.test_file, 'w') as f:
            f.write('\n')  # Empty line
            f.write('{"key2": {"value1": 30}}\n')
            f.write('  \n')  # Whitespace line
            f.write('{"key1": {"value1": 10}}\n')
        
        # Lint the file
        lint_jsonl(self.test_file)
        
        # Check if file is properly sorted and cleaned
        loaded = load_jsonl(self.test_file)
        self.assertEqual(list(loaded.keys()), sorted(list(loaded.keys())))

    def test_error_handling(self):
        """Test error handling scenarios."""
        # Test non-existent file
        with self.assertRaises(FileNotFoundError):
            load_jsonl("nonexistent.jsonl")
        
        # Test file with various formatting issues
        with open(self.test_file, 'w') as f:
            f.write('{"key1": {"value1": 10}}\n')          # Valid line
            f.write('invalid json\n')                       # Invalid JSON
            f.write('    {"key2": {"value1": 20}}\n')      # Valid with leading spaces
            f.write('\n')                                   # Empty line
            f.write('  \n')                                # Whitespace line
            f.write('{"key3"}\n')                          # Incomplete JSON
            f.write('["not_an_object"]\n')                 # Valid JSON but wrong format
            f.write('{"key4": "not_a_dict"}\n')            # Valid JSON but value not a dict
            f.write('{"key5": {"value1": 50}}\n')          # Valid line
        
        self.print_file_contents("malformed file test")
        loaded = load_jsonl(self.test_file)
        print("Loaded data from malformed file:", loaded)
        
        # Should only load the valid lines with proper format
        self.assertEqual(len(loaded), 3)  # Only key1, key2, and key5 should be loaded
        self.assertIn("key1", loaded)
        self.assertIn("key2", loaded)
        self.assertIn("key5", loaded)
        self.assertEqual(loaded["key1"], {"value1": 10})
        self.assertEqual(loaded["key2"], {"value1": 20})
        self.assertEqual(loaded["key5"], {"value1": 50})
        
        # Test empty file
        with open(self.test_file, 'w') as f:
            f.write('')
        loaded = load_jsonl(self.test_file)
        self.assertEqual(loaded, {})

    def test_datetime_keys(self):
        """Test using datetime objects as linekeys."""
        dt1 = datetime.datetime(2024, 1, 1)
        dt2 = datetime.datetime(2024, 1, 2)
        
        test_data = {
            dt1: {"value": 1},
            dt2: {"value": 2}
        }
        
        # Save and load
        save_jsonl(self.test_file, test_data)
        loaded = load_jsonl(self.test_file)
        
        # Keys should be serialized to ISO format strings
        self.assertIn(dt1.isoformat(), loaded)
        self.assertIn(dt2.isoformat(), loaded)

if __name__ == '__main__':
    unittest.main() 