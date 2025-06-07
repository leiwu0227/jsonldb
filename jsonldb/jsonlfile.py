"""
Core JSONL file operations for JSONLDB.
"""

import os
import json
import pandas as pd
from typing import Dict, List, Optional, Union, Any
import datetime as dt
import orjson
import numpy as np
from numba import jit
import mmap

# --------------------------------------------------------
# Configuration
# --------------------------------------------------------

# Buffer size for file operations (10MB)
BUFFER_SIZE: int = 1024 * 1024 * 50

# JSON serialization options
JSON_OPTS: Dict[str, bool] = {
    'separators': (',', ':'),  # Remove whitespace
    'ensure_ascii': False,     # Faster for non-ASCII
}

# Type aliases for better readability
LineKey = Union[str, dt.datetime]
DataDict = Dict[str, dict]
IndexDict = Dict[str, int]

# --------------------------------------------------------
# Numba optimized functions
# --------------------------------------------------------

@jit(nopython=True)
def _sort_numeric_keys(keys: np.ndarray) -> np.ndarray:
    """Sort array of numeric values using Numba.
    
    Args:
        keys (np.ndarray): Array of numeric values to sort
    
    Returns:
        np.ndarray: Array of indices that would sort the array
    """
    return np.argsort(keys)

def _convert_key_to_sortable(key: Union[str, dt.datetime, float]) -> float:
    """Convert a key to a sortable numeric value.
    
    Args:
        key: String, datetime, or numeric key
        
    Returns:
        float: Numeric value for sorting
    """
    if isinstance(key, dt.datetime):
        return key.timestamp()
    elif isinstance(key, str):
        # Convert string to numeric value for sorting
        # Use UTF-8 bytes for consistent ordering
        return float(sum(b * 256**i for i, b in enumerate(key.encode('utf-8'))))
    return float(key)  # fallback for numeric keys

def _fast_sort_records(records: Dict[str, Any]) -> Dict[str, Any]:
    """Sort records by converting keys to sortable numeric values.
    
    Args:
        records (dict): Dictionary with string or datetime keys
        
    Returns:
        dict: Sorted dictionary
    """
    if not records:
        return records
        
    # Convert keys to sortable numeric values
    keys = list(records.keys())
    numeric_keys = np.array([_convert_key_to_sortable(k) for k in keys], dtype=np.float64)
    
    # Get sorted indices using Numba
    sorted_indices = _sort_numeric_keys(numeric_keys)
    
    # Create new sorted dictionary
    sorted_dict = {}
    for idx in sorted_indices:
        key = keys[idx]
        sorted_dict[key] = records[key]
    
    return sorted_dict

@jit(nopython=True)
def _select_keys_in_range(keys: np.ndarray, lower: str, upper: str) -> np.ndarray:
    """
    Numba-optimized function to select keys within a range.
    
    Args:
        keys: NumPy array of string keys
        lower: Lower bound key (inclusive)
        upper: Upper bound key (inclusive)
        
    Returns:
        Boolean mask array indicating which keys are in range
    """
    mask = np.zeros(len(keys), dtype=np.bool_)
    for i in range(len(keys)):
        mask[i] = lower <= keys[i] <= upper
    return mask

# --------------------------------------------------------
# Indexing Functions
# --------------------------------------------------------

def build_jsonl_index(jsonl_file_path: str) -> None:
    """
    Build an index file mapping linekeys to byte locations.
    
    Creates a .idx file containing a JSON object mapping each linekey
    to its byte offset in the JSONL file. The index is sorted by linekey.
    
    Args:
        jsonl_file_path: Path to the JSONL file to index
        
    Raises:
        FileNotFoundError: If the JSONL file doesn't exist
        OSError: If there are permission issues
    """
    index_file_path = f"{jsonl_file_path}.idx"
    index_dict: IndexDict = {}

    if not os.path.exists(jsonl_file_path):
        raise FileNotFoundError(f"JSONL file not found: {jsonl_file_path}")

    # Handle empty file case
    if os.path.getsize(jsonl_file_path) == 0:
        with open(index_file_path, 'w', encoding='utf-8') as f:
            json.dump(index_dict, f, indent=2)
        return

    try:
        with open(jsonl_file_path, 'rb', buffering=BUFFER_SIZE) as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                current_pos = 0
                while True:
                    line = mm.readline()
                    if not line:
                        break

                    line_str = line.decode('utf-8').strip()
                    if not line_str:  # Skip empty lines
                        current_pos = mm.tell()
                        continue

                    try:
                        data = json.loads(line_str)
                        linekey = next(iter(data))
                        index_dict[linekey] = current_pos
                    except (json.JSONDecodeError, StopIteration):
                        pass  # Skip invalid lines

                    current_pos = mm.tell()

        # Sort and save index
        index_dict = dict(sorted(index_dict.items()))
        with open(index_file_path, 'w', encoding='utf-8') as f:
            json.dump(index_dict, f, indent=2)
            
    except OSError as e:
        raise OSError(f"Failed to build index for {jsonl_file_path}: {str(e)}")

def ensure_index_exists(jsonl_file_path: str) -> None:
    """
    Ensure an index file exists for the given JSONL file.
    
    Creates the index if it doesn't exist or if the JSONL file is newer
    than the index file.
    
    Args:
        jsonl_file_path: Path to the JSONL file
    """
    index_file_path = f"{jsonl_file_path}.idx"
    
    should_rebuild = False
    if not os.path.exists(index_file_path):
        should_rebuild = True
    else:
        # Rebuild if JSONL file is newer than index
        jsonl_mtime = os.path.getmtime(jsonl_file_path)
        index_mtime = os.path.getmtime(index_file_path)
        if jsonl_mtime > index_mtime:
            should_rebuild = True
            
    if should_rebuild:
        build_jsonl_index(jsonl_file_path)

def lint_jsonl(jsonl_file_path: str) -> None:
    """
    Clean and optimize a JSONL file.
    
    - Loads all valid records
    - Sorts by linekey using Numba
    - Removes whitespace
    - Rewrites file in optimized format
    - Rebuilds index
    
    Args:
        jsonl_file_path: Path to the JSONL file to optimize
    """
    # Load all records
    records = load_jsonl(jsonl_file_path)
    
    # Sort records using Numba-optimized sorting
    sorted_records = _fast_sort_records(records)
    
    # Save sorted records
    save_jsonl(jsonl_file_path, sorted_records)

    ensure_index_exists(jsonl_file_path)

# --------------------------------------------------------
# Utility Functions
# --------------------------------------------------------

def serialize_linekey(linekey: LineKey) -> str:
    """
    Convert a linekey to its string representation.
    
    Args:
        linekey: String or datetime object to serialize
        
    Returns:
        String representation of the linekey
    """
    if isinstance(linekey, str):
        return linekey
    elif isinstance(linekey, dt.datetime):
        return linekey.isoformat(timespec='seconds')
    return str(linekey)

def deserialize_linekey(linekey_str: str, default_format: Optional[str] = None) -> LineKey:
    """
    Convert a string linekey back to its original type.
    
    Args:
        linekey_str: String to deserialize
        default_format: Format hint for deserialization ('datetime' supported)
        
    Returns:
        Original type of the linekey (datetime or string)
    """
    if default_format == "datetime":
        return dt.datetime.fromisoformat(linekey_str)
    return linekey_str

def _fast_dumps(obj: dict) -> str:
    """
    Fast JSON serialization using orjson if available.
    
    Args:
        obj: Dictionary to serialize
        
    Returns:
        JSON string with newline
    """
    try:
        return orjson.dumps(obj,option=orjson.OPT_SERIALIZE_NUMPY).decode('utf-8') + '\n'
    except ImportError:
        return json.dumps(obj, **JSON_OPTS) + '\n'

def check_dict_format(data_dict: Dict[Any, Any]) -> bool:
    """Check if a dictionary follows the required JSONL format.
    
    Required format:
    {
        "linekey1": {"value1": 10, "value2": 11},
        "linekey2": {"value1": 11, "value2": 12}
    }
    
    Args:
        data_dict (Dict[str, Any]): Dictionary to check
        
    Returns:
        bool: True if format is valid, False otherwise
        
    Raises:
        ValueError: If format is invalid, with specific error message
    """
    if not isinstance(data_dict, dict):
        raise ValueError("Input must be a dictionary")
        
    # if not data_dict:
    #     raise ValueError("Dictionary cannot be empty")
        
    # Check each key-value pair
    for linekey, value in data_dict.items():
        # Check if linekey is a string
        # No need to check if linekey is a string
            
        # Check if value is a dictionary
        if not isinstance(value, dict):
            raise ValueError(f"Value for linekey '{linekey}' must be a dictionary")
            
        # Check if value dictionary is not empty
        if not value:
            raise ValueError(f"Value dictionary for linekey '{linekey}' cannot be empty")
            
    # Check for duplicate linekeys
    linekeys = list(data_dict.keys())
    if len(linekeys) != len(set(linekeys)):
        raise ValueError("Duplicate linekeys found")
        
    return True

# --------------------------------------------------------
# Core CRUD Functions
# --------------------------------------------------------

def save_jsonl(jsonl_file_path: str, db_dict: DataDict) -> None:
    """
    Save a dictionary to a JSONL file with automatic indexing.
    
    Efficiently writes records and maintains an index of byte positions.
    Handles empty dictionaries and ensures atomic writes.
    
    Args:
        jsonl_file_path: Path to save the JSONL file
        db_dict: Dictionary of records to save
        
    Raises:
        OSError: If file operations fail
    """
    index: IndexDict = {}
    
    try:
        # Handle empty dictionary case
        if not db_dict:
            with open(jsonl_file_path, 'wb') as f:
                pass  # create empty file
            with open(f"{jsonl_file_path}.idx", 'w', encoding='utf-8') as f:
                json.dump({}, f, indent=2)
            return

        byte_offset = 0
        lines = []
        
        # Pre-process all lines
        for linekey, data in db_dict.items():
            serialized_key = serialize_linekey(linekey)
            line_dict = {serialized_key: data}
            line = _fast_dumps(line_dict).encode('utf-8')
            lines.append(line)
            index[serialized_key] = byte_offset
            byte_offset += len(line)

        # Write all lines at once
        with open(jsonl_file_path, 'wb', buffering=BUFFER_SIZE) as f:
            for line in lines:
                f.write(line)

        # Write index atomically
        with open(f"{jsonl_file_path}.idx", 'w', encoding='utf-8') as f:
            json.dump(dict(sorted(index.items())), f, indent=2)
            
    except OSError as e:
        raise OSError(f"Failed to save JSONL file {jsonl_file_path}: {str(e)}")

def load_jsonl(jsonl_file_path: str, auto_deserialize: bool = True) -> DataDict:
    """
    Load a JSONL file into a dictionary.
    
    Reads each line as a JSON object and builds a dictionary.
    Handles datetime deserialization and skips invalid lines.
    
    Args:
        jsonl_file_path: Path to the JSONL file to load
        auto_deserialize: Whether to convert datetime strings back to datetime objects
        
    Returns:
        Dictionary of loaded records
        
    Raises:
        FileNotFoundError: If the file doesn't exist
        OSError: If file operations fail
    """
    if not os.path.exists(jsonl_file_path):
        raise FileNotFoundError(f"JSONL file not found: {jsonl_file_path}")

    result_dict: DataDict = {}
    
    try:
        with open(jsonl_file_path, 'r', encoding='utf-8', buffering=BUFFER_SIZE) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                    
                try:
                    data = json.loads(line)
                    if isinstance(data, dict) and len(data) == 1:
                        linekey = next(iter(data))
                        # if isinstance(data[linekey], dict): #TODO: is this really needed?
                        if auto_deserialize and 'T' in linekey and len(linekey) == 19:
                            try:
                                actual_key = deserialize_linekey(linekey, "datetime")
                                result_dict[actual_key] = data[linekey]
                            except ValueError:
                                result_dict[linekey] = data[linekey]
                        else:
                            result_dict[linekey] = data[linekey]
                except json.JSONDecodeError:
                    print("WARNING: invalid JSON line "+line)
                    continue  # Skip invalid JSON lines
                    
        return result_dict
        
    except OSError as e:
        raise OSError(f"Failed to load JSONL file {jsonl_file_path}: {str(e)}")

def select_jsonl(jsonl_file_path: str, lower_key: Optional[LineKey] = None, upper_key: Optional[LineKey] = None, auto_deserialize: bool = True) -> DataDict:
    """
    Select records from a JSONL file within a key range.
    
    Args:
        jsonl_file_path: Path to the JSONL file
        lower_key: Lower bound key (inclusive). If None, uses smallest key.
        upper_key: Upper bound key (inclusive). If None, uses largest key.
        auto_deserialize: Whether to auto-deserialize datetime keys
        
    Returns:
        Dictionary of records within the range
        
    Raises:
        FileNotFoundError: If file or index doesn't exist
        OSError: If file operations fail
    """
    # If both keys are None, return all records
    if lower_key is None and upper_key is None:
        return load_jsonl(jsonl_file_path, auto_deserialize)

    if lower_key == upper_key:
        return select_line_jsonl(jsonl_file_path, lower_key, auto_deserialize)

    ensure_index_exists(jsonl_file_path)

    
    try:
        # Load index
        with open(f"{jsonl_file_path}.idx", 'r') as f:
            index_dict = json.load(f)
            
        # If no keys in index, return empty dict
        if not index_dict:
            return {}
            
        # Get all keys from index
        all_keys = list(index_dict.keys())
        
        # Set default values if None
        if lower_key is None:
            lower_key = min(all_keys)
        if upper_key is None:
            upper_key = max(all_keys)
            
        # Serialize the keys
        lower_key = serialize_linekey(lower_key)
        upper_key = serialize_linekey(upper_key)
        
        result_dict: DataDict = {}
        
        # Get keys in range using Numba-optimized function
        keys = np.array(all_keys)
        mask = _select_keys_in_range(keys, lower_key, upper_key)
        selected_linekeys = keys[mask]
        # print(f"Selected {len(selected_linekeys)} keys from {len(keys)} keys")
        # print(f"First key: {selected_linekeys[0]}")
        # print(f"Last key: {selected_linekeys[-1]}")

        # Load selected records
        with open(jsonl_file_path, 'r', encoding='utf-8', buffering=BUFFER_SIZE) as f:
            for linekey in selected_linekeys:
           
                f.seek(index_dict[linekey])
                line = f.readline().strip()
                # print(f"Line: {line}")
                data = json.loads(line)
                
                if auto_deserialize and 'T' in linekey and len(linekey) == 19:
                    try:
                        actual_key = deserialize_linekey(linekey, "datetime")
                        result_dict[actual_key] = data[linekey]
                    except ValueError:
                        result_dict[linekey] = data[linekey]
                else:
                    result_dict[linekey] = data[linekey]
    
                # print(f"Error loading linekey: {linekey}")
                    
               
                    
        return result_dict
        
    except OSError as e:
        raise OSError(f"Failed to select from JSONL file {jsonl_file_path}: {str(e)}")

def select_line_jsonl(jsonl_file_path: str, linekey: LineKey, auto_serialize: bool = True) -> Optional[str]:
    """
    Get a specific line from a JSONL file based on the linekey.
    
    Args:
        jsonl_file_path: Path to the JSONL file
        linekey: The key to look for
        auto_serialize: Whether to automatically serialize the key
        
    Returns:
        The line as a string if found, None otherwise
    """
    # Serialize the key if needed
    if auto_serialize:
        linekey = serialize_linekey(linekey)
    
    ensure_index_exists(jsonl_file_path)
    
    index_path = jsonl_file_path + '.idx'
    # Read the index file
    with open(index_path, 'r') as f:
        index_dict = json.load(f)
    
    # Check if key exists in index
    if linekey not in index_dict:
        return {}
    
    result_dict: DataDict = {}
        

    # Load selected records
    with open(jsonl_file_path, 'r', encoding='utf-8', buffering=BUFFER_SIZE) as f:
        try:
            f.seek(index_dict[linekey])
            line = f.readline().strip()
            data = json.loads(line)
            
            if auto_serialize and 'T' in linekey and len(linekey) == 19:
                try:
                    actual_key = deserialize_linekey(linekey, "datetime")
                    result_dict[actual_key] = data[linekey]
                except ValueError:
                    result_dict[linekey] = data[linekey]
            else:
                result_dict[linekey] = data[linekey]
        except (json.JSONDecodeError, KeyError):
            return {}
        
    return result_dict



def update_jsonl(jsonl_file_path: str, update_dict: DataDict) -> None:
    """
    Update or insert records in a JSONL file.
    
    Efficiently handles both updates and inserts:
    - Updates in place if new record fits in old space
    - Appends to file if record grows
    - Maintains index automatically
    
    Args:
        jsonl_file_path: Path to the JSONL file
        update_dict: Dictionary of records to update/insert
        
    Raises:
        OSError: If file operations fail
    """
    ensure_index_exists(jsonl_file_path)
    
    try:
        # Load index
        with open(f"{jsonl_file_path}.idx", 'r', encoding='utf-8', buffering=BUFFER_SIZE) as f:
            index = json.load(f)

        updates = []
        appends = []
        
        # Process records
        with open(jsonl_file_path, 'rb+', buffering=BUFFER_SIZE) as f:
            f.seek(0, os.SEEK_END)
            append_pos = f.tell()
            
            for linekey, data in update_dict.items():
                linekey = serialize_linekey(linekey)
                new_line = _fast_dumps({linekey: data}).encode('utf-8')

                if linekey in index:
                    f.seek(index[linekey])
                    old_line = f.readline()
                    
                    if len(new_line) <= len(old_line):
                        updates.append((index[linekey], new_line, len(old_line)))
                    else:
                        updates.append((index[linekey], b' ' * (len(old_line) - 1) + b'\n', len(old_line)))
                        appends.append((linekey, new_line))
                else:
                    appends.append((linekey, new_line))

            # Apply updates
            for pos, line, old_len in updates:
                f.seek(pos)
                f.write(line)
                if len(line) < old_len:
                    f.write(b' ' * (old_len - len(line)))

            # Apply appends
            if appends:
                f.seek(append_pos)
                for linekey, line in appends:
                    index[linekey] = f.tell()
                    f.write(line)

        # Update index
        with open(f"{jsonl_file_path}.idx", 'w', encoding='utf-8', buffering=BUFFER_SIZE) as f:
            json.dump(dict(sorted(index.items())), f, indent=2)
            
    except OSError as e:
        raise OSError(f"Failed to update JSONL file {jsonl_file_path}: {str(e)}")

def delete_jsonl(jsonl_file_path: str, linekeys: List[LineKey]) -> None:
    """
    Delete records from a JSONL file.
    
    Marks deleted lines with spaces and updates the index.
    Maintains file size but removes entries from index.
    
    Args:
        jsonl_file_path: Path to the JSONL file
        linekeys: List of keys to delete
        
    Raises:
        OSError: If file operations fail
    """
    ensure_index_exists(jsonl_file_path)
    
    try:
        # Load index using orjson for faster JSON parsing
        with open(f"{jsonl_file_path}.idx", 'rb') as f:
            index = orjson.loads(f.read())

        # Process deletions
        linekeys = [serialize_linekey(key) for key in linekeys]
        
        # Use regular file operations like update_jsonl
        with open(jsonl_file_path, 'rb+', buffering=BUFFER_SIZE) as f:
            for linekey in linekeys:
                if linekey in index:
                    f.seek(index[linekey])
                    line = f.readline()
                    if not line.endswith(b'\n'):
                        line += b'\n'
                    
                    # Mark as deleted using _fast_dumps for consistency
                    deleted_line = _fast_dumps({linekey: {}}).encode('utf-8')
                    f.seek(index[linekey])
                    f.write(b' ' * (len(line) - 1) + b'\n')
                    del index[linekey]

        # Update index using orjson for faster JSON serialization
        with open(f"{jsonl_file_path}.idx", 'wb') as f:
            f.write(orjson.dumps(dict(sorted(index.items())), option=orjson.OPT_SERIALIZE_NUMPY|orjson.OPT_INDENT_2))
            
    except OSError as e:
        raise OSError(f"Failed to delete from JSONL file {jsonl_file_path}: {str(e)}")


