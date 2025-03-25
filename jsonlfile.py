import json
import os
from typing import Dict, Tuple, List
import mmap
from pathlib import Path
import jsonlines
import datetime

# Global configuration
BUFFER_SIZE = 1024 * 1024 * 10  # 10MB buffer

# --------------------------------------------------------
# Indexer Function
# --------------------------------------------------------

def build_jsonl_index(jsonl_file_path: str) -> None:
    """
    Build an index file for the JSONL file that maps linekeys to byte locations.
    Skips empty or whitespace-only lines.
    The index file will have the same name as the JSONL file but with .idx extension.
    """
    index_file_path = f"{jsonl_file_path}.idx"
    index_dict = {}

    # Check if file is empty
    if os.path.getsize(jsonl_file_path) == 0:
        # Save empty index for empty file
        with open(index_file_path, 'w', encoding='utf-8') as f:
            json.dump(index_dict, f, indent=2)
        return

    with open(jsonl_file_path, 'rb', buffering=BUFFER_SIZE) as f:  # Use global buffer size
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            current_pos = 0
            while True:
                line = mm.readline()
                if not line:
                    break

                line_str = line.decode('utf-8').strip()
                if not line_str:  # Skip empty or whitespace-only lines
                    current_pos = mm.tell()
                    continue

                try:
                    data = json.loads(line_str)
                    linekey = next(iter(data))
                    index_dict[linekey] = current_pos
                except (json.JSONDecodeError, StopIteration):
                    # Malformed line or no keys, skip indexing
                    pass

                current_pos = mm.tell()

    # Sort the linekeys before writing the index to file
    index_dict = dict(sorted(index_dict.items()))

    # Save the index to file
    with open(index_file_path, 'w', encoding='utf-8') as f:
        json.dump(index_dict, f, indent=2)


def ensure_index_exists(jsonl_file_path: str) -> None:
    """
    Check if the .idx file exists for the given JSONL file, and create it if it doesn't.
    """
    index_file_path = f"{jsonl_file_path}.idx"
    if not os.path.exists(index_file_path):
        build_jsonl_index(jsonl_file_path)


def lint_jsonl(jsonl_file_path):
    """
    Loads a JSONL file, sorts by linekey, removes extra whitespace (compacts),
    and rewrites the file using optimized save_jsonl.
    """
    # Load all valid records using load_jsonl
    records = load_jsonl(jsonl_file_path)
    
    # Sort records by linekey
    sorted_records = dict(sorted(records.items(), key=lambda item: str(item[0])))
    
    # Save sorted records using optimized save_jsonl
    save_jsonl(jsonl_file_path, sorted_records)

# --------------------------------------------------------
# Utility Functions
# --------------------------------------------------------

def serialize_linekey(linekey):
    """
    Convert a linekey to a string.
    If the linekey is already a string, return it.
    If the linekey is a datetime object, serialize it to an ISO format string.
    If the linekey is another object, serialize it using the default function.
    """
    if isinstance(linekey, str):
        return linekey
    elif isinstance(linekey, datetime.datetime):
        return linekey.isoformat()
    else:
        return str(linekey)
    
def deserialize_linekey(linekey_str,default_format=None):
    """
    Deserialize a linekey string back to its original object.
    If the linekey is already a string, return it.
    If the linekey is an ISO format string, deserialize it to a datetime object.    
    If the linekey is another object, deserialize it using the default function.
    """
    
    if default_format == "datetime":
        return datetime.datetime.fromisoformat(linekey_str)
    
    return linekey_str



# --------------------------------------------------------
# Core Functions
# --------------------------------------------------------
def save_jsonl(jsonl_file_path: str, db_dict: Dict[str, Dict]) -> None:
    """
    Save a dictionary to a JSONL file efficiently using buffering.
    Each line will be a JSON object with a serialized linekey and its associated data.
    Also tracks and saves an index mapping linekeys to byte offsets without flushing buffer.
    """
    index = {}

    # Handle empty dictionary case
    if not db_dict:
        with open(jsonl_file_path, 'wb') as jsonl_file:
            pass  # create empty file
        with open(f"{jsonl_file_path}.idx", 'w', encoding='utf-8') as idx_file:
            json.dump({}, idx_file, indent=2)
        return

    byte_offset = 0  # Manually track byte offsets

    with open(jsonl_file_path, 'wb', buffering=BUFFER_SIZE) as jsonl_file:  # Use global buffer size
        for linekey, data in db_dict.items():
            serialized_key = serialize_linekey(linekey)
            line_dict = {serialized_key: data}

            # Serialize line to JSON string with minimal whitespace
            line_str = json.dumps(line_dict, separators=(',', ':')) + '\n'

            # Encode line to utf-8 bytes
            line_bytes = line_str.encode('utf-8')

            # Record byte offset (without flushing)
            index[serialized_key] = byte_offset

            # Write bytes directly
            jsonl_file.write(line_bytes)

            # Increment byte_offset manually
            byte_offset += len(line_bytes)

    # After writing all lines, flush and write index
    with open(f"{jsonl_file_path}.idx", 'w', encoding='utf-8') as idx_file:
        json.dump(index, idx_file, indent=2)

        
def load_jsonl(jsonl_file_path: str, auto_deserialize: bool = True) -> Dict[str, Dict]:
    """
    Load a JSONL file into a dictionary.
    Each line must be a valid JSON object with a single key (linekey) mapping to a dictionary.
    Skips any invalid lines silently.
    Raises FileNotFoundError if the file doesn't exist.
    
    Args:
        jsonl_file_path (str): Path to the JSONL file
        auto_deserialize (bool, optional): Whether to automatically deserialize datetime keys. Defaults to True.
    """
    if not os.path.exists(jsonl_file_path):
        raise FileNotFoundError(f"The file {jsonl_file_path} does not exist.")

    result_dict = {}
    with open(jsonl_file_path, 'r', encoding='utf-8', buffering=BUFFER_SIZE) as f:  # Use global buffer size
        for line in f:
            # Strip both leading and trailing whitespace
            line = line.strip()
            
            # Skip empty or whitespace-only lines
            if not line:
                continue
                
            try:
                # Try to parse the JSON
                data = json.loads(line)
                
                # Verify the line is a dictionary with a single key
                if isinstance(data, dict) and len(data) == 1:
                    linekey = next(iter(data))
                    if isinstance(data[linekey], dict):  # Verify value is a dictionary
                        if auto_deserialize:
                            if 'T' in linekey and len(linekey) == 19:  # ISO format datetime string
                                try:
                                    actual_key = deserialize_linekey(linekey, default_format="datetime")
                                    result_dict[actual_key] = data[linekey]
                                except ValueError:
                                    result_dict[linekey] = data[linekey]
                            else:
                                result_dict[linekey] = data[linekey]
                        else:
                            result_dict[linekey] = data[linekey]
            except (json.JSONDecodeError, StopIteration):
                print(f"ERROR: Invalid JSON line at line  {line}")
    return result_dict

def select_jsonl(jsonl_file_path: str, linekey_range: Tuple[str, str], auto_deserialize: bool = True) -> Dict[str, Dict]:
    """
    Select lines from JSONL file where linekey is between linekey_lower and linekey_upper.
    
    Args:
        jsonl_file_path (str): Path to the JSONL file
        linekey_range (Tuple[str, str]): Tuple of (lower_key, upper_key) for range selection
        auto_deserialize (bool, optional): Whether to automatically deserialize datetime keys. Defaults to True.
    """
    linekey_lower, linekey_upper = linekey_range
    linekey_lower = serialize_linekey(linekey_lower)
    linekey_upper = serialize_linekey(linekey_upper)
    result_dict = {}
    
    # Load the index
    index_file_path = jsonl_file_path + '.idx'

    ensure_index_exists(jsonl_file_path)

    with open(index_file_path, 'r') as index_file:
        index_dict = json.load(index_file)
    
    # Get the linekeys within the range (index_dict is already sorted)
    selected_linekeys = [k for k in index_dict if linekey_lower <= k <= linekey_upper]
    
    with open(jsonl_file_path, 'r') as f:
        for linekey in selected_linekeys:
            try:
                f.seek(index_dict[linekey])
                line = f.readline()
                data = json.loads(line.strip())
                # Check if the linekey is a datetime string and auto_deserialize is enabled
                if auto_deserialize and 'T' in linekey and len(linekey) == 19:  # ISO format datetime string
                    try:
                        actual_key = deserialize_linekey(linekey, default_format="datetime")
                        result_dict[actual_key] = data[linekey]
                    except ValueError:
                        result_dict[linekey] = data[linekey]
                else:
                    result_dict[linekey] = data[linekey]
            except (json.JSONDecodeError, StopIteration):
                continue
    return result_dict

def update_jsonl(jsonl_file_path, update_dict):
    """
    Efficiently updates existing linekeys or inserts new ones using the index file.
    """
    index_file_path = f"{jsonl_file_path}.idx"

    ensure_index_exists(jsonl_file_path)

    # Load the existing index
    with open(index_file_path, 'r', encoding='utf-8') as idx_f:
        index = json.load(idx_f)

    # Open jsonl file in read+write binary mode
    with open(jsonl_file_path, 'rb+') as f:
        for linekey, data in update_dict.items():
            linekey = serialize_linekey(linekey)
            new_line_dict = {linekey: data}
            new_line_bytes = (json.dumps(new_line_dict) + '\n').encode('utf-8')
            new_line_len = len(new_line_bytes)

            if linekey in index:
                f.seek(index[linekey])
                original_line_bytes = f.readline()
                original_line_len = len(original_line_bytes)

                if new_line_len <= original_line_len:
                    # Overwrite in place and pad with spaces
                    f.seek(index[linekey])
                    padding = b' ' * (original_line_len - new_line_len)
                    f.write(new_line_bytes + padding)
                else:
                    # Mark the old record as deleted (spaces)
                    f.seek(index[linekey])
                    f.write(b' ' * original_line_len)
                    # Append the new record at the end
                    f.seek(0, os.SEEK_END)
                    new_pos = f.tell()
                    f.write(new_line_bytes)
                    # Update index to new position
                    index[linekey] = new_pos
            else:
                # Append new line at the end
                f.seek(0, os.SEEK_END)
                new_pos = f.tell()
                f.write(new_line_bytes)
                # Update index to new position
                index[linekey] = new_pos

    # Sort index by linekey before writing
    index = dict(sorted(index.items()))
    # Save updated index back to disk
    with open(index_file_path, 'w', encoding='utf-8') as idx_f:
        json.dump(index, idx_f, indent=2)

def delete_jsonl(jsonl_file_path: str, linekeys: List[str]) -> None:
    """
    Efficiently deletes lines corresponding to linekeys from JSONL file using the index file.
    Marks deleted lines with spaces and updates the index file accordingly.
    """
    index_file_path = f"{jsonl_file_path}.idx"
    ensure_index_exists(jsonl_file_path)

    # Load index
    with open(index_file_path, 'r', encoding='utf-8') as idx_f:
        index = json.load(idx_f)

    # Convert linekeys to their serialized form
    linekeys = [serialize_linekey(key) for key in linekeys]
    with open(jsonl_file_path, 'rb+', buffering=BUFFER_SIZE) as f:
        for linekey in linekeys:
            if linekey in index:
                pos = index[linekey]
                f.seek(pos)
                original_line = f.readline()
                
                # Ensure we preserve the line ending
                if not original_line.endswith(b'\n'):
                    original_line += b'\n'
                
                # Mark the line as deleted by overwriting with spaces
                # Keep the newline at the end
                f.seek(pos)
                f.write(b' ' * (len(original_line) - 1) + b'\n')

                # Remove the key from the index
                del index[linekey]

    # Sort index by linekey before writing
    index = dict(sorted(index.items()))
    
    # Save updated index back to disk
    with open(index_file_path, 'w', encoding='utf-8') as idx_f:
        json.dump(index, idx_f, indent=2)