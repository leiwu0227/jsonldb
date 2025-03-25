import json
import os
from typing import Dict, List, Tuple, Union, Optional
import mmap
from pathlib import Path
import jsonlines

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

    with open(jsonl_file_path, 'rb') as f:
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

def lint_jsonl(jsonl_file_path):
    """
    Loads a JSONL file, sorts by linekey, removes extra whitespace (compacts),
    rewrites the file using jsonlines for efficiency, and rebuilds the index file.
    """
    index_file_path = f"{jsonl_file_path}.idx"

    # Load valid records into memory
    records = {}
    with open(jsonl_file_path, 'rb') as f:
        line = f.readline()
        while line:
            if line.strip():  # Skip deleted (space-only) lines
                line_dict = json.loads(line.decode('utf-8').strip())
                linekey = next(iter(line_dict))
                records[linekey] = line_dict[linekey]
            line = f.readline()

    # Sort records by linekey
    sorted_records = dict(sorted(records.items(), key=lambda item: item[0]))

    # Rewrite the file compactly and sorted with jsonlines library
    index = {}
    with jsonlines.open(jsonl_file_path, mode='w') as writer:
        for linekey, data in sorted_records.items():
            pos = writer._fp.tell()  # Direct access to underlying file pointer
            writer.write({linekey: data})
            index[linekey] = pos

    # Rebuild index file
    with open(index_file_path, 'w', encoding='utf-8') as idx_f:
        json.dump(index, idx_f, indent=2)

# --------------------------------------------------------
# Core Functions
# --------------------------------------------------------

def save_jsonl(jsonl_file_path: str, db_dict: Dict[str, Dict]) -> None:
    """
    Save a dictionary to a JSONL file.
    Each line will be a JSON object with a linekey and its associated data.
    """

    with jsonlines.open(jsonl_file_path, mode='w') as writer:
        for linekey, data in db_dict.items():
            line_dict = {linekey: data}
            writer.write(line_dict)
    
    # Build the index after saving
    build_jsonl_index(jsonl_file_path)

def load_jsonl(jsonl_file_path: str) -> Dict[str, Dict]:
    """
    Load a JSONL file into a dictionary.
    """
    result_dict = {}
    with jsonlines.open(jsonl_file_path, mode='r') as reader:
        for data in reader:
            if not data:  # Skip empty lines
                continue
            try:
                linekey = next(iter(data))
                result_dict[linekey] = data[linekey]
            except StopIteration:
                continue
    return result_dict

def select_jsonl(jsonl_file_path: str, linekey_range: Tuple[str, str]) -> Dict[str, Dict]:
    """
    Select lines from JSONL file where linekey is between linekey_lower and linekey_upper.
    """
    linekey_lower, linekey_upper = linekey_range
    result_dict = {}
    
    # Load the index
    index_file_path = jsonl_file_path + '.idx'
    with open(index_file_path, 'r') as index_file:
        index_dict = json.load(index_file)
    
    # Get the linekeys within the range (index_dict is already sorted)
    selected_linekeys = [k for k in index_dict if linekey_lower <= k <= linekey_upper]
    
    with open(jsonl_file_path, 'r') as f:
        for linekey in selected_linekeys:
            try:
                f.seek(index_dict[linekey])
                line = f.readline()
                data = json.loads(line.strip().decode('utf-8'))
                result_dict[linekey] = data[linekey]
            except (json.JSONDecodeError, StopIteration):
                continue
    return result_dict

def update_jsonl(jsonl_file_path, update_dict):
    """
    Efficiently updates existing linekeys or inserts new ones using the index file.
    """
    index_file_path = f"{jsonl_file_path}.idx"

    # Load the existing index
    with open(index_file_path, 'r', encoding='utf-8') as idx_f:
        index = json.load(idx_f)

    # Open jsonl file in read+write binary mode
    with open(jsonl_file_path, 'rb+') as f:
        for linekey, data in update_dict.items():
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

def delete_jsonl(jsonl_file_path, linekeys):
    """
    Efficiently deletes lines corresponding to linekeys from JSONL file using the index file.
    Marks deleted lines with spaces and updates the index file accordingly.
    """
    index_file_path = f"{jsonl_file_path}.idx"

    # Load index
    with open(index_file_path, 'r', encoding='utf-8') as idx_f:
        index = json.load(idx_f)

    with open(jsonl_file_path, 'rb+') as f:
        for linekey in linekeys:
            if linekey in index:
                pos = index[linekey]
                f.seek(pos)
                original_line_bytes = f.readline()
                line_length = len(original_line_bytes)

                # Mark the line as deleted by overwriting it with spaces
                f.seek(pos)
                f.write(b' ' * line_length)

                # Remove the key from the index
                del index[linekey]

    # Save updated index back to disk
    with open(index_file_path, 'w', encoding='utf-8') as idx_f:
        json.dump(index, idx_f, indent=2)