"""
Core JSONL file operations for JSONLDB.
"""

import os
import logging
import pandas as pd
from typing import Dict, List, Optional, Union, Any
import datetime as dt
import orjson
from bisect import bisect_left, bisect_right
import mmap


logger = logging.getLogger(__name__)

# --------------------------------------------------------
# Configuration
# --------------------------------------------------------

# Buffer size for file operations (50MB)
BUFFER_SIZE: int = 1024 * 1024 * 50
TIME_SPEC = 'seconds'  #or seconds/microseconds

# Type aliases for better readability
LineKey = Union[str, dt.datetime]
DataDict = Dict[str, dict]
IndexDict = Dict[str, int]

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
        with open(index_file_path, 'wb') as f:
            f.write(orjson.dumps(index_dict, option=orjson.OPT_SORT_KEYS))
        return

    try:
        with open(jsonl_file_path, 'rb', buffering=BUFFER_SIZE) as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                current_pos = 0
                while True:

                    line = mm.readline()
                    if not line:
                        break

                    line = line.strip()
                    if not line:  # Skip empty lines
                        current_pos = mm.tell()
                        continue

                    try:
                        data = orjson.loads(line)
                        linekey = next(iter(data))
                        index_dict[linekey] = current_pos
                    except (orjson.JSONDecodeError, ValueError, StopIteration):
                        logger.warning(
                            "invalid JSON line %s",
                            line.decode('utf-8', errors='replace'),
                        )
                        continue

                    current_pos = mm.tell()

        # Save index (OPT_SORT_KEYS sorts on dump)
        with open(index_file_path, 'wb') as f:
            f.write(orjson.dumps(index_dict, option=orjson.OPT_SORT_KEYS))
            
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
    corrupt = False
    if not os.path.exists(index_file_path):
        should_rebuild = True
    elif os.path.getsize(index_file_path) == 0:
        # An empty .idx (e.g. left by an interrupted write) is corrupt: a valid
        # empty index is b'{}' (2 bytes), never zero-length. Treat it as missing.
        should_rebuild = True
        corrupt = True
    elif os.path.getmtime(jsonl_file_path) > os.path.getmtime(index_file_path):
        # Rebuild if JSONL file is newer than index (stale, but not corrupt)
        should_rebuild = True

    if should_rebuild:
        if corrupt:
            logger.warning("rebuilt empty index %s", index_file_path)
        build_jsonl_index(jsonl_file_path)


def load_index(jsonl_file_path: str) -> dict:
    """Load a JSONL file's index, self-healing if it is missing/empty/corrupt.

    The .idx is fully derived from the .jsonl (the source of truth), so an empty
    or unparseable index is treated exactly like a missing one: rebuilt via
    build_jsonl_index and re-read. This is the single index-read path for the
    library — every other read site routes through here.

    Args:
        jsonl_file_path: Path to the JSONL file

    Returns:
        dict: key -> byte offset index
    """
    index_file_path = f"{jsonl_file_path}.idx"
    ensure_index_exists(jsonl_file_path)  # heals missing / empty / stale
    try:
        with open(index_file_path, 'rb') as f:
            return orjson.loads(f.read())
    except (orjson.JSONDecodeError, OSError):
        # Non-empty but unparseable/unreadable index -> rebuild from the .jsonl
        logger.warning("rebuilt corrupt index %s", index_file_path)
        build_jsonl_index(jsonl_file_path)
        with open(index_file_path, 'rb') as f:
            return orjson.loads(f.read())


def _count_newlines(jsonl_file_path: str) -> int:
    """Count record and tombstone terminators without parsing JSON lines."""
    count = 0
    with open(jsonl_file_path, 'rb', buffering=BUFFER_SIZE) as f:
        while True:
            chunk = f.read(BUFFER_SIZE)
            if not chunk:
                return count
            count += chunk.count(b'\n')


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
        try:
            with open(jsonl_file_path, 'rb', buffering=BUFFER_SIZE) as f:
                for check_key in [keys[0], keys[-1]]:
                    f.seek(index_dict[check_key])
                    line = f.readline()
                    data = orjson.loads(line)
                    parsed_key = next(iter(data))
                    if parsed_key != check_key:
                        raise ValueError("key mismatch")
        except (orjson.JSONDecodeError, ValueError, TypeError, OSError):
            # Spot-check failed on a parseable-but-wrong index (bad offsets/keys):
            # load_index only heals empty/unparseable indexes, so force a rebuild
            # first, then re-read through the single loader.
            build_jsonl_index(jsonl_file_path)
            index_dict = load_index(jsonl_file_path)

    if not index_dict:
        return True

    sorted_keys = sorted(index_dict, key=str)
    offsets = [index_dict[key] for key in sorted_keys]
    offsets_strictly_increase = all(
        offsets[i] < offsets[i + 1] for i in range(len(offsets) - 1)
    )
    newline_count = _count_newlines(jsonl_file_path)

    if offsets_strictly_increase and newline_count == len(index_dict):
        if offsets[0] == 0:
            with open(jsonl_file_path, 'rb', buffering=BUFFER_SIZE) as f:
                f.seek(offsets[-1])
                last_line = f.readline()
                expected_end = offsets[-1] + len(last_line)
                actual_size = os.path.getsize(jsonl_file_path)
                if expected_end == actual_size:
                    return True

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
            index_dict = load_index(jsonl_file_path)
            return _verify_and_compact(jsonl_file_path, index_dict)

    # Full path: mmap line-count scan
    with open(jsonl_file_path, 'rb') as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            non_blank_count = sum(
                1 for line in iter(mm.readline, b'')
                if line.strip()
            )

    index_dict = load_index(jsonl_file_path)

    if non_blank_count != len(index_dict):
        # Index cardinality disagrees with the data (orphan/missing lines):
        # rebuild, then re-read through the single loader.
        build_jsonl_index(jsonl_file_path)
        index_dict = load_index(jsonl_file_path)

    return _verify_and_compact(jsonl_file_path, index_dict)

# --------------------------------------------------------
# Utility Functions
# --------------------------------------------------------

def _is_datetime_string(linekey: str, timespec: Optional[str] = None) -> bool:
    """Check if a string represents a datetime in ISO format.

    Args:
        linekey: String to check
        timespec: Datetime precision ('seconds' or 'microseconds').
            Defaults to the module-level TIME_SPEC.

    Returns:
        bool: True if the string appears to be a datetime in ISO format
    """
    if (timespec or TIME_SPEC) == 'seconds':
        return len(linekey) == 19 and 'T' in linekey and '-' in linekey and ':' in linekey
    else:  # microseconds
        return len(linekey) == 26 and 'T' in linekey and '-' in linekey and ':' in linekey

def serialize_linekey(linekey: LineKey, timespec: Optional[str] = None) -> str:
    """
    Convert a linekey to its string representation.

    Args:
        linekey: String or datetime object to serialize
        timespec: Datetime precision ('seconds' or 'microseconds').
            Defaults to the module-level TIME_SPEC.

    Returns:
        String representation of the linekey
    """
    if isinstance(linekey, str):
        return linekey
    elif isinstance(linekey, dt.datetime):
        return linekey.isoformat(timespec=timespec or TIME_SPEC)
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

def detect_timespec(linekey: str) -> Optional[str]:
    """Detect the datetime precision of a linekey string.

    Example: detect_timespec("2024-01-01T12:00:00.123456") -> 'microseconds'
             detect_timespec("2024-01-01T12:00:00") -> 'seconds'
             detect_timespec("key1") -> None

    Args:
        linekey: String to classify

    Returns:
        'seconds' or 'microseconds' if the string is a parseable ISO datetime
        of that precision, None otherwise
    """
    for spec in ('seconds', 'microseconds'):
        if _is_datetime_string(linekey, spec):
            try:
                dt.datetime.fromisoformat(linekey)
                return spec
            except ValueError:
                return None
    return None

def _store_with_key(result_dict: DataDict, linekey: str, value: dict,
                    auto_deserialize: bool, timespec: Optional[str] = None) -> None:
    """Store value under linekey, converting datetime-looking keys when requested.

    Example: _store_with_key(d, "2024-01-01T00:00:00", {"v": 1}, True)
    stores under datetime(2024, 1, 1); non-datetime keys stay strings.
    """
    if auto_deserialize and _is_datetime_string(linekey, timespec):
        try:
            result_dict[deserialize_linekey(linekey, "datetime")] = value
            return
        except ValueError:
            pass
    result_dict[linekey] = value

def _fast_dumps(obj: dict) -> str:
    """
    Fast JSON serialization using orjson if available.
    
    Args:
        obj: Dictionary to serialize
        
    Returns:
        JSON string with newline
    """
    return orjson.dumps(obj, option=orjson.OPT_SERIALIZE_NUMPY).decode('utf-8') + '\n'

# --------------------------------------------------------
# Core CRUD Functions
# --------------------------------------------------------

def save_jsonl(jsonl_file_path: str, db_dict: DataDict, timespec: Optional[str] = None) -> None:
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
            with open(f"{jsonl_file_path}.idx", 'wb') as f:
                f.write(orjson.dumps({}, option=orjson.OPT_SORT_KEYS))
            return

        # Stream lines to the file while tracking byte offsets
        byte_offset = 0
        with open(jsonl_file_path, 'wb', buffering=BUFFER_SIZE) as f:
            for linekey, data in db_dict.items():
                serialized_key = serialize_linekey(linekey, timespec)
                line = _fast_dumps({serialized_key: data}).encode('utf-8')
                f.write(line)
                index[serialized_key] = byte_offset
                byte_offset += len(line)

        # Write index (OPT_SORT_KEYS sorts on dump)
        with open(f"{jsonl_file_path}.idx", 'wb') as f:
            f.write(orjson.dumps(index, option=orjson.OPT_SORT_KEYS))
            
    except OSError as e:
        raise OSError(f"Failed to save JSONL file {jsonl_file_path}: {str(e)}")

def load_jsonl(jsonl_file_path: str, auto_deserialize: bool = True, timespec: Optional[str] = None) -> DataDict:
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
        with open(jsonl_file_path, 'rb', buffering=BUFFER_SIZE) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    data = orjson.loads(line)
                    if isinstance(data, dict) and len(data) == 1:
                        linekey = next(iter(data))
                        _store_with_key(result_dict, linekey, data[linekey], auto_deserialize, timespec)
                except (orjson.JSONDecodeError, ValueError):
                    logger.warning(
                        "invalid JSON line %s",
                        line.decode('utf-8', errors='replace'),
                    )
                    continue  # Skip invalid JSON lines

        return result_dict

    except OSError as e:
        raise OSError(f"Failed to load JSONL file {jsonl_file_path}: {str(e)}")

def select_jsonl(jsonl_file_path: str, lower_key: Optional[LineKey] = None, upper_key: Optional[LineKey] = None, auto_deserialize: bool = True, timespec: Optional[str] = None) -> DataDict:
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
        return load_jsonl(jsonl_file_path, auto_deserialize, timespec)

    if lower_key == upper_key:
        return select_line_jsonl(jsonl_file_path, lower_key, auto_deserialize, timespec)

    try:
        # Load index (self-heals an empty/corrupt .idx)
        index_dict = load_index(jsonl_file_path)

        # If no keys in index, return empty dict
        if not index_dict:
            return {}
            
        # Get all keys from index
        all_keys = list(index_dict.keys())
        
        # Set default values if None
        if lower_key is None:
            lower_key = all_keys[0]   # index keys are stored sorted
        if upper_key is None:
            upper_key = all_keys[-1]
            
        # Serialize the keys
        lower_key = serialize_linekey(lower_key, timespec)
        upper_key = serialize_linekey(upper_key, timespec)
        
        # Use bisect for O(log n) range selection
        lo = bisect_left(all_keys, lower_key)
        hi = bisect_right(all_keys, upper_key)
        selected_linekeys = all_keys[lo:hi]

        # Read in offset order for sequential I/O
        offset_key_pairs = sorted(
            [(index_dict[k], k) for k in selected_linekeys]
        )
        raw_results = {}
        with open(jsonl_file_path, 'rb', buffering=BUFFER_SIZE) as f:
            for offset, linekey in offset_key_pairs:
                f.seek(offset)
                line = f.readline()
                data = orjson.loads(line)
                raw_results[linekey] = data[linekey]

        # Rebuild in sorted key order with deserialization
        result_dict = {}
        for linekey in selected_linekeys:
            _store_with_key(result_dict, linekey, raw_results[linekey], auto_deserialize, timespec)
        return result_dict
        
    except OSError as e:
        raise OSError(f"Failed to select from JSONL file {jsonl_file_path}: {str(e)}")

def select_line_jsonl(jsonl_file_path: str, linekey: LineKey, auto_serialize: bool = True, timespec: Optional[str] = None) -> DataDict:
    """
    Get a single record from a JSONL file based on the linekey.

    Args:
        jsonl_file_path: Path to the JSONL file
        linekey: The key to look for
        auto_serialize: Whether to serialize the lookup key and deserialize
            datetime-looking keys in the result
        timespec: Datetime precision ('seconds' or 'microseconds').
            Defaults to the module-level TIME_SPEC.

    Returns:
        Single-record dict {linekey: value} if found, {} otherwise.
        Example: {"key1": {"v": 1}}
    """
    # Serialize the key if needed
    if auto_serialize:
        linekey = serialize_linekey(linekey, timespec)
    
    # Read the index file (self-heals an empty/corrupt .idx)
    index_dict = load_index(jsonl_file_path)

    # Check if key exists in index
    if linekey not in index_dict:
        return {}
    
    result_dict: DataDict = {}
        

    # Load selected records
    with open(jsonl_file_path, 'rb', buffering=BUFFER_SIZE) as f:
        try:
            f.seek(index_dict[linekey])
            line = f.readline().strip()
            data = orjson.loads(line)
            _store_with_key(result_dict, linekey, data[linekey], auto_serialize, timespec)
        except (orjson.JSONDecodeError, ValueError, KeyError):
            return {}

    return result_dict



def update_jsonl(jsonl_file_path: str, update_dict: DataDict, timespec: Optional[str] = None) -> None:
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
    try:
        # Load index (self-heals an empty/corrupt .idx)
        index = load_index(jsonl_file_path)

        updates = []
        appends = []
        
        # Process records
        with open(jsonl_file_path, 'rb+', buffering=BUFFER_SIZE) as f:
            f.seek(0, os.SEEK_END)
            # Heal a missing trailing newline so appends start on a fresh line
            if f.tell() > 0:
                f.seek(-1, os.SEEK_END)
                if f.read(1) != b'\n':
                    f.write(b'\n')
            append_pos = f.tell()

            for linekey, data in update_dict.items():
                linekey = serialize_linekey(linekey, timespec)
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
                if len(line) < old_len:
                    # Pad before the newline so the record keeps its exact old
                    # length and the line stays newline-terminated
                    line = line[:-1] + b' ' * (old_len - len(line)) + b'\n'
                f.seek(pos)
                f.write(line)

            # Apply appends
            if appends:
                f.seek(append_pos)
                for linekey, line in appends:
                    index[linekey] = f.tell()
                    f.write(line)

        # Update index
        with open(f"{jsonl_file_path}.idx", 'wb') as f:
            f.write(orjson.dumps(index, option=orjson.OPT_SORT_KEYS))
            
    except OSError as e:
        raise OSError(f"Failed to update JSONL file {jsonl_file_path}: {str(e)}")

def delete_jsonl(jsonl_file_path: str, linekeys: List[LineKey], timespec: Optional[str] = None) -> None:
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
    try:
        # Load index (self-heals an empty/corrupt .idx)
        index = load_index(jsonl_file_path)

        # Process deletions
        linekeys = [serialize_linekey(key, timespec) for key in linekeys]
        
        # Use regular file operations like update_jsonl
        with open(jsonl_file_path, 'rb+', buffering=BUFFER_SIZE) as f:
            for linekey in linekeys:
                if linekey in index:
                    f.seek(index[linekey])
                    line = f.readline()
                    if not line.endswith(b'\n'):
                        line += b'\n'
                    
                    f.seek(index[linekey])
                    f.write(b' ' * (len(line) - 1) + b'\n')
                    del index[linekey]

        # Update index using orjson for faster JSON serialization
        with open(f"{jsonl_file_path}.idx", 'wb') as f:
            f.write(orjson.dumps(index, option=orjson.OPT_SORT_KEYS))
            
    except OSError as e:
        raise OSError(f"Failed to delete from JSONL file {jsonl_file_path}: {str(e)}")
