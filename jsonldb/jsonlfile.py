"""
Core JSONL file operations for JSONLDB.
"""

import os
import pandas as pd
from typing import Dict, List, Optional, Union, Any
import datetime as dt
import orjson
from bisect import bisect_left, bisect_right
import mmap
import tempfile

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
INDEX_READ_RETRIES = 3


class _IndexOwnerMismatch(Exception):
    """Signal that an index snapshot does not belong to the opened owner."""


def _file_identity(file_stat: os.stat_result) -> tuple:
    """Return the fields needed to recognize an atomically replaced file."""
    return (
        file_stat.st_dev,
        file_stat.st_ino,
        file_stat.st_size,
        file_stat.st_mtime_ns,
    )


def _sync_file(path: str) -> None:
    """Flush a completely staged file to storage before publication."""
    with open(path, "rb") as staged_file:
        os.fsync(staged_file.fileno())


def _write_index_atomic(index_file_path: str, index_dict: IndexDict,
                        owner_mtime_ns: int) -> None:
    """Publish an index atomically and tag it with its owner's modification time."""
    index_dir = os.path.dirname(os.path.abspath(index_file_path))
    prefix = f".{os.path.basename(index_file_path)}."
    fd, tmp_path = tempfile.mkstemp(prefix=prefix, suffix=".tmp", dir=index_dir)
    try:
        with os.fdopen(fd, "wb") as index_file:
            index_file.write(orjson.dumps(index_dict, option=orjson.OPT_SORT_KEYS))
            index_file.flush()
            os.fsync(index_file.fileno())
        os.utime(tmp_path, ns=(owner_mtime_ns, owner_mtime_ns))
        os.replace(tmp_path, index_file_path)
    except BaseException:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise

# --------------------------------------------------------
# Opaque Companion Functions
# --------------------------------------------------------

def get_aux_path(jsonl_file_path: str) -> str:
    """Return the sole opaque companion path for a JSONL ticker.

    The companion identity is fixed: ``<ticker>.jsonl.aux``.  This helper only
    derives the canonical path; it does not require either file to exist.
    """
    path = os.fspath(jsonl_file_path)
    if not isinstance(path, str) or not path.endswith(".jsonl"):
        raise ValueError("Companions are only supported for .jsonl ticker paths")
    return path + ".aux"


def read_aux(jsonl_file_path: str) -> bytes:
    """Read a ticker's opaque companion without interpreting its bytes."""
    with open(get_aux_path(jsonl_file_path), "rb") as companion:
        return companion.read()


def write_aux(jsonl_file_path: str, payload: bytes) -> None:
    """Atomically replace a ticker's opaque companion payload.

    A temporary file is written and flushed in the owner's directory before
    ``os.replace`` publishes it.  The JSONL owner must exist both before the
    write and immediately before publication, preventing creation of a known
    orphan through this API.
    """
    owner_path = os.fspath(jsonl_file_path)
    companion_path = get_aux_path(owner_path)
    if not isinstance(payload, (bytes, bytearray, memoryview)):
        raise TypeError("Companion payload must be bytes-like")
    if not os.path.isfile(owner_path):
        raise FileNotFoundError(f"JSONL file not found: {owner_path}")

    payload_bytes = bytes(payload)
    owner_dir = os.path.dirname(os.path.abspath(owner_path))
    prefix = f".{os.path.basename(companion_path)}."
    fd, tmp_path = tempfile.mkstemp(prefix=prefix, suffix=".tmp", dir=owner_dir)
    try:
        with os.fdopen(fd, "wb") as tmp_file:
            tmp_file.write(payload_bytes)
            tmp_file.flush()
            os.fsync(tmp_file.fileno())
        if not os.path.isfile(owner_path):
            raise FileNotFoundError(f"JSONL file not found: {owner_path}")
        os.replace(tmp_path, companion_path)
    except BaseException:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise


def remove_aux(jsonl_file_path: str) -> bool:
    """Remove a ticker's optional companion, returning whether one existed."""
    companion_path = get_aux_path(jsonl_file_path)
    if not os.path.lexists(companion_path):
        return False
    os.remove(companion_path)
    return True


def _invalidate_aux_if_jsonl(path: str) -> None:
    """Remove a companion before mutating a JSONL ticker, if applicable."""
    path = os.fspath(path)
    if isinstance(path, str) and path.endswith(".jsonl"):
        remove_aux(path)


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
    if not os.path.exists(jsonl_file_path):
        raise FileNotFoundError(f"JSONL file not found: {jsonl_file_path}")

    index_file_path = f"{jsonl_file_path}.idx"
    for _ in range(INDEX_READ_RETRIES):
        index_dict: IndexDict = {}
        try:
            with open(jsonl_file_path, 'rb', buffering=BUFFER_SIZE) as owner:
                owner_stat = os.fstat(owner.fileno())
                current_pos = 0
                for line in owner:
                    stripped_line = line.strip()
                    if stripped_line:
                        try:
                            data = orjson.loads(stripped_line)
                            linekey = next(iter(data))
                            index_dict[linekey] = current_pos
                        except (orjson.JSONDecodeError, ValueError, StopIteration):
                            print(
                                "WARNING: invalid JSON line "
                                + stripped_line.decode('utf-8', errors='replace')
                            )
                    current_pos = owner.tell()

            if _file_identity(os.stat(jsonl_file_path)) != _file_identity(owner_stat):
                continue

            _write_index_atomic(
                index_file_path,
                index_dict,
                owner_stat.st_mtime_ns,
            )
            if _file_identity(os.stat(jsonl_file_path)) == _file_identity(owner_stat):
                return
        except OSError as error:
            raise OSError(
                f"Failed to build index for {jsonl_file_path}: {str(error)}"
            ) from error

    raise OSError(
        f"Failed to build index for {jsonl_file_path}: owner changed repeatedly"
    )

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
            print(f"WARNING: rebuilt empty index {index_file_path}")
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
        print(f"WARNING: rebuilt corrupt index {index_file_path}")
        build_jsonl_index(jsonl_file_path)
        with open(index_file_path, 'rb') as f:
            return orjson.loads(f.read())


def _read_with_index(jsonl_file_path: str, reader):
    """Run an indexed read against one stable owner/index generation."""
    last_error = None
    for _ in range(INDEX_READ_RETRIES):
        try:
            owner_before = os.stat(jsonl_file_path)
            index_dict = load_index(jsonl_file_path)
            owner_after = os.stat(jsonl_file_path)
            if _file_identity(owner_before) != _file_identity(owner_after):
                continue

            with open(jsonl_file_path, 'rb', buffering=BUFFER_SIZE) as owner:
                if _file_identity(os.fstat(owner.fileno())) != _file_identity(owner_after):
                    continue
                return reader(index_dict, owner)
        except (
            _IndexOwnerMismatch,
            orjson.JSONDecodeError,
            KeyError,
            TypeError,
            ValueError,
        ) as error:
            last_error = error
            build_jsonl_index(jsonl_file_path)

    message = f"Could not obtain a stable index snapshot for {jsonl_file_path}"
    if last_error is not None:
        message += f": {last_error}"
    raise OSError(message)

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

    keys = list(index_dict.keys())
    is_sorted = all(keys[i] <= keys[i+1] for i in range(len(keys)-1))

    if is_sorted:
        if index_dict[keys[0]] == 0:
            with open(jsonl_file_path, 'rb', buffering=BUFFER_SIZE) as f:
                f.seek(index_dict[keys[-1]])
                last_line = f.readline()
                expected_end = index_dict[keys[-1]] + len(last_line)
                actual_size = os.path.getsize(jsonl_file_path)
                if expected_end == actual_size:
                    return True

    sorted_keys = sorted(keys, key=str)
    tmp_path = jsonl_file_path + '.tmp'

    with open(jsonl_file_path, 'rb', buffering=BUFFER_SIZE) as src:
        with open(tmp_path, 'wb', buffering=BUFFER_SIZE) as dst:
            for key in sorted_keys:
                src.seek(index_dict[key])
                line = src.readline()
                dst.write(line)

    _invalidate_aux_if_jsonl(jsonl_file_path)
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
    jsonl_file_path = os.fspath(jsonl_file_path)
    if not os.path.exists(jsonl_file_path):
        if (isinstance(jsonl_file_path, str)
                and jsonl_file_path.endswith(".jsonl")
                and os.path.lexists(jsonl_file_path + ".aux")):
            print(f"WARNING: orphan companion {jsonl_file_path}.aux")
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
    _invalidate_aux_if_jsonl(jsonl_file_path)
    
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
                    print("WARNING: invalid JSON line " + line.decode('utf-8', errors='replace'))
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

    serialized_lower = (
        serialize_linekey(lower_key, timespec)
        if lower_key is not None
        else None
    )
    serialized_upper = (
        serialize_linekey(upper_key, timespec)
        if upper_key is not None
        else None
    )

    def read_range(index_dict, owner):
        # If no keys in index, return empty dict
        if not index_dict:
            return {}
            
        # Get all keys from index
        all_keys = list(index_dict.keys())
        
        # Set default values if None
        effective_lower = (
            all_keys[0] if serialized_lower is None else serialized_lower
        )
        effective_upper = (
            all_keys[-1] if serialized_upper is None else serialized_upper
        )
        
        # Use bisect for O(log n) range selection
        lo = bisect_left(all_keys, effective_lower)
        hi = bisect_right(all_keys, effective_upper)
        selected_linekeys = all_keys[lo:hi]

        # Read in offset order for sequential I/O
        offset_key_pairs = sorted(
            [(index_dict[k], k) for k in selected_linekeys]
        )
        raw_results = {}
        for offset, selected_key in offset_key_pairs:
            owner.seek(offset)
            line = owner.readline()
            data = orjson.loads(line)
            if (
                not isinstance(data, dict)
                or len(data) != 1
                or selected_key not in data
            ):
                raise _IndexOwnerMismatch(
                    f"index key {selected_key!r} does not match owner"
                )
            raw_results[selected_key] = data[selected_key]

        # Rebuild in sorted key order with deserialization
        result_dict = {}
        for selected_key in selected_linekeys:
            _store_with_key(
                result_dict,
                selected_key,
                raw_results[selected_key],
                auto_deserialize,
                timespec,
            )
        return result_dict

    try:
        return _read_with_index(jsonl_file_path, read_range)
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
    
    def read_line(index_dict, owner):
        if linekey not in index_dict:
            return {}

        owner.seek(index_dict[linekey])
        line = owner.readline().strip()
        data = orjson.loads(line)
        if not isinstance(data, dict) or len(data) != 1 or linekey not in data:
            raise _IndexOwnerMismatch(
                f"index key {linekey!r} does not match owner"
            )

        result_dict: DataDict = {}
        _store_with_key(
            result_dict,
            linekey,
            data[linekey],
            auto_serialize,
            timespec,
        )
        return result_dict

    return _read_with_index(jsonl_file_path, read_line)



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
        _invalidate_aux_if_jsonl(jsonl_file_path)

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
        if not any(linekey in index for linekey in linekeys):
            return
        _invalidate_aux_if_jsonl(jsonl_file_path)
        
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


def _load_replacement_owner(jsonl_file_path: str) -> DataDict:
    """Strictly load one owner so staging cannot silently discard stored rows."""
    records: DataDict = {}
    with open(jsonl_file_path, "rb", buffering=BUFFER_SIZE) as owner:
        for raw_line in owner:
            line = raw_line.strip()
            if not line:
                continue
            data = orjson.loads(line)
            if not isinstance(data, dict) or len(data) != 1:
                raise ValueError("Existing JSONL owner contains an invalid record")
            linekey = next(iter(data))
            payload = data[linekey]
            if linekey in records:
                raise ValueError(
                    f"Existing JSONL owner contains duplicate key {linekey!r}"
                )
            if not isinstance(payload, dict) or not payload:
                raise ValueError(
                    f"Existing JSONL owner contains invalid payload for {linekey!r}"
                )
            records[linekey] = payload
    return records


def _serialized_replacement(replacement_dict: DataDict, lower_key: str,
                            upper_key: str, timespec: Optional[str]) -> DataDict:
    """Validate and serialize all replacement rows before any publication."""
    serialized: DataDict = {}
    for linekey, payload in replacement_dict.items():
        serialized_key = serialize_linekey(linekey, timespec)
        if serialized_key in serialized:
            raise ValueError(
                f"Replacement keys collide after serialization: {serialized_key!r}"
            )
        if not lower_key <= serialized_key <= upper_key:
            raise ValueError(
                f"Replacement key {serialized_key!r} is outside "
                f"[{lower_key!r}, {upper_key!r}]"
            )
        if not isinstance(payload, dict) or not payload:
            raise ValueError(
                f"Replacement payload for {serialized_key!r} must be a non-empty dict"
            )
        _fast_dumps({serialized_key: payload})
        serialized[serialized_key] = payload
    return serialized


def replace_jsonl_range(jsonl_file_path: str, lower_key: LineKey,
                        upper_key: LineKey, replacement_dict: DataDict,
                        timespec: Optional[str] = None) -> None:
    """Atomically replace an inclusive range in an existing JSONL owner.

    The complete new owner and index are staged and synced before the optional
    opaque companion is invalidated. The owner is the atomic publication point;
    the derived index follows and remains recoverable if publication stops.
    """
    owner_path = os.fspath(jsonl_file_path)
    if not os.path.isfile(owner_path):
        raise FileNotFoundError(f"JSONL file not found: {owner_path}")
    if not isinstance(replacement_dict, dict):
        raise TypeError("Replacement records must be a dictionary")

    serialized_lower = serialize_linekey(lower_key, timespec)
    serialized_upper = serialize_linekey(upper_key, timespec)
    if serialized_lower > serialized_upper:
        raise ValueError("lower_key must be less than or equal to upper_key")

    replacement = _serialized_replacement(
        replacement_dict,
        serialized_lower,
        serialized_upper,
        timespec,
    )
    existing = _load_replacement_owner(owner_path)
    merged = {
        key: existing[key]
        for key in sorted(existing)
        if key < serialized_lower
    }
    merged.update({key: replacement[key] for key in sorted(replacement)})
    merged.update({
        key: existing[key]
        for key in sorted(existing)
        if key > serialized_upper
    })

    owner_dir = os.path.dirname(os.path.abspath(owner_path))
    prefix = f".{os.path.basename(owner_path)}."
    fd, staged_owner = tempfile.mkstemp(
        prefix=prefix,
        suffix=".range.tmp",
        dir=owner_dir,
    )
    os.close(fd)
    staged_index = staged_owner + ".idx"
    try:
        save_jsonl(staged_owner, merged, timespec)
        _sync_file(staged_owner)
        _sync_file(staged_index)

        old_owner_stat = os.stat(owner_path)
        old_index_mtime = (
            os.stat(owner_path + ".idx").st_mtime_ns
            if os.path.exists(owner_path + ".idx")
            else 0
        )
        publication_mtime = max(
            old_owner_stat.st_mtime_ns,
            old_index_mtime,
        ) + 1_000_000_000
        os.utime(
            staged_owner,
            ns=(publication_mtime, publication_mtime),
        )
        os.utime(
            staged_index,
            ns=(publication_mtime, publication_mtime),
        )

        remove_aux(owner_path)
        os.replace(staged_owner, owner_path)
        os.replace(staged_index, owner_path + ".idx")
    finally:
        for staged_path in (staged_owner, staged_index):
            if os.path.exists(staged_path):
                os.remove(staged_path)
