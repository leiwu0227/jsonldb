"""
Core JSONL file operations for JSONLDB.
"""

import os
import logging
import tempfile
from typing import Dict, List, Optional, Union
import datetime as dt
import orjson
from bisect import bisect_left, bisect_right
import mmap

from . import metaslot


logger = logging.getLogger(__name__)
# Buffer size for file operations (50MB)
BUFFER_SIZE: int = 1024 * 1024 * 50
TIME_SPEC = 'seconds'  #or seconds/microseconds
REMOVED_DETAIL_BYTES = 160

# Type aliases for better readability
LineKey = Union[str, dt.datetime]
DataDict = Dict[str, dict]
IndexDict = Dict[str, int]

def _validate_row_keys(db_dict: DataDict,
                       timespec: Optional[str] = None) -> None:
    """Reject invalid records and the reserved key before file mutation."""
    for linekey, record in db_dict.items():
        if not isinstance(record, dict):
            raise TypeError("JSONL record values must be dictionaries")
        if serialize_linekey(linekey, timespec) == metaslot.META_KEY:
            raise ValueError("'_meta' is reserved for table metadata")


def _parse_row(line: bytes):
    """Parse one physical JSONL row and enforce the one-key record shape."""
    data = orjson.loads(line)
    if not isinstance(data, dict) or len(data) != 1:
        raise ValueError("JSONL rows must contain exactly one key")
    linekey, value = next(iter(data.items()))
    if not isinstance(value, dict):
        raise ValueError("JSONL record values must be dictionaries")
    return linekey, value


def _warn_invalid_row(jsonl_file_path: str, offset: int, line: bytes) -> None:
    """Report one skipped row with enough location detail to diagnose it."""
    logger.warning(
        "invalid JSON line in %s at byte %d: %s",
        jsonl_file_path,
        offset,
        line.decode('utf-8', errors='replace').strip(),
        extra={"jsonldb_file": jsonl_file_path, "jsonldb_offset": offset, "jsonldb_kind": "invalid_json"},
    )

def _atomic_write_bytes(file_path: str, content: bytes) -> None:
    """Write complete bytes to a same-directory temporary, then replace."""
    directory = os.path.dirname(os.path.abspath(file_path))
    prefix = "." + os.path.basename(file_path) + "."
    fd, tmp_path = tempfile.mkstemp(dir=directory, prefix=prefix, suffix=".tmp")
    try:
        with os.fdopen(fd, 'wb') as f:
            f.write(content)
        os.replace(tmp_path, file_path)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except FileNotFoundError:
            pass
        raise


def _serialize_index(index: IndexDict) -> bytes:
    return orjson.dumps(index, option=orjson.OPT_SORT_KEYS)


def _write_index(jsonl_file_path: str, index: IndexDict) -> None:
    _atomic_write_bytes(f"{jsonl_file_path}.idx", _serialize_index(index))

def build_jsonl_index(jsonl_file_path: str, warn_invalid: bool = True) -> None:
    """Build a sorted absolute-offset index, optionally warning on bad rows."""
    index_dict: IndexDict = {}

    if not os.path.exists(jsonl_file_path):
        raise FileNotFoundError(f"JSONL file not found: {jsonl_file_path}")

    # Handle empty file case
    if os.path.getsize(jsonl_file_path) == 0:
        _write_index(jsonl_file_path, index_dict)
        return

    try:
        with open(jsonl_file_path, 'rb', buffering=BUFFER_SIZE) as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                current_pos = 0
                while True:
                    raw_line = mm.readline()
                    if not raw_line:
                        break
                    next_pos = mm.tell()

                    if current_pos == 0 and metaslot.classify_line(raw_line).is_slot:
                        current_pos = next_pos
                        continue

                    line = raw_line.strip()
                    if not line:  # Skip empty lines
                        current_pos = next_pos
                        continue

                    try:
                        linekey, _ = _parse_row(line)
                        index_dict[linekey] = current_pos
                    except (orjson.JSONDecodeError, ValueError, TypeError):
                        if warn_invalid:
                            _warn_invalid_row(jsonl_file_path, current_pos, raw_line)

                    current_pos = next_pos

        # Save index (OPT_SORT_KEYS sorts on dump)
        _write_index(jsonl_file_path, index_dict)
            
    except OSError as e:
        raise OSError(f"Failed to build index for {jsonl_file_path}: {str(e)}")

def ensure_index_exists(jsonl_file_path: str) -> None:
    """Rebuild a missing, corrupt-empty, or stale index."""
    index_file_path = f"{jsonl_file_path}.idx"

    should_rebuild = False
    reason = None
    if not os.path.exists(index_file_path):
        should_rebuild = True
        reason = "missing"
    elif os.path.getsize(index_file_path) == 0:
        # Zero bytes are corrupt; a valid empty index is b'{}'.
        should_rebuild = True
        reason = "empty"
    elif os.path.getmtime(jsonl_file_path) > os.path.getmtime(index_file_path):
        should_rebuild = True
        reason = "stale"

    if should_rebuild:
        if reason != "stale":
            logger.warning("rebuilt %s index %s", reason, index_file_path)
        build_jsonl_index(jsonl_file_path)


def load_index(jsonl_file_path: str) -> dict:
    """Load the single self-healing index-read path."""
    index_file_path = f"{jsonl_file_path}.idx"
    ensure_index_exists(jsonl_file_path)  # heals missing / empty / stale
    try:
        with open(index_file_path, 'rb') as f:
            index = orjson.loads(f.read())
        if isinstance(index, dict):
            return index
    except (orjson.JSONDecodeError, OSError):
        pass
    # Unparseable, unreadable, or non-object index -> rebuild from the .jsonl.
    logger.warning("rebuilt corrupt index %s", index_file_path)
    build_jsonl_index(jsonl_file_path)
    with open(index_file_path, 'rb') as f:
        return orjson.loads(f.read())


def _lint_counts(jsonl_file_path: str, full: bool):
    newlines = non_blank = 0
    if os.path.getsize(jsonl_file_path) == 0:
        return newlines, non_blank
    with open(jsonl_file_path, 'rb') as f:
        if full:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                for line in iter(mm.readline, b''):
                    newlines += line.count(b'\n')
                    non_blank += bool(line.strip())
        else:
            while True:
                chunk = f.read(BUFFER_SIZE)
                if not chunk:
                    break
                newlines += chunk.count(b'\n')
    return newlines, non_blank


def _lint_load_index(jsonl_file_path: str):
    index_path = jsonl_file_path + '.idx'
    fresh = (os.path.exists(index_path) and os.path.getsize(index_path) > 0
             and os.path.getmtime(index_path) >= os.path.getmtime(jsonl_file_path))
    reason = "missing" if not os.path.exists(index_path) else "stale"
    if os.path.exists(index_path) and os.path.getsize(index_path) == 0:
        reason = "empty"
    if fresh:
        try:
            with open(index_path, 'rb') as f:
                index = orjson.loads(f.read())
            if not isinstance(index, dict):
                raise TypeError("index is not an object")
            return index, True
        except (orjson.JSONDecodeError, OSError, TypeError):
            reason = "corrupt"
    if reason != "stale":
        logger.warning("rebuilt %s index %s", reason, index_path)
    build_jsonl_index(jsonl_file_path, warn_invalid=False)
    with open(index_path, 'rb') as f:
        return orjson.loads(f.read()), False


def _lint_index_valid(jsonl_file_path: str, index: dict, force: bool) -> bool:
    keys = sorted(index, key=str)
    checks = keys if force else (keys[:1] + keys[-1:])
    try:
        with open(jsonl_file_path, 'rb', buffering=BUFFER_SIZE) as f:
            for key in checks:
                offset = index[key]
                if type(offset) is not int or offset < 0:
                    return False
                f.seek(offset)
                parsed_key, _ = _parse_row(f.readline().strip())
                if parsed_key != key:
                    return False
    except (orjson.JSONDecodeError, ValueError, TypeError, OSError):
        return False
    return True


def _lint_removed(jsonl_file_path: str, intervals, size: int):
    regions, end = [], 0
    with open(jsonl_file_path, 'rb') as source:
        for start, stop in sorted(intervals):
            if start > end:
                source.seek(end)
                regions.append((end, start - end,
                                source.read(min(REMOVED_DETAIL_BYTES, start - end))))
            end = max(end, stop)
        if end < size:
            source.seek(end)
            regions.append((end, size - end,
                            source.read(min(REMOVED_DETAIL_BYTES, size - end))))
    return regions


def _log_lint_removed(jsonl_file_path: str, regions) -> None:
    for offset, count, removed in regions:
        logger.warning("lint removed %d bytes from %s at byte %d",
                       count, jsonl_file_path, offset,
                       extra={"jsonldb_removed": removed,
                              "jsonldb_file": jsonl_file_path})


def _lint_rewrite(jsonl_file_path: str, index: dict, slot: Optional[bytes]) -> None:
    directory = os.path.dirname(os.path.abspath(jsonl_file_path))
    fd, tmp_path = tempfile.mkstemp(
        dir=directory, prefix='.' + os.path.basename(jsonl_file_path) + '.',
        suffix='.tmp')
    new_index, kept, offset = {}, [], 0
    try:
        with os.fdopen(fd, 'wb', buffering=BUFFER_SIZE) as dst:
            if slot is not None:
                dst.write(slot)
                offset = len(slot)
            with open(jsonl_file_path, 'rb', buffering=BUFFER_SIZE) as src:
                for key in sorted(index, key=str):
                    start = index[key]
                    src.seek(start)
                    raw = src.readline()
                    kept.append((start, start + len(raw)))
                    line = raw.rstrip(b'\r\n') + b'\n'
                    new_index[key] = offset
                    dst.write(line)
                    offset += len(line)
        old_size = os.path.getsize(jsonl_file_path)
        info = metaslot.inspect_file(jsonl_file_path) if old_size else None
        if info is not None and info.is_slot and slot == info.raw_line:
            kept.append((0, len(info.raw_line)))
        elif (info is not None and info.is_unknown_version and slot is not None
              and slot.rstrip() == info.raw_line.rstrip()):
            kept.append((0, len(info.raw_line.rstrip())))
        removed = _lint_removed(jsonl_file_path, kept, old_size)
        os.replace(tmp_path, jsonl_file_path)
        _log_lint_removed(jsonl_file_path, removed)
        _write_index(jsonl_file_path, new_index)
        logger.warning("lint repaired layout in %s", jsonl_file_path,
                       extra={"jsonldb_file": jsonl_file_path,
                              "jsonldb_kind": "layout_repaired"})
    except BaseException:
        try:
            os.unlink(tmp_path)
        except FileNotFoundError:
            pass
        raise


def _verify_and_compact(jsonl_file_path: str, index_dict: dict) -> bool:
    return _lint_file(jsonl_file_path, index_dict, False, None)


def _lint_file(jsonl_file_path: str, index: dict, force: bool,
               slot_bytes: Optional[int]) -> bool:
    size = os.path.getsize(jsonl_file_path)
    info = metaslot.inspect_file(jsonl_file_path) if size else None
    desired_slot = metaslot.lint_slot(info, slot_bytes)
    if not _lint_index_valid(jsonl_file_path, index, force):
        build_jsonl_index(jsonl_file_path, warn_invalid=False)
        index = load_index(jsonl_file_path)

    newlines, non_blank = _lint_counts(jsonl_file_path, force)
    slot_count = int(info is not None and info.is_slot)
    if force and non_blank - slot_count != len(index):
        build_jsonl_index(jsonl_file_path, warn_invalid=False)
        index = load_index(jsonl_file_path)

    keys = sorted(index, key=str)
    offsets = [index[key] for key in keys]
    valid_offsets = all(
        type(value) is int and 0 <= value < size for value in offsets)
    if not valid_offsets:
        build_jsonl_index(jsonl_file_path, warn_invalid=False)
        index = load_index(jsonl_file_path)
        keys = sorted(index, key=str)
        offsets = [index[key] for key in keys]
    valid_offsets = all(a < b for a, b in zip(offsets, offsets[1:]))
    start = len(info.raw_line) if info is not None and info.is_slot else 0
    slot_ok = ((desired_slot is None and
                (info is None or not info.is_slot)) or
               (info is not None and info.raw_line == desired_slot))
    end_ok = size == start if not offsets else False
    if offsets and valid_offsets:
        with open(jsonl_file_path, 'rb', buffering=BUFFER_SIZE) as f:
            f.seek(offsets[-1])
            last = f.readline()
        end_ok = offsets[0] == start and offsets[-1] + len(last) == size \
            and last.endswith(b'\n')
    canonical = (slot_ok and valid_offsets and end_ok
                 and newlines == len(index) + int(desired_slot is not None))
    if not canonical:
        _lint_rewrite(jsonl_file_path, index, desired_slot)
    return True


def lint_jsonl(jsonl_file_path: str, force: bool = False,
               slot_bytes: Optional[int] = None) -> bool:
    """Restore fidelity and canonical layout with a fresh-index fast path."""
    if not os.path.exists(jsonl_file_path):
        return False
    index, fresh = _lint_load_index(jsonl_file_path)
    return _lint_file(jsonl_file_path, index, force or not fresh, slot_bytes)
def _is_datetime_string(linekey: str, timespec: Optional[str] = None) -> bool:
    """Return whether text resembles an ISO datetime at this precision."""
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

def save_jsonl(jsonl_file_path: str, db_dict: DataDict,
               timespec: Optional[str] = None, meta: Optional[dict] = None,
               slot_bytes: Optional[int] = None) -> None:
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
    _validate_row_keys(db_dict, timespec)

    existing_slot = None
    if os.path.exists(jsonl_file_path) and os.path.getsize(jsonl_file_path):
        info = metaslot.inspect_file(jsonl_file_path)
        if info.is_slot:
            existing_slot = info

    width = slot_bytes
    if width is None and existing_slot is not None:
        width = existing_slot.width
    if meta is not None and width is None:
        raise ValueError("metadata slot is not enabled for this file")

    placeholder = final_slot = None
    if width is not None:
        placeholder = metaslot.encode_slot(None, width)
        if meta is not None:
            final_slot = metaslot.encode_slot(meta, width)
        elif existing_slot is not None and slot_bytes is None:
            final_slot = existing_slot.raw_line
        else:
            record = existing_slot.record if existing_slot is not None else None
            final_slot = metaslot.encode_slot(record, width)
    
    try:
        # Handle empty legacy dictionary case
        if not db_dict and width is None:
            with open(jsonl_file_path, 'wb') as f:
                pass  # create empty file
            _write_index(jsonl_file_path, {})
            return

        # Stream lines to the file while tracking byte offsets
        byte_offset = width or 0
        with open(jsonl_file_path, 'wb', buffering=BUFFER_SIZE) as f:
            if placeholder is not None:
                # The absent-record placeholder reserves offsets. The actual
                # record is published only after every row has been flushed.
                f.write(placeholder)
            for linekey, data in db_dict.items():
                serialized_key = serialize_linekey(linekey, timespec)
                line = _fast_dumps({serialized_key: data}).encode('utf-8')
                f.write(line)
                index[serialized_key] = byte_offset
                byte_offset += len(line)

            if final_slot is not None:
                f.flush()
                f.seek(0)
                f.write(final_slot)

        # Write index (OPT_SORT_KEYS sorts on dump)
        _write_index(jsonl_file_path, index)
            
    except OSError as e:
        raise OSError(f"Failed to save JSONL file {jsonl_file_path}: {str(e)}")


def save_jsonl_atomic(jsonl_file_path: str, db_dict: DataDict,
                      timespec: Optional[str] = None) -> None:
    """Atomically replace a complete JSONL file, then publish its index last.

    This narrow path is intended for small protected control files. Ordinary
    table and db.meta saves retain their existing in-place publication policy.
    """
    _validate_row_keys(db_dict, timespec)
    directory = os.path.dirname(os.path.abspath(jsonl_file_path))
    prefix = "." + os.path.basename(jsonl_file_path) + "."
    fd, tmp_path = tempfile.mkstemp(dir=directory, prefix=prefix, suffix=".tmp")
    index: IndexDict = {}
    try:
        byte_offset = 0
        with os.fdopen(fd, 'wb', buffering=BUFFER_SIZE) as f:
            for linekey, data in db_dict.items():
                serialized_key = serialize_linekey(linekey, timespec)
                line = _fast_dumps({serialized_key: data}).encode('utf-8')
                f.write(line)
                index[serialized_key] = byte_offset
                byte_offset += len(line)
        os.replace(tmp_path, jsonl_file_path)
        _write_index(jsonl_file_path, index)
    except BaseException as e:
        try:
            os.unlink(tmp_path)
        except FileNotFoundError:
            pass
        if isinstance(e, OSError):
            raise OSError(
                f"Failed to save JSONL file {jsonl_file_path}: {str(e)}"
            ) from e
        raise


def migrate_jsonl_slot(jsonl_file_path: str, slot_bytes: int) -> bool:
    """Atomically give one table ``slot_bytes`` while preserving its contents.

    The table is replaced before its derived index. A retry rebuilds the index
    even when the table already has the target width, which repairs an
    interruption between those two publication boundaries without rewriting
    the conforming table.
    """
    info = metaslot.inspect_file(jsonl_file_path)
    slot = metaslot.encode_slot(info.record if info.is_slot else None, slot_bytes)
    if info.is_slot and info.width == slot_bytes:
        build_jsonl_index(jsonl_file_path)
        return False

    directory = os.path.dirname(os.path.abspath(jsonl_file_path))
    prefix = "." + os.path.basename(jsonl_file_path) + "."
    fd, tmp_path = tempfile.mkstemp(dir=directory, prefix=prefix, suffix=".tmp")
    try:
        with os.fdopen(fd, 'wb', buffering=BUFFER_SIZE) as dst:
            dst.write(slot)
            with open(jsonl_file_path, 'rb', buffering=BUFFER_SIZE) as src:
                if info.is_slot:
                    src.readline()
                while True:
                    chunk = src.read(BUFFER_SIZE)
                    if not chunk:
                        break
                    dst.write(chunk)
        os.replace(tmp_path, jsonl_file_path)
        build_jsonl_index(jsonl_file_path)
        return True
    except BaseException:
        try:
            os.unlink(tmp_path)
        except FileNotFoundError:
            pass
        raise

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
            offset = 0
            for raw_line in f:
                if offset == 0 and metaslot.classify_line(raw_line).is_slot:
                    offset += len(raw_line)
                    continue
                line = raw_line.strip()
                if not line:
                    offset += len(raw_line)
                    continue

                try:
                    linekey, value = _parse_row(line)
                    _store_with_key(
                        result_dict, linekey, value, auto_deserialize, timespec
                    )
                except (orjson.JSONDecodeError, ValueError, TypeError):
                    _warn_invalid_row(jsonl_file_path, offset, raw_line)
                offset += len(raw_line)

        return result_dict

    except OSError as e:
        raise OSError(f"Failed to load JSONL file {jsonl_file_path}: {str(e)}")


def _load_legacy_rows(jsonl_file_path: str) -> dict:
    """Read pre-validation scalar control rows for one-time migration."""
    result = {}
    with open(jsonl_file_path, 'rb') as source:
        for raw_line in source:
            try:
                value = orjson.loads(raw_line)
                if isinstance(value, dict) and len(value) == 1:
                    result.update(value)
            except (orjson.JSONDecodeError, TypeError):
                continue
    return result


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
                try:
                    parsed_key, value = _parse_row(line.strip())
                    if parsed_key != linekey:
                        raise ValueError("key mismatch")
                    raw_results[linekey] = value
                except (orjson.JSONDecodeError, ValueError, KeyError, TypeError):
                    _warn_invalid_row(jsonl_file_path, offset, line)

        # Rebuild in sorted key order with deserialization
        result_dict = {}
        for linekey in selected_linekeys:
            if linekey in raw_results:
                _store_with_key(
                    result_dict, linekey, raw_results[linekey],
                    auto_deserialize, timespec
                )
        return result_dict
        
    except OSError as e:
        raise OSError(f"Failed to select from JSONL file {jsonl_file_path}: {str(e)}")

def select_line_jsonl(
    jsonl_file_path: str,
    linekey: LineKey,
    auto_deserialize: bool = True,
    timespec: Optional[str] = None,
) -> DataDict:
    """
    Get a single record from a JSONL file based on the linekey.

    Args:
        jsonl_file_path: Path to the JSONL file
        linekey: The key to look for
        auto_deserialize: Whether to serialize the lookup key and deserialize
            datetime-looking keys in the result
        timespec: Datetime precision ('seconds' or 'microseconds').
            Defaults to the module-level TIME_SPEC.

    Returns:
        Single-record dict {linekey: value} if found, {} otherwise.
        Example: {"key1": {"v": 1}}
    """
    linekey = serialize_linekey(linekey, timespec)
    
    # Read the index file (self-heals an empty/corrupt .idx)
    index_dict = load_index(jsonl_file_path)

    # Check if key exists in index
    if linekey not in index_dict:
        return {}
    
    result_dict: DataDict = {}
        

    # Load selected records
    with open(jsonl_file_path, 'rb', buffering=BUFFER_SIZE) as f:
        offset = index_dict[linekey]
        line = b''
        try:
            f.seek(offset)
            line = f.readline()
            parsed_key, value = _parse_row(line.strip())
            if parsed_key != linekey:
                raise ValueError("key mismatch")
            _store_with_key(
                result_dict, linekey, value, auto_deserialize, timespec
            )
        except (orjson.JSONDecodeError, ValueError, KeyError, TypeError):
            _warn_invalid_row(jsonl_file_path, offset, line)
            return {}

    return result_dict



def _blank_old_lines(f, old_lines) -> None:
    """Blank grown records only after their replacement rows are flushed."""
    for pos, old_len in old_lines:
        f.seek(pos)
        f.write(b' ' * (old_len - 1) + b'\n')


def update_jsonl(jsonl_file_path: str, update_dict: DataDict,
                 timespec: Optional[str] = None,
                 meta: Optional[dict] = None) -> None:
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
    _validate_row_keys(update_dict, timespec)
    encoded_meta = None
    if meta is not None:
        info = metaslot.inspect_file(jsonl_file_path)
        if not info.is_slot:
            raise ValueError("metadata slot is not enabled for this file")
        encoded_meta = metaslot.encode_slot(meta, info.width)

    try:
        # Load index (self-heals an empty/corrupt .idx)
        index = load_index(jsonl_file_path)

        updates = []
        appends = []
        old_lines = []
        
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
                        appends.append((linekey, new_line))
                        old_lines.append((index[linekey], len(old_line)))
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
                # A process interruption after this boundary leaves the old
                # row plus a complete replacement, never a missing key.
                f.flush()

            if encoded_meta is not None:
                # Rows must reach the OS before the record that describes them.
                f.flush()
                f.seek(0)
                f.write(encoded_meta)
                f.flush()

            _blank_old_lines(f, old_lines)

        # Update index
        _write_index(jsonl_file_path, index)
            
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
    _validate_row_keys({key: {} for key in linekeys}, timespec)
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
        _write_index(jsonl_file_path, index)
            
    except OSError as e:
        raise OSError(f"Failed to delete from JSONL file {jsonl_file_path}: {str(e)}")


def read_jsonl_meta(jsonl_file_path: str):
    """Read the valid known-version record from line one, if present."""
    return metaslot.read_slot(jsonl_file_path)


def write_jsonl_meta(jsonl_file_path: str, meta: Optional[dict]) -> None:
    """Publish one slot-only metadata change, then mark the index fresh last."""
    load_index(jsonl_file_path)
    metaslot.write_slot(jsonl_file_path, meta)
    os.utime(jsonl_file_path + '.idx', None)
