"""Core JSONL file operations for JSONLDB."""

import os
import logging
import tempfile
import sys
from collections import OrderedDict
from threading import RLock
from typing import Dict, List, NamedTuple, Optional, Union
import datetime as dt
import orjson
from pandas import Timestamp as _Timestamp
from bisect import bisect_left, bisect_right
import mmap

from . import metaslot, _tabletimezone


logger = logging.getLogger(__name__)
BUFFER_SIZE: int = 1024 * 1024 * 50
TIME_SPEC = 'seconds'  #or seconds/microseconds
REMOVED_DETAIL_BYTES = 160

LineKey = Union[str, dt.datetime]
DataDict = Dict[str, dict]
IndexDict = Dict[str, int]


class _CachedIndex(NamedTuple):
    index: dict
    fingerprint: tuple
    epoch: int
    keys: Optional[tuple]
    size: int


_INDEX_CACHE_LIMIT = 64 * 1024 * 1024
_INDEX_CACHE = OrderedDict()
_INDEX_CACHE_LOCK = RLock()
_INDEX_CACHE_BYTES = _INDEX_CACHE_EPOCH = 0


def _file_identity(stat):
    return (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)


def _index_fingerprint(path):
    try:
        return _file_identity(os.stat(path)), _file_identity(os.stat(f'{path}.idx'))
    except OSError:
        return None


def _invalidate_index_cache(path):
    """Forget affected reads before mutation; None clears all retained entries."""
    global _INDEX_CACHE_BYTES, _INDEX_CACHE_EPOCH
    with _INDEX_CACHE_LOCK:
        _INDEX_CACHE_EPOCH += 1
        if path is None:
            _INDEX_CACHE.clear()
            _INDEX_CACHE_BYTES = 0
        else:
            entry = _INDEX_CACHE.pop(os.path.abspath(path), None)
            if entry is not None:
                _INDEX_CACHE_BYTES -= entry.size


def _reset_cache_after_fork():
    global _INDEX_CACHE_LOCK
    _INDEX_CACHE_LOCK = RLock()
    _invalidate_index_cache(None)


if hasattr(os, 'register_at_fork'):
    os.register_at_fork(after_in_child=_reset_cache_after_fork)


def _read_index_snapshot(path):
    """Associate parsed bytes with stable paths and the opened index identity."""
    with _INDEX_CACHE_LOCK:
        epoch = _INDEX_CACHE_EPOCH
    before = _index_fingerprint(path)
    with open(f'{path}.idx', 'rb') as source:
        start = _file_identity(os.fstat(source.fileno()))
        raw = source.read()
        index = orjson.loads(raw)
        end = _file_identity(os.fstat(source.fileno()))
    after = _index_fingerprint(path)
    stable = before is not None and before == after and start == end == after[1]
    # Conservative decoded-object bound: JSON bytes cover key characters;
    # non-ASCII/escaped keys may need four bytes per decoded character.
    ascii_keys = raw.isascii() and b'\\' not in raw
    header = sys.getsizeof('' if ascii_keys else '\U00010000')
    weight = (sys.getsizeof(index) + len(raw) * (1 if ascii_keys else 4)
              + len(index) * (header + sys.getsizeof((1 << 64) - 1))) if isinstance(index, dict) else 0
    return index, after if stable else None, epoch, weight


def _read_cached_index(path, with_keys=False):
    """Return a private read snapshot; never lend it to callers or writers."""
    global _INDEX_CACHE_BYTES
    cache_key = os.path.abspath(path)
    fingerprint = _index_fingerprint(path)
    with _INDEX_CACHE_LOCK:
        entry = _INDEX_CACHE.get(cache_key)
        if entry is not None and (fingerprint is None or entry.fingerprint != fingerprint):
            _invalidate_index_cache(path)
            entry = None
        if entry is not None:
            _INDEX_CACHE.move_to_end(cache_key)
            if not with_keys or entry.keys is not None:
                return entry.index, entry.keys
        epoch = _INDEX_CACHE_EPOCH
    if entry is None:
        index, fingerprint, epoch, weight = _load_index_snapshot(path)
        keys = tuple(index) if with_keys else None
        if fingerprint is None or not isinstance(index, dict) or weight > _INDEX_CACHE_LIMIT:
            return index, keys
        # Malformed offset objects retain ordinary read behavior, but are not
        # admitted: shallow accounting cannot bound arbitrary nested objects.
        if set(map(type, index.values())) - {int}:
            return index, keys
        size = (weight + sys.getsizeof(cache_key)
                + sys.getsizeof(fingerprint) + sum(map(sys.getsizeof, fingerprint))
                + sum(sys.getsizeof(part) for stamp in fingerprint for part in stamp)
                + 256)  # Entry and OrderedDict-node/accounting allowance.
    else:
        index, fingerprint, _, _, size = entry
        keys = tuple(index)
    if keys is not None:
        size += sys.getsizeof(keys)
    candidate = _CachedIndex(index, fingerprint, epoch, keys, size)
    with _INDEX_CACHE_LOCK:
        if (epoch == _INDEX_CACHE_EPOCH and size <= _INDEX_CACHE_LIMIT
                and _INDEX_CACHE.get(cache_key) is entry):
            previous = _INDEX_CACHE.pop(cache_key, None)
            if previous is not None:
                _INDEX_CACHE_BYTES -= previous.size
            while _INDEX_CACHE and _INDEX_CACHE_BYTES + size > _INDEX_CACHE_LIMIT:
                _, evicted = _INDEX_CACHE.popitem(last=False)
                _INDEX_CACHE_BYTES -= evicted.size
            _INDEX_CACHE[cache_key] = candidate
            _INDEX_CACHE_BYTES += size
    return index, keys

def _validate_row_keys(db_dict: DataDict, timespec: Optional[str] = None, *, reuse=False):
    """Validate eagerly; optionally defer iteration over stable prepared keys."""
    keys = [] if (reuse and type(db_dict) is dict and db_dict
                  and type(next(iter(db_dict))) in (dt.datetime, _Timestamp)
                  and type(timespec) in (str, type(None)) and type(TIME_SPEC) is str) else None
    for linekey, record in db_dict.items():
        if not isinstance(record, dict):
            raise TypeError("JSONL record values must be dictionaries")
        serialized = serialize_linekey(linekey, timespec)
        if serialized == metaslot.META_KEY:
            raise ValueError("'_meta' is reserved for table metadata")
        if keys is not None:
            if (type(linekey) is str or (type(linekey) in (dt.datetime, _Timestamp)
                    and (linekey.tzinfo is None or type(linekey.tzinfo) is dt.timezone))):
                keys.append(serialized)
            else:
                keys = None  # Custom conversions retain both passes for the whole input.
    if reuse:
        return lambda: zip(keys, db_dict.values()) if keys is not None else db_dict.items()


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

def _read_failure(path, offset, line, error, strict):
    if strict:
        raise ValueError(f"invalid observation in {path} at byte {offset}: {error}") from error
    _warn_invalid_row(path, offset, line)


def _check_read_offset(path, offset):
    if type(offset) is not int or not 0 <= offset <= sys.maxsize:
        raise ValueError(f"invalid observation in {path} at byte {offset!r}: invalid indexed offset")


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
    _invalidate_index_cache(jsonl_file_path)
    _atomic_write_bytes(f"{jsonl_file_path}.idx", _serialize_index(index))

def build_jsonl_index(jsonl_file_path: str, warn_invalid: bool = True) -> None:
    """Build a sorted absolute-offset index, optionally warning on bad rows."""
    index_dict: IndexDict = {}
    _invalidate_index_cache(jsonl_file_path)

    if not os.path.exists(jsonl_file_path):
        raise FileNotFoundError(f"JSONL file not found: {jsonl_file_path}")

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
                    if not line:
                        current_pos = next_pos
                        continue

                    try:
                        linekey, _ = _parse_row(line)
                        index_dict[linekey] = current_pos
                    except (orjson.JSONDecodeError, ValueError, TypeError):
                        if warn_invalid:
                            _warn_invalid_row(jsonl_file_path, current_pos, raw_line)

                    current_pos = next_pos

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
    """Load an independent mutable index through the shared recovery path."""
    return _load_index_snapshot(jsonl_file_path)[0]


def _load_index_snapshot(jsonl_file_path):
    """Recover once, then return the final parsed index and admission evidence."""
    index_file_path = f"{jsonl_file_path}.idx"
    ensure_index_exists(jsonl_file_path)  # heals missing / empty / stale
    try:
        snapshot = _read_index_snapshot(jsonl_file_path)
        if isinstance(snapshot[0], dict):
            return snapshot
    except (orjson.JSONDecodeError, OSError):
        pass
    # Unparseable, unreadable, or non-object index -> rebuild from the .jsonl.
    logger.warning("rebuilt corrupt index %s", index_file_path)
    build_jsonl_index(jsonl_file_path)
    return _read_index_snapshot(jsonl_file_path)


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
    _invalidate_index_cache(jsonl_file_path)
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
    metaslot.lint_slot(metaslot.inspect_file(jsonl_file_path), slot_bytes)
    index, fresh = _lint_load_index(jsonl_file_path)
    return _lint_file(jsonl_file_path, index, force or not fresh, slot_bytes)
def _is_datetime_string(linekey: str, timespec: Optional[str] = None) -> bool:
    """Return whether text resembles an ISO datetime at this precision."""
    if (timespec or TIME_SPEC) == 'seconds':
        return len(linekey) == 19 and 'T' in linekey and '-' in linekey and ':' in linekey
    else:  # microseconds
        return len(linekey) == 26 and 'T' in linekey and '-' in linekey and ':' in linekey

def serialize_linekey(linekey: LineKey, timespec: Optional[str] = None) -> str:
    """Serialize strings, datetimes at the requested precision, or other keys."""
    if isinstance(linekey, str):
        return linekey
    elif isinstance(linekey, dt.datetime):
        return linekey.isoformat(timespec=timespec or TIME_SPEC)
    return str(linekey)

def deserialize_linekey(linekey_str: str, default_format: Optional[str] = None) -> LineKey:
    """Convert an ISO key to datetime only when explicitly requested."""
    if default_format == "datetime":
        return dt.datetime.fromisoformat(linekey_str)
    return linekey_str

def detect_timespec(linekey: str) -> Optional[str]:
    """Identify parseable ISO keys at second or microsecond precision."""
    for spec in ('seconds', 'microseconds'):
        if _is_datetime_string(linekey, spec):
            try:
                dt.datetime.fromisoformat(linekey)
                return spec
            except ValueError:
                return None
    return None

def _store_with_key(result_dict: DataDict, linekey: str, value: dict,
                    auto_deserialize: bool, timespec: Optional[str] = None) -> LineKey:
    """Store a row and return its possibly converted key."""
    if auto_deserialize and _is_datetime_string(linekey, timespec):
        try:
            linekey = deserialize_linekey(linekey, "datetime")
        except ValueError:
            pass
    result_dict[linekey] = value
    return linekey


def _ordered_rows(rows, serialized_keys):
    """Check logical order once; retain original spellings of converted keys."""
    previous = None
    key_text = lambda key: serialized_keys.get(key, key)
    for text in map(key_text, rows) if serialized_keys else rows:
        if previous is not None and previous > text:
            return {key: rows[key] for key in sorted(rows, key=key_text if serialized_keys else None)}
        previous = text
    return rows


def _fast_dumps(obj: dict) -> bytes:
    """Serialize with NumPy support and a trailing newline."""
    return orjson.dumps(obj, option=orjson.OPT_SERIALIZE_NUMPY | orjson.OPT_APPEND_NEWLINE)

def save_jsonl(jsonl_file_path: str, db_dict: DataDict,
               timespec: Optional[str] = None, meta: Optional[dict] = None,
               slot_bytes: Optional[int] = None) -> None:
    """Rewrite rows in input order, preserve/configure the slot, and publish the index last."""
    _save_jsonl(jsonl_file_path, db_dict, timespec, meta, slot_bytes)

def _index_stats(path, index):
    """Measure the final effective keys without exposing the writer's index."""
    return dict(min_index=min(index, default=None), max_index=max(index, default=None),
                size=os.path.getsize(path), count=len(index))

def _save_jsonl(jsonl_file_path, db_dict, timespec=None, meta=None,
                slot_bytes=None, *, with_stats=False):
    """Internal save; statistics are requested only by metadata-maintenance callers."""
    _invalidate_index_cache(jsonl_file_path)
    index: IndexDict = {}
    info = _tabletimezone.inspect(jsonl_file_path)
    items = _tabletimezone.validate(db_dict, info, timespec, _validate_row_keys, serialize_linekey)
    placeholder, final_slot = _tabletimezone.save_slots(info, meta, slot_bytes)
    width = len(placeholder) if placeholder is not None else None

    try:
        byte_offset = width or 0
        with open(jsonl_file_path, 'wb', buffering=BUFFER_SIZE if db_dict or width else -1) as f:
            if placeholder is not None:
                # Reserve offsets; publish the final slot after flushing every row.
                f.write(placeholder)
            for linekey, data in items():
                serialized_key = serialize_linekey(linekey, timespec)
                line = _fast_dumps({serialized_key: data})
                f.write(line)
                index[serialized_key] = byte_offset
                byte_offset += len(line)

            if final_slot is not None:
                f.flush()
                f.seek(0)
                f.write(final_slot)

        _write_index(jsonl_file_path, index)

    except OSError as e:
        raise OSError(f"Failed to save JSONL file {jsonl_file_path}: {str(e)}")
    return _index_stats(jsonl_file_path, index) if with_stats else None

def save_jsonl_atomic(jsonl_file_path: str, db_dict: DataDict,
                      timespec: Optional[str] = None) -> None:
    """Atomically replace a protected control file; ordinary saves stay in place."""
    _invalidate_index_cache(jsonl_file_path)
    if _tabletimezone.read(jsonl_file_path) is not None:
        raise ValueError('atomic control-file save cannot replace a timezone-declared table')
    items = _validate_row_keys(db_dict, timespec, reuse=True)
    directory = os.path.dirname(os.path.abspath(jsonl_file_path))
    prefix = "." + os.path.basename(jsonl_file_path) + "."
    fd, tmp_path = tempfile.mkstemp(dir=directory, prefix=prefix, suffix=".tmp")
    index: IndexDict = {}
    try:
        byte_offset = 0
        with os.fdopen(fd, 'wb', buffering=BUFFER_SIZE) as f:
            for linekey, data in items():
                serialized_key = serialize_linekey(linekey, timespec)
                line = _fast_dumps({serialized_key: data})
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
    """Atomically resize a slot; an unchanged-width retry still rebuilds its index."""
    _invalidate_index_cache(jsonl_file_path)
    info = metaslot.inspect_file(jsonl_file_path)
    slot = metaslot._preserve_slot(info, slot_bytes)
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

def load_jsonl(jsonl_file_path: str, auto_deserialize: bool = True, timespec: Optional[str] = None, *, strict: bool = False) -> DataDict:
    """Stream all observations, apply strictness, and return ascending serialized-key order."""
    if not os.path.exists(jsonl_file_path):
        raise FileNotFoundError(f"JSONL file not found: {jsonl_file_path}")

    result_dict: DataDict = {}
    serialized_keys = {}

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
                    stored = _store_with_key(result_dict, linekey, value, auto_deserialize, timespec)
                    if stored != linekey:
                        serialized_keys[stored] = linekey
                except (orjson.JSONDecodeError, ValueError, TypeError) as error:
                    _read_failure(jsonl_file_path, offset, raw_line, error, strict)
                offset += len(raw_line)

        return _ordered_rows(result_dict, serialized_keys)

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


def select_jsonl(jsonl_file_path: str, lower_key: Optional[LineKey] = None, upper_key: Optional[LineKey] = None, auto_deserialize: bool = True, timespec: Optional[str] = None, *, strict: bool = False) -> DataDict:
    """Read an inclusive key range in key order; omitted bounds load all rows."""
    if lower_key is None and upper_key is None:
        return load_jsonl(jsonl_file_path, auto_deserialize, timespec, strict=strict)

    if lower_key is not None:
        lower_key = _tabletimezone.bound(jsonl_file_path, lower_key, timespec, serialize_linekey)
    if upper_key is not None:
        upper_key = _tabletimezone.bound(jsonl_file_path, upper_key, timespec, serialize_linekey)
    if lower_key == upper_key:
        return select_line_jsonl(jsonl_file_path, lower_key, auto_deserialize, timespec, strict=strict)

    try:
        index_dict, all_keys = _read_cached_index(jsonl_file_path, with_keys=True)

        if not index_dict:
            return {}

        if lower_key is None:
            lower_key = all_keys[0]   # index keys are stored sorted
        if upper_key is None:
            upper_key = all_keys[-1]

        lower_key = serialize_linekey(lower_key, timespec)
        upper_key = serialize_linekey(upper_key, timespec)
        lo = bisect_left(all_keys, lower_key)
        hi = bisect_right(all_keys, upper_key)
        selected_linekeys = all_keys[lo:hi]

        if strict:
            for key in selected_linekeys:
                _check_read_offset(jsonl_file_path, index_dict[key])
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
                except (orjson.JSONDecodeError, ValueError, KeyError, TypeError) as error:
                    _read_failure(jsonl_file_path, offset, line, error, strict)

        result_dict = {}
        for linekey in selected_linekeys:
            if linekey in raw_results:
                _store_with_key(result_dict, linekey, raw_results[linekey], auto_deserialize, timespec)
        return result_dict

    except OSError as e:
        raise OSError(f"Failed to select from JSONL file {jsonl_file_path}: {str(e)}")

def select_line_jsonl(
    jsonl_file_path: str,
    linekey: LineKey,
    auto_deserialize: bool = True,
    timespec: Optional[str] = None,
    *, strict: bool = False,
) -> DataDict:
    """Seek one indexed row; strict mode raises on encountered damage, never on absence."""
    linekey = _tabletimezone.key(jsonl_file_path, linekey, timespec, serialize_linekey)

    index_dict, _ = _read_cached_index(jsonl_file_path)

    if linekey not in index_dict:
        return {}

    result_dict: DataDict = {}

    with open(jsonl_file_path, 'rb', buffering=BUFFER_SIZE) as f:
        offset = index_dict[linekey]
        if strict:
            _check_read_offset(jsonl_file_path, offset)
        line = b''
        try:
            f.seek(offset)
            line = f.readline()
            parsed_key, value = _parse_row(line.strip())
            if parsed_key != linekey:
                raise ValueError("key mismatch")
            _store_with_key(result_dict, linekey, value, auto_deserialize, timespec)
        except (orjson.JSONDecodeError, ValueError, KeyError, TypeError) as error:
            _read_failure(jsonl_file_path, offset, line, error, strict)
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
    """Upsert rows in place when they fit, otherwise append and blank old rows."""
    _update_jsonl(jsonl_file_path, update_dict, timespec, meta)

def _update_jsonl(jsonl_file_path, update_dict, timespec=None, meta=None,
                  *, with_stats=False):
    """Internal upsert with an optional post-publication statistics result."""
    _invalidate_index_cache(jsonl_file_path)
    info = _tabletimezone.inspect(jsonl_file_path)
    items = _tabletimezone.validate(update_dict, info, timespec, _validate_row_keys, serialize_linekey)
    encoded_meta = _tabletimezone.metadata_bytes(jsonl_file_path, meta) if meta is not None else None

    try:
        index = load_index(jsonl_file_path)

        updates = []
        appends = []
        old_lines = []

        with open(jsonl_file_path, 'rb+', buffering=BUFFER_SIZE) as f:
            f.seek(0, os.SEEK_END)
            # Heal a missing trailing newline so appends start on a fresh line
            if f.tell() > 0:
                f.seek(-1, os.SEEK_END)
                if f.read(1) != b'\n':
                    f.write(b'\n')
            append_pos = f.tell()

            for linekey, data in items():
                linekey = serialize_linekey(linekey, timespec)
                new_line = _fast_dumps({linekey: data})

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

            for pos, line, old_len in updates:
                if len(line) < old_len:
                    # Padding preserves both the old length and terminal newline.
                    line = line[:-1] + b' ' * (old_len - len(line)) + b'\n'
                f.seek(pos)
                f.write(line)

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

        _write_index(jsonl_file_path, index)

    except OSError as e:
        raise OSError(f"Failed to update JSONL file {jsonl_file_path}: {str(e)}")
    return _index_stats(jsonl_file_path, index) if with_stats else None

def delete_jsonl(jsonl_file_path: str, linekeys: List[LineKey], timespec: Optional[str] = None) -> None:
    """Blank selected rows without moving others, then publish the reduced index."""
    _invalidate_index_cache(jsonl_file_path)
    timezone = _tabletimezone.read(jsonl_file_path)
    if timezone is not None:
        linekeys = [k for k, _ in _tabletimezone.prepare(
            {key: {} for key in linekeys}, timezone, timespec, serialize_linekey)()]
    else:
        _validate_row_keys({key: {} for key in linekeys}, timespec)
    try:
        index = load_index(jsonl_file_path)

        linekeys = [serialize_linekey(key, timespec) for key in linekeys]

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

        _write_index(jsonl_file_path, index)

    except OSError as e:
        raise OSError(f"Failed to delete from JSONL file {jsonl_file_path}: {str(e)}")


def read_jsonl_meta(jsonl_file_path: str):
    """Read the valid known-version record from line one, if present."""
    return metaslot.read_slot(jsonl_file_path)


def write_jsonl_meta(jsonl_file_path: str, meta: Optional[dict]) -> None:
    """Publish one slot-only metadata change, then mark the index fresh last."""
    _invalidate_index_cache(jsonl_file_path)
    _tabletimezone.metadata_bytes(jsonl_file_path, meta)  # Fit before index recovery.
    load_index(jsonl_file_path)
    metaslot.write_slot(jsonl_file_path, meta)
    os.utime(jsonl_file_path + '.idx', None)


def read_jsonl_timezone(jsonl_file_path: str):
    """Return the fixed table offset, independently of consumer metadata."""
    return _tabletimezone.read(jsonl_file_path)


def write_jsonl_timezone(jsonl_file_path: str, timezone: Optional[str]) -> None:
    """Set/remove timezone on an empty slotted table; equal offsets are a no-op."""
    encoded = _tabletimezone.configure_bytes(jsonl_file_path, timezone)
    if encoded is not None:
        _invalidate_index_cache(jsonl_file_path)
        _tabletimezone.publish(jsonl_file_path, encoded, load_index)
