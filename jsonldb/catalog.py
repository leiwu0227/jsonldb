"""Revisioned immutable FolderDB catalog primitives.

The filesystem is intentionally treated as untrusted here.  Decoded objects are
validated completely before immutable public values are constructed or cached.
"""

from __future__ import annotations

import errno
import hashlib
import os
import threading
import time
import uuid
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Callable, Dict, Mapping, Optional, Tuple

import orjson


CATALOG_SCHEMA = "jsonldb.folder-catalog"
PENDING_SCHEMA = "jsonldb.folder-catalog-pending"
CATALOG_VERSION = 2
LEGACY_CATALOG_VERSION = 1
PENDING_VERSION = 1
IGNORE_CONTENT = b"pending.json\nwriter.lock\n*.tmp\n"


class CatalogBusyError(RuntimeError):
    """The catalog could not become stable within the bounded timeout."""


class UnsupportedCatalogVersionError(RuntimeError):
    """A recognized catalog envelope uses a newer unsupported version."""


class CatalogFilesystemError(RuntimeError):
    """The filesystem cannot provide a required catalog guarantee."""


class CatalogRecoveryError(RuntimeError):
    """Locked recovery could not establish a trustworthy catalog."""


class CatalogChangedError(RuntimeError):
    """A family read could not remain bound to one managed generation."""


@dataclass(frozen=True, init=False)
class FolderCatalogEntry:
    __slots__ = (
        "min_index", "max_index", "count", "size", "linted", "lint_time",
        "aux_present", "aux_size", "aux_sha256",
    )

    min_index: Optional[str]
    max_index: Optional[str]
    count: int
    size: int
    linted: bool
    lint_time: str
    aux_present: bool
    aux_size: Optional[int]
    aux_sha256: Optional[str]

    def __init__(
        self,
        min_index: Optional[str],
        max_index: Optional[str],
        count: int,
        size: int,
        linted: bool,
        lint_time: str,
        aux_present: bool = False,
        aux_size: Optional[int] = None,
        aux_sha256: Optional[str] = None,
    ):
        object.__setattr__(self, "min_index", min_index)
        object.__setattr__(self, "max_index", max_index)
        object.__setattr__(self, "count", count)
        object.__setattr__(self, "size", size)
        object.__setattr__(self, "linted", linted)
        object.__setattr__(self, "lint_time", lint_time)
        object.__setattr__(self, "aux_present", aux_present)
        object.__setattr__(self, "aux_size", aux_size)
        object.__setattr__(self, "aux_sha256", aux_sha256)


@dataclass(frozen=True)
class TickerFamilyRead:
    __slots__ = ("name", "data", "aux", "catalog_id", "revision", "aux_sha256")

    name: str
    data: Mapping[Any, Dict[str, Any]]
    aux: Optional[bytes]
    catalog_id: str
    revision: int
    aux_sha256: Optional[str]


@dataclass(frozen=True)
class FolderCatalogSnapshot:
    __slots__ = (
        "schema", "version", "catalog_id", "revision", "last_transaction_id",
        "timespec", "file_identity", "entries",
    )

    schema: str
    version: int
    catalog_id: str
    revision: int
    last_transaction_id: str
    timespec: str
    file_identity: Tuple[Any, ...]
    entries: Mapping[str, FolderCatalogEntry]


@dataclass(frozen=True)
class PendingState:
    __slots__ = (
        "transaction_id", "base_catalog_id", "base_revision",
        "target_revision", "scope", "tickers",
    )

    transaction_id: str
    base_catalog_id: str
    base_revision: int
    target_revision: int
    scope: str
    tickers: Tuple[str, ...]


_cache_lock = threading.Lock()
_snapshot_cache: Dict[str, FolderCatalogSnapshot] = {}


def canonical_root(path: str) -> str:
    return os.path.normcase(os.path.realpath(os.path.abspath(path)))


def file_identity(path: str, stat_result: os.stat_result) -> Tuple[Any, ...]:
    return (
        os.path.realpath(path), stat_result.st_dev, stat_result.st_ino,
        stat_result.st_size, stat_result.st_mtime_ns, stat_result.st_ctime_ns,
    )


def cached_snapshot(root: str, identity: Tuple[Any, ...]) -> Optional[FolderCatalogSnapshot]:
    with _cache_lock:
        candidate = _snapshot_cache.get(root)
        if candidate is not None and candidate.file_identity == identity:
            return candidate
    return None


def install_snapshot(root: str, snapshot: FolderCatalogSnapshot) -> FolderCatalogSnapshot:
    with _cache_lock:
        current = _snapshot_cache.get(root)
        if current is not None and current.file_identity == snapshot.file_identity:
            return current
        _snapshot_cache[root] = snapshot
        return snapshot


def invalidate_snapshot(root: str) -> None:
    with _cache_lock:
        _snapshot_cache.pop(root, None)


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _canonical_uuid(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        return str(uuid.UUID(value)) == value
    except (ValueError, AttributeError):
        return False


def _exact_keys(value: Any, keys: set) -> bool:
    return isinstance(value, dict) and set(value) == keys


def aux_identity(path: str) -> Tuple[bool, Optional[int], Optional[str]]:
    """Return exact structural identity for one optional opaque companion."""
    if not os.path.lexists(path):
        return False, None, None
    digest = hashlib.sha256()
    size = 0
    with open(path, "rb") as stream:
        while True:
            chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            size += len(chunk)
            digest.update(chunk)
    return True, size, "sha256:" + digest.hexdigest()


def validate_catalog_envelope(
    value: Any,
    identity: Tuple[Any, ...],
    name_validator: Callable[[str], bool],
) -> FolderCatalogSnapshot:
    if isinstance(value, dict) and value.get("schema") == CATALOG_SCHEMA:
        version = value.get("version")
        if _is_int(version) and version > CATALOG_VERSION:
            raise UnsupportedCatalogVersionError(
                "catalog uses unsupported newer version {} at {}".format(version, identity[0])
            )
    keys = {
        "schema", "version", "catalog_id", "revision", "last_transaction_id",
        "timespec", "entries",
    }
    if not _exact_keys(value, keys):
        raise ValueError("catalog envelope has an invalid top-level shape")
    version = value["version"]
    if value["schema"] != CATALOG_SCHEMA or not _is_int(version) or version not in (
        LEGACY_CATALOG_VERSION, CATALOG_VERSION,
    ):
        raise ValueError("catalog envelope has an unsupported schema or version")
    if not _canonical_uuid(value["catalog_id"]):
        raise ValueError("catalog_id is not a canonical UUID")
    if not _canonical_uuid(value["last_transaction_id"]):
        raise ValueError("last_transaction_id is not a canonical UUID")
    if not _is_int(value["revision"]) or value["revision"] < 1:
        raise ValueError("revision must be a positive integer")
    if value["timespec"] not in ("seconds", "microseconds"):
        raise ValueError("timespec is invalid")
    if not isinstance(value["entries"], dict):
        raise ValueError("entries must be an object")

    entry_keys = {"min_index", "max_index", "count", "size", "linted", "lint_time"}
    if version == CATALOG_VERSION:
        entry_keys |= {"aux_present", "aux_size", "aux_sha256"}
    entries: Dict[str, FolderCatalogEntry] = {}
    previous_name = None
    for name, raw in value["entries"].items():
        if not isinstance(name, str) or not name_validator(name):
            raise ValueError("catalog contains an invalid ticker name")
        if previous_name is not None and name <= previous_name:
            raise ValueError("catalog entries are not in logical-name order")
        previous_name = name
        if not _exact_keys(raw, entry_keys):
            raise ValueError("catalog entry has an invalid shape")
        count = raw["count"]
        size = raw["size"]
        lower = raw["min_index"]
        upper = raw["max_index"]
        if not _is_int(count) or count < 0 or not _is_int(size) or size < 0:
            raise ValueError("catalog entry count or size is invalid")
        if not isinstance(raw["linted"], bool) or not isinstance(raw["lint_time"], str):
            raise ValueError("catalog entry lint state is invalid")
        aux_present = raw.get("aux_present", False)
        aux_size = raw.get("aux_size")
        aux_sha256 = raw.get("aux_sha256")
        if version == CATALOG_VERSION:
            valid_digest = (
                isinstance(aux_sha256, str)
                and len(aux_sha256) == 71
                and aux_sha256.startswith("sha256:")
                and all(character in "0123456789abcdef" for character in aux_sha256[7:])
            )
            if not isinstance(aux_present, bool):
                raise ValueError("catalog entry AUX presence is invalid")
            if aux_present:
                if not _is_int(aux_size) or aux_size < 0 or not valid_digest:
                    raise ValueError("catalog entry AUX identity is invalid")
            elif aux_size is not None or aux_sha256 is not None:
                raise ValueError("absent catalog AUX identity must be null")
        if count == 0:
            if lower is not None or upper is not None:
                raise ValueError("empty catalog entry must have null boundaries")
        elif (
            not isinstance(lower, str) or not isinstance(upper, str) or lower > upper
        ):
            raise ValueError("non-empty catalog entry has invalid boundaries")
        entries[name] = FolderCatalogEntry(
            lower, upper, count, size, raw["linted"], raw["lint_time"],
            aux_present, aux_size, aux_sha256,
        )
    return FolderCatalogSnapshot(
        value["schema"], value["version"], value["catalog_id"],
        value["revision"], value["last_transaction_id"], value["timespec"],
        identity, MappingProxyType(entries),
    )


def validate_pending_envelope(
    value: Any, name_validator: Callable[[str], bool]
) -> PendingState:
    if isinstance(value, dict) and value.get("schema") == PENDING_SCHEMA:
        version = value.get("version")
        if _is_int(version) and version > PENDING_VERSION:
            raise UnsupportedCatalogVersionError(
                "pending state uses unsupported newer version {}".format(version)
            )
    keys = {
        "schema", "version", "transaction_id", "base_catalog_id",
        "base_revision", "target_revision", "scope", "tickers",
    }
    if not _exact_keys(value, keys):
        raise ValueError("pending envelope has an invalid top-level shape")
    if (
        value["schema"] != PENDING_SCHEMA
        or not _is_int(value["version"])
        or value["version"] != PENDING_VERSION
    ):
        raise ValueError("pending envelope has an unsupported schema or version")
    if not _canonical_uuid(value["transaction_id"]) or not _canonical_uuid(value["base_catalog_id"]):
        raise ValueError("pending IDs are not canonical UUIDs")
    base = value["base_revision"]
    target = value["target_revision"]
    if not _is_int(base) or base < 1 or not _is_int(target) or target != base + 1:
        raise ValueError("pending revisions are invalid")
    if value["scope"] not in ("tickers", "full") or not isinstance(value["tickers"], list):
        raise ValueError("pending scope is invalid")
    tickers = value["tickers"]
    if value["scope"] == "full":
        if tickers:
            raise ValueError("full pending scope must have no tickers")
    elif (
        not tickers
        or tickers != sorted(set(tickers))
        or any(not isinstance(name, str) or not name_validator(name) for name in tickers)
    ):
        raise ValueError("ticker pending scope is invalid")
    return PendingState(
        value["transaction_id"], value["base_catalog_id"], base, target,
        value["scope"], tuple(tickers),
    )


def snapshot_envelope(snapshot: FolderCatalogSnapshot) -> Dict[str, Any]:
    return catalog_envelope(
        snapshot.catalog_id, snapshot.revision, snapshot.last_transaction_id,
        snapshot.timespec, snapshot.entries, version=snapshot.version,
    )


def catalog_envelope(
    catalog_id: str,
    revision: int,
    transaction_id: str,
    timespec: str,
    entries: Mapping[str, FolderCatalogEntry],
    version: int = CATALOG_VERSION,
) -> Dict[str, Any]:
    if version not in (LEGACY_CATALOG_VERSION, CATALOG_VERSION):
        raise ValueError("catalog envelope version is invalid")
    serialized_entries = {}
    for name, entry in sorted(entries.items()):
        raw = {
            "min_index": entry.min_index,
            "max_index": entry.max_index,
            "count": entry.count,
            "size": entry.size,
            "linted": entry.linted,
            "lint_time": entry.lint_time,
        }
        if version == CATALOG_VERSION:
            raw.update({
                "aux_present": entry.aux_present,
                "aux_size": entry.aux_size,
                "aux_sha256": entry.aux_sha256,
            })
        serialized_entries[name] = raw
    return {
        "schema": CATALOG_SCHEMA,
        "version": version,
        "catalog_id": catalog_id,
        "revision": revision,
        "last_transaction_id": transaction_id,
        "timespec": timespec,
        "entries": serialized_entries,
    }


def pending_envelope(state: PendingState) -> Dict[str, Any]:
    return {
        "schema": PENDING_SCHEMA,
        "version": PENDING_VERSION,
        "transaction_id": state.transaction_id,
        "base_catalog_id": state.base_catalog_id,
        "base_revision": state.base_revision,
        "target_revision": state.target_revision,
        "scope": state.scope,
        "tickers": list(state.tickers),
    }


def read_object(path: str) -> Any:
    try:
        with open(path, "rb") as stream:
            return orjson.loads(stream.read())
    except UnsupportedCatalogVersionError:
        raise
    except Exception as exc:
        raise ValueError("unable to decode managed state at {}".format(path)) from exc


def sync_directory(path: str) -> None:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    try:
        descriptor = os.open(path, flags)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except OSError as exc:
        raise CatalogFilesystemError("directory sync unavailable at {}".format(path)) from exc


def atomic_write(path: str, value: Any) -> None:
    directory = os.path.dirname(path)
    temporary = os.path.join(directory, ".{}.{}.tmp".format(os.path.basename(path), uuid.uuid4()))
    payload = orjson.dumps(value, option=orjson.OPT_SORT_KEYS)
    try:
        with open(temporary, "xb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        sync_directory(directory)
    except CatalogFilesystemError:
        raise
    except OSError as exc:
        raise CatalogFilesystemError("durable replacement failed at {}".format(path)) from exc
    finally:
        try:
            if os.path.exists(temporary):
                os.unlink(temporary)
        except OSError:
            pass


def atomic_write_bytes(path: str, payload: bytes) -> None:
    directory = os.path.dirname(path)
    temporary = os.path.join(directory, ".{}.{}.tmp".format(os.path.basename(path), uuid.uuid4()))
    try:
        with open(temporary, "xb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        sync_directory(directory)
    except CatalogFilesystemError:
        raise
    except OSError as exc:
        raise CatalogFilesystemError("durable replacement failed at {}".format(path)) from exc
    finally:
        try:
            if os.path.exists(temporary):
                os.unlink(temporary)
        except OSError:
            pass


def clear_pending(path: str) -> None:
    try:
        os.unlink(path)
        sync_directory(os.path.dirname(path))
    except FileNotFoundError:
        return
    except OSError as exc:
        raise CatalogFilesystemError("unable to durably clear pending state at {}".format(path)) from exc


class WriterLock:
    """Fail-closed POSIX/Windows exclusive advisory lock."""

    def __init__(self, path: str, timeout_seconds: float):
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)):
            raise TypeError("timeout_seconds must be a number")
        if timeout_seconds < 0:
            raise ValueError("timeout_seconds must be non-negative")
        self.path = path
        self.timeout_seconds = float(timeout_seconds)
        self._stream = None

    def __enter__(self) -> "WriterLock":
        try:
            self._stream = open(self.path, "a+b")
            self._stream.seek(0, os.SEEK_END)
            if self._stream.tell() == 0:
                self._stream.write(b"\0")
                self._stream.flush()
                os.fsync(self._stream.fileno())
        except OSError as exc:
            raise CatalogFilesystemError("writer lock unavailable at {}".format(self.path)) from exc

        deadline = time.monotonic() + self.timeout_seconds
        delay = 0.01
        while True:
            try:
                self._lock_nonblocking()
                return self
            except OSError as exc:
                if not self._is_contention(exc):
                    self._stream.close()
                    self._stream = None
                    raise CatalogFilesystemError(
                        "writer locking unsupported at {}".format(self.path)
                    ) from exc
                if time.monotonic() >= deadline:
                    self._stream.close()
                    self._stream = None
                    raise CatalogBusyError(
                        "writer lock remained busy at {}".format(self.path)
                    )
                time.sleep(min(delay, max(0.0, deadline - time.monotonic())))
                delay = min(0.1, delay * 2)

    def _lock_nonblocking(self) -> None:
        if os.name == "posix":
            import fcntl
            fcntl.flock(self._stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        elif os.name == "nt":
            import msvcrt
            self._stream.seek(0)
            msvcrt.locking(self._stream.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            raise OSError(errno.ENOSYS, "unsupported locking platform")

    @staticmethod
    def _is_contention(exc: OSError) -> bool:
        return exc.errno in (errno.EACCES, errno.EAGAIN, errno.EDEADLK)

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if self._stream is None:
            return
        try:
            if os.name == "posix":
                import fcntl
                fcntl.flock(self._stream.fileno(), fcntl.LOCK_UN)
            elif os.name == "nt":
                import msvcrt
                self._stream.seek(0)
                msvcrt.locking(self._stream.fileno(), msvcrt.LK_UNLCK, 1)
        finally:
            self._stream.close()
            self._stream = None
