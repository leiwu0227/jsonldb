"""
A simple file-based database that stores data in JSONL format.
Each table is stored in a separate JSONL file.
"""

import os
import hashlib
import shutil
import time
import uuid
import pandas as pd
from contextlib import contextmanager
from typing import Dict, List, Union, Optional, Any
from datetime import datetime
from jsonldb.jsonlfile import (
    save_jsonl, load_jsonl, select_jsonl, update_jsonl, delete_jsonl,
    lint_jsonl, build_jsonl_index, select_line_jsonl, serialize_linekey,
    detect_timespec
)
from jsonldb.jsonldf import (
    save_jsonldf, load_jsonldf, update_jsonldf, select_jsonldf, delete_jsonldf
)
import jsonldb.jsonlfile as jsonlfile
from jsonldb.catalog import (
    CATALOG_VERSION, CatalogBusyError, CatalogChangedError,
    CatalogFilesystemError, CatalogRecoveryError, FolderCatalogEntry,
    FolderCatalogSnapshot, PendingState, TickerFamilyRead,
    UnsupportedCatalogVersionError, WriterLock, atomic_write,
    atomic_write_bytes, aux_identity, cached_snapshot, canonical_root, catalog_envelope,
    clear_pending, file_identity, install_snapshot, invalidate_snapshot,
    pending_envelope, read_object, sync_directory, validate_catalog_envelope,
    validate_pending_envelope,
)

# Version control (gitpython) is imported lazily inside commit/revert/version
# so that importing FolderDB does not load git.

class FolderDB:
    """
    A simple file-based database that stores data in JSONL format.
    Each table is stored in a separate JSONL file.
    """
    
    # =============== Core/Initialization ===============
    def __init__(self, folder_path: str,hierarchy_depth: int = None):
        """
        Initialize the database.
        
        Args:
            folder_path: Path to the folder where the database files will be stored
            
        Raises:
            FileNotFoundError: If the folder doesn't exist
        """
        self.folder_path = folder_path
        if not os.path.exists(folder_path):
            raise FileNotFoundError(f"Folder not found: {folder_path}")

        self.use_hierarchy = False
        self.delimiter = '.'

        # Initialize all paths first
        self.hmeta_path = os.path.join(folder_path, "h.meta")
        self.dbmeta_path = os.path.join(folder_path, "db.meta")
        self.configmeta_path = os.path.join(folder_path, "config.meta")
        self.invalid_tickers_path = os.path.join(folder_path, ".invalid_tickers")
        self._catalog_root = canonical_root(folder_path)
        self.control_path = os.path.join(folder_path, ".jsonldb")
        self.catalog_path = os.path.join(self.control_path, "catalog.json")
        self.pending_path = os.path.join(self.control_path, "pending.json")
        self.writer_lock_path = os.path.join(self.control_path, "writer.lock")
        self.control_ignore_path = os.path.join(self.control_path, ".gitignore")
        self._active_catalog_transaction = None
        self._catalog_lint_overrides = {}
        self._catalog_entry_deletions = set()
        self.timespec = jsonlfile.TIME_SPEC

        if os.path.exists(self.configmeta_path):
            config_meta = select_jsonl(self.configmeta_path)
            if config_meta.get("timespec"):
                self.timespec = config_meta["timespec"]
            else:
                self._write_configmeta()
        else:
            self._write_configmeta()

        if os.path.exists(self.hmeta_path):
            hmeta = select_jsonl(self.hmeta_path)
            self.use_hierarchy = hmeta["use_hierarchy"]
            self.delimiter = hmeta["delimiter"]
            self.hierarchy_depth = hmeta["hierarchy_depth"]

            if hierarchy_depth is not None and self.hierarchy_depth != hierarchy_depth: 
                #current hierarchy depth is not the same as the one provided, so we need to lint the hierarchy
                self.lint_hierarchy(hierarchy_depth)
        else:
            #no h.meta file found, so we need to build it
            if hierarchy_depth is not None:
                self.use_hierarchy = True 
                self.hierarchy_depth = hierarchy_depth
                self.lint_hierarchy(hierarchy_depth)

        # Catalog migration, projection healing, and precision reconciliation
        # are deliberately deferred to load_catalog_snapshot() or get_dbmeta().
        # Construction therefore never discovers owners or loads ticker indexes.
        # A pre-catalog database retains the supported precision-healing behavior
        # from earlier releases; managed databases never pay for this scan.
        if not os.path.exists(self.catalog_path):
            self._heal_legacy_timespec_on_open()

    def build_hmeta(self) -> None:
        """
        Save the folder information to a file.
        """
        with self._catalog_transaction("full"):
            self._write_hmeta()

    def _write_hmeta(self) -> None:
        if self.use_hierarchy:
            hierachy_info= {
                "use_hierarchy": self.use_hierarchy,
                "delimiter": self.delimiter,
                "hierarchy_depth": self.hierarchy_depth
            }
            save_jsonl(self.hmeta_path, hierachy_info)


    def build_configmeta(self) -> None:
        """
        Save the folder information to a file.
        """
        with self._catalog_transaction("full"):
            self._write_configmeta()

    def _write_configmeta(self) -> None:
        config_info = {
            "timespec": self.timespec
        }
        save_jsonl(self.configmeta_path, config_info)

    def _detect_data_timespec(self) -> Optional[str]:
        """Stage 1 trigger: candidate precision from db.meta boundary keys.

        Returns a precision only when a datetime-like min/max key disagrees
        with self.timespec; None when boundaries look healthy or db.meta is
        missing. Costs no I/O beyond one db.meta read.
        """
        if not os.path.exists(self.dbmeta_path):
            return None
        metadata = select_jsonl(self.dbmeta_path)
        for info in metadata.values():
            if not isinstance(info, dict):
                continue
            for key in (info.get("min_index"), info.get("max_index")):
                if isinstance(key, str):
                    spec = detect_timespec(key)
                    if spec and spec != self.timespec:
                        return spec
        return None

    def _scan_index_timespecs(self) -> set:
        """Stage 2 confirmation: datetime-key precisions across every .idx file.

        Example: {'microseconds'} for a uniformly contaminated database,
        {'seconds', 'microseconds'} for mixed data, set() for string keys only.
        """
        found = set()
        for name in self.get_file_list():
            file_path = self._get_file_path(name)
            if not os.path.exists(file_path + '.idx'):
                continue
            index = jsonlfile.load_index(file_path)  # heals empty/corrupt
            for key in index:
                spec = detect_timespec(key)
                if spec:
                    found.add(spec)
        return found

    def _heal_legacy_timespec_on_open(self) -> None:
        found = set()
        boundary_mismatch = False
        for name in self.get_file_list():
            owner = self._get_file_path(name)
            if not os.path.isfile(owner):
                continue
            index = jsonlfile.load_index(owner)
            keys = list(index)
            for boundary in (keys[0], keys[-1]) if keys else ():
                detected = detect_timespec(boundary)
                if detected and detected != self.timespec:
                    boundary_mismatch = True
            if boundary_mismatch:
                for key in keys:
                    detected = detect_timespec(key)
                    if detected:
                        found.add(detected)
        if not boundary_mismatch:
            return
        # Complete the scan after a late trigger so earlier owners are included.
        found = self._scan_index_timespecs()
        if len(found) == 1:
            detected = next(iter(found))
            if detected != self.timespec:
                print(f"WARNING: config.meta timespec '{self.timespec}' does not match "
                      f"data ('{detected}'); auto-correcting config.meta")
                self.timespec = detected
                self._write_configmeta()
        elif len(found) > 1:
            print(f"WARNING: mixed datetime key precisions {sorted(found)} found in "
                  f"{self.folder_path}; keeping timespec '{self.timespec}'")

    # =============== Revisioned catalog ===============

    def _catalog_name_valid(self, name: str) -> bool:
        if not name or "\0" in name or os.path.isabs(name):
            return False
        if name == ".jsonldb":
            return False
        separators = [separator for separator in (os.sep, os.altsep, "/", "\\") if separator]
        if any(separator in name for separator in separators):
            return False
        return self.validate_name(name)

    def _ensure_control_namespace(self) -> None:
        created = not os.path.exists(self.control_path)
        try:
            os.makedirs(self.control_path, exist_ok=True)
            if created:
                sync_directory(self.folder_path)
        except OSError as exc:
            raise CatalogFilesystemError(
                f"catalog control directory unavailable at {self.control_path}"
            ) from exc
        expected = b"pending.json\nwriter.lock\n*.tmp\n"
        try:
            current = None
            if os.path.exists(self.control_ignore_path):
                with open(self.control_ignore_path, "rb") as stream:
                    current = stream.read()
            if current != expected:
                atomic_write_bytes(self.control_ignore_path, expected)
        except CatalogFilesystemError:
            raise
        except OSError as exc:
            raise CatalogFilesystemError(
                f"catalog ignore file unavailable at {self.control_ignore_path}"
            ) from exc

    def _read_valid_catalog(self) -> FolderCatalogSnapshot:
        before = os.stat(self.catalog_path)
        identity = file_identity(self.catalog_path, before)
        cached = cached_snapshot(self._catalog_root, identity)
        if cached is not None:
            return cached
        raw = read_object(self.catalog_path)
        after = os.stat(self.catalog_path)
        after_identity = file_identity(self.catalog_path, after)
        if identity != after_identity:
            raise CatalogBusyError(f"catalog changed while reading {self.catalog_path}")
        snapshot = validate_catalog_envelope(raw, identity, self._catalog_name_valid)
        return install_snapshot(self._catalog_root, snapshot)

    def load_catalog_snapshot(
        self, timeout_seconds: float = 5.0
    ) -> FolderCatalogSnapshot:
        """Return one fully validated immutable managed metadata generation."""
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)):
            raise TypeError("timeout_seconds must be a number")
        if timeout_seconds < 0:
            raise ValueError("timeout_seconds must be non-negative")
        deadline = time.monotonic() + float(timeout_seconds)
        last_error = None
        while True:
            if os.path.exists(self.pending_path) or not os.path.exists(self.catalog_path):
                return self._recover_catalog(max(0.0, deadline - time.monotonic()))
            try:
                before = os.stat(self.catalog_path)
                identity = file_identity(self.catalog_path, before)
                cached = cached_snapshot(self._catalog_root, identity)
                if cached is not None:
                    if not os.path.exists(self.pending_path):
                        if cached.version < CATALOG_VERSION:
                            return self._recover_catalog(
                                max(0.0, deadline - time.monotonic())
                            )
                        self.timespec = cached.timespec
                        return cached
                    continue
                raw = read_object(self.catalog_path)
                after = os.stat(self.catalog_path)
                if identity != file_identity(self.catalog_path, after):
                    raise CatalogBusyError("catalog changed during read")
                snapshot = validate_catalog_envelope(raw, identity, self._catalog_name_valid)
                if os.path.exists(self.pending_path):
                    continue
                if snapshot.version < CATALOG_VERSION:
                    return self._recover_catalog(
                        max(0.0, deadline - time.monotonic())
                    )
                snapshot = install_snapshot(self._catalog_root, snapshot)
                self.timespec = snapshot.timespec
                return snapshot
            except UnsupportedCatalogVersionError:
                raise
            except (OSError, ValueError) as exc:
                last_error = exc
                return self._recover_catalog(max(0.0, deadline - time.monotonic()))
            except CatalogBusyError as exc:
                last_error = exc
            if time.monotonic() >= deadline:
                raise CatalogBusyError(
                    f"catalog did not become stable at {self.catalog_path}"
                ) from last_error
            time.sleep(min(0.01, max(0.0, deadline - time.monotonic())))

    def _recover_catalog(self, timeout_seconds: float) -> FolderCatalogSnapshot:
        self._ensure_control_namespace()
        with WriterLock(self.writer_lock_path, timeout_seconds):
            try:
                return self._recover_locked()
            except (UnsupportedCatalogVersionError, CatalogFilesystemError):
                raise
            except Exception as exc:
                raise CatalogRecoveryError(
                    f"catalog recovery failed at {self.control_path}"
                ) from exc

    def _try_catalog_locked(self) -> Optional[FolderCatalogSnapshot]:
        if not os.path.exists(self.catalog_path):
            return None
        return self._read_valid_catalog()

    def _try_pending_locked(self) -> Optional[PendingState]:
        if not os.path.exists(self.pending_path):
            return None
        return validate_pending_envelope(
            read_object(self.pending_path), self._catalog_name_valid
        )

    def _projection_lint_state(self) -> Dict[str, Dict[str, Any]]:
        if not os.path.exists(self.dbmeta_path):
            return {}
        try:
            value = load_jsonl(self.dbmeta_path, auto_deserialize=False)
            return value if isinstance(value, dict) else {}
        except (OSError, ValueError, TypeError):
            return {}

    def _catalog_entry(
        self, name: str, previous: Optional[FolderCatalogEntry] = None,
        projection: Optional[Dict[str, Any]] = None,
        timespecs: Optional[set] = None,
    ) -> FolderCatalogEntry:
        state = (projection or {}).get(name, {})
        linted = state.get("linted", previous.linted if previous else False)
        lint_time = state.get("lint_time", previous.lint_time if previous else "")
        if name in self._catalog_lint_overrides:
            linted, lint_time = self._catalog_lint_overrides[name]
        raw = self._make_meta_entry(
            name, self._get_file_path(name), bool(linted),
            lint_time if isinstance(lint_time, str) else "",
            timespecs,
        )
        aux_present, aux_size, aux_sha256 = aux_identity(
            jsonlfile.get_aux_path(self._get_file_path(name))
        )
        return FolderCatalogEntry(
            raw["min_index"], raw["max_index"], raw["count"], raw["size"],
            raw["linted"], raw["lint_time"], aux_present, aux_size, aux_sha256,
        )

    def _with_aux_identities(
        self, entries: Dict[str, FolderCatalogEntry]
    ) -> Dict[str, FolderCatalogEntry]:
        """Add current companion identity without touching owners or indexes."""
        identified = {}
        for name, entry in entries.items():
            present, size, digest = aux_identity(
                jsonlfile.get_aux_path(self._get_file_path(name))
            )
            identified[name] = FolderCatalogEntry(
                entry.min_index, entry.max_index, entry.count, entry.size,
                entry.linted, entry.lint_time, present, size, digest,
            )
        return identified

    def _reconcile_entries(
        self, base: Optional[FolderCatalogSnapshot] = None,
        tickers: Optional[List[str]] = None,
    ) -> Dict[str, FolderCatalogEntry]:
        projection = self._projection_lint_state()
        previous = dict(base.entries) if base is not None else {}
        if tickers is None:
            entries = {}
            found_timespecs = set()
            for name in sorted(self.get_file_list()):
                if not self._catalog_name_valid(name):
                    raise CatalogRecoveryError(
                        f"invalid managed ticker name discovered under {self.folder_path}"
                    )
                owner = self._get_file_path(name)
                if not os.path.exists(owner + ".idx"):
                    build_jsonl_index(owner)
                entries[name] = self._catalog_entry(
                    name, previous.get(name), projection, found_timespecs
                )
            if len(found_timespecs) == 1:
                detected = next(iter(found_timespecs))
                if detected != self.timespec:
                    print(f"WARNING: config.meta timespec '{self.timespec}' does not match "
                          f"data ('{detected}'); auto-correcting config.meta")
                    self.timespec = detected
                    self._write_configmeta()
            elif len(found_timespecs) > 1:
                print(f"WARNING: mixed datetime key precisions {sorted(found_timespecs)} found in "
                      f"{self.folder_path}; keeping timespec '{self.timespec}'")
            return entries

        entries = previous
        for name in sorted(set(tickers)):
            if name in self._catalog_entry_deletions:
                entries.pop(name, None)
                continue
            owner = self._get_file_path(name)
            if os.path.isfile(owner):
                if not os.path.exists(owner + ".idx"):
                    build_jsonl_index(owner)
                entries[name] = self._catalog_entry(name, previous.get(name), projection)
            else:
                entries.pop(name, None)
        return entries

    def _write_projection(self, entries: Dict[str, FolderCatalogEntry]) -> None:
        if not entries:
            with open(self.dbmeta_path, "wb") as stream:
                stream.write(b"\n")
            try:
                os.unlink(self.dbmeta_path + ".idx")
            except FileNotFoundError:
                pass
            return
        projection = {
            name: {
                "name": name,
                "path": self._get_file_path(name),
                "min_index": entry.min_index,
                "max_index": entry.max_index,
                "size": entry.size,
                "count": entry.count,
                "lint_time": entry.lint_time,
                "linted": entry.linted,
            }
            for name, entry in sorted(entries.items())
        }
        save_jsonl(self.dbmeta_path, projection, self.timespec)

    def _publish_catalog(
        self, catalog_id: str, revision: int, transaction_id: str,
        entries: Dict[str, FolderCatalogEntry],
    ) -> FolderCatalogSnapshot:
        envelope = catalog_envelope(
            catalog_id, revision, transaction_id, self.timespec, entries
        )
        atomic_write(self.catalog_path, envelope)
        invalidate_snapshot(self._catalog_root)
        return self._read_valid_catalog()

    def _new_lineage(self, entries: Dict[str, FolderCatalogEntry]) -> FolderCatalogSnapshot:
        return self._publish_catalog(
            str(uuid.uuid4()), 1, str(uuid.uuid4()), entries
        )

    def _migrate_catalog_locked(
        self, catalog: FolderCatalogSnapshot
    ) -> FolderCatalogSnapshot:
        """Publish v2 in the existing lineage using only cataloged AUX paths."""
        if catalog.version == CATALOG_VERSION:
            return catalog
        transaction_id = str(uuid.uuid4())
        pending = PendingState(
            transaction_id, catalog.catalog_id, catalog.revision,
            catalog.revision + 1, "full", (),
        )
        atomic_write(self.pending_path, pending_envelope(pending))
        entries = self._with_aux_identities(dict(catalog.entries))
        migrated = self._publish_catalog(
            catalog.catalog_id, pending.target_revision, transaction_id, entries,
        )
        clear_pending(self.pending_path)
        return migrated

    def _recover_locked(self) -> FolderCatalogSnapshot:
        pending = None
        pending_malformed = False
        try:
            pending = self._try_pending_locked()
        except UnsupportedCatalogVersionError:
            raise
        except (OSError, ValueError, TypeError):
            pending_malformed = True

        catalog = None
        catalog_invalid = False
        try:
            catalog = self._try_catalog_locked()
        except UnsupportedCatalogVersionError:
            raise
        except (OSError, ValueError, TypeError, CatalogBusyError):
            catalog_invalid = True

        if pending is None and not pending_malformed and catalog is not None:
            self.timespec = catalog.timespec
            if catalog.version < CATALOG_VERSION:
                return self._migrate_catalog_locked(catalog)
            return catalog

        if pending is not None and catalog is not None:
            if (
                catalog.catalog_id == pending.base_catalog_id
                and catalog.revision == pending.target_revision
                and catalog.last_transaction_id == pending.transaction_id
            ):
                clear_pending(self.pending_path)
                invalidate_snapshot(self._catalog_root)
                self.timespec = catalog.timespec
                return self._read_valid_catalog()
            if (
                catalog.catalog_id == pending.base_catalog_id
                and catalog.revision == pending.base_revision
            ):
                self.timespec = catalog.timespec
                names = list(pending.tickers) if pending.scope == "tickers" else None
                entries = self._reconcile_entries(catalog, names)
                if catalog.version < CATALOG_VERSION:
                    entries = self._with_aux_identities(entries)
                self._write_projection(entries)
                recovered = self._publish_catalog(
                    catalog.catalog_id, pending.target_revision,
                    pending.transaction_id, entries,
                )
                clear_pending(self.pending_path)
                return recovered

        entries = self._reconcile_entries(None, None)
        self._write_projection(entries)
        recovered = self._new_lineage(entries)
        if os.path.exists(self.pending_path):
            clear_pending(self.pending_path)
        return recovered

    @contextmanager
    def _catalog_transaction(
        self, scope: str, tickers: Optional[List[str]] = None,
        timeout_seconds: float = 5.0,
    ):
        requested = tuple(sorted(set(tickers or [])))
        if scope not in ("tickers", "full"):
            raise ValueError("catalog transaction scope is invalid")
        if scope == "tickers" and (
            not requested or any(not self._catalog_name_valid(name) for name in requested)
        ):
            for name in requested:
                if not self.validate_name(name):
                    self._get_file_path(name)
            raise ValueError("ticker transaction requires valid affected names")
        active = self._active_catalog_transaction
        if active is not None:
            active_scope, active_tickers = active
            if active_scope != "full" and (
                scope == "full" or not set(requested).issubset(active_tickers)
            ):
                raise CatalogRecoveryError("nested mutation exceeds its pending scope")
            yield
            return

        self._ensure_control_namespace()
        with WriterLock(self.writer_lock_path, timeout_seconds):
            base = self._recover_locked()
            transaction_id = str(uuid.uuid4())
            pending = PendingState(
                transaction_id, base.catalog_id, base.revision, base.revision + 1,
                scope, requested if scope == "tickers" else (),
            )
            atomic_write(self.pending_path, pending_envelope(pending))
            self._active_catalog_transaction = (scope, set(requested))
            self._catalog_lint_overrides = {}
            self._catalog_entry_deletions = set()
            try:
                yield
                names = list(requested) if scope == "tickers" else None
                entries = self._reconcile_entries(base, names)
                self._write_projection(entries)
                self._publish_catalog(
                    base.catalog_id, pending.target_revision,
                    transaction_id, entries,
                )
                clear_pending(self.pending_path)
            finally:
                self._active_catalog_transaction = None
                self._catalog_lint_overrides = {}
                self._catalog_entry_deletions = set()

    @contextmanager
    def _catalog_writer(self, timeout_seconds: float = 5.0):
        self._ensure_control_namespace()
        with WriterLock(self.writer_lock_path, timeout_seconds):
            yield self._recover_locked()


    def validate_name(self, name: str) -> bool:
        """
        Validate a file name according to the current mode.
        
        Args:
            name: Name of the file to validate
            
        Returns:
            bool: True if the name is valid, False otherwise
        """
        if not self.use_hierarchy:
            return True
            
        # Remove .jsonl extension if present
        if name.endswith('.jsonl'):
            name = name[:-6]
            
        # Count delimiters
        delimiter_count = name.count(self.delimiter)
        
        # In hierarchy mode, name must contain at least hierarchy_depth-1 delimiters
        # e.g., for depth=3: "users.level1.level2" has 2 delimiters
        return delimiter_count >= self.hierarchy_depth - 1


    def __str__(self) -> str:
        """Return a string representation of the database."""
        result = f"FolderDB at {self.folder_path}\n"
        result += "-" * 50 + "\n"
        
        # Get metadata
        if os.path.exists(self.dbmeta_path):
            metadata = select_jsonl(self.dbmeta_path)
            result += f"Found {len(metadata)} JSONL files\n\n"
            
            for name, info in metadata.items():
                result += f"{name}:\n"
                result += f"  Size: {info['size']} bytes\n"
                result += f"  Count: {info['count']}\n"
                result += f"  Key range: {info['min_index']} to {info['max_index']}\n"
                result += f"  Linted: {info['linted']}\n\n"
        
        return result
    
    def __repr__(self) -> str:
        return self.__str__()

    # =============== File Path Management ===============

    def _get_hierarchy_path(self, name: str) -> str:
        """
        Get the hierarchical path for a file.
        
        Args:
            name: Name of the file (with or without .jsonl extension)
            
        Returns:
            str: Full path to the directory where the file should be stored
        """
        if self.use_hierarchy:
            # Take first hierarchy_depth parts for the path
            parts = name.split(self.delimiter)[:self.hierarchy_depth]
            return os.path.join(self.folder_path, *parts)
        return self.folder_path


    def create_folder(self, folder_path: str) -> None:
        if not os.path.exists(folder_path):
            os.makedirs(folder_path, exist_ok=True)

    def _get_file_path(self, name: str) -> str:
        """Get the full path for a JSONL file (read-only, no folder creation)"""
        if self.use_hierarchy and not self.validate_name(name):
            raise ValueError(f"Invalid hierarchical name '{name}'. Name must contain at least {self.hierarchy_depth-1} '{self.delimiter}' delimiters")
        folder_path = self._get_hierarchy_path(name)
        if name.endswith('.jsonl'):
            return os.path.join(folder_path, name)
        return os.path.join(folder_path, f"{name}.jsonl")

    def _get_or_create_file_path(self, name: str) -> str:
        """Get the full path for a JSONL file, creating folders as needed (for writes)"""
        if self.use_hierarchy and not self.validate_name(name):
            raise ValueError(f"Invalid hierarchical name '{name}'. Name must contain at least {self.hierarchy_depth-1} '{self.delimiter}' delimiters")

        folder_path = self._get_hierarchy_path(name)
        self.create_folder(folder_path) #create the folder if it doesn't exist

        if name.endswith('.jsonl'):
            return os.path.join(folder_path, name)
        return os.path.join(folder_path, f"{name}.jsonl")
    
    def _get_file_name(self, name: str) -> str:
        """Get the name of a JSONL file"""
        if name.endswith('.jsonl'):
            return name
        return f"{name}.jsonl"

    def get_aux_path(self, name: str) -> str:
        """Return the canonical opaque companion path for a ticker."""
        return jsonlfile.get_aux_path(self._get_file_path(name))

    def read_aux(self, name: str) -> bytes:
        """Read a ticker's opaque companion bytes."""
        return jsonlfile.read_aux(self._get_file_path(name))

    def write_aux(self, name: str, payload: bytes) -> None:
        """Atomically replace a ticker's opaque companion bytes."""
        logical_name = name[:-6] if name.endswith('.jsonl') else name
        with self._catalog_transaction("tickers", [logical_name]):
            jsonlfile.write_aux(self._get_file_path(name), payload)

    def remove_aux(self, name: str) -> bool:
        """Remove a ticker's optional opaque companion."""
        logical_name = name[:-6] if name.endswith('.jsonl') else name
        with self._catalog_transaction("tickers", [logical_name]):
            return jsonlfile.remove_aux(self._get_file_path(name))

    def _family_generation_matches(self, snapshot: FolderCatalogSnapshot) -> bool:
        if snapshot.version != CATALOG_VERSION or os.path.exists(self.pending_path):
            return False
        try:
            current = file_identity(self.catalog_path, os.stat(self.catalog_path))
        except OSError:
            return False
        return current == snapshot.file_identity

    def _read_family_generation(
        self,
        name: str,
        snapshot: FolderCatalogSnapshot,
        lower_key: Optional[Any],
        upper_key: Optional[Any],
        auto_deserialize: bool,
    ) -> TickerFamilyRead:
        if not self._family_generation_matches(snapshot):
            raise CatalogChangedError("catalog generation changed before family read")
        entry = snapshot.entries.get(name)
        owner_path = self._get_file_path(name)
        if entry is None or not os.path.isfile(owner_path):
            if not self._family_generation_matches(snapshot):
                raise CatalogChangedError("catalog generation changed before owner read")
            raise FileNotFoundError(f"JSONL file not found: {owner_path}")

        try:
            data = select_jsonl(
                owner_path, lower_key, upper_key, auto_deserialize,
                timespec=snapshot.timespec,
            )
        except FileNotFoundError:
            if not self._family_generation_matches(snapshot):
                raise CatalogChangedError("catalog generation changed during owner read")
            raise

        aux_path = jsonlfile.get_aux_path(owner_path)
        aux = None
        if entry.aux_present:
            try:
                with open(aux_path, "rb") as stream:
                    aux = stream.read()
            except OSError as exc:
                raise CatalogChangedError("recorded companion was unavailable") from exc
            digest = "sha256:" + hashlib.sha256(aux).hexdigest()
            if len(aux) != entry.aux_size or digest != entry.aux_sha256:
                raise CatalogChangedError("companion identity did not match the catalog")
        elif os.path.lexists(aux_path):
            raise CatalogChangedError("unexpected companion was present")

        if not self._family_generation_matches(snapshot):
            raise CatalogChangedError("catalog generation changed during family read")
        return TickerFamilyRead(
            name, data, aux, snapshot.catalog_id, snapshot.revision,
            entry.aux_sha256,
        )

    def read_family(
        self,
        name: str,
        snapshot: Optional[FolderCatalogSnapshot] = None,
        lower_key: Optional[Any] = None,
        upper_key: Optional[Any] = None,
        auto_deserialize: bool = True,
        timeout_seconds: float = 5.0,
    ) -> TickerFamilyRead:
        """Read one owner selection and opaque companion from one generation."""
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)):
            raise TypeError("timeout_seconds must be a number")
        if timeout_seconds < 0:
            raise ValueError("timeout_seconds must be non-negative")
        logical_name = name[:-6] if isinstance(name, str) and name.endswith('.jsonl') else name
        if not isinstance(logical_name, str) or not self._catalog_name_valid(logical_name):
            raise ValueError("family read requires a valid ticker name")

        if snapshot is not None:
            if not isinstance(snapshot, FolderCatalogSnapshot):
                raise TypeError("snapshot must be a FolderCatalogSnapshot")
            if not snapshot.file_identity:
                raise CatalogChangedError("snapshot belongs to another FolderDB root")
            snapshot_root = canonical_root(
                os.path.dirname(os.path.dirname(snapshot.file_identity[0]))
            )
            if snapshot_root != self._catalog_root:
                raise CatalogChangedError("snapshot belongs to another FolderDB root")
            return self._read_family_generation(
                logical_name, snapshot, lower_key, upper_key, auto_deserialize,
            )

        deadline = time.monotonic() + float(timeout_seconds)
        last_error = None
        while True:
            remaining = max(0.0, deadline - time.monotonic())
            try:
                current = self.load_catalog_snapshot(remaining)
                return self._read_family_generation(
                    logical_name, current, lower_key, upper_key, auto_deserialize,
                )
            except CatalogChangedError as exc:
                last_error = exc
            except CatalogBusyError as exc:
                last_error = exc
            if time.monotonic() >= deadline:
                raise CatalogChangedError(
                    "family read could not obtain one stable catalog generation"
                ) from last_error
            time.sleep(min(0.01, max(0.0, deadline - time.monotonic())))

    def _move_jsonl_family(self, source_path: str, target_path: str) -> None:
        """Move an owner first and its optional companion last.

        Removing a pre-existing target companion first and publishing the
        source companion only after the owner move makes every interruption
        state prefer an absent companion over stale content.
        """
        jsonlfile.remove_aux(target_path)
        shutil.move(source_path, target_path)

        source_index = source_path + '.idx'
        if os.path.exists(source_index):
            shutil.move(source_index, target_path + '.idx')

        source_aux = jsonlfile.get_aux_path(source_path)
        if os.path.lexists(source_aux):
            shutil.move(source_aux, jsonlfile.get_aux_path(target_path))

    def _get_aux_files(self) -> List[str]:
        """List managed and orphan companion paths, excluding foreign internals."""
        companions = []
        for root, dirs, files in os.walk(self.folder_path, topdown=True):
            dirs[:] = [
                directory for directory in dirs
                if directory != '.jsonldb' and (
                    not directory.startswith('.') or directory == '.invalid_tickers'
                )
            ]
            for file in files:
                if file.endswith('.jsonl.aux'):
                    companions.append(os.path.join(root, file))
        return sorted(companions)

    def _find_orphan_aux_files(self) -> List[str]:
        """Find companions whose derived JSONL owners are absent."""
        return [
            companion_path
            for companion_path in self._get_aux_files()
            if not os.path.isfile(companion_path[:-4])
        ]

    
    def get_file_list(self) -> List[str]:
        """
        Get a list of all JSONL file names in the database without the .jsonl extension.
        If use_hierarchy is True, it will search through subfolders and return
        paths relative to the root folder with the specified delimiter.
        
        Returns:
            List of file names (without .jsonl extension). If use_hierarchy is True,
            names will include the full hierarchical path using the specified delimiter.
        """
        if self.use_hierarchy:
            result = []
            
            for root, dirs, files in os.walk(self.folder_path, topdown=True):
                # Skip hidden/system directories (starting with '.')
                dirs[:] = [d for d in dirs if d != '.jsonldb' and not d.startswith('.')]
                # Skip .invalid_tickers folder
                if '.invalid_tickers' in root:  # this is important to skip the .invalid_tickers folder
                    continue
                # Add all JSONL files in this directory
                for file in files:
                    if file.endswith('.jsonl'):
                        name = os.path.splitext(file)[0]
                        result.append(name)
                
            return result
        else:
            # Original behavior for non-hierarchical mode - exclude .invalid_tickers
            jsonl_files = []
            for f in os.listdir(self.folder_path):
                if f.endswith('.jsonl') and f != '.invalid_tickers':
                    jsonl_files.append(f)
            return [os.path.splitext(f)[0] for f in jsonl_files]
        
    def search_file_list(self, regex: str) -> List[str]:
        """
        Search for file names that match a regular expression pattern.
        
        Args:
            regex: Regular expression pattern to match against file names
            
        Returns:
            List of file names that match the regex pattern
        """
        import re
        
        # Get all file names
        all_files = self.get_file_list()
        
        # Filter files that match the regex pattern
        pattern = re.compile(regex)
        matching_files = [f for f in all_files if pattern.search(f)]
        
        return matching_files

    # =============== DataFrame Operations ===============
    def overwrite_df(self, name: str, df: pd.DataFrame) -> None:
        logical_name = name[:-6] if name.endswith('.jsonl') else name
        with self._catalog_transaction("tickers", [logical_name]):
            file_path = self._get_or_create_file_path(name)
            if os.path.exists(file_path):
               jsonlfile.remove_aux(file_path)
               os.remove(file_path)

            save_jsonldf(file_path, df, self.timespec)
            self.update_dbmeta(self._get_file_name(name))

    def overwrite_dfs(self, dict_dfs: Dict[Any, pd.DataFrame]) -> None:
        """
        Update or insert multiple DataFrames into JSONL files.
        
        Args:
            dict_dfs: Dictionary mapping file names to DataFrames
        """
        names = [name[:-6] if str(name).endswith('.jsonl') else name for name in dict_dfs]
        if not names:
            return
        with self._catalog_transaction("tickers", names):
            for name, df in dict_dfs.items():
                self.overwrite_df(name, df)

    def upsert_df(self, name: str, df: pd.DataFrame) -> None:
        """
        Update or insert a DataFrame into a JSONL file.

        Args:
            name: Name of the JSONL file
            df: DataFrame to save/update
        """
        logical_name = name[:-6] if name.endswith('.jsonl') else name
        with self._catalog_transaction("tickers", [logical_name]):
            file_path = self._get_or_create_file_path(name)
            if os.path.exists(file_path):
                update_jsonldf(file_path, df, self.timespec)
            else:
                save_jsonldf(file_path, df, self.timespec)

            self.update_dbmeta(self._get_file_name(name))

    def replace_df_range(self, name: str, lower_key: Any, upper_key: Any,
                         df: pd.DataFrame) -> None:
        """Atomically replace one inclusive key range in an existing ticker.

        The complete resulting owner and index are validated and staged before
        the ticker's opaque companion is invalidated. An empty DataFrame clears
        the range. This operation never creates a missing owner.

        Args:
            name: Name of the existing JSONL ticker.
            lower_key: Inclusive lower replacement bound.
            upper_key: Inclusive upper replacement bound.
            df: Authoritative replacement rows, keyed by DataFrame index.
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError("df must be a pandas DataFrame")
        if not df.index.is_unique:
            raise ValueError("DataFrame index must be unique")

        file_path = self._get_file_path(name)
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"JSONL file not found: {file_path}")

        replacement = df.to_dict("index")
        logical_name = name[:-6] if name.endswith('.jsonl') else name
        with self._catalog_transaction("tickers", [logical_name]):
            jsonlfile.replace_jsonl_range(
                file_path,
                lower_key,
                upper_key,
                replacement,
                self.timespec,
            )
            self.update_dbmeta(self._get_file_name(name))

    def upsert_dfs(self, dict_dfs: Dict[Any, pd.DataFrame]) -> None:
        """
        Update or insert multiple DataFrames into JSONL files.
        
        Args:
            dict_dfs: Dictionary mapping file names to DataFrames
        """
        names = [name[:-6] if str(name).endswith('.jsonl') else name for name in dict_dfs]
        if not names:
            return
        with self._catalog_transaction("tickers", names):
            for name, df in dict_dfs.items():
                self.upsert_df(name, df)

    def get_df(self, names: List[str]=None, lower_key: Optional[Any] = None, upper_key: Optional[Any] = None,auto_deserialize: bool = True) -> Dict[str, pd.DataFrame]:
        """
        Get DataFrames from multiple JSONL files within a key range.
        
        Args:
            names: List of JSONL file names
            lower_key: Lower bound of the key range
            upper_key: Upper bound of the key range
            auto_deserialize: Whether to automatically deserialize datetime keys

        Returns:
            Dictionary mapping file names to selected DataFrames
        """
        if names is None:
            names = self.get_file_list()

        result = {}
        for name in names:
            file_path = self._get_file_path(name)
            if os.path.exists(file_path):
                result[name] = select_jsonldf(file_path, lower_key, upper_key, auto_deserialize, timespec=self.timespec)
            else:
                print(f"File {name} not found")
        return result

    # =============== Dictionary Operations ===============
    def overwrite_dict(self, name: str, data_dict: Dict[Any, Dict[str, Any]]) -> None:
        logical_name = name[:-6] if name.endswith('.jsonl') else name
        with self._catalog_transaction("tickers", [logical_name]):
            file_path = self._get_or_create_file_path(name)
            if os.path.exists(file_path):
               jsonlfile.remove_aux(file_path)
               os.remove(file_path)

            save_jsonl(file_path, data_dict, self.timespec)
            self.update_dbmeta(self._get_file_name(name))

    def overwrite_dicts(self, dict_dicts: Dict[Any, Dict[str, Dict[str, Any]]]) -> None:
        """
        Update or insert multiple DataFrames into JSONL files.
        
        Args:
            dict_dfs: Dictionary mapping file names to DataFrames
        """
        names = [name[:-6] if str(name).endswith('.jsonl') else name for name in dict_dicts]
        if not names:
            return
        with self._catalog_transaction("tickers", names):
            for name, data_dict in dict_dicts.items():
                self.overwrite_dict(name, data_dict)

    def upsert_dict(self, name: str, data_dict: Dict[Any, Dict[str, Any]]) -> None:
        """
        Update or insert a dictionary into a JSONL file.
        
        Args:
            name: Name of the JSONL file
            data_dict: Dictionary to save/update
        """
        logical_name = name[:-6] if name.endswith('.jsonl') else name
        with self._catalog_transaction("tickers", [logical_name]):
            file_path = self._get_or_create_file_path(name)
            if os.path.exists(file_path):
                update_jsonl(file_path, data_dict, self.timespec)
            else:
                save_jsonl(file_path, data_dict, self.timespec)

            self.update_dbmeta(self._get_file_name(name))

    def upsert_dicts(self, dict_dicts: Dict[Any, Dict[str, Dict[str, Any]]]) -> None:
        """
        Update or insert multiple dictionaries into JSONL files.
        
        Args:
            dict_dicts: Dictionary mapping file names to data dictionaries
        """
        names = [name[:-6] if str(name).endswith('.jsonl') else name for name in dict_dicts]
        if not names:
            return
        with self._catalog_transaction("tickers", names):
            for name, data_dict in dict_dicts.items():
                self.upsert_dict(name, data_dict)

    def get_dict(self, names: List[str]=None, lower_key: Optional[Any] = None, upper_key: Optional[Any] = None,auto_deserialize: bool = True) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """
        Get dictionaries from multiple JSONL files within a key range.
        
        Args:
            names: List of JSONL file names
            lower_key: Lower bound of the key range
            upper_key: Upper bound of the key range
            auto_deserialize: Whether to automatically deserialize datetime keys

        Returns:
            Dictionary mapping file names to selected data dictionaries
        """
        if names is None:
            names = self.get_file_list()

        if not isinstance(names, list):
            names = [names]

        result = {}
        for name in names:
            file_path = self._get_file_path(name)
            if os.path.exists(file_path):
                result[name] = select_jsonl(file_path, lower_key, upper_key, auto_deserialize, timespec=self.timespec)
        return result

    # =============== Delete Operations ===============
    def clear_folder(self,force=False) -> None:
        """
        Clear all JSONL files in the database folder.
        """
        if not force:
            print("WARNING: This will delete all data in the database folder. Call clear_folder with force=True to proceed.")
            return
        with self._catalog_transaction("full"):
            # Invalidate every managed or orphan companion before deleting owners.
            for companion_path in self._get_aux_files():
                os.remove(companion_path)
            for root, dirs, files in os.walk(self.folder_path, topdown=True):
                # Preserve the root catalog namespace while clearing managed data.
                dirs[:] = [
                    d for d in dirs
                    if d != '.jsonldb' and (
                        not d.startswith('.') or d == '.invalid_tickers'
                    )
                ]
                for file in files:
                    if file.endswith(('.idx', '.jsonl', '.meta')):
                        os.remove(os.path.join(root, file))
            self.delete_empty_folders()

    def delete_file(self, name: str) -> None:
        """
        Delete a JSONL file.

        Args:
            name: Name of the JSONL file
        """
        logical_name = name[:-6] if name.endswith('.jsonl') else name
        with self._catalog_transaction("tickers", [logical_name]):
            file_path = self._get_file_path(name)
            jsonlfile.remove_aux(file_path)
            if os.path.exists(file_path):
                os.remove(file_path)
                if os.path.exists(file_path + '.idx'):
                    os.remove(file_path + '.idx')
                if self.use_hierarchy:
                    self.delete_empty_folders()

    def delete_file_keys(self, name: str, keys: List[str]) -> None:
        """
        Delete specific keys from a JSONL file.

        Args:
            name: Name of the JSONL file
            keys: List of keys to delete
        """
        logical_name = name[:-6] if name.endswith('.jsonl') else name
        with self._catalog_transaction("tickers", [logical_name]):
            file_path = self._get_file_path(name)
            if os.path.exists(file_path):
                delete_jsonl(file_path, keys, self.timespec)
                self.update_dbmeta(self._get_file_name(name))

    def delete_file_range(self, name: str, lower_key: Any, upper_key: Any) -> None:
        """
        Delete all keys within a range from a JSONL file.

        Args:
            name: Name of the JSONL file
            lower_key: Lower bound of the key range
            upper_key: Upper bound of the key range
        """
        logical_name = name[:-6] if name.endswith('.jsonl') else name
        with self._catalog_transaction("tickers", [logical_name]):
            file_path = self._get_file_path(name)
            if not os.path.exists(file_path):
                return

            # Read the index file (self-heals missing/empty/corrupt)
            index = jsonlfile.load_index(file_path)

            # Filter keys within range (bounds must use the same serialization as
            # stored keys — str(datetime) uses a space, isoformat uses 'T')
            lower_str = serialize_linekey(lower_key, self.timespec)
            upper_str = serialize_linekey(upper_key, self.timespec)
            keys_to_delete = [
                key for key in index.keys()
                if lower_str <= key <= upper_str
            ]
            
            if keys_to_delete:
                delete_jsonl(file_path, keys_to_delete)

    def delete_range(self, names: List[str], lower_key: Any, upper_key: Any) -> None:
        """
        Delete all keys within a range from multiple JSONL files.
        
        Args:
            names: List of JSONL file names
            lower_key: Lower bound of the key range
            upper_key: Upper bound of the key range
        """
        logical_names = [name[:-6] if name.endswith('.jsonl') else name for name in names]
        if not logical_names:
            return
        with self._catalog_transaction("tickers", logical_names):
            for name in names:
                self.delete_file_range(name, lower_key, upper_key)

    # =============== Metadata Management ===============
    def _make_meta_entry(self, name: str, file_path: str, linted: bool = False,
                         lint_time: str = "", timespecs: Optional[set] = None) -> Dict[str, Any]:
        """Build one db.meta entry for a JSONL file from its index file.

        Example: {"name": "users", "path": ".../users.jsonl", "min_index": "a",
                  "max_index": "z", "size": 123, "count": 3, "lint_time": "", "linted": False}
        """
        index_file = file_path + '.idx'
        min_index = max_index = None
        count = 0
        if os.path.exists(index_file):
            index = jsonlfile.load_index(file_path)  # heals empty/corrupt
            if index:
                keys = list(index.keys())
                min_index, max_index = keys[0], keys[-1]
                count = len(keys)
                if timespecs is not None:
                    for key in keys:
                        detected = detect_timespec(key)
                        if detected:
                            timespecs.add(detected)
        return {
            "name": name,
            "path": file_path,
            "min_index": min_index,
            "max_index": max_index,
            "size": os.path.getsize(file_path),
            "count": count,
            "lint_time": lint_time,
            "linted": linted,
        }

    def build_dbmeta(self) -> None:
        """
        Build or update the db.meta file with information about all JSONL files.
        
        The db.meta file contains metadata for each JSONL file including:
        - name: filename without .jsonl extension
        - min_index: smallest index from the index file
        - max_index: biggest index from the index file
        - size: size of the file in bytes
        - lint_time: ISO format timestamp of last lint
        - linted: boolean indicating if file has been linted
        """
        with self._catalog_transaction("full"):
            pass

    def get_dbmeta(self) -> Dict[str, Any]:
        """
        Get the database metadata as a dictionary.
        
        Returns:
            Dictionary containing metadata for all JSONL files in the database
        """
        with self._catalog_transaction("full"):
            pass
        return load_jsonl(self.dbmeta_path)
    
    def delete_dbmeta(self,name: str) -> None:
        """
        Delete the metadata for a specific JSONL file in db.meta.
        """
        meta_key = name[:-6] if name.endswith('.jsonl') else name
        with self._catalog_transaction("tickers", [meta_key]):
            self._catalog_entry_deletions.add(meta_key)
            if os.path.exists(self.dbmeta_path):
                delete_jsonl(self.dbmeta_path, [meta_key])
    
    def update_dbmeta(self, name: str, linted: bool = False) -> None:
        """
        Update the metadata for a specific JSONL file in db.meta.
        
        Args:
            name: Name of the JSONL file (with or without .jsonl extension)
            linted: Value to set for the linted field
        """
        # Metadata key is the name without the .jsonl extension
        meta_key = name[:-6] if name.endswith('.jsonl') else name
        with self._catalog_transaction("tickers", [meta_key]):
            lint_time = datetime.now().isoformat() if linted else ""
            self._catalog_lint_overrides[meta_key] = (linted, lint_time)

    def lint_db(self, force: bool = False) -> None:
        with self._catalog_transaction("full"):
            self._lint_db_unlocked(force)

    def _lint_db_unlocked(self, force: bool = False) -> None:
        """Lint all JSONL files in the database.

        Args:
            force: If True, run full mmap line-count verification on every file.
                   If False (default), skip the scan when the index is fresh.
        """
        meta_file = os.path.join(self.folder_path, "db.meta")
        if not os.path.exists(meta_file):
            self.build_dbmeta()

        metadata = select_jsonl(meta_file)
        print(f"Found {len(metadata)} JSONL files to lint.")
        for orphan_path in self._find_orphan_aux_files():
            print(f"WARNING: orphan companion {orphan_path}")

        all_meta = {}

        for name in metadata:
            print(f"Linting file: {name}")
            file_path = self._get_file_path(name)
            exist_flag = (
                lint_jsonl(file_path, force=force)
                if os.path.exists(file_path)
                else False
            )

            if not exist_flag:
                print(f"File {name} no longer exist, deleting metadata.")
                # Simply skip — don't add to all_meta
            else:
                all_meta[name] = self._make_meta_entry(
                    name, file_path, linted=True,
                    lint_time=datetime.now().isoformat())

        # Single write for all metadata
        save_jsonl(self.dbmeta_path, all_meta)
        lint_jsonl(self.dbmeta_path, force=force)

        if self.use_hierarchy:
            lint_jsonl(self.hmeta_path, force=force)
            self.delete_empty_folders()

    def lint_hierarchy(self, hierarchy_depth:int) -> None:
        if hierarchy_depth < 1:
            raise ValueError("Hierarchy level must be positive")
        with self._catalog_transaction("full"):
            self._lint_hierarchy_unlocked(hierarchy_depth)

    def _lint_hierarchy_unlocked(self, hierarchy_depth:int) -> None:
        """
        Reorganize JSONL files according to hierarchy levels and move invalid files to .invalid_tickers.
        
        Args:
            hierarchy_level: Target hierarchy level. If None, uses value from h.meta or defaults to 1
        """

        if hierarchy_depth < 1:
            raise ValueError("Hierarchy level must be positive")

        for orphan_path in self._find_orphan_aux_files():
            print(f"WARNING: orphan companion {orphan_path}")

        print(f"Organizing files for hierarchy level {hierarchy_depth}")
            
        self.use_hierarchy = True
        self.hierarchy_depth = hierarchy_depth


        # Create .invalid_tickers folder if it doesn't exist
        if not os.path.exists(self.invalid_tickers_path):
            os.makedirs(self.invalid_tickers_path, exist_ok=True)
        
        # Get all JSONL files in the root folder and immediate subdirectories
        all_files = []
        for root, dirs, files in os.walk(self.folder_path, topdown=True):
            # Skip hidden/system directories (starting with '.')
            dirs[:] = [d for d in dirs if d != '.jsonldb' and not d.startswith('.')]
            # Skip .invalid_tickers folder
            if '.invalid_tickers' in root:
                continue
            for file in files:
                if file.endswith('.jsonl'):
                    file_path = os.path.join(root, file)
                    name = os.path.splitext(file)[0]
                    all_files.append((name, file_path))
        
        valid_files = []
        invalid_files = []
        
        # Categorize files as valid or invalid
        for name, file_path in all_files:
            if self.validate_name(name):
                valid_files.append((name, file_path))
            else:
                invalid_files.append((name, file_path))
        
        print(f"Found {len(valid_files)} valid files and {len(invalid_files)} invalid files")
        
        # Move invalid files to .invalid_tickers folder
        for name, file_path in invalid_files:
            try:
                dest_path = os.path.join(self.invalid_tickers_path, os.path.basename(file_path))
                if file_path != dest_path:  # Avoid moving file to itself
                    self._move_jsonl_family(file_path, dest_path)
                    print(f"Moved invalid file {name} to .invalid_tickers")
            except Exception as e:
                print(f"Warning: Could not move invalid file {name}: {str(e)}")
        
        # Reorganize valid files according to hierarchy
        for name, file_path in valid_files:
            try:
                target_dir = self._get_hierarchy_path(name)
                target_file = os.path.join(target_dir, os.path.basename(file_path))
                
                # Skip if file is already in correct location
                if file_path == target_file:
                    continue
                    
                # Create target directory if needed
                self.create_folder(target_dir)
                
                # Move owner first and optional companion last.
                self._move_jsonl_family(file_path, target_file)
                print(f"Moved {name} to {target_dir}")
                    
            except Exception as e:
                print(f"Warning: Could not move valid file {name}: {str(e)}")
        
        # Clean up empty directories
        self.delete_empty_folders()
        
        # Rebuild metadata to reflect new structure
        self.build_dbmeta()
        self.build_hmeta()
        
        print("Hierarchy organization completed")

    def reprocess_invalid_tickers(self) -> None:
        with self._catalog_transaction("full"):
            self._reprocess_invalid_tickers_unlocked()

    def _reprocess_invalid_tickers_unlocked(self) -> None:
        """
        Reprocess files in .invalid_tickers folder and move any that now match naming convention.
        """
        if not os.path.exists(self.invalid_tickers_path):
            print("No .invalid_tickers folder found")
            return

        for orphan_path in self._find_orphan_aux_files():
            print(f"WARNING: orphan companion {orphan_path}")
            
        # Get all JSONL files in .invalid_tickers folder
        invalid_files = []
        for file in os.listdir(self.invalid_tickers_path):
            if file.endswith('.jsonl'):
                file_path = os.path.join(self.invalid_tickers_path, file)
                name = os.path.splitext(file)[0]
                invalid_files.append((name, file_path))
        
        if not invalid_files:
            print("No files found in .invalid_tickers folder")
            return
            
        print(f"Found {len(invalid_files)} files to reprocess")
        
        now_valid_files = []
        still_invalid_files = []
        
        # Check each file against current naming rules
        for name, file_path in invalid_files:
            if self.validate_name(name):
                now_valid_files.append((name, file_path))
            else:
                still_invalid_files.append((name, file_path))
        
        print(f"{len(now_valid_files)} files are now valid, {len(still_invalid_files)} remain invalid")
        
        # Move now-valid files to appropriate hierarchy folders
        for name, file_path in now_valid_files:
            try:
                target_dir = self._get_hierarchy_path(name)
                target_file = os.path.join(target_dir, os.path.basename(file_path))
                
                # Create target directory if needed
                self.create_folder(target_dir)
                
                # Move owner first and optional companion last.
                had_index = os.path.exists(file_path + '.idx')
                self._move_jsonl_family(file_path, target_file)
                print(f"Moved {name} from .invalid_tickers to {target_dir}")
                
                if not had_index:
                    # Build index for newly valid file
                    build_jsonl_index(target_file)
                
                # Update metadata for this file
                self.update_dbmeta(name)
                    
            except Exception as e:
                print(f"Warning: Could not move file {name}: {str(e)}")
        
        # Rebuild metadata to include newly valid files
        if now_valid_files:
            self.build_dbmeta()
            
        print("Reprocessing completed")

    def delete_empty_folders(self) -> None:
        """
        Delete empty folders in the database.
        Recursively removes empty folders from bottom up.
        """
        for root, dirs, files in os.walk(self.folder_path, topdown=False):
            if root == self.folder_path:
                continue
            real_root = os.path.realpath(root)
            real_control = os.path.realpath(self.control_path)
            if real_root == real_control or real_root.startswith(real_control + os.sep):
                continue
            try:
                if not os.listdir(root):
                    os.rmdir(root)
            except OSError:
                pass

    # =============== Version Control ===============
    def commit(self, msg: str = "") -> None:
        """
        Commit changes in the database folder.
        
        If the folder is not already a git repository, it will be initialized first.
        
        Args:
            msg: Optional commit message. If empty, an auto-generated message will be used.
            
        Raises:
            git.exc.GitCommandError: If git commands fail
        """
        from .vercontrol import init_folder, commit as vercontrol_commit, is_versioned

        with self._catalog_writer():
            # Check if folder is a git repo, if not initialize it
            if not is_versioned(self.folder_path):
                init_folder(self.folder_path)

            # Commit stable version content; pending/lock/temp are ignored.
            vercontrol_commit(self.folder_path, msg)
            print("Commit successful.")
    
    def revert(self, version_hash: str) -> None:
        """
        Revert the database to a previous version.
        
        Args:
            version_hash: Hash of the commit to revert to
            
        Raises:
            git.exc.GitCommandError: If git commands fail
            ValueError: If the specified commit is not found
        """
        from .vercontrol import revert as vercontrol_revert

        self._ensure_control_namespace()
        with WriterLock(self.writer_lock_path, 5.0):
            base = self._recover_locked()
            transaction_id = str(uuid.uuid4())
            state = PendingState(
                transaction_id, base.catalog_id, base.revision,
                base.revision + 1, "full", (),
            )
            atomic_write(self.pending_path, pending_envelope(state))

            # Git reset does not remove untracked files. Invalidate every
            # companion before owner content can change; tracked target
            # companions, if any, are restored by the reset itself.
            for companion_path in self._get_aux_files():
                os.remove(companion_path)
            vercontrol_revert(self.folder_path, version_hash)
            self._ensure_control_namespace()
            invalidate_snapshot(self._catalog_root)

            # Refresh layout/config state restored by Git before proving facts.
            if os.path.exists(self.hmeta_path):
                hmeta = select_jsonl(self.hmeta_path)
                self.use_hierarchy = hmeta["use_hierarchy"]
                self.delimiter = hmeta["delimiter"]
                self.hierarchy_depth = hmeta["hierarchy_depth"]
            else:
                self.use_hierarchy = False
            if os.path.exists(self.configmeta_path):
                config = select_jsonl(self.configmeta_path)
                self.timespec = config.get("timespec", jsonlfile.TIME_SPEC)
            else:
                self.timespec = jsonlfile.TIME_SPEC

            restored = None
            try:
                restored = self._try_catalog_locked()
            except UnsupportedCatalogVersionError:
                raise
            except (OSError, ValueError, TypeError, CatalogBusyError):
                restored = None
            entries = self._reconcile_entries(restored, None)
            if (
                restored is not None
                and restored.version == CATALOG_VERSION
                and restored.timespec == self.timespec
                and dict(restored.entries) == entries
            ):
                self._write_projection(entries)
                invalidate_snapshot(self._catalog_root)
                self._read_valid_catalog()
            elif (
                restored is not None
                and restored.timespec == self.timespec
                and dict(restored.entries) == entries
            ):
                self._write_projection(entries)
                self._publish_catalog(
                    restored.catalog_id, restored.revision + 1,
                    str(uuid.uuid4()), entries,
                )
            else:
                self._write_projection(entries)
                self._new_lineage(entries)
            clear_pending(self.pending_path)
            print(f"Successfully reverted the folder: {self.folder_path} to version: {version_hash}")
    
    def version(self) -> Dict[str, str]:
        """
        List all versions of the database.
        
        Returns:
            Dictionary with commit hashes as keys and commit messages as values
            
        Raises:
            git.exc.GitCommandError: If git commands fail
        """
        from .vercontrol import list_version

        return list_version(self.folder_path)
