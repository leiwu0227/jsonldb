"""
A simple file-based database that stores data in JSONL format.
Each table is stored in a separate JSONL file.
"""

import os
import logging
import pandas as pd
from typing import Dict, List, Optional, Any, NamedTuple
from datetime import datetime
from jsonldb.jsonlfile import (
    save_jsonl, load_jsonl, select_jsonl, update_jsonl, delete_jsonl,
    lint_jsonl, build_jsonl_index, save_jsonl_atomic, serialize_linekey,
    detect_timespec
)
from jsonldb.jsonldf import (
    save_jsonldf, update_jsonldf, select_jsonldf
)
import jsonldb.jsonlfile as jsonlfile
from jsonldb.reports import capture_report


logger = logging.getLogger(__name__)


def _load_control(file_path: str, record_key: str):
    """Read one canonical control record, migrating legacy scalar rows."""
    rows = load_jsonl(file_path, auto_deserialize=False)
    record = rows.get(record_key)
    if len(rows) == 1 and isinstance(record, dict):
        return record, True
    return jsonlfile._load_legacy_rows(file_path), False


class TableWithMeta(NamedTuple):
    """A metadata record paired with rows read after that record."""

    meta: Any
    rows: Any

class FolderDB:
    """
    A simple file-based database that stores data in JSONL format.
    Each table is stored in a separate JSONL file.
    """
    
    # =============== Core/Initialization ===============
    def __init__(self, folder_path: str, hierarchy_depth: int = None):
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

        with capture_report(folder_path, "open", "integrity.log"):
            self._open(hierarchy_depth)

    def _open(self, hierarchy_depth: int = None) -> None:
        """Load and reconcile database controls under integrity capture."""

        self.use_hierarchy = False
        self.delimiter = '.'
        self.hierarchy_depth = 0

        # Initialize all paths first
        self.hmeta_path = os.path.join(self.folder_path, "h.meta")
        self.dbmeta_path = os.path.join(self.folder_path, "db.meta")
        self.configmeta_path = os.path.join(self.folder_path, "config.meta")
        self.invalid_tickers_path = os.path.join(
            self.folder_path, ".invalid_tickers")

        hmeta, canonical = (_load_control(self.hmeta_path, "hierarchy")
                            if os.path.exists(self.hmeta_path) else ({}, False))
        valid_hierarchy = (
            type(hmeta.get('use_hierarchy')) is bool
            and isinstance(hmeta.get('delimiter'), str) and bool(hmeta['delimiter'])
            and type(hmeta.get('hierarchy_depth')) is int
            and hmeta['hierarchy_depth'] >= int(hmeta['use_hierarchy']))
        hierarchy_recovered = False
        if valid_hierarchy:
            self.use_hierarchy = hmeta["use_hierarchy"]
            self.delimiter = hmeta["delimiter"]
            self.hierarchy_depth = hmeta["hierarchy_depth"]
            if not canonical:
                self.build_hmeta()

            if hierarchy_depth is not None and self.hierarchy_depth != hierarchy_depth: 
                #current hierarchy depth is not the same as the one provided, so we need to lint the hierarchy
                self.lint_hierarchy(hierarchy_depth)
        else:
            try:
                hierarchy_recovered = self._recover_hierarchy(hierarchy_depth)
            except (ValueError, OSError) as exc:
                logger.warning("hierarchy recovery failed for %s: %s",
                               self.hmeta_path, exc,
                               extra={"jsonldb_file": self.hmeta_path,
                                      "jsonldb_kind": "hierarchy_recovery_failed"})
                raise

        # Configuration is per-instance state; never mutate module globals.
        # Retain unknown settings when rewriting the protected control file.
        self.timespec = jsonlfile.TIME_SPEC
        self.meta_slot_bytes = None
        self._config_meta = {}
        if os.path.exists(self.configmeta_path):
            config_meta, canonical = _load_control(
                self.configmeta_path, "config")
            self._config_meta = dict(config_meta)
            self.meta_slot_bytes = config_meta.get("meta_slot_bytes")
            if config_meta.get("timespec"):
                self.timespec = config_meta["timespec"]
            if not canonical or not config_meta.get("timespec"):
                self.build_configmeta()
        else:
            logger.warning("regenerated missing control file %s",
                           self.configmeta_path,
                           extra={"jsonldb_file": self.configmeta_path,
                                  "jsonldb_kind": "control_regenerated"})
            self.build_configmeta()

        # Only rebuild db.meta if it doesn't exist or the folder has been
        # modified externally.  Writes already call update_dbmeta()
        # incrementally, so db.meta stays in sync during normal operation.
        if hierarchy_recovered:
            self.build_dbmeta()
        elif os.path.exists(self.dbmeta_path):
            dbmeta_mtime = os.path.getmtime(self.dbmeta_path)
            folder_mtime = self._latest_database_directory_mtime()
            if folder_mtime > dbmeta_mtime:
                self.build_dbmeta()
        else:
            logger.warning("regenerated missing control file %s",
                           self.dbmeta_path,
                           extra={"jsonldb_file": self.dbmeta_path,
                                  "jsonldb_kind": "control_regenerated"})
            self.build_dbmeta()

        # Guard against config.meta disagreeing with the data's actual datetime
        # precision (possible from the pre-instance-scoping contamination bug).
        # Stage 1 checks db.meta boundary keys (no extra I/O); stage 2 confirms
        # with a full index scan before healing. Mixed precision keeps the
        # configured value. See _detect_data_timespec/_scan_index_timespecs.
        candidate = self._detect_data_timespec()
        if candidate is not None:
            found = self._scan_index_timespecs()
            if found == {candidate}:
                logger.warning(
                    "%s timespec '%s' does not match data ('%s'); "
                    "auto-correcting config.meta",
                    self.configmeta_path,
                    self.timespec,
                    candidate,
                    extra={"jsonldb_file": self.configmeta_path,
                           "jsonldb_kind": "timespec_corrected"},
                )
                self.timespec = candidate
                self.build_configmeta()
            elif len(found) > 1:
                logger.warning(
                    "mixed datetime key precisions %s found in %s; keeping timespec '%s'",
                    sorted(found),
                    self.folder_path,
                    self.timespec,
                    extra={"jsonldb_file": self.folder_path,
                           "jsonldb_kind": "mixed_timespec"},
                )

    def _recover_hierarchy(self, requested_depth: Optional[int]) -> bool:
        """Infer lost controls, preflight every move, then publish settings last."""
        def scan_error(error):
            raise error

        tables, directories = [], []
        for root, dirs, files in os.walk(self.folder_path, onerror=scan_error):
            dirs[:] = sorted(d for d in dirs if not d.startswith('.')
                             and not os.path.islink(os.path.join(root, d)))
            relative = os.path.relpath(root, self.folder_path)
            parts = [] if relative == '.' else relative.split(os.sep)
            if parts:
                directories.append(root)
            for filename in sorted(files):
                source = os.path.join(root, filename)
                if (filename.endswith('.jsonl') and os.path.isfile(source)
                        and not os.path.islink(source)):
                    tables.append((filename[:-6], source, parts))

        damaged = os.path.exists(self.hmeta_path)
        depths = sorted({len(parts) for _, _, parts in tables})
        if not damaged and not any(depths) and requested_depth is None:
            return False
        depth = min(depths, default=0)
        if requested_depth is not None:
            if type(requested_depth) is not int or requested_depth < 1:
                raise ValueError('Hierarchy level must be a positive integer')
            depth = requested_depth
            if not damaged and not any(depths):
                # Explicit creation on a flat folder retains ordinary reorganization.
                logger.warning('regenerated missing control file %s', self.hmeta_path,
                               extra={"jsonldb_file": self.hmeta_path,
                                      "jsonldb_kind": "control_regenerated"})
                self.lint_hierarchy(depth)
                return True

        # Intersect separators that reproduce every observed directory prefix.
        # The shortest match avoids treating whole name segments as delimiters.
        candidates = None
        if depth:
            for name, source, parts in tables:
                if not parts or (len(parts) == 1 and name == parts[0]):
                    continue
                separators = set()
                if name.startswith(parts[0]):
                    tail = name[len(parts[0]):]
                    for length in range(1, len(tail)):
                        separator = tail[:length]
                        segments = name.split(separator)
                        if all(segments) and segments[:len(parts)] == parts:
                            separators.add(separator)
                candidates = (separators if candidates is None
                              else candidates & separators)
                if not candidates:
                    raise ValueError('contradictory hierarchy prefixes at %s' % source)
        delimiter = min(candidates, key=len) if candidates else '.'
        self.use_hierarchy, self.hierarchy_depth, self.delimiter = bool(depth), depth, delimiter

        moves, destinations = [], set()
        for name, source, _ in tables:
            index_path = source + '.idx'
            if os.path.lexists(index_path) and (not os.path.isfile(index_path)
                                               or os.path.islink(index_path)):
                raise ValueError('hierarchy recovery index is not a regular file: %s' % index_path)
            if not self.validate_name(name):
                raise ValueError('recovered depth %d would exclude table %s' % (depth, source))
            if depth and any(not part or part.startswith('.')
                             for part in name.split(delimiter)[:depth]):
                raise ValueError('unsafe hierarchy prefix in %s' % source)
            target = self._get_file_path(name)
            if target in destinations:
                raise ValueError('hierarchy recovery collision at %s' % target)
            destinations.add(target)
            if source == target:
                continue
            parent = os.path.abspath(os.path.dirname(target))
            while parent != os.path.abspath(self.folder_path):
                if os.path.lexists(parent) and (not os.path.isdir(parent) or os.path.islink(parent)):
                    raise ValueError('hierarchy recovery directory collision at %s' % parent)
                parent = os.path.dirname(parent)
            for old, new in ((source, target), (source + '.idx', target + '.idx')):
                if os.path.lexists(new):
                    raise ValueError('hierarchy recovery collision at %s' % new)
                if os.path.lexists(old):
                    moves.append((old, new))

        for source, target in moves:
            os.makedirs(os.path.dirname(target), exist_ok=True)
            if os.path.lexists(target):
                raise ValueError('hierarchy recovery collision at %s' % target)
            os.rename(source, target)
        if self.use_hierarchy:
            self.build_hmeta()
        else:
            # Absence of h.meta is the canonical flat configuration.
            for path in (self.hmeta_path, self.hmeta_path + '.idx'):
                if os.path.exists(path):
                    os.remove(path)
        for directory in reversed(directories):
            try:
                os.rmdir(directory)
            except OSError:
                pass
        logger.warning(
            'recovered hierarchy for %s: source=%s observed_depths=%s depth=%d delimiter=%r tables=%d',
            self.hmeta_path, 'invalid' if damaged else 'missing', depths,
            depth, delimiter, len(tables),
            extra={"jsonldb_file": self.hmeta_path, "jsonldb_kind": "hierarchy_recovered"})
        return True

    def build_hmeta(self) -> None:
        """
        Save the folder information to a file.
        """
        if self.use_hierarchy:
            hierarchy_info = {
                "use_hierarchy": self.use_hierarchy,
                "delimiter": self.delimiter,
                "hierarchy_depth": self.hierarchy_depth
            }
            save_jsonl_atomic(self.hmeta_path, {"hierarchy": hierarchy_info})


    def build_configmeta(self) -> None:
        """
        Save the folder information to a file.
        """
        config_info = dict(self._config_meta)
        config_info["timespec"] = self.timespec
        if self.meta_slot_bytes is None:
            config_info.pop("meta_slot_bytes", None)
        else:
            config_info["meta_slot_bytes"] = self.meta_slot_bytes
        save_jsonl_atomic(self.configmeta_path, {"config": config_info})
        self._config_meta = config_info

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
            if name.endswith('.jsonl'):
                name = name[:-6]
            # Take first hierarchy_depth parts for the path
            parts = name.split(self.delimiter)[:self.hierarchy_depth]
            return os.path.join(self.folder_path, *parts)
        return self.folder_path


    def create_folder(self, folder_path: str) -> None:
        if not os.path.exists(folder_path):
            os.makedirs(folder_path, exist_ok=True)

    def _latest_database_directory_mtime(self) -> float:
        """Return the newest visible directory mtime relevant to discovery."""
        latest = os.path.getmtime(self.folder_path)
        if not self.use_hierarchy:
            return latest

        for root, dirs, _ in os.walk(self.folder_path, topdown=True):
            dirs[:] = [directory for directory in dirs if not directory.startswith('.')]
            latest = max(latest, os.path.getmtime(root))
        return latest

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
                dirs[:] = [d for d in dirs if not d.startswith('.')]
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
    def overwrite_df(self, name: str, df: pd.DataFrame,
                     meta: Optional[dict] = None) -> None:
        file_path = self._get_or_create_file_path(name)
        save_jsonldf(
            file_path, df, self.timespec, meta=meta,
            slot_bytes=self.meta_slot_bytes,
        )
        self.update_dbmeta(self._get_file_name(name))

    def overwrite_dfs(self, dict_dfs: Dict[Any, pd.DataFrame]) -> None:
        """
        Update or insert multiple DataFrames into JSONL files.
        
        Args:
            dict_dfs: Dictionary mapping file names to DataFrames
        """
        for name, df in dict_dfs.items():
            self.overwrite_df(name, df)

    def upsert_df(self, name: str, df: pd.DataFrame,
                  meta: Optional[dict] = None) -> None:
        """
        Update or insert a DataFrame into a JSONL file.

        Args:
            name: Name of the JSONL file
            df: DataFrame to save/update
        """
        file_path = self._get_or_create_file_path(name)
        if os.path.exists(file_path):
            update_jsonldf(file_path, df, self.timespec, meta=meta)
        else:
            save_jsonldf(
                file_path, df, self.timespec, meta=meta,
                slot_bytes=self.meta_slot_bytes,
            )
        
        self.update_dbmeta(self._get_file_name(name))

    def upsert_dfs(self, dict_dfs: Dict[Any, pd.DataFrame]) -> None:
        """
        Update or insert multiple DataFrames into JSONL files.
        
        Args:
            dict_dfs: Dictionary mapping file names to DataFrames
        """
        for name, df in dict_dfs.items():
            self.upsert_df(name, df)

    def get_df(
        self,
        names: List[str] = None,
        lower_key: Optional[Any] = None,
        upper_key: Optional[Any] = None,
        auto_deserialize: bool = True,
    ) -> Dict[str, pd.DataFrame]:
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
                logger.warning("file not found: %s", file_path)
        return result

    def get_df_with_meta(
        self,
        name: str,
        lower_key: Optional[Any] = None,
        upper_key: Optional[Any] = None,
        auto_deserialize: bool = True,
    ) -> TableWithMeta:
        """Read one table's metadata first, followed by its DataFrame rows."""
        file_path = self._get_file_path(name)
        if not os.path.exists(file_path):
            return TableWithMeta(None, pd.DataFrame())
        meta = self.read_meta(name)
        rows = select_jsonldf(
            file_path, lower_key, upper_key, auto_deserialize,
            timespec=self.timespec,
        )
        return TableWithMeta(meta, rows)

    # =============== Dictionary Operations ===============
    def overwrite_dict(self, name: str,
                       data_dict: Dict[Any, Dict[str, Any]],
                       meta: Optional[dict] = None) -> None:
        file_path = self._get_or_create_file_path(name)
        save_jsonl(
            file_path, data_dict, self.timespec, meta=meta,
            slot_bytes=self.meta_slot_bytes,
        )
        self.update_dbmeta(self._get_file_name(name))

    def overwrite_dicts(self, dict_dicts: Dict[Any, Dict[str, Dict[str, Any]]]) -> None:
        """
        Update or insert multiple DataFrames into JSONL files.
        
        Args:
            dict_dfs: Dictionary mapping file names to DataFrames
        """
        for name, data_dict in dict_dicts.items():
            self.overwrite_dict(name, data_dict)

    def upsert_dict(self, name: str,
                    data_dict: Dict[Any, Dict[str, Any]],
                    meta: Optional[dict] = None) -> None:
        """
        Update or insert a dictionary into a JSONL file.
        
        Args:
            name: Name of the JSONL file
            data_dict: Dictionary to save/update
        """
        file_path = self._get_or_create_file_path(name)
        if os.path.exists(file_path):
            update_jsonl(file_path, data_dict, self.timespec, meta=meta)
        else:
            save_jsonl(
                file_path, data_dict, self.timespec, meta=meta,
                slot_bytes=self.meta_slot_bytes,
            )

        self.update_dbmeta(self._get_file_name(name))

    def upsert_dicts(self, dict_dicts: Dict[Any, Dict[str, Dict[str, Any]]]) -> None:
        """
        Update or insert multiple dictionaries into JSONL files.
        
        Args:
            dict_dicts: Dictionary mapping file names to data dictionaries
        """
        for name, data_dict in dict_dicts.items():
            self.upsert_dict(name, data_dict)

    def get_dict(
        self,
        names: List[str] = None,
        lower_key: Optional[Any] = None,
        upper_key: Optional[Any] = None,
        auto_deserialize: bool = True,
    ) -> Dict[str, Dict[str, Dict[str, Any]]]:
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

    def get_dict_with_meta(
        self,
        name: str,
        lower_key: Optional[Any] = None,
        upper_key: Optional[Any] = None,
        auto_deserialize: bool = True,
    ) -> TableWithMeta:
        """Read one table's metadata first, followed by its dictionary rows."""
        file_path = self._get_file_path(name)
        if not os.path.exists(file_path):
            return TableWithMeta(None, {})
        meta = self.read_meta(name)
        rows = select_jsonl(
            file_path, lower_key, upper_key, auto_deserialize,
            timespec=self.timespec,
        )
        return TableWithMeta(meta, rows)

    # =============== Delete Operations ===============
    def clear_folder(self, force: bool = False) -> None:
        """
        Clear all JSONL files in the database folder.
        """
        if not force:
            logger.warning(
                "This will delete all data in the database folder. "
                "Call clear_folder with force=True to proceed."
            )
            return
        for root, dirs, files in os.walk(self.folder_path, topdown=True):
            # Skip hidden/system directories (e.g. .git, .invalid_tickers)
            dirs[:] = [d for d in dirs if not d.startswith('.')]
            for file in files:
                if file.endswith(('.idx', '.jsonl', '.meta')):
                    os.remove(os.path.join(root, file))
        self.delete_empty_folders()
        self.build_dbmeta()

    def delete_file(self, name: str) -> None:
        """
        Delete a JSONL file.

        Args:
            name: Name of the JSONL file
        """
        file_path = self._get_file_path(name)
        if os.path.exists(file_path):
            os.remove(file_path)
            if os.path.exists(file_path + '.idx'):
                os.remove(file_path + '.idx')
            if self.use_hierarchy:
                self.delete_empty_folders()
        self.delete_dbmeta(name)

    def delete_file_keys(self, name: str, keys: List[str]) -> None:
        """
        Delete specific keys from a JSONL file.

        Args:
            name: Name of the JSONL file
            keys: List of keys to delete
        """
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
            delete_jsonl(file_path, keys_to_delete, self.timespec)
            self.update_dbmeta(name)

    def delete_range(self, names: List[str], lower_key: Any, upper_key: Any) -> None:
        """
        Delete all keys within a range from multiple JSONL files.
        
        Args:
            names: List of JSONL file names
            lower_key: Lower bound of the key range
            upper_key: Upper bound of the key range
        """
        for name in names:
            self.delete_file_range(name, lower_key, upper_key)

    # =============== Metadata Management ===============
    def set_meta_slot_bytes(self, width: int = 4096) -> None:
        """Enable or resize metadata slots across every table in the folder."""
        jsonlfile.metaslot.encode_slot(None, width)
        tables = []
        blockers = []
        for name in sorted(self.get_file_list()):
            file_path = self._get_file_path(name)
            info = jsonlfile.metaslot.inspect_file(file_path)
            tables.append((name, file_path, info))
            if info.is_slot:
                try:
                    jsonlfile.metaslot.encode_slot(info.record, width)
                except (TypeError, ValueError):
                    blockers.append(name)

        if blockers:
            raise ValueError(
                "metadata records do not fit in %d bytes: %s"
                % (width, ", ".join(blockers))
            )

        self.meta_slot_bytes = width
        try:
            self.build_configmeta()
        except BaseException:
            config_meta, _ = _load_control(self.configmeta_path, "config")
            self._config_meta = dict(config_meta)
            self.meta_slot_bytes = config_meta.get("meta_slot_bytes")
            raise
        for _, file_path, _ in tables:
            jsonlfile.migrate_jsonl_slot(file_path, width)

    def read_meta(self, name: str):
        """Return one table's metadata record, or ``None`` when unavailable."""
        file_path = self._get_file_path(name)
        if not os.path.exists(file_path):
            return None
        return jsonlfile.read_jsonl_meta(file_path)

    def clear_meta(self, name: str) -> None:
        """Clear an existing table's slot record without changing its rows."""
        file_path = self._get_file_path(name)
        if not os.path.exists(file_path):
            return
        jsonlfile.write_jsonl_meta(file_path, None)

    def _make_meta_entry(self, name: str, file_path: str, linted: bool = False,
                         lint_time: str = "") -> Dict[str, Any]:
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
        # Get all JSONL files
        jsonl_files = self.get_file_list()
        
        # Initialize metadata dictionary
        metadata = {}
        
        # Process each JSONL file
        for name in jsonl_files:
            file_path = self._get_file_path(name)

            # Build index if it doesn't exist
            if not os.path.exists(file_path + '.idx'):
                build_jsonl_index(file_path)

            metadata[name] = self._make_meta_entry(name, file_path)
        
        # Save metadata using jsonlfile
        save_jsonl(self.dbmeta_path, metadata)

    def get_dbmeta(self) -> Dict[str, Any]:
        """
        Get the database metadata as a dictionary.
        
        Returns:
            Dictionary containing metadata for all JSONL files in the database
        """
        if not os.path.exists(self.dbmeta_path):
            self.build_dbmeta()
        return load_jsonl(self.dbmeta_path)
    
    def delete_dbmeta(self, name: str) -> None:
        """
        Delete the metadata for a specific JSONL file in db.meta.
        """
        if not os.path.exists(self.dbmeta_path):
            self.build_dbmeta()
        meta_key = name[:-6] if name.endswith('.jsonl') else name
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

        # Use hierarchical path for the data file
        file_path = self._get_file_path(name)
        lint_time = datetime.now().isoformat() if linted else ""
        entry = self._make_meta_entry(meta_key, file_path, linted, lint_time)

        # Update metadata file using jsonlfile
        update_jsonl(self.dbmeta_path, {meta_key: entry})

    def lint_db(self, force: bool = False) -> None:
        """Lint the database and replace its scoped persistent report."""
        with capture_report(self.folder_path, "lint_db", "lint.log"):
            self._lint_db(force)

    def _lint_db(self, force: bool = False) -> None:
        """Lint all JSONL files in the database.

        Args:
            force: If True, run full mmap line-count verification on every file.
                   If False (default), skip the scan when the index is fresh.
        """
        meta_file = os.path.join(self.folder_path, "db.meta")
        if not os.path.exists(meta_file):
            self.build_dbmeta()

        metadata = select_jsonl(meta_file)
        names = sorted(set(metadata) | set(self.get_file_list()))
        logger.info("found %d JSONL files to lint in %s",
                    len(names), self.folder_path)

        all_meta = {}

        for name in names:
            file_path = self._get_file_path(name)
            logger.info("linting file %s", file_path)
            exist_flag = lint_jsonl(
                file_path, force=force, slot_bytes=self.meta_slot_bytes)

            if not exist_flag:
                logger.warning("file no longer exists; deleting metadata: %s",
                               file_path,
                               extra={"jsonldb_file": file_path,
                                      "jsonldb_kind": "metadata_removed"})
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

    def lint_hierarchy(self, hierarchy_depth: int) -> None:
        """
        Reorganize JSONL files according to hierarchy levels and move invalid files to .invalid_tickers.
        
        Args:
            hierarchy_depth: Target hierarchy depth
        """

        if hierarchy_depth < 1:
            raise ValueError("Hierarchy level must be positive")

        import shutil
            
        logger.info("organizing %s for hierarchy level %d",
                    self.folder_path, hierarchy_depth)
            
        self.use_hierarchy = True
        self.hierarchy_depth = hierarchy_depth


        # Create .invalid_tickers folder if it doesn't exist
        if not os.path.exists(self.invalid_tickers_path):
            os.makedirs(self.invalid_tickers_path, exist_ok=True)
        
        # Get all JSONL files in the root folder and immediate subdirectories
        all_files = []
        for root, dirs, files in os.walk(self.folder_path, topdown=True):
            # Skip hidden/system directories (starting with '.')
            dirs[:] = [d for d in dirs if not d.startswith('.')]
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
        
        logger.info("found %d valid files and %d invalid files in %s",
                    len(valid_files), len(invalid_files), self.folder_path)
        
        # Move invalid files to .invalid_tickers folder
        for name, file_path in invalid_files:
            try:
                dest_path = os.path.join(self.invalid_tickers_path, os.path.basename(file_path))
                if file_path != dest_path:  # Avoid moving file to itself
                    shutil.move(file_path, dest_path)
                    logger.info("moved invalid file %s to %s",
                                file_path, dest_path)
                    
                    # Also move .idx file if it exists
                    idx_path = file_path + '.idx'
                    if os.path.exists(idx_path):
                        idx_dest = dest_path + '.idx'
                        shutil.move(idx_path, idx_dest)
            except Exception as e:
                logger.warning("could not move invalid file %s: %s",
                               file_path, e)
        
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
                
                # Move JSONL file
                shutil.move(file_path, target_file)
                logger.info("moved %s to %s", file_path, target_file)
                
                # Move .idx file if it exists
                idx_path = file_path + '.idx'
                if os.path.exists(idx_path):
                    idx_target = target_file + '.idx'
                    shutil.move(idx_path, idx_target)
                    
            except Exception as e:
                logger.warning("could not move valid file %s: %s",
                               file_path, e)
        
        # Clean up empty directories
        self.delete_empty_folders()
        
        # Rebuild metadata to reflect new structure
        self.build_dbmeta()
        self.build_hmeta()
        
        logger.info("hierarchy organization completed for %s", self.folder_path)

    def reprocess_invalid_tickers(self) -> None:
        """
        Reprocess files in .invalid_tickers folder and move any that now match naming convention.
        """
        import shutil
        
        if not os.path.exists(self.invalid_tickers_path):
            logger.info("no invalid-tickers folder found at %s",
                        self.invalid_tickers_path)
            return
            
        # Get all JSONL files in .invalid_tickers folder
        invalid_files = []
        for file in os.listdir(self.invalid_tickers_path):
            if file.endswith('.jsonl'):
                file_path = os.path.join(self.invalid_tickers_path, file)
                name = os.path.splitext(file)[0]
                invalid_files.append((name, file_path))
        
        if not invalid_files:
            logger.info("no files found in %s", self.invalid_tickers_path)
            return
            
        logger.info("found %d files to reprocess in %s",
                    len(invalid_files), self.invalid_tickers_path)
        
        now_valid_files = []
        still_invalid_files = []
        
        # Check each file against current naming rules
        for name, file_path in invalid_files:
            if self.validate_name(name):
                now_valid_files.append((name, file_path))
            else:
                still_invalid_files.append((name, file_path))
        
        logger.info("%d files in %s are now valid; %d remain invalid",
                    len(now_valid_files), self.invalid_tickers_path,
                    len(still_invalid_files))
        
        # Move now-valid files to appropriate hierarchy folders
        for name, file_path in now_valid_files:
            try:
                target_dir = self._get_hierarchy_path(name)
                target_file = os.path.join(target_dir, os.path.basename(file_path))
                
                # Create target directory if needed
                self.create_folder(target_dir)
                
                # Move JSONL file
                shutil.move(file_path, target_file)
                logger.info("moved %s to %s", file_path, target_file)
                
                # Move .idx file if it exists (but don't build new one yet)
                idx_path = file_path + '.idx'
                if os.path.exists(idx_path):
                    idx_target = target_file + '.idx'
                    shutil.move(idx_path, idx_target)
                else:
                    # Build index for newly valid file
                    build_jsonl_index(target_file)
                
                # Update metadata for this file
                self.update_dbmeta(name)
                    
            except Exception as e:
                logger.warning("could not move file %s: %s", file_path, e)
        
        # Rebuild metadata to include newly valid files
        if now_valid_files:
            self.build_dbmeta()
            
        logger.info("reprocessing completed for %s", self.invalid_tickers_path)

    def delete_empty_folders(self) -> None:
        """
        Delete empty folders in the database.
        Recursively removes empty folders from bottom up.
        """
        for root, dirs, files in os.walk(self.folder_path, topdown=False):
            if root == self.folder_path:
                continue
            try:
                if not os.listdir(root):
                    os.rmdir(root)
            except OSError:
                pass
