"""
A simple file-based database that stores data in JSONL format.
Each table is stored in a separate JSONL file.
"""

import os
import json
import pandas as pd
from typing import Dict, List, Union, Optional, Any
from datetime import datetime
from jsonldb.jsonlfile import (
    save_jsonl, load_jsonl, select_jsonl, update_jsonl, delete_jsonl,
    lint_jsonl, build_jsonl_index, select_line_jsonl
)
from jsonldb.jsonldf import (
    save_jsonldf, load_jsonldf, update_jsonldf, select_jsonldf, delete_jsonldf
)
import jsonldb.jsonlfile as jsonlfile

from .vercontrol import init_folder, commit as vercontrol_commit, revert as vercontrol_revert, list_version, is_versioned

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

        if os.path.exists(self.configmeta_path):
            config_meta = select_jsonl(self.configmeta_path) 
            if config_meta:
               if "timespec" in config_meta:
                #    print(f"Using timespec: {config_meta['timespec']}")
                   jsonlfile.TIME_SPEC = config_meta["timespec"]

        
        self.build_dbmeta()
        self.build_configmeta()


   

    def build_hmeta(self) -> None:
        """
        Save the folder information to a file.
        """
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
        config_info = {
            "timespec": jsonlfile.TIME_SPEC
        }
        save_jsonl(self.configmeta_path, config_info)


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
        
        # In hierarchy mode, name must contain exactly hierarchy_depth-1 delimiters
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
        """Get the full path for a JSONL file"""
        if self.use_hierarchy and not self.validate_name(name):
            raise ValueError(f"Invalid hierarchical name '{name}'. Name must contain exactly {self.hierarchy_depth-1} '{self.delimiter}' delimiters")
            
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
    def overwrite_df(self, name: str, df: pd.DataFrame) -> None:
        file_path = self._get_file_path(name)
        if os.path.exists(file_path):
           os.remove(file_path)
      
        save_jsonldf(file_path, df)
        self.update_dbmeta(self._get_file_name(name))

    def overwrite_dfs(self, dict_dfs: Dict[Any, pd.DataFrame]) -> None:
        """
        Update or insert multiple DataFrames into JSONL files.
        
        Args:
            dict_dfs: Dictionary mapping file names to DataFrames
        """
        for name, df in dict_dfs.items():
            self.overwrite_df(name, df)

    def upsert_df(self, name: str, df: pd.DataFrame) -> None:
        """
        Update or insert a DataFrame into a JSONL file.
        
        Args:
            name: Name of the JSONL file
            df: DataFrame to save/update
        """
        file_path = self._get_file_path(name)
        if os.path.exists(file_path):
            update_jsonldf(file_path, df)
        else:
            save_jsonldf(file_path, df)
        
        self.update_dbmeta(self._get_file_name(name))

    def upsert_dfs(self, dict_dfs: Dict[Any, pd.DataFrame]) -> None:
        """
        Update or insert multiple DataFrames into JSONL files.
        
        Args:
            dict_dfs: Dictionary mapping file names to DataFrames
        """
        for name, df in dict_dfs.items():
            self.upsert_df(name, df)

    def get_df(self, names: List[str]=None, lower_key: Optional[Any] = None, upper_key: Optional[Any] = None) -> Dict[str, pd.DataFrame]:
        """
        Get DataFrames from multiple JSONL files within a key range.
        
        Args:
            names: List of JSONL file names
            lower_key: Lower bound of the key range
            upper_key: Upper bound of the key range
            
        Returns:
            Dictionary mapping file names to selected DataFrames
        """
        if names is None:
            names = self.get_file_list()

        result = {}
        for name in names:
            file_path = self._get_file_path(name)
            if os.path.exists(file_path):
                result[name] = select_jsonldf(file_path, lower_key, upper_key)
            else:
                print(f"File {name} not found")
        return result

    # =============== Dictionary Operations ===============
    def overwrite_dict(self, name: str, data_dict: Dict[Any, Dict[str, Any]]) -> None:
        file_path = self._get_file_path(name)
        if os.path.exists(file_path):
           os.remove(file_path)
      
        save_jsonl(file_path, data_dict)
        self.update_dbmeta(self._get_file_name(name))

    def overwrite_dicts(self, dict_dicts: Dict[Any, Dict[str, Dict[str, Any]]]) -> None:
        """
        Update or insert multiple DataFrames into JSONL files.
        
        Args:
            dict_dfs: Dictionary mapping file names to DataFrames
        """
        for name, data_dict in dict_dicts.items():
            self.overwrite_dict(name, data_dict)

    def upsert_dict(self, name: str, data_dict: Dict[Any, Dict[str, Any]]) -> None:
        """
        Update or insert a dictionary into a JSONL file.
        
        Args:
            name: Name of the JSONL file
            data_dict: Dictionary to save/update
        """
        file_path = self._get_file_path(name)
        if os.path.exists(file_path):
            update_jsonl(file_path, data_dict)
        else:
            save_jsonl(file_path, data_dict)

        self.update_dbmeta(self._get_file_name(name))

    def upsert_dicts(self, dict_dicts: Dict[Any, Dict[str, Dict[str, Any]]]) -> None:
        """
        Update or insert multiple dictionaries into JSONL files.
        
        Args:
            dict_dicts: Dictionary mapping file names to data dictionaries
        """
        for name, data_dict in dict_dicts.items():
            self.upsert_dict(name, data_dict)

    def get_dict(self, names: List[str]=None, lower_key: Optional[Any] = None, upper_key: Optional[Any] = None) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """
        Get dictionaries from multiple JSONL files within a key range.
        
        Args:
            names: List of JSONL file names
            lower_key: Lower bound of the key range
            upper_key: Upper bound of the key range
            
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
                result[name] = select_jsonl(file_path, lower_key, upper_key)
        return result

    # =============== Delete Operations ===============
    def clear_folder(self,force=False) -> None:
        """
        Clear all JSONL files in the database folder.
        """
        if not force:
            print("WARNING: This will delete all data in the database folder. Call clear_folder with force=True to proceed.")
            return
        for file in os.listdir(self.folder_path):
            if file.endswith(('.idx', '.jsonl', '.meta')):
                os.remove(os.path.join(self.folder_path, file))
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
            os.remove(os.path.join(file_path + '.idx'))
            if self.use_hierarchy:
                self.delete_empty_folders()

    def delete_file_keys(self, name: str, keys: List[str]) -> None:
        """
        Delete specific keys from a JSONL file.
        
        Args:
            name: Name of the JSONL file
            keys: List of keys to delete
        """
        file_path = self._get_file_path(name)
        if os.path.exists(file_path):
            delete_jsonl(file_path, keys)
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
            
        # Get all keys from the index
        index_path = file_path + '.idx'
        if not os.path.exists(index_path):
            build_jsonl_index(file_path)
            
        # Read the index file
        with open(index_path, 'r') as f:
            index = json.load(f)
            
        # Filter keys within range
        keys_to_delete = [
            key for key in index.keys()
            if str(lower_key) <= key <= str(upper_key)
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
        for name in names:
            self.delete_file_range(name, lower_key, upper_key)

    # =============== Metadata Management ===============
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
        
        if not jsonl_files:
            # If no JSONL files are found, create an empty db.meta file
            with open(self.dbmeta_path, 'w', encoding='utf-8') as f:
                f.write('\n')
            return
            
        # Initialize metadata dictionary
        metadata = {}
        
        # Process each JSONL file
        for name in jsonl_files:
            # print(name)
            file_path = self._get_file_path(name)
            index_file = file_path + '.idx'
            
            # Build index if it doesn't exist
            if not os.path.exists(index_file):
                build_jsonl_index(file_path)
            
            # Get index range from index file
            min_index = None
            max_index = None
            count = 0
            
            if os.path.exists(index_file):
                with open(index_file, 'r') as f:
                    index = json.load(f)
                    if index:
                        keys = list(index.keys())
                        if keys:
                            min_index = keys[0]
                            max_index = keys[-1]
                            count = len(keys)
            
            file_path = self._get_file_path(name)
            # Create metadata entry using name without extension as key
            metadata[name] = {
                "name": name,
                "path": file_path,
                "min_index": min_index,
                "max_index": max_index,
                "size": os.path.getsize(file_path),
                "count": count,
                "lint_time": "",
                "linted": False,  # Default to False
            }
        
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
    
    def delete_dbmeta(self,name: str) -> None:
        """
        Delete the metadata for a specific JSONL file in db.meta.
        """
        if not os.path.exists(self.dbmeta_path):
            self.build_dbmeta()
        delete_jsonl(self.dbmeta_path, [name])
    
    def update_dbmeta(self, name: str, linted: bool = False) -> None:
        """
        Update the metadata for a specific JSONL file in db.meta.
        
        Args:
            name: Name of the JSONL file (with or without .jsonl extension)
            linted: Value to set for the linted field
        """
        # Get name without extension for metadata key
        jsonl_file = name if name.endswith('.jsonl') else f"{name}.jsonl"
        meta_key = name.replace('.jsonl', '')

        # Load existing metadata
        metadata = {}
        if os.path.exists(self.dbmeta_path):
            metadata = select_line_jsonl(self.dbmeta_path, meta_key)
        
        # Get index range from index file (use hierarchical path)
        file_path = self._get_file_path(name)
        index_file = file_path + ".idx"
        min_index = None
        max_index = None
        count = 0

        if os.path.exists(index_file):
            with open(index_file, 'r') as f:
                index = json.load(f)
                if index:
                    keys = list(index.keys())
                    if keys:
                        min_index = keys[0]
                        max_index = keys[-1]
                        count = len(keys)

        lint_time = datetime.now().isoformat() if linted else ""
        # print(f"Updating metadata for {name} with path {file_path}")

        # Update metadata for the specified file using name without extension as key
        metadata[meta_key] = {
            "name": meta_key,
            "path": file_path,
            "min_index": min_index,
            "max_index": max_index,
            "size": os.path.getsize(file_path),
            "count": count,
            "lint_time": lint_time,
            "linted": linted
        }
        
        # Update metadata file using jsonlfile
        update_jsonl(self.dbmeta_path, {meta_key: metadata[meta_key]})

    def lint_db(self) -> None:
        """Lint all JSONL files in the database."""
        meta_file = os.path.join(self.folder_path, "db.meta")
        if not os.path.exists(meta_file):
            self.build_dbmeta()
            
        metadata = select_jsonl(meta_file)
        print(f"Found {len(metadata)} JSONL files to lint.")
        
        for name in metadata:
            print(f"Linting file: {name}")
            file_path = self._get_file_path(name)
            
            # try:
                # Try to lint the file
            exist_flag = lint_jsonl(file_path)
            if not exist_flag:
                print(f"File {name} no longer exist, deleting metadata.")
                self.delete_dbmeta(name)
            else:
                # print(f"Successfully linted and updated metadata for {name}.")
                self.update_dbmeta(name, linted=True)
            # except Exception as e:
            #     print(f"Error linting {name}: {str(e)}")
            #     self.update_dbmeta(name, linted=False)

        lint_jsonl(self.dbmeta_path)

        #if using hierarchy, then we need to lint the h.meta file
        if self.use_hierarchy:
            lint_jsonl(self.hmeta_path)

            self.delete_empty_folders()

    def lint_hierarchy(self, hierarchy_depth:int) -> None:
        """
        Reorganize JSONL files according to hierarchy levels and move invalid files to .invalid_tickers.
        
        Args:
            hierarchy_level: Target hierarchy level. If None, uses value from h.meta or defaults to 1
        """

        if hierarchy_depth < 1:
            raise ValueError("Hierarchy level must be positive")

        import shutil
            
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
        
        print(f"Found {len(valid_files)} valid files and {len(invalid_files)} invalid files")
        
        # Move invalid files to .invalid_tickers folder
        for name, file_path in invalid_files:
            try:
                dest_path = os.path.join(self.invalid_tickers_path, os.path.basename(file_path))
                if file_path != dest_path:  # Avoid moving file to itself
                    shutil.move(file_path, dest_path)
                    print(f"Moved invalid file {name} to .invalid_tickers")
                    
                    # Also move .idx file if it exists
                    idx_path = file_path + '.idx'
                    if os.path.exists(idx_path):
                        idx_dest = dest_path + '.idx'
                        shutil.move(idx_path, idx_dest)
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
                
                # Move JSONL file
                shutil.move(file_path, target_file)
                print(f"Moved {name} to {target_dir}")
                
                # Move .idx file if it exists
                idx_path = file_path + '.idx'
                if os.path.exists(idx_path):
                    idx_target = target_file + '.idx'
                    shutil.move(idx_path, idx_target)
                    
            except Exception as e:
                print(f"Warning: Could not move valid file {name}: {str(e)}")
        
        # Clean up empty directories
        self.delete_empty_folders()
        
        # Rebuild metadata to reflect new structure
        self.build_dbmeta()
        self.build_hmeta()
        
        print("Hierarchy organization completed")

    def reprocess_invalid_tickers(self) -> None:
        """
        Reprocess files in .invalid_tickers folder and move any that now match naming convention.
        """
        import shutil
        
        if not os.path.exists(self.invalid_tickers_path):
            print("No .invalid_tickers folder found")
            return
            
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
                
                # Move JSONL file
                shutil.move(file_path, target_file)
                print(f"Moved {name} from .invalid_tickers to {target_dir}")
                
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
        # Check if folder is a git repo, if not initialize it
        if not is_versioned(self.folder_path):
            init_folder(self.folder_path)
            
        # Commit changes
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
        vercontrol_revert(self.folder_path, version_hash)
        print(f"Successfully reverted the folder: {self.folder_path} to version: {version_hash}")
    
    def version(self) -> Dict[str, str]:
        """
        List all versions of the database.
        
        Returns:
            Dictionary with commit hashes as keys and commit messages as values
            
        Raises:
            git.exc.GitCommandError: If git commands fail
        """
        return list_version(self.folder_path)
