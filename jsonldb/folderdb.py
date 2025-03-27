"""
A simple file-based database that stores data in JSONL format.
Each table is stored in a separate JSONL file.
"""

import os
import json
import pandas as pd
from typing import Dict, List, Union, Optional, Any
from datetime import datetime
from jsonldb import visual
from jsonldb.jsonlfile import (
    save_jsonl, load_jsonl, select_jsonl, update_jsonl, delete_jsonl,
    lint_jsonl, build_jsonl_index, select_line_jsonl
)
from jsonldb.jsonldf import (
    save_jsonldf, load_jsonldf, update_jsonldf, select_jsonldf, delete_jsonldf
)
from .vercontrol import init_folder, commit as vercontrol_commit, revert as vercontrol_revert, list_version, is_versioned

class FolderDB:
    """
    A simple file-based database that stores data in JSONL format.
    Each table is stored in a separate JSONL file.
    """
    
    def __init__(self, folder_path: str):
        """
        Initialize the database.
        
        Args:
            folder_path: Path to the folder where the database files will be stored
        """
        self.folder_path = folder_path
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            
        self.build_dbmeta()

    # =============== File Path Management ===============
    def _get_file_path(self, name: str) -> str:
        """Get the full path for a JSONL file"""
        if name.endswith('.jsonl'):
            return os.path.join(self.folder_path, name)
        return os.path.join(self.folder_path, f"{name}.jsonl")
    
    def _get_file_name(self, name: str) -> str:
        """Get the name of a JSONL file"""
        if name.endswith('.jsonl'):
            return name
        return f"{name}.jsonl"

    # =============== Data Operations ===============
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

    def get_df(self, names: List[str], lower_key: Optional[Any] = None, upper_key: Optional[Any] = None) -> Dict[str, pd.DataFrame]:
        """
        Get DataFrames from multiple JSONL files within a key range.
        
        Args:
            names: List of JSONL file names
            lower_key: Lower bound of the key range
            upper_key: Upper bound of the key range
            
        Returns:
            Dictionary mapping file names to selected DataFrames
        """
        result = {}
        for name in names:
            file_path = self._get_file_path(name)
            if os.path.exists(file_path):
                result[name] = select_jsonldf(file_path, lower_key, upper_key)
        return result

    def get_dict(self, names: List[str], lower_key: Optional[Any] = None, upper_key: Optional[Any] = None) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """
        Get dictionaries from multiple JSONL files within a key range.
        
        Args:
            names: List of JSONL file names
            lower_key: Lower bound of the key range
            upper_key: Upper bound of the key range
            
        Returns:
            Dictionary mapping file names to selected data dictionaries
        """
        result = {}
        for name in names:
            file_path = self._get_file_path(name)
            if os.path.exists(file_path):
                result[name] = select_jsonl(file_path, lower_key, upper_key)
        return result

    # =============== Delete Operations ===============
    def delete_file(self, name: str, keys: List[str]) -> None:
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
        jsonl_files = [f for f in os.listdir(self.folder_path) if f.endswith('.jsonl')]
        
        self.dbmeta_path = os.path.join(self.folder_path, "db.meta")
        if not jsonl_files:
            # If no JSONL files are found, create an empty db.meta file
            with open(self.dbmeta_path, 'w', encoding='utf-8') as f:
                f.write('\n')
            return
        # Initialize metadata dictionary
        metadata = {}
        
        # Process each JSONL file
        for jsonl_file in jsonl_files:
            # Get index range from index file
            index_file = os.path.join(self.folder_path, f"{jsonl_file}.idx")
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

            # Get name without extension
            name = os.path.splitext(jsonl_file)[0]
            
            # Create metadata entry
            metadata[jsonl_file] = {
                "name": name,
                "min_index": min_index,
                "max_index": max_index,
                "size": os.path.getsize(os.path.join(self.folder_path, jsonl_file)),
                "count": count,
                "lint_time": datetime.now().isoformat(),
                "linted": False  # Default to False
            }
        
        # Save metadata using jsonlfile
        save_jsonl(self.dbmeta_path, metadata)

    def update_dbmeta(self, name: str, linted: bool = False) -> None:
        """
        Update the metadata for a specific JSONL file in db.meta.
        
        Args:
            name: Name of the JSONL file (with or without .jsonl extension)
            linted: Value to set for the linted field
        """
        jsonl_file = name if name.endswith('.jsonl') else f"{name}.jsonl"
        
        # Load existing metadata
        metadata = {}
        if os.path.exists(self.dbmeta_path):
            metadata = select_line_jsonl(self.dbmeta_path, jsonl_file)
        
        # Get index range from index file
        index_file = os.path.join(self.folder_path, f"{jsonl_file}.idx")
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

        # Update metadata for the specified file
        metadata[jsonl_file] = {
            "name": os.path.splitext(jsonl_file)[0],
            "min_index": min_index,
            "max_index": max_index,
            "size": os.path.getsize(os.path.join(self.folder_path, jsonl_file)),
            "count": count,
            "lint_time": datetime.now().isoformat(),
            "linted": linted
        }
        
        # Update metadata file using jsonlfile
        update_jsonl(self.dbmeta_path, {jsonl_file: metadata[jsonl_file]})

    def lint_db(self) -> None:
        """
        Lint all JSONL files in the database and update metadata.
        
        This function:
        1. Lints each JSONL file using jsonlfile.lint_jsonl
        2. Updates the db.meta file for each file after linting
        """
        # Get all JSONL files
        jsonl_files = [f for f in os.listdir(self.folder_path) if f.endswith('.jsonl')]
        print(f"Found {len(jsonl_files)} JSONL files to lint.")

        # Process each JSONL file
        for jsonl_file in jsonl_files:
            file_path = os.path.join(self.folder_path, jsonl_file)
            print(f"Linting file: {jsonl_file}")
            
            # Lint the file
            try:
                lint_jsonl(file_path)
                # Update metadata with linted=True only if linting succeeds
                self.update_dbmeta(jsonl_file, linted=True)
                print(f"Successfully linted and updated metadata for {jsonl_file}.")
            except Exception as e:
                print(f"Error linting {jsonl_file}: {str(e)}")
                # No need to update metadata again since we already set linted=False 

        lint_jsonl(self.dbmeta_path)

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

    # =============== Visualization ===============
    def visualize(self) -> visual.figure:
        """
        Create a visualization of the database's data distribution.
        
        Returns:
            Bokeh figure object showing the scatter plot of data distribution
        """
        return visual.visualize_folderdb(self.folder_path)

    # =============== String Representation ===============
    def __str__(self) -> str:
        """Return a string representation of the database."""
        lines = [f"FolderDB at {self.folder_path}"]
        lines.append("-" * 50)
        
        dbmeta = load_jsonl(self.dbmeta_path)

        for jsonl_file, meta in dbmeta.items():
            lines.append(f"{jsonl_file}:")
            lines.append(f"  Size: {meta['size']} bytes")
            lines.append(f"  Count: {meta['count']}")
            lines.append(f"  Key range: {meta['min_index']} to {meta['max_index']}")
            lines.append(f"  Linted: {meta['linted']}")

        return "\n".join(lines)
    
    def __repr__(self) -> str:
        return self.__str__()