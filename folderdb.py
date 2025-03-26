import os
from typing import Dict, List, Optional, Union, Any
import pandas as pd
from jsonldf import save_jsonldf, update_jsonldf, select_jsonldf, delete_jsonldf
from jsonlfile import save_jsonl, update_jsonl, select_jsonl, delete_jsonl, build_jsonl_index
import json

class FolderDB:
    def __init__(self, folder_path: str):
        """
        Initialize FolderDB with a folder path.
        
        Args:
            folder_path: Path to the folder containing JSONL files
            
        Raises:
            FileNotFoundError: If the folder doesn't exist
        """
        if not os.path.exists(folder_path):
            raise FileNotFoundError(f"Folder {folder_path} does not exist")
        self.folder_path = folder_path

    def _get_file_path(self, name: str) -> str:
        """Get the full path for a JSONL file"""
        if name.endswith('.jsonl'):
            return os.path.join(self.folder_path, name)
        return os.path.join(self.folder_path, f"{name}.jsonl")

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

    def __str__(self) -> str:
        """Return a string representation of the FolderDB"""
        result = []
        result.append(f"FolderDB at {self.folder_path}")
        result.append("-" * 50)
        
        # Get all JSONL files
        jsonl_files = [f for f in os.listdir(self.folder_path) if f.endswith('.jsonl')]
        
        if not jsonl_files:
            result.append("No JSONL files found")
        else:
            result.append(f"Found {len(jsonl_files)} JSONL files:")
            for name in sorted(jsonl_files):
                file_path = self._get_file_path(name)
                size = os.path.getsize(file_path)
                
                # Get key range and count from index
                index_path = file_path + '.idx'
                if os.path.exists(index_path):
                    with open(index_path, 'r') as f:
                        keys = [line.strip() for line in f]
                    key_range = f"{keys[0]} to {keys[-1]}" if keys else "empty"
                    count = len(keys)
                else:
                    key_range = "no index"
                    count = "unknown"
                
                result.append(f"\n{name}:")
                result.append(f"  Size: {size:,} bytes")
                result.append(f"  Key range: {key_range}")
                result.append(f"  Count: {count}")
        
        return "\n".join(result) 