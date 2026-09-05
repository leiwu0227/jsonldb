"""
DataFrame-specific operations for JSONLDB.
"""


import pandas as pd
from typing import Dict, List, Optional, Union, Any
from jsonldb.jsonlfile import save_jsonl, load_jsonl, select_jsonl, update_jsonl, delete_jsonl, build_jsonl_index, lint_jsonl
from jsonldb.jsonlfile import _save_jsonl, _update_jsonl

def _df_records(df):
    """Validate overwrite uniqueness before converting the records."""
    if not df.index.is_unique:
        raise ValueError("DataFrame index must be unique")
    return df.to_dict('index')

def _save_jsonldf(path, df, timespec=None, meta=None, slot_bytes=None):
    return _save_jsonl(path, _df_records(df), timespec, meta, slot_bytes,
                       with_stats=True)

def _update_jsonldf(path, df, timespec=None, meta=None):
    return _update_jsonl(path, df.to_dict('index'), timespec, meta, with_stats=True)

def save_jsonldf(jsonl_file_path: str, df: pd.DataFrame,
                 timespec: Optional[str] = None,
                 meta: Optional[dict] = None,
                 slot_bytes: Optional[int] = None) -> None:
    """Convert DataFrame to JSONL format and save it using index as keys.
    
    Args:
        jsonl_file_path (str): Path to the JSONL file
        df (pd.DataFrame): DataFrame to save
        
    Raises:
        ValueError: If DataFrame index is not unique
    """
    save_jsonl(
        jsonl_file_path, _df_records(df), timespec, meta=meta,
        slot_bytes=slot_bytes,
    )

def load_jsonldf(jsonl_file_path: str, timespec: Optional[str] = None,
                 auto_deserialize: bool = True) -> pd.DataFrame:
    """Load JSONL file into a DataFrame using line keys as index.
    
    Args:
        jsonl_file_path (str): Path to the JSONL file
        
    Returns:
        pd.DataFrame: DataFrame containing the JSONL data with line keys as index
    """
    # Load JSONL data
    records_dict = load_jsonl(
        jsonl_file_path, auto_deserialize=auto_deserialize, timespec=timespec)
    
    if not records_dict:
        # Return empty DataFrame
        return pd.DataFrame()
    
    # Convert dict to DataFrame, keeping keys as index
    df = pd.DataFrame.from_dict(records_dict, orient='index')
    
    return df

def update_jsonldf(jsonl_file_path: str, df: pd.DataFrame,
                   timespec: Optional[str] = None,
                   meta: Optional[dict] = None) -> None:
    """Update JSONL file with data from DataFrame using index as keys.
    
    Args:
        jsonl_file_path (str): Path to the JSONL file
        df (pd.DataFrame): DataFrame containing updates
    """
    # Convert DataFrame to dict using index as keys
    updates_dict = df.to_dict('index')
    
    # Update JSONL file
    update_jsonl(jsonl_file_path, updates_dict, timespec, meta=meta)

def select_jsonldf(
    jsonl_file_path: str,
    lower_key: Optional[Any] = None,
    upper_key: Optional[Any] = None,
    auto_deserialize: bool = True,
    timespec: Optional[str] = None
) -> pd.DataFrame:
    """
    Select records from JSONL file within a specified key range.
    
    Args:
        jsonl_file_path: Path to the JSONL file
        lower_key: Lower bound of the key range (inclusive). If None, uses smallest key.
        upper_key: Upper bound of the key range (inclusive). If None, uses largest key.
        auto_deserialize: Whether to automatically deserialize datetime keys
        
    Returns:
        DataFrame containing the selected records
        
    Raises:
        ValueError: If lower_key is greater than upper_key
    """
    # Get records within the specified range
    records = select_jsonl(
        jsonl_file_path,
        lower_key=lower_key,
        upper_key=upper_key,
        auto_deserialize=auto_deserialize,
        timespec=timespec
    )
    
    # Convert to DataFrame
    if not records:
        # print(f"No records found in {jsonl_file_path}")
        return pd.DataFrame()
        
    return pd.DataFrame.from_dict(records, orient='index')

def delete_jsonldf(jsonl_file_path: str, keys: List[Union[str, int]], timespec: Optional[str] = None) -> None:
    """Delete records from JSONL file by their keys.
    
    Args:
        jsonl_file_path (str): Path to the JSONL file
        keys (list): List of keys to delete
    """
    delete_jsonl(jsonl_file_path, keys, timespec)

def lint_jsonldf(jsonl_file_path: str, force: bool = False,
                 slot_bytes: Optional[int] = None) -> bool:
    """Sort and clean the JSONL file.
    
    Args:
        jsonl_file_path (str): Path to the JSONL file
    """
    return lint_jsonl(jsonl_file_path, force=force, slot_bytes=slot_bytes)
