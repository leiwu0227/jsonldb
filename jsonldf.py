import pandas as pd
from typing import Optional, Tuple, List, Union
from jsonldb.jsonlfile import (
    save_jsonl, load_jsonl, update_jsonl, 
    select_jsonl, delete_jsonl, lint_jsonl
)

def save_jsonldf(jsonl_file_path: str, df: pd.DataFrame) -> None:
    """Convert DataFrame to JSONL format and save it using index as keys.
    
    Args:
        jsonl_file_path (str): Path to the JSONL file
        df (pd.DataFrame): DataFrame to save
        
    Raises:
        ValueError: If DataFrame index is not unique
    """
    if not df.index.is_unique:
        raise ValueError("DataFrame index must be unique")
    
    # Convert DataFrame to dict using index as keys
    records_dict = df.to_dict('index')
    
    # Save to JSONL
    save_jsonl(jsonl_file_path, records_dict)

def load_jsonldf(jsonl_file_path: str) -> pd.DataFrame:
    """Load JSONL file into a DataFrame using line keys as index.
    
    Args:
        jsonl_file_path (str): Path to the JSONL file
        
    Returns:
        pd.DataFrame: DataFrame containing the JSONL data with line keys as index
    """
    # Load JSONL data
    records_dict = load_jsonl(jsonl_file_path)
    
    if not records_dict:
        # Return empty DataFrame
        return pd.DataFrame()
    
    # Convert dict to DataFrame, keeping keys as index
    df = pd.DataFrame.from_dict(records_dict, orient='index')
    
    return df

def update_jsonldf(jsonl_file_path: str, df: pd.DataFrame) -> None:
    """Update JSONL file with data from DataFrame using index as keys.
    
    Args:
        jsonl_file_path (str): Path to the JSONL file
        df (pd.DataFrame): DataFrame containing updates
    """
    # Convert DataFrame to dict using index as keys
    updates_dict = df.to_dict('index')
    
    # Update JSONL file
    update_jsonl(jsonl_file_path, updates_dict)

def select_jsonldf(
    jsonl_file_path: str,
    key_range: Optional[Tuple[str, str]] = None
) -> pd.DataFrame:
    """Select records from JSONL file within the specified key range.
    
    Args:
        jsonl_file_path (str): Path to the JSONL file
        key_range (tuple, optional): Tuple of (lower_key, upper_key). Defaults to None
        
    Returns:
        pd.DataFrame: DataFrame containing the selected records with line keys as index
    """
    # Select records from JSONL
    records_dict = select_jsonl(jsonl_file_path, key_range)
    
    if not records_dict:
        # Return empty DataFrame
        return pd.DataFrame()
    
    # Convert dict to DataFrame, keeping keys as index
    df = pd.DataFrame.from_dict(records_dict, orient='index')
    
    return df

def delete_jsonldf(jsonl_file_path: str, keys: List[Union[str, int]]) -> None:
    """Delete records from JSONL file by their keys.
    
    Args:
        jsonl_file_path (str): Path to the JSONL file
        keys (list): List of keys to delete
    """
    delete_jsonl(jsonl_file_path, keys)

def lint_jsonldf(jsonl_file_path: str) -> None:
    """Sort and clean the JSONL file.
    
    Args:
        jsonl_file_path (str): Path to the JSONL file
    """
    lint_jsonl(jsonl_file_path) 