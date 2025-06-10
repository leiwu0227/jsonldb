"""
Visualization functions for JSONL files and FolderDB using Bokeh.
"""

import os
import json
from datetime import datetime
from typing import Dict, List, Union, Optional
import pandas as pd
from bokeh.plotting import figure
from bokeh.models import ColumnDataSource, HoverTool
from bokeh.palettes import Category10
import numpy as np
from jsonldb.jsonlfile import load_jsonl, select_jsonl
from jsonldb.folderdb import FolderDB

def _parse_linekey(linekey: str) -> Union[float, datetime]:
    """
    Parse a linekey string into either a number or datetime.
    Returns a float if the string can be converted to a number,
    or a datetime if the string matches a datetime format.
    """
    try:
        # Try to convert to float first
        return float(linekey)
    except ValueError:
        try:
            # Try to parse as datetime
            return datetime.fromisoformat(linekey)
        except ValueError:
            # If neither, use the string's hash as a number
            return float(hash(linekey))

def _is_datetime_key(key: str) -> bool:
    """
    Check if a key string is in datetime format.
    """
    try:
        datetime.fromisoformat(key)
        return True
    except ValueError:
        return False

def visualize_jsonl(jsonl_path: str) -> figure:
    """
    Create a scatter plot visualization of a JSONL file's linekeys using Bokeh.
    
    Args:
        jsonl_path: Path to the JSONL file
        
    Returns:
        Bokeh figure object showing the scatter plot
    """
    # Get the index file path
    idx_path = jsonl_path + '.idx'
    
    if not os.path.exists(idx_path):
        raise FileNotFoundError(f"Index file not found: {idx_path}")
    
    # Read the index file
    with open(idx_path, 'r') as f:
        index_data = json.load(f)
    
    # Convert linekeys to numbers or datetimes
    linekeys = [_parse_linekey(k) for k in index_data.keys()]
    line_numbers = list(index_data.values())
    

    # Determine if linekeys are datetime
    x_axis_type = "datetime" if isinstance(linekeys[0], datetime) else "linear"
    
    # Create the figure
    p = figure(
        title="JSONL Line Keys Distribution",
        x_axis_label="Line Key",
        y_axis_label="Line Number",
        tools="pan,wheel_zoom,box_zoom,reset,save",
        x_axis_type=x_axis_type
    )
    
    # Add hover tool
    hover = HoverTool(
        tooltips=[
            ("Line Key", "@x{%F %T}" if x_axis_type == "datetime" else "@x"),
            ("Line Number", "@y")
        ],
        formatters={
            '@x': 'datetime' if x_axis_type == "datetime" else 'numeral'
        }
    )
    p.add_tools(hover)
    
    # Create the scatter plot
    source = ColumnDataSource(data={
        'x': linekeys,
        'y': line_numbers
    })
    
    p.scatter('x', 'y', source=source, size=1, alpha=0.6)
    
    # Customize the plot
    p.grid.grid_line_color = "gray"
    p.grid.grid_line_alpha = 0.3
    
    return p

def visualize_folderdb(folderdb: FolderDB,prefix: str = None) -> figure:
    """
    Create a scatter plot visualization of all JSONL files in a FolderDB using Bokeh.
    
    Args:
        folder_path: Path to the FolderDB directory
        
    Returns:
        Bokeh figure object showing the scatter plot
    """
    # db = FolderDB(folder_path)
    # Get all JSONL files in the folder
    jsonl_files = folderdb.get_file_list()
    
    if not jsonl_files:
        raise FileNotFoundError(f"No JSONL files found in: {folderdb.folder_path}")
    
    print(f"Found {len(jsonl_files)} JSONL files in {folderdb.folder_path}")

    if prefix is not None:
        jsonl_files = [file for file in jsonl_files if file.startswith(prefix)]
        print(f"Found {len(jsonl_files)} JSONL files with prefix: {prefix}")
    
    # Prepare data for plotting
    colors = ["orange"]  # Use orange color for all files
    
    # Check if all first keys are datetime
    all_datetime = True
    for file_name in jsonl_files:
        idx_path = folderdb._get_file_path(file_name) + '.idx'
        if not os.path.exists(idx_path):
            print(f"Warning: Index file not found for {idx_path}")
            continue
            
        # Read the index file
        with open(idx_path, 'r') as f:
            index_data = json.load(f)
        
        if index_data:
            first_key = next(iter(index_data.keys()))
            if not _is_datetime_key(first_key):
                all_datetime = False
                break
    
    # Create the figure with appropriate x-axis type
    p = figure(
        title="FolderDB Line Keys Distribution",
        x_axis_label="Line Key",
        y_axis_label="File Name",
        tools="pan,wheel_zoom,box_zoom,reset,save",
        x_axis_type="datetime" if all_datetime else "linear",
        y_range=[file_name.replace('.jsonl', '') for file_name in jsonl_files],  # Set y-axis as categorical with filenames
        width=800,
        height=600
    )
    
    # Plot each file's data
    has_data = False
    for i, file_name in enumerate(jsonl_files):
        idx_path = folderdb._get_file_path(file_name) + '.idx'
        if not os.path.exists(idx_path):
            print(f"Warning: Index file not found for {idx_path}")
            continue
            
        # Read the index file
        with open(idx_path, 'r') as f:
            index_data = json.load(f)
        
        if not index_data:  # Skip empty files
            print(f"Warning: Empty index file for {file_name}")
            continue
            
        # print(f"Processing {file_name} with {len(index_data)} entries")
            
        # Convert linekeys to numbers or datetimes
        linekeys = [_parse_linekey(k) for k in index_data.keys()]
        
        # Create data source for this file
        source = ColumnDataSource(data={
            'x': linekeys,
            'y': [file_name.replace('.jsonl', '')] * len(linekeys),
            'filename': [file_name.replace('.jsonl', '')] * len(linekeys)

        })
  
        # Add scatter plot
        p.scatter(
            'x', 'y',
            source=source,
            size=1,
            alpha=0.6,
            color="orange",
            legend_label=file_name.replace('.jsonl', '')
        )
        has_data = True
        # print(f"Added {len(linekeys)} points for {file_name}")
    
    if not has_data:
        print("Warning: No valid data found to plot")
        # Add a dummy point to prevent the "no renderers" warning
        p.scatter([0], ["No Data"], size=0, alpha=0)
    
    # Customize the plot
    p.grid.grid_line_color = "gray"
    p.grid.grid_line_alpha = 0.3
    
    # Only set legend properties if we have data
    if has_data:
        p.legend.click_policy = "hide"  # Allow toggling visibility of each file

    p.legend.visible = False
    
    return p 