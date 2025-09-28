"""
Visualization functions for JSONL files and FolderDB using Bokeh and Matplotlib.
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
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
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

def visualize_jsonl_bokeh(jsonl_path: str) -> figure:
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

def visualize_jsonl_matplot(jsonl_path: str, start_index = None, end_index = None):
    """
    Create a scatter plot visualization of a JSONL file's linekeys using Matplotlib.

    Args:
        jsonl_path: Path to the JSONL file
        start_index: Start linekey value for filtering (inclusive)
        end_index: End linekey value for filtering (exclusive)

    Returns:
        Matplotlib figure and axes objects
    """
    # Get the index file path
    idx_path = jsonl_path + '.idx'

    if not os.path.exists(idx_path):
        raise FileNotFoundError(f"Index file not found: {idx_path}")

    # Read the index file
    with open(idx_path, 'r') as f:
        index_data = json.load(f)

    # Convert linekeys to numbers or datetimes
    all_linekeys = [_parse_linekey(k) for k in index_data.keys()]
    all_line_numbers = list(index_data.values())

    # Apply start_index and end_index filtering based on linekey values
    if start_index is not None or end_index is not None:
        filtered_data = []
        for linekey, line_num in zip(all_linekeys, all_line_numbers):
            include = True
            if start_index is not None:
                if isinstance(linekey, datetime) and isinstance(start_index, str):
                    start_parsed = _parse_linekey(start_index)
                    include = include and linekey >= start_parsed
                else:
                    include = include and linekey >= start_index
            if end_index is not None:
                if isinstance(linekey, datetime) and isinstance(end_index, str):
                    end_parsed = _parse_linekey(end_index)
                    include = include and linekey < end_parsed
                else:
                    include = include and linekey < end_index
            if include:
                filtered_data.append((linekey, line_num))

        if filtered_data:
            linekeys, line_numbers = zip(*filtered_data)
            linekeys = list(linekeys)
            line_numbers = list(line_numbers)
        else:
            linekeys, line_numbers = [], []
    else:
        linekeys = all_linekeys
        line_numbers = all_line_numbers

    # Create the figure and axis
    fig, ax = plt.subplots(figsize=(10, 6))

    # Determine if linekeys are datetime (check if we have any data)
    is_datetime = len(linekeys) > 0 and isinstance(linekeys[0], datetime)

    # Create the scatter plot
    ax.scatter(linekeys, line_numbers, s=1, alpha=0.6, color='blue')

    # Set labels and title
    ax.set_xlabel("Line Key")
    ax.set_ylabel("Line Number")
    ax.set_title("JSONL Line Keys Distribution")

    # Format x-axis for datetime if needed
    if is_datetime:
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
        plt.xticks(rotation=45)

    # Add grid
    ax.grid(True, alpha=0.3, color='gray')

    # Adjust layout to prevent label cutoff
    plt.tight_layout()

    return fig, ax

def visualize_folderdb_bokeh(folderdb: FolderDB,prefix: str = None,height=1200) -> figure:
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
        height=height
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

def visualize_folderdb_matplot(folderdb: FolderDB, prefix: str = None, height: int = 10, start_index = None, end_index = None):
    """
    Create a scatter plot visualization of all JSONL files in a FolderDB using Matplotlib.

    Args:
        folderdb: FolderDB instance
        prefix: Optional prefix to filter files
        height: Maximum figure height in inches
        start_index: Start linekey value for filtering data points (inclusive)
        end_index: End linekey value for filtering data points (exclusive)

    Returns:
        Matplotlib figure and axes objects
    """
    # Get all JSONL files in the folder
    jsonl_files = folderdb.get_file_list()

    if not jsonl_files:
        raise FileNotFoundError(f"No JSONL files found in: {folderdb.folder_path}")

    print(f"Found {len(jsonl_files)} JSONL files in {folderdb.folder_path}")

    if prefix is not None:
        jsonl_files = [file for file in jsonl_files if file.startswith(prefix)]
        print(f"Found {len(jsonl_files)} JSONL files with prefix: {prefix}")

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

    # Calculate auto-adjusting height based on number of files
    # Use a minimum height per file to ensure readability
    min_height_per_file = 0.5  # inches per file
    auto_height = max(3, len(jsonl_files) * min_height_per_file)  # minimum 3 inches
    actual_height = min(auto_height, height)  # use smaller of auto or user-specified

    # Create the figure with appropriate size
    fig, ax = plt.subplots(figsize=(12, actual_height))

    # Plot each file's data
    has_data = False
    colors = plt.cm.tab10(np.linspace(0, 1, len(jsonl_files)))

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

        # Convert linekeys to numbers or datetimes
        all_linekeys = [_parse_linekey(k) for k in index_data.keys()]

        # Apply start_index and end_index filtering based on linekey values
        if start_index is not None or end_index is not None:
            filtered_linekeys = []
            for linekey in all_linekeys:
                include = True
                if start_index is not None:
                    if isinstance(linekey, datetime) and isinstance(start_index, str):
                        start_parsed = _parse_linekey(start_index)
                        include = include and linekey >= start_parsed
                    else:
                        include = include and linekey >= start_index
                if end_index is not None:
                    if isinstance(linekey, datetime) and isinstance(end_index, str):
                        end_parsed = _parse_linekey(end_index)
                        include = include and linekey < end_parsed
                    else:
                        include = include and linekey < end_index
                if include:
                    filtered_linekeys.append(linekey)
            linekeys = filtered_linekeys
        else:
            linekeys = all_linekeys

        # Only plot if we have data after filtering
        if linekeys:
            # Create scatter plot for this file
            y_position = [i] * len(linekeys)  # Use file index as y position
            ax.scatter(
                linekeys,
                y_position,
                s=1,
                alpha=0.6,
                color=colors[i],
                label=file_name.replace('.jsonl', '')
            )
            has_data = True

    if not has_data:
        print("Warning: No valid data found to plot")
        ax.text(0.5, 0.5, 'No Data', transform=ax.transAxes, ha='center', va='center')

    # Set labels and title
    ax.set_xlabel("Line Key")
    ax.set_ylabel("File")
    ax.set_title("FolderDB Line Keys Distribution")

    # Set y-axis with file names
    file_labels = [file.replace('.jsonl', '') for file in jsonl_files]
    ax.set_yticks(range(len(jsonl_files)))
    ax.set_yticklabels(file_labels)

    # Format x-axis for datetime if needed
    if all_datetime and has_data:
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
        plt.xticks(rotation=45)

    # Add grid
    ax.grid(True, alpha=0.3, color='gray')

    # Hide legend (similar to bokeh version)
    # ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')

    # Adjust layout to prevent label cutoff
    plt.tight_layout()

    return fig, ax

def visualize_jsonl(jsonl_path: str, plot_lib: str = "matplot", start_index = None, end_index = None):
    """
    Create a scatter plot visualization of a JSONL file's linekeys.

    Args:
        jsonl_path: Path to the JSONL file
        plot_lib: Plotting library to use ("matplot" or "bokeh")
        start_index: Start linekey value for filtering (inclusive)
        end_index: End linekey value for filtering (exclusive)

    Returns:
        Bokeh figure object if plot_lib="bokeh", else Matplotlib figure and axes objects
    """
    if plot_lib == "bokeh":
        return visualize_jsonl_bokeh(jsonl_path)
    elif plot_lib == "matplot":
        return visualize_jsonl_matplot(jsonl_path, start_index, end_index)
    else:
        raise ValueError(f"Unsupported plot_lib: {plot_lib}. Use 'matplot' or 'bokeh'.")

def visualize_folderdb(folderdb: FolderDB, prefix: str = None, height: int = 1200, plot_lib: str = "matplot", start_index = None, end_index = None):
    """
    Create a scatter plot visualization of all JSONL files in a FolderDB.

    Args:
        folderdb: FolderDB instance
        prefix: Optional prefix to filter files
        height: Figure height (pixels for bokeh, inches for matplotlib)
        plot_lib: Plotting library to use ("matplot" or "bokeh")
        start_index: Start linekey value for filtering data points (inclusive)
        end_index: End linekey value for filtering data points (exclusive)

    Returns:
        Bokeh figure object if plot_lib="bokeh", else Matplotlib figure and axes objects
    """
    if plot_lib == "bokeh":
        return visualize_folderdb_bokeh(folderdb, prefix, height)
    elif plot_lib == "matplot":
        # Convert height from pixels to inches for matplotlib (assuming ~100 DPI)
        height_inches = height / 100 if height > 50 else height
        return visualize_folderdb_matplot(folderdb, prefix, height_inches, start_index, end_index)
    else:
        raise ValueError(f"Unsupported plot_lib: {plot_lib}. Use 'matplot' or 'bokeh'.")