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

def _parse_linekey(linekey: str) -> Union[float, datetime]:
    """
    Parse a linekey string into either a number or datetime.
    Returns a float if the string can be converted to a number,
    or a datetime if the string matches a datetime format.
    """


    # Try to parse as datetime
    if 'T' in linekey and len(linekey) == 19:
        return datetime.fromisoformat(linekey)

    return float(hash(linekey))

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
    
    p.scatter('x', 'y', source=source, size=5, alpha=0.6)
    
    # Customize the plot
    p.grid.grid_line_color = "gray"
    p.grid.grid_line_alpha = 0.3
    
    return p

def visualize_folderdb(folder_path: str) -> figure:
    """
    Create a scatter plot visualization of all JSONL files in a FolderDB using Bokeh.
    
    Args:
        folder_path: Path to the FolderDB directory
        
    Returns:
        Bokeh figure object showing the scatter plot
    """
    # Get all JSONL files in the folder
    jsonl_files = [f for f in os.listdir(folder_path) if f.endswith('.jsonl')]
    
    if not jsonl_files:
        raise FileNotFoundError(f"No JSONL files found in: {folder_path}")
    
    # Prepare data for plotting
    all_data = []
    colors = Category10[10]  # Use a color palette for different files
    
    for i, file_name in enumerate(jsonl_files):
        idx_path = os.path.join(folder_path, file_name + '.idx')
        
        if not os.path.exists(idx_path):
            continue
            
        # Read the index file
        with open(idx_path, 'r') as f:
            index_data = json.load(f)
        
        # Convert linekeys to numbers or datetimes
        linekeys = [_parse_linekey(k) for k in index_data.keys()]
        
        # Add data for this file
        all_data.extend([(k, file_name) for k in linekeys])
    
    if not all_data:
        raise ValueError("No valid data found in any JSONL files")
    
    # Convert to DataFrame for easier handling
    df = pd.DataFrame(all_data, columns=['linekey', 'filename'])
    
    # Create the figure
    p = figure(
        title="FolderDB Line Keys Distribution",
        x_axis_label="Line Key",
        y_axis_label="File Name",
        tools="pan,wheel_zoom,box_zoom,reset,save"
    )
    
    # Add hover tool
    hover = HoverTool(
        tooltips=[
            ("File", "@filename"),
            ("Line Key", "@x")
        ]
    )
    p.add_tools(hover)
    
    # Plot each file's data
    for i, file_name in enumerate(df['filename'].unique()):
        file_data = df[df['filename'] == file_name]
        source = ColumnDataSource(data={
            'x': file_data['linekey'],
            'y': [file_name] * len(file_data),
            'filename': [file_name] * len(file_data)
        })
        
        p.scatter(
            'x', 'y',
            source=source,
            size=5,
            alpha=0.6,
            color=colors[i % len(colors)],
            legend_label=file_name
        )
    
    # Customize the plot
    p.grid.grid_line_color = "gray"
    p.grid.grid_line_alpha = 0.3
    p.legend.click_policy = "hide"  # Allow toggling visibility of each file
    
    return p 