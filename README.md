# JSONLDB

A simple file-based database that stores data in JSONL format with version control and visualization capabilities.

## Features

- Store data in JSONL format with efficient key-based querying
- Support for both DataFrame and dictionary data types
- Built-in version control using Git
- Interactive data visualization
- Efficient metadata management
- File linting and validation

## Installation

### Development Installation

To install the package in development mode:

```bash
git clone https://github.com/yourusername/jsonldb.git
cd jsonldb
pip install -e .
```

### Regular Installation

To install the package from PyPI (when available):

```bash
pip install jsonldb
```

## Quick Start

```python
from jsonldb import FolderDB
import pandas as pd
from datetime import datetime

# Initialize the database
db = FolderDB("my_database")

# Store DataFrame data
df = pd.DataFrame({
    "id": [1, 2, 3],
    "value": ["a", "b", "c"]
})
db.store("my_table", df)

# Store dictionary data
data = {
    "key1": {"value": 1},
    "key2": {"value": 2}
}
db.store("my_dict", data)

# Query data
result = db.query("my_table", key=1)
print(result)

# Use version control
db.commit("Initial commit")
versions = db.version()
db.revert(versions[0])  # Revert to first version

# Visualize data distribution
db.visualize()
```

## Version Control

The package includes built-in version control using Git:

```python
# Commit changes
db.commit("Added new data")

# List versions
versions = db.version()
for commit_hash, message in versions.items():
    print(f"{commit_hash}: {message}")

# Revert to a previous version
db.revert(commit_hash)
```

## Visualization

Create interactive visualizations of your data:

```python
# Visualize a single JSONL file
from jsonldb import visual
visual.visualize_jsonl("path/to/file.jsonl")

# Visualize entire database
db.visualize()
```

## DataFrame Operations

```python
# Store DataFrame
db.store("table", df)

# Query DataFrame
result = db.query("table", key=1)

# Update DataFrame
db.update("table", key=1, value={"new": "data"})

# Delete from DataFrame
db.delete("table", key=1)
```

## Dictionary Operations

```python
# Store dictionary
db.store("dict", data)

# Query dictionary
result = db.query("dict", key="key1")

# Update dictionary
db.update("dict", key="key1", value={"new": "value"})

# Delete from dictionary
db.delete("dict", key="key1")
```

## Delete Operations

```python
# Delete entire table
db.delete_table("table_name")

# Delete specific key
db.delete("table_name", key=1)
```

## Metadata Management

```python
# Get metadata
metadata = db.get_metadata("table_name")

# Update metadata
db.update_metadata("table_name", {"new": "metadata"})
```

## Requirements

- Python >= 3.8
- pandas >= 1.3.0
- gitpython >= 3.1.0
- bokeh >= 3.0.0
- numpy >= 1.20.0
- orjson >= 3.6.0
- numba >= 0.54.0

## License

This project is licensed under the MIT License - see the LICENSE file for details. 