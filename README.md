# JSONL File Management System

A Python package for efficient management of JSONL (JSON Lines) files with features for version control.

## Features

- Efficient JSONL file operations with index-based access
- Support for time-series data and range queries
- Version control integration using Git
- Pandas DataFrame integration
- Performance optimized for large datasets

## Installation

```bash
pip install jsonldb
```

## Quick Start

### Basic JSONL Operations

```python
from jsonlfile import save_jsonl, load_jsonl, select_jsonl, update_jsonl, delete_jsonl, lint_jsonl

# Save data
data = {"key1": {"value": 1}, "key2": {"value": 2}}
save_jsonl("data.jsonl", data)

# Load data
loaded_data = load_jsonl("data.jsonl")

# Select range
selected = select_jsonl("data.jsonl", key_range=("key1", "key2"))

# Update records
update_jsonl("data.jsonl", {"key1": {"value": 3}})

# Delete records
delete_jsonl("data.jsonl", ["key1"])

# Lint and optimize
lint_jsonl("data.jsonl")
```

### Version Control Integration

```python
from vercontrol import init_folder, commit, list_version, revert

# Initialize version control
init_folder("data_folder")

# Commit changes
commit("data_folder", "Updated data")

# List versions
versions = list_version("data_folder")

# Revert to previous version
revert("data_folder", "commit_hash")
```

### Pandas Integration

```python
from jsonldf import save_jsonldf, load_jsonldf, update_jsonldf, select_jsonldf, delete_jsonldf, lint_jsonldf

# Save DataFrame to JSONL
df = pd.DataFrame({"key": [1, 2], "value": ["a", "b"]})
save_jsonldf("data.jsonl", df)

# Load JSONL to DataFrame
df = load_jsonldf("data.jsonl")
```

## Examples

The package includes several example notebooks and scripts:

### Basic Operations
- `example_jsonlfile/basic_operations.ipynb`: Demonstrates core JSONL operations
- `example_jsonlfile/performance_test.ipynb`: Tests performance with large datasets
- `example_jsonlfile/timeseries_example.ipynb`: Shows time-series data handling

### Version Control
- `example_vercontrol/version_control_example.ipynb`: Demonstrates Git-based version control

## Performance

- Index-based access for O(1) record retrieval
- Efficient range queries
- Optimized for large datasets
- Memory-efficient processing

## Requirements

- Python 3.6+
- Git (for version control features)
- Dependencies:
  - pandas
  - numpy
  - gitpython

## License

MIT License 