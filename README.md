# jsonldb

A Python package for efficient JSONL file operations with indexing support.

## Installation

```bash
pip install -r requirements.txt
```

## Usage

The package provides several functions for working with JSONL files:

### Basic Operations

```python
from jsonldb import save_jsonl, load_jsonl, select_jsonl, update_jsonl, delete_jsonl

# Save data to JSONL file
data = {
    "key1": {"value1": 10, "value2": 11},
    "key2": {"value1": 11, "value2": 12}
}
save_jsonl("data.jsonl", data)

# Load data from JSONL file
loaded_data = load_jsonl("data.jsonl")

# Select data within a key range
selected_data = select_jsonl("data.jsonl", ("key1", "key2"))

# Update or insert data
update_data = {
    "key1": {"value1": 20, "value2": 21},
    "key3": {"value1": 30, "value2": 31}
}
update_jsonl("data.jsonl", update_data)

# Delete data by keys
delete_jsonl("data.jsonl", ["key1", "key2"])
```

### Index File

Each JSONL file will have an accompanying `.idx` file that maps linekeys to byte locations for fast access. The index file is automatically created and updated when using the save functions.

## Features

- Efficient JSONL file operations
- Automatic index file creation and management
- Support for key-based operations
- Memory-efficient file handling
- Type hints for better IDE support

## File Format

Each line in the JSONL file follows this format:
```json
{"linekey": {"data_key1": "value1", "data_key2": "value2"}}
```

The linekey must be unique and is used as the primary identifier for each record. 