# jsonldb

A Python package for efficient JSONL file operations with byte-position indexing. This package maintains an index file (.idx) alongside each JSONL file for O(1) access to records by their linekeys.


## Installation

```bash
pip install -r requirements.txt
```

## Dependencies
- Python >= 3.7
- typing >= 3.7.4
- jsonlines >= 3.1.0

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

# Load entire JSONL file into a dictionary
loaded_data = load_jsonl("data.jsonl")

# Select data within a key range (inclusive)
selected_data = select_jsonl("data.jsonl", ("key1", "key2"))

# Update existing or insert new records
update_data = {
    "key1": {"value1": 20, "value2": 21},  # updates existing record
    "key3": {"value1": 30, "value2": 31}   # inserts new record
}
update_jsonl("data.jsonl", update_data)

# Delete records by their linekeys
delete_jsonl("data.jsonl", ["key1", "key2"])
```

### Index File (.idx)

Each JSONL file will have an accompanying `.idx` file that maps linekeys to byte positions for O(1) access. The index file is automatically created and maintained when using any of the provided functions.

The index file is a JSON file with the following format:
```json
{
  "linekey1": byte_position1,
  "linekey2": byte_position2,
  ...
}
```

### JSONL File Format

Each line in the JSONL file must follow this format:
```json
{"linekey": {"data_key1": "value1", "data_key2": "value2", ...}}
```

Requirements:
- Each line must be a valid JSON object
- Each line must have exactly one top-level key (the linekey)
- The linekey must be unique within the file
- The value associated with the linekey must be a dictionary

## Features

- Memory-efficient operations using file streaming and memory mapping
- O(1) access to records using byte-position indexing
- Automatic index file creation and maintenance
- Support for range-based queries
- Safe file operations with proper error handling
- Uses jsonlines library for robust JSONL parsing

## Performance Considerations

- Read operations (load, select) use the index file for O(1) access to specific records
- Write operations (save, update) rebuild the index file
- Delete operations create a new file without the deleted records and update the index accordingly
- The package is optimized for read operations and moderate write/update operations

## Error Handling

The package handles various error conditions:
- Invalid JSON data
- Missing index files
- File I/O errors
- Malformed lines in JSONL files

All operations maintain data integrity by using proper error handling and atomic file operations where necessary. 