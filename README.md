# jsonlfile

A high-performance Python package for efficient JSONL file operations with byte-position indexing. This package maintains an index file (.idx) alongside each JSONL file for O(1) access to records by their linekeys, with optimized operations using Numba and orjson.


## Installation

```bash
pip install -r requirements.txt
```

## Dependencies
- Python >= 3.7
- typing >= 3.7.4
- jsonlines >= 3.1.0
- numba >= 0.57.0 (for optimized operations)
- numpy >= 1.20.0 (required by numba)
- orjson >= 3.9.0 (for faster JSON operations)

## Usage

The package provides several optimized functions for working with JSONL files:

### Basic Operations

```python
from jsonlfile import save_jsonl, load_jsonl, select_jsonl, update_jsonl, delete_jsonl

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

# Lint and sort the file (optimized with Numba)
lint_jsonl("data.jsonl")
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
- Numba-optimized operations for sorting and range queries
- Fast JSON operations using orjson
- Automatic index file creation and maintenance
- Support for range-based queries with optimized performance
- Safe file operations with proper error handling
- Configurable buffer size for optimal I/O performance

## Performance Characteristics

### Operation Performance (1M records)

- **Save Operation**: ~133K records/s
  - Optimized with orjson for faster serialization
  - Efficient buffered writing

- **Load Operation**: ~195K records/s
  - O(1) access using index
  - Optimized JSON parsing with orjson

- **Select Operation**:
  - Small ranges (1%): ~1M records/s
  - Medium ranges (10%): ~600K records/s
  - Large ranges (50%): ~200K records/s
  - Numba-optimized range queries

- **Update Operation**: ~185K records/s
  - In-place updates when possible
  - Efficient appending for new/grown records

- **Delete Operation**: ~200K records/s
  - O(1) access using index
  - Space-preserving deletion

- **Lint Operation**: ~74K records/s
  - Numba-optimized sorting
  - Efficient key processing

### Optimization Features

1. **Numba JIT Compilation**:
   - Optimized sorting operations
   - Fast range query processing
   - Efficient key comparisons

2. **orjson Integration**:
   - Faster JSON serialization/deserialization
   - Reduced memory usage
   - Optimized string handling

3. **Buffer Management**:
   - Configurable buffer size (default: 10MB)
   - Optimized for different workloads
   - Efficient memory usage

4. **Index Optimization**:
   - O(1) record access
   - Sorted key storage
   - Efficient range queries

## Error Handling

The package handles various error conditions:
- Invalid JSON data
- Missing index files
- File I/O errors
- Malformed lines in JSONL files
- Buffer overflow protection
- Memory management errors

All operations maintain data integrity by using proper error handling and atomic file operations where necessary.

## Performance Tips

1. **Range Queries**:
   - Smaller ranges perform better
   - Use specific ranges when possible
   - Consider batch processing for large ranges

2. **Buffer Size**:
   - Default 10MB is optimal for most cases
   - Increase for large files/operations
   - Decrease for memory-constrained environments

3. **Batch Operations**:
   - Group updates when possible
   - Use bulk operations for better performance
   - Consider record size in buffer calculations 