# JSONLDB

A simple file-based database that stores keyed records in JSONL format, with
optional Git version control and visualization.

## Features

- Store dictionaries and pandas DataFrames in JSONL files
- Select records by an inclusive key range
- Organize tables in a flat or hierarchical folder layout
- Track database snapshots with Git
- Inspect table layout with Matplotlib or Bokeh
- Maintain and lint per-table metadata and indexes

## Installation

Install the package directly from GitHub:

```bash
pip install git+https://github.com/leiwu0227/jsonldb.git
```

## Quick Start

```python
from jsonldb import FolderDB

db = FolderDB("my_database")

# Replace the complete contents of a table.
db.overwrite_dict("users", {
    "user-001": {"name": "Ada", "active": True},
    "user-002": {"name": "Linus", "active": True},
})

# Insert a new key or replace an existing key.
db.upsert_dict("users", {
    "user-002": {"name": "Linus", "active": False},
    "user-003": {"name": "Grace", "active": True},
})

# Reads return a mapping from table name to its selected records.
result = db.get_dict(
    ["users"],
    lower_key="user-001",
    upper_key="user-002",
)
print(result["users"])
```

Keys are ordered by their serialized text representation. Use strings whose
lexicographic order matches the intended range order, such as zero-padded
numeric strings.

## Dictionary Operations

```python
db.overwrite_dict("settings", {
    "app": {"theme": "dark"},
    "email": {"enabled": True},
})

db.upsert_dict("settings", {
    "email": {"enabled": False},
})

all_settings = db.get_dict(["settings"])["settings"]
```

The plural methods operate on several tables at once:

```python
db.overwrite_dicts({
    "cities": {"HKG": {"name": "Hong Kong"}},
    "countries": {"HK": {"name": "Hong Kong"}},
})
```

## DataFrame Operations

The DataFrame index becomes the JSONL record key.

```python
import pandas as pd

frame = pd.DataFrame(
    {"value": ["a", "b", "c"]},
    index=["row-001", "row-002", "row-003"],
)

db.overwrite_df("measurements", frame)
db.upsert_df(
    "measurements",
    pd.DataFrame({"value": ["updated"]}, index=["row-002"]),
)

selected = db.get_df(
    ["measurements"],
    lower_key="row-001",
    upper_key="row-002",
)["measurements"]
```

Plural variants are also available as `overwrite_dfs` and `upsert_dfs`.

## Delete Operations

```python
# Delete selected keys.
db.delete_file_keys("users", ["user-001"])

# Delete an inclusive key range.
db.delete_file_range("users", "user-002", "user-003")

# Delete the entire table and its index.
db.delete_file("users")
```

Use `delete_range(["table_a", "table_b"], lower_key, upper_key)` to remove the
same inclusive range from multiple tables.

## Metadata and Linting

```python
# Metadata is keyed by table name.
metadata = db.get_dbmeta()
measurements_metadata = metadata.get("measurements")

# Validate tables, compact dead space, and refresh db.meta.
db.lint_db()
```

## Version Control

The database folder can be committed to Git and restored to an earlier commit:

```python
db.commit("Added new data")

versions = db.version()
for commit_hash, message in versions.items():
    print(f"{commit_hash}: {message}")

db.revert(commit_hash)
```

`revert` restores the database folder with a hard Git reset, so uncommitted
changes in that folder are discarded.

## Visualization

Visualization is provided by functions in `jsonldb.visual`, not methods on
`FolderDB`:

```python
from jsonldb.visual import visualize_folderdb, visualize_jsonl

figure, axes = visualize_folderdb(db)
file_figure, file_axes = visualize_jsonl("my_database/measurements.jsonl")

# Request a Bokeh figure instead of the default Matplotlib result.
bokeh_figure = visualize_folderdb(db, plot_lib="bokeh")
```

## Requirements

- Python >= 3.8
- pandas >= 1.3.0
- gitpython >= 3.1.0
- bokeh >= 2.0.0
- numpy >= 1.20.0
- orjson >= 3.6.0
- matplotlib >= 3.0.0

## License

This project is licensed under the MIT License. See `LICENSE` for details.
