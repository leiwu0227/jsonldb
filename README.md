# JSONLDB

A simple file-based database that stores keyed records in JSONL format.

## Features

- Store dictionaries and pandas DataFrames in JSONL files
- Select records by an inclusive key range
- Organize tables in a flat or hierarchical folder layout
- Maintain and lint per-table metadata and indexes

## Installation

Install the package directly from GitHub:

```bash
pip install git+https://github.com/leiwu0227/jsonldb.git
```

For development from a clone, create an environment from the generated lock
and run the tracked test suite:

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements-dev.lock
python -m pip install --no-deps -e .
python -m pytest
```

Regenerate `requirements-dev.lock` after changing `setup.py` or
`requirements-dev.in`:

```bash
uv pip compile setup.py requirements-dev.in \
    --python-version 3.8 \
    --universal \
    --generate-hashes \
    --output-file requirements-dev.lock
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

New `FolderDB` table names may contain ASCII letters, digits, underscores,
hyphens and dots, for example `prices.2024-01-01`. The optional `.jsonl`
suffix is still accepted. Empty names, leading/trailing dots, consecutive dots,
empty hierarchy segments, spaces, path separators and other special characters
are rejected. `_meta` (case-insensitive) and Windows device names such as `CON`,
`NUL`, `COM1` and `LPT1` are reserved, including device names with extensions and
device names used as hierarchy directories. Invalid new names raise `ValueError`
before folders or files are created. Names are never silently changed.

These creation rules do not rename or exclude existing tables: they remain
readable, writable and maintainable under their historical names. Database
metadata always treats table names as strings; datetime conversion applies only
to row keys in user tables. Low-level file-store APIs continue accepting paths.

Repeated point and range reads automatically reuse unchanged indexes in a
private process-local cache. The cache checks both files on every read and uses
a 64 MiB budget for conservatively accounted retained index objects; it does not
cache rows or keep files open. Public index loads remain independent mutable
dictionaries. Writes invalidate affected entries, and lint retains its existing
verification. No additional files or caller configuration are needed.

The benefit is largest when repeatedly querying tables whose indexes fit the
budget. First reads, reads after writes and working sets exceeding the budget
still load indexes; full-table loads remain sequential. The budget is not a
limit on total process memory. Compare these workloads with
`python profile_test/benchmark.py --index-cache --save results.json` and
`--compare baseline.json`.

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

`clear_folder(force=True)` removes tables and their indexes under non-hidden
folders and resets table statistics. It preserves database configuration,
including hierarchy, delimiter, datetime precision, and metadata-slot width, so
the existing instance and a reopened database retain the same settings. Without
`force=True`, it only warns and leaves the database unchanged.

## Metadata and Linting

```python
# Metadata is keyed by table name.
metadata = db.get_dbmeta()
measurements_metadata = metadata.get("measurements")

# Validate tables, compact dead space, and refresh db.meta.
db.lint_db()
```

Opening a database automatically recovers missing or invalid `h.meta` settings
from visible table paths. Recovery infers the delimiter from directory and
filename prefixes and chooses the shallowest observed table-directory depth.
Root-level tables or an empty database imply a flat layout. It reconciles mixed
layouts by moving tables with their indexes, preserves table contents and hidden
directories, and records the inferred settings and evidence in
`.jsonldb/integrity.log`. Contradictory prefixes, destination collisions, or an
unreadable directory stop recovery with an error before tables are moved.
An interrupted move can be retried by opening the database again. Explicitly
supplying `hierarchy_depth` takes precedence over the inferred depth.

## Optional Table Timezone

Existing databases and callers keep their current behavior. To declare a fixed
UTC offset for a new table, explicitly enable metadata slots and configure the
empty table before writing rows:

```python
from datetime import datetime, timedelta, timezone

# Enabling slots rewrites existing tables to reserve their metadata space.
db.set_meta_slot_bytes(4096)
db.overwrite_dict("prices", {})
db.set_timezone("prices", "+8:00")
assert db.read_timezone("prices") == "+08:00"

key = datetime(2026, 9, 5, 10, 30, tzinfo=timezone(timedelta(hours=8)))
db.upsert_dict("prices", {key: {"price": 123.0}}, meta={"source": "feed"})
rows = db.get_dict(["prices"])["prices"]
# The stored key is 2026-09-05T10:30:00; reads return a naive datetime.
```

The envelope stores `timezone` alongside `v` and `data`. All datetime linekeys
in that file share the declared offset; row values and consumer `meta` remain
uninterpreted. Replacing or clearing consumer metadata preserves timezone.
The low-level equivalents are `jsonlfile.read_jsonl_timezone(path)` and
`jsonlfile.write_jsonl_timezone(path, offset)`.

Accepted offsets are signed `H:MM` or `HH:MM`, with hours 0–23 and minutes
0–59. `UTC` and signed zero normalize to `+00:00`. This deliberately supports
fixed offsets up to `±23:59`, rather than a registry of geographic zones.
Named zones and daylight-saving transitions are not supported. Naive datetime
keys represent local time at the declared offset. Aware keys and ISO timestamp
strings with offsets must match it; conflicting offsets are rejected without
conversion. The same rules apply to lookup, range and deletion bounds.

Timezone is optional: absence means unspecified, not UTC or the machine's
local timezone. `auto_deserialize=False` continues returning stored strings.
Tables without a declaration retain their existing handling of timezone-aware
keys, including their offset suffixes.

Timezone setters require an existing slotted table. Set `None` to remove the
declaration while empty; repeating the same offset is allowed on populated
tables. Adding, changing or removing timezone on populated tables is refused;
an explicit migration facility is not included. Recognizably damaged declarations
are preserved and refused during mutation or repair. If the declaration is
completely erased, it cannot be distinguished from intentional absence, and
jsonldb cannot reconstruct it safely.

## Requirements

- Python >= 3.8
- pandas >= 1.3.0
- numpy >= 1.20.0
- orjson >= 3.6.0

## License

This project is licensed under the MIT License. See `LICENSE` for details.
