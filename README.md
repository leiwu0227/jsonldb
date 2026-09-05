# JSONLDB

**A portable, file-based time-series database for Python and agents.**

Keep observations in readable JSON Lines files. Use Python for indexed reads,
timestamp ranges, updates and pandas access. Use ordinary file tools to inspect
the data, copy the database folder to move it, and optionally use Git to review
and version changes.

## Why JSONLDB exists

Data should remain accessible independently of the program that manages it.
An agent or person can open a table directly without a database server, client,
or JSONLDB API. The library supplies indexing and maintenance around those files.

The primary use case is time-series data: research datasets, market observations,
sensor readings and reproducible local pipelines. A table typically represents
one series; a datetime key identifies one observation; its record contains the
values at that time. String-keyed dictionaries remain supported.

```text
market_data/                       # database
  prices.AAPL.jsonl                # one series
  prices.AAPL.jsonl.idx            # rebuildable index
  config.meta                     # precision and optional metadata-slot width
  db.meta                         # generated table statistics
  .jsonldb/                       # integrity and lint reports
```

An ordinary table contains lines such as:

```jsonl
{"2026-01-05T09:30:00":{"price":185.0,"volume":120}}
{"2026-01-05T09:31:00":{"price":185.5,"volume":80}}
```

## Install

Install this repository directly; the similarly named PyPI package is a different
implementation:

```bash
python -m pip install git+https://github.com/leiwu0227/jsonldb.git
```

The package declares Python >= 3.8, pandas >= 1.3.0, NumPy >= 1.20.0 and
orjson >= 3.6.0. See [development and verification](CONTRIBUTING.md) for setup
and the distinction between declared support and exercised runtimes.

## Start with a time series

```python
from datetime import datetime
from pathlib import Path
from jsonldb import FolderDB

root = Path("market_data")
root.mkdir(exist_ok=True)  # FolderDB opens an existing directory.
db = FolderDB(str(root))

# Create the series, or update these observations if it already exists.
db.upsert_dict("prices.AAPL", {
    datetime(2026, 1, 5, 9, 30): {"price": 185.0, "volume": 120},
    datetime(2026, 1, 5, 9, 31): {"price": 185.5, "volume": 80},
})

# Both range endpoints are inclusive; results are keyed by table name.
window = db.get_dict(
    ["prices.AAPL"],
    lower_key=datetime(2026, 1, 5, 9, 30),
    upper_key=datetime(2026, 1, 5, 9, 31),
)["prices.AAPL"]
assert len(window) == 2

frame = db.get_df(["prices.AAPL"])["prices.AAPL"]
assert len(frame) == 2

# The same data is accessible without the database API.
print((root / "prices.AAPL.jsonl").read_text())
```

The default datetime precision is **seconds**. Choose microseconds before
writing a database if needed. A repeated timestamp identifies the same row;
upserts replace its record. A timezone is unspecified until explicitly declared.
See [precision, timezone and metadata](docs/usage.md#precision-and-timezone).

## What it provides

- Timestamp-keyed records, inclusive range reads, upserts and deletes.
- Dictionary and pandas DataFrame interfaces, including multiple-table calls.
- Optional hierarchy to organize many series into directories.
- Optional in-file consumer metadata and a fixed-offset table timezone.
- Rebuildable indexes, cached indexed reads, compaction and repair reports.
- Plain files that can be inspected, copied and tracked with ordinary Git.

## Operating boundaries

One process owns writes to a database. There are no transactions, coordinated
concurrent writers, or fsync-based power-loss durability guarantees. Stop writes
before copying or taking a Git snapshot. Recovery skips malformed rows and lint
can remove damage; inspect the reports when recovery occurs.

This is a time-series **storage** library. SQL, joins, aggregation, resampling,
retention scheduling and geographic timezone/DST rules are outside its current
scope. Perform analysis in pandas or another tool. Updates avoid rewriting the
whole data file, but still publish a complete table index; performance depends
on table size and workload.

## Documentation

| I want to… | Start here |
| --- | --- |
| Save, query and update time series | [Usage guide](docs/usage.md) |
| Find a function and its behavior | [API reference](docs/api.md) |
| Read files directly as a person or agent | [File format](docs/file-format.md) |
| Copy, restore or version data with Git | [Portability and Git](docs/portability-and-git.md) |
| Understand objectives and design choices | [Design decisions](docs/design.md) |
| Develop, test or benchmark the library | [Contributing](CONTRIBUTING.md) |
| Run the time-series tutorial | [Time series notebook](examples/01_time_series.ipynb) |
| Explore portable datasets and metadata | [Portability notebook](examples/02_portable_datasets.ipynb) |

## License

[MIT](LICENSE).
