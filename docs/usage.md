# Working with time series

This guide describes the current API. Each executable example creates its own
database under the working directory. Use a fresh scratch directory to try them.

## Write, query and correct observations

```python
from datetime import datetime
from pathlib import Path
from jsonldb import FolderDB

root = Path("observations")
root.mkdir(exist_ok=True)
db = FolderDB(str(root))
t0 = datetime(2026, 1, 5, 9, 30)
t1 = datetime(2026, 1, 5, 9, 31)
db.overwrite_dict("prices.AAPL", {
    t0: {"price": 185.0, "volume": 120},
    t1: {"price": 185.5, "volume": 80},
})
# Replace the complete record at t1, not just individual fields within it.
db.upsert_dict("prices.AAPL", {t1: {"price": 185.6, "volume": 85}})
one = db.get_dict(["prices.AAPL"], t1, t1)["prices.AAPL"]
assert one[t1] == {"price": 185.6, "volume": 85}
window = db.get_dict(["prices.AAPL"], t0, t1)["prices.AAPL"]
assert list(window) == [t0, t1]
text_keys = db.get_dict(["prices.AAPL"], auto_deserialize=False)["prices.AAPL"]
assert "2026-01-05T09:30:00" in text_keys
```

`overwrite_*` replaces the entire table. `upsert_*` creates a missing table and
replaces or adds supplied keys. Records must be dictionaries with JSON-serializable
values. Queries return a mapping from table name to rows. Pass a list of names
for either dictionary or DataFrame reads; omit names to read all discovered tables.
A missing table is omitted; DataFrame reads also log a warning.

Ranges include both endpoints. Omit one bound to use the first or last key; equal
bounds select one key. With both bounds omitted, loading follows physical file
order, which may differ from timestamp order after updates. Use a bounded range
or sort the result when chronological order is required.

## pandas

```python
from pathlib import Path
import pandas as pd
from jsonldb import FolderDB

root = Path("frame_data")
root.mkdir(exist_ok=True)
db = FolderDB(str(root))
frame = pd.DataFrame(
    {"temperature": [21.1, 21.4]},
    index=pd.to_datetime(["2026-01-05T09:30:00", "2026-01-05T09:31:00"]),
)
db.overwrite_df("sensor.room1", frame)
loaded = db.get_df(["sensor.room1"])["sensor.room1"]
assert loaded.loc[pd.Timestamp("2026-01-05T09:31:00"), "temperature"] == 21.4
```

The DataFrame index becomes the linekey. Saves require a unique DataFrame index;
ensure keys also remain unique after serialization at the database precision.
Column ordering and exact dtype restoration are not contracts. Empty results
produce empty DataFrames. Use pandas for resampling, joins and aggregation.

Plural writes accept `{table_name: dictionary_or_dataframe}` through
`overwrite_dicts`, `upsert_dicts`, `overwrite_dfs` and `upsert_dfs`. They process
tables in sequence: a later failure does not roll back completed tables.

## Precision and timezone

Precision is a database setting: `seconds` by default, or `microseconds`. Datetime
objects serialize to that precision. Distinct inputs can therefore become the
same key; multiple independent events at one timestamp require another data model.
String keys are generally preserved, so callers must keep timestamp-string
precision uniform too. Keys are ordered lexically by stored text.

Choose microseconds by writing configuration before the first open in a new,
empty directory. There is no timespec argument on `FolderDB` and no populated
database precision migration operation.

```python
from datetime import datetime
from pathlib import Path
from jsonldb import FolderDB, jsonlfile

root = Path("microsecond_data")
root.mkdir()  # A fresh database, not an existing database to reconfigure.
jsonlfile.save_jsonl_atomic(
    str(root / "config.meta"), {"config": {"timespec": "microseconds"}}
)
db = FolderDB(str(root))
key = datetime(2026, 1, 5, 9, 30, 0, 123456)
db.upsert_dict("sensor.room1", {key: {"temperature": 21.1}})
assert key in db.get_dict(["sensor.room1"])["sensor.room1"]
```

Automatic datetime recognition accepts parseable offset-free timestamps of
exactly 19 characters at seconds precision or 26 at microseconds precision.
Reads return naive datetimes. `auto_deserialize=False` retains strings.

A table may separately declare a fixed UTC offset. Enable metadata slots, create
an empty table, then establish its timezone before adding observations:

```python
from datetime import datetime, timedelta, timezone
from pathlib import Path
from jsonldb import FolderDB

root = Path("zoned_data")
root.mkdir()
db = FolderDB(str(root))
db.set_meta_slot_bytes(4096)
db.overwrite_dict("prices.HK", {})
db.set_timezone("prices.HK", "+8:00")
key = datetime(2026, 1, 5, 9, 30, tzinfo=timezone(timedelta(hours=8)))
db.upsert_dict("prices.HK", {key: {"price": 100.0}}, meta={"source": "feed"})
assert db.read_timezone("prices.HK") == "+08:00"
result = db.get_dict_with_meta("prices.HK")
assert result.meta == {"source": "feed"}
assert datetime(2026, 1, 5, 9, 30) in result.rows
```

Signed `H:MM` or `HH:MM` offsets normalize to signed `HH:MM`; hours must be 0–23
and minutes 0–59. `UTC` and signed zero normalize to `+00:00`. Named geographic
zones and daylight-saving rules are unsupported. Absence means unspecified,
never inferred UTC or the machine's timezone.

In a declared table, naive timestamps represent local time at that offset.
Aware datetime inputs and supported offset-bearing ISO strings must match it;
matching inputs lose their suffix when stored, and conflicts are rejected before
mutation. Queries and deletes apply the same interpretation. Undeclared tables
retain historical handling of aware keys, including stored offset suffixes that
ordinary automatic deserialization does not recognize.

`set_timezone(name, None)` removes an empty table's declaration. Adding, changing
or removing timezone on a populated table is refused. Repeating its current
normalized offset is a no-op. Setters require an existing known-version slot;
they do not create files or enable slots implicitly.

## Consumer metadata

`set_meta_slot_bytes(width=4096)` enables or resizes optional slots for the whole
folder, rewriting existing tables as necessary. Shrinking is refused before
migration if any existing envelope does not fit. New tables then receive slots.

On single-table writes, `meta=None` preserves existing consumer metadata and a
dictionary replaces it after the rows are written. `read_meta` retrieves it;
`clear_meta` removes it. Both preserve table timezone. Consumer metadata may
contain its own `timezone` key without special meaning.

`get_dict_with_meta` and `get_df_with_meta` return a named tuple with `.meta` and
`.rows`, reading metadata first. Missing tables return `None` plus an empty row
container. This ordering does not make the read transactional.

## Names and hierarchy

New table names use ASCII letters, digits, `_`, `-` and dots. An optional `.jsonl`
suffix is tolerated. Empty names, leading/trailing dots, consecutive dots, spaces,
path separators and other special characters are rejected before creation.
Empty hierarchy segments are also rejected. `_meta` is reserved case-insensitively;
Windows device names such as `CON`, `NUL`, `COM1` and `LPT1` are excluded as file
stems, including with extensions, and as hierarchy directories. Existing tables
keep access under their historical names. Low-level APIs accept file paths.

`hierarchy_depth` is the maximum number of nested directories. Split the name
on the configured delimiter (default `.`), exclude its final segment, and use up
to that many prefixes. The filename retains the complete table name. For example,
with `FolderDB(path, hierarchy_depth=6)`:

| Table | Relative file path |
| --- | --- |
| `a` | `a.jsonl` |
| `a.b.c` | `a/b/a.b.c.jsonl` |
| `a.b.c.d.e.f.g.h` | `a/b/c/d/e/f/a.b.c.d.e.f.g.h.jsonl` |

Short names are valid, and names longer than the maximum are also valid. Flat
mode keeps all tables at the root. Saved settings live in `h.meta`; keep this
file because existing tables do not always reveal the configured maximum.

Opening automatically migrates older hierarchy layouts, even with an omitted or
unchanged depth. A different explicit positive maximum reorganizes visible tables.
Table bytes and embedded metadata are preserved, and indexes move alongside.
Collisions and unsafe destinations stop the operation before any planned move.
Moves are not transactional: after an interruption, reopen the database or repeat
the maintenance call to finish the recorded operation. A temporary hidden
`.hierarchy.pending` file retains the intended settings and unfinished moves;
keep it with the database until recovery completes. Older library versions are
not supported after migration.

An open that successfully resumes interrupted moves records `hierarchy_resumed`
in `.jsonldb/integrity.log`. Clean opens do not emit that finding.

If `h.meta` is missing or damaged, recovery honors an explicit maximum or uses
the deepest observed visible table directory depth. With no nested tables and
no explicit maximum, it falls back to flat mode. It infers a consistent delimiter
from directory/name prefixes, defaulting to `.` when there is no evidence, and
rejects contradictory prefixes. The open report identifies inferred settings;
they need not equal the lost original maximum.

Previously quarantined tables remain hidden under `.invalid_tickers` until
`reprocess_invalid_tickers()` explicitly restores safe names, including short
ones. Restoration refuses to overwrite existing tables; unsafe names stay put.

## Deletion and maintenance

| Call | Effect |
| --- | --- |
| `delete_file_keys(name, keys)` | Delete the listed observations. |
| `delete_file_range(name, lower, upper)` | Delete an inclusive range; supply both bounds. |
| `delete_range(names, lower, upper)` | Apply that range deletion to several tables. |
| `delete_file(name)` | Remove a table and its index. |
| `clear_folder(force=True)` | Remove visible tables, preserve configuration and hidden directories, reset statistics. |
| `build_dbmeta()` | Rebuild statistics and recover indexes as needed. |
| `lint_db()` | Check and compact tables, refresh statistics and replace the lint report. |
| `lint_db(force=True)` | Also run full verification, useful after external edits or suspected damage. |

Key deletes leave blank tombstones until lint. Opening observes damage and repairs
controls/indexes; lint can remove damaged rows. Review `.jsonldb/integrity.log`
after opening and `.jsonldb/lint.log` after lint. Reports replace their previous
contents. Ordinary reads/writes use logging but do not rewrite those reports.

Continue with the [API reference](api.md) or [copying and Git workflow](portability-and-git.md).
