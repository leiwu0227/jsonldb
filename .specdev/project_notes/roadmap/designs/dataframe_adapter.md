# DataFrame Adapter

The adapter lets callers work in pandas while the file store stays dictionary-based. It is intentionally thin: every function converts between a DataFrame and the `{linekey: record}` dictionary and delegates to the matching file-store function, forwarding the timespec unchanged.

## Mapping

- The DataFrame **index** becomes the linekeys. Each row becomes the record dictionary keyed by column name.
- On the way back, the loaded dictionary becomes a DataFrame with linekeys as its index, so a datetime-keyed table returns a datetime index when auto-deserialization is on.
- An empty result is an empty DataFrame, never `None`.

The index must be unique because linekeys are unique; saving a DataFrame with duplicate index values is rejected up front rather than silently collapsing rows.

## Operations

Save, load, update (upsert), select by range, delete by keys, and lint mirror the file store one to one. Save requires a unique index; the others accept whatever the file store accepts. Column order is not a contract: records are dictionaries, and a table whose rows carry different column sets produces missing values on load.

## Why not a richer layer

Value fidelity is deliberately limited to what JSON expresses. Numbers, text, booleans, nulls, lists, and nested objects round-trip; pandas-specific dtypes such as categoricals or timezone-aware timestamps are not restored on load. Callers that need exact dtypes reapply them after loading. Keeping the adapter thin means every storage guarantee is stated once, in the file-store note, and the adapter cannot drift from it.

## Source target

- `jsonldb/jsonldf.py`: at most 150 lines.
