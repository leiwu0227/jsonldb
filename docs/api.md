# API reference

Import `FolderDB` from `jsonldb`. This reference groups the existing public
operations by purpose; [the usage guide](usage.md) provides runnable examples.

## FolderDB

`FolderDB(folder_path, hierarchy_depth=None)` opens an **existing** directory.
Pass a string path. An explicit positive maximum hierarchy depth creates or changes the
layout; omission uses persisted settings or recovery inference. Timespec is
configured through `config.meta`, not a constructor argument.

### Writing

| Method | Input and behavior |
| --- | --- |
| `overwrite_dict(name, data_dict, meta=None)` | Replace the table with `{key: record_dict}`. |
| `upsert_dict(name, data_dict, meta=None)` | Create a missing table or replace/add supplied rows. |
| `overwrite_df(name, df, meta=None)` | Replace using a DataFrame with a unique index. |
| `upsert_df(name, df, meta=None)` | Create/update using the DataFrame index as row keys. |
| `overwrite_dicts(dict_dicts)` | `{name: data_dict}`; call overwrite for each table. |
| `upsert_dicts(dict_dicts)` | `{name: data_dict}`; call upsert for each table. |
| `overwrite_dfs(dict_dfs)` | `{name: dataframe}`; call overwrite for each table. |
| `upsert_dfs(dict_dfs)` | `{name: dataframe}`; call upsert for each table. |

These operations return `None`. Upsert replaces the entire record at a key.
Single-table writes accept `meta`; plural signatures do not. Without enabled
slots, metadata-bearing writes are refused. `None` preserves metadata. Every
successful table write synchronously refreshes its statistics. Multiple-table
operations may partially complete before an error.

### Reading

| Method | Return |
| --- | --- |
| `get_dict(names=None, lower_key=None, upper_key=None, auto_deserialize=True)` | `{name: {key: record}}`. |
| `get_df(names=None, lower_key=None, upper_key=None, auto_deserialize=True)` | `{name: DataFrame}`. |
| `get_dict_with_meta(name, lower_key=None, upper_key=None, auto_deserialize=True)` | Named tuple: `.meta`, `.rows` dictionary. |
| `get_df_with_meta(name, lower_key=None, upper_key=None, auto_deserialize=True)` | Named tuple: `.meta`, `.rows` DataFrame. |

Use lists for `names`; `get_dict` also accepts one string. Omitted names discover
all tables. A missing table is omitted from multi-table results; DataFrame reads
warn. The `_with_meta` methods return `None` plus an empty container when missing.
Range endpoints are inclusive; both omitted means a sequential full load.
Recognized datetime keys become naive datetimes unless deserialization is disabled.

### Deleting

| Method | Behavior |
| --- | --- |
| `delete_file_keys(name, keys)` | Blank matching rows and refresh statistics. |
| `delete_file_range(name, lower_key, upper_key)` | Delete an inclusive range and refresh statistics if rows changed. |
| `delete_range(names, lower_key, upper_key)` | Repeat range deletion across tables. |
| `delete_file(name)` | Remove the file, its index and its statistics entry. |
| `clear_folder(force=False)` | With `True`, clear visible tables while retaining configuration and hidden trees; otherwise warn only. |

Delete methods return `None`. Key/range deletion leaves unindexed blank
tombstones; lint compacts the physical file. Supply both range bounds.

### Metadata and timezone

| Method | Behavior |
| --- | --- |
| `set_meta_slot_bytes(width=4096)` | Enable/resize slots across tables, preflight fit before migration. |
| `read_meta(name)` | Consumer record or `None`. |
| `clear_meta(name)` | Clear consumer data, preserve timezone; missing table is a no-op. |
| `read_timezone(name)` | Canonical fixed offset or `None` for unspecified/missing. |
| `set_timezone(name, timezone)` | Set/remove offset on an existing empty known-version slotted table; same offset is idempotent. |
| `get_dbmeta()` | Statistics dictionary keyed by literal table-name strings. |
| `update_dbmeta(name, linted=False)` | Measure and refresh one existing table from disk. |
| `build_dbmeta()` | Rebuild all discovered table statistics. |
| `delete_dbmeta(name)` | Remove only a statistics entry; leaves the table intact. |

Malformed known-version timezone declarations raise rather than returning an
inferred value. Setters return `None`; `None` as a timezone requests removal.
Consumer metadata and timezone are separate; [usage](usage.md#consumer-metadata)
and [file format](file-format.md) describe their rules.

### Discovery, layout and maintenance

| Method | Behavior |
| --- | --- |
| `get_file_list()` | Table names without `.jsonl`; hierarchy discovery excludes hidden directories. |
| `search_file_list(regex)` | Regex search over discovered names. |
| `validate_name(name)` | Check safe hierarchy directory components; short names are valid and creation applies additional filename restrictions. |
| `lint_db(force=False)` | Verify/compact tables and write `.jsonldb/lint.log`. |
| `lint_hierarchy(hierarchy_depth)` | Reorganize to a positive maximum depth; short names remain visible; collisions raise. |
| `reprocess_invalid_tickers()` | Explicitly restore safe quarantined tables, including short names, without overwriting destinations. |
| `delete_empty_folders()` | Prune empty visible directories. |
| `build_configmeta()` / `build_hmeta()` | Persist live configuration/hierarchy settings; normal callers use the higher-level operations. |
| `create_folder(folder_path)` | Ensure a directory exists; does not configure a database. |
| `str(db)` / `repr(db)` | Render table statistics without loading table rows. |

## Single-file interfaces

Use `from jsonldb import jsonlfile, jsonldf` for paths independent of a database
folder. These APIs do not maintain `FolderDB` statistics or persistent reports.
Default `timespec=None` falls back to the file module's default of seconds.

| Purpose | `jsonlfile` | `jsonldf` |
| --- | --- | --- |
| Save | `save_jsonl(path, rows, timespec=None, meta=None, slot_bytes=None)` | `save_jsonldf(path, df, timespec=None, meta=None, slot_bytes=None)` |
| Load | `load_jsonl(path, auto_deserialize=True, timespec=None)` | `load_jsonldf(path, timespec=None, auto_deserialize=True)` |
| Range | `select_jsonl(path, lower_key=None, upper_key=None, auto_deserialize=True, timespec=None)` | `select_jsonldf(path, lower_key=None, upper_key=None, auto_deserialize=True, timespec=None)` |
| Upsert existing | `update_jsonl(path, rows, timespec=None, meta=None)` | `update_jsonldf(path, df, timespec=None, meta=None)` |
| Delete keys | `delete_jsonl(path, keys, timespec=None)` | `delete_jsonldf(path, keys, timespec=None)` |
| Lint | `lint_jsonl(path, force=False, slot_bytes=None)` | `lint_jsonldf(path, force=False, slot_bytes=None)` |

Argument names `path`, `rows` and `keys` above are descriptive shorthand for
positional arguments; consult function signatures for keyword use. The ordering
of `timespec` and `auto_deserialize` differs between the two load functions.
Only save creates files; low-level update requires an existing table. Reads
return dictionaries/DataFrames, writes return `None`, and lint returns whether
the file existed. Missing load/select/update/delete paths raise. Malformed data
rows are skipped with logging on reads; lint may remove them.

Additional file-store functions:

- `select_line_jsonl(path, linekey, auto_deserialize=True, timespec=None)`:
  indexed point read; absent/unreadable key returns `{}`.
- `load_index(path)`: independent mutable index, recovering missing/invalid/stale
  indexes. `build_jsonl_index(path, warn_invalid=True)` explicitly rebuilds it;
  `ensure_index_exists(path)` handles missing, empty and stale indexes.
- `read_jsonl_meta(path)` / `write_jsonl_meta(path, meta)`: read/publish a slot
  record; write requires a slot and preserves timezone.
- `read_jsonl_timezone(path)` / `write_jsonl_timezone(path, timezone)`:
  table timezone access, with the same lifecycle rules as `FolderDB`.
- `migrate_jsonl_slot(path, slot_bytes)`: resize/insert a slot; return whether the
  data file changed. A retry also repairs its index.
- `save_jsonl_atomic(path, rows, timespec=None)`: protected control-file writer;
  it is not a transactional table-write API and refuses timezone-declared tables.
- `serialize_linekey(linekey, timespec=None)`,
  `deserialize_linekey(linekey_str, default_format=None)` and
  `detect_timespec(linekey)`: key-format helpers. Explicit deserialization needs
  `default_format="datetime"`; detection returns a recognized precision or `None`.

`metaslot` and `reports` implement storage/report details. Underscore-prefixed
helpers and cache objects are private implementation machinery.
