# Folder Database

`FolderDB` turns a directory into a database of named tables. It owns naming, layout, configuration, metadata, and reports. Data operations resolve paths, apply settings, and delegate to the file store.

## Opening a database

The root directory must already exist. Opening reads control files, restores missing controls where possible, and can reorganize tables or correct a timespec that disagrees with the data. [Hierarchical Layout](folder_database/hierarchical_layout.md) owns placement, recovery, and migration, including access to older layouts at unchanged depth. Opening preserves table contents and records recovery in `.jsonldb/integrity.log`.

## Table naming and paths

A table name is a string without the `.jsonl` suffix; the suffix is tolerated and stripped. Hierarchy uses delimiter-separated prefixes (default `.`) as directories up to the configured maximum, excluding the final segment. Short names are valid, single-segment names live at the root, and filenames retain the complete name. Two resolvers exist on purpose:

- a **read resolver** that maps a name to a path without creating anything, used by every query and delete; and
- a **write resolver** that also creates intermediate folders, used by saves and upserts.

Reads of missing tables leave no disk trace. Discovery includes both root-level and nested `.jsonl` files in hierarchy mode, skipping hidden directories; a regex search over the names serves group operations.

## Data operations

Dictionary and DataFrame operations share these shapes:

| Verb | Meaning |
|---|---|
| `overwrite_*` | Replace the table wholesale; `meta=` sets its record. |
| `upsert_*` | Update existing keys in place and add new ones; create the table if absent; `meta=None` keeps the record, a dict replaces it after the rows. |
| `get_*` | Read tables in ascending [observation order](jsonl_file_store/ordered_reads.md), with inclusive optional bounds; omitted names select all tables. |
| `get_*_with_meta` | One table: a named tuple of `meta` and `rows`, read record first. |

Plural writes take `{name: content}` mappings; read results use table names. Missing tables are omitted from dictionary reads, reported for DataFrame reads, and yield `None` plus empty rows with metadata.

Dictionary, DataFrame, and `_with_meta` reads forward keyword-only `strict=False` using the [file-store contract](jsonl_file_store/strict_reads.md); strict failures propagate without a partial return.

Record operations: `read_meta` returns the record or `None`, `clear_meta` blanks it, `set_meta_slot_bytes` records the folder's slot width and rewrites every table to it, refusing to shrink below any existing record. Any code needing record and rows reads the record first; the `_with_meta` pair makes that order unmissable.

Every save and upsert finishes by refreshing that table's entry in `db.meta`.

## Delete operations

Key deletion accepts a batch; range deletion uses inclusive bounds interpreted with database precision and table timezone. Default deletion leaves tombstones and preserves the table and its consumer metadata.

Single-table key and range deletion accept keyword-only `remove_if_empty=False`. Opting in removes the table when the request deletes its last rows. Empty key batches and requests matching no rows preserve an already-empty table. A batch reuses one table index for selection, mutation, and statistics, without scanning row history. Partial deletion publishes one reduced index; final removal avoids publishing an empty table or index.

Whole-table deletion removes data, index, and the table's database metadata entry, then prunes empty visible folders. Cleanup is retryable after partial completion, including when the data file is already absent. A failed unlink must leave final rows recoverable for retry. Cleanup does not provide a transaction or rollback. [Metadata and Timespec](folder_database/metadata_and_timespec.md) owns statistics refresh and retry consistency.

With `force=True`, clearing removes visible tables and indexes, preserves configuration and hierarchy controls, and resets `db.meta` to an empty indexed table. The instance retains its configuration. Without force, only warn.

## Maintenance

Database-level lint iterates the tables recorded in `db.meta`, lints each, drops entries for vanished tables, rewrites `db.meta` in one pass with lint timestamps, then lints `db.meta` and, in hierarchy mode, `h.meta`. In a slot-enabled folder it also repairs any table whose line one is not a slot of the folder width. `force` passes through to every table; findings go to `.jsonldb/lint.log`. Tables absent from `db.meta` are picked up by the next metadata rebuild, not by lint.

## Inspection

Printing a database renders `db.meta`: sizes, row counts, key ranges, lint status.

## Design choices and trade-offs

- **Per-instance configuration.** Timespec and slot width live on the instance and are passed explicitly on every call. Independent settings let databases coexist in one process.
- **Metadata as JSONL.** Control files reuse the table format and file-store functions, gaining indexing and lint for free at the cost of an `.idx` each.
- **No locking.** Concurrent writers to one database are not coordinated, matching the single-owner deployment model. Reads concurrent with a publish are not serialized; the record-first order bounds what they can see.

## Source target

- `jsonldb/folderdb.py`: at most 1250 lines in total, shared with the hierarchical-layout, metadata, metadata-slot, and integrity-logs notes.
