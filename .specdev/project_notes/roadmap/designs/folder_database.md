# Folder Database

`FolderDB` is the public entry point. It turns a directory into a database of named tables and is the only layer that knows table names, folder layout, per-database configuration, metadata, and reports. Every data operation resolves a name to a path, applies the database settings, and delegates downward.

## Opening a database

The directory must already exist; the library never creates the root. Opening reads the control files (`h.meta`, `config.meta`, `db.meta`), creates the ones that are missing, and may perform two repairs: reorganizing files when a hierarchy depth is requested that differs from the stored one, and correcting the recorded timespec when the data disagrees with it. Opening never writes to a table; whatever it regenerated or skipped is recorded in `.jsonldb/integrity.log`. Opening is where the database is made self-consistent.

## Table naming and paths

A table name is a string without the `.jsonl` suffix; the suffix is tolerated and stripped. Names may contain a delimiter (default `.`) that hierarchy mode interprets as path segments. Two resolvers exist on purpose:

- a **read resolver** that maps a name to a path without creating anything, used by every query and delete; and
- a **write resolver** that also creates intermediate folders, used by saves and upserts.

This split guarantees that reading a non-existent table leaves no trace on disk. Discovery lists `.jsonl` files at the root or walks the tree in hierarchy mode, skipping hidden directories; a regex search over the names serves group operations.

## Data operations

Two families with identical shapes, one for dictionaries and one for DataFrames:

| Verb | Meaning |
|---|---|
| `overwrite_*` | Replace the table wholesale; `meta=` sets its record. |
| `upsert_*` | Update existing keys in place and add new ones; create the table if absent; `meta=None` keeps the record, a dict replaces it after the rows. |
| `get_*` | Select an inclusive key range from one or many tables; no names means all tables. |
| `get_*_with_meta` | One table: a named tuple of `meta` and `rows`, read record first. |

Plural forms take a `{name: content}` mapping and loop; results are keyed by table name. A missing table is omitted from a dictionary read, reported for a DataFrame read, and returned as `None` plus an empty container by the `_with_meta` pair.

Record operations: `read_meta` returns the record or `None`, `clear_meta` blanks it, `set_meta_slot_bytes` records the folder's slot width and rewrites every table to it, refusing to shrink below any existing record. Any code needing record and rows reads the record first; the `_with_meta` pair makes that order unmissable.

Every save and upsert finishes by refreshing that table's entry in `db.meta`.

## Delete operations

- Delete specific keys from one table, or an inclusive key range from one or several tables. Range bounds are serialized with the database timespec so datetime bounds match stored keys. The record is preserved.
- Delete a whole table, removing its data and index and pruning folders it leaves empty.
- Clear the database, removing every data, index, and metadata file under non-hidden folders; this requires an explicit `force` flag and otherwise only warns.

Key and range deletes leave tombstones; table deletion and clearing remove files.

## Maintenance

Database-level lint iterates the tables recorded in `db.meta`, lints each, drops entries for vanished tables, rewrites `db.meta` in one pass with lint timestamps, then lints `db.meta` and, in hierarchy mode, `h.meta`. In a slot-enabled folder it also repairs any table whose line one is not a slot of the folder width and removes orphan companions. `force` passes through to every table; findings go to `.jsonldb/lint.log`. Tables absent from `db.meta` are picked up by the next metadata rebuild, not by lint.

## Version-control facade

`commit`, `revert`, and `version` forward to the Git module, initializing a repository on the first commit. The module is imported inside those methods, so GitPython loads only when used.

## Inspection

Printing a database renders `db.meta`: sizes, row counts, key ranges, lint status.

## Design choices and trade-offs

- **Per-instance configuration.** Timespec and slot width live on the instance and are passed explicitly on every call. Two databases with different settings coexist in one process; an earlier design mutated a module global, which leaked precision between databases.
- **Metadata as JSONL.** Control files reuse the table format and file-store functions, gaining indexing and lint for free at the cost of an `.idx` each.
- **No locking.** Concurrent writers to one database are not coordinated, matching the single-owner deployment model. Reads concurrent with a publish are not serialized; the record-first order bounds what they can see.

## Source target

- `jsonldb/folderdb.py`: at most 1250 lines in total, shared with the hierarchical-layout, metadata, metadata-slot, and integrity-logs notes.
