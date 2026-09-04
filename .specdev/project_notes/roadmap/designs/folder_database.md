# Folder Database

`FolderDB` is the public entry point. It turns a directory into a database of named tables and is the only layer that knows table names, folder layout, per-database configuration, and metadata. Every data operation resolves a name to a path, applies the database's timespec, and delegates to the file store or the DataFrame adapter.

## Opening a database

The directory must already exist; the library never creates the root. Opening reads the control files (`h.meta`, `config.meta`, `db.meta`), creates the ones that are missing, and may perform two repairs: reorganizing files when a hierarchy depth is requested that differs from the stored one, and correcting the recorded timespec when the data disagrees with it. Both are described in the sibling notes. Opening is therefore not free on a large database, but it is the point at which the database is guaranteed self-consistent.

## Table naming and paths

A table name is a string without the `.jsonl` suffix; the suffix is tolerated and stripped. Names may contain a delimiter (default `.`) that hierarchy mode interprets as path segments. Two resolvers exist on purpose:

- a **read resolver** that maps a name to a path without creating anything, used by every query and delete; and
- a **write resolver** that also creates intermediate folders, used by saves and upserts.

This split guarantees that reading a non-existent table leaves no trace on disk.

Table discovery lists `.jsonl` files at the root, or walks the tree in hierarchy mode, skipping hidden directories. A regex search over discovered names serves callers that address groups of tables.

## Data operations

Two families with identical shapes, one for dictionaries and one for DataFrames:

| Verb | Meaning |
|---|---|
| `overwrite_*` | Replace the table wholesale with the given content. |
| `upsert_*` | Update existing keys in place and add new ones; create the table if absent. |
| `get_*` | Select an inclusive key range from one or many tables; no names means all tables. |

Plural forms take a `{name: content}` mapping and loop. Results are keyed by table name. A table that does not exist is omitted from a dictionary read and reported for a DataFrame read.

Every save and upsert finishes by refreshing that table's entry in `db.meta`, so statistics stay current during normal operation without a full rebuild.

## Delete operations

- Delete specific keys from one table, or an inclusive key range from one or several tables. Range bounds are serialized with the database timespec before comparison so datetime bounds match stored keys.
- Delete a whole table, removing its data and index and pruning folders it leaves empty.
- Clear the database, which removes every data, index, and metadata file under non-hidden folders. Clearing requires an explicit `force` flag and otherwise only warns, because it is the one operation that destroys everything.

Key and range deletes leave tombstones; table deletion and clearing remove files.

## Maintenance

Database-level lint iterates the tables recorded in `db.meta`, lints each, drops entries for tables that no longer exist, rewrites `db.meta` in one pass with lint timestamps, then lints `db.meta` itself and, in hierarchy mode, `h.meta`. The `force` flag is passed through to every table. Tables present on disk but absent from `db.meta` are picked up by the next full metadata rebuild, not by lint.

## Version-control facade

`commit`, `revert`, and `version` forward to the Git module, initializing a repository in the database folder on the first commit. The module is imported inside those methods so that a database without version control never loads GitPython.

## Inspection

Printing a database renders `db.meta`: each table's size, row count, key range, and lint status.

## Design choices and trade-offs

- **Per-instance configuration.** The timespec is stored on the instance and passed explicitly on every call. Two databases with different precisions can coexist in one process. An earlier design mutated a module global, which leaked precision between databases and is the origin of the timespec repair on open.
- **Metadata as JSONL.** Control files reuse the table format and the same file-store functions, so they get indexing and lint for free. The cost is an `.idx` beside each control file.
- **No locking.** Concurrent writers to one database are not coordinated. This keeps the storage layer dependency-free and matches the single-owner deployment model.

## Source target

- `jsonldb/folderdb.py`: at most 1000 lines in total, shared with the hierarchical-layout and metadata notes.
