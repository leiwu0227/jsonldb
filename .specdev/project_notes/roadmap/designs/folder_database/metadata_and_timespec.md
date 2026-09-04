# Metadata and Timespec

Beside its tables, a database keeps two small control files that record what the library needs to know about the database as a whole: `config.meta` for the datetime precision and `db.meta` for per-table statistics. Both are JSONL files in the ordinary record format and are read and written with the file store.

## Timespec

The timespec fixes how datetime linekeys are serialized for the whole database: `seconds` or `microseconds`. It is recorded in `config.meta` as a single record. A database opened without one receives the library default of seconds; a different precision is chosen by writing `config.meta` before the first open. Every subsequent operation on that database serializes and recognizes datetime keys at that precision.

Uniform precision is what makes textual key order equal chronological order. Mixing precisions in one table breaks range selection silently, which is why the value is a database property rather than a per-call option.

### Repair on open

Because an earlier version of the library held the timespec in a shared module variable, a database could be written at one precision while its configuration said another. Opening a database now checks for that disagreement in two stages, cheapest first:

1. Look at the minimum and maximum keys recorded in `db.meta`. If a datetime-looking boundary key has a precision other than the configured one, continue; otherwise stop with no extra I/O.
2. Scan every indexed table's keys and collect the precisions found. If exactly one precision is present and it differs from the configuration, rewrite `config.meta` to match and warn. If more than one is present, keep the configuration and warn that the data is mixed; repairing mixed data is a caller decision.

The staged check keeps the common healthy case free of index scans while still healing the contaminated case automatically.

## Table statistics

`db.meta` holds one record per table:

- name and resolved path;
- smallest and largest key, taken from the sorted index;
- file size in bytes and record count;
- whether the table has been linted and when.

The statistics are informational. No read or write path consults them, so a stale `db.meta` can never corrupt data; it can only misreport.

### Refresh policy

Statistics are kept current by three mechanisms:

- **Incremental.** Saves, upserts, and key deletes refresh that one table's record after the write.
- **On open.** If `db.meta` is missing, or the database folder was modified after `db.meta` was written, every table is re-measured. Folder modification time changes only when entries are added or removed at the root, so this catches external file drops in flat mode and misses edits to existing files, which is accepted.
- **On lint.** Database lint rewrites the whole file in one pass and marks every surviving table as linted.

Table deletion and range deletion do not refresh statistics; they rely on the open-time check or the next lint. Measuring a table with no index builds the index first, so a metadata rebuild also ensures every table is indexed.

A database with no tables has an intentionally empty `db.meta` rather than none, so the presence of the file marks a folder as having been opened as a database.

## Inspection

The printed form of a database is a rendering of `db.meta`, giving an operator table sizes, counts, key ranges, and lint status without opening any table.

## Trade-offs

- Storing metadata as JSONL keeps one file format in the library, at the cost of an `.idx` sidecar per control file and of metadata writes going through the same in-place update path as data.
- Repairing the timespec on open makes opening slightly more expensive on a large database with datetime keys, in exchange for never operating on a misconfigured database.

## Source target

- `jsonldb/folderdb.py`: at most 1000 lines in total, shared with the folder-database and hierarchical-layout notes.
