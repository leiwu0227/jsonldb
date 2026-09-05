# Metadata and Timespec

Beside its tables, a database keeps two small control files recording what the library needs to know about the database as a whole: `config.meta` for settings and `db.meta` for per-table statistics. Both are JSONL files in the ordinary record format, read and written with the file store, and never carry a metadata slot.

## Settings in `config.meta`

**Timespec** fixes how datetime linekeys are serialized for the whole database: `seconds` or `microseconds`. A database opened without one receives the library default of seconds; a different precision is chosen by writing `config.meta` before the first open. Uniform precision and a shared fixed offset make timestamp text order chronological. Mixing precisions in one table breaks range selection silently, which is why the value is a database property rather than a per-call option.

**Slot width** is recorded by the explicit width operation, which also rewrites every table to that width. Its presence makes the folder slot-enabled: new tables get a slot of that width at creation and lint repairs any table that deviates. Its absence leaves every file untouched. Readers never consult it; they measure line one. The metadata-slot note owns the rest.

Timezone is an optional per-table envelope property, separate from database precision and consumer metadata. [Table Timezone](../jsonl_file_store/table_timezone.md) defines normalization, validation, and lifecycle. It is not inferred from configuration or precision repair.

### Timespec repair on open

Because an earlier version held the timespec in a shared module variable, a database could be written at one precision while its configuration said another. Open checks for that disagreement in two stages, cheapest first:

1. Look at the minimum and maximum keys recorded in `db.meta`. If a datetime-looking boundary key has a precision other than the configured one, continue; otherwise stop with no extra I/O.
2. Scan every indexed table's keys and collect the precisions found. If exactly one is present and it differs, rewrite `config.meta` to match and warn. If more than one is present, keep the configuration and warn that the data is mixed; repairing mixed data is a caller decision.

The staged check keeps the healthy case free of index scans while healing the contaminated case automatically.

## Table statistics in `db.meta`

One record per table: name and resolved path; smallest and largest key, from the sorted index; file size and record count; whether and when the table was linted. Statistics are informational. No read or write path consults them, so a stale `db.meta` can misreport but never corrupt.

### Refresh policy

- **Incremental.** Saves, upserts, and key deletes refresh that one table's record. Saves and upserts reuse statistics from the successful write, avoiding a reread of the published table index. These statistics match the published index and file size. Refresh remains synchronous for each table; a later metadata failure does not roll back the table. Explicit refresh, rebuild, and lint derive statistics from disk and retain recovery behavior.
- **On open.** If `db.meta` is missing, or the folder was modified after `db.meta` was written, every table is re-measured. Folder modification time changes only when entries are added or removed at the root, so this catches external file drops in flat mode and misses edits to existing files, which is accepted.
- **On lint.** Database lint rewrites the whole file and marks every surviving table as linted.

Table deletion and range deletion do not refresh statistics; they rely on the open-time check or the next lint. Measuring a table with no index builds the index first. A database with no tables has an intentionally empty `db.meta` rather than none.

## Atomic control files

`config.meta` and `h.meta` are single-record files written rarely. They are written through a temporary file and an atomic replace so that open can never meet a torn control file; a torn `h.meta` would otherwise make a hierarchy database fail to open. `db.meta` is written often and in place, and a torn line there costs only one table's statistics until the next rebuild.

## Reports

A hidden `.jsonldb/` folder holds `integrity.log`, replaced by every open, and `lint.log`, replaced by every lint. The integrity-logs note defines them.

## Inspection

The printed form of a database renders `db.meta`: table sizes, counts, key ranges, and lint status, without opening any table.

## Trade-offs

- Storing metadata as JSONL keeps one file format, at the cost of an `.idx` per control file and of `db.meta` writes going through the in-place update path.
- Repairing the timespec on open makes opening slightly more expensive on a large database with datetime keys, in exchange for never operating on a misconfigured database.

## Source target

- `jsonldb/folderdb.py`: at most 1250 lines in total, shared with the folder-database, hierarchical-layout, metadata-slot, and integrity-logs notes.
