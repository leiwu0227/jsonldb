# Metadata and Timespec

A database keeps settings in `config.meta` and table statistics in `db.meta`. Both use ordinary JSONL records without metadata slots. Statistics inform consumers; they do not replace table observations.

## Settings in `config.meta`

**Timespec** fixes datetime linekey precision for the database: `seconds` or `microseconds`. Opening without configuration selects seconds. Another precision requires configuration before the first open. Uniform precision and a shared fixed offset make timestamp text order chronological. Mixed precision breaks range selection, so precision is a database property.

**Slot width** is recorded by the explicit width operation, which rewrites every table. Its presence enables slots: new tables receive that width, and lint repairs deviations. Its absence leaves files untouched. Readers measure line one instead of consulting configuration. The metadata-slot note owns the remaining behavior.

Timezone is an optional table envelope property, separate from precision and consumer metadata. [Table Timezone](../jsonl_file_store/table_timezone.md) owns normalization, validation, and lifecycle. Configuration and precision repair do not infer timezone.

### Timespec repair on open

Earlier versions could write a precision that disagreed with configuration. Opening checks recorded minimum and maximum keys first. If a datetime-looking boundary disagrees, it scans indexed table keys. One consistent differing precision causes configuration repair and a warning. Mixed precisions leave configuration unchanged and produce a warning; the caller decides how to repair them. Healthy boundaries require no extra index scan.

## Table statistics in `db.meta`

Each table record holds its name, resolved path, smallest and largest sorted keys, file size, row count, and lint status and time. Stale statistics can misreport table contents but do not determine data operations.

### Refresh policy

- **Incremental.** Saves, upserts, and key or range deletes synchronously refresh the affected table. They reuse resulting index statistics and file size without rereading the index or scanning row history. Collect statistics only when needed. Explicit refresh, rebuild, and lint retain disk-based recovery.
- **On open.** Missing metadata or detected folder changes trigger rebuilding. Directory modification times detect entry changes, including hierarchy folders, but do not reliably detect changes to existing contents.
- **On lint.** Database lint rewrites metadata and marks surviving tables as linted.

Whole-table deletion removes its entry, including retries after the data file disappears. Deleting an entry does not rebuild missing database metadata.

Metadata failure does not roll back completed table mutations. Retrying partial deletion reconciles statistics with surviving rows, even when requested keys are already absent. Recovery preserves the no-match table-retention rule in [Folder Database](../folder_database.md).

Measuring a table without an index builds it. A database without tables has an intentionally empty `db.meta`.

### Passive statistics reads

Consumers can request existing metadata without building it. Default reads retain build-if-missing behavior. Passive mode returns an explicit unavailable result when `db.meta` is absent.

Statistics describe the last published persisted state across the full table history. Count means stored rows, not populated fields. Pending consumer cache changes are excluded. An available empty summary has count zero and null bounds. A missing entry means unavailable, not proof of an empty table.

Passive reads do not rebuild metadata or indexes, scan table observations, create files, or flush changes. Malformed or unreadable metadata fails the read without returning partial statistics or implying zero counts. Default error handling remains unchanged.

This guarantee applies to reads on existing instances. Opening retains recovery behavior. Passive reads add no locking or cross-table snapshot guarantee. Cost scales with metadata read, potentially covering every table. Consumers own selection, key-bound interpretation, and display calculations.

## Atomic control files

Rarely written `config.meta` and `h.meta` use temporary files and atomic replacement to prevent torn controls on open. Frequently written `db.meta` uses in-place updates. Torn lines can make statistics unavailable until rebuilding.

## Reports and inspection

The hidden `.jsonldb/` folder holds `integrity.log`, replaced on open, and `lint.log`, replaced on lint. The integrity-logs note owns reports. Printing renders metadata sizes, counts, ranges, and lint status without opening tables.

## Trade-offs

JSONL metadata reuses one format but needs control-file indexes and in-place statistics updates. Automatic precision repair adds opening cost when boundaries disagree, while preserving the cheap healthy path.

## Source target

- `jsonldb/folderdb.py`: at most 1250 lines in total, shared with the folder-database, hierarchical-layout, metadata-slot, and integrity-logs notes.
