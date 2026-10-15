# Persisted statistics for the OceanData Portal

Date: 2026-10-15
Status: proposed handoff; no implementation or public API approval implied.

## Consumer requirement

OceanData Portal needs a lightweight ticker inventory with stored row count,
first stored date, and last stored date. The user confirmed that the **last
persisted state is sufficient**. Pending cache inserts, replacements, and
deletions do not need to be included. Statistics may lag until normal writes
or flushes publish their metadata.

This simplifies the earlier [OceanData requirement](../oceandata/.specdev/project_notes/thoughts/20261015_stored_ticker_statistics_for_portal.md),
which requested statistics for the current logical view, including pending
changes. This handoff records the revised freshness requirement.

Count means stored dated rows, not populated fields. Portal derives inclusive
calendar days and the percentage from the count and date bounds. For example,
11 rows from 2026-10-01 through 2026-10-15 display as `11 / 15 (73.3%)`.
The statistic describes the entire persisted history, independent of the
Monitor browsing range. It does not measure business-day completeness.

## Existing JSONLDB support

Source inspection of `jsonldb/folderdb.py` found that:

- `_make_meta_entry()` records `count`, `min_index`, and `max_index` in `db.meta`.
- Normal write paths publish these fields through `_update_dbmeta()` using
  write statistics. Deletion paths also update or remove the metadata entry.
- `get_dbmeta()` reads existing database metadata without loading ticker
  observation histories.
- If `db.meta` is absent, `get_dbmeta()` calls `build_dbmeta()`. That operation
  enumerates tables and loads their indexes through `_make_meta_entry()`.

The required summary fields already exist. No new storage format or cache
overlay calculation appears necessary. These findings come from source
inspection; query performance and failure cases have not been tested.

## Proposed small API addition

Provide a public way to read existing database metadata without building it.
One possible signature is:

```python
def get_dbmeta(self, *, build_if_missing: bool = True) -> dict | None:
    ...
```

The name and exact return type remain open for JSONLDB design approval.
Suggested behavior:

- Preserve existing behavior for callers that omit the option.
- With `build_if_missing=False`, read existing metadata normally.
- If metadata is absent, return an explicit unavailable result, such as `None`.
- Do not build metadata, load or rebuild table indexes, scan observation
  payloads, create files, or trigger a flush in this mode.
- Preserve error reporting for malformed or unreadable metadata. Do not turn
  these failures into an empty database or a zero row count.

A missing table entry does not by itself prove that a declared ticker is empty.
OceanData must distinguish known empty histories from unavailable summaries.
Available empty summaries have count zero and null bounds.

This addition is recommended for a clean public integration. The underlying
statistics already exist; it is not a prerequisite for calculating them.
It avoids making OceanData inspect JSONLDB metadata paths before calling the API.

## Scope and ownership

JSONLDB owns the passive metadata read. OceanData owns ticker selection,
mapping `min_index` and `max_index` to date bounds, and routing the query through
FolderData and DirectoryData. OceanData App exposes the query through
`/data_cmd`. Portal requests batches and displays approximate statistics or an
explicit unavailable state.

No cross-table transactional snapshot or reconciliation with pending OceanData
cache changes is requested. Normal reader/write coordination still applies.
Memory-only data has no persisted summary; returning unavailable is the proposed
initial behavior. The HTTP schema and memory-only behavior remain downstream
design decisions.

Reading `db.meta` still costs time proportional to the metadata read, potentially
covering all tables in a folder. Do not claim constant-time or per-ticker bounded
cost without measurement. No new per-ticker index is requested at this stage.

## Suggested verification

Use disposable data to verify populated and empty summaries, inserts,
overwrites, deletion of boundary rows, and deletion of all rows. Confirm that
existing default callers retain their behavior. In the new passive mode, verify
that absent metadata returns unavailable without creating files or invoking
metadata/index rebuilding. Verify that malformed metadata does not become zero.

Downstream tests should confirm that pending cache changes do not affect the
reported persisted summary and that normal flush/reopen operations expose the
updated summary. A representative large-folder check should document metadata
read cost without reading complete ticker payloads.
