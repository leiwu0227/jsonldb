# JSONL File Store

The file store treats one `.jsonl` file as an ordered key-value table with an optional metadata slot. Every function takes a path; higher layers supply database settings and folder organization.

## Record format

One record per line, one line per record:

```
{"2024-01-01T09:30:00": {"open": 101.2, "volume": 4300}}
{"2024-01-01T09:31:00": {"open": 101.4, "volume": 3900}}
```

The single top-level key is the serialized linekey; its value is the record. orjson produces newline-terminated UTF-8 bytes, written without intermediate text conversion, and supports NumPy scalars. Blank lines represent space left by deleted or moved records. Line one may instead be a metadata slot: a padded single-key object under the reserved key `_meta`, defined in the metadata-slot note.

## Key handling

Linekeys are strings or datetimes, serialized before file access at the caller's timespec, defaulting to seconds. Default-on `auto_deserialize` recognizes offset-free timestamps of exactly 19 or 26 characters at the selected precision. Optional [table timezone](jsonl_file_store/table_timezone.md) governs key interpretation and input validation without changing this recognition. `_meta` is reserved and rejected as a linekey.

Saves and upserts validate record shapes and reserved keys before mutation. Within a write, stable datetime key conversions may be retained and reused. Reuse preserves input row order, physical rows and effective-index behavior when distinct keys normalize to the same text. Custom keys, datetime subclasses, timezones or mappings whose behavior is not proven stable retain their existing conversion and iteration behavior. Temporary retention trades memory for speed: its cost scales with input rows, including keys that collide, rather than effective index size. Validation failures and publication ordering remain unchanged.

## Operations

| Operation | Behaviour | Cost |
|---|---|---|
| save | Rewrite the file from a dict in the order given, with a slot when enabled; write a fresh sorted index. An empty dict yields an empty table. | O(n) |
| load | Stream the whole file into a dict, skipping the slot, blank and malformed lines. | O(n) |
| select range | Bisect the sorted index keys for an inclusive `[lower, upper]` range, read the chosen lines in file order, return them in key order. An omitted bound means the table's first or last key. Both omitted is a plain load; equal bounds is a single-key lookup. | O(log n + k) |
| select one | Index lookup, seek, parse. A missing or unreadable key returns an empty dict. | O(1) |
| update (upsert) | Per key: overwrite in place if the new line fits, else append and blank the old line; unknown keys append. Slot rewritten in place when a record is given. Index written once at the end. | O(k) writes |
| delete | Tombstone each listed line and drop its key from the index. | O(k) writes |

Range bounds are compared as serialized strings, so the same lexical ordering that governs the index governs selection.

## Index-driven and sequential paths

Index-driven paths (single lookup, range select, delete, in-place update) never see line one because the index excludes it. Sequential paths (index build, full load, save, lint) classify line one explicitly. New behaviour attaches to the index whenever possible so the slot stays invisible by construction.

## In-place update rules

Records that still fit stay at their offsets, padded with spaces before the newline to preserve their length. Larger records append; the index points to the new offset, then the old line is blanked after the record write. A missing trailing newline is healed before appending.

Upserts write in proportion to touched keys, leaving dead space and unsorted physical rows that lint repairs.

## Durability model

Rows are written directly to the target file. Python's buffer is flushed between a table's rows and its slot, so for a process crash every completed write is visible in order and the slot ordering rule holds. No fsync is issued; power loss is outside the model. The index is written through a temporary file and an atomic replace, so a reader never sees a torn index. Only lint and the slot-width operation rewrite a data file, always through a temporary file and an atomic replace.

## Error policy

- A missing file on load, select, update, or delete raises; only save creates files.
- Reads never raise on a torn line. Every read path, including the range read, skips it and reports it through `logging`; only lint removes it.
- Filesystem errors are re-raised with the path added to the message.

## Configuration

Two module constants: a large read and write buffer for sequential throughput on big tables, and the default timespec, a fallback higher layers never mutate.

## Source target

- `jsonldb/jsonlfile.py`: at most 950 lines in total, shared with the index-integrity and metadata-slot notes.
