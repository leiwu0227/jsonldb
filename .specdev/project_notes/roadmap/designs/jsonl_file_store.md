# JSONL File Store

The file store is the lowest layer: a set of functions that treat one `.jsonl` file as an ordered key-value table with an optional metadata slot. It has no notion of folders or databases; every function takes a path. Higher layers compose it and pass down the database settings.

## Record format

One record per line, one line per record:

```
{"2024-01-01T09:30:00": {"open": 101.2, "volume": 4300}}
{"2024-01-01T09:31:00": {"open": 101.4, "volume": 3900}}
```

The single top-level key is the serialized linekey; its value is the record. Lines are UTF-8, newline-terminated, and serialized with orjson, which also accepts NumPy scalars. Blank lines are legal and mean "nothing here"; they are how deletions and moved records leave their old space behind. Line one may instead be a metadata slot: a padded single-key object under the reserved key `_meta`, defined in the metadata-slot note.

## Key handling

Every public function accepts linekeys as strings or datetimes and serializes them before touching the file. Datetimes become ISO text at the caller's timespec, falling back to the module default of seconds. On the way out, `auto_deserialize` (on by default) converts keys that look like datetimes at that precision back into `datetime` objects. Passing the same timespec on every call keeps a table's keys uniform; the folder layer does this automatically. `_meta` is reserved and rejected as a linekey.

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

An update never moves a record that still fits. The replacement line is padded with spaces before its newline so the record keeps its exact old length and following offsets stay valid. A record that outgrows its slot is appended, the index is pointed at the new offset, and the old line is blanked after the record write. Before appending, a file lacking a trailing newline is healed so the append starts on a fresh line.

This keeps upserts proportional to the number of touched keys rather than the file size, at the price of dead space and an unsorted physical layout. Both are repaired by lint.

## Durability model

Rows are written directly to the target file. Python's buffer is flushed between a table's rows and its slot, so for a process crash every completed write is visible in order and the slot ordering rule holds. No fsync is issued; power loss is outside the model. The index is written through a temporary file and an atomic replace, so a reader never sees a torn index. Only lint and the slot-width operation rewrite a data file, always through a temporary file and an atomic replace.

## Error policy

- A missing file on load, select, update, or delete raises; only save creates files.
- Reads never raise on a torn line. Every read path, including the range read, skips it and reports it through `logging`; only lint removes it.
- Filesystem errors are re-raised with the path added to the message.

## Configuration

Two module constants: a large read and write buffer for sequential throughput on big tables, and the default timespec, a fallback higher layers never mutate.

## Source target

- `jsonldb/jsonlfile.py`: at most 900 lines in total, shared with the index-integrity and metadata-slot notes.
