# JSONL File Store

The file store is the lowest layer: a set of functions that treat one `.jsonl` file as an ordered key-value table. It has no notion of folders or databases; every function takes a path. Higher layers compose it and pass down the database timespec.

## Record format

One record per line, one line per record:

```
{"2024-01-01T09:30:00": {"open": 101.2, "volume": 4300}}
{"2024-01-01T09:31:00": {"open": 101.4, "volume": 3900}}
```

The single top-level key is the serialized linekey; its value is the record. Lines are UTF-8, newline-terminated, and serialized with orjson, which also accepts NumPy scalars. Blank lines are legal and mean "nothing here"; they are how deletions and moved records leave their old space behind.

## Key handling

Every public function accepts linekeys as strings or datetimes and serializes them before touching the file. Datetimes become ISO text at the caller's timespec, falling back to the module default of seconds when none is given. On the way out, `auto_deserialize` (on by default) converts keys that look like datetimes at that precision back into `datetime` objects. Passing the same timespec on every call keeps a table's keys uniform; the folder layer does this automatically.

## Operations

| Operation | Behaviour | Cost |
|---|---|---|
| save | Rewrite the file from a dict in the order given; write a fresh sorted index. An empty dict yields an empty file and an empty index. | O(n) |
| load | Stream the whole file into a dict, skipping blank and malformed lines. | O(n) |
| select range | Bisect the sorted index keys for an inclusive `[lower, upper]` range, read the chosen lines in file order, return them in key order. An omitted bound means the table's first or last key. Both omitted is a plain load; equal bounds is a single-key lookup. | O(log n + k) |
| select one | Index lookup, seek, parse. A missing or unreadable key returns an empty dict. | O(1) |
| update (upsert) | Per key: overwrite in place if the new line fits, else tombstone and append; unknown keys append. Index rewritten once at the end. | O(k) writes |
| delete | Tombstone each listed line and drop its key from the index. | O(k) writes |

Range bounds are compared as serialized strings, so the same lexical ordering that governs the index governs selection.

## In-place update rules

An update never moves a record that still fits. The replacement line is padded with spaces before its newline so the record keeps its exact old length and the following line's offset stays valid. A record that outgrows its slot is blanked in place and re-appended, and the index is pointed at the new offset. Before appending, a file that lacks a trailing newline is healed so the append starts on a fresh line.

This keeps upserts proportional to the number of touched keys rather than the file size, at the price of dead space and an unsorted physical layout. Both are repaired by lint.

## Durability model

Saves, updates, and deletes write directly to the target file, and the index is written after the data. A crash mid-write can leave a partial data file or a stale index. The index side is self-healing (see the index-integrity note). The data side is not: the library assumes the host process controls when writes happen and can re-run them. Only the lint rewrite uses a temporary file and an atomic replace.

## Error policy

- A missing file on load, select, update, or delete raises; the file store never creates a table implicitly. Only save creates files.
- Malformed JSON lines are reported to standard output and skipped on both load and index build.
- Filesystem errors are re-raised with the path added to the message.

## Configuration

Two module constants shape behaviour: a large read and write buffer (tens of megabytes) chosen for sequential throughput on big tables, and the default timespec. The timespec constant is a fallback only; higher layers never mutate it.

## Source target

- `jsonldb/jsonlfile.py`: at most 750 lines in total, shared with the index-integrity note.
