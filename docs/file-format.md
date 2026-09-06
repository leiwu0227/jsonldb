# Reading JSONLDB files directly

Files are part of JSONLDB's public value: agents and people can inspect the data
without importing the library. This guide explains the physical representation
and where it differs from the logical table returned by an indexed query.

## Rows and keys

Each data row is one UTF-8 JSON object, on one newline-terminated physical line:

```jsonl
{"2026-01-05T09:30:00":{"price":185.0,"volume":120}}
{"2026-01-05T09:31:00":{"price":185.5,"volume":80}}
```

There is exactly one top-level key. That key is the row identity, rather than a
field inside the record. The value must be an object. Nested objects, lists and
other JSON values may appear inside it. This is a specific keyed JSONL convention;
arbitrary JSONL datasets may require conversion before JSONLDB can index them.

Time-series keys normally use `YYYY-MM-DDTHH:MM:SS` or
`YYYY-MM-DDTHH:MM:SS.ffffff`. Precision comes from database configuration. An
optional table timezone supplies the shared fixed offset; absence is unspecified.
Other string keys are supported. Ordering is lexical, so `"10"` sorts before `"2"`.

## Optional first-line slot

Only line one can be a library metadata slot. The following example omits its
trailing padding spaces:

```jsonl
{"_meta":{"v":1,"timezone":"+08:00","data":{"source":"feed"}}}
{"2026-01-05T09:30:00":{"price":100.0}}
```

- `_meta` is reserved as a row key by writers.
- `v` is the library's envelope version.
- `timezone` is an optional library-owned fixed-offset declaration.
- `data` is optional opaque consumer metadata; its internal keys are unrestricted.
- The physical slot has a fixed **byte** width, including padding and newline.

An unknown envelope version is preserved opaquely and excluded from rows; it is
not interpreted as current consumer metadata. A damaged recognizable slot is
also excluded from rows. Recognizably damaged timezone declarations are refused
by mutation/repair; a completely erased declaration cannot be distinguished
from intentional absence. Never infer a replacement timezone from the machine.

## Physical state versus logical rows

Deletes blank lines with spaces. Updates that fit overwrite in place with padding;
larger records append and their old lines are blanked. Consequently:

1. Ignore blank lines and trailing padding when inspecting rows.
2. Do not treat the first-line slot as an observation.
3. Do not assume physical lines are chronologically sorted.
4. After an interrupted write, duplicate keys can remain. A sequential logical
   reconstruction uses the last valid physical occurrence of each key.
5. Malformed JSON and invalid row shapes can exist after damage. Library reads
   skip them and log their location; lint can remove them.

For a clean, quiescent table, a small standard-library-only inspection is enough:

```python
import json
from pathlib import Path

# A minimal clean-file illustration; no JSONLDB import is needed.
path = Path("inspection.jsonl")
path.write_text('{"2026-01-05T09:30:00":{"value":1}}\n', encoding="utf-8")
rows = {}
with path.open(encoding="utf-8") as source:
    for number, line in enumerate(source):
        if not line.strip():
            continue
        item = json.loads(line)  # A damaged line raises for the inspector to handle.
        if number == 0 and isinstance(item, dict) and "_meta" in item:
            continue
        if isinstance(item, dict) and len(item) == 1:
            key, record = next(iter(item.items()))
            if isinstance(record, dict):
                rows[key] = record
assert rows == {"2026-01-05T09:30:00": {"value": 1}}
```

This example leaves keys as strings and is not a repair tool. Direct inspection
is supported; arbitrary concurrent file editing is not. Before external edits,
stop the writer and preserve a copy. After edits, rebuild indexes and verify the
result as described in [portability and Git](portability-and-git.md).

## Index and control files

`<table>.jsonl.idx` is a JSON object mapping serialized keys to absolute byte
offsets, with keys written in sorted order. Slots and tombstones are excluded.
Offsets include the full byte width of the slot. An index is derived and can be
rebuilt from its table; it is not a second source of observations.

| File | Meaning | Preserve when moving data? |
| --- | --- | --- |
| `*.jsonl` | Observations and optional per-table slot. | Yes. |
| `config.meta` | Precision and optional slot width; may retain other settings. | Yes. |
| `h.meta` | Hierarchy mode, delimiter and maximum directory depth, when enabled. | Yes. |
| `db.meta` | Per-table paths, key bounds, counts, size and lint status. | Rebuildable statistics. |
| `*.idx` | Derived indexes for tables/control files. | Rebuildable. |
| `.jsonldb/integrity.log` | Findings from the most recent open. | Optional diagnostic history. |
| `.jsonldb/lint.log` | Findings from the most recent lint. | Optional diagnostic history. |
| `.hierarchy.pending` | Temporary intent record for interrupted hierarchy moves. | Yes, while present. |
| `.invalid_tickers/` | Previously quarantined tables awaiting explicit safe-name restoration. | Yes, if any data remains there. |

Control files use keyed JSONL records without metadata slots. Configuration and
hierarchy are meaningful settings; keeping them avoids relying on recovery guesses.
`db.meta` paths are informational and can be refreshed after relocation.

## Reports

Each report begins with a timestamp and operation. Findings contain a kind, path,
offset where available, and detail. Removed-byte excerpts are limited to 160
characters, and reports retain at most 100 findings plus an omitted count. A clean
run writes only its header. A report describes what that operation encountered;
an open report is not proof that every table was inspected.
