# Index Integrity and Lint

The index is a cache of the data file. This note describes how the library keeps that cache trustworthy without caller involvement, and how lint restores the canonical layout.

## Single loader

All index reads in the library go through one loader. Before returning, it guarantees the index is present and parseable:

- **Missing** index: rebuilt from the data file.
- **Empty** index file: treated as corrupt (a valid empty index is `{}`, never zero bytes) and rebuilt with a warning.
- **Stale** index, older than the data file by modification time: rebuilt silently.
- **Unparseable** index: rebuilt with a warning and re-read.

The rebuild scans the data file, records the byte offset of every non-blank line by its linekey, and writes the index with keys sorted. Tombstones are naturally excluded, so a rebuild after a crash also forgets a deletion that reached the data file but not the index.

Routing every reader through one loader is deliberate: a stale or empty index once caused silent write failures downstream, and centralizing the healing fixes every call site at once.

## What the loader does not catch

An index that parses but points at wrong offsets, or lists different keys than the file holds, looks healthy to the loader. Detecting that requires comparing against the data file, which is lint's job.

## Lint guarantees two properties

Lint is the maintenance operation for one table. It reports whether the file existed, so a folder-level caller can drop metadata for vanished tables, and guarantees on return:

- **Index fidelity.** Every indexed offset points at the line holding that key, and every non-blank line in the file is indexed.
- **Canonical layout.** Physical order equals key order, and no bytes exist outside indexed lines: no tombstones, no blank lines, no trailing fragments.

The two are independent: deletes and growing upserts create tombstones without touching any indexed offset, so a faithful index says nothing about layout. Lint checks each property separately and never infers one from the other.

## Checking fidelity

1. **Cardinality** (full path only): count non-blank lines with a memory-mapped scan and compare with the index size. A mismatch means orphan or missing lines; rebuild the index.
2. **Spot check**: parse the first and last indexed lines and confirm their keys match. Any failure, including a non-integer offset, forces a rebuild.

## Checking layout

The layout check must be exact, because a tombstone lint misses is never reclaimed later. A file is canonical when:

- offsets strictly increase when the index keys are walked in sorted order;
- the first offset is zero and the last indexed line ends exactly at end of file;
- the number of newline bytes in the file equals the index size.

The first two conditions come from the index alone. The third cannot: the index lists what exists, not what does not. Every indexed line and every tombstone the library writes ends in exactly one newline, so a count above the index size proves dead space exists. The count is a parse-free byte scan at memory speed, run on every lint as the accepted price of an exact answer. Foreign bytes without a newline, which only external edits create, are outside this guarantee.

Choosing a byte count over writer-side dead-space accounting is deliberate. Accounting would have to live in the index, changing it from a plain key-to-offset map, and would still be wrong after any external edit. The count is stateless.

If any condition fails, lint rewrites the file by reading each line at its indexed offset in sorted key order into a temporary file, atomically replaces the original, and rebuilds the index. Tombstones vanish because they are not in the index.

## Fast path and `force`

The cardinality scan parses every line and, for tables of tens of megabytes, dominated save time in production. By default lint skips it when the index is at least as new as the data file, because every library write updates the index synchronously, so a fresh index is faithful. Passing `force` runs it regardless: after a crash, after external edits, or when an index was restored from elsewhere.

The fast path skips only the cardinality scan. The spot check and the full layout check, including the newline count, run on every lint.

The accepted trade-off is that a stale index carrying a fresh timestamp, which only external tampering produces, is not caught by default.

## Interaction with datetime keys

Lint sorts by serialized text, as the index does, so lint and range selection always agree on order; only a single-timespec database has chronological order for datetime keys.

## Source target

- `jsonldb/jsonlfile.py`: at most 750 lines in total, shared with the JSONL file store note.
