# Index Integrity and Lint

The index is a cache of the data file. This note describes how the library keeps that cache trustworthy without asking callers to manage it, and how lint restores the data file to its canonical layout.

## Single loader

All index reads in the library go through one loader. Before returning, it guarantees the index is present and parseable:

- **Missing** index: rebuilt from the data file.
- **Empty** index file: treated as corrupt (a valid empty index is `{}`, never zero bytes) and rebuilt with a warning.
- **Stale** index, older than the data file by modification time: rebuilt silently.
- **Unparseable** index: rebuilt with a warning and re-read.

The rebuild is a full scan of the data file that records the byte offset of every non-blank line keyed by its linekey, then writes the index with keys sorted. A blank-line tombstone is naturally excluded, so rebuilding after a crash also forgets any record whose deletion reached the data file but not the index.

Routing every reader through this loader is a deliberate choice: a stale or empty index once caused silent write failures downstream, and centralizing the healing removes that class of bug from every call site at once.

## What the loader does not catch

An index that parses but points at the wrong offsets, or that lists a different set of keys than the file holds, looks healthy to the loader. Detecting that requires comparing against the data file, which is lint's job.

## Lint

Lint is the maintenance operation for one table. Its contract: after lint, the index matches the data file exactly, and the data file is sorted by key and free of dead space. It reports whether the file existed at all, so a folder-level caller can drop metadata for vanished tables.

Lint proceeds through checks of increasing cost, stopping early when the file is already clean:

1. **Cardinality** (full path only): count non-blank lines with a memory-mapped scan and compare with the index size. A mismatch means orphan or missing lines; rebuild the index.
2. **Spot check**: parse the first and last indexed lines and confirm their keys match. Any failure, including a non-integer offset, forces a rebuild.
3. **Layout check**: if the index keys are already in order, the first record starts at byte zero, and the last record ends exactly at end of file, the file is sorted and compact and lint stops.

Otherwise lint rewrites the file by reading each line at its indexed offset in sorted key order into a temporary file, atomically replaces the original, and rebuilds the index. Tombstones vanish because they are not in the index.

## Fast path and `force`

The cardinality scan is proportional to file size, and for tables of tens of megabytes it dominated save time in production. By default lint skips it when the index is at least as new as the data file, on the reasoning that every library write updates the index synchronously, so a fresh index already reflects the file. Passing `force` runs the scan regardless. Use `force` after a crash, after external edits, or when an index was restored from elsewhere; use the default for routine post-write maintenance.

The accepted trade-off is that a stale index carrying a fresh timestamp, which only external tampering produces, is not caught by default.

## Interaction with datetime keys

Lint sorts by serialized text. Because the index is also sorted textually, lint and range selection always agree on order, but only a database with a single timespec has chronological order for datetime keys.

## Source target

- `jsonldb/jsonlfile.py`: at most 750 lines in total, shared with the JSONL file store note.
