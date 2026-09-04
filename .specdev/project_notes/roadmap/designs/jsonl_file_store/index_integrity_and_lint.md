# Index Integrity and Lint

The index is a cache of the data file. This note covers how the cache stays trustworthy without caller involvement and how lint restores canonical layout.

## Single loader

All index reads go through one loader that guarantees the index is present and parseable before returning:

- **Missing**: rebuilt from the data file.
- **Empty** file: corrupt (a valid empty index is `{}`, never zero bytes), rebuilt with a warning.
- **Stale**, older than the data file by modification time: rebuilt silently.
- **Unparseable**: rebuilt with a warning and re-read.

The rebuild scans the data file, skips a slot on line one, records the byte offset of every other non-blank line by its linekey, and writes the index with keys sorted. Tombstones are excluded, so a post-crash rebuild forgets a deletion that reached the file but not the index. The loader never modifies the data file: torn lines are skipped with a warning and removed only by lint.

One loader is deliberate: a stale or empty index once caused silent write failures downstream.

## Every writer finishes with the index

Freshness is judged by modification time, which on some deployments ticks once per second: two writes in the same tick compare equal, and equal means fresh. So the rule is "last", not "after": every operation that modifies a data file ends by writing the index, or by touching its modification time when offsets are unchanged, as in a slot-only write.

The index is written through a temporary file and an atomic replace, so a reader never observes a torn index.

An index that parses but points at wrong offsets or lists different keys looks healthy to the loader; detecting that is lint's job.

## Lint guarantees two properties

Lint reports whether the file existed, so the folder layer can drop metadata for vanished tables, and guarantees on return:

- **Index fidelity.** Every indexed offset points at the line holding that key, and every non-blank row is indexed.
- **Canonical layout.** Physical order equals key order, and no bytes exist outside the slot and indexed lines.

They are independent: deletes and growing upserts create tombstones without touching any indexed offset.

## Checking fidelity

1. **Cardinality** (full path only): count non-blank lines with a memory-mapped scan, less one for a slot, and compare with the index size. A mismatch means orphan or missing lines; rebuild.
2. **Spot check**: parse the first and last indexed lines and confirm their keys. Any failure, including a non-integer offset, forces a rebuild.

## Checking layout

The check must be exact: a missed tombstone is never reclaimed. A file is canonical when:

- offsets strictly increase when the index keys are walked in sorted order;
- the first offset equals the length of line one if it is a slot, else zero, and the last indexed line ends exactly at end of file;
- the number of newline bytes equals the index size, plus one for a slot;
- in a slot-enabled folder, line one is a slot of exactly the folder width.

The first two come from the index. The third cannot: every line the library writes ends in exactly one newline, so a count above the expected proves dead space, at the cost of a parse-free byte scan. Foreign bytes without a newline, which only external edits create, are outside the guarantee. Writer-side dead-space accounting was rejected: it would live in the index and still be wrong after external edits.

On any failure lint rewrites the file into a temporary file and atomically replaces it: the slot first, re-padded to the folder width or freshly created when missing, its record preserved, then each indexed line in sorted key order; then it rebuilds the index. A torn tail is truncated and torn unindexed lines are dropped by the rebuild; each is recorded in `lint.log`. A torn line the index points at, which only power loss produces, is found and blanked only under `force`, since only the forced path parses every indexed line and the rewrite copies lines by offset.

## Fast path and `force`

The cardinality scan parses every line and dominated production save time on large tables. By default lint skips it when the index is at least as new as the data file, because every library write finishes with the index; `force` runs it regardless, after a crash, external edits, or a restored index. The spot check and layout check run on every lint. A stale index with a fresh timestamp, which only tampering produces, is the accepted gap.

## Source target

- `jsonldb/jsonlfile.py`: at most 950 lines in total, shared with the file-store and metadata-slot notes.
