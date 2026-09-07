# Integrity Logs

By default, the library repairs and skips damage rather than raising: torn rows are skipped, indexes rebuilt, control files regenerated, line one reclassified. Those repairs are silent unless someone watches process output. Two small report files make them visible after the fact, so that after a crash and relaunch a user can see what the library found and what lint removed without configuring anything.

## Files

Both live in `.jsonldb/` inside the database folder. A hidden directory is invisible to table discovery, hierarchy moves, and `clear_folder`, so the reports can never be mistaken for tables or removed by data operations.

- `.jsonldb/integrity.log` is written by `FolderDB` open, replacing the previous contents every time.
- `.jsonldb/lint.log` is written by `lint_db`, replacing the previous contents every time.

## Format

A header line with the timestamp and the writing operation, then one line per anomaly: the kind, the file, the byte offset, a one-line detail, and, for anything removed, the removed bytes decoded with replacement and capped at a fixed length. A clean run writes the header alone, so "checked at T, nothing found" is as visible as a bad run. Writing stops after a fixed number of lines, with a final line giving the count omitted, so a badly damaged table cannot produce a line per row.

## Open observes, lint repairs

Open never writes to a table. It reads control files, rebuilds what it must, and records what it saw. Torn lines it encounters while building an index are skipped and reported, not removed, because open touches only some tables and a partial removal would make its report look like a complete one.

Lint is the only operation that removes damage. It truncates an unparseable last line, heals a parseable last line that lacks a newline, drops torn unindexed lines through the index rebuild, overwrites a line one keyed `_meta` that does not parse with an empty envelope of the same length, records the removed bytes in `lint.log`, and compacts. After a crash the interrupted table's index is stale, so even a default lint rebuilds it. A torn line the index points at, which only power loss produces, is found and blanked only under `force`, the one path that parses every indexed line.

The read path never repairs. A reader that rebuilt an index while another process was appending would see a partial last line and could truncate a write in progress, so the loader keeps skipping torn lines with a warning and never modifies a data file.

## What counts as an anomaly

Anything repaired, removed, or skipped instead of raised: a torn line removed or blanked, an index rebuilt and why, a control file regenerated, a timespec corrected, a slot blanked or inserted or resized, dead lines compacted.

## Bounds and cost

Nothing appends across runs, so each file is bounded by one run's findings and needs no rotation. Each is one write per run of its operation. Ordinary reads and writes never touch either file.

## Transport

The file-level module reports anomalies through Python `logging`, with the file path in the message, and knows nothing about the report files. `FolderDB` captures those records during open and lint and writes the reports. Standard output is never used; print statements are retired as a rule. A host that wants ordinary reads and writes reported on disk configures its own handler. Opt-in [strict reads](../jsonl_file_store/strict_reads.md) deliver encountered row errors as exceptions; callers need no logging handler to receive them. Index rebuilding retains its existing warning behavior.

## Limits

The open report covers control files and whatever open rebuilt; it is a record of what open saw, not of the folder's health. Damage in any table appears in `lint.log` after `lint_db(force=True)`, or when a read encounters it, through process logging by default or a strict-read exception. The two files together are the post-crash inventory; either alone is not.

## Source targets

- `jsonldb/reports.py`: at most 150 lines; the logger capture handler and the two report writers, importing nothing from the package.
- `jsonldb/folderdb.py`: at most 1250 lines in total, shared with the other folder-database notes; the two hook points at open and lint.
- `jsonldb/jsonlfile.py`: at most 950 lines in total, shared with the file-store notes; anomaly reporting through `logging`.
