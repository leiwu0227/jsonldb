# Proposal: Fix data-integrity bugs in the JSONL write path

A verification pass (live reproduction scripts run against the current code) confirmed five
data-integrity bugs in `jsonldb/jsonlfile.py` and `jsonldb/folderdb.py`. The worst one silently
destroys records: appending via `update_jsonl` to a file whose last line lacks a trailing newline
concatenates the new record onto the old line, producing one invalid JSON line — both records
become unreadable. The others corrupt file structure (shrink-in-place updates overwrite the line
terminator), apply wrong datetime range-delete boundaries, leave data behind on `clear_folder` in
hierarchy mode, and crash `delete_file` when an index file is missing.

This assignment fixes all five as pure behavior-preserving bugfixes (no API changes), each driven
by a failing test converted from the verification scripts.
