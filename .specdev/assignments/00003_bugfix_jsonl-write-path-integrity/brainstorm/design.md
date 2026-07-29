# Design: Fix data-integrity bugs in the JSONL write path

## Overview

Five verified bugs, all in the write/delete path. Each was reproduced with a live script before
this assignment was created, so every fix starts from a known-failing test. All fixes are
behavior-preserving: no public API signatures change.

| # | Bug | Location | Severity |
|---|-----|----------|----------|
| 1 | Append corrupts file when last line lacks trailing newline | `jsonlfile.py` `update_jsonl` (~line 599) | Silent data loss |
| 2 | Shrink-in-place update overwrites the line terminator | `jsonlfile.py` `update_jsonl` (~lines 620-624) | Structural corruption |
| 3 | `delete_file_range` uses `str(datetime)` for bounds | `folderdb.py` (~lines 481-484) | Wrong rows deleted / kept |
| 4 | `clear_folder` only clears the root folder | `folderdb.py` (~lines 426-428) | Data survives a forced clear |
| 5 | `delete_file` crashes when `.idx` is missing | `folderdb.py` (~line 441) | Crash |

## Root Cause

1. **Append without newline guard:** `update_jsonl` seeks to EOF and appends. If the existing last
   line has no trailing `\n` (file written externally or truncated), the appended record
   concatenates onto it: `{"a":...}{"b":...}` — invalid JSON, both records unreadable.
   `delete_jsonl` already guards this case; `update_jsonl` does not.
2. **Shrink padding placement:** when a record shrinks, the code writes the new line (ending in
   `\n`) and then pads with raw spaces *after* it. The padding overwrites the old line's final
   `\n`, so the padding merges with the next record's line. Reads survive only because orjson
   tolerates leading whitespace; index offsets drift onto padding bytes. The grow path and
   `delete_jsonl` both correctly write `spaces + b'\n'`.
3. **Bound serialization mismatch:** index keys are stored via `serialize_linekey()`
   (`isoformat()`, `T` separator) but `delete_file_range` compares against `str(key)` (space
   separator). Lexically `' ' < 'T'`, so any bound sharing a date with data compares wrong —
   verified to both over-delete and under-delete.
4. **`os.listdir` vs tree walk:** hierarchy mode stores files in nested folders; `clear_folder`
   lists only the root.
5. **Unconditional `os.remove` of the `.idx`** with no existence check.

## Fix Design

1. **Append guard (`update_jsonl`):** after `f.seek(0, os.SEEK_END)`, if the file is non-empty and
   its last byte is not `\n`, write `b'\n'` before recording `append_pos`/appending. Mirrors the
   existing guard in `delete_jsonl`.
2. **Shrink padding (`update_jsonl`):** when `len(new_line) < len(old_line)`, write
   `new_line[:-1] + b' ' * (old_len - new_len) + b'\n'` — the record region keeps its exact old
   length and its terminating newline. Trailing spaces inside a line are valid JSON whitespace, so
   `readline()` + `orjson.loads()` round-trips. Equal-length writes stay as-is.
3. **Bounds (`delete_file_range`):** replace `str(lower_key)`/`str(upper_key)` with
   `serialize_linekey(lower_key)`/`serialize_linekey(upper_key)` (import from `jsonlfile`).
   String keys are unaffected (`serialize_linekey` is identity for `str`).
4. **`clear_folder`:** walk the tree with `os.walk` and remove `.jsonl`/`.idx`/`.meta` files at any
   depth, then call `delete_empty_folders()` and `build_dbmeta()` as today. Keep skipping
   hidden directories (e.g. `.git`, `.invalid_tickers`) so version control data is untouched.
5. **`delete_file`:** remove the `.idx` only if it exists; switch the three delete methods
   (`delete_file`, `delete_file_keys`, `delete_file_range`) from `_get_or_create_file_path` to
   `_get_file_path` so deletes stop creating directories.

## Key Decisions

- **Pad-before-newline (not blank-and-append) for shrink:** keeps the in-place update O(1) and
  preserves the existing file-size behavior; only the placement of the padding changes.
- **Heal-on-append (write the missing `\n`) rather than reject:** matches `delete_jsonl`'s
  existing precedent and repairs files damaged by older versions instead of failing on them.
- **`_get_file_path` for deletes:** a delete should never create folders; behavior for existing
  files is identical.

## Success Criteria

- Five new failing-first tests (converted from the verification scripts) pass:
  1. Append to a file with no trailing newline → both records load correctly.
  2. Shrink-update → raw bytes show the padded region ends with `\n`; rebuilt index points at
     real record starts; round-trip load matches.
  3. Datetime range delete: same-day range deletes exactly the in-range keys; a noon lower bound
     does not delete a 1 AM key.
  4. `clear_folder(force=True)` on a hierarchy DB leaves no `.jsonl` files anywhere in the tree.
  5. `delete_file` with a missing `.idx` deletes the data file without raising.
- The full existing suite (`unit_tests/`) still passes.
- No public API signature changes.

## Testing Approach

TDD per task: add the failing test to the matching file in `unit_tests/` (`test_jsonlfile.py` for
fixes 1-2, `test_folderdb.py` for 3-5), confirm it fails for the expected reason, apply the
minimal fix, confirm green, then run the whole suite.
