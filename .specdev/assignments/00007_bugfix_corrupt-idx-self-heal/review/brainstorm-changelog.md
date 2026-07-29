## Round 1

- [F1.1] **Addressed.** Brought `visual.py` into scope. The design's Root Cause
  now enumerates the six raw `json.load` index reads in
  `visualize_jsonl_bokeh` (`visual.py:64`), `visualize_jsonl_matplot` (`:129`),
  `visualize_folderdb_bokeh` (`:228`, `:258`), and `visualize_folderdb_matplot`
  (`:342`, `:371`), and notes they currently `raise FileNotFoundError` on a
  missing index. The Fix Design (section 3) routes all six through
  `jsonlfile.load_index`, removing the `FileNotFoundError` guards (build-from-
  `.jsonl` is a benign improvement, source of truth) and eliminating the divergent
  stdlib-`json` loader. Added Success Criterion 7 (visual paths recover; no raw
  `orjson.loads`/`json.load` of an `.idx` remains outside `load_index`), a
  visual.py testing bullet, and updated the proposal to name all three modules.
  The "single index-read path" claim is now literally true across the library.

## Round 2

- [F2.1] **Addressed.** Corrected the mislabel: the existing rebuild-and-reread
  index reads are in `lint_jsonl` (`:203` fast path, `:219`/`:224` full mmap
  path) and `_verify_and_compact` (`:136`), not `load_jsonl` (which reads no
  `.idx`). Updated the Overview, Root Cause, and Fix Design section 3 to name
  `lint_jsonl`/`_verify_and_compact` and to explicitly route the raw
  `lint_jsonl(force=True)` reads at `:219`/`:224` through `load_index`. Added
  Success Criterion 8 (`lint_jsonl(force=True)` against a garbage idx recovers
  via `load_index`) and a matching `lint_jsonl(..., force=True)` testing case so
  the maintenance/lint path is not left outside the single loader.

