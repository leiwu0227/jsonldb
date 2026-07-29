## Round 1

- [F1.1] **Addressed.** Removed the missing-`.idx` skip guards
  (`if not os.path.exists(idx_path): print(...); continue`) from all four
  folder-level visualization loops in `visualize_folderdb_bokeh` and
  `visualize_folderdb_matplot`. They now call
  `index_data = load_index(folderdb._get_file_path(file_name))` directly, so a
  missing sidecar index is rebuilt from the `.jsonl` source of truth — consistent
  with the single-file visualizers and the "missing index is recoverable"
  contract. Empty returned indexes are still skipped by the existing
  `if not index_data` / `if index_data:` checks after the loader has had a chance
  to rebuild. `import os` became unused in `visual.py` and was removed.

- [F1.2] **Addressed.** `_verify_and_compact`'s spot-check fallback no longer does
  a raw `orjson.loads(f2.read())` after rebuilding. It now keeps the explicit
  `build_jsonl_index(jsonl_file_path)` (still required — `load_index` alone will
  not rebuild a parseable-but-wrong/bad-offset index) and then re-reads through
  `index_dict = load_index(jsonl_file_path)`. After the explicit rebuild the index
  is valid, so `load_index` reads it without re-rebuilding, and no raw `.idx` read
  remains outside `load_index` (the only two `orjson.loads(f.read())` calls left
  are the read and rebuild-reread *inside* `load_index` itself). Bad-offset
  recovery is preserved — `test_lint_bad_index_offsets_recovery` and
  `test_lint_non_integer_index_offsets_recovery` still pass.

Full suite: 87 passed.
