---
verdict: approved
material_divergence: false
---

## Findings

No blocking findings.

**Scope inspected.** Working tree at `fc22cba` (matches every receipt's pinned revision): `jsonldb/jsonlfile.py`, `jsonldb/folderdb.py`, `README.md`, plus untracked design/implementation/outcome artifacts. `unit_tests/` is excluded by a pre-existing `.gitignore` rule (line 31), so the companion tests are present on disk but absent from `git diff` — read directly and verified.

**AC-1 (canonical path, opacity, atomicity, discovery).** `get_aux_path` derives `<owner>.jsonl.aux` and rejects non-`.jsonl` owners; `read_aux`/`write_aux` round-trip raw bytes with no parsing. `write_aux` writes a same-directory `mkstemp` file, `flush` + `fsync`, re-checks owner existence, then `os.replace` — with `except BaseException` temp cleanup that is guarded by `os.path.exists`, so a successful publish cannot be unlinked. Payload type and owner presence are validated at the public boundary (api-security guide). Discovery is unaffected: every ticker scan filters `endswith('.jsonl')`, and neither the companion nor the `.tmp` staging file matches.

**AC-2 (lifecycle ordering).** I swept all mutation sites (`shutil.move`/`os.rename`/`os.replace`/`os.remove`) across `jsonldb/*.py`; each is covered. `save_jsonl`, `update_jsonl`, `delete_jsonl`, and `_verify_and_compact` invalidate before data can diverge (`_verify_and_compact` invalidates immediately before its `os.replace`). `overwrite_df`/`overwrite_dict`, `delete_file`, `clear_folder`, and `revert` remove companions before owner content changes. The `.jsonldf` layer delegates to the covered low-level functions, so it inherits invalidation. `_move_jsonl_family` removes any stale target companion, moves the owner, then the index, then publishes the source companion last — the interruption test asserts exactly this call order and the absent-target-companion outcome.

The added `delete_jsonl` early return (no requested key present in the index) is safe: `load_index` already persists any self-healed `.idx` before that point, and skipping a no-op correctly avoids invalidating a companion whose data never changed.

**AC-3 (conservative orphans).** `lint_jsonl`, `lint_db`, `organize_folder`, and `reprocess_invalid_tickers` print `WARNING: orphan companion <path>` and leave bytes untouched; tests assert both the message and unchanged content. `_get_aux_files` skips hidden directories while retaining `.invalid_tickers`, so `.git` is never traversed.

**Receipts.** Five receipts, all `passed`, all pinned to `working-tree@fc22cba…`: the focused companion selection, 48 low-level JSONL regressions, 16 focused FolderDB regressions, `py_compile`, and `git diff --check`. The claimed "17 companion tests" reconciles exactly against the files (8 in `test_jsonlfile.py` including a 4-way parametrize; 9 in `test_folderdb.py` including a 3-way parametrize). No dependency was added or upgraded — `tempfile` and `shutil` are stdlib — so the registry/lockfile evidence rule does not apply.

**Disclosed deviation, non-blocking.** No interpreter carried all declared test deps together, so DataFrame-focused `FolderDB` tests and one unrelated NumPy assertion were not run. This is recorded in both `progress.json` and `outcome.md` rather than concealed, the contract permits focused testing only, and the uncovered DataFrame adapters route through low-level functions that were exercised.

**Non-blocking observations (no action required).** Cross-device `shutil.move` of the companion is not atomic, so a partial companion is theoretically possible at the target — explicitly reserved by the contract's risk section and restated in `outcome.md`; owner content is unchanged in that path. `_invalidate_aux_if_jsonl` silently skips `bytes` paths, consistent with the library's existing str-path assumption.
