---
verdict: approved
material_divergence: false
scope_divergence: none
procedure_divergence: none
evidence_integrity: complete
user_reapproval_required: false
---

## Findings

**Evidence integrity — complete.** All four artifact digests recomputed from disk match the candidate receipt exactly (contract `4a36872b…`, plan `1a07c277…`, progress `2d67d640…`, outcome `8ed9bf58…`), and the receipt identity matches `review/implementation-state.json`. The five recorded source hashes in `implementation/source-hashes.json` also match the working tree byte-for-byte, so nothing changed since the freeze at `working-tree@58bdf0d`. I reused the four existing receipts and added one narrow authorized confirmation: `unit_tests/test_write_statistics.py` reproduces 49 passing cases in 0.40s, consistent with the receipt's "49 new cases." No full suite was run.

**Scope — none.** Changed project paths are exactly the three in-scope modules (`jsonldb/jsonlfile.py`, `jsonldb/jsonldf.py`, `jsonldb/folderdb.py`), the new focused test, the shipped benchmark helper, and a `.gitignore` negation for that helper — which the contract's verification authority explicitly requires ("profiling ignore rules must not omit them"). `git check-ignore` confirms the helper is not ignored, so documented benchmark commands work from a fresh checkout. No new dependency, module, setting, disk format, or design edit; no registry/lockfile evidence requirement is triggered.

**Contract conformance verified by inspection, not just receipts:**

- *AC-1 (correctness of derived statistics).* The contract's named risk is that the in-memory write index is unsorted while its published form is sorted. `_index_stats` correctly uses `min(index, default=None)`/`max(index, default=None)` rather than first/last insertion key. `_serialize_index` uses `orjson.OPT_SORT_KEYS`, and UTF-8 byte order coincides with Python's code-point order across the full scalar range, so the write-time bounds provably equal the disk fallback's `keys[0]`/`keys[-1]`. `size` is read via `getsize` after `_write_index`, and metadata slots are excluded because only row entries enter `index`. `assert_disk_metadata` recomputes bounds independently with `sorted(index)` rather than trusting file order — a genuinely independent oracle. Key order in the `**stats` spread preserves the original `min_index, max_index, size, count` literal order, so `db.meta` bytes are unchanged, consistent with the 40 byte-identical snapshots.
- *AC-2 (failure boundaries).* `_validate_row_keys` and the DataFrame uniqueness check still run before any write; `_df_records` preserves check-before-convert ordering. Statistics are returned only after the `try` block completes, so row/index failures cannot publish them. `test_failure_boundaries` covers all three injection points plus cache invalidation and post-metadata-failure row readability; plural partial-failure ordering and corrupt-index refresh/rebuild/lint fallback are covered.
- *AC-3 (no post-publication reload, demonstrated gain).* All six FolderDB write entry points now pass `stats` into `_update_dbmeta`, so `load_index` is not reached; public `save_jsonl`/`update_jsonl`/`save_jsonldf`/`update_jsonldf` retain their signatures and `None` returns and never compute the summary. `performance.md` reports dict/DataFrame, overwrite/create, empty/small and low-level costs separately with summary-derivation overhead isolated, and shows 1.492x (dict) and 1.539x (DataFrame) for one-row upserts into a 100,000-row table. Line caps hold at 950/950, 134/150, 1188/1250.

**Non-blocking observations** (no change requested):

1. `jsonldb/jsonlfile.py:571` selects buffering with `BUFFER_SIZE if db_dict or width else -1`. If `width == 0` were ever reachable with an empty dict, the buffer size would differ from the pre-change path. This affects only buffer sizing, never file contents or the published index, and the empty-legacy default-buffer regression it was written to fix is documented in `performance.md`.
2. `test_no_post_publication_table_reads` guards `builtins.open`, so a hypothetically cache-served `load_index` would evade it. The production path demonstrably supplies `stats` and never reaches the fallback, which I confirmed by reading `_update_dbmeta`/`_make_meta_entry` directly, so the guard's narrowness does not weaken the claim.
3. The disclosed +1.5–1.7% on empty and 10-row low-level overwrite is roughly 2 microseconds and inside the stated sub-millisecond noise floor. The 10-row dict-upsert anomaly (0.825x) was retained rather than omitted and did not reproduce under a denser 201-sample follow-up (1.089x). This is not a material unresolved regression, and the intended benefit reserved to the user was demonstrated, so no user reapproval is required.

Every acceptance criterion has a final result, all four authoritative verification receipts passed with no superseded or omitted entries, and no blocking contract defect remains.