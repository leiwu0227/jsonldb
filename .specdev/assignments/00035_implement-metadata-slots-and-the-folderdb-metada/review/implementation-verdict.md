---
verdict: approved
material_divergence: false
scope_divergence: none
procedure_divergence: none
evidence_integrity: complete
user_reapproval_required: false
---

## Findings

No blocking findings.

**Evidence integrity (complete).** All four receipt digests recompute exactly on the working tree: contract `27da0c51…`, plan `55004fa8…`, progress `af4098c5…`, outcome `94894166…`. The receipt revision `working-tree@cf07c5d5e69662beeb579b73a92c514c8ef13ba9` matches `HEAD`, and the candidate's five project paths (`jsonldb/{folderdb,jsonldf,jsonlfile,metaslot}.py`, `tests/test_metadata_slots.py`) are present as the modified/untracked set. Acceptance is 2/2 with a final result and 0 omitted; verification is 2/2 passed, 0 skipped, 0 missing, 0 superseded, with one `authoritative_acceptance` run and one `qualification` run. I re-derived only the cheap, non-suite parts of the qualification claim: current line counts are metaslot 101/250, jsonlfile 882/950, folderdb 1024/1250, jsonldf 120/150, vercontrol 128/160, visual 454/500, `reports.py` absent — identical to the receipt's stated numbers, confirming the source has not drifted since evidence capture. No suite was run.

**Procedure (none).** The contract's verification authority is focused slot/`FolderDB` tests plus the parent-required Python 3.8 audit and seven-file inventory, with the full tracked suite reserved for Mission integration. The receipts contain exactly that and nothing broader. No external dependency is added or upgraded (`orjson` and `pandas` were already imported by `jsonlfile.py`/`jsonldf.py`; `metaslot.py` adds no new distribution), so no package-manager, registry, or lockfile evidence is required.

**Scope (none).** Correctness spot-checks against both acceptance criteria hold on inspection:

- AC-1: `metaslot.classify_line` (`jsonldb/metaslot.py:32`) classifies from file bytes alone — legacy rows, valid v1, torn envelopes via the `{"_meta"` prefix probe, extra-key, and unknown-version all land on the specified results, with `record` exposed only for a known version. `_validate_row_keys` (`jsonldb/jsonlfile.py:34`) rejects `_meta` before any mutation on `save_jsonl`, `save_jsonl_atomic`, `update_jsonl`, and `delete_jsonl`, so the DataFrame wrappers inherit the rejection. The bounded `FolderDB` surface is complete per plan T-2 (`meta=` on the four singular calls, `read_meta`, `get_dict_with_meta`, `get_df_with_meta`, `clear_meta`) and every missing-table path returns `TableWithMeta(None, {})` / `(None, DataFrame())` / `None` rather than raising.
- AC-2: reads are record-first (`folderdb.py` `get_dict_with_meta`/`get_df_with_meta` call `read_meta` before `select_*`). Writes are rows-first: `save_jsonl` reserves offsets with a null-record placeholder and republishes the real slot only after all rows are flushed; `update_jsonl` flushes rows, then writes the slot, then blanks superseded rows. Fit refusal happens strictly before mutation — `encode_slot` raises outside the `try`/open in `save_jsonl` and before `load_index` in `update_jsonl`. Offsets stay absolute: `byte_offset` starts at `width`, and both `build_jsonl_index` and `load_jsonl` skip only an offset-0 classified slot while still advancing past its bytes. `write_jsonl_meta` performs a same-width in-place overwrite and then `os.utime` on the `.idx` last, which is consistent with the `>`-based staleness check in `ensure_index_exists`.

Dropping `os.remove(file_path)` from `overwrite_df`/`overwrite_dict` is a real behavior change, but a necessary one: the slot width must survive a full overwrite, and `save_jsonl` opens `'wb'` (truncating), so row replacement semantics are unchanged. The legacy `FolderDB`/JSONL/DataFrame regressions in the authoritative run cover this path. Omitting `fsync` in `metaslot.write_slot` matches the repository's existing convention — there is no `fsync` call anywhere in `jsonldb/`.

**Non-blocking observations (no action required for this Assignment).**

1. `_verify_and_compact` (`jsonldb/jsonlfile.py:249`) counts the slot terminator in `newline_count`, so `newline_count == len(index_dict)` never holds for a slotted table; lint therefore always takes the compaction path and rewrites the file from index offsets only, discarding the slot. This is squarely inside the contract's declared non-goal ("lint repair … remain out of scope") and is disclosed in the receipt's unresolved risks as belonging to a later Mission child, so it is not a defect here — but it is the concrete failure mode that child must close.
2. Test coverage exercises slot preservation across a no-meta `upsert` (`tests/test_metadata_slots.py:102`) but not across a no-meta *overwrite*, which is the branch where `final_slot = existing_slot.raw_line` in `save_jsonl`. The behavior is implemented and no acceptance criterion demands the case; worth a line in a later child's tests.
3. `FolderDB` exposes no way to enable a slot (only `jsonlfile.save_jsonl(..., slot_bytes=…)` can), so the folder-level `meta=` arguments raise `ValueError("metadata slot is not enabled for this file")` on any table not already slotted. That is the contract's reserved "folder-wide width enablement" boundary working as specified, not a gap.
