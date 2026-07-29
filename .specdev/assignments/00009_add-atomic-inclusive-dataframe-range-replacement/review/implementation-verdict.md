---
verdict: approved
material_divergence: false
---

## Findings

No blocking findings. The frozen candidate (dirty working tree at revision 5fff5db, matching every receipt's `working-tree@5fff5db…` revision) delivers the contract:

- **AC-1 (exact inclusive semantics + rejection safety):** `FolderDB.replace_df_range` (jsonldb/folderdb.py:415) validates DataFrame type, index uniqueness, and existing-owner presence, then delegates to `replace_jsonl_range` (jsonldb/jsonlfile.py:900), which serializes bounds with the owning timespec, rejects inverted bounds, serialization collisions, out-of-range keys, and invalid payloads/owners before any staging. Tracked parameterized tests cover middle, both edges, whole-range, empty-clear, insertion, and single-key correction, plus unchanged-artifact assertions for every rejected operation. Receipt: 26 focused tests passed (3.2s).
- **AC-2 (stage-first publication, `.aux` ordering, recovery):** The complete new owner and its index are staged and fsynced beside the destination before any visible change; `remove_aux` runs immediately before the atomic owner `os.replace`, then the index replaces. Failure injection at stage/sync/aux/owner/index/metadata boundaries confirms unchanged-old or complete-new owners, absent-not-stale `.aux`, and deterministic recovery. The mtime-bump generation tag (`publication_mtime = max(old owner, old idx) + 1s`) makes a leftover old index strictly stale so `ensure_index_exists` rebuilds it, and `get_dbmeta` (jsonldb/folderdb.py:711) repairs stale entries for both the current and a reopened `FolderDB` instance — both explicitly tested.
- **AC-3 (race-safe indexed reads + compatibility):** `select_jsonl` and `select_line_jsonl` route through `_read_with_index`, which verifies owner file identity (dev/ino/size/mtime_ns) around the index snapshot and re-validates each fetched line's key, retrying up to `INDEX_READ_RETRIES` with rebuild on mismatch and raising rather than returning partial data on exhaustion. Tracked tests force an old-index/new-owner race for both read kinds, plus hierarchy, lint, corrupt-index repair, and microsecond-timespec isolation. Receipt: 25 selected existing regressions passed (0.4s), plus `git diff --check`/`py_compile` clean.

No new external dependency is introduced (new imports are stdlib `tempfile`/`os` usage; `orjson` was already a dependency), so no registry or advisory evidence is required. Tests live at the tracked-eligible path `tests/test_atomic_replace_range.py` (untracked-new, not gitignored), satisfying the version-controlled-tests decision at the upcoming Git gate. `progress.json` records all tasks completed with no deviations, and `outcome.md` gives every acceptance criterion a final Passed result with matching evidence.

Non-blocking observations: `get_dbmeta` now rewrites `db.meta` on any detected drift, making a read path perform a repair write (within the delegated metadata-freshness authority, and it preserves `linted`/`lint_time`); the outcome correctly retains platform-dependent filesystem atomicity as an accepted residual risk.
