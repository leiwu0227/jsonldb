# Todo

Findings from the 2026-09-04 code review of `master` (package code at 548ada1). Items are ordered by suggested priority, not severity alone. Each item is Adhoc-sized unless marked otherwise. Nothing here is authorized work; pick items explicitly.

## 1. Make the test suite runnable from a clean clone

- `unit_tests/` and `profile_test/` are gitignored, so a clone of `master` has no tests and CI is impossible.
- The local `test_jsonlfile.py` and `test_folderdb.py` import `get_aux_path`, `write_aux`, and friends, which exist only on branch `post-fc22cba`. Both modules fail at collection on `master`.
- No interpreter on this machine has `orjson` or `pandas`; there is no `pyproject.toml` or lockfile. `.prodinclude` lists `pyproject.toml` but the file does not exist.
- Action: track the test folders, restore `master`-compatible test modules, add `pyproject.toml` with a locked dev environment. Larger than Adhoc if a packaging migration is included.

## 2. Lint does not reclaim interior dead space

- Deleting a middle record, or a growing upsert of the last record, leaves a tombstone that lint never compacts, on both the fast and `force` paths.
- Already forecast as item 1 in `forecast.md` against `designs/jsonl_file_store/index_integrity_and_lint.md`. Fix per that note: strictly-increasing offsets and a newline-count check. Add tests for middle-key delete and last-record growth.

## 3. Non-string keys are ordered as text

- Integer keys serialize through `str`, so a range select of 1 to 3 over keys 1 to 11 returns 1, 10, 11, 2, 3. The README quick start uses integer ids.
- Decide the policy: reject non-string, non-datetime keys at save time, or document zero-padding prominently. A rejection is a public-contract change and belongs in an Assignment.

## 4. Route warnings through `logging` or `warnings`, not `print`

- Every "WARNING:" in `jsonlfile.py` and `folderdb.py` goes to standard output. Downstream consumers cannot capture, filter, or alert on them. This contributed to a silent data-loss incident recorded in `project_notes/thoughts/20260612_corrupt_idx_silent_save_failure.md`.

## 5. Metadata upkeep is inconsistent

- `delete_file` and `delete_file_range` do not refresh `db.meta`; `delete_file_keys` does.
- The open-time staleness check compares the root folder mtime only, so changes inside hierarchy subfolders are missed.
- `lint_db` iterates the names in `db.meta`, so a table present on disk but absent from `db.meta` is never linted.

## 6. README documents an API that does not exist

- `store`, `query`, `update`, `delete`, `delete_table`, `get_metadata`, `update_metadata`, and `db.visualize()` are not methods of `FolderDB`. Rewrite the examples around `overwrite_*`, `upsert_*`, `get_dict`, `get_df`, `delete_file*`, `commit`, `revert`, `version`, and `jsonldb.visual`.
- README says `bokeh >= 3.0.0`; `setup.py` says `>= 2.0.0`.

## 7. Package hygiene

- `jsonldb/folderdb.py.tmp` is a stale pre-refactor copy inside the package. Delete it.
- Unused imports: `pandas` in `jsonlfile.py`, `lint_jsonl` in `vercontrol.py`, `Category10` in `visual.py`.
- Typos and naming: `hierachy_info`, docstring says `hierarchy_level` for the `hierarchy_depth` argument, `select_line_jsonl` calls its flag `auto_serialize` although it also controls deserialization.
- Formatting drift in signatures, for example `folder_path: str,hierarchy_depth`.

## 8. Visualization edge cases

- The single-file Bokeh plot indexes the first key without checking for an empty index and will raise on an empty table.
- The vertical axis is labelled "Line Number" but plots byte offsets.
- The start and end filtering loop is duplicated between the two Matplotlib functions.

## 9. Surprising side effects

- Constructing a `FolderDB` with a `hierarchy_depth` that differs from `h.meta` reorganizes the tree on open. Consider a separate explicit call.
- `revert` is `git reset --hard`; later commits leave the branch. `revert` also catches a bad name after `rev_parse` has already raised, so that branch is dead code.
- These are behaviour changes and belong in an Assignment, not Adhoc.

## 10. Durability is best-effort

- Saves, upserts, and deletes write in place with the index written afterwards. A crash mid-line becomes an "invalid JSON line" warning and a lost record. Acceptable for a single-owner cache; document it, or adopt temp-file-and-replace for saves if the library is a system of record. Design decision first, then an Assignment.
