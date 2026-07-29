# Implementation Changelog

## Round 1 response

### F1.1 — regression tests not in the task commits

**Resolution: repo convention, not an omission — tests exist, pass, and live where this
repository deliberately keeps them.**

- All five required failing-first tests were written and are on disk in
  `unit_tests/test_jsonlfile.py` (`test_update_append_heals_missing_trailing_newline`,
  `test_update_shrink_keeps_line_terminator`) and `unit_tests/test_folderdb.py`
  (`test_delete_file_range_datetime_bounds`, `test_clear_folder_clears_hierarchy_subfolders`,
  `test_delete_file_tolerates_missing_index`). Each was run RED first (failing for the bug's
  exact signature) and then GREEN after the fix; `python -m pytest unit_tests/ -q` = 75 passed.
- `unit_tests/` is untracked **by explicit project-owner decision**: commit `7d9c16c`
  ("chore: untrack local tooling, agent configs, and generated artifacts", 2026-04-21) removed
  `unit_tests/` from the index and added it to `.gitignore`, with the stated intent "Files
  remain on disk." Every test added by prior assignments (e.g. 00002's diagnostic force tests)
  lives under the same convention.
- The plan's task files (`unit_tests/test_jsonlfile.py`, `unit_tests/test_folderdb.py`) were
  modified exactly as specified; the plan does not require changing the repository's tracking
  policy, and force-adding ignored files would silently reverse a deliberate, documented
  owner decision — out of scope for this bugfix assignment.
- The concern that fixes "travel" without their regression tests is acknowledged and has been
  flagged to the project owner in the assignment summary as a follow-up decision
  (track `unit_tests/` again vs. keep local-only).

No code changes required for this round; all five fixes and their tests are unchanged and green.
