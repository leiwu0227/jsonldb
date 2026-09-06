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

**Evidence integrity — complete.** All four artifact digests recomputed on disk match `review/candidate-receipt.json` byte-for-byte (contract `7d5a3609…`, plan `ced709ab…`, progress `2302d7b0…`, outcome `6bfae570…`). `git rev-parse HEAD` is `c2dbf85ab76a437bde66eeac8cd8bdb24188b502`, matching every receipt revision. The receipt's 10 `changed_project_paths` correspond exactly to the working tree (`docs/api.md`, `docs/file-format.md`, `docs/portability-and-git.md`, `docs/usage.md`, `examples/02_portable_datasets.ipynb`, `jsonldb/folderdb.py`, and the four `unit_tests/` modules including untracked `test_max_hierarchy.py`). No dependency, packaging, or lockfile change is present, so no registry/version evidence is required.

**Scope — none.** The only artifact touched beyond the receipt's project paths is `.specdev/project_notes/big_picture.md` (one bullet). Design plan T-3 authorizes it, `outcome.md` discloses it, and it is a project note rather than a roadmap file, so the contract's "no roadmap edits" non-goal is not engaged. The untracked `.specdev/discussions/` and `.ripplegraph/calls/D0000{3,4}` are independent Discussion state that the contract places outside ownership.

**Procedure — none.** The authoritative acceptance run covered 13 focused modules of the 30 present in `unit_tests/`, executed through the mandated `.specdev/cache/bin/python-runtime-env python3` wrapper; no full suite was run, consistent with the reserved-authority clause. The three `failed` receipts are all `role: qualification` (collection error, then obsolete fixed-depth expectations, then over-strict timestamp assertions), each superseded at the same revision by the 552-test `authoritative_acceptance` run that passed. Required implementation review is this review; `worker-result.md` correctly stops short of delivery pending it.

**Acceptance — 5 of 5 with final results, all passed.** Verified by targeted inspection rather than re-execution:

- AC-1: `_get_hierarchy_path` (`jsonldb/folderdb.py:457`) computes `name.split(delimiter)[:-1][:hierarchy_depth]`, reproducing the contract's mapping exactly — `a` → `a.jsonl`, `a.b.c` → `a/b/a.b.c.jsonl`, `a.b.c.d.e.f.g.h` → `a/b/c/d/e/f/a.b.c.d.e.f.g.h.jsonl` at maximum six. `unit_tests/test_max_hierarchy.py:33-35` asserts those same paths. All read paths use `_get_file_path` (no `create_folder`); only the four write entry points use `_get_or_create_file_path`, so missing-table reads create no directories.
- AC-2: `_open` (`folderdb.py:99-102`) reorganizes whenever `use_hierarchy` is set, including an omitted or unchanged depth argument, while flat mode with a `None` argument is skipped — matching "flat mode does not gain implicit hierarchy from dotted filenames". `_organize_hierarchy`'s `not moves and not changed and not force` short-circuit (`folderdb.py:285`) gives a stable second open.
- AC-3: `_recover_hierarchy` (`folderdb.py:209-240`) uses `max(depths)` for inference, falls back to flat when no nested tables and no explicit depth, intersects per-table separator candidates and raises `contradictory hierarchy prefixes` before any move, and logs `maximum=inferred (original unknown)`.
- AC-4: `_organize_hierarchy` preflights every source/destination through `_check_move_path` and a `destinations` set before writing the journal, and restores prior settings on any `BaseException`. The pending-journal ordering is sound: `_open`, `lint_hierarchy`, and `reprocess_invalid_tickers` each call `_resume_hierarchy()` before any path that writes a new `.hierarchy.pending`, so an interrupted move set can never be overwritten by a newer intent. No code path writes into `.invalid_tickers` any longer; restoration is explicit and refuses existing destinations.
- AC-5: `check_evidence.py` asserts the cap; `jsonldb/folderdb.py` is 1095 lines against the published 1250 limit. `docs/usage.md`, `api.md`, `file-format.md`, `portability-and-git.md`, and `big_picture.md` all describe maximum depth, final-segment exclusion, migration, recovery inference, `.hierarchy.pending`, and restore-only quarantine consistently with the contract.

**Non-blocking observations (no change requested).**

1. A legacy table with an empty delimiter segment (e.g. `a..b`) now fails `validate_name`, so `_organize_hierarchy` raises and the `FolderDB` constructor fails, where the prior behavior quarantined such a name. Data is preserved and the failure is reported, which satisfies "Report errors without claiming successful completion", but access degrades to a hard open failure. Such names have been rejected at creation since the preceding name-validation change, so this is reachable only for pre-existing databases.
2. `_organize_hierarchy` rejects any occupied destination even when that file is itself scheduled to move. This is conservative and data-preserving; because the full table name is retained in every filename, genuine swap cases are effectively unreachable under the new mapping.