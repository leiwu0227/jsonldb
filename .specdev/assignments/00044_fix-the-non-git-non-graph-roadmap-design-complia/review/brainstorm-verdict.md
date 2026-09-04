---
verdict: approved
material_divergence: true
scope_divergence: material
procedure_divergence: disclosed
evidence_integrity: complete
user_reapproval_required: true
---

## Findings

No blocking findings. The contract is internally consistent, every factual premise it relies on is currently true, and every constraint it imposes is satisfiable from the present working tree.

**Premises re-verified (read-only, no suite run):**
- `.specdev/project_notes/roadmap/designs/source_code_folder_structure.md:14-17` names `unit_tests/` holding exactly `test_jsonlfile.py`, `test_jsonldf.py`, `test_folderdb.py`, so the relocation target and the three top-level filenames are taken verbatim from published authority, not invented.
- `548a174b:tests/legacy/` contains exactly those three names, and `tests/legacy/` at HEAD is byte-identical to that revision, so the `git show`-based byte comparison starts satisfiable.
- The local `unit_tests/` tree is gitignored (`.gitignore:33`) and holds only those same three discarded Assignment 00008 filenames plus `__pycache__` — the name collision the contract must overwrite is real and bounded.
- `jsonldb/jsonlfile.py` is exactly 950 lines (at cap, so the line-neutral requirement is correctly stated); all other capped modules are below cap.
- All twelve design notes are ≤ 799 words (max 797) and are byte-identical to `548a174b` with no untracked additions, so "byte-for-byte unchanged **and** below 800 words" is not self-contradictory and both immutability checks currently pass.
- `pyproject.toml:6` `testpaths = ["tests"]` is the only reference to the suite path outside `.specdev/` and `tests/` itself; there is no CI workflow and `.prodinclude`/`README.md` do not name it, so the relocation has no undeclared fan-out.

**Round-1 findings discharged by this revision:** the irreversibility wording now states plainly that the discarded files have no Git recovery source and that the relocated suite "must not be represented as their backup"; scope now requires removing the old `tests/` tree "including ignored generated residue", which closes the `test ! -e tests` false-negative path through `tests/__pycache__/`; scope now fixes the three designed names as exact top-level matches that supplements may not replace; and the final command adds `git status --porcelain --untracked-files=all -- .../designs`, closing the added-untracked-note hole in the diff check.

**Material divergence from the frozen baseline (informational, for the approval gate):** the test-layout decision is inverted. The baseline kept tracked `tests/` canonical and edited the structure design note to name it; the current contract treats the published design notes as immutable authority and relocates the maintained suite into `unit_tests/`. This materially changes scope (relocation and ignore-rule removal instead of a documentation edit), constraints (new byte-for-byte design-note immutability; three named top-level legacy files instead of a preserved `tests/legacy/` directory), reserved authority (the suite may no longer move away from `unit_tests/`, and no design note may be edited), and the meaning of AC-3/AC-4. The final integrated command changed correspondingly. The reversal is disclosed in the contract's own Important decisions and is the sounder reading of published authority — it is flagged for user re-approval, not as a defect.

**Materially useful, non-blocking:**
1. `pyproject.toml:6` is in scope ("point pytest at it") but the authorized final command passes `unit_tests` explicitly, which overrides `testpaths` and would therefore pass even if the config still said `["tests"]`. A stale value would leave a bare `python3 -m pytest` erroring on a nonexistent testpath. Updating `testpaths` is delegated already; the gap is only that the command does not prove it.
2. Residual design gap after relocation. `source_code_folder_structure.md:49` says each core module keeps a matching `unit_tests/test_<module>.py`; after the move there is still no `test_metaslot.py`, `test_vercontrol.py`, or `test_visual.py`. This is consistent with the contract — it fixes only the three names in the design's structure block and forbids Forecast from claiming full compliance while out-of-scope gaps remain — so recording this residue in `forecast.md` is the natural discharge under AC-4.
3. The Forecast/Todo "below 200 words per numbered section" invariant is asserted in Constraints but not checked by the final command, unlike every other cap. Manual confirmation at outcome time is enough; noted so the guarantee is not over-read.

**Execution risk already acknowledged:** `jsonlfile.py` must absorb shared row-shape validation across writers, readers, index rebuild, selection, and lint, plus the malformed-envelope repair, while staying at or below its exact current 950 lines. The contract names this and prescribes a single shared check, which is the right mitigation; it remains the tightest part of the work.
