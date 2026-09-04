---
verdict: approved
material_divergence: true
scope_divergence: material
procedure_divergence: disclosed
evidence_integrity: complete
user_reapproval_required: true
---

## Findings

No blocking findings. The contract is internally consistent, its authority split is concrete, the five-child decomposition is feasible against current code, and the single authorized final command is proportionate to the acceptance it certifies.

**Evidence integrity.** `review/brainstorm-baseline.md` hashes to `61e3fdbf…`, exactly the `baseline_hash` in `review/brainstorm-state.json`, so the frozen baseline is unaltered. The contract now hashes to `7e082430…` against the recorded `a9707a85…`, so this is a fresh round over an edited contract, not a re-read of previously approved text.

**Delta since the last approved round.** The caps were corrected to the published design values and extended to all seven governed modules (`metaslot.py` 250, `jsonlfile.py` 950, `reports.py` 150, `folderdb.py` 1250, `jsonldf.py` 150, `vercontrol.py` 160, `visual.py` 500); each is quoted verbatim from a design note (`metadata_slot.md:69-71`, `jsonl_file_store.md:59`, `integrity_logs.md:42-44`, `folder_database.md:63`, `dataframe_adapter.md:23`, `version_control.md:29`, `visualization.md:28`) and matches `todo.md:3`. A new constraint fixes the test tree (`tests/` tracked, `tests/legacy/` immutable, ignored `unit_tests/` out of scope), AC-1 adds the append-time terminal-newline heal, AC-3 swaps `.jsonl.aux` cleanup for malformed-envelope repair, AC-4 adds report-directory invisibility, AC-5 adds the Python 3.8 floor audit and per-child design-rule compliance, and the final command gains legacy-immutability, seven-file cap, and 3.8-parse clauses with a broadened print gate.

**Each changed criterion traces to a published rule.** AC-1's newline heal is `jsonl_file_store.md:39`; AC-3's malformed-`_meta` overwrite is `integrity_logs.md:20`; AC-4's hidden-directory invisibility is `integrity_logs.md:7`. Dropping "atomically" from AC-4's report write matches `integrity_logs.md:9-10`, which requires replacement of contents, not atomic publication — an alignment, not a weakening of the reserved atomicity guarantees, which remain scoped to indexes and the two control files. The `.jsonl.aux` removal tracks the user's own roadmap commits (`c7e1413`, `2838cc8`, `c54d134`), after which forecast item 9 no longer mentions the companion.

**Feasibility, checked against current code.** Headroom under the corrected caps: `jsonlfile.py` 706/950, `folderdb.py` 967/1250, `jsonldf.py` 116/150, `vercontrol.py` 128/160, `visual.py` 454/500. The correction loosens the previously binding constraint (`folderdb.py` gains 50 lines, `jsonlfile.py` 50) while adding two new modules that start empty. `folderdb.py`'s ~283 remaining lines against the metadata API, migration, lint extensions, capture hooks, and 20 print conversions is still the real exposure; the contract discloses it in Risks and delegates "concise source cleanup needed to satisfy the line caps," which is adequate authority.

**Final command, dry-checked clause-wise without running the suite.** `git diff --exit-code d5cd7b1… -- tests/legacy` resolves (that commit exists and contains the three legacy files) and exits 0 today. The cap clause evaluates cleanly for the five existing modules and would raise on the two not-yet-created ones, which at final verification correctly doubles as an existence gate for `metaslot.py` and `reports.py`. The combined parse/print clause parses all of `jsonldb/` under `feature_version=(3,8)` and fails today with 32 offenders (`folderdb.py` 20, `visual.py` 8, `vercontrol.py` 4), so the gate is live rather than vacuous, and it now also covers `builtins.print` and `sys.stdout/stderr.write`, closing most of the prior round's non-blocking gap. `tests` matches `pyproject.toml:6` `testpaths` and the seven tracked test files. Each clause maps to a stated criterion (AC-4 prints, AC-5 suite, caps, 3.8 floor, no discarded code) and nothing broader; children stay bound to narrower evidence plus the seven-file inventory.

**Authority and decomposition.** The five planned sequential children map one-to-one onto the Todo packages and cover all ten forecast gaps (items 2–5, 6–7, 8, 9, 1+10), with AC-5 as the integration gate. The reserved list still covers child scope and order, atomicity, publish order, repair, compatibility, cap weakening, the version-1 envelope and 4096-byte default, locking/fsync, the discarded `00008`/`post-fc22cba` inputs, and cross-repository expansion. The new test-tree constraint resolves a real repository/design mismatch — `source_code_folder_structure.md` prescribes `unit_tests/`, which `.gitignore:33` ignores — in favour of the tracked layout that `pyproject.toml` already targets, and states explicitly that no source target, dependency rule, or cap is waived.

**Divergence (informational, not a defect).** Material because constraints, three acceptance criteria, the verification authority, and the exact final command all changed from the frozen baseline. Scope is classified material because the deliverable set moved in both directions — `.jsonl.aux` cleanup removed, test-tree ownership and the 3.8 floor audit added — even though no work package was added or reordered and every change follows the currently published roadmap. Procedure is disclosed: the broadened final command, the per-child cap inventory, and the interpreter limitation are all stated in the contract text. `user_reapproval_required: true` reflects only that the approval gate should read the current text.

### Non-blocking

1. The constraints say "logical lines" while the gate counts physical lines via `splitlines()`; the adjacent rule "Count an unterminated final line" and the design notes' plain "at most N lines" both point at the physical count, so the operative definition is unambiguous. Children should run the contract's own expression for their inventory rather than a statement-based counter, or a child could pass its own audit and fail integration.

2. `git diff <commit> -- tests/legacy` compares tracked content only, so a *new untracked* file dropped into `tests/legacy/` would not trip the clause. The constraint text already forbids it ("add focused tests outside that subtree"); nothing under `tests/legacy` is untracked today.

3. `ast.parse(..., feature_version=(3,8))` is best-effort and catches no 3.9+ library-API usage; the available interpreter here is 3.14.3. The contract already discloses this and pushes the real floor audit onto child evidence — the reporting and durability children should carry that explicitly rather than lean on the gate.

4. The blanket "obey all applicable published Roadmap design rules and source targets" decision also reaches `source_code_folder_structure.md`'s "performance-sensitive changes add a scenario to `profile_test/benchmark.py`". The in-scope list and ACs do not require benchmark work, and `profile_test/` is untracked; the lint child should not read that line as an added deliverable.
