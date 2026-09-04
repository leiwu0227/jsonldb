---
verdict: approved
material_divergence: false
scope_divergence: none
procedure_divergence: none
evidence_integrity: complete
user_reapproval_required: false
---

## Findings

Evidence integrity: the candidate contract at `.specdev/assignments/00039_make-standalone-lint-converge-when-a-metadata-on/brainstorm/contract.md` is byte-identical to the frozen baseline at `review/brainstorm-baseline.md` (3570 bytes, `diff -q` clean). The cited Mission hash `7e0824300f897190603ccb138e56aaf5254389c0b40f69967908352465ceac4f` matches the current SHA-256 of the approved Mission contract. `status.json` (kind `bugfix`, mission `M00001`, run `mission-lifecycle-20260904T09260745`) agrees with queue entry `00039` in `design/assignments.yaml` (wave 6, `running`, `gap_id: gap-f4323203e2f7d1e0`, `gap_stage: resolution`), and that gap record in `mission.yaml` names `00037` as source with artifact `.specdev/assignments/00037_.../outcome.md`, matching the contract's Objective.

Authority and acceptance stay wholly inside the parent. Repairing terminal newlines while preserving valid slot records is already inside Mission AC-3; child AC-1 narrows it to one observable case (standalone `lint_jsonl`, recognized version-1 metadata-only slot) and adds no obligation the Mission lacks. The "Important decisions" bar on inventing a slot for a legacy file or inferring a folder width keeps the child clear of Mission-reserved slot-format and width policy; reserved authority is inherited verbatim. Verification authority correctly leaves the full tracked suite to the Mission's final integrated verification while retaining the two Mission-mandated child audits (Python 3.8 syntax/API floor, seven-file logical-line inventory), and it forbids evidence-only completion. The defect is real and matches the contract's framing: at `jsonldb/jsonlfile.py:307-308` standalone lint sets `desired_slot = info.raw_line`, so an unterminated slot line makes the canonicality test at `jsonldb/jsonlfile.py:340-341` (`newlines == len(index) + 1`) fail on every pass and `_lint_rewrite` re-emits identical bytes forever. No blocking finding.

Non-blocking, materially useful:

1. `jsonldb/jsonlfile.py` is currently at exactly 950 physical lines against its 950-line Mission cap (`metaslot.py` 101/250, `folderdb.py` 1121/1250). The contract's risk wording ("little line budget") understates this: headroom is zero, so the repair must be net-zero lines in that module or pay for itself by consolidation. An in-place edit at `jsonlfile.py:308` satisfies this; any added helper line breaks the Mission's own final-verification cap assertion.

2. The child scope and AC-1 are worded around "a recognized version-1 metadata-only slot", but `metaslot.classify_line` returns `is_slot=True` for torn envelopes (`_looks_like_torn_slot`) and for unknown-version records, and `jsonlfile.py:307-308` keys only off `info.is_slot`. If the implementation gates normalization on version-1 recognition, an unterminated torn or non-version-1 metadata-only slot keeps the identical non-convergent rewrite loop, and the same gap class can resurface. Keying the terminator normalization off `is_slot` (line lacks `\n`) costs nothing extra and closes the sibling states without inventing a slot for a legacy file; if the narrow gate is kept deliberately, the residual should be stated in the outcome rather than left implicit.

3. Implementation trap for the AC-1 "no further layout-repair observation" clause: `_lint_rewrite` only credits the old slot region as kept when `slot == info.raw_line` (`jsonlfile.py:278`). A newline-normalized slot fails that equality, so the first repair pass will report the preserved metadata line as removed bytes via `_lint_removed`/`_log_lint_removed` even though the record is preserved. The second pass is clean, so convergence evidence still holds, but the first-pass removal diagnostic should be checked deliberately against the Mission's metadata-record-preservation and bounded removed-byte requirements rather than assumed benign.
