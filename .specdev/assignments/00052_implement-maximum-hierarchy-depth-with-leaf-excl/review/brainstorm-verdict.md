---
verdict: approved
material_divergence: false
scope_divergence: none
procedure_divergence: none
evidence_integrity: complete
user_reapproval_required: false
---

## Findings

No blocking findings. The candidate contract is byte-identical to the frozen baseline (`diff` clean), so no divergence classification applies.

Verification performed (narrow, read-only; no suite run):
- Design alignment: the mapping in `## Expected behavior` (`a` → `a.jsonl`, `a.b.c` → `a/b/a.b.c.jsonl`, `a.b.c.d.e.f.g.h` → `a/b/c/d/e/f/…`) matches `min(m, n-1)` in `.specdev/project_notes/roadmap/designs/folder_database/hierarchical_layout.md:9-17`. All three cited design notes exist.
- Open questions the design deferred are settled by the contract rather than left open: fallback policy (deepest observed depth → flat), migration trigger (reconcile on open even when depth is omitted or equal), and interrupted-migration behavior (retryable, settings published last). The "deepest observed" rule deliberately replaces the current `min(depths)` at `jsonldb/folderdb.py:204`, consistent with the design's statement that shallowest depth does not determine the maximum.
- "Existing recovery convention" for delimiter inference accurately describes `jsonldb/folderdb.py:217-236` (shortest separator reproducing every prefix, contradictory prefixes rejected, dot default). "Explicit reprocess operation" maps to `reprocess_invalid_tickers` (`folderdb.py:1124`).
- AC-5 is actionable: `docs/api.md`, `docs/usage.md`, and `examples/02_portable_datasets.ipynb` document `hierarchy_depth`.
- Verification authority is consistent with repository instructions; the full suite is correctly reserved to the user.

Non-blocking observations for the user (no change requested):
1. `.specdev/project_notes/big_picture.md:18` still shows the pre-change example `A.B.ticker.jsonl` → `A/B/ticker.jsonl`, which contradicts the new filename rule. The contract correctly denies it authority but does not authorize correcting it, and non-goals only exclude roadmap edits. Since big_picture is user-owned durable context, leaving it stale is defensible; a follow-up publication may be wanted.
2. `jsonldb/folderdb.py` is at 1209 of the published 1250-line cap — roughly 41 lines of headroom for migration, depth inference, and restoration work. The contract already anticipates this ("bounded refactoring if needed") and reserves raising the cap to the user, so the constraint is sound but will bind early during implementation.
