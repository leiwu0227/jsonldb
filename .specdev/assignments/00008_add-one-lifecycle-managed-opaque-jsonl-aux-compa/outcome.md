# Outcome

## Delivered behavior

JSONLDB now exposes one fixed opaque companion slot at
`<ticker>.jsonl.aux` through low-level JSONL helpers and `FolderDB` methods.
Payload bytes round-trip unchanged, replacement uses a flushed same-directory
temporary file plus atomic `os.replace`, and publication requires an existing
owner. Ticker discovery remains limited to `.jsonl` owners.

Every managed content-changing path invalidates the companion before data can
diverge. Delete, clear, Git revert, hierarchy reorganization, and
invalid-ticker repair now remove or move companions with interruption-safe
ordering. Lint and repair report orphan companions without reading, deleting,
attaching, or synthesizing their payloads.

## Deviations

No contract or product-scope deviations. The available interpreters split the
declared dependencies, so verification used Python 3.9's real `orjson` with
ignored import-only pandas/GitPython shims. The unrelated NumPy assertion and
DataFrame-focused FolderDB tests were not run; low-level functions shared by
the DataFrame adapter and focused FolderDB dictionary/metadata regressions
passed.

## Unresolved risks

No unresolved blocker. As reserved by the contract, owner and companion moves
cannot be jointly atomic; interruption can leave the companion absent or
orphaned at its source, but cannot leave stale content attached at the target.

| Acceptance | Evidence | Result |
|---|---|---|
| AC-1 | 17 companion tests cover canonical paths, byte opacity, owner/payload validation, atomic publish failure, and discovery exclusion; 48 low-level JSONL regressions passed. | Passed |
| AC-2 | Focused tests cover save/update/delete/lint invalidation, owner deletion and clearing, hierarchy and invalid-ticker movement, simulated interrupted movement, and pre-revert invalidation; 16 FolderDB regressions passed. | Passed |
| AC-3 | Direct lint, database lint, hierarchy repair, and invalid-ticker repair report and preserve orphans; low-level index/lint and FolderDB metadata/self-heal regressions passed. | Passed |
