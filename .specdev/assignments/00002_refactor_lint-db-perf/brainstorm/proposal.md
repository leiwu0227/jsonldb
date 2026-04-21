# Proposal: Optimize lint_db Performance

`lint_jsonl()` performs an O(file_size) mmap line-count scan on every call, even when the file was just written with a fresh, synchronized index. For large files (92MB+), this dominates execution time — measured at 409s for a single file in production workloads.

This refactor adds an mtime-based fast path to `lint_jsonl()` that skips the expensive scan when the index is fresh, and exposes a `force` parameter on both `lint_jsonl()` and `FolderDB.lint_db()` so callers can explicitly request full verification when needed (crash recovery, manual maintenance). The result: near-instant linting for the common post-save case while preserving full verification as an opt-in path.
