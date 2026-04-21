# Project Notes Diff — lint_db Performance Optimization
**Date:** 2026-04-19  |  **Assignment:** 00002_refactor_lint-db-perf

## Gaps Found
- `big_picture.md` describes `lint_jsonl` as using "line-count check + spot verification" but doesn't mention the new mtime-based fast path or the `force` parameter. Should note that `lint_jsonl` now has a fast path that skips the mmap scan when the index is fresh, and a `force` parameter for exhaustive verification.
- `big_picture.md` doesn't mention `FolderDB.lint_db()` at all — it's a key public method that orchestrates linting across all tables. Should mention it has a `force` parameter.

## No Changes Needed
- Layer architecture description is accurate
- Index-driven reads description is correct
- Conventions section about orjson, buffer sizes, `.idx` format are all still current
