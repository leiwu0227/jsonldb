# Workflow Diff — lint_db Performance Optimization
**Date:** 2026-04-19  |  **Assignment:** 00002_refactor_lint-db-perf

## What Worked
- Starting from a detailed performance analysis note (`project_notes/lint_db_performance.md`) gave clear scope and metrics from the start — brainstorm was fast because the problem was already well-understood.
- Extracting `_verify_and_compact()` as Task 1 before adding the fast path made Task 2 clean — shared logic was already in one place.
- Using orphaned-line tests to prove `force=True` vs `force=False` behavioral differences was effective — Codex accepted these after rejecting non-diagnostic tests.

## What Didn't
- Codex reviewloop spent 3 rounds on the stale-index interaction (`ensure_index_exists()` rebuilds before mtime gate). The design was correct but the test name (`test_lint_stale_index_triggers_full_scan`) was misleading, which confused the reviewer. Lesson: test names must match actual behavior, not intended conceptual behavior.
- Codex escalated edge cases (bad index offsets, non-integer values) that were pre-existing issues not introduced by this refactor. The fixes were valuable but added 2 extra rounds. Consider noting pre-existing vs new scope explicitly in review responses to reduce round count.
- Max rounds (5) were exhausted without final approval despite all findings being addressed. The last finding (TypeError on non-integer offsets) was a valid but very edge-case concern.
