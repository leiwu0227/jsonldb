# Codex Review: Test Names Must Match Actual Behavior

When Codex reviews tests, it takes test names literally. A test named `test_lint_stale_index_triggers_full_scan` will be flagged if the implementation doesn't actually trigger the full scan path — even if the behavior is correct and by design.

**Lesson:** Name tests after observable outcomes, not internal implementation paths. `test_lint_stale_index_recovers` is better than `test_lint_stale_index_triggers_full_scan` when the test only verifies recovery, not which code path executed.

**Lesson:** When disputing a Codex finding, fix the misleading artifact (test name, doc wording) rather than only arguing the behavior is correct. Codex will re-raise the same finding if the artifact still contradicts the code.
