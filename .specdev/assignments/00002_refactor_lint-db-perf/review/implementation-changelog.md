## Round 1

- [F1.1] **Disputed — this is by design, not a bug.** The design document explicitly states the mtime gate runs after `ensure_index_exists()`. When `ensure_index_exists()` rebuilds a stale index, the rebuilt index is correct — the mmap scan after a fresh rebuild would be redundant. No code change needed.
- [F1.2] **Acknowledged, no change.** Tests verify correct outcomes, not internal branch routing.

## Round 2

- [F2.1] **Addressed with clarification.** Updated the design's Correctness Guarantees section to explicitly document the `ensure_index_exists()` → mtime gate interaction: "If the index is stale, it is rebuilt before the mtime gate is evaluated. After rebuild, the index is fresh and correct, so the mtime gate routes to the fast path. This is intentional — a freshly rebuilt index is already verified against the file, making the mmap cardinality scan redundant." Also renamed `test_lint_stale_index_triggers_full_scan` to `test_lint_stale_index_recovers` with updated docstring and added data integrity assertion, removing the misleading claim about triggering the full scan.
- [F2.2] **Addressed.** Renamed test removes the misleading "triggers full scan" claim. The test now accurately describes what it verifies: that lint handles stale indexes gracefully and preserves data integrity.

## Round 3

- [F3.1] **Addressed.** Replaced `test_lint_force_runs_full_scan` with two diagnostic tests: `test_lint_force_detects_orphan_lines` (force=True detects orphaned lines via mmap cardinality scan, rebuilds index to include them) and `test_lint_default_compacts_orphan_lines` (force=False compacts orphans away via sort/compaction). The behavioral difference between the two paths is now observable in test assertions.
- [F3.2] **Addressed.** Rewrote `test_lint_db_force_parameter` to verify force pass-through semantics: appends an orphaned line, touches index to appear fresh, then calls `lint_db(force=True)` and asserts the orphan is present in loaded data — proving `lint_db` forwarded `force=True` to `lint_jsonl` which ran the mmap scan.

## Round 4

- [F4.1] **Addressed.** Wrapped the spot-check loop in `_verify_and_compact()` with `try/except (orjson.JSONDecodeError, ValueError, OSError)`. On any parse or seek failure (including bad byte offsets), falls back to `build_jsonl_index()` and reloads. Added `test_lint_bad_index_offsets_recovery` that writes an index with valid JSON but wrong offsets (`{"a":1,"b":999999}`) and verifies lint recovers. 69 tests pass.

## Round 5

- [F5.1] **Addressed.** Added `TypeError` to the except clause in `_verify_and_compact()` spot check. Now catches `(orjson.JSONDecodeError, ValueError, TypeError, OSError)`. Added `test_lint_non_integer_index_offsets_recovery` test that writes `{"key1":"not-an-int","key2":0}` as index and verifies lint recovers. 70 tests pass.
