## Round 1

- [F1.1] **Addressed.** Added explicit try/except around `orjson.loads()` on the fast path. On `orjson.JSONDecodeError` or `OSError`, falls back to `build_jsonl_index()` and reloads — lint remains self-healing. See Design section 1, "Failure path" paragraph.
- [F1.2] **Addressed.** Design now explicitly states the mtime gate runs **after** `ensure_index_exists()`. The code example shows the ordering: Step 1 is `ensure_index_exists()`, Step 2 is the mtime gate. Added explanatory sentence: "This ordering is intentional: index existence and freshness are normalized first, then the scan-skip decision is made against a known-good index file."
- [F1.3] **Addressed.** Added new "Correctness Guarantees" section documenting exact coverage for `force=False` (structural lint: spot check, sort, compaction) vs `force=True` (exhaustive: adds line/index cardinality verification via mmap scan). Explicitly states what the fast path does NOT check and why that's acceptable.
