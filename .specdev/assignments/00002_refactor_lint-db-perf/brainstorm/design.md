# Design: Optimize lint_db Performance

## Overview

`lint_jsonl()` performs an O(file_size) mmap line-count scan on every call, even when the file was just written with a fresh, synchronized index. For large files (e.g., 92MB / 146K entries), this scan alone takes hundreds of seconds.

This refactor adds an mtime-based fast path to `lint_jsonl()`: if the `.idx` file is at least as recent as the `.jsonl` file, the expensive line-count scan is skipped in favor of a lightweight spot check. Additionally, `FolderDB.lint_db()` gains a `force` parameter so callers can explicitly request full verification when needed (e.g., crash recovery, manual maintenance).

Together these changes make `lint_jsonl()` near-instant for the common post-save case while preserving full verification as an opt-in path.

## Non-Goals

- **Removing `lint_db()` from downstream callers** — the call site in `oceanseed`'s `JsonlFolderStorager.save()` is out of scope for this repo. That's a separate change.
- **Changing lint_jsonl's correctness guarantees** — when `force=True` or mtime indicates staleness, the full scan runs exactly as today.
- **Optimizing the sort/compaction check or stream-lint rewrite** — those are already gated behind early-exit conditions and aren't the bottleneck.
- **Changing db.meta structure or semantics** — metadata continues to work as-is.

## Design

### 1. mtime-based skip in `lint_jsonl()` (jsonlfile.py)

The mtime gate is evaluated **after** `ensure_index_exists()` (which already handles missing/nonexistent indexes). This ordering is intentional: index existence and freshness are normalized first, then the scan-skip decision is made against a known-good index file.

```python
def lint_jsonl(jsonl_file_path, force=False):
    ...
    # Step 1: Ensure index exists (unchanged — runs first always)
    ensure_index_exists(jsonl_file_path)

    # Step 2: mtime gate (new — after index is guaranteed to exist)
    if not force:
        index_path = jsonl_file_path + ".idx"
        idx_mtime = os.path.getmtime(index_path)
        data_mtime = os.path.getmtime(jsonl_file_path)
        if idx_mtime >= data_mtime:
            # Index is fresh — skip line-count scan, load index for spot check
            try:
                with open(index_path, 'rb') as f:
                    index_dict = orjson.loads(f.read())
            except (orjson.JSONDecodeError, OSError):
                # Corrupt/truncated index — fall back to full rebuild
                build_jsonl_index(jsonl_file_path)
                with open(index_path, 'rb') as f:
                    index_dict = orjson.loads(f.read())
            # Jump to spot check + sort/compaction check (existing logic)
            ...

    # Step 3: Full mmap line-count scan (original path — runs when force=True or mtime stale)
    ...
```

**Failure path:** If the index file is corrupt or truncated on the fast path, `orjson.loads()` will raise. The handler catches this, rebuilds the index via `build_jsonl_index()`, and continues — lint remains self-healing.

**WSL2 caveat:** mtime resolution on `/mnt/` is ~1 second. A write-then-lint within the same second could see equal mtimes. This is safe — equal mtime triggers the fast path, which is correct because the index was just written alongside the data.

### 2. `force` parameter on `lint_jsonl()` (jsonlfile.py)

Add `force=False` parameter to `lint_jsonl()`:

```python
def lint_jsonl(jsonl_file_path, force=False):
```

When `force=True`, the mtime check is bypassed and the full mmap scan always runs. This is the escape hatch for crash recovery or manual maintenance.

### 3. `force` parameter on `FolderDB.lint_db()` (folderdb.py)

Add `force=False` parameter, passed through to each `lint_jsonl()` call:

```python
def lint_db(self, force=False) -> None:
    ...
    for name in metadata:
        exist_flag = lint_jsonl(file_path, force=force)
    ...
```

Default behavior becomes fast (mtime-skip). Callers requesting full verification pass `force=True`.

### 4. No changes to db.meta handling

`db.meta` is already maintained incrementally by `update_dbmeta()` after every write. The `lint_time`/`linted` fields update only when lint actually runs, which accurately reflects reality.

## Correctness Guarantees

### `force=False` (default) — structural lint

Note: `ensure_index_exists()` always runs first. If the index is stale (older than data file), it is rebuilt before the mtime gate is evaluated. After rebuild, the index is fresh and correct, so the mtime gate routes to the fast path. This is intentional — a freshly rebuilt index is already verified against the file, making the mmap cardinality scan redundant.

When the index is fresh (mtime gate passes), `lint_jsonl()` provides:
- **Index-file agreement (spot check):** Verifies first and last entries can be seeked and parsed, and their keys match the index. Catches offset corruption and key mismatches.
- **Sort order verification:** Confirms all index keys are in sorted order.
- **Compaction check:** Verifies no dead space (first entry at offset 0, last entry ends at EOF).
- **Stream-lint rewrite if needed:** If sort or compaction fails, rewrites the file in sorted order.

What it does **not** check:
- **Line/index cardinality:** Does not verify that every non-blank line in the file has a corresponding index entry. A file with orphaned lines (e.g., from a crash mid-write that added data but didn't update the index) would not be detected. This is acceptable because normal write operations always update the index synchronously.

### `force=True` — exhaustive lint

Full mmap line-count scan runs. Provides everything above **plus**:
- **Line/index cardinality verification:** Counts all non-blank lines via mmap and compares against index entry count. On mismatch, rebuilds the index from scratch.

This mode should be used for: crash recovery, manual maintenance, after external file edits, periodic integrity audits.

## Key Decisions

| Decision | Reasoning |
|----------|-----------|
| mtime comparison for freshness | Simple, no extra state needed; index is always written alongside data by save/upsert operations |
| `>=` comparison (not `>`) | Equal mtime means written in same second — index is still fresh |
| Default `force=False` | Optimizes the common case; explicit opt-in for full verification |
| Keep spot check on fast path | Catches index corruption (wrong offsets) without the O(n) scan cost |
| No db.meta changes | Already maintained incrementally; lint_time/linted fields are informational only |

## Success Criteria

1. **mtime fast path works:** When `.idx` mtime >= `.jsonl` mtime, `lint_jsonl()` skips the mmap line-count scan and completes in O(1) rather than O(file_size).
2. **Spot check still runs on fast path:** Even when the mmap scan is skipped, first/last entry spot checks and sort/compaction checks still execute.
3. **`force=True` bypasses mtime check:** Calling `lint_jsonl(path, force=True)` or `lint_db(force=True)` runs the full mmap scan regardless of mtime.
4. **Default behavior is backward-compatible:** `lint_jsonl(path)` and `lint_db()` still lint — they just skip the expensive scan when the index is fresh.
5. **Existing tests pass:** All `test_jsonlfile.py` and `test_folderdb.py` tests continue to pass.
6. **Benchmark confirms speedup:** On a file with a fresh index, `lint_jsonl()` completes significantly faster than before.

## Testing Approach

- Unit tests for `lint_jsonl()` with fresh index (mtime skip path)
- Unit tests for `lint_jsonl()` with `force=True` (full scan path)
- Unit tests for `lint_jsonl()` with stale index (mtime triggers full scan)
- Integration test for `lint_db(force=False)` vs `lint_db(force=True)`
- Existing test suite passes unchanged
