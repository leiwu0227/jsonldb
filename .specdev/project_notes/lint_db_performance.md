# lint_db Performance Analysis

## Problem

`lint_db()` is called after every `JsonlFolderStorager.save()` (line 144 of `oceanseed/storage/storager_types/jsonlfolder_storager.py`). For large datasets, this dominates save time.

**Measured on 30 signals x 10 years (146K actions, 241MB on disk):**

| Metric | With lint_db | Without lint_db | lint_db cost |
|---|---|---|---|
| action_sequence save | 2.4s | 1.3s | 1.1s (45%) |
| sim_action_chain save | 410.8s | 1.3s | 409.5s (100%) |
| **full vessel.save()** | **417.1s** | **5.2s** | **412.0s (99%)** |

Disabling `lint_db()` via mock patch reduced total save from **417s to 5s** (80x).

## What lint_db Does

`FolderDB.lint_db()` (folderdb.py:638) iterates all JSONL files and calls `lint_jsonl()` on each. Per file:

1. **Line count via mmap** (jsonlfile.py:138-143) — scans entire file byte-by-byte: `sum(1 for line in iter(mm.readline, b'') if line.strip())`. O(file_size).
2. **Index validation** — compares line count vs `.idx` entry count. Rebuilds index on mismatch.
3. **Spot check** — seeks to first/last entry, parses JSON, verifies key matches index.
4. **Sort + compaction check** — verifies keys are sorted and no dead space exists.
5. **Rewrite if needed** — reads file in sorted key order via random seeks, writes to temp, atomically replaces.
6. **Metadata rebuild** — writes `db.meta` with file stats (size, count, min/max index, lint_time).

### Why sim_action_chain Is the Worst

sim_action_chain stores 146K actions in a single JSONL file (~92MB). The mmap line-count scan alone takes hundreds of seconds on this file. Even when no rewrite is needed (file is already sorted), the scan still runs.

## Why lint_db Exists

It's a **safety net** for:
- File corruption from crashes mid-write
- Index desynchronization (`.idx` deleted or stale)
- Dead space accumulation from delete operations
- External file edits

## Why It's Not Needed After Save

Normal save operations maintain file integrity by construction:

- `save_jsonl()` writes a complete, correctly-ordered file with a matching index
- `update_jsonl()` maintains the index synchronously after every operation
- `overwrite_dicts()` clears and rewrites — the result is always clean

Calling `lint_db()` immediately after these operations re-verifies work that was just done correctly. It's redundant.

## What Breaks If We Skip It

**Nothing in normal operation.** The only risks are:

1. **Stale `db.meta`** — metadata (file sizes, counts, lint_time) won't be updated. This metadata is informational; no read/write operations depend on it.
2. **Dead space accumulation** — after many delete operations, deleted lines remain as blank space. Only cosmetic; reads still work via index offsets.
3. **Undetected external corruption** — if someone manually edits a JSONL file or the `.idx` is deleted, lint would have caught it. Without lint, the next read may fail with a seek error.

## Where lint_db Is Called

Single call site in the codebase:

```
oceanseed/storage/storager_types/jsonlfolder_storager.py:144
    self.folderdb.lint_db()
```

Called unconditionally at the end of `JsonlFolderStorager.save()`, after `overwrite_dicts()` / `upsert_dicts()`.

## Recommended Fix

### Option A: Remove lint_db from save() (simplest, biggest win)

Change `JsonlFolderStorager.save()` to not call `lint_db()`:

```python
# jsonlfolder_storager.py:144
# Remove or guard:
# self.folderdb.lint_db()
```

Users call `lint_db()` explicitly when needed (e.g., after crash recovery, periodic maintenance).

**Impact:** 417s → 5s. Zero risk for normal operations.

### Option B: Make lint_db opt-in via save param

Add a `lint` parameter to `JsonlFolderStorager.save()`:

```python
def save(self, data_dict, table_modes=None, lint=False):
    ...
    if lint:
        self.folderdb.lint_db()
```

Callers opt in when they want verification. Default is fast.

### Option C: Optimize lint_jsonl itself (smaller win, complementary)

Skip the expensive mmap line-count scan when the index is trustworthy:

```python
def lint_jsonl(jsonl_file_path):
    index_path = f"{jsonl_file_path}.idx"
    # If index is same age or newer than data file, trust it
    if os.path.exists(index_path):
        if os.path.getmtime(index_path) >= os.path.getmtime(jsonl_file_path):
            # Skip mmap scan, jump to sort/compact check only
            ...
```

This helps when lint IS called (e.g., on load or manual maintenance) but doesn't fix the "calling it unnecessarily" problem.

## Recommendation

**Do Option A or B first** (in oceanseed's storager layer), then optionally do Option C (in jsonldb) as a follow-up. The bottleneck is calling lint, not lint being slow — though it is also slow and could benefit from the mtime optimization.
