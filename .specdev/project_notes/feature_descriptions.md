# Feature Descriptions

Running catalog of completed assignments. See `.specdev/_guides/task/validation_guide.md` (Gate 5) for update instructions.

---

## Features

*(Updated by feature assignments)*

---

## Architecture & Structure

### Performance Optimization (00001)
- **Assignment:** 00001_refactor_perf-optimization
- **Completed:** 2026-03-10
- **Description:** Unified JSON operations on orjson, replaced Numba with bisect for O(log n) range selection, compacted .idx files, introduced stream-based linting with skip path, batched metadata writes, eliminated redundant filesystem calls in FolderDB.
- **Key files:** `jsonldb/jsonlfile.py`, `jsonldb/folderdb.py`, `profile_test/benchmark.py`, `setup.py`

### lint_db Performance Optimization (00002)
- **Assignment:** 00002_refactor_lint-db-perf
- **Completed:** 2026-04-19
- **Description:** Added mtime-based fast path to `lint_jsonl()` that skips O(file_size) mmap scan when index is fresh. Added `force` parameter to both `lint_jsonl()` and `FolderDB.lint_db()` for explicit full verification. Improved fault tolerance with guarded spot checks.
- **Key files:** `jsonldb/jsonlfile.py`, `jsonldb/folderdb.py`, `unit_tests/test_jsonlfile.py`, `unit_tests/test_folderdb.py`

---

## System Documentation

*(Updated by familiarization assignments)*
