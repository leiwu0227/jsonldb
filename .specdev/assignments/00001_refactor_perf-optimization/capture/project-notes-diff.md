# Project Notes Diff — Performance Optimization
**Date:** 2026-03-10  |  **Assignment:** 00001_refactor_perf-optimization

## Gaps Found
- big_picture.md references "Numba JIT for sort/range-select hot paths" — Numba has been removed, replaced by stdlib bisect
- big_picture.md says "Serialization uses orjson for speed with json stdlib as fallback" — stdlib json is no longer used anywhere in jsonlfile.py or folderdb.py; orjson is the sole JSON library
- big_picture.md doesn't mention the stream-based linting approach or the skip path for already-clean files
- big_picture.md doesn't mention the benchmark script in profile_test/

## No Changes Needed
- Layered architecture description is accurate
- Index-driven reads description is accurate
- JSONL format conventions are accurate
- Hierarchical storage description is accurate
