# Proposal: Performance Optimization

JSONLDB has several performance bottlenecks that become significant at scale (millions of lines per file, hundreds of files). The core issues are: inconsistent use of orjson (reads use stdlib json), suboptimal O(n) Numba string scans for range selection, bloated indented index files, memory-hungry linting that loads entire files, and redundant filesystem calls in FolderDB initialization and path resolution.

This refactor targets `jsonlfile.py`, `jsonldf.py`, and `folderdb.py` with surgical optimizations: unify on orjson for all JSON parsing/serialization, replace Numba string operations with bisect-based O(log n) range selection, compact `.idx` files, introduce stream-based linting with a smart fast/slow path (line-count check to detect orphans), batch `db.meta` updates during lint, and eliminate redundant I/O in FolderDB. All changes are internal — no public API or `.jsonl` format changes.
