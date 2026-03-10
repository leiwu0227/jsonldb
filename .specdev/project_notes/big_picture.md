# Project Big Picture

## Overview
JSONLDB is a file-based database library that stores data in JSONL (JSON Lines) format. It provides efficient key-based querying, hierarchical folder organization, built-in Git version control, and data visualization.

## Users / Consumers
Internal team use.

## Tech Stack
- **Language:** Python (>= 3.8)
- **Key dependencies:** pandas, orjson, numpy, gitpython, bokeh, matplotlib, mmap, bisect (stdlib)

## Architecture
- **Layered design:** `jsonlfile` (low-level single-file CRUD with byte-offset indexing via `.idx` files) → `jsonldf` (DataFrame adapter over jsonlfile) → `folderdb` (folder-level DB managing multiple JSONL tables with metadata)
- **Index-driven reads:** Each `.jsonl` file has a companion `.idx` file mapping linekeys to byte offsets for O(1) lookups and range queries
- **Sorted keys:** Records are kept sorted by linekey; bisect is used for O(log n) range selection on sorted index keys
- **In-place updates:** Updates that fit in the existing line space are written in-place; oversized updates append and blank the old line
- **Hierarchical storage:** Optional `hierarchy_depth` splits files into subdirectories based on dot-delimited key prefixes (e.g. `A.B.ticker.jsonl` → `A/B/ticker.jsonl`)
- **Metadata files:** `db.meta` (per-table stats), `h.meta` (hierarchy config), `config.meta` (runtime config like time precision)
- **Modular imports:** `vercontrol` (Git wrapper) and `visual` (Bokeh/Matplotlib) are lazy-loaded to avoid pulling heavy deps unnecessarily

## Conventions & Constraints
- Each JSONL line is `{"linekey": {value_dict}}` — one top-level key per line, value must be a non-empty dict
- Linekeys can be strings or ISO datetimes; datetime precision is configurable (`seconds` or `microseconds`)
- Serialization uses orjson exclusively (no stdlib json fallback)
- Files are written with large buffer sizes (50 MB) for throughput
- `lint_jsonl` uses stream-based approach: line-count check + spot verification to skip already-clean files; otherwise seeks per line in sorted order, writes to temp file, atomic rename
- `.idx` files use compact (no-indent) orjson format for minimal disk I/O
- `profile_test/benchmark.py` provides performance benchmarks with `--save`/`--compare` for before/after comparison
