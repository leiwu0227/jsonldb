# Objectives and design decisions

JSONLDB's primary purpose is portable time-series storage for Python programs and
agents. A useful dataset should remain understandable as files, travel with a
project, and support ordinary Git review when its owner wants version history.
The current implementation also supports non-datetime string keys.

This guide explains existing choices and their trade-offs. It is an orientation
to the implemented library, not a promise of additional database features.

## Files are an interface

Observations live in readable JSONL. A person or agent can inspect them with a
text editor, standard JSON tools or a small script, independently of JSONLDB.
Each line gives the key and its complete record. This favors portability and
inspection at the cost of JSON encoding overhead and a less compact representation
than specialized binary or columnar storage.

The library supplies efficient indexed access and disciplined mutation around
these files. Direct inspection needs the small set of rules documented in the
[file-format guide](file-format.md), including slots, tombstones and duplicates
that may remain after an interrupted write.

## One file per series

A logical table is one `.jsonl` file. A table name typically identifies a series,
and a datetime key identifies its observation. This makes selecting and moving
individual datasets straightforward. Optional hierarchy groups many series into
directories while preserving their full names in filenames.

The trade-off is filesystem overhead from many files. Metadata and directory
operations also become more expensive as the number of tables grows. A timestamp
is one key per table; applications needing multiple events at the same timestamp
must distinguish their series or choose another identity scheme.

## Explicit time interpretation

Database precision is uniform: seconds or microseconds. Uniform timestamp text
supports lexical ordering and range selection. String input remains flexible,
so callers are responsible for consistent precision when providing strings.

A timezone is an optional per-table fixed offset stored beside consumer metadata.
Matching aware inputs normalize to local suffix-free keys; conflicting offsets
are refused. Reads remain naive datetimes or strings. No machine timezone is
inferred. Geographic zones, DST and populated-table timezone migration are outside
the current implementation. These choices keep interpretation explicit and
portable while preserving historical undeclared-table behavior.

## Derived indexes and selective reads

A sidecar maps keys to byte offsets. It is disposable and rebuilt through a shared
loader when missing, empty, corrupt or stale. Point reads seek directly; ranges
bisect ordered keys and read selected rows in physical offset order before
returning key order. Full-table loads retain a sequential path.

A private process-local LRU cache retains validated indexes under a 64 MiB budget,
checking table/index identity, size and timestamps on reuse. It stores no rows
or open handles. The budget bounds retained cache objects, not total process
memory. Cold reads and writes still load whole indexes, and each write publishes
the full sorted index. Small updates therefore do not have constant total cost
as a table grows. Benchmark the actual table sizes and access pattern.

## Incremental mutation and explicit compaction

An updated row stays in place when it fits. A larger row appends, and the old line
is blanked. Deletes also leave blank lines. Lint reclaims that dead space and
restores sorted physical order. This reduces data-file rewrite work during normal
updates, with periodic compaction and noisier physical diffs as the trade-offs.

Consumer metadata uses a fixed-width first-line slot. Its bytes are published
after the rows they describe and read before those rows. Library-owned timezone
must already be present when dependent observations are published. Same-file
storage keeps the declaration and consumer record with their series; reserved
padding and explicit slot-width migration are the costs.

## Failure boundaries and diagnostics

The library targets recoverable process interruptions. Index/control publication
uses atomic replacement where implemented; ordinary table writes are in place.
This does not supply transactions, rollback, coordinated concurrent writers, or
fsync-based power-loss durability. One process owns writes. Stop it before copying,
Git operations or external edits.

Reads skip malformed rows with logging. Opening repairs controls/indexes and may
reorganize hierarchy paths, while preserving table bytes. Lint can remove damage.
Bounded reports record what open and lint encountered, including removed-byte
excerpts. Recovery is not lossless reconstruction of torn observations or erased
timezone declarations.

## Git remains the caller's choice

Git can version readable observations and configuration alongside code. JSONLDB
does not own repository state, automatic commits, merge resolution or remote
synchronization. This keeps the library usable in ordinary folders as well as Git
working trees. [The Git guide](portability-and-git.md) explains derived artifacts,
snapshot boundaries and index rebuilding after restores.

## Package map

| Module | Responsibility |
| --- | --- |
| `folderdb.py` | Names, hierarchy, configuration, per-table statistics and report hooks. |
| `jsonldf.py` | Convert DataFrames to/from the file-store interface. |
| `jsonlfile.py` | Single-file rows, indexes, cached reads, mutation and lint. |
| `metaslot.py` | First-line envelope classification, encoding and preservation. |
| `_tabletimezone.py` | Private timezone interpretation and mutation preflight. |
| `reports.py` | Scoped logging capture and bounded report files. |

Maintainer specifications and delivery records live under `.specdev/`; they may
include historical proposals and superseded behavior. The public guides describe
current use. Source-file size budgets encourage bounded modules, but future work
must preserve readability rather than compressing code to meet a count.
