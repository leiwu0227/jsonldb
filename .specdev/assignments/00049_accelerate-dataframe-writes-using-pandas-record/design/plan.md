# Implementation plan

**Implementation Guides:** [api-security]
**Review Guides:** [api-security]

## Tasks

1. T-1: Share guarded eager record conversion across public and FolderDB save/
   upsert adapters, preserving their distinct validation and fallback behavior
   within the 150-line adapter cap (AC-1, AC-2).
2. T-2: Compare converted values, warnings, exceptions, physical storage, caller
   frames and failure ordering against the original path. Exercise supported
   pandas branches in isolated environments and run affected focused tests
   (AC-1, AC-2).
3. T-3: Ship and run a repeated conversion/write/allocation benchmark pinned to
   5276e8e, including small and fallback costs. Complete independent review,
   repair and one final delivery commit (AC-3).

## Decisions and evidence

The measured benefit comes from pandas 3.0's index/record conversion paths.
Initially restrict acceleration to validated pandas 3.0 behavior and ordinary
frames with ordinary indexes, unique labels and nonempty columns; keep other
versions and unfamiliar inputs on the original path. Validate any widening with
evidence. Keep all records materialized before calling the writer. No low-level
writer or FolderDB ownership change is needed. The adapter starts at 134/150
lines; focused removal of redundant conversion comments can provide headroom.

Use D00003's dataframe_design.md and dataframe_evidence.md as experimental leads,
the published DataFrame adapter as authority, and Assignments 00047/00048 outcomes
for metadata and byte-serialization guarantees. Preserve the concurrent Discussion
and unpublished roadmap draft. Compatibility environments and fixtures are ignored
scratch; full-suite execution is not authorized. No dependency or trust-boundary
change is introduced.
