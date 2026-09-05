# Implementation plan

**Implementation Guides:** [api-security]
**Review Guides:** [api-security]

Authority: contract 1017c4ae172ad45806e2ea335370e8cb07ffb47799230ed0c4522949cff6de77.
Baseline product: 0c54d29. Current boundary also includes the independently
published file-store note at 5f56588. D00003 stays outside delivery ownership.

## T-1 — Reuse validation output in the three shared writers (AC-1, AC-2, AC-4)

Extend private validation with optional reuse; return a deferred items factory
so custom mapping iteration retains its original timing. Retain an ordered key
list only for exact dictionaries starting with a known datetime/Timestamp,
ordinary timespecs and stable key types/timezones. Any custom key or unsupported
timezone discards reuse for the entire input. Use existing pandas dependency
for exact Timestamp recognition; named/custom timezones conservatively fall back.
Writers still serialize row payloads at their original mutation boundary; the
prepared keys are strings. Delete and read behavior are not optimized.

Consolidate only adjacent slot setup and writer descriptions as needed, preserving
placeholder/final-slot choice and exception order. No new product module or cap
increase. Measure actual physical lines after changes and review repairs.

## T-2 — Compatibility and failure evidence (AC-1, AC-2)

Compare physical bytes, public signatures, errors, warnings and conversion calls
with baseline functions; check datetime precision/collisions, timezones, subclasses,
custom mapping side effects, empty/fallback cases and all writer wrappers. Run
focused serialization, durability, slots, metadata, cache and DataFrame tests.
Reuse isolated supported pandas environments from Assignment 00049. Record
unavailable Python/dependency branches without claiming runtime verification.

## T-3 — Performance, delivery evidence and independent review (AC-3, AC-4)

Track a benchmark helper with baseline-package loading. Alternate baseline/candidate
processes; measure full FolderDB datetime dictionary/DataFrame overwrite, large
upsert, small/fallback operations and unique/collision-heavy traced allocation.
Run its documented command from a fresh staged checkout. Record source hashes,
raw samples, medians and explicit AC-4 counts; finish progress/outcome artifacts.
Run required Claude Opus 5.0 xhigh implementation review and address blockers.

Selected implementation/review guide: api-security@1. Knowledge context:
project_notes/big_picture.md and Assignment 00049 outcome. Relevant design notes:
jsonl_file_store.md, its metadata-slot/index-read-cache notes, dataframe_adapter.md,
and folder_database/metadata_and_timespec.md. Keep benchmarks out of concurrent
Discussion ownership and avoid full-suite execution.
