# Implementation plan

**Implementation Guides:** [api-security]
**Review Guides:** [api-security]

Authority: approved contract `f33c45565dde43850f0bb1b8ab7aedfb08bc39dda50faeee70869cc09e291a08`; baseline `f05791e`.

## Tasks

### T-1: Envelope and key semantics (AC-1, AC-2, AC-3)

Extend SlotInfo through a property, preserving its tuple shape. Keep offset validation and envelope encoding in metaslot (250 physical lines, no package imports). Introduce cohesive private `jsonldb/_tabletimezone.py` (maximum 250 physical lines) for table-aware key normalization, write preparation and accessor preflight. It may import metaslot; metaslot cannot import it. Centralize save-slot preparation here to make room in jsonlfile (950 lines). FolderDB stays at 1250 and jsonldf at 150.

Use the known v1 field directly; consumer data remains opaque. Validate complete envelope fit before mutable operations or index rebuilding. Determine table emptiness from physical nonblank rows rather than a potentially stale index. Preserve timezone in overwrite placeholders while continuing to defer consumer data. Refuse recognizable malformed declarations. Full loss is undetectable and documented.

Keep the old validation/reuse path for undeclared tables. Declared-table preparation caches an ordered list of canonical strings without collapsing normalization collisions. Low-level saves/upserts/deletes own enforcement; adapters inherit it. Apply identical normalization to lookup/range/delete bounds, before equal-bound shortcuts. Ordinary naive/string read keys need no additional header I/O; only potential aware keys require table interpretation.

### T-2: Accessors, regression evidence and documentation (AC-1, AC-2, AC-3, AC-5)

Add only the four approved timezone accessors. Configure/remove on empty existing slotted files, idempotent same-offset on populated files, independent of data. Preserve timezone on metadata writes, lint, resize and restart. Atomic control-file save refuses declared tables.

Test offset canonicalization and errors; aware/naive datetime/Timestamp/string keys; collisions; both precisions; all wrapper operations; index recovery/cache; malformed declarations; slot shrink; publication failure boundaries. Separately exercise historical unslotted files and unchanged public signatures against baseline, including existing key-reuse and DataFrame tests. Test representative available pandas runtimes. Document opt-in creation and naive read results, no populated migration, offset range and recovery limits.

### T-3: Delivery evidence and independent review handoff (AC-4, AC-5)

Record focused command evidence, physical-line counts and Python 3.8 syntax validation. Compare undeclared-table results and bounded latency against a disposable baseline snapshot, without creating a worktree. Run required Claude Opus 5.0 xhigh implementation review through SpecDev; repair findings and recheck relevant evidence and line caps. Full-suite execution is not authorized. Preserve concurrent D00003 state and deliver one Assignment commit.

Selected guide: api-security@1. Knowledge lead: Assignment 00050 outcome; unknown metadata preservation baseline de7217f. No historical metadata-envelope compatibility work.

## Suffix-validation refinement

Validate each numeric offset component before Python ISO parsing: fromisoformat normalizes overflow and can erase fractional zero offsets. Reject nonzero fractional offsets against the minute-resolution table declaration. Detect a suffix only immediately after the timestamp/fraction so labels such as `_batch-7` remain arbitrary strings. Existing undeclared-table conversion remains unchanged. Added 33 targeted cases, followed by the complete focused checks and pandas matrix.
