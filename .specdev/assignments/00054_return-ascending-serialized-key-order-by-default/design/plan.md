# Implementation plan

**Implementation Guides:** none
**Review Guides:** none

## T-1 — Ordered reconstruction (AC-1, AC-2, AC-3)

Retain full physical scanning and per-row strict handling. Let the existing key-storage helper return the stored key, preserving its conversion behavior. Full reads retain original serialized spellings for converted keys so ordering supports mixed datetime/string outputs and conversion collisions without changing physical last-winner values. Check final logical keys once, return the existing result if ascending, or rebuild in lexical order with shared values. Indexed paths and wrappers inherit behavior without additional sorts. Stay inside source caps through bounded comment/documentation simplification.

## T-2 — Behavioral evidence and documentation (AC-1, AC-2)

Cover full/bounded/one-sided/point wrapper parity, duplicate resolution, converted-key collisions, nulls/fields, slots, strict damage excluded from indexes, missing/empty tables and immutable bytes. Verify the ordered helper returns the same dictionary on its fast path, stops at an inversion, and shares record values on reorder. Update only obsolete physical-order expectations and public ordering documentation.

## T-3 — Performance and handoff (AC-3)

Compare final code with baseline commit a55e844 on sorted, 1% backfilled, and shuffled large tables in fresh processes. Record warm-cache elapsed time and peak RSS, including date keys and datetime conversion, retain raw results and a reproducer. Run focused affected tests plus source-cap/Python 3.8/diff checks; write outcome and delivery artifacts for required independent review.

Context: published ordered/strict-read designs, Assignment 00053 outcome, and the ignored ordered-read prototype. The prototype is not final acceptance evidence. Review is a controller phase after these author tasks complete.
