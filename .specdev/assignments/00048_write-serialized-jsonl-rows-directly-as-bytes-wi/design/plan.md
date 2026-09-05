# Implementation plan

**Implementation Guides:** [api-security]
**Review Guides:** [api-security]

## Tasks

1. T-1: Return newline-terminated bytes from the private row serializer and
   remove re-encoding at its three writer call sites. Preserve serialization
   options, streaming, validation and publication boundaries (AC-1, AC-2).
2. T-2: Add focused byte/offset and serialization-failure compatibility cases;
   run affected JSONL, DataFrame, FolderDB, metadata/cache and durability suites
   and compare public signatures/storage against the pinned baseline (AC-1, AC-2).
3. T-3: Ship reproducible serialization/control-write measurements, reuse the
   existing FolderDB/write benchmark, document repeated baseline/candidate results
   and complete independent review and delivery (AC-3).

## Verification and constraints

Baseline is commit 784b038 (product bytes identical to Assignment 00047's d6e3de4).
Use disposable extracted source, not a worktree. The file store is exactly at its
950-line cap; the implementation must be line-neutral or smaller. New helper
modules are profiling artifacts only and must be tracked despite ignore rules.
The existing test_write_statistics failure injector delegates the serializer's
return value and needs no change for the new bytes result. Published designs and
Assignment 00034's outcome govern durability; the unpublished serialization note
and concurrent Discussion remain outside delivery ownership. No dependency or
trust-boundary change occurs. Full-suite execution is not authorized.
