# Implementation plan

**Implementation Guides:** [api-security]

**Review Guides:** []

## Tasks

1. **T-1 — Specify the catalog and inventory responsibilities (AC-1, AC-2).**
   Add an implementation-ready design artifact that freezes the immutable
   snapshot API and identity, version-1 catalog and pending envelopes, validation
   and recovery matrix, process cache and cross-process freshness rules,
   single-writer transaction boundary, compatibility/raw-edit policy, and
   clear/Git lifecycle. Include a focused inventory of current JSONLDB metadata
   consumers and every owner/index/catalog-relevant mutation path, plus the known
   OceanData startup consumers and each path's future responsibility.

2. **T-2 — Add the synthetic structural benchmark (AC-3).**
   Add a repository-tracked benchmark with configurable fixture dimensions and
   deterministic instrumentation for owner discovery and per-ticker index loads.
   Report elapsed time and structural counts for compact catalog load, current
   full reconciliation, one-entry metadata update, and repeated unchanged-catalog
   reads. Keep setup outside measured phases and avoid timing thresholds.

3. **T-3 — Add focused benchmark verification (AC-3, AC-4).**
   Add narrow tests for output completeness, fixture scale, current reconciliation
   counts, one-entry update counts, compact-load and unchanged-read zero-walk/
   zero-index targets, and snapshot reuse. Confirm benchmark imports and execution
   do not alter `FolderDB` runtime behavior.

4. **T-4 — Execute focused evidence and finalize receipts (AC-1, AC-2, AC-3,
   AC-4).** Run only the new focused test module and one default synthetic
   benchmark invocation, record commands against the dirty candidate revision,
   inspect the narrow diff, and write `implementation/progress.json` and
   `outcome.md` with acceptance evidence, deviations, and residual risks.
