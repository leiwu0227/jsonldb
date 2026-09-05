# Implementation plan

Preserve the approved contract and existing publication order. Private file-store
implementations optionally return scalar statistics after index publication;
public wrappers retain their signatures and None returns. DataFrame adapters
forward the result to FolderDB's private metadata updater. Disk-derived fallback
remains the path for explicit refresh, rebuild and lint.

**Implementation Guides:** [api-security]
**Review Guides:** [api-security]

## Tasks

1. T-1: Add operation-local writer statistics and private adapters, retaining
   validation, error boundaries and module line caps (AC-1, AC-2, AC-3).
2. T-2: Verify independent disk equivalence, public compatibility, absence of
   post-publication index reads, failure ordering and fallback recovery with
   focused tests (AC-1, AC-2, AC-3).
3. T-3: Ship a reproducible before/after benchmark, record representative costs,
   complete independent review and deliver one commit (AC-3).

## Verification

Run the focused file-store, DataFrame, FolderDB, metadata, cache, lint and recovery
suites used by Assignment 00046, plus the new write-statistics tests. Compare
against commit 58bdf0d in disposable extracted source trees without a worktree.
Use repeated same-host samples, measure summary derivation separately, and
verify benchmark execution from a fresh snapshot. Full-suite execution is not
authorized. No new trust boundary or dependency is introduced.

## Supporting evidence

- Assignment 00035 outcome: row/slot/index publication guarantees.
- Assignment 00046 outcome: independent writer/cache ownership and benchmark
  helper tracking hazard.
- Published file-store, index-read-cache, and FolderDB metadata/timespec notes.
