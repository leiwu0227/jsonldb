# Assignment contract

Kind: change

## Objective and context

Implement the published internal index read cache so repeated point and small-range reads avoid reparsing unchanged indexes, without changing public interfaces or recovery behavior.

Authority is the design published in commit `b744cbd39a86b126fc49d544afabc849e28f9c17`:

- `.specdev/project_notes/roadmap/designs/jsonl_file_store/index_read_cache.md`
- `.specdev/project_notes/roadmap/designs/jsonl_file_store/index_integrity_and_lint.md`

The existing file-store, metadata-slot and source-layout designs remain applicable. Discussion D00003's probe and evidence are supporting exploration, not a production implementation or a substitute for before/after measurements. The project big picture and historical lint performance note contain outdated descriptions; current code and published designs govern this change. The bounded knowledge search for `index cache` found no authoritative cache implementation guidance.

## Scope and non-goals

- In scope: private process-local reuse for point/range selectors; shared authoritative uncached loading and recovery; stable admission and per-read freshness checks; invalidation at low-level mutation boundaries; bounded memory and LRU eviction; compatible integration through existing folder/DataFrame selectors; focused compatibility and performance evidence.
- Non-goals: writer-seeded caching, write-statistics reuse, serialization or DataFrame conversion optimizations, cached rows or file handles, persistent cache files, background workers/watchers, new dependencies or public settings, stronger transaction/concurrent-writer guarantees, and changes to roadmap or unrelated discussion artifacts.

## Expected behavior

Unchanged files permit index reuse after an initial load. Range-key sequences are retained lazily within the same memory budget. Changed or unverifiable files fall back to the existing recovery behavior. Stable indexes repaired during a miss can be retained from the final successful parse. Full-table reads remain sequential; public index loads and writers keep independent mutable dictionaries.

## Important decisions

- Cache ownership stays in the file layer; FolderDB does not manage cache entries. Direct folder deletion/movement is detected by required file checks.
- Validate identity, size and modification/change timestamps for both files on every hit. Admit only a stable final read with no intervening invalidation; uncertain results remain uncached, without unbounded retries.
- Invalidate before table/index mutation, including metadata-only writes and repair paths. Preserve existing publication order and error handling even when a write fails.
- Synchronize cache bookkeeping without holding its lock during disk I/O, parsing or sizing. Account for retained indexes and optional key sequences, evict least-recently-used entries and bypass retention for oversized entries.
- Use 64 MiB as the initial internal budget candidate; the implementer may calibrate the final default from measured object sizes and working sets. This is a retained-object budget, not a process-RSS guarantee.

## Constraints and invariants

- Preserve signatures, return shapes/order, key conversion, metadata behavior, disk formats, warning/report behavior and missing-file/error policy. Cached dictionaries must never be exposed for caller or writer mutation.
- Keep lint's existing verification independent of cached reads. No removal or weakening of recovery, validation or diagnostics to obtain a benchmark gain.
- Respect existing module dependencies and the published maximum of 950 total lines for `jsonldb/jsonlfile.py`. Focused internal refactoring is allowed; adding a cache module or raising the cap is not delegated.
- Preserve unrelated working-tree changes, including concurrent Discussion D00003 artifacts and lifecycle state. No normal Assignment worktree is created.

## Delegated and reserved authority

- Delegated after approval: internal implementation choices within these constraints; focused refactoring, tests, repeatable benchmarks and supporting delivery evidence; benchmark-based internal budget selection; required independent implementation review and repairs; final Assignment delivery commit through SpecDev.
- Reserved for the user: changes to public behavior, published architecture or line caps; additional optimization scope; weakened acceptance criteria; full-suite execution; review waiver; accepting failure of the warm-read target or a material unresolved cold-path/memory tradeoff.

## Risks and assumptions

Filesystem metadata cannot detect arbitrary edits with indistinguishable fingerprints or provide transactional snapshots across concurrent writes. Preserve the current single-writer/non-transactional-read model. Initial parsing, ownership sizing and first-range preparation can add cold-read overhead; a working set larger than the budget may receive little benefit. These costs must be measured and considered in the chosen policy rather than hidden by a warm-only headline. The existing 940-line file leaves little room under its cap, so implementation requires focused refactoring without sacrificing readability or contracts.

## Verification authority

- Focused tests for changed modules: allowed after repository instructions are satisfied
- Full suite: requires explicit user approval unless already authorized here
- Repeatable local benchmarks on disposable fixtures: allowed, including pre-change baseline capture and representative cache-budget pressure. Timing thresholds belong in benchmark evidence rather than ordinary unit tests.
- Brainstorm review: optional. Independent implementation review: required.

## Acceptance criteria

- AC-1: Compatible cached reads: point and range selectors, including folder/DataFrame entry points, return the same records, order and key types with slots and malformed rows as before. Repeated reads of unchanged files reuse parsed indexes and retained range keys; independent public index dictionaries cannot poison the cache. Full-table reads retain their sequential path.
- AC-2: Freshness and recovery: supported writes, partial failures, slot changes, lint/index repairs, replacement, deletion and movement cannot cause subsequent reads to trust invalidated entries. Missing, empty, stale, malformed and non-object indexes retain existing recovery/error/report behavior. Stable repaired loads may be reused, while replacement during admission and intervening invalidation cannot install a mismatched snapshot. Lint fidelity/layout behavior remains intact.
- AC-3: Bounded retention and coordination: retained-object accounting covers indexes and optional range keys, remains within the selected budget under admission/eviction, and bypasses oversized entries. Concurrent read/admission/invalidation bookkeeping is consistent without serializing disk I/O under the cache lock. The chosen budget and the limits of retained-object accounting are documented.
- AC-4: Measured benefit and tradeoffs: against the pre-change implementation on the same machine, both warm point reads and inclusive 100-row ranges over a 100,000-row table whose index fits the cache achieve at least 2x median speedup with equivalent results. Use warmup and repeated samples. Separately report cold admission/first-range latency, one-shot reads, reads after writes, smaller/larger tables and multi-table working sets exceeding the budget, together with retained memory and eviction/oversize behavior. Use those results to justify the default policy; any material unresolved regression or missed target returns for a user decision rather than being silently accepted.
