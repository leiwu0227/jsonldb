# Assignment contract

Kind: change

## Objective and context

Reuse information already held by a successful writer to refresh FolderDB table statistics, eliminating the read and parse of the table index immediately after publication. The user agreed to this next optimization after Assignment 00046's read cache; that cache deliberately leaves writer and metadata-maintenance loads independent.

The existing file-store, DataFrame-adapter, index-integrity/read-cache, and folder metadata/timespec design notes remain authoritative. The discussion authorizes an internal optimization, not new public behavior. The knowledge search for `metadata index` identified Assignment 00035's outcome as relevant evidence for rows/slot/index publication; the current code and designs supersede historical performance descriptions and stale big-picture features.

## Scope and non-goals

- In scope: private write-result plumbing through the file store, DataFrame adapter and FolderDB; dictionary/DataFrame overwrite and upsert paths, including their existing plural wrappers; immediate per-table metadata refresh; focused compatibility, failure and performance evidence.
- Non-goals: optimizing deletes, metadata rebuilds or lint; batching/deferred metadata publication; seeding or redesigning the read cache; serialization or DataFrame conversion optimization; new settings, dependencies, modules, disk formats, transaction semantics or roadmap edits.

## Expected behavior

After successful table/index publication, the internal FolderDB write path receives a small operation-local statistics summary and uses it for that table's `db.meta` entry before returning. Public APIs retain their signatures and return values, including existing `None` results. Explicit metadata refresh, database rebuild and lint retain their disk-derived behavior when no write summary exists.

## Important decisions

- Derive count and serialized-key bounds from the final effective index, not input row count or dictionary insertion order. Empty tables have zero records and null bounds; metadata slots are excluded. Obtain size from the successfully written table.
- Expose the summary only after table and index publication succeed. Keep it private and local to the operation; do not pass writer-owned dictionaries into the read cache or use global callbacks to transfer results between layers.
- Compute statistics only for the internal paths needing them; ordinary low-level callers should not pay a new O(n) summary pass merely to discard it.
- Keep metadata refresh synchronous and per table, including plural calls. Preserve existing behavior if metadata publication fails after table publication; do not introduce rollback or atomicity across files.

## Constraints and invariants

- Preserve table/slot/index publication order, upfront validation, serialized-key collision behavior, DataFrame uniqueness checks, datetime precision, naming/hierarchy handling, supported path inputs, errors, warnings and cache invalidation.
- Retain every `db.meta` field and its existing meaning, including lint flags/time. Successful summaries must agree with the disk-derived statistics of the published index. Standalone metadata refresh/recovery remains available.
- Respect existing module boundaries and total line caps: file store 950, DataFrame adapter 150, FolderDB 1250. Focused internal refactoring is allowed without reducing readability or removing guarantees.
- Preserve concurrent Discussion artifacts and unrelated work. No normal Assignment worktree is created.

## Delegated and reserved authority

- Delegated after approval: private result types/helpers and necessary internal refactoring; focused tests and reproducible benchmarks; documentation of delivered behavior; required independent implementation review, repairs and the final SpecDev delivery commit.
- Reserved for the user: public behavior or design/line-cap changes; added optimization scope; weakened publication/recovery guarantees; full-suite execution; review waiver; acceptance of a material unresolved regression or failure to demonstrate the intended performance benefit.

## Risks and assumptions

The current in-memory write index need not be sorted even though its published representation is sorted. Bounds must therefore be correct for unsorted inputs, inserted boundary keys and serialization collisions. Summary derivation itself has a cost; removing a parse does not prove an end-to-end gain. The file store currently has only five lines of headroom, making focused refactoring necessary. The existing single-writer/non-transactional-read model remains unchanged.

## Verification authority

- Focused tests for changed modules: allowed after repository instructions are satisfied
- Full suite: requires explicit user approval unless already authorized here
- Local before/after benchmarks on disposable fixtures and fresh-checkout benchmark smoke checks: allowed. Ship the benchmark helpers needed by documented commands; profiling ignore rules must not omit them.
- Brainstorm review: optional. Independent implementation review: required.

## Acceptance criteria

- AC-1: Successful dictionary/DataFrame overwrites and upserts, including plural methods and empty tables, return the same public results and produce table data, slots and `db.meta` fields equivalent to independent disk-derived statistics. Cover unsorted and boundary-changing keys, datetime precision, normalization collisions and existing validation behavior.
- AC-2: Table/index failures do not publish success statistics, and later metadata failures preserve the existing committed-table/error behavior. Explicit metadata refresh, rebuild and lint retain their recovery semantics; cache invalidation and private mutable ownership remain intact. Plural methods retain per-table completion and partial-failure ordering.
- AC-3: On healthy optimized writes, metadata upkeep performs no post-publication table-index read/parse or table-data rescan; initial upsert index loading and normal `db.meta` I/O remain permitted. Repeated same-host comparisons against the pre-change implementation demonstrate lower median end-to-end latency for one-row FolderDB upserts into a 100,000-row table. Report dictionary and DataFrame results, overwrite/create, empty/small tables and ordinary low-level write costs separately, including summary-derivation overhead. No fixed twofold gain is promised; a missed benefit or material unresolved regression returns for a user decision. Benchmark commands must work from a fresh checkout.
