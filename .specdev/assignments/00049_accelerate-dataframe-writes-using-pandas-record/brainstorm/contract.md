# Assignment contract

Kind: change

## Objective and context

Accelerate DataFrame writes by combining pandas record-oriented conversion with index keys obtained in bulk. Preserve eager materialization and all existing public behavior. Discussion D00003's dataframe_design.md and dataframe_evidence.md motivate the approach: pandas 3.0.5 prototypes improved tested string-indexed writes by 1.14–1.29x, with little benefit for datetime indexes. Those measurements are exploratory, not universal guarantees.

The published DataFrame-adapter, file-store and folder metadata designs remain authoritative. The bounded knowledge search for `DataFrame conversion` identified Assignments 00047 and 00048; their outcomes establish the metadata/statistics and byte-serialization behavior that this change must preserve. The baseline includes both deliveries. Unpublished roadmap drafts and concurrent Discussion artifacts remain outside Assignment ownership.

## Scope and non-goals

Optimize conversion for public DataFrame save/update and private FolderDB DataFrame overwrite/upsert paths, including their plural wrappers. Exclude numeric-only tuple specialization, streaming/lazy rows, read conversion, datetime key-format reuse, writer/index/cache redesign, new dependencies, public settings, product modules and roadmap publication.

## Expected behavior

Eligible frames become the same complete key-to-record dictionary before entering the writer, using pandas to normalize row values. All public signatures, None returns and storage behavior remain unchanged. Other inputs keep the existing index-oriented conversion.

## Important decisions

- Use pandas record conversion and bulk index extraction; do not implement custom scalar normalization or serialize index keys early. Preserve original key identity/order and normalization-collision behavior.
- Retain the existing path for zero-column frames and cases whose equivalence is unverified. Conservative guards for subclasses, unusual layouts/indexes, duplicate labels and pandas-version differences are delegated; no supported input may lose its existing behavior merely to obtain a fast path.
- Keep conversion eager and before writer invocation. Preserve each entry point's distinct validation/error ordering, including overwrite versus upsert duplicate-index errors and pandas warnings.

## Constraints and invariants

Preserve row/index/column ordering as observed by existing calls, missing-value and dtype conversion, datetime precision, unsupported-payload exceptions, caller-frame contents, slots, byte offsets, cache invalidation, synchronous per-table metadata and partial-failure behavior. Respect module boundaries and line caps: DataFrame adapter 150, file store 950, FolderDB 1250. Do not narrow supported Python/pandas versions or create a normal Assignment worktree.

## Delegated and reserved authority

After approval, delegate private conversion helpers and conservative fallbacks, focused tests and benchmarks, independent implementation review and repairs, and one final delivery commit. Use Claude Opus 5.0 at xhigh effort for review. Brainstorm review is optional; implementation review is required.

Public/design or compatibility changes, expanded optimization scope, full-suite execution, review waiver, or acceptance of a material unresolved regression or missed intended benefit require user agreement.

## Risks and assumptions

Record and index orientations can differ for empty columns, duplicate labels, extension values, index boxing and subclass overrides. The exploration covered only pandas 3.0.5; equivalence across the declared pandas range cannot be assumed. Record-list allocation may increase conversion-time peak memory even if total write memory looks unchanged. Datetime formatting and storage costs can dominate end-to-end latency.

## Verification authority

Focused tests, disposable benchmarks, and isolated compatibility environments using existing supported dependency versions are allowed. Verify relevant supported pandas branches; retain the existing conversion on branches or cases without sufficient equivalence evidence. Full-suite execution requires explicit permission. Ship benchmark helpers and verify documented commands from a fresh checkout.

## Acceptance criteria

- AC-1: Public and FolderDB DataFrame saves/upserts, including plural methods, preserve results, physical rows/indexes, metadata and caller frames versus the baseline. Cover numeric/mixed/nullable/object/categorical values, datetime and unusual indexes, normalization collisions, duplicate labels, zero rows/columns, subclasses and representative supported pandas branches, including fallback execution.
- AC-2: Conversion, invalid-key, unsupported-payload and publication failures retain existing exception and warning behavior, mutation timing, atomic control-file/slot guarantees, cache invalidation and per-table/plural metadata ordering. No lazy conversion moves a pre-write conversion error into a partially written table.
- AC-3: Repeated same-host comparisons pinned to the implementation after Assignment 00048 demonstrate lower median end-to-end FolderDB overwrite latency for 100,000-row string-indexed numeric and mixed numeric/text frames on the validated fast path. Report conversion-only cost, writes/upserts, datetime-indexed and empty/small frames, fallback overhead and conversion/full-write peak allocation separately. No fixed twofold gain is promised; a missed intended benefit or material unresolved regression returns for a user decision.
