# Assignment contract

Kind: change

## Objective and context

Eliminate the bytes-to-text-to-bytes conversion for serialized JSONL rows while preserving public interfaces and storage behavior. This follows Assignment 00047's write-statistics optimization. The earlier Discussion's 1.85x serialization-loop result is exploratory evidence, not an end-to-end performance promise.

Published file-store, metadata-slot, index-integrity/cache and folder metadata designs remain authoritative. The serialization design draft is unpublished and stays outside this Assignment's ownership; implementation does not depend on publishing it. The bounded knowledge search for `serialization bytes` identified Assignment 00034's outcome as relevant evidence for serialization failures, publication order and atomic control-file replacement. Current code governs the baseline where historical descriptions are stale.

## Scope and non-goals

Optimize private row serialization in ordinary save, atomic save and upsert, including their higher-level callers. Exclude DataFrame conversion, index serialization, lint, metadata-slot serialization and roadmap publication.

## Expected behavior

Serialized rows remain bytes until written, with unchanged public results, record format and publication behavior.

## Important decisions

- Produce newline-terminated UTF-8 bytes directly in the private row serializer and consume those bytes in ordinary save, atomic save and upsert. Higher-level DataFrame and FolderDB calls inherit the optimization.
- Preserve orjson's existing serialization capabilities, including NumPy support, exact record bytes and one trailing newline. Byte lengths continue to determine offsets, padding and append decisions.
## Constraints and invariants

- Preserve every public signature and return value, upfront key validation, serialization-error timing and partial-write behavior, slot ordering, atomic replacement boundaries, cache invalidation and synchronous metadata upkeep.
- Keep the existing streaming/buffering approach, including empty legacy saves. Do not add row batching, pre-serialization of whole tables, new dependencies, settings or product modules. DataFrame conversion, index serialization, lint and metadata-slot serialization are outside optimization scope.
- Respect existing source line caps, including 950 total lines for the file store. Preserve concurrent Discussion state and the unapproved roadmap draft; no roadmap publication or worktree is authorized here.

## Delegated and reserved authority

After approval, delegate the private implementation, focused compatibility/failure tests, reproducible benchmarks, independent implementation review and repairs, and one final delivery commit. Use Claude Opus 5.0 at xhigh effort for review, retaining the user's established preference. Brainstorm review is optional; implementation review is required.

A full suite, public/design changes, expanded optimization scope, review waiver, or acceptance of a material unresolved regression or missed intended benefit requires user agreement.

## Risks and assumptions

Unicode byte lengths and newline handling affect physical offsets and padding. Shared serialization also serves atomic control files, so failure behavior must remain unchanged. Serialization gains may be diluted by file I/O, index publication and DataFrame conversion; the existing single-writer durability model remains unchanged.

## Verification authority

Focused tests and disposable local benchmarks are allowed; full-suite execution requires explicit approval. Benchmark helpers needed by documented commands must ship and work from a fresh checkout. Independent implementation review is required; brainstorm review is optional.

## Acceptance criteria

- AC-1: Ordinary and atomic saves and upserts produce byte-identical records and indexes versus the pre-change implementation, with identical public results. Cover Unicode and escaped newlines, NumPy values, datetime keys, normalization collisions, empty tables, metadata slots, and in-place versus appended updates, including DataFrame/FolderDB integration.
- AC-2: Invalid keys, unsupported payloads, serialization failures and filesystem/publication failures preserve existing exceptions, mutation timing and recovery guarantees. Slot/index ordering, atomic control-file preservation, cache invalidation and write-derived metadata remain intact.
- AC-3: The targeted row paths contain no intermediate text conversion. Repeated same-host comparisons against the implementation after Assignment 00047 demonstrate lower median end-to-end save latency for a 100,000-row table of small records. Report serialization-only cost separately from full saves, dictionary/DataFrame FolderDB writes, small upserts, atomic control-file saves and empty/small tables. No fixed twofold gain is promised; a missed benefit or material unresolved regression returns for a user decision.
