# Assignment contract

Kind: change

## Objective and context

As the first implementation child of `.specdev/missions/M00001_implement-the-five-dependency-ordered-roadmap-wo/brainstorm/contract.md` at approved hash `7e0824300f897190603ccb138e56aaf5254389c0b40f69967908352465ceac4f`, make JSONL row and index publication process-crash-safe enough for the later metadata-slot, migration, and lint children to build on.

## Scope and non-goals

- In scope: torn-row-tolerant reads and index rebuilds, healing a missing terminal newline before append, safe publication ordering for grown upserts, atomic publication of every index write, and atomic replacement of `config.meta` and `h.meta`; all later-child features and all parent Mission non-goals remain out of scope.

## Expected behavior

Full loads, index builds, single-key lookups, and range reads skip torn rows rather than failing the whole read and report each skipped row through path-bearing `logging`; appends first restore a missing terminal newline, an interrupted grown upsert leaves either the old or new record available, and index plus protected control-file readers observe a complete old or new file rather than partial publication.

## Important decisions

- Publish a grown replacement before blanking its old copy and publish the index last; serialize indexes and protected control files completely to a same-directory temporary file before atomic replacement, while retaining the Roadmap's single index-loader recovery rules.

## Constraints and invariants

- Inherit all unchanged Mission constraints and invariants; specifically for this child, preserve absolute offsets, compact sorted indexes, existing public-call behavior and Python 3.8 compatibility, keep control files slot-free, and leave `db.meta` on its existing non-atomic policy.

## Delegated and reserved authority

- Delegated: internal helpers, temporary-file naming and cleanup, and focused fault-injection seams within the Mission's module boundaries; reserved: every authority retained by the Mission, including any weakening of publication order or atomicity and any addition of locking, fsync, or power-loss guarantees.

## Risks and assumptions

- The process-crash guarantees depend on same-filesystem replacement and precise failure boundaries; tests must not infer publication from sub-second mtimes on WSL2, and this child assumes no concurrent writers as established by the Mission.

## Verification authority

- Run focused JSONL/index/control-file tests with injected interruption at append, grown-upsert, serialization, and replacement boundaries, plus the parent-required Python 3.8 audit and seven-file logical-line inventory; the full tracked suite remains reserved for final Mission integration.

## Acceptance criteria

- AC-1: Full loads, index builds, single-key lookups, and range reads skip torn rows and report each skipped row through path-bearing `logging`, and appending to a file without a terminal newline heals the boundary without merging records.
- AC-2: Injected interruption around a grown upsert demonstrates that the key remains available as either its old or new record, while every completed data mutation leaves a complete, compact, sorted index published last by atomic replacement.
- AC-3: Injected interruption during `config.meta` or `h.meta` publication leaves a complete old or new control file, while `db.meta` retains its existing policy.
