# Assignment contract

Kind: change

<!-- Keep this proportional: reference existing project context, state only change-specific decisions, and use the fewest independent observable acceptance criteria (normally 1-5). -->

## Objective and context

Implement the normative design in
`.specdev/assignments/00010_define-and-benchmark-a-revisioned-immutable-fold/design/catalog_snapshot_design.md`
so `FolderDB` has a production revisioned immutable metadata snapshot, one
root-level catalog control namespace, single-writer publication, deterministic
recovery, and a 100% backward-compatible explicit reconciliation and legacy API
path, with startup metadata performance as a primary acceptance dimension.

## Scope and non-goals

- In scope: the public frozen snapshot/entry values and catalog exceptions;
  `.jsonldb/` creation, validation, atomic I/O, process cache, and platform lock;
  legacy migration; `db.meta` projection/reconciliation; every managed mutation,
  batch, lint, hierarchy, clear, commit, and revert responsibility enumerated in
  Assignment 00010; pending-state recovery; a complete supported-compatibility
  inventory; and focused structural, interruption, concurrency, compatibility,
  scaling, and before/after benchmark evidence.
- Non-goals: modifying OceanData or its adapters; caching the unified DirectoryData
  ticker map; production-dataset or 105-mount benchmarking; multi-writer execution;
  transparent detection of raw/out-of-band changes on the fast path; redesigning
  JSONL owner/index formats; or addressing lazy-worker shutdown latency.

## Expected behavior

`FolderDB.load_catalog_snapshot(timeout_seconds=5.0)` returns the fully validated,
immutable snapshot defined by Assignment 00010. A legacy or missing supported
catalog migrates through locked reconciliation; afterward a healthy load and
unchanged repeated loads access only stable `.jsonldb/catalog.json` state, reuse
the process snapshot across independent `FolderDB` instances, and perform zero
owner-tree walks and zero per-ticker index loads. Newer unsupported versions fail
closed without being overwritten.

Every managed mutation obtains the one folder-scoped writer ownership boundary,
publishes the exact pending envelope before catalog-relevant change, updates the
owner/index/aux/config/hierarchy and legacy projection as applicable, atomically
publishes one next catalog revision as the commit point, then durably clears
pending and updates the cache. Batched calls publish once. Direct low-level
`jsonlfile`/`jsonldf` or Git use below a managed root remains out of band and is
made visible by explicit `get_dbmeta()` reconciliation.

Readers never return a candidate observed across a catalog change or known pending
state. Active writer contention is bounded; abandoned valid ticker pending state
repairs only the proven set, while malformed, full, or untrusted state repairs the
whole folder. Clear, Git commit/revert, hierarchy, lint, configuration, and legacy
projection behavior follows the exact lifecycle and recovery matrix in the
normative design.

All existing supported callers and datasets continue to work without code,
configuration, or manual migration changes. Existing public signatures, accepted
inputs, return types/shapes, exceptions in previously valid uncontended operation,
JSONL/index/aux/config/hierarchy/`db.meta` formats, metadata fields, CRUD and lint
semantics, constructor behavior, explicit reconciliation, and managed Git behavior
remain observable-compatible. `.jsonldb/`, the snapshot API/types, and errors for
the new API or previously unsupported contention/filesystem guarantees are additive
only; no compatibility test may be deleted, weakened, or rewritten to accept a
regression.

Performance is a release criterion, not descriptive follow-up. A cold healthy
snapshot load scales with compact catalog bytes only; an unchanged load performs
bounded O(1) path/pending identity checks, does not decode the catalog again, and
returns the same object. Healthy construction/snapshot use never scales with owner
or ticker-index count. A ticker-scoped mutation discovers no owner tree and loads
only affected ticker indexes; a batch acquires once and publishes one pending
record, projection update, catalog revision, and cache transition rather than
repeating those costs per ticker.

## Important decisions

- Assignment 00010's public surface, exact version-1 envelopes, `.jsonldb/`
  tracked/ignored layout, cache identity, lock policy, transaction order, recovery
  matrix, raw-edit policy, and Git/clear lifecycle are frozen implementation input,
  not delegated design questions.
- `get_dbmeta()` remains the mutable, explicit full-reconciliation compatibility
  API and republishes both representations; `load_catalog_snapshot()` is the cheap
  managed-state API.
- The catalog is authoritative for healthy managed metadata reads, while JSONL
  owners remain authoritative for repair. `db.meta` remains a maintained legacy
  projection and never becomes the fast-load source.
- One active writer is enforced per canonical folder root across processes.
  Concurrent reader behavior and generation-safe derived-index repair retain the
  boundaries specified in the normative design.
- Backward compatibility takes precedence if an internal optimization in the
  normative design would alter supported existing behavior. In particular, the
  implementation must preserve externally observable constructor and
  `get_dbmeta()` healing semantics while ensuring that a healthy managed catalog
  takes the zero-walk/zero-index snapshot path. Any necessary design divergence
  must be surfaced for user approval rather than silently accepted.
- Correctness, backward compatibility, and performance are co-equal blocking
  outcomes. Durability work may add bounded write latency, but it does not permit
  hidden full reconciliation, unaffected-ticker index loads, per-ticker catalog
  commits inside a batch, repeated catalog decoding, or other avoidable O(number
  of tickers) work on healthy paths.

## Constraints and invariants

- Publication must use flushed and synced same-directory temporary files,
  `os.replace`, directory sync, and fail-closed platform locking; no fallback may
  silently weaken atomicity or durability.
- A fast snapshot must not call `get_file_list()`, read owners or ticker indexes,
  or consult `db.meta`, `h.meta`, or `config.meta`. Validation is all-or-nothing
  before cache insertion and errors never expose decoded record contents.
- Catalog replacement is the sole commit point. Any exception before or after it
  leaves state that the pending envelope can classify and repair without presenting
  known-stale metadata as current.
- Existing Python >=3.8 compatibility, public import paths and signatures, accepted
  inputs, return/error behavior for supported calls, timespec behavior, hierarchy
  naming, `.jsonl`/`.idx`/`.aux`/`.meta` formats, indexed-read self-healing, CRUD,
  lint, clear, and Git semantics remain compatible. Internal catalog lifecycle
  changes do not authorize an externally observable regression.
- Cold snapshot complexity is O(serialized catalog bytes) with one catalog decode;
  unchanged snapshot complexity is O(1) with zero catalog decodes/file loads after
  the first generation load. Ticker-scoped mutation complexity is limited to the
  affected owner/index work plus one compact catalog/projection publication; batch
  publication overhead is constant per public batch, not per ticker.
- The process registry retains at most one current snapshot per canonical root;
  callers may keep older immutable snapshots, but the registry itself cannot grow
  with revision count.
- Only focused verification is authorized; the real datasets and full test suite
  are not.

## Delegated and reserved authority

- Delegated: internal module boundaries and helper names, cache/lock factoring,
  re-entrant context implementation, focused test decomposition, failure-injection
  mechanics, and performance-neutral cleanup needed to route the enumerated paths
  through the frozen protocol.
- Reserved for the user: changing the public API or exact envelope/layout,
  weakening single-writer, validation, durability, pending recovery, Git/clear, or
  compatibility guarantees; accepting any pre-existing supported behavior or data
  as incompatible; accepting an algorithmic or material measured performance
  regression without explicit review; adding multi-writer behavior; treating raw
  edits as fast-path supported; modifying OceanData; or using production datasets.

## Risks and assumptions

- Mutation coverage is broad and current paths have inconsistent metadata refresh
  behavior; missing even one owner, deletion, hierarchy, lint, aux, clear, or Git
  path can silently stale the catalog. The implementation must use the Assignment
  00010 responsibility inventory as a completeness checklist.
- POSIX and Windows locking differ, and directory sync/replace guarantees vary by
  filesystem. Focused tests may mock the non-host adapter, but unsupported runtime
  semantics must raise `CatalogFilesystemError`.
- Failure injection must cover every pending/owner/index/projection/catalog/cleanup
  boundary. Tests must distinguish an active writer from abandoned pending state
  without relying on wall-clock timing races.
- Rewriting or projecting metadata may affect mutation latency. Correctness and the
  zero-walk/zero-index healthy-load structure take precedence, but the worker must
  measure projection, sync, and catalog costs separately so avoidable overhead is
  not hidden inside one aggregate number.
- “100% backward compatible” applies to supported JSONLDB behavior, not accidental
  reliance on private internals or formerly undefined concurrent-writer races. The
  implementation must document that boundary explicitly and treat any uncertainty
  about a public or persisted behavior as compatibility-sensitive.

## Verification authority

- Focused tests for changed modules: allowed after repository instructions are satisfied
- Full suite: requires explicit user approval unless already authorized here

## Acceptance criteria

- AC-1: The production public values, exceptions, `.jsonldb/` layout, strict
  envelope validation, legacy migration, stable-file reader, and process cache
  implement the normative design; focused spies prove cold/unchanged healthy loads
  perform zero owner discovery and zero ticker-index loads and reuse the immutable
  snapshot across independent instances.
- AC-2: Every managed mutation responsibility enumerated by Assignment 00010,
  including batches, deletion gaps, aux/config/hierarchy/lint/clear, explicit
  reconciliation, and Git wrappers, executes within one writer transaction and
  publishes exactly the required membership/facts, legacy projection, transaction
  identity, and revision without leaving nested or per-ticker batch commits.
- AC-3: Focused cross-process and failure-injection tests prove bounded writer
  contention, no lost publication, active-versus-abandoned pending handling,
  targeted and full repair, before/after-commit recovery, unsupported-version
  preservation, cache invalidation, clear preservation, and interruption-safe Git
  commit/revert outcomes.
- AC-4: A complete supported-compatibility matrix and focused regression evidence
  prove unchanged public imports/signatures, accepted inputs, return and exception
  behavior in valid uncontended use, existing dataset open/read/write behavior,
  JSONL/index/aux/config/hierarchy/`db.meta` formats and fields, constructor and
  `get_dbmeta()` healing, CRUD/batch/delete/lint/clear, timespec, hierarchy,
  companion, and managed Git behavior. No existing compatibility test is removed or
  weakened, and any discovered incompatibility blocks completion.
- AC-5: Reproducible before/after benchmarks at multiple synthetic catalog sizes
  record median cold load, unchanged reuse, reconciliation, one-ticker mutation,
  and batch mutation timings plus catalog decodes/loads, cache hits, owner walks,
  affected and unaffected ticker-index loads, revisions, and publications. At the
  256-ticker reference size, production cold snapshot median is at least 5x faster
  than current full reconciliation on the same fixture; unchanged reads perform one
  initial catalog load followed by cache reuse; healthy cold/reuse paths have zero
  owner walks and ticker-index loads at every size; one-ticker mutation loads no
  unaffected index; and a batch publishes exactly once. Any structural regression
  or failure of the relative read-speed gate blocks completion; absolute
  machine-specific time thresholds do not apply.
