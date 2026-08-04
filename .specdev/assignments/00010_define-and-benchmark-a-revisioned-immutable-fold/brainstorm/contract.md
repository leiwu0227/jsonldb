# Assignment contract

Kind: change

<!-- Keep this proportional: reference existing project context, state only change-specific decisions, and use the fewest independent observable acceptance criteria (normally 1-5). -->

## Objective and context

Define an implementation-ready contract and reproducible benchmark baseline for a
revisioned, immutable `FolderDB` metadata-catalog snapshot. The contract follows
`.specdev/project_notes/thoughts/20260804_directorydata_startup_metadata_catalog_refactor_handoff.md`
and separates healthy compact-catalog loading from explicit reconciliation without
weakening existing recovery behavior.

## Scope and non-goals

- In scope: an in-repository call-site and mutation-path inventory; the proposed
  snapshot/API and pinned root-control-directory/storage-envelope contract;
  schema, revision, validation, compatibility, out-of-band-edit,
  pending-mutation, single-writer, Git, clear, and recovery semantics; and a
  reproducible synthetic benchmark large enough to expose per-ticker owner walks
  or index loads.
- Non-goals: implementing the transactional catalog or fast production API;
  changing current `get_dbmeta()` behavior; modifying OceanData; benchmarking the
  105-mount production dataset; parallel mount construction; parallel/multi-writer
  execution within one folder; or addressing lazy worker shutdown latency.

## Expected behavior

The design artifact defines one immutable snapshot per observed catalog revision.
It exposes ticker membership and raw `min_index`, `max_index`, `count`, and `size`
facts without exposing mutable catalog dictionaries. A healthy load validates and
reads only the compact catalog envelope; it performs no owner-tree discovery and
loads no ticker `.idx` files. Repeated reads for an unchanged revision reuse the
same snapshot, while a changed revision reloads the compact catalog.

The contract gives explicit fail-closed outcomes for missing, corrupt,
unsupported, inconsistent, or interrupted catalog state and specifies how a
later implementation enforces one active managed writer transaction per
`FolderDB` folder across processes. Writer-capable processes may coexist, but a
folder-scoped ownership mechanism serializes their mutations; concurrent readers
do not acquire that ownership on the healthy snapshot path. Independent
`FolderDB` instances in one process reuse a snapshot keyed by canonical folder
path and catalog identity. Existing `get_dbmeta()` remains the compatibility
reconciliation API. Raw out-of-band changes, including direct low-level writes
beneath a managed root, are recognized only through explicit reconciliation; the
new fast path does not claim to discover arbitrary filesystem edits.

The benchmark produces repeatable wall-time and structural call-count results for
compact stored-catalog load, current full reconciliation, one-entry metadata
update, and repeated unchanged-catalog reads. Structural evidence is primary:
the baseline must reveal accidental owner discovery and `.idx` loads independently
of timing noise.

## Pinned catalog design

Each `FolderDB` root owns exactly one reserved hidden control directory; hierarchy
subfolders never receive their own catalogs:

```text
<folderdb-root>/.jsonldb/
  .gitignore       # tracked; ignores pending.json, writer.lock, and *.tmp
  catalog.json     # durable compact catalog; tracked with database versions
  pending.json     # transient mutation/recovery record; normally absent
  writer.lock      # persistent advisory-lock target; contents are not authority
```

`catalog.json` is one compact orjson object, not JSONL and not accompanied by an
`.idx`. It is atomically published from a flushed and synced same-directory
temporary file with `os.replace`, followed by a directory sync. Its version-1
envelope contains `schema="jsonldb.folder-catalog"`, `version`, `catalog_id`,
integer `revision`, `last_transaction_id`, `timespec`, and `entries`. Each entry is
keyed by logical ticker name and contains only raw `min_index`, `max_index`,
`count`, `size`, `linted`, and `lint_time`; redundant names and absolute owner
paths are reconstructed only for the legacy view.

`catalog_id` identifies the catalog lineage and changes when a legacy/missing or
untrustworthy catalog is rebuilt from authoritative owners. `revision` starts at
one and increases exactly once for each forward managed catalog publication.
Snapshot equality uses `(catalog_id, revision)` plus file identity rather than a
numeric newer-than comparison: an explicit Git revert may restore an older tracked
catalog whose historical identity correctly matches the reverted owners.

`pending.json` is atomically published after writer ownership is acquired and
before any catalog-relevant mutation. It contains its own schema/version, a unique
transaction ID, base catalog identity/revision, target revision, scope
(`tickers` or `full`), and the sorted affected ticker names. The managed sequence
is: acquire the exclusive folder writer lock; recover any prior pending record;
publish pending; mutate owner/index and the legacy `db.meta` projection; atomically
publish `catalog.json` with the target revision and transaction ID as the commit
point; clear pending; invalidate process caches; release the lock. All managed
owner, index, companion, configuration, hierarchy, lint/repair, clear, and Git
revert mutations participate in the single-writer boundary; read-side atomic
rebuilding of a derived index retains its existing generation checks.

A healthy reader verifies pending is absent before and after reading a stable
catalog file, validates the complete envelope, and returns a frozen entry mapping.
An unchanged file/catalog identity reuses the cached snapshot. If pending exists,
the reader retries while another process owns the writer lock; if it can acquire
the released lock, it treats pending as interrupted and performs targeted repair
only for a valid ticker scope, otherwise full repair. Missing or internally invalid
catalog state performs locked full reconciliation and publishes a new catalog ID;
an unsupported newer schema fails clearly without destructive downgrade.

The root `.jsonldb/` directory is always excluded from ticker/aux discovery,
hierarchy movement, and empty-folder cleanup. `clear_folder()` preserves it and
publishes an empty next revision. JSONLDB Git commit runs under writer ownership
with no pending mutation and tracks `catalog.json`; Git revert publishes a
full-scope ignored pending record before reset so interruption remains detectable,
then validates or repairs the restored catalog and clears pending. A copy or clone
that omits `.jsonldb/` remains recoverable through the one-time full migration
path, but does not receive the fast path until that repair completes.

## Important decisions

- Preserve `get_dbmeta()` as the reconciling compatibility surface and design a
  distinct fast snapshot surface rather than silently changing getter semantics.
- Reserve `.jsonldb/` as one root-level JSONLDB control namespace. Do not repurpose
  the flat `db.meta`, create per-hierarchy catalogs, or introduce generation files
  plus a second current-pointer publication when one atomic catalog replacement is
  sufficient.
- Keep `db.meta` as a maintained compatibility projection and retain
  `get_dbmeta()` as explicit reconciliation. The new catalog is authoritative for
  healthy managed metadata reads; interrupted projection/catalog disagreement is
  guarded by pending state and repaired under writer ownership.
- A known pending mutation, invalid envelope, or unsupported version cannot use the
  fast path. Missing/invalid supported state repairs under lock, a live writer
  causes bounded retry, stale valid pending state repairs by declared scope, and a
  newer unsupported schema raises without overwriting it.
- The concurrency model is single active writer with multiple concurrent readers.
  The later implementation must realize the pinned cross-process ownership,
  contention, stale-pending recovery, and transaction boundary covering pending
  state, owner/index mutation, catalog publication, and revision increment.
- Existing per-ticker `.jsonl.aux` files remain opaque owner-associated state and
  are not reused as a catalog generation or journal.
- Benchmark fixtures are synthetic and repository-local; no real OceanData dataset
  is read or modified by this Assignment.

## Constraints and invariants

- Current `.jsonl`, `.idx`, `db.meta`, hierarchy, lint, and configured timespec
  semantics remain unchanged by this design-and-benchmark Assignment.
- The future publication protocol must prefer absent derived state over stale state
  presented as current, preserve deterministic ticker addition/deletion, and retain
  index self-healing through normal indexed reads and explicit reconciliation.
- Cross-process readers must be able to notice a committed revision without walking
  the owner tree. At most one managed writer transaction may be active in a folder;
  no catalog entry, pending record, or revision may be published outside that
  ownership boundary.
- Catalog and pending publication use same-directory atomic replacement and durable
  sync ordering. Temporary, lock, and pending files never become tickers or Git
  version content; committed catalog state does.
- The benchmark must avoid flaky timing thresholds and must not run the repository's
  full suite or production-scale dataset.

## Delegated and reserved authority

- Delegated: exact fast API/type names, frozen Python type implementation,
  design-artifact location, platform adapter and timeout for the advisory writer
  lock, synthetic fixture dimensions, instrumentation technique, and benchmark
  output formatting, provided the pinned layout and observable contract remain.
- Reserved for the user: changing `get_dbmeta()` semantics, weakening fail-closed
  recovery or revision guarantees, changing the single-writer/multiple-reader
  model, changing the `.jsonldb/catalog.json` layout or tracked/ignored boundaries,
  claiming transparent support for arbitrary out-of-band edits, expanding into
  transactional production code or OceanData, or using the real 105-mount dataset.

## Risks and assumptions

- The current flat `db.meta` shape has no envelope namespace; the design must give
  existing readers an unambiguous compatibility projection and one-time migration
  path. Existing absolute `path` fields are not portable and must not enter the new
  catalog.
- `FolderDB.__init__()` currently combines folder-mtime reconciliation with
  timespec healing, so the inventory must identify initialization work that could
  independently trigger owner/index scans after `get_dbmeta()` is separated.
- Microbenchmark wall times are cache- and machine-sensitive. Call counts and I/O
  boundaries are the stable acceptance evidence; timings are descriptive.
- Folder-scoped writer ownership deliberately trades parallel write throughput for
  a simple deterministic catalog and recovery model. Lock granularity must not be
  silently widened into a multi-writer design during implementation.
- Current clearing skips hidden directories, while Git adds quoted `*` pathspecs
  including hidden content. The later implementation must explicitly preserve and
  exclude `.jsonldb/` from data cleanup while relying on its tracked `.gitignore`
  to keep lock, pending, and temporary files out of versions.
- Network or unusual filesystems may not provide the advisory-lock, atomic-replace,
  or directory-sync semantics required by the protocol; unsupported behavior must
  fail clearly rather than pretend to provide the guarantee.
- This Assignment deliberately leaves production behavior unchanged. Its output
  must be specific enough that the transactional implementation can proceed without
  reopening storage, compatibility, recovery, or out-of-band-edit policy decisions.

## Verification authority

- Focused tests for changed modules: allowed after repository instructions are satisfied
- Full suite: requires explicit user approval unless already authorized here

## Acceptance criteria

- AC-1: A version-controlled, implementation-ready design specifies the immutable
  snapshot facts and identity, the exact `.jsonldb/` tracked/ignored layout and
  version-1 envelope, fast-load and process-cache behavior, `db.meta` compatibility,
  raw-edit policy, pending transaction/commit point, cross-process freshness,
  folder-scoped writer ownership, Git/clear lifecycle, and recovery outcomes
  required by the later transactional implementation.
- AC-2: A focused inventory accounts for current in-repository and known OceanData
  metadata consumers plus every JSONLDB path that creates, mutates, moves, lints,
  repairs, or removes an owner, and assigns each path a future fast-load,
  publication, invalidation, or reconciliation responsibility.
- AC-3: A reproducible synthetic benchmark reports compact catalog load, full
  reconciliation, one-entry update, and repeated unchanged-catalog reads with both
  elapsed time and structural owner-discovery/index-load counts, and its fixture is
  large enough to expose accidental O(number of tickers) work without brittle
  timing assertions.
- AC-4: Focused verification demonstrates that the benchmark/inventory additions
  do not alter current runtime behavior, and records the structural target for the
  next Assignment: a healthy snapshot load performs zero owner-tree walks and zero
  per-ticker `.idx` loads.
