# Assignment contract

Kind: change

<!-- Keep this proportional: reference existing project context, state only change-specific decisions, and use the fewest independent observable acceptance criteria (normally 1-5). -->

## Objective and context

Evolve the immutable folder catalog so each ticker identifies its exact opaque
`.aux` companion, and add one supported snapshot-consistent read of a ticker's
dictionary data plus companion bytes. This is the bounded JSONLDB follow-up from
`project_notes/thoughts/20260805_catalog_aux_snapshot_and_batch_publication_handoff.md`.
It must retain the existing managed lifecycle, supported API/data compatibility,
and zero-scan catalog fast path established by Assignments 00008, 00010, and
00011.

## Scope and non-goals

- In scope: an explicit catalog version 2; transparent migration from valid
  version 1 catalogs and legacy folders; structural AUX presence, size, and
  SHA-256 identity; all existing managed owner/AUX mutation, recovery,
  reconciliation, clear, hierarchy, commit, and revert paths; an additive
  immutable family-read result and typed retryable generation-change error; and
  focused compatibility, race/failure, migration, and performance evidence.
- Non-goals: multi-family reads, staged or cohort publication, cross-root
  transactions, a combined owner-plus-AUX write API, changes to owner/index/AUX
  formats, interpretation or authentication of AUX payload semantics, OceanData
  types or dependencies, automatic discovery of arbitrary out-of-band edits on
  the healthy fast path, and changes to the legacy `db.meta` projection shape.

## Expected behavior

Catalog version 2 retains the version-1 top-level envelope and six existing
entry facts, and adds exactly these entry fields:

```json
{
  "aux_present": true,
  "aux_size": 487,
  "aux_sha256": "sha256:<64 lowercase hexadecimal characters>"
}
```

An absent companion is represented only as `aux_present: false` with
`aux_size: null` and `aux_sha256: null`. A present companion requires a
non-negative integer size and the exact prefixed digest format; empty bytes are
valid and have size zero. Validation remains exact and all-or-nothing before a
snapshot is cached. `FolderCatalogEntry` adds the three immutable fields with
backward-compatible defaults so its existing six-argument construction remains
valid. `db.meta` continues to project only its existing fields.

New code accepts strict version 1 and version 2 catalogs. On the first snapshot
load or managed reconciliation of a valid version-1 catalog, it takes writer
ownership and durably publishes version 2 as one normal next revision in the
same catalog lineage. Migration derives AUX identity from only the companion
path for each already-cataloged ticker; it does not load owner data/indexes or
walk the owner tree unless existing recovery rules independently require full
reconciliation. A missing catalog or legacy folder follows existing recovery.
Recognized catalog versions newer than 2 remain fail-closed and are never
overwritten. The unchanged pending protocol remains strict version 1.

Every managed owner mutation continues to invalidate its companion before the
owner can diverge and commits an absent AUX identity. AUX write/remove, targeted
or full recovery, explicit `get_dbmeta()` reconciliation, hierarchy moves,
clear, and managed Git lifecycle publish identities matching the final stable
companion bytes. A successful public batch still advances exactly one catalog
revision. Hashing is structural identity only; JSONLDB never parses AUX bytes.

Add an API equivalent to:

```python
FolderDB.read_family(
    name,
    snapshot=None,
    lower_key=None,
    upper_key=None,
    auto_deserialize=True,
    timeout_seconds=5.0,
) -> TickerFamilyRead
```

`TickerFamilyRead` is a frozen, slotted additive public value containing the
logical ticker name, the selected dictionary data, optional opaque AUX bytes,
the catalog ID and revision, and the recorded AUX digest. If `snapshot` is
supplied, it must belong to this canonical FolderDB root and the method either
returns exactly that generation or raises the additive retryable
`CatalogChangedError`; it never silently substitutes a newer generation. With
no snapshot, the method obtains and may retry against a current snapshot within
the bounded timeout. Missing ticker owners raise `FileNotFoundError`.

The read verifies a stable matching catalog generation before and after reading
the existing owner/index selection and optional AUX bytes. Presence, byte length,
and SHA-256 must match the entry, including detecting an unexpected companion
when the entry records absence. A pending mutation, changed generation, or AUX
identity mismatch must retry or raise `CatalogChangedError`, never return a
mixed managed generation. A healthy read takes no writer lock, performs no tree
walk, and loads no unrelated owner, index, or companion. Existing out-of-band
owner-edit limitations remain unchanged.

## Important decisions

- Use one strict catalog-v2 envelope rather than a separately synchronized AUX
  manifest. This preserves a single commit point and simple recovery. It is
  backward-compatible in the supported upgrade direction: new JSONLDB opens
  existing folders and v1 catalogs. After v2 is published, pre-Assignment-00012
  binaries reject it and require explicit catalog rebuild/downgrade; alternating
  old and new binaries is not supported.
- SHA-256 identifies exact opaque bytes without importing domain semantics.
  Size or file-stat identity alone is insufficient because same-size replacement
  and portable clone/Git identity matter.
- The new family read returns the canonical dictionary representation; callers
  may construct a DataFrame themselves. Existing `get_dict()` and `get_df()`
  remain unchanged and retain their existing consistency contract.
- Snapshot-bound reads use optimistic catalog checks and the pending protocol,
  not reader acquisition of the single-writer lock. Global revision changes,
  including unrelated managed ticker changes, may conservatively cause retry.

## Constraints and invariants

- Preserve supported existing imports, method signatures, return/exception
  behavior, owner JSONL, `.idx`, `.aux`, configuration/hierarchy files, flat
  `db.meta`, Git lifecycle, hierarchy, lint, timespec, clear, CRUD, and batch
  semantics. New values, error types, fields, and methods are additive.
- Catalog publication remains the single managed commit point. Pending is
  durable before mutation; recovery is deterministic before/after migration,
  owner mutation, AUX replacement/removal, and catalog commit interruption.
- Healthy v2 `load_catalog_snapshot()` reads only the compact catalog and path
  state: zero owner-tree walks, owner/index loads, or AUX reads/hashes. Snapshot
  objects remain immutable and shared by canonical root/generation.
- AUX hashing occurs only on an affected managed AUX operation, v1 migration,
  targeted/full reconciliation, or recovery. One-ticker work must not hash or
  load unaffected companions.
- Filesystem inputs remain untrusted; exact validation precedes caching and
  errors must not expose record or AUX contents. No new dependency is allowed.
- Maintain Python >=3.8 and the supported POSIX/Windows locking and durability
  behavior. Do not run or weaken the full suite without explicit approval.

## Delegated and reserved authority

- Delegated: choose private helpers, hashing implementation, bounded retry
  mechanics, result field type annotations, migration/recovery factoring, and
  focused test/benchmark organization while preserving the behavior above.
- Reserved for the user: any multi-family API or atomicity promise, separate
  manifest instead of catalog v2, AUX semantic schema/validation, combined
  owner-plus-AUX publication API, legacy `db.meta` expansion, dependency change,
  weaker compatibility/performance gates, or full-suite authorization.

## Risks and assumptions

- Catalog v2 intentionally prevents old binaries from reopening an upgraded
  control catalog; owner data remains unchanged and explicit downgrade/rebuild
  is possible but is outside this assignment.
- Managed writers provide consistency. Raw actors that bypass FolderDB remain
  outside automatic fast-path discovery; AUX digest mismatch is detected when
  the new family read actually reads that companion, and `get_dbmeta()` remains
  the explicit full reconciliation surface.
- Companion payloads are assumed small relative to owners. Benchmarks must still
  measure realistic absent, empty, small, and larger AUX payloads so hashing or
  larger catalog decoding cannot hide a material regression.
- A global folder revision intentionally makes the first family-read protocol
  conservative: unrelated managed writes can force retry rather than risk mixed
  data.

## Verification authority

- Focused existing and new tests for catalog, FolderDB, opaque companions,
  range selection, hierarchy, clear, and injected Git lifecycle: allowed.
- Deterministic process/race and interruption injection tests plus synthetic
  multi-size migration/load/read/AUX-publication benchmarks: allowed.
- Full suite: not authorized. Optional-dependency or platform-specific checks
  unavailable on the implementation host must be reported, not weakened.

## Acceptance criteria

- AC-1: Strict version-2 immutable snapshots expose exact AUX presence, size,
  and SHA-256; valid v1 catalogs migrate durably in the same lineage with one
  revision and without owner-tree/index work on the otherwise healthy path; all
  managed mutation, recovery, reconciliation, clear, hierarchy, and Git paths
  publish correct final AUX identity while unsupported newer versions fail
  closed.
- AC-2: `read_family()` returns selected dictionary data and optional exact AUX
  bytes bound to one catalog generation. Supplied stale/foreign snapshots,
  pending or concurrent owner/AUX replacement, absence/presence/size/digest
  mismatch, and before/after-commit interruption return a consistent generation
  or the specified typed failure—never a mixed managed family—and healthy reads
  do not take the writer lock or touch unrelated tickers.
- AC-3: The supported compatibility inventory passes unchanged for legacy
  folders, v1 catalogs, public APIs, formats, CRUD/batches, AUX invalidation,
  lint/hierarchy/timespec/clear/Git behavior, and mutable `db.meta`; no existing
  test is removed or weakened, JSONLDB never interprets AUX content, and the
  documented old-binary limitation is the only format-compatibility exclusion.
- AC-4: A reproducible benchmark at representative 16/64/256-ticker sizes proves
  healthy v2 cold loads perform one catalog decode with zero tree walks and zero
  owner/index/AUX loads, unchanged loads reuse the cached object, one-ticker AUX
  work touches only that companion, and the 256-ticker cold-load median remains
  at least 5x faster than full reconciliation. AUX hash cost is reported for
  absent, empty, small, and larger payloads, and material regressions block
  delivery.
