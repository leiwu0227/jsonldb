# Catalog-aware auxiliary publication for sanitized data — handoff

Date: 2026-08-05
Status: Cross-repository architecture handoff; no implementation started
Origin: OceanData SpecDev Discussion D00011

## Executive summary

OceanData is designing a new pipeline:

```text
retriever -> sanitizer -> generator
```

Retriever outputs will be source-faithful `raw.*` tickers. A corresponding
sanitizer will materialize each clean ticker after automatic checks, imported
historical patch reconciliation, and any required human review. Generators must
consume only clean sanitizer tickers and must fail closed when a required clean
ticker has not been approved and published.

The proposed per-ticker gate is the existing managed opaque companion:

```text
<clean-ticker>.jsonl
<clean-ticker>.jsonl.aux
```

For a sanitizer-owned ticker, the JSONL holds clean data and `.aux` is the
consumer-owned publication certificate. Presence of a valid, compatible `.aux`
means the clean ticker may be consumed for its declared reviewed intervals.
Missing, malformed, incompatible, or insufficient `.aux` means the downstream
generator is blocked.

The current JSONLDB implementation is already sufficient for a correct first
OceanData implementation:

- `.aux` bytes are opaque to JSONLDB;
- replacement is atomic;
- owner mutation invalidates `.aux` first;
- `.aux` publication is catalog-transaction managed; and
- every managed owner or `.aux` mutation advances the folder catalog revision.

No JSONLDB change is therefore a hard prerequisite for per-ticker gating.
However, three bounded JSONLDB improvements would make discovery, consistent
reading, and multi-ticker publication stronger:

1. include opaque companion identity in catalog entries;
2. expose a snapshot-consistent owner-plus-aux family read; and
3. expose staged multi-ticker owner-plus-aux publication.

JSONLDB must remain semantically neutral. It should guarantee lifecycle,
identity, consistency, and publication ordering without interpreting OceanData
coverage, sanitizer readiness, human approval, patch, or quarantine semantics.

## Related work

Relevant JSONLDB artifacts:

- `.specdev/assignments/00008_add-one-lifecycle-managed-opaque-jsonl-aux-compa/`
- `.specdev/assignments/00010_define-and-benchmark-a-revisioned-immutable-fold/`
- `.specdev/project_notes/thoughts/20260729_managed_canonical_coverage_sidecar_handoff.md`
- `.specdev/project_notes/thoughts/20260804_directorydata_startup_metadata_catalog_refactor_handoff.md`

Relevant current implementation:

- `jsonldb/jsonlfile.py`: opaque `.aux` path, read, atomic write, removal, and
  invalidation before owner mutation;
- `jsonldb/folderdb.py`: managed `.aux` lifecycle and revisioned catalog
  transactions;
- `jsonldb/catalog.py`: strict immutable version-1 catalog snapshots.

The originating OceanData discussion is:

```text
lib/oceandata/.specdev/discussions/
  D00011_design-a-safe-extensible-data-quality-and-cleani/
```

## OceanData consumption model

### Clean ticker publication certificate

OceanData intends to define a strict auxiliary document resembling:

```json
{
  "kind": "oceandata.sanitized-ticker",
  "version": 1,
  "ticker": "fx.or.lvl.usdjpy.tky.spot",
  "publication_id": "<uuid>",
  "source": {
    "ticker": "raw.fx.or.lvl.usdjpy.tky.spot",
    "coverage_revision": "<uuid>"
  },
  "sanitizer_contract": "sha256:<digest>",
  "patch_digest": "sha256:<digest>",
  "reviewed_intervals": [
    {"start": "2020-01-01", "end": "2026-08-04"}
  ],
  "quality_ledger_digest": "sha256:<digest>"
}
```

The exact schema belongs to OceanData and is illustrative here. JSONLDB must not
parse or validate these fields.

### Fail-closed rule

For every required clean input, a generator must establish:

1. the ticker owner exists;
2. its `.aux` exists and has the expected OceanData schema;
3. its certificate names the correct owner ticker;
4. its sanitizer/patch/source bindings satisfy the selected build contract; and
5. its reviewed intervals cover the generator request.

Failure of any check raises a typed OceanData readiness error before generator
execution. A generator depending on several tickers is blocked if any required
ticker is not ready. Unrelated generators may proceed.

`.aux` must not be used to persist `awaiting_review`. Its existence is positive
authority to consume. Draft findings, unresolved conflicts, and suggested human
actions belong in OceanData staging/review artifacts. While review is unresolved,
the sanitizer does not publish a new certificate.

A reviewed quarantine is resolved rather than pending. OceanData may publish a
certificate for the reviewed scope while the quality ledger records deliberately
withheld observations. The auxiliary schema must distinguish reviewed scope from
the physical presence of one row for every date.

## What the current JSONLDB already guarantees

### Managed opaque companions

JSONLDB provides:

```python
db.get_aux_path(ticker)
db.read_aux(ticker)
db.write_aux(ticker, payload)
db.remove_aux(ticker)
```

The payload remains consumer-owned opaque bytes. This is the correct ownership
boundary for sanitizer certificates.

### Conservative invalidation

Managed owner writes remove the existing `.aux` before the owner can diverge.
The preferred interruption result is therefore an absent certificate rather than
a stale certificate presented as current. The sanitizer can publish a replacement
`.aux` only after its owner has been completely written and validated.

This establishes the safe per-ticker sequence:

```text
invalidate prior aux
publish clean owner/index
validate final clean owner
publish aux last
```

### Revision participation

`FolderDB.write_aux()` and `FolderDB.remove_aux()` execute inside catalog
transactions. They advance the folder catalog revision even though the current
version-1 entry projection does not change. A catalog revision therefore acts as
a managed folder mutation boundary covering owner and companion operations.

### Current limitation

The version-1 `FolderCatalogEntry` contains only:

```text
min_index
max_index
count
size
linted
lint_time
```

It does not expose whether `.aux` exists or identify its bytes. A caller must read
the companion separately. Loading one catalog snapshot, reading the owner and
auxiliary bytes, then confirming the catalog identity/revision has not changed is
a viable optimistic consistency loop, but JSONLDB does not currently package that
loop as one supported read operation.

## Recommended enhancement A: auxiliary identity in the catalog

Add semantically neutral auxiliary facts to each catalog entry. Candidate fields:

```json
{
  "aux_present": true,
  "aux_size": 487,
  "aux_sha256": "sha256:<digest>"
}
```

For an absent companion:

```json
{
  "aux_present": false,
  "aux_size": null,
  "aux_sha256": null
}
```

Exact null/zero representation should be pinned in the assignment contract.

### Benefits

- Consumers can discover companion availability from the compact catalog without
  recursively scanning the folder.
- A snapshot identifies the exact auxiliary bytes associated with an entry.
- A caller can verify separately read bytes against `aux_sha256`.
- Replacing `.aux` is observable in both the catalog revision and entry contents.
- JSONLDB remains unaware of the payload's domain semantics.

### Constraints

- Catalog schema evolution must be explicit. The current validator requires an
  exact version-1 entry shape, so silently adding keys is not compatible.
- A migration/reconciliation path must construct companion identity for existing
  databases.
- `.aux` files are expected to be small, so hashing them during managed write and
  reconciliation should be inexpensive. This assumption should be benchmarked.
- The digest is structural integrity metadata, not semantic validity.
- Missing or orphan companions must retain the conservative policies established
  by Assignment 00008.
- The catalog must not embed the auxiliary payload itself.

## Recommended enhancement B: snapshot-consistent ticker-family reads

Expose a public read that treats the owner, index, and optional opaque companion
as one logical ticker family and binds the result to a catalog snapshot.

Illustrative API only:

```python
family = db.read_family(
    "fx.or.lvl.usdjpy.tky.spot",
    snapshot=snapshot,
    lower_key=start,
    upper_key=end,
)
```

Illustrative result:

```python
TickerFamilyRead(
    data=frame_or_dict,
    aux=payload_or_none,
    catalog_id=snapshot.catalog_id,
    catalog_revision=snapshot.revision,
    aux_sha256=entry.aux_sha256,
)
```

The operation should either return a family consistent with the supplied catalog
generation or raise a typed error such as `CatalogChangedError`. It must not return
an owner from one managed generation and an auxiliary companion from another.

Possible implementation strategies include an internal before/read/after catalog
identity check with bounded retry. The contract should choose the public behavior
without exposing locking or retry details unnecessarily.

### Why this belongs in JSONLDB

Every consumer would otherwise have to reproduce the same race-handling protocol.
JSONLDB owns the publication ordering and catalog identity, so it is the appropriate
layer to expose a correct family read. The operation remains generic: the result is
data plus opaque companion bytes, not a sanitized ticker.

### Non-goals

- Do not interpret auxiliary payloads.
- Do not make a read-only consumer acquire the writer lock for a healthy read
  unless the assignment demonstrates that this is necessary.
- Do not promise arbitrary out-of-band edit detection beyond the existing catalog
  contract.
- Do not add OceanData exception types or dependencies to JSONLDB.

## Recommended enhancement C: staged multi-ticker family publication

Expose a supported public operation for publishing several owner-plus-aux families
as one catalog generation.

Illustrative API only:

```python
db.publish_families({
    ticker_a: TickerFamilyUpdate(data=data_a, aux=aux_a),
    ticker_b: TickerFamilyUpdate(data=data_b, aux=aux_b),
})
```

Required high-level behavior:

1. validate all requested ticker names, owner payloads, indexes, and auxiliary
   byte payloads before active publication;
2. stage complete replacements in the database filesystem;
3. invalidate any superseded active companions conservatively;
4. publish each matching owner/index/aux family with aux last;
5. publish exactly one new catalog revision as the commit boundary;
6. recover deterministically after interruption at every stage; and
7. never expose a stale auxiliary companion as matching changed owner data.

The current private `_catalog_transaction` and bulk owner methods are useful
implementation foundations, but OceanData must not depend on private APIs. The
assignment must inspect whether the current transaction protocol provides the
desired observable atomicity or only crash-aware catalog reconciliation. It must
not label a batch atomic until readers through supported APIs are proven unable to
observe partially promoted families.

### When this enhancement is needed

It is not required for the first per-ticker sanitizer gate. Per-ticker publication
allows:

```text
ticker A approved   -> A owner + aux consumable
ticker B unresolved -> B aux absent; B dependants blocked
ticker C approved   -> C owner + aux consumable
```

Multi-ticker publication becomes important if OceanData later requires an entire
sanitizer run, curve family, or dataset cohort to appear as one indivisible
generation.

## Consistency model with and without the enhancements

### Safe first implementation using current JSONLDB

OceanData can use this optimistic read protocol:

```text
load catalog snapshot A
read clean ticker data
read and validate clean ticker aux
load catalog snapshot B
if (catalog_id, revision) changed: retry
otherwise consume
```

This is conservative. An unrelated managed ticker mutation may force a retry, but
it does not permit unsafe consumption.

### With auxiliary catalog identity

The caller additionally verifies the read auxiliary bytes against the snapshot
entry's `aux_sha256`. It can reject absent companions immediately from the compact
snapshot.

### With snapshot-consistent family read

The consistency loop becomes one supported JSONLDB operation. OceanData performs
only its semantic certificate validation afterward.

### With staged multi-family publication

OceanData may optionally pin and consume a cohort published under one catalog
revision. This should be an explicit higher-level choice, not an accidental
consequence of per-ticker gating.

## Important distinctions

### Catalog bounds are not completeness

Catalog `min_index`, `max_index`, and `count` can cheaply reject obviously missing
ranges. They cannot establish that a sparse market-data source was successfully
queried or human-reviewed for every required interval. OceanData's `.aux` document
remains the semantic completeness and readiness authority.

### Catalog revision is not a sanitizer publication

The revision covers all managed folder mutations and can change for unrelated
tickers. OceanData may record it as an observation boundary, but should use its own
per-ticker or cohort `publication_id` for sanitizer semantics.

### Auxiliary digest is not approval

`aux_sha256` proves which opaque bytes belong to a catalog entry. It does not prove
that those bytes represent an approved sanitizer result. Only OceanData can decode
and validate that contract.

### JSONLDB transactions are not cross-database transactions

Raw retriever and clean sanitizer stores may be separate FolderDB roots. JSONLDB
should not attempt to coordinate semantic transactions across them. OceanData
binds the clean certificate to the selected raw publication, sanitizer contract,
and patch digest.

## Suggested implementation order

### Assignment 1 — catalog auxiliary identity and consistent family read

This is the recommended next bounded assignment.

Suggested scope:

- define catalog schema evolution and migration for `aux_present`, `aux_size`, and
  `aux_sha256`;
- update every managed auxiliary lifecycle path and catalog recovery path;
- expose a snapshot-bound owner-plus-aux read;
- preserve the version-1 data model and opaque-payload boundary;
- add focused crash, mutation-race, migration, orphan, and performance evidence.

Suggested acceptance outcomes:

- a healthy catalog load still performs no owner-tree walk or per-ticker index load;
- companion discovery requires no recursive folder scan;
- the snapshot's digest matches the exact bytes returned by a successful family
  read;
- owner or aux replacement during a read returns a consistent generation or a
  typed retryable error, never a mixed family;
- owner mutation cannot retain a previously authoritative auxiliary identity;
- databases without auxiliaries remain compatible; and
- JSONLDB never parses auxiliary payload content.

### Assignment 2 — staged multi-family publication

Open this only when OceanData chooses a concrete cohort-atomicity requirement.
Suggested scope:

- pin the public batch API and staging layout;
- define the precise reader-visible commit boundary;
- define rollback/recovery for every interruption point;
- prove one revision publication for the complete batch;
- retain conservative aux invalidation and supported legacy single-ticker APIs;
- benchmark representative many-ticker batches without broad suite execution.

Separating these assignments avoids coupling a small catalog/read improvement to a
substantially larger transactional publication problem.

## JSONLDB versus OceanData ownership

JSONLDB owns:

- owner/index/aux family lifecycle;
- atomic replacement of individual files;
- conservative auxiliary invalidation;
- catalog membership, identity, revision, and structural auxiliary facts;
- snapshot-consistent generic reads;
- optional generic batch publication and interruption recovery.

OceanData owns:

- raw versus clean ticker naming;
- sanitizer orchestration and state machine;
- suspect detection and human-review workflow;
- portable patch and raw-row conflict semantics;
- sanitizer certificate schema and validation;
- reviewed intervals, quarantine, and quality ledger;
- generator dependency policy and readiness exceptions.

## Handoff conclusion

OceanData can begin per-ticker sanitizer gating with the JSONLDB implementation
that exists today. The safe semantic is simple: a valid `.aux` grants consumption;
absence or incompatibility blocks it, and the clean owner is published before the
certificate.

The best immediate JSONLDB follow-up is not to understand sanitizers. It is to make
opaque companion identity part of the immutable catalog and to expose a
snapshot-consistent ticker-family read. A later, separately justified assignment
can add staged multi-family publication if OceanData requires cohort-level atomic
visibility.
