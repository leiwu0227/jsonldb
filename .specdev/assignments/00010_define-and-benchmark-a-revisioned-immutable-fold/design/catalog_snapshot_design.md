# Revisioned immutable FolderDB catalog design

This artifact is the implementation contract for the follow-on transactional
catalog change. It does not change the current `FolderDB` runtime. The terms
MUST, MUST NOT, and SHOULD are normative.

## Public surface and immutable values

The follow-on implementation adds:

```python
FolderDB.load_catalog_snapshot(timeout_seconds: float = 5.0) -> FolderCatalogSnapshot
```

`FolderCatalogSnapshot` is a frozen, slotted value with `schema`, `version`,
`catalog_id`, `revision`, `last_transaction_id`, `timespec`, `file_identity`, and
`entries`. `FolderCatalogEntry` is a frozen, slotted value with only
`min_index`, `max_index`, `count`, `size`, `linted`, and `lint_time`. `entries`
is a read-only `Mapping[str, FolderCatalogEntry]`; neither it nor its values may
expose the decoded mutable catalog dictionaries. Ticker membership is
`snapshot.entries.keys()`.

`file_identity` contains the canonical catalog path and the stable `stat` tuple
used to read it (`st_dev`, `st_ino`, `st_size`, `st_mtime_ns`, and
`st_ctime_ns`). Snapshot generation identity is the complete
`(canonical_root, catalog_id, revision, file_identity)` tuple. Numeric revisions
are compared only within one catalog lineage and never used as a global
newer-than test. A Git revert may therefore restore an older, correct identity.

The exception surface is explicit: `CatalogBusyError` for bounded lock/read
contention, `UnsupportedCatalogVersionError` for a recognized newer format,
`CatalogFilesystemError` when required locking/replacement/sync semantics are
unavailable, and `CatalogRecoveryError` when locked reconciliation cannot
establish a trustworthy state.

`get_dbmeta()` keeps its current meaning: it explicitly discovers owners,
repairs indexes as needed, reconciles all facts, publishes the catalog, refreshes
the legacy projection, and returns the mutable legacy dictionary. Callers that
want cheap managed-state metadata use `load_catalog_snapshot()` instead.

## Root control namespace and tracked boundary

There is exactly one control directory at the canonical `FolderDB` root:

```text
<folderdb-root>/.jsonldb/
  .gitignore
  catalog.json
  pending.json
  writer.lock
```

Hierarchy directories never contain a catalog. The tracked `.gitignore` has
exactly these ignore rules:

```gitignore
pending.json
writer.lock
*.tmp
```

`.jsonldb/.gitignore` and `catalog.json` are version content. `pending.json`,
`writer.lock`, and same-directory `*.tmp` files are not. Every owner/auxiliary
discovery walk, hierarchy move, quarantine scan, and empty-directory cleanup
MUST explicitly prune the root `.jsonldb` directory. `clear_folder()` preserves
the directory and both tracked files.

Catalog and pending files are compact `orjson` objects, not JSONL; neither has an
`.idx`. Publication serializes with sorted keys, writes to a unique
same-directory `*.tmp`, flushes and `fsync`s that file, calls `os.replace`, then
`fsync`s the `.jsonldb` directory. Clearing pending unlinks it and syncs the
directory. A platform that cannot provide atomic same-directory replacement,
an advisory exclusive lock, and directory durability fails with
`CatalogFilesystemError`; it MUST NOT silently weaken the protocol.

## Version-1 envelopes

`catalog.json` has this exact top-level shape; no other top-level or entry fields
are accepted in version 1:

```json
{
  "schema": "jsonldb.folder-catalog",
  "version": 1,
  "catalog_id": "550e8400-e29b-41d4-a716-446655440000",
  "revision": 7,
  "last_transaction_id": "819c2f70-51a9-4ed0-87e7-bfc4d0f26f12",
  "timespec": "microseconds",
  "entries": {
    "region.ticker": {
      "min_index": "2026-01-01T00:00:00.000001",
      "max_index": "2026-01-02T00:00:00.000001",
      "count": 2,
      "size": 186,
      "linted": true,
      "lint_time": "2026-08-04T12:00:00"
    }
  }
}
```

`catalog_id` and `last_transaction_id` are canonical UUID strings. `revision`
is an integer (not a boolean), starts at 1, and increases exactly once for every
forward managed catalog publication in that lineage. `timespec` is `seconds` or
`microseconds`. Ticker keys are non-empty logical names accepted by the existing
FolderDB name/hierarchy rules and cannot be absolute, contain a path separator or
NUL, or resolve into `.jsonldb`. Entries are emitted in logical-name order.

`count` and `size` are non-negative integers (not booleans), `linted` is a
boolean, and `lint_time` is a string. Boundaries are raw serialized strings or
null. `count == 0` requires both boundaries to be null; `count > 0` requires both
strings and `min_index <= max_index`. Validation is all-or-nothing and occurs
before any decoded value enters the process cache.

`pending.json` has exactly:

```json
{
  "schema": "jsonldb.folder-catalog-pending",
  "version": 1,
  "transaction_id": "819c2f70-51a9-4ed0-87e7-bfc4d0f26f12",
  "base_catalog_id": "550e8400-e29b-41d4-a716-446655440000",
  "base_revision": 6,
  "target_revision": 7,
  "scope": "tickers",
  "tickers": ["region.ticker"]
}
```

The IDs and integer rules match the catalog. `target_revision` equals
`base_revision + 1`. `scope` is `tickers` or `full`. Ticker scope requires a
non-empty, sorted, duplicate-free valid-name list. Full scope requires an empty
list because no subset is trusted.

The filesystem is an untrusted input boundary. Envelope validation establishes
only structural trust; it does not authenticate arbitrary raw filesystem edits.
Managed writers supply the consistency guarantee. Errors include paths and
state categories but never decoded record contents.

## Healthy load, freshness, and process cache

The process registry is guarded by an in-process mutex and indexed by canonical
root (`realpath(abspath(root))`, with platform case normalization) and
`catalog_id`. It retains only the current snapshot per root; older immutable
objects remain valid for existing callers but are not returned after a changed
file identity. Independent `FolderDB` instances therefore share the same object
for the same root and generation.

A healthy `load_catalog_snapshot()` does the following with bounded retries:

1. Verify `pending.json` is absent.
2. `stat` `catalog.json`. If its complete file identity matches the cached
   generation, verify pending is still absent and return the same object.
3. Otherwise read only `catalog.json`, `stat` it again, and retry if identity
   changed.
4. Decode and completely validate the envelope, freeze entries, verify pending
   is still absent, install the generation in the registry, and return it.

It never reads `db.meta`, `h.meta`, `config.meta`, any JSONL owner, or any
per-ticker `.idx`, and never calls `get_file_list()`. A managed `os.replace`
changes the observed file identity, so another process notices a commit through
constant-size path checks and reloads only the compact catalog. A raw actor that
preserves the full `stat` identity is outside the managed guarantee and requires
explicit reconciliation.

`FolderDB.__init__()` in the follow-on implementation MUST stop using root-folder
mtime as permission to reconcile and MUST defer legacy migration/recovery until
snapshot load or `get_dbmeta()`. Existing hierarchy/config reads may remain, but
timespec disagreement cannot trigger owner/index scans on the healthy snapshot
path. The catalog timespec is authoritative for snapshot consumers; projection
disagreement is repaired during explicit reconciliation or pending recovery.

## Folder writer ownership and publication

All managed catalog-related mutations acquire one exclusive lock on the
persistent `.jsonldb/writer.lock` file. POSIX uses `fcntl.flock`; Windows uses a
one-byte `msvcrt.locking` adapter. The target is opened/created but never removed
or treated as authority. Acquisition uses `time.monotonic()` with a default
five-second timeout and 10 ms backoff capped at 100 ms. Healthy readers never
take this lock.

Writer-capable processes may coexist, but only the lock holder may create or
clear pending state, mutate managed owners/indexes/companions/configuration,
publish either metadata representation, increment a revision, clear data, or
perform a managed Git revert. Batched public calls use one transaction and one
revision, not one nested transaction per ticker. Re-entrant mutation on the same
`FolderDB` context joins the active transaction; a second independent active
transaction is rejected.

The locked sequence is:

1. Recover any prior pending record and load/reconcile a trustworthy base.
2. Compute and validate the exact affected ticker set; use full scope whenever a
   subset cannot prove completeness.
3. Atomically publish pending with a fresh transaction ID and target revision.
4. Mutate owners, derived indexes, opaque companions, hierarchy/config files,
   and the legacy `db.meta` projection.
5. Recompute affected catalog entries from the final stable indexes (or all
   entries for full scope), build the complete next envelope, and durably replace
   `catalog.json`. This catalog replacement is the commit point.
6. Durably clear pending, install or invalidate the process cache, and release
   the lock.

An exception before the commit point leaves pending in place. An exception after
the commit point also leaves enough information for recovery: matching
`last_transaction_id`, target revision, and base lineage prove the catalog
publication committed even if pending cleanup did not.

## Recovery and compatibility outcomes

| Observed state | Required outcome under the writer lock |
| --- | --- |
| No pending; valid stable v1 catalog | Return/reload the immutable snapshot; no owner discovery. |
| Pending exists and another process owns the lock | Retry until the bounded timeout, then raise `CatalogBusyError`. |
| Valid pending; catalog has its target revision and transaction ID | Treat publication as committed, clear pending, invalidate/reload cache. |
| Valid ticker pending; catalog is still the declared base | Reconcile exactly the declared ticker set, publish the target revision, refresh projection, clear pending. |
| Valid full pending, malformed pending, or any identity combination not proven safe | Perform full reconciliation; publish a new lineage when the old lineage is untrustworthy. |
| Catalog missing, corrupt, wrong-schema, older/unknown supported state, or internally inconsistent | Full owner/index reconciliation; publish a fresh catalog ID at revision 1 and a matching projection. |
| Recognized schema with a version newer than 1 | Raise `UnsupportedCatalogVersionError`; never overwrite or downgrade it. |
| Catalog changes during read or pending appears before return | Discard decoded data and retry/recover; never return the candidate. |
| Required lock, replace, or sync guarantee unavailable | Raise `CatalogFilesystemError`; do not claim a snapshot is current. |

The one-time path for a legacy database, clone, or copy lacking `.jsonldb` is the
same locked full reconciliation. It creates the control directory and ignore
file, derives entries from authoritative owners/indexes, creates a new lineage at
revision 1, and writes the legacy projection. It receives the fast path only
after durable publication completes.

`db.meta` remains a flat JSONL compatibility projection. Each entry reconstructs
`name` and the current absolute `path`, then copies the six raw catalog facts.
Absolute paths and redundant names never enter the catalog. Fast loads do not
read or validate the projection. `get_dbmeta()` always performs explicit full
reconciliation, preserving its out-of-band-edit detection and current mutable
return type.

Direct low-level changes beneath a managed root, raw file copies/removals,
manual index or metadata edits, and direct Git operations are out of band. They
are recognized by explicit `get_dbmeta()` reconciliation, not by the fast API.
Structural catalog validation MUST NOT be described as discovering or
authenticating these edits.

## Clear and Git lifecycle

`clear_folder(force=True)` performs one full-scope transaction, removes managed
owners, indexes, opaque companions, hierarchy/config files, invalid quarantine
content, and legacy metadata while pruning `.jsonldb`, then publishes an empty
next revision and an empty `db.meta`. Empty-folder cleanup cannot remove the
control directory.

`FolderDB.commit()` acquires writer ownership, completes recovery, requires no
pending mutation, and commits a stable catalog with the database. It does not
increment the revision merely for committing. The tracked control ignore file
keeps pending, lock, and temp state out of Git even though the current Git helper
adds a quoted `*` pathspec.

`FolderDB.revert()` acquires writer ownership, recovers first, publishes an
ignored full-scope pending record, and performs the reset while still holding the
lock. It then fully reconciles restored owners/indexes with the restored catalog.
If they match, it preserves the historical catalog ID/revision and regenerates
the legacy projection; if the catalog is absent or mismatched, it publishes a
new lineage at revision 1. It clears pending only after the restored state is
durable. Because pending is ignored/untracked, interruption remains visible
across the reset.

## Current call-site and mutation responsibility inventory

| Current path | Current role | Follow-on responsibility |
| --- | --- | --- |
| `FolderDB.__init__` | Reads hierarchy/config; folder-mtime staleness can call `build_dbmeta`; timespec healing may scan every index. | Create/defer legacy control state; never owner-walk or index-load for a healthy catalog; invalidate/reconcile only under the rules above. |
| `FolderDB.__str__` | Reads the flat `db.meta` projection. | Remain a compatibility consumer or format an immutable snapshot without exposing mutable entries. |
| `get_file_list`, `_get_aux_files`, `_find_orphan_aux_files` | Discover owners/companions recursively. | Explicitly prune `.jsonldb`; owner discovery is allowed only for reconciliation and data APIs that explicitly request inventories. |
| `_make_meta_entry`, `build_dbmeta`, `get_dbmeta` | Load ticker indexes and construct/reconcile the complete legacy view. | Become locked reconciliation primitives; `get_dbmeta` stays the public explicit full-repair surface and publishes both catalog and projection. |
| `update_dbmeta`, `delete_dbmeta` | Incrementally mutate only the legacy projection. | Internal transaction steps only; publish/update/delete the matching catalog entry in the same writer transaction. |
| `overwrite_df`, `overwrite_dict` and plural wrappers | Replace/create owners, indexes, and invalidate companions; then update metadata. | Ticker-scoped transaction; plural calls use one sorted affected set and one publication. |
| `upsert_df`, `upsert_dict` and plural wrappers | Create/update owners and indexes; then update metadata. | Ticker-scoped transaction and one catalog entry refresh per affected ticker; batch once. |
| `replace_df_range` | Atomically replaces owner/index range and refreshes metadata. | Ticker-scoped transaction surrounding pending, existing staged replacement, projection, and catalog commit. |
| `delete_file_keys` | Mutates owner/index and refreshes metadata. | Ticker-scoped transaction; remove membership if the future policy removes an empty owner, otherwise publish zero-boundary facts. |
| `delete_file_range`, `delete_range` | Mutate owner/index but currently do not refresh `db.meta`. | Ticker/batch transaction and mandatory entry refresh; batch publishes once. |
| `delete_file` | Removes owner/index/aux but currently leaves stale metadata until reconciliation. | Ticker-scoped transaction that deterministically removes membership and projection before catalog commit. |
| `clear_folder` | Deletes visible managed data and rebuilds empty metadata. | Full transaction preserving `.jsonldb` and publishing one empty next revision. |
| `write_aux`, `remove_aux`, `_move_jsonl_family` | Mutate or move opaque owner-associated state. | Run inside ticker/full writer ownership; aux-only operations still publish one managed revision even when the six facts are unchanged. |
| `build_hmeta`, `build_configmeta` | Publish hierarchy/timespec compatibility configuration. | Internal full-scope transaction steps; catalog `timespec` and projection/config state commit together. |
| `lint_db` | Lints/repairs owners/indexes, reports orphans, and rebuilds metadata. | Full-scope reconciliation transaction and one complete publication. |
| `lint_hierarchy` | Moves owners/indexes/aux, quarantines invalid tickers, deletes directories, and rebuilds metadata/config. | Full-scope transaction; prune control namespace and publish only after all moves/config are stable. |
| `reprocess_invalid_tickers` | Moves newly valid owner families and rebuilds metadata. | Full scope unless the complete moved set is known before pending publication; otherwise sorted ticker scope and one publication. |
| `create_folder`, `_get_or_create_file_path`, `delete_empty_folders` | Create/remove hierarchy directories as child steps. | Participate in the caller's writer boundary; never create a catalog below root or remove root control state. |
| `FolderDB.commit`, `FolderDB.revert`, `jsonldb.vercontrol` | Commit all paths or hard-reset Git state; revert invalidates aux first. | Managed wrappers follow the lifecycle above. Direct `vercontrol` calls are documented out-of-band and require reconciliation. |
| `jsonlfile.save_jsonl`, `update_jsonl`, `delete_jsonl`, `replace_jsonl_range` and `jsonldf` wrappers | Low-level owner/index mutation with no FolderDB/catalog context. | Remain usable low-level APIs; direct calls under a managed root are out-of-band and become visible only after explicit reconciliation. |
| `jsonlfile.ensure_index_exists`, `load_index`, `build_jsonl_index`, `lint_jsonl` | Create/repair derived indexes during reads or maintenance. | Retain generation-safe self-healing. Read-side index repair does not claim raw owner edits in the catalog; explicit reconciliation republishes facts. |
| `jsonlfile.write_aux`, `remove_aux` | Low-level opaque companion mutation. | FolderDB wrappers transact; direct low-level use is out-of-band. |
| `visual.py` metadata-adjacent reads | Calls `get_file_list` and loads ticker indexes for visualization. | Remains an explicit data/index consumer; it is not the fast metadata path. |
| Current focused tests | Call `get_dbmeta()` to verify reconciled counts/bounds and reopen behavior. | Continue exercising compatibility reconciliation; add separate snapshot structural tests in the implementation Assignment. |
| OceanData `FolderDBReader.read_ticker_list` / `read_ticker_max_date` | Calls `get_dbmeta()` for membership/boundaries. | Future OceanData adoption consumes one immutable snapshot; no JSONLDB semantic change in this Assignment. |
| OceanData `FolderCacheAPI` and `LazyFolderCacheAPI._initialize_persisted_state` | Reconcile and convert every ticker's max index during construction. | Hydrate from one passed/shared snapshot in the later OceanData Assignment. |
| OceanData cache `read_ticker_list` and `DirectoryData.update_ticker_map` | Reconcile again for cross-process ticker sync and duplicate validation. | Compare compact catalog identity, reload snapshot on change, and reuse its keys for the initial map. |
| Separate OceanData `FolderDBReader` / `FolderDBWriter` instances | Construct independent `FolderDB` objects for one folder. | Reuse the process registry by canonical root and catalog identity; optional adapter context sharing remains an OceanData decision. |

## Benchmark and next-Assignment structural target

`jsonldb/benchmarks/catalog_snapshot_baseline.py` creates only a synthetic temporary
database. Its default 256 tickers (four records each) are large enough that one
owner walk and 256 ticker index loads are unambiguous without a timing cutoff.
Setup builds current owners/indexes, the current `db.meta`, and a proposed compact
envelope outside measured phases. Instrumentation counts `FolderDB.get_file_list`
calls and `jsonlfile.load_index` calls whose owner ends in `.jsonl`.

It reports elapsed wall time and structural counts for a cold compact envelope
load, current `get_dbmeta()` full reconciliation, current one-entry
`update_dbmeta()`, and repeated unchanged envelope reads. The standalone
benchmark reader is a measurement model, not a production API. Tests assert call
counts and identity reuse, never timing. The transactional implementation
Assignment must turn the recorded target into production evidence: a healthy
`load_catalog_snapshot()` performs zero owner-tree walks and zero per-ticker
`.idx` loads.
