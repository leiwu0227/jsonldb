# DirectoryData startup metadata-catalog refactor handoff

## Why this note exists

OceanData Assignment 00115 measured startup and shutdown for the current
105-mount development dataset at:

`/Users/leiwu/code/oceanwave/hubs/datasets/dev-20260731/dd_config.json`

That baseline established that `DirectoryData.from_config()` spends most of its
time constructing mounts. A subsequent read-only smoke test instrumented the
JSONLDB calls inside that construction against the same real dataset. The test
confirms that OceanData currently reconciles JSONLDB metadata by loading every
ticker index twice during startup.

This note hands the root problem to JSONLDB. It proposes a revisioned,
crash-aware metadata catalog that is cheap to load during normal managed
operation, while retaining an explicit full reconciliation path for recovery
and out-of-band changes.

No implementation is authorized by this note.

## Provenance and measurement boundaries

The durable OceanData baseline is recorded in:

`lib/oceandata/.specdev/assignments/00115_establish-a-reproducible-read-only-directorydata/`

Its three fresh-process trials reported:

- 105 `lazy_cached`, `READ_WRITE` mounts;
- 18,176 unified tickers;
- 37,485 files in the source/dataset metadata snapshot;
- `DirectoryData.from_config()` median 28.090 seconds, range
  27.654–31.034 seconds; and
- `DirectoryData.shutdown()` median 69.836 seconds, range
  66.466–70.835 seconds.

The follow-up root-cause smoke test used a fresh Python process, a temporary
copy of `dd_config.json`, all 105 real mount paths, warm filesystem caches, and
disabled verbose logging. It instrumented existing method boundaries without
changing product code. It requested stop for all lazy workers before final
cleanup because shutdown was not the subject of this smoke test. A complete
size/mtime/mode digest of all 37,485 dataset files was identical before and
after.

The smoke test's absolute startup time was 11.278 seconds. That number is not a
replacement for the 28.090-second representative baseline because cache and
logging conditions differed. The relative call counts and nested phase timings
are the evidence relevant to this handoff.

## Real-data smoke-test results

| Operation | Calls | Aggregate seconds | Maximum one call |
| --- | ---: | ---: | ---: |
| `FolderData._create_api()` | 105 | 7.144673 | 0.385117 |
| `FolderDB.get_dbmeta()` | 210 | 7.034751 | 0.300149 |
| lazy persisted-state initialization | 105 | 7.013037 | 0.382287 |
| `FolderDB._make_meta_entry()` | 36,352 | 6.667470 | 0.003124 |
| `jsonlfile.load_index()` | 36,352 | 6.000979 | 0.003076 |
| `DirectoryData.update_ticker_map()` | 1 | 2.249652 | 2.249652 |
| generator initialization | 105 | 1.438941 | 0.401563 |
| retriever initialization | 105 | 0.418962 | 0.037198 |
| `FolderDB.get_file_list()` | 210 | 0.223465 | 0.011069 |
| `FolderDB.__init__()` | 210 | 0.124063 | 0.003808 |
| metadata `select_jsonl()` | 630 | 0.097025 | 0.002147 |
| `db.meta` `load_jsonl()` | 210 | 0.029815 | 0.001033 |
| lazy flush-worker start | 105 | 0.006157 | 0.000100 |

Nested timings overlap and must not be added indiscriminately. The decisive
facts are the exact counts:

- 18,176 tickers produced 36,352 `_make_meta_entry()` calls;
- the same 36,352 calls loaded the full `.idx` companion; and
- `get_dbmeta()` ran 210 times, exactly twice per mount.

Directory traversal itself consumed only about 0.223 seconds. Loading the
stored `db.meta` files consumed only about 0.030 seconds. The dominant work was
opening and parsing every ticker's `.idx`, reconstructing min/max/count/size,
and repeating that operation for the initial ticker map.

Starting 105 flush workers took only about 6 milliseconds. Lazy worker startup
is therefore not the cause of this startup problem, although the sleeping
workers are independently implicated in the measured shutdown latency.

## Current call path

The startup path is effectively:

```text
DirectoryData.from_config
  -> FolderData for each of 105 mounts
    -> LazyFolderCacheAPI
      -> separate FolderDBReader and FolderDBWriter
        -> separate FolderDB objects
      -> _initialize_persisted_state
        -> reader.folderdb.get_dbmeta()
          -> get_file_list()
          -> _make_meta_entry() for every ticker
            -> load_index() for every ticker
        -> pd.to_datetime(max_index) one ticker at a time

DirectoryData.__init__
  -> update_ticker_map()
    -> read_ticker_list() for every mount
      -> reader.folderdb.get_dbmeta()
        -> repeat full ticker/index reconciliation
```

Relevant source anchors at the time of writing:

- `jsonldb/folderdb.py`: `FolderDB.__init__`, `get_file_list`,
  `_make_meta_entry`, `build_dbmeta`, `get_dbmeta`, and `update_dbmeta`
- `jsonldb/jsonlfile.py`: `ensure_index_exists` and `load_index`
- `lib/oceandata/oceandata/folderdata/data_layer.py`: independent
  `FolderDBReader` and `FolderDBWriter` construction
- `lib/oceandata/oceandata/folderdata/api_impl_layer.py`:
  `LazyFolderCacheAPI._initialize_persisted_state` and `read_ticker_list`
- `lib/oceandata/oceandata/directorydata/core.py`: `update_ticker_map`

## Why the current code reconciles every file

`FolderDB.get_dbmeta()` is not a simple getter. It is a self-healing
reconciliation operation:

1. load stored `db.meta`;
2. discover current JSONL owners;
3. reconstruct every entry from the JSONL owner and `.idx`;
4. add, update, or remove metadata entries; and
5. rewrite `db.meta` when differences are found.

This protects callers from raw files being added, removed, or modified outside
the managed `FolderDB` mutation APIs, and it lets `load_index()` heal a missing,
stale, empty, or corrupt index.

The cost is unnecessary during healthy managed operation. Normal JSONLDB write
paths already call `update_dbmeta()` after owner/index mutation. `FolderDB`
construction also has an existing `db.meta`/folder-mtime staleness check. The
unconditional full reconciliation in every `get_dbmeta()` call therefore pays
recovery cost on the normal read path and pays it twice during OceanData
startup.

## Root architectural direction

### 1. Separate catalog loading from reconciliation

Introduce two explicit concepts rather than one ambiguous `get_dbmeta()`:

- **load snapshot:** read and validate the stored metadata-catalog envelope,
  returning immutable entries without walking owner/index files; and
- **reconcile/repair:** discover owners, validate/rebuild indexes, reconstruct
  entries, and atomically publish a repaired catalog.

Names and compatibility policy should be decided in a JSONLDB Assignment. A
safe migration can add a new fast API first while preserving the current
`get_dbmeta()` behavior for callers that explicitly depend on reconciliation.

The fast snapshot should include a schema version and catalog revision. Loading
it should be proportional to `db.meta` size, not to all JSONL and `.idx` files.

### 2. Make managed writes catalog-authoritative and crash-aware

Every managed mutation that can change owner contents, index boundaries, row
count, size, or ticker membership should update the catalog as part of one
documented transaction protocol.

A candidate protocol is:

1. atomically publish a small pending-mutation record naming affected tickers;
2. mutate the JSONL owner and index using the existing atomic mechanisms;
3. atomically publish updated catalog entries and a new revision; and
4. clear the pending record.

On startup, a missing pending record plus a valid catalog permits the fast
path. A surviving pending record triggers reconciliation of only the affected
tickers when that is provably sufficient, otherwise a full repair. This makes
crash recovery explicit instead of making every healthy startup behave like a
crash-recovery pass.

Concurrent writers require a clear serialization mechanism for catalog entry
updates and revision increments. The implementation must not permit two writers
to lose each other's metadata changes.

### 3. Cache one immutable snapshot per folder generation

Within one process, repeated reads should return the same immutable snapshot
while the catalog file identity/revision is unchanged. A managed write updates
or invalidates that snapshot. A reader observing a different revision reloads
the compact catalog, not the full owner tree.

The snapshot should expose the facts OceanData needs directly:

- ticker names;
- raw min/max index values;
- count and size; and
- catalog revision/schema identity.

It should not expose mutable internal dictionaries.

### 4. Share the folder-level database/catalog context

OceanData currently constructs separate `FolderDB` instances for
`FolderDBReader` and `FolderDBWriter`. A later OceanData change should allow
both adapters to share one folder-level context or at least one immutable
catalog snapshot. Thread-safety and writer serialization must be audited before
sharing mutable state.

### 5. Reuse the snapshot in OceanData startup

After JSONLDB supplies the fast snapshot contract, OceanData should:

- hydrate lazy persisted state from it once;
- avoid per-ticker `pd.to_datetime()` calls where raw normalized boundaries or
  batch/on-demand conversion is sufficient;
- build the initial ticker inventory from `snapshot.entries.keys()`; and
- pass that same inventory to DirectoryData duplicate validation instead of
  calling reconciliation again.

A later DirectoryData manifest may cache the unified ticker map against the
directory-config fingerprint plus every mount's catalog revision. That is an
OceanData concern and should not be embedded into JSONLDB.

## Correctness constraints

- A fast path must never silently accept a known interrupted catalog mutation.
- Missing, corrupt, unsupported-version, or internally inconsistent catalog
  state must fail closed into repair or a clear error.
- Owner/index/catalog publication ordering must preserve the current rule that
  derived data may be absent but must not be stale and presented as current.
- Missing, empty, stale, or corrupt `.idx` self-healing remains available in
  reconciliation and normal indexed reads.
- Ticker addition and deletion must update catalog membership deterministically.
- Cross-process readers must notice committed catalog revisions without walking
  the entire database.
- Concurrent writers must not lose catalog entry updates or revision bumps.
- Raw out-of-band edits require a defined policy: explicit repair, strict mode,
  or a cheap trustworthy dirty signal. They must not be silently called fully
  supported while removing the only detection mechanism.
- `db.meta` recovery must preserve existing lint fields and time-precision
  behavior.
- Per-ticker `.jsonl.aux` companions remain opaque owner-associated state. Do
  not overload `.aux` as the folder-level catalog revision or transaction
  journal.
- Existing JSONLDB callers need a documented compatibility path; do not quietly
  reinterpret `get_dbmeta()` without auditing its uses.

## Recommended implementation sequence

### Assignment A — JSONLDB catalog contract and benchmark

- Add focused call-site inventory and microbenchmarks for stored-catalog load,
  full reconciliation, one-entry update, and repeated snapshot reads.
- Define the immutable snapshot, schema/revision envelope, compatibility
  behavior, and out-of-band-edit policy.
- Preserve a reproducible fixture large enough to expose accidental O(N index
  loads) on the fast path.

### Assignment B — JSONLDB transactional catalog implementation

- Implement fast snapshot loading and explicit reconciliation.
- Add atomic revision publication and pending-mutation recovery.
- Route every owner/ticker mutation through the catalog protocol.
- Prove concurrency and crash/interruption behavior with focused tests.
- Prove the fast path performs zero owner-tree walks and zero `.idx` loads.

### Assignment C — OceanData snapshot adoption

- Share or pass one snapshot through API construction.
- Hydrate tracker state without repeated index reconciliation.
- Reuse ticker inventory for initial duplicate validation.
- Optimize max-index conversion without weakening datetime semantics.
- Re-run the 105-mount three-process baseline and compare with Assignment
  00115.

Only after these steps should bounded parallel mount construction be evaluated.
Parallelizing the current duplicate index scans would add lifecycle and
determinism risk while preserving the root waste.

## Suggested acceptance evidence

- Unit tests distinguishing fast snapshot load from explicit reconciliation.
- Tests for missing/corrupt/old-version catalog fallback.
- Tests for add, update, replace-range, delete-range, delete-owner, hierarchy
  move, lint, and clear-folder catalog behavior.
- Interruption tests at every owner/index/catalog/pending-record boundary.
- Concurrent-writer tests proving no lost metadata updates.
- Cross-process tests proving revision-based reload without full reconciliation.
- Spy-based tests proving the healthy fast path calls neither `get_file_list()`
  nor `load_index()`.
- Before/after real-data evidence reporting call counts as well as wall time.
- An unchanged-data digest around the real-data benchmark.

The primary performance acceptance should be structural: one compact catalog
load per mount generation, zero per-ticker index loads on a healthy startup,
and no second reconciliation for DirectoryData's initial ticker map. Wall-clock
improvement should be recorded but not used to trade away recovery semantics.

## Open decisions for Brainstorm

- Whether to preserve `get_dbmeta()` as the reconciling API and add a new fast
  snapshot method, or introduce an explicit refresh policy with conservative
  defaults.
- The catalog envelope/file format and how to version it without confusing
  existing `db.meta` readers.
- The smallest safe pending-mutation representation for single and batch
  writes.
- Whether raw out-of-band JSONL edits are supported in normal mode, supported
  only by explicit repair, or detected by a separate dirty-generation signal.
- Whether shared reader/writer context belongs in JSONLDB itself or remains an
  OceanData adapter decision.
- How to preserve inexpensive cross-process freshness without turning every
  metadata read back into O(number of tickers).

## Expected payoff

The smoke test shows that compact `db.meta` loading is already cheap—about
0.030 seconds across all 210 calls—while repeated index loading costs about
6.001 seconds under warm conditions. Eliminating duplicate reconciliation and
per-ticker conversion should remove a material fraction of startup without
parallelism. The representative 28.090-second baseline must be rerun after the
root changes; this note does not claim a final wall-clock result in advance.
