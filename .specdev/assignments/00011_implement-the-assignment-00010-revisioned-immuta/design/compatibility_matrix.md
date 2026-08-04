# Supported compatibility matrix

| Surface | Preserved behavior | Focused evidence |
| --- | --- | --- |
| Imports and signatures | Existing `FolderDB`, low-level modules, method signatures, accepted supported inputs, and valid uncontended exceptions remain; catalog values/errors are additive exports. | Existing tests plus `test_public_signatures_are_additive_and_legacy_projection_is_mutable`. |
| Construction and metadata | Existing folders open without migration steps; construction reads hierarchy/config only and defers owner/index work; `get_dbmeta()` remains a mutable explicit full-healing reconciliation and republishes `db.meta`. | Constructor spies, shared-load test, legacy projection test, existing reopen/metadata tests. |
| JSONL/index/metadata formats | Owner JSONL, `.idx`, `.aux`, `config.meta`, `h.meta`, and flat `db.meta` fields/absolute paths are unchanged; compact `.jsonldb/catalog.json` and its exact ignore boundary are additive. | Existing low-level/range tests, snapshot shape and projection checks. |
| CRUD and batches | DataFrame/dict overwrite/upsert, inclusive range replacement, key/range/file deletion, and plural calls retain data/return behavior; managed publication adds one revision per public batch. | Existing CRUD/range tests and exact batch revision test. |
| Lint, hierarchy, and timespec | Existing lint output/repair, hierarchy naming/moves/quarantine, companion moves, and mixed/uniform precision behavior remain inside full writer ownership. | Existing lint/timespec/aux tests plus catalog reconciliation tests. |
| Clear | `force=False` warning/no-op and `force=True` data cleanup remain; the additive control namespace and empty catalog are preserved. | Existing clear tests and control-preservation test. |
| Git | Direct `jsonldb.vercontrol` remains out of band; `FolderDB.commit()` and `revert()` retain their public behavior while serializing recovery and tracking stable catalog state. | Existing Git tests and focused managed lifecycle checks. |
| Raw/out-of-band edits | Low-level APIs remain usable; explicit `get_dbmeta()` discovers and republishes raw owner/index changes, while snapshot load intentionally does not. | Legacy reconciliation and zero-walk snapshot spies. |

The compatibility boundary excludes private implementation details, unsupported
concurrent-writer races, and transparent fast-path discovery of raw filesystem or
direct Git edits, as frozen by the approved contract.
