# Todo

Prepare one SpecDev Mission containing five sequential Assignment children. The Mission targets `master`; Assignment `00008` and `post-fc22cba` are discarded and must not supply code. Preserve existing public-call compatibility and enforce the published caps: 950 lines for `jsonlfile.py`, 1250 for `folderdb.py`, 250 for the new `metaslot.py`, and 150 for the new `reports.py`.

## 1. Harden JSONL and control-file durability

Create an Assignment covering forecast items 2–5. Make range reads tolerate torn lines, replace every index atomically, replace `config.meta` and `h.meta` atomically, and append grown records before blanking their old locations. Require focused normal-path and injected-interruption tests.

## 2. Implement the metadata-slot contract

Create an Assignment covering forecast items 6–7. Implement line-one classification, reserve `_meta`, preserve absolute offsets, support slot reads and ordered writes, enforce fit and legacy-folder rules, and expose the `FolderDB` metadata and `_with_meta` APIs without changing existing calls or construction.

## 3. Add slot-width enablement and migration

Create an Assignment covering forecast item 8. Add `set_meta_slot_bytes`, persist the folder width, validate every record before shrinking, atomically rewrite only tables needing migration, and create new tables with the configured slot. Verify interrupted and refused migrations leave usable data.

## 4. Complete lint detection and repair

Create an Assignment covering forecast item 9. Extend lint to repair slot width and content, torn tails, missing terminal newlines, indexed damage under `force`, and dead space. Preserve metadata records and fast-path behavior while logging removed bytes.

## 5. Add logging and persistent integrity reports

Create an Assignment covering forecast items 1 and 10. Retire all remaining runtime `print` calls, include paths in anomaly records, and write bounded replacement `.jsonldb/integrity.log` and `.jsonldb/lint.log` reports during open and lint. Ordinary reads and writes must not touch these reports.

