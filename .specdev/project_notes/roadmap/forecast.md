# Forecast

<!-- Treat designs as the target. Record absent or incomplete code requirements in dependency order. Ignore code-only features. Use numbered sections, one per gap, below 200 words. Each section should cite the Roadmap design note or notes it is based on. -->

Checked on 2026-09-04 against `master` (design notes at 5560d2a, package code at 79cf618). Every section of the twelve published notes was compared read-only with the implementation. Gaps are listed in the order they should be built; later items assume earlier ones.

## 1. Retire print in favour of logging

Based on `designs/folder_database/integrity_logs.md` (Transport) and `designs/jsonl_file_store.md` (Error policy).

The file store already reports through `logging`. `folderdb.py` still has twenty print calls beside four logger calls, and `vercontrol.py` and `visual.py` print exclusively. The design retires standard output as a rule, and the report files in item 10 are built by capturing logger records, so every remaining print must become a logger call with the file path in the message. No behaviour change beyond the transport.

## 2. Reads never raise on a torn line

Based on `designs/jsonl_file_store.md` (Error policy).

The full load, the index build, and the single-key lookup already guard each parse and skip a torn line. The range read parses each indexed line without a guard, so a torn indexed line raises out of a read. Add the same guard: skip the line, report it through `logging`, return the intact rows.

## 3. Atomic index writes

Based on `designs/jsonl_file_store/index_integrity_and_lint.md` (Every writer finishes with the index) and `designs/jsonl_file_store.md` (Durability model).

The index is written directly at four sites: save, update, delete, and the index build. Each must write to a temporary file and atomically replace, so a reader never observes a torn index and a rebuild racing a writer replaces the file whole. This also retires the empty-index case the loader currently heals, though the healing stays.

## 4. Atomic control-file writes

Based on `designs/folder_database/metadata_and_timespec.md` (Atomic control files).

`config.meta` and `h.meta` are written in place through the ordinary save. A torn `h.meta` makes a hierarchy database fail to open. Both are single-record files written rarely; write them through a temporary file and an atomic replace so a torn control file cannot exist. `db.meta` stays as it is.

## 5. Upsert order for grown records

Based on `designs/jsonl_file_store.md` (In-place update rules) and `designs/jsonl_file_store/metadata_slot.md` (Write path).

Upsert currently blanks the old line of a grown record before appending the new copy, so a crash between the two loses the record. The design appends first and blanks after the record write, with the index last. Reorder the update path now, without the slot, so the slot's write order in item 7 drops in without a second reordering. Include the tests for a crash between append and blank.

## 6. Line-one classification and the reserved key

Based on `designs/jsonl_file_store/metadata_slot.md` (Detection, On-disk form), `designs/jsonl_file_store.md` (Index-driven and sequential paths), and `designs/source_code_folder_structure.md`.

Nothing in the code knows about line one. Create `jsonldb/metaslot.py`, a leaf that imports nothing from the package, holding the envelope registry, the three-way classification, padding, and the in-place slot read and write. Classification: a valid envelope of a known version is a slot with or without a record; a `_meta` object that is not a valid envelope, or an unknown version, is a slot without a record and not a row; anything else is a legacy file. The index build and the full load skip a slot, index offsets stay absolute, and every writer rejects `_meta` as a linekey. Version 1 of the envelope registry is the only version. This item makes slotted files readable before anything writes them.

## 7. Slot read and write with the publish order

Based on `designs/jsonl_file_store/metadata_slot.md` (Contract, Write path, Failure model, Legacy, API) and `designs/folder_database.md` (Data operations).

File store, using `metaslot`: write a padded envelope in place on line one, flush the buffer before it, and touch the index modification time after a slot-only write. `FolderDB`: `read_meta`; `meta=` on the four write calls, `None` keeping and a dict replacing after the rows in the order in-place, append, slot, blank, index; `clear_meta`; `get_dict_with_meta` and `get_df_with_meta` returning a named tuple of `meta` and `rows`, record read first, `None` and an empty container for a missing table. A record-carrying write into a folder without a recorded width is refused, and a record that does not fit is refused with an error naming both sizes. Existing calls and the constructor are unchanged.

## 8. The width operation

Based on `designs/jsonl_file_store/metadata_slot.md` (Width and enablement) and `designs/folder_database/metadata_and_timespec.md` (Settings in `config.meta`).

Add `set_meta_slot_bytes(width)`: record `meta_slot_bytes` in `config.meta`, verify every existing record fits when shrinking and refuse naming the tables, then rewrite every table whose line one is not a slot of that width through a temporary file and an atomic replace. Default 4096. In an enabled folder, save creates new tables with a slot of the folder width. This is the only full-folder rewrite and the only way to enable a folder.

## 9. Lint repairs the slot, torn lines, and companions

Based on `designs/jsonl_file_store/index_integrity_and_lint.md` (Checking fidelity, Checking layout, Fast path and `force`) and `designs/folder_database/integrity_logs.md` (Open observes, lint repairs).

Layout conditions gain: first offset equals the slot length when line one is a slot, newline count equals index size plus one, line one is a slot of the folder width in an enabled folder, and no `.jsonl.aux` sits beside the table. The rewrite carries the slot first, re-padded or created with the record preserved. Lint truncates an unparseable last line, heals a missing trailing newline, deletes orphan companions, and under `force` parses every indexed line and blanks torn ones in place. The cardinality scan subtracts the slot. Each removal is reported with the removed bytes.

## 10. Report files

Based on `designs/folder_database/integrity_logs.md` and `designs/source_code_folder_structure.md`.

Create `jsonldb/reports.py`, a leaf holding the capture handler and the two writers, and `.jsonldb/` in the database folder. Open writes `integrity.log` and `lint_db` writes `lint.log`, each replacing the previous contents: a header with timestamp and writer, then one line per anomaly with kind, file, offset, detail, and removed bytes capped per entry, stopping after a fixed count with an omitted total. `FolderDB` captures logger records from the library during those two operations, filtered to its own folder. Ordinary reads and writes never touch the files. Open never writes to a table.

## Notes with no gaps

- `designs/source_code_folder_structure.md`
- `designs/dataframe_adapter.md`
- `designs/visualization.md`
- `designs/folder_database/hierarchical_layout.md`
- `designs/version_control.md`

Code-only features are outside this forecast by rule. The opaque companion, catalog system, and range replacement on branch `post-fc22cba` remain in that category.
