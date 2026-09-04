# Forecast

<!-- Treat designs as the target. Record absent or incomplete code requirements in dependency order. Ignore code-only features. Use numbered sections, one per gap, below 200 words. Each section should cite the Roadmap design note or notes it is based on. -->

Checked on 2026-09-05 against the Assignment 00044 candidate based on
`548a174b`. Completed M00001 and Assignment 00044 work is omitted. The remaining
verified gaps are Git-related and outside Assignment 00044.

## 1. Make Git snapshots and reverts cover the whole database

Based on `designs/version_control.md` (Model, Why whole-folder snapshots) and
`designs/folder_database/integrity_logs.md` (Files).

`vercontrol.commit` stages through `repo.index.add("*")`, which does not prove
that deletions and hidden `.jsonldb/` reports enter the snapshot. The design
requires every table, index, control file, and report to be captured together.
Use Git's whole-working-tree staging semantics and verify additions,
modifications, deletions, and hidden report files. `revert` performs a hard
reset but leaves untracked files, so the restored folder can still contain
tables or controls absent from the selected version. Restore the exact
whole-folder snapshot, including removal of later untracked database files.

## 2. Refresh a live FolderDB after revert

Based on `designs/version_control.md` (Why whole-folder snapshots) and
`designs/folder_database.md` (Opening a database, Design choices).

`FolderDB.revert` restores disk state but leaves the existing object's cached
timespec, metadata-slot width, hierarchy settings, and metadata view unchanged.
Subsequent operations can therefore apply post-revert settings to restored
files. After a successful revert, reload the same control state established by
opening the database, without importing Git eagerly or rewriting table data.

## 3. Add the matching version-control test module

Based on `designs/source_code_folder_structure.md` (Where new code goes) and
`designs/version_control.md`.

The canonical suite now has matching modules for every non-Git concern, but
`unit_tests/test_vercontrol.py` is absent. Add it with the Git fixes above so it
proves complete snapshots, exact reverts, error paths, and lazy optional import
behavior using the real Git dependency.
