# Forecast

<!-- Treat designs as the target. Record absent or incomplete code requirements in dependency order. Ignore code-only features. Use numbered sections, one per gap, below 200 words. Each section should cite the Roadmap design note or notes it is based on. -->

Checked on 2026-09-05 against the current package after removal of Git and
visualization from the target design. Those still-present code-only capabilities
are omitted under the Roadmap superset rule pending Assignment 00045.

## 1. Heal structurally invalid index documents

Based on `designs/core_concepts.md` (The derived index) and
`designs/jsonl_file_store/index_integrity_and_lint.md` (Single loader).

The ordinary index loader accepts any JSON value that parses. A fresh index
containing a list or scalar is returned instead of rebuilt, so indexed writes
can fail with unrelated type errors. Treat every non-object index as corrupt,
rebuild it through the single loader, and retain lint's deeper responsibility
for object indexes whose offsets or key coverage are wrong.

## 2. Keep stale-index rebuilding silent

Based on `designs/jsonl_file_store/index_integrity_and_lint.md` (Single loader).

The index loader currently emits a warning when it rebuilds an index merely
because the data file is newer. The design makes this expected freshness repair
silent while reserving warnings for corrupt states. Rebuild stale indexes
without a warning or persistent integrity finding.

## 3. Preserve unknown metadata-envelope versions during lint

`designs/jsonl_file_store/metadata_slot.md` (Envelope registry, Detection) and
`designs/jsonl_file_store/index_integrity_and_lint.md` (Checking layout).

Standalone lint classifies an unknown envelope version as a slot but then
replaces it with an empty version-1 envelope. This destroys the version and
opaque bytes even though files retain the version that wrote them. Preserve a
well-formed unknown-version slot unchanged while keeping it excluded from rows
and returning no readable record; repair only malformed slots and explicit
folder-width mismatches.

## 4. Emit an indexed canonical empty `db.meta`

Based on `designs/core_concepts.md` (Control and report files) and
`designs/folder_database/metadata_and_timespec.md` (Table statistics).

Opening an empty database writes `db.meta` directly as one blank line and does
not create `db.meta.idx`. The designs make it an intentionally empty table that
reuses the ordinary JSONL format and owns an index. Publish it through the file
store so its bytes and empty index are canonical immediately after open.

## 5. Keep hidden directories outside empty-folder pruning

Based on `designs/folder_database/hierarchical_layout.md` (Discovery under
hierarchy) and `designs/folder_database/integrity_logs.md` (Files).

Empty-directory cleanup walks hidden directories and may remove empty internal
folders belonging to reports or external tools. Hidden trees are outside data
operations. Prune only visible hierarchy directories created for tables, while
leaving every hidden directory and its descendants untouched.

## 6. Reconcile live settings when clearing a database

Based on `designs/folder_database.md` (Delete operations, Design choices),
`designs/folder_database/hierarchical_layout.md` (Configuration), and
`designs/jsonl_file_store/metadata_slot.md` (Width and enablement).

`clear_folder(force=True)` deletes persisted hierarchy and slot configuration
but leaves those settings active on the existing `FolderDB`. A later write can
therefore create a hierarchical slotted table that a reopened database cannot
discover under its now-default settings. After clearing, make live and persisted
configuration describe the same empty database before another operation runs.
