# Forecast

<!-- Treat designs as the target. Record absent or incomplete code requirements in dependency order. Ignore code-only features. Use numbered sections, one per gap, below 200 words. Each section should cite the Roadmap design note or notes it is based on. -->

Checked on 2026-09-04 against `master` (package code at 548ada1, design notes at 2a5e160). Every section of the ten published design notes was compared read-only with the implementation.

## 1. Lint does not reclaim interior dead space

Based on `designs/jsonl_file_store/index_integrity_and_lint.md`, sections "Lint guarantees two properties" and "Checking layout".

The note requires an exact layout check with three conditions, the third being that the number of newline bytes in the file equals the index size. The current layout check tests only that the sorted index keys are in order, the first offset is zero, and the last indexed line ends at end of file. A tombstone left by deleting a middle record, or by a growing upsert of the last record, satisfies all of those and is never compacted, on both the fast path and the `force` path. Verified by probe: after deleting the middle of three records, lint leaves the file at its pre-delete size.

Required: add the strictly-increasing-offset and newline-count conditions to the layout check so any failure triggers the rewrite. Pair the change with tests that delete a middle key and grow the last record, each asserting the file shrinks after lint. No index format change; `jsonlfile.py` stays within its 750-line cap.

## Notes with no gaps

- `designs/core_concepts.md`
- `designs/source_code_folder_structure.md`
- `designs/jsonl_file_store.md`
- `designs/dataframe_adapter.md`
- `designs/folder_database.md`
- `designs/folder_database/hierarchical_layout.md`
- `designs/folder_database/metadata_and_timespec.md`
- `designs/version_control.md`
- `designs/visualization.md`

Code-only features are outside this forecast by rule. The opaque companion, catalog system, and range replacement on branch `post-fc22cba` fall in that category until a Roadmap session adds them to the designs.
