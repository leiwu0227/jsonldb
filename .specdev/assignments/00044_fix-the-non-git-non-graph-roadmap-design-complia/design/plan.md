# Implementation plan

## Context

The approved contract and published Roadmap designs are authoritative. Current
inspection confirmed that row parsing accepts non-dictionary values, single-key
lookup couples input serialization to output deserialization, standalone lint
preserves malformed recognized slots, the DataFrame adapter omits forwarding,
and the package inventory omits `metaslot`. The maintained tracked suite is in
`tests/`, while the designed `unit_tests/` path contains only ignored discarded
files and generated residue.

Relevant bounded context:

- `.specdev/project_notes/roadmap/designs/core_concepts.md`
- `.specdev/project_notes/roadmap/designs/jsonl_file_store.md`
- `.specdev/project_notes/roadmap/designs/jsonl_file_store/index_integrity_and_lint.md`
- `.specdev/project_notes/roadmap/designs/jsonl_file_store/metadata_slot.md`
- `.specdev/project_notes/roadmap/designs/dataframe_adapter.md`
- `.specdev/project_notes/roadmap/designs/source_code_folder_structure.md`

**Implementation Guides:** api-security

**Review Guides:** none

## Tasks

1. **T-1 (AC-1, AC-2):** Make the shared JSONL row parser require a
   dictionary-valued record and make every writer validate that shape before
   mutation. Serialize every single lookup key independently of output
   deserialization. Repair recognized malformed standalone slots to a valid
   same-width envelope when possible and remove too-short slot damage during
   canonical rewrite. Keep `jsonlfile.py` at or below 950 lines.
2. **T-2 (AC-3):** Add backward-compatible trailing DataFrame load and lint
   options, forward them exactly, return the file-store lint result, and add
   `metaslot` to the lazy package submodule inventory.
3. **T-3 (AC-1, AC-2, AC-3):** Delete the bounded ignored Assignment 00008
   test files and generated residue, relocate the tracked suite to
   `unit_tests/`, promote the three immutable legacy files to the designed
   top-level names, update pytest discovery, and add focused regressions for
   every changed behavior.
4. **T-4 (AC-4):** Refresh Forecast and Todo to remove completed Mission work
   and retain only verified future work, without modifying any published design
   note. Keep every numbered section below 200 words.
5. **T-5 (AC-1, AC-2, AC-3, AC-4):** Run focused regressions, then the exact
   authorized integrated verification. Record commands, working-tree identity,
   durations, and results in `implementation/progress.json`; complete the
   outcome and worker-result envelope for independent review.
