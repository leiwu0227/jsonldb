# Adhoc AH-20260905T113626205Z-76b1

- Scope: Preserve unknown metadata envelopes during row overwrites and slot resizing; reject undersized slots before mutation and retain explicit replacement/clearing
- Title: Preserve unknown metadata envelopes across writes
- Started: 2026-09-05T11:36:26.205Z
- Completed: 2026-09-05T11:42:23.954Z
- Starting working tree: Clean.

## Outcome

Preserved opaque unknown-version metadata envelopes across row overwrites with meta=None and slot resizing, sharing the preservation logic with lint. Reject undersized slots before table or configuration mutation; retain explicit v1 metadata replacement and clearing. Added 35 parameterized regression cases across dictionary/DataFrame writes, resizing, refusal atomicity, and clearing. Focused metadata, durability, serialization, statistics, cache, DataFrame, and datetime regression checks passed. Source-file limits and git diff --check passed.

## Focused workflow coexistence

No focused Assignment or Mission coexistence was recorded.

## Delivery path facts

### Requested adopted paths

None.

### Committed paths

- `.specdev/adhoc/2026-09/AH-20260905T113626205Z-76b1_preserve-unknown-metadata-envelopes-during-row-o.md`
- `jsonldb/folderdb.py`
- `jsonldb/jsonlfile.py`
- `jsonldb/metaslot.py`
- `unit_tests/test_unknown_metadata_writes.py`

### Rejected paths

None.

### Remaining owned paths

None.

## Verification summary

No manual verification summary was supplied.

## Verification attempt history

- **metadata-envelope-regression: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_unknown_metadata_writes.py unit_tests/test_unknown_metadata_lint.py unit_tests/test_metadata_slot_migration.py unit_tests/test_metaslot.py unit_tests/test_durability_atomicity.py unit_tests/test_byte_serialization.py unit_tests/test_write_statistics.py unit_tests/test_index_cache.py unit_tests/test_dataframe_conversion.py unit_tests/test_datetime_key_reuse.py` (5755 ms, working-tree@01aae2601371615305b756648adf79fabb5b65b3)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    tdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 61%]
    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 77%]
    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 92%]
    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                        [100%]stdout: 
    466 passed in 5.57s
- **source-limits-and-whitespace: passed.** `.specdev/cache/bin/python-runtime-env python3 -c from pathlib import Path; import subprocess; limits = {"jsonldb/jsonlfile.py": 950, "jsonldb/folderdb.py": 1250, "jsonldb/metaslot.py": 250, "jsonldb/jsonldf.py": 150}; counts = {name: len(Path(name).read_bytes().splitlines()) for name in limits}; print(counts); assert all(counts[name] <= limit for name, limit in limits.items()), counts; subprocess.run(["git", "diff", "--check"], check=True)` (41 ms, working-tree@01aae2601371615305b756648adf79fabb5b65b3)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: {'jsonldb/jsonlfile.py': 949, 'jsonldb/folderdb.py': 1188, 'jsonldb/metaslot.py': 138, 'jsonldb/jsonldf.py': 144}

## Current acceptance evidence

- **metadata-envelope-regression: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_unknown_metadata_writes.py unit_tests/test_unknown_metadata_lint.py unit_tests/test_metadata_slot_migration.py unit_tests/test_metaslot.py unit_tests/test_durability_atomicity.py unit_tests/test_byte_serialization.py unit_tests/test_write_statistics.py unit_tests/test_index_cache.py unit_tests/test_dataframe_conversion.py unit_tests/test_datetime_key_reuse.py` (5755 ms, working-tree@01aae2601371615305b756648adf79fabb5b65b3)
- **source-limits-and-whitespace: passed.** `.specdev/cache/bin/python-runtime-env python3 -c from pathlib import Path; import subprocess; limits = {"jsonldb/jsonlfile.py": 950, "jsonldb/folderdb.py": 1250, "jsonldb/metaslot.py": 250, "jsonldb/jsonldf.py": 150}; counts = {name: len(Path(name).read_bytes().splitlines()) for name in limits}; print(counts); assert all(counts[name] <= limit for name, limit in limits.items()), counts; subprocess.run(["git", "diff", "--check"], check=True)` (41 ms, working-tree@01aae2601371615305b756648adf79fabb5b65b3)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: {'jsonldb/jsonlfile.py': 949, 'jsonldb/folderdb.py': 1188, 'jsonldb/metaslot.py': 138, 'jsonldb/jsonldf.py': 144}

## Structured verification

    {
      "version": 1,
      "path_facts": {
        "requested": [],
        "committed": [
          ".specdev/adhoc/2026-09/AH-20260905T113626205Z-76b1_preserve-unknown-metadata-envelopes-during-row-o.md",
          "jsonldb/folderdb.py",
          "jsonldb/jsonlfile.py",
          "jsonldb/metaslot.py",
          "unit_tests/test_unknown_metadata_writes.py"
        ],
        "rejected": [],
        "remaining": []
      },
      "attempt_history": [
        {
          "version": 1,
          "id": "V-001",
          "label": "metadata-envelope-regression",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_unknown_metadata_writes.py unit_tests/test_unknown_metadata_lint.py unit_tests/test_metadata_slot_migration.py unit_tests/test_metaslot.py unit_tests/test_durability_atomicity.py unit_tests/test_byte_serialization.py unit_tests/test_write_statistics.py unit_tests/test_index_cache.py unit_tests/test_dataframe_conversion.py unit_tests/test_datetime_key_reuse.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_unknown_metadata_writes.py",
            "unit_tests/test_unknown_metadata_lint.py",
            "unit_tests/test_metadata_slot_migration.py",
            "unit_tests/test_metaslot.py",
            "unit_tests/test_durability_atomicity.py",
            "unit_tests/test_byte_serialization.py",
            "unit_tests/test_write_statistics.py",
            "unit_tests/test_index_cache.py",
            "unit_tests/test_dataframe_conversion.py",
            "unit_tests/test_datetime_key_reuse.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T11:38:56.334Z",
          "completed_at": "2026-09-05T11:39:02.090Z",
          "duration_ms": 5755,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@01aae2601371615305b756648adf79fabb5b65b3",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 15%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 30%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 46%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 61%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 77%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 92%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                        [100%]stdout: \n466 passed in 5.57s",
            "truncated": false,
            "captured_bytes": 580
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "source-limits-and-whitespace",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -c from pathlib import Path; import subprocess; limits = {\"jsonldb/jsonlfile.py\": 950, \"jsonldb/folderdb.py\": 1250, \"jsonldb/metaslot.py\": 250, \"jsonldb/jsonldf.py\": 150}; counts = {name: len(Path(name).read_bytes().splitlines()) for name in limits}; print(counts); assert all(counts[name] <= limit for name, limit in limits.items()), counts; subprocess.run([\"git\", \"diff\", \"--check\"], check=True)",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-c",
            "from pathlib import Path; import subprocess; limits = {\"jsonldb/jsonlfile.py\": 950, \"jsonldb/folderdb.py\": 1250, \"jsonldb/metaslot.py\": 250, \"jsonldb/jsonldf.py\": 150}; counts = {name: len(Path(name).read_bytes().splitlines()) for name in limits}; print(counts); assert all(counts[name] <= limit for name, limit in limits.items()), counts; subprocess.run([\"git\", \"diff\", \"--check\"], check=True)"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T11:42:14.586Z",
          "completed_at": "2026-09-05T11:42:14.627Z",
          "duration_ms": 41,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@01aae2601371615305b756648adf79fabb5b65b3",
          "output": {
            "text": "stdout: {'jsonldb/jsonlfile.py': 949, 'jsonldb/folderdb.py': 1188, 'jsonldb/metaslot.py': 138, 'jsonldb/jsonldf.py': 144}",
            "truncated": false,
            "captured_bytes": 114
          }
        }
      ],
      "acceptance_evidence": [
        {
          "version": 1,
          "id": "V-001",
          "label": "metadata-envelope-regression",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_unknown_metadata_writes.py unit_tests/test_unknown_metadata_lint.py unit_tests/test_metadata_slot_migration.py unit_tests/test_metaslot.py unit_tests/test_durability_atomicity.py unit_tests/test_byte_serialization.py unit_tests/test_write_statistics.py unit_tests/test_index_cache.py unit_tests/test_dataframe_conversion.py unit_tests/test_datetime_key_reuse.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_unknown_metadata_writes.py",
            "unit_tests/test_unknown_metadata_lint.py",
            "unit_tests/test_metadata_slot_migration.py",
            "unit_tests/test_metaslot.py",
            "unit_tests/test_durability_atomicity.py",
            "unit_tests/test_byte_serialization.py",
            "unit_tests/test_write_statistics.py",
            "unit_tests/test_index_cache.py",
            "unit_tests/test_dataframe_conversion.py",
            "unit_tests/test_datetime_key_reuse.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T11:38:56.334Z",
          "completed_at": "2026-09-05T11:39:02.090Z",
          "duration_ms": 5755,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@01aae2601371615305b756648adf79fabb5b65b3",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 15%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 30%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 46%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 61%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 77%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 92%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                        [100%]stdout: \n466 passed in 5.57s",
            "truncated": false,
            "captured_bytes": 580
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "source-limits-and-whitespace",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -c from pathlib import Path; import subprocess; limits = {\"jsonldb/jsonlfile.py\": 950, \"jsonldb/folderdb.py\": 1250, \"jsonldb/metaslot.py\": 250, \"jsonldb/jsonldf.py\": 150}; counts = {name: len(Path(name).read_bytes().splitlines()) for name in limits}; print(counts); assert all(counts[name] <= limit for name, limit in limits.items()), counts; subprocess.run([\"git\", \"diff\", \"--check\"], check=True)",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-c",
            "from pathlib import Path; import subprocess; limits = {\"jsonldb/jsonlfile.py\": 950, \"jsonldb/folderdb.py\": 1250, \"jsonldb/metaslot.py\": 250, \"jsonldb/jsonldf.py\": 150}; counts = {name: len(Path(name).read_bytes().splitlines()) for name in limits}; print(counts); assert all(counts[name] <= limit for name, limit in limits.items()), counts; subprocess.run([\"git\", \"diff\", \"--check\"], check=True)"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T11:42:14.586Z",
          "completed_at": "2026-09-05T11:42:14.627Z",
          "duration_ms": 41,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@01aae2601371615305b756648adf79fabb5b65b3",
          "output": {
            "text": "stdout: {'jsonldb/jsonlfile.py': 949, 'jsonldb/folderdb.py': 1188, 'jsonldb/metaslot.py': 138, 'jsonldb/jsonldf.py': 144}",
            "truncated": false,
            "captured_bytes": 114
          }
        }
      ]
    }
