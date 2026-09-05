# Adhoc AH-20260905T020049719Z-7874

- Scope: Recover missing or invalid hierarchy controls from visible table paths: infer a consistent delimiter and safest observed depth, reconcile mixed layouts without overwriting collisions, and log recovery evidence
- Title: Recover hierarchy settings from table layout
- Started: 2026-09-05T02:00:49.719Z
- Completed: 2026-09-05T02:10:30.390Z
- Starting working tree: Clean.

## Outcome

Validated hierarchy controls on open and added automatic recovery for missing or invalid controls from visible table paths. Recovery infers the shortest delimiter matching directory/filename prefixes and the shallowest observed table-directory depth, falls back to flat for root-level or empty layouts, honors explicit depth, preflights data/index/directory collisions and scan failures, preserves table bytes and hidden trees, moves indexes with tables, publishes controls after moves, rebuilds statistics, and logs inferred settings and evidence. Contradictory prefixes and unsafe paths fail before moving tables; interrupted moves and control publication are retryable. Preserved valid-control behavior and explicit hierarchy creation/quarantine behavior. README documents recovery and its conflict behavior. Verification passed 26 recovery tests plus 75 focused existing regressions, git diff --check, Python 3.8 syntax checks for changed Python files, and the 1250-line folderdb cap (1197 lines).

## Focused workflow coexistence

No focused Assignment or Mission coexistence was recorded.

## Delivery path facts

### Requested adopted paths

None.

### Committed paths

- `.specdev/adhoc/2026-09/AH-20260905T020049719Z-7874_recover-missing-or-invalid-hierarchy-controls-fr.md`
- `README.md`
- `jsonldb/folderdb.py`
- `unit_tests/test_hierarchy_recovery.py`

### Rejected paths

None.

### Remaining owned paths

None.

## Verification summary

No manual verification summary was supplied.

## Verification attempt history

- **hierarchy-recovery: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_hierarchy_recovery.py` (365 ms, working-tree@e1dfdb4b0b66761d569179d9332c26eda2361ef6)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                    [100%]stdout: 
    22 passed in 0.25s
- **folder-recovery-regressions: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_folderdb.py unit_tests/test_metadata_upkeep.py unit_tests/test_metadata_slot_migration.py unit_tests/test_reports.py unit_tests/test_durability_atomicity.py unit_tests/test_roadmap_compliance.py` (1185 ms, working-tree@e1dfdb4b0b66761d569179d9332c26eda2361ef6)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 96%]
    stdout: .stdout: .stdout: .stdout:                                                                       [100%]stdout: 
    75 passed in 1.05s
- **hierarchy-recovery: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_hierarchy_recovery.py` (377 ms, working-tree@e1dfdb4b0b66761d569179d9332c26eda2361ef6)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                [100%]stdout: 
    26 passed in 0.26s
- **folder-recovery-regressions: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_folderdb.py unit_tests/test_metadata_upkeep.py unit_tests/test_metadata_slot_migration.py unit_tests/test_reports.py unit_tests/test_durability_atomicity.py unit_tests/test_roadmap_compliance.py` (1120 ms, working-tree@e1dfdb4b0b66761d569179d9332c26eda2361ef6)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 96%]
    stdout: .stdout: .stdout: .stdout:                                                                       [100%]stdout: 
    75 passed in 0.99s
- **diff-hygiene: passed.** `git diff --check` (12 ms, working-tree@e1dfdb4b0b66761d569179d9332c26eda2361ef6)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    (no output)

## Current acceptance evidence

- **hierarchy-recovery: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_hierarchy_recovery.py` (377 ms, working-tree@e1dfdb4b0b66761d569179d9332c26eda2361ef6)
- **folder-recovery-regressions: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_folderdb.py unit_tests/test_metadata_upkeep.py unit_tests/test_metadata_slot_migration.py unit_tests/test_reports.py unit_tests/test_durability_atomicity.py unit_tests/test_roadmap_compliance.py` (1120 ms, working-tree@e1dfdb4b0b66761d569179d9332c26eda2361ef6)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 96%]
    stdout: .stdout: .stdout: .stdout:                                                                       [100%]stdout: 
    75 passed in 0.99s
- **diff-hygiene: passed.** `git diff --check` (12 ms, working-tree@e1dfdb4b0b66761d569179d9332c26eda2361ef6)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    (no output)

## Structured verification

    {
      "version": 1,
      "path_facts": {
        "requested": [],
        "committed": [
          ".specdev/adhoc/2026-09/AH-20260905T020049719Z-7874_recover-missing-or-invalid-hierarchy-controls-fr.md",
          "README.md",
          "jsonldb/folderdb.py",
          "unit_tests/test_hierarchy_recovery.py"
        ],
        "rejected": [],
        "remaining": []
      },
      "attempt_history": [
        {
          "version": 1,
          "id": "V-001",
          "label": "hierarchy-recovery",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_hierarchy_recovery.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_hierarchy_recovery.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T02:05:44.820Z",
          "completed_at": "2026-09-05T02:05:45.185Z",
          "duration_ms": 365,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@e1dfdb4b0b66761d569179d9332c26eda2361ef6",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                    [100%]stdout: \n22 passed in 0.25s",
            "truncated": false,
            "captured_bytes": 99
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "folder-recovery-regressions",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_folderdb.py unit_tests/test_metadata_upkeep.py unit_tests/test_metadata_slot_migration.py unit_tests/test_reports.py unit_tests/test_durability_atomicity.py unit_tests/test_roadmap_compliance.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_folderdb.py",
            "unit_tests/test_metadata_upkeep.py",
            "unit_tests/test_metadata_slot_migration.py",
            "unit_tests/test_reports.py",
            "unit_tests/test_durability_atomicity.py",
            "unit_tests/test_roadmap_compliance.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T02:06:29.397Z",
          "completed_at": "2026-09-05T02:06:30.583Z",
          "duration_ms": 1185,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@e1dfdb4b0b66761d569179d9332c26eda2361ef6",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 96%]\nstdout: .stdout: .stdout: .stdout:                                                                       [100%]stdout: \n75 passed in 1.05s",
            "truncated": false,
            "captured_bytes": 179
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "hierarchy-recovery",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_hierarchy_recovery.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_hierarchy_recovery.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T02:09:27.549Z",
          "completed_at": "2026-09-05T02:09:27.926Z",
          "duration_ms": 377,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@e1dfdb4b0b66761d569179d9332c26eda2361ef6",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                [100%]stdout: \n26 passed in 0.26s",
            "truncated": false,
            "captured_bytes": 99
          }
        },
        {
          "version": 1,
          "id": "V-004",
          "label": "folder-recovery-regressions",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_folderdb.py unit_tests/test_metadata_upkeep.py unit_tests/test_metadata_slot_migration.py unit_tests/test_reports.py unit_tests/test_durability_atomicity.py unit_tests/test_roadmap_compliance.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_folderdb.py",
            "unit_tests/test_metadata_upkeep.py",
            "unit_tests/test_metadata_slot_migration.py",
            "unit_tests/test_reports.py",
            "unit_tests/test_durability_atomicity.py",
            "unit_tests/test_roadmap_compliance.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T02:09:28.089Z",
          "completed_at": "2026-09-05T02:09:29.209Z",
          "duration_ms": 1120,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@e1dfdb4b0b66761d569179d9332c26eda2361ef6",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 96%]\nstdout: .stdout: .stdout: .stdout:                                                                       [100%]stdout: \n75 passed in 0.99s",
            "truncated": false,
            "captured_bytes": 179
          }
        },
        {
          "version": 1,
          "id": "V-005",
          "label": "diff-hygiene",
          "annotation": null,
          "command": "git diff --check",
          "argv": [
            "git",
            "diff",
            "--check"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T02:10:01.357Z",
          "completed_at": "2026-09-05T02:10:01.369Z",
          "duration_ms": 12,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@e1dfdb4b0b66761d569179d9332c26eda2361ef6",
          "output": {
            "text": "",
            "truncated": false,
            "captured_bytes": 0
          }
        }
      ],
      "acceptance_evidence": [
        {
          "version": 1,
          "id": "V-003",
          "label": "hierarchy-recovery",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_hierarchy_recovery.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_hierarchy_recovery.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T02:09:27.549Z",
          "completed_at": "2026-09-05T02:09:27.926Z",
          "duration_ms": 377,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@e1dfdb4b0b66761d569179d9332c26eda2361ef6",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                [100%]stdout: \n26 passed in 0.26s",
            "truncated": false,
            "captured_bytes": 99
          }
        },
        {
          "version": 1,
          "id": "V-004",
          "label": "folder-recovery-regressions",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_folderdb.py unit_tests/test_metadata_upkeep.py unit_tests/test_metadata_slot_migration.py unit_tests/test_reports.py unit_tests/test_durability_atomicity.py unit_tests/test_roadmap_compliance.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_folderdb.py",
            "unit_tests/test_metadata_upkeep.py",
            "unit_tests/test_metadata_slot_migration.py",
            "unit_tests/test_reports.py",
            "unit_tests/test_durability_atomicity.py",
            "unit_tests/test_roadmap_compliance.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T02:09:28.089Z",
          "completed_at": "2026-09-05T02:09:29.209Z",
          "duration_ms": 1120,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@e1dfdb4b0b66761d569179d9332c26eda2361ef6",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 96%]\nstdout: .stdout: .stdout: .stdout:                                                                       [100%]stdout: \n75 passed in 0.99s",
            "truncated": false,
            "captured_bytes": 179
          }
        },
        {
          "version": 1,
          "id": "V-005",
          "label": "diff-hygiene",
          "annotation": null,
          "command": "git diff --check",
          "argv": [
            "git",
            "diff",
            "--check"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T02:10:01.357Z",
          "completed_at": "2026-09-05T02:10:01.369Z",
          "duration_ms": 12,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@e1dfdb4b0b66761d569179d9332c26eda2361ef6",
          "output": {
            "text": "",
            "truncated": false,
            "captured_bytes": 0
          }
        }
      ]
    }
