# Adhoc AH-20260904T074739861Z-4de9

- Scope: Make db.meta upkeep consistent by refreshing metadata after file and range deletion, detecting hierarchical child changes on open, and linting on-disk tables missing from metadata
- Title: Make metadata upkeep consistent
- Started: 2026-09-04T07:47:39.861Z
- Completed: 2026-09-04T07:51:40.894Z
- Starting working tree: Clean.

## Outcome

db.meta now removes deleted tables, refreshes range-deleted table statistics, rebuilds on visible hierarchy child-directory additions and removals, and lint includes on-disk tables absent from metadata; regressions cover these behaviors and prior lint and warning changes.

## Focused workflow coexistence

Assignment 00008 remained the focused owner at run assignment-lifecycle-20260729T07073993. Its workflow paths were excluded from this delivery. Post-detour contract revalidation is required before it advances.

## Delivery path facts

### Requested adopted paths

None.

### Committed paths

- `.specdev/adhoc/2026-09/AH-20260904T074739861Z-4de9_make-db-meta-upkeep-consistent-by-refreshing-met.md`
- `jsonldb/folderdb.py`
- `tests/test_metadata_upkeep.py`

### Rejected paths

None.

### Remaining owned paths

None.

## Verification summary

No manual verification summary was supplied.

## Verification attempt history

- **metadata and prior regressions: passed.** `/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m pytest -q tests` (383 ms, working-tree@c7813bc709175b56117fe890b67b925df8f43173)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                                 [100%]stdout: 
    9 passed in 0.25s
- **Python compilation: passed.** `/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m py_compile jsonldb/folderdb.py tests/test_metadata_upkeep.py` (22 ms, working-tree@c7813bc709175b56117fe890b67b925df8f43173)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    (no output)
- **source size: passed.** `/bin/sh -c test "$(wc -l < jsonldb/folderdb.py)" -le 1000` (5 ms, working-tree@c7813bc709175b56117fe890b67b925df8f43173)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    (no output)
- **whitespace: passed.** `git diff --check` (11 ms, working-tree@c7813bc709175b56117fe890b67b925df8f43173)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    (no output)

## Current acceptance evidence

- **metadata and prior regressions: passed.** `/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m pytest -q tests` (383 ms, working-tree@c7813bc709175b56117fe890b67b925df8f43173)
- **Python compilation: passed.** `/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m py_compile jsonldb/folderdb.py tests/test_metadata_upkeep.py` (22 ms, working-tree@c7813bc709175b56117fe890b67b925df8f43173)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    (no output)
- **source size: passed.** `/bin/sh -c test "$(wc -l < jsonldb/folderdb.py)" -le 1000` (5 ms, working-tree@c7813bc709175b56117fe890b67b925df8f43173)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    (no output)
- **whitespace: passed.** `git diff --check` (11 ms, working-tree@c7813bc709175b56117fe890b67b925df8f43173)
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
          ".specdev/adhoc/2026-09/AH-20260904T074739861Z-4de9_make-db-meta-upkeep-consistent-by-refreshing-met.md",
          "jsonldb/folderdb.py",
          "tests/test_metadata_upkeep.py"
        ],
        "rejected": [],
        "remaining": []
      },
      "attempt_history": [
        {
          "version": 1,
          "id": "V-001",
          "label": "metadata and prior regressions",
          "annotation": null,
          "command": "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m pytest -q tests",
          "argv": [
            "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python",
            "-m",
            "pytest",
            "-q",
            "tests"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T07:51:34.178Z",
          "completed_at": "2026-09-04T07:51:34.562Z",
          "duration_ms": 383,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@c7813bc709175b56117fe890b67b925df8f43173",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                                 [100%]stdout: \n9 passed in 0.25s",
            "truncated": false,
            "captured_bytes": 98
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "Python compilation",
          "annotation": null,
          "command": "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m py_compile jsonldb/folderdb.py tests/test_metadata_upkeep.py",
          "argv": [
            "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python",
            "-m",
            "py_compile",
            "jsonldb/folderdb.py",
            "tests/test_metadata_upkeep.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T07:51:34.706Z",
          "completed_at": "2026-09-04T07:51:34.729Z",
          "duration_ms": 22,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@c7813bc709175b56117fe890b67b925df8f43173",
          "output": {
            "text": "",
            "truncated": false,
            "captured_bytes": 0
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "source size",
          "annotation": null,
          "command": "/bin/sh -c test \"$(wc -l < jsonldb/folderdb.py)\" -le 1000",
          "argv": [
            "/bin/sh",
            "-c",
            "test \"$(wc -l < jsonldb/folderdb.py)\" -le 1000"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T07:51:34.866Z",
          "completed_at": "2026-09-04T07:51:34.871Z",
          "duration_ms": 5,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@c7813bc709175b56117fe890b67b925df8f43173",
          "output": {
            "text": "",
            "truncated": false,
            "captured_bytes": 0
          }
        },
        {
          "version": 1,
          "id": "V-004",
          "label": "whitespace",
          "annotation": null,
          "command": "git diff --check",
          "argv": [
            "git",
            "diff",
            "--check"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T07:51:35.008Z",
          "completed_at": "2026-09-04T07:51:35.019Z",
          "duration_ms": 11,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@c7813bc709175b56117fe890b67b925df8f43173",
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
          "id": "V-001",
          "label": "metadata and prior regressions",
          "annotation": null,
          "command": "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m pytest -q tests",
          "argv": [
            "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python",
            "-m",
            "pytest",
            "-q",
            "tests"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T07:51:34.178Z",
          "completed_at": "2026-09-04T07:51:34.562Z",
          "duration_ms": 383,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@c7813bc709175b56117fe890b67b925df8f43173",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                                 [100%]stdout: \n9 passed in 0.25s",
            "truncated": false,
            "captured_bytes": 98
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "Python compilation",
          "annotation": null,
          "command": "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m py_compile jsonldb/folderdb.py tests/test_metadata_upkeep.py",
          "argv": [
            "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python",
            "-m",
            "py_compile",
            "jsonldb/folderdb.py",
            "tests/test_metadata_upkeep.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T07:51:34.706Z",
          "completed_at": "2026-09-04T07:51:34.729Z",
          "duration_ms": 22,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@c7813bc709175b56117fe890b67b925df8f43173",
          "output": {
            "text": "",
            "truncated": false,
            "captured_bytes": 0
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "source size",
          "annotation": null,
          "command": "/bin/sh -c test \"$(wc -l < jsonldb/folderdb.py)\" -le 1000",
          "argv": [
            "/bin/sh",
            "-c",
            "test \"$(wc -l < jsonldb/folderdb.py)\" -le 1000"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T07:51:34.866Z",
          "completed_at": "2026-09-04T07:51:34.871Z",
          "duration_ms": 5,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@c7813bc709175b56117fe890b67b925df8f43173",
          "output": {
            "text": "",
            "truncated": false,
            "captured_bytes": 0
          }
        },
        {
          "version": 1,
          "id": "V-004",
          "label": "whitespace",
          "annotation": null,
          "command": "git diff --check",
          "argv": [
            "git",
            "diff",
            "--check"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T07:51:35.008Z",
          "completed_at": "2026-09-04T07:51:35.019Z",
          "duration_ms": 11,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@c7813bc709175b56117fe890b67b925df8f43173",
          "output": {
            "text": "",
            "truncated": false,
            "captured_bytes": 0
          }
        }
      ]
    }
