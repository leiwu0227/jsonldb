# Adhoc AH-20260904T071845945Z-a818

- Scope: Route JSONLDB warning emissions through Python warnings or logging instead of standard-output print calls
- Title: Route warnings through standard channels
- Started: 2026-09-04T07:18:45.945Z
- Completed: 2026-09-04T07:20:57.432Z
- Starting working tree: Clean.

## Outcome

All seven JSONLDB warning emissions in jsonlfile.py and folderdb.py now use module-level logging at WARNING level instead of standard-output print calls; focused tests cover every site, filtering/capture behavior, and empty stdout.

## Focused workflow coexistence

Assignment 00008 remained the focused owner at run assignment-lifecycle-20260729T07073993. Its workflow paths were excluded from this delivery. Post-detour contract revalidation is required before it advances.

## Delivery path facts

### Requested adopted paths

None.

### Committed paths

- `.specdev/adhoc/2026-09/AH-20260904T071845945Z-a818_route-jsonldb-warning-emissions-through-python-w.md`
- `jsonldb/folderdb.py`
- `jsonldb/jsonlfile.py`
- `tests/test_warning_channels.py`

### Rejected paths

None.

### Remaining owned paths

None.

## Verification summary

No manual verification summary was supplied.

## Verification attempt history

- **warning and lint regressions: passed.** `/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m pytest -q tests/test_lint_compaction.py tests/test_warning_channels.py` (329 ms, working-tree@fedf4ec26c6a9495ba21b13812c57efdc908c7ef)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                                    [100%]stdout: 
    6 passed in 0.19s
- **Python compilation: passed.** `/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m py_compile jsonldb/jsonlfile.py jsonldb/folderdb.py tests/test_warning_channels.py` (24 ms, working-tree@fedf4ec26c6a9495ba21b13812c57efdc908c7ef)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    (no output)
- **warning print inventory: passed.** `/bin/sh -c if rg -n "print\\([^\\n]*WARNING|WARNING:" jsonldb/jsonlfile.py jsonldb/folderdb.py; then exit 1; fi` (10 ms, working-tree@fedf4ec26c6a9495ba21b13812c57efdc908c7ef)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    (no output)
- **whitespace: passed.** `git diff --check` (11 ms, working-tree@fedf4ec26c6a9495ba21b13812c57efdc908c7ef)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    (no output)

## Current acceptance evidence

- **warning and lint regressions: passed.** `/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m pytest -q tests/test_lint_compaction.py tests/test_warning_channels.py` (329 ms, working-tree@fedf4ec26c6a9495ba21b13812c57efdc908c7ef)
- **Python compilation: passed.** `/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m py_compile jsonldb/jsonlfile.py jsonldb/folderdb.py tests/test_warning_channels.py` (24 ms, working-tree@fedf4ec26c6a9495ba21b13812c57efdc908c7ef)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    (no output)
- **warning print inventory: passed.** `/bin/sh -c if rg -n "print\\([^\\n]*WARNING|WARNING:" jsonldb/jsonlfile.py jsonldb/folderdb.py; then exit 1; fi` (10 ms, working-tree@fedf4ec26c6a9495ba21b13812c57efdc908c7ef)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    (no output)
- **whitespace: passed.** `git diff --check` (11 ms, working-tree@fedf4ec26c6a9495ba21b13812c57efdc908c7ef)
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
          ".specdev/adhoc/2026-09/AH-20260904T071845945Z-a818_route-jsonldb-warning-emissions-through-python-w.md",
          "jsonldb/folderdb.py",
          "jsonldb/jsonlfile.py",
          "tests/test_warning_channels.py"
        ],
        "rejected": [],
        "remaining": []
      },
      "attempt_history": [
        {
          "version": 1,
          "id": "V-001",
          "label": "warning and lint regressions",
          "annotation": null,
          "command": "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m pytest -q tests/test_lint_compaction.py tests/test_warning_channels.py",
          "argv": [
            "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python",
            "-m",
            "pytest",
            "-q",
            "tests/test_lint_compaction.py",
            "tests/test_warning_channels.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T07:20:36.881Z",
          "completed_at": "2026-09-04T07:20:37.210Z",
          "duration_ms": 329,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@fedf4ec26c6a9495ba21b13812c57efdc908c7ef",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                                    [100%]stdout: \n6 passed in 0.19s",
            "truncated": false,
            "captured_bytes": 98
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "Python compilation",
          "annotation": null,
          "command": "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m py_compile jsonldb/jsonlfile.py jsonldb/folderdb.py tests/test_warning_channels.py",
          "argv": [
            "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python",
            "-m",
            "py_compile",
            "jsonldb/jsonlfile.py",
            "jsonldb/folderdb.py",
            "tests/test_warning_channels.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T07:20:37.400Z",
          "completed_at": "2026-09-04T07:20:37.425Z",
          "duration_ms": 24,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@fedf4ec26c6a9495ba21b13812c57efdc908c7ef",
          "output": {
            "text": "",
            "truncated": false,
            "captured_bytes": 0
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "warning print inventory",
          "annotation": null,
          "command": "/bin/sh -c if rg -n \"print\\\\([^\\\\n]*WARNING|WARNING:\" jsonldb/jsonlfile.py jsonldb/folderdb.py; then exit 1; fi",
          "argv": [
            "/bin/sh",
            "-c",
            "if rg -n \"print\\\\([^\\\\n]*WARNING|WARNING:\" jsonldb/jsonlfile.py jsonldb/folderdb.py; then exit 1; fi"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T07:20:37.619Z",
          "completed_at": "2026-09-04T07:20:37.629Z",
          "duration_ms": 10,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@fedf4ec26c6a9495ba21b13812c57efdc908c7ef",
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
          "started_at": "2026-09-04T07:20:37.812Z",
          "completed_at": "2026-09-04T07:20:37.824Z",
          "duration_ms": 11,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@fedf4ec26c6a9495ba21b13812c57efdc908c7ef",
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
          "label": "warning and lint regressions",
          "annotation": null,
          "command": "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m pytest -q tests/test_lint_compaction.py tests/test_warning_channels.py",
          "argv": [
            "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python",
            "-m",
            "pytest",
            "-q",
            "tests/test_lint_compaction.py",
            "tests/test_warning_channels.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T07:20:36.881Z",
          "completed_at": "2026-09-04T07:20:37.210Z",
          "duration_ms": 329,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@fedf4ec26c6a9495ba21b13812c57efdc908c7ef",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                                    [100%]stdout: \n6 passed in 0.19s",
            "truncated": false,
            "captured_bytes": 98
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "Python compilation",
          "annotation": null,
          "command": "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m py_compile jsonldb/jsonlfile.py jsonldb/folderdb.py tests/test_warning_channels.py",
          "argv": [
            "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python",
            "-m",
            "py_compile",
            "jsonldb/jsonlfile.py",
            "jsonldb/folderdb.py",
            "tests/test_warning_channels.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T07:20:37.400Z",
          "completed_at": "2026-09-04T07:20:37.425Z",
          "duration_ms": 24,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@fedf4ec26c6a9495ba21b13812c57efdc908c7ef",
          "output": {
            "text": "",
            "truncated": false,
            "captured_bytes": 0
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "warning print inventory",
          "annotation": null,
          "command": "/bin/sh -c if rg -n \"print\\\\([^\\\\n]*WARNING|WARNING:\" jsonldb/jsonlfile.py jsonldb/folderdb.py; then exit 1; fi",
          "argv": [
            "/bin/sh",
            "-c",
            "if rg -n \"print\\\\([^\\\\n]*WARNING|WARNING:\" jsonldb/jsonlfile.py jsonldb/folderdb.py; then exit 1; fi"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T07:20:37.619Z",
          "completed_at": "2026-09-04T07:20:37.629Z",
          "duration_ms": 10,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@fedf4ec26c6a9495ba21b13812c57efdc908c7ef",
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
          "started_at": "2026-09-04T07:20:37.812Z",
          "completed_at": "2026-09-04T07:20:37.824Z",
          "duration_ms": 11,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@fedf4ec26c6a9495ba21b13812c57efdc908c7ef",
          "output": {
            "text": "",
            "truncated": false,
            "captured_bytes": 0
          }
        }
      ]
    }
