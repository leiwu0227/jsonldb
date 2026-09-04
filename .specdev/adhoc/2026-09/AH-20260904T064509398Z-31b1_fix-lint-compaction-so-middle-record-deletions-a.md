# Adhoc AH-20260904T064509398Z-31b1

- Scope: Fix lint compaction so middle-record deletions and last-record growth reclaim dead space
- Title: Fix lint dead-space compaction
- Started: 2026-09-04T06:45:09.398Z
- Completed: 2026-09-04T06:49:38.284Z
- Starting working tree: Clean.

## Outcome

Lint now treats canonical layout as strictly increasing sorted-key offsets with exact first/last boundaries and one newline per indexed record, so default and force lint compact tombstones from middle-record deletion and last-record growth; four focused regressions pass.

## Focused workflow coexistence

Assignment 00008 remained the focused owner at run assignment-lifecycle-20260729T07073993. Its workflow paths were excluded from this delivery. Post-detour contract revalidation is required before it advances.

## Delivery path facts

### Requested adopted paths

None.

### Committed paths

- `.specdev/adhoc/2026-09/AH-20260904T064509398Z-31b1_fix-lint-compaction-so-middle-record-deletions-a.md`
- `jsonldb/jsonlfile.py`
- `tests/test_lint_compaction.py`

### Rejected paths

None.

### Remaining owned paths

None.

## Verification summary

No manual verification summary was supplied.

## Verification attempt history

- **lint compaction regressions: passed.** `/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m pytest -q tests/test_lint_compaction.py` (3897 ms, working-tree@5577d6b952120de3dfd298fe46e5b975d03019da)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout:                                                                      [100%]stdout: 
    4 passed in 3.72s
- **Python compilation: passed.** `/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m py_compile jsonldb/jsonlfile.py tests/test_lint_compaction.py` (21 ms, working-tree@5577d6b952120de3dfd298fe46e5b975d03019da)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    (no output)
- **whitespace: passed.** `git diff --check` (11 ms, working-tree@5577d6b952120de3dfd298fe46e5b975d03019da)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    (no output)

## Current acceptance evidence

- **lint compaction regressions: passed.** `/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m pytest -q tests/test_lint_compaction.py` (3897 ms, working-tree@5577d6b952120de3dfd298fe46e5b975d03019da)
- **Python compilation: passed.** `/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m py_compile jsonldb/jsonlfile.py tests/test_lint_compaction.py` (21 ms, working-tree@5577d6b952120de3dfd298fe46e5b975d03019da)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    (no output)
- **whitespace: passed.** `git diff --check` (11 ms, working-tree@5577d6b952120de3dfd298fe46e5b975d03019da)
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
          ".specdev/adhoc/2026-09/AH-20260904T064509398Z-31b1_fix-lint-compaction-so-middle-record-deletions-a.md",
          "jsonldb/jsonlfile.py",
          "tests/test_lint_compaction.py"
        ],
        "rejected": [],
        "remaining": []
      },
      "attempt_history": [
        {
          "version": 1,
          "id": "V-001",
          "label": "lint compaction regressions",
          "annotation": null,
          "command": "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m pytest -q tests/test_lint_compaction.py",
          "argv": [
            "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python",
            "-m",
            "pytest",
            "-q",
            "tests/test_lint_compaction.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T06:49:15.755Z",
          "completed_at": "2026-09-04T06:49:19.652Z",
          "duration_ms": 3897,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@5577d6b952120de3dfd298fe46e5b975d03019da",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout:                                                                      [100%]stdout: \n4 passed in 3.72s",
            "truncated": false,
            "captured_bytes": 98
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "Python compilation",
          "annotation": null,
          "command": "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m py_compile jsonldb/jsonlfile.py tests/test_lint_compaction.py",
          "argv": [
            "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python",
            "-m",
            "py_compile",
            "jsonldb/jsonlfile.py",
            "tests/test_lint_compaction.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T06:49:19.837Z",
          "completed_at": "2026-09-04T06:49:19.858Z",
          "duration_ms": 21,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@5577d6b952120de3dfd298fe46e5b975d03019da",
          "output": {
            "text": "",
            "truncated": false,
            "captured_bytes": 0
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "whitespace",
          "annotation": null,
          "command": "git diff --check",
          "argv": [
            "git",
            "diff",
            "--check"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T06:49:20.040Z",
          "completed_at": "2026-09-04T06:49:20.051Z",
          "duration_ms": 11,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@5577d6b952120de3dfd298fe46e5b975d03019da",
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
          "label": "lint compaction regressions",
          "annotation": null,
          "command": "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m pytest -q tests/test_lint_compaction.py",
          "argv": [
            "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python",
            "-m",
            "pytest",
            "-q",
            "tests/test_lint_compaction.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T06:49:15.755Z",
          "completed_at": "2026-09-04T06:49:19.652Z",
          "duration_ms": 3897,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@5577d6b952120de3dfd298fe46e5b975d03019da",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout:                                                                      [100%]stdout: \n4 passed in 3.72s",
            "truncated": false,
            "captured_bytes": 98
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "Python compilation",
          "annotation": null,
          "command": "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m py_compile jsonldb/jsonlfile.py tests/test_lint_compaction.py",
          "argv": [
            "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python",
            "-m",
            "py_compile",
            "jsonldb/jsonlfile.py",
            "tests/test_lint_compaction.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T06:49:19.837Z",
          "completed_at": "2026-09-04T06:49:19.858Z",
          "duration_ms": 21,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@5577d6b952120de3dfd298fe46e5b975d03019da",
          "output": {
            "text": "",
            "truncated": false,
            "captured_bytes": 0
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "whitespace",
          "annotation": null,
          "command": "git diff --check",
          "argv": [
            "git",
            "diff",
            "--check"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T06:49:20.040Z",
          "completed_at": "2026-09-04T06:49:20.051Z",
          "duration_ms": 11,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@5577d6b952120de3dfd298fe46e5b975d03019da",
          "output": {
            "text": "",
            "truncated": false,
            "captured_bytes": 0
          }
        }
      ]
    }
