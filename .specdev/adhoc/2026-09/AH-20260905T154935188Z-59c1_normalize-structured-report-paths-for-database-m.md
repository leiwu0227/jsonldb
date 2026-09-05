# Adhoc AH-20260905T154935188Z-59c1

- Scope: Normalize structured report paths for database membership, retain a fallback for legacy log messages, and verify relative and absolute open/lint reports exclude unrelated databases
- Title: Capture integrity findings for relative database paths
- Started: 2026-09-05T15:49:35.188Z
- Completed: 2026-09-05T15:53:36.862Z
- Starting working tree: Clean.

## Outcome

Report capture now uses normalized structured file paths and component-based database membership, with path extraction for known legacy messages. Relative dot, parent, other relative and absolute open/lint paths capture repairs consistently; foreign databases, prefix siblings and escaping parent paths are excluded, and structured ownership overrides message text. All 95 focused report/folder/hierarchy tests pass, including 18 new path regressions. reports.py is 124/150 physical lines and git diff --check passes. Report formatting, bounds and public APIs remain unchanged; concurrent Discussion state preserved.

## Focused workflow coexistence

No focused Assignment or Mission coexistence was recorded.

## Delivery path facts

### Requested adopted paths

None.

### Committed paths

- `.specdev/adhoc/2026-09/AH-20260905T154935188Z-59c1_normalize-structured-report-paths-for-database-m.md`
- `jsonldb/reports.py`
- `unit_tests/test_report_paths.py`

### Rejected paths

None.

### Remaining owned paths

None.

## Verification summary

No manual verification summary was supplied.

## Verification attempt history

- **report-paths-and-regressions: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q unit_tests/test_report_paths.py unit_tests/test_reports.py unit_tests/test_folderdb.py unit_tests/test_hierarchy_recovery.py` (993 ms, working-tree@4d1aad5a625a2b0bfd66af027a75b60067ff0c1a)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 75%]
    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                   [100%]stdout: 
    95 passed in 0.85s
- **source-cap-and-diff: passed.** `.specdev/cache/bin/python-runtime-env python3 -c from pathlib import Path; import subprocess; count=len(Path("jsonldb/reports.py").read_bytes().splitlines()); print("reports.py: %d/150 physical lines" % count); assert count <= 150; subprocess.run(["git", "diff", "--check"], check=True)` (38 ms, working-tree@4d1aad5a625a2b0bfd66af027a75b60067ff0c1a)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: reports.py: 124/150 physical lines

## Current acceptance evidence

- **report-paths-and-regressions: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q unit_tests/test_report_paths.py unit_tests/test_reports.py unit_tests/test_folderdb.py unit_tests/test_hierarchy_recovery.py` (993 ms, working-tree@4d1aad5a625a2b0bfd66af027a75b60067ff0c1a)
- **source-cap-and-diff: passed.** `.specdev/cache/bin/python-runtime-env python3 -c from pathlib import Path; import subprocess; count=len(Path("jsonldb/reports.py").read_bytes().splitlines()); print("reports.py: %d/150 physical lines" % count); assert count <= 150; subprocess.run(["git", "diff", "--check"], check=True)` (38 ms, working-tree@4d1aad5a625a2b0bfd66af027a75b60067ff0c1a)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: reports.py: 124/150 physical lines

## Structured verification

    {
      "version": 1,
      "path_facts": {
        "requested": [],
        "committed": [
          ".specdev/adhoc/2026-09/AH-20260905T154935188Z-59c1_normalize-structured-report-paths-for-database-m.md",
          "jsonldb/reports.py",
          "unit_tests/test_report_paths.py"
        ],
        "rejected": [],
        "remaining": []
      },
      "attempt_history": [
        {
          "version": 1,
          "id": "V-001",
          "label": "report-paths-and-regressions",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q unit_tests/test_report_paths.py unit_tests/test_reports.py unit_tests/test_folderdb.py unit_tests/test_hierarchy_recovery.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "unit_tests/test_report_paths.py",
            "unit_tests/test_reports.py",
            "unit_tests/test_folderdb.py",
            "unit_tests/test_hierarchy_recovery.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T15:52:38.341Z",
          "completed_at": "2026-09-05T15:52:39.334Z",
          "duration_ms": 993,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@4d1aad5a625a2b0bfd66af027a75b60067ff0c1a",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 75%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                   [100%]stdout: \n95 passed in 0.85s",
            "truncated": false,
            "captured_bytes": 179
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "source-cap-and-diff",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -c from pathlib import Path; import subprocess; count=len(Path(\"jsonldb/reports.py\").read_bytes().splitlines()); print(\"reports.py: %d/150 physical lines\" % count); assert count <= 150; subprocess.run([\"git\", \"diff\", \"--check\"], check=True)",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-c",
            "from pathlib import Path; import subprocess; count=len(Path(\"jsonldb/reports.py\").read_bytes().splitlines()); print(\"reports.py: %d/150 physical lines\" % count); assert count <= 150; subprocess.run([\"git\", \"diff\", \"--check\"], check=True)"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T15:53:23.964Z",
          "completed_at": "2026-09-05T15:53:24.002Z",
          "duration_ms": 38,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@4d1aad5a625a2b0bfd66af027a75b60067ff0c1a",
          "output": {
            "text": "stdout: reports.py: 124/150 physical lines",
            "truncated": false,
            "captured_bytes": 35
          }
        }
      ],
      "acceptance_evidence": [
        {
          "version": 1,
          "id": "V-001",
          "label": "report-paths-and-regressions",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q unit_tests/test_report_paths.py unit_tests/test_reports.py unit_tests/test_folderdb.py unit_tests/test_hierarchy_recovery.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "unit_tests/test_report_paths.py",
            "unit_tests/test_reports.py",
            "unit_tests/test_folderdb.py",
            "unit_tests/test_hierarchy_recovery.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T15:52:38.341Z",
          "completed_at": "2026-09-05T15:52:39.334Z",
          "duration_ms": 993,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@4d1aad5a625a2b0bfd66af027a75b60067ff0c1a",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 75%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                   [100%]stdout: \n95 passed in 0.85s",
            "truncated": false,
            "captured_bytes": 179
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "source-cap-and-diff",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -c from pathlib import Path; import subprocess; count=len(Path(\"jsonldb/reports.py\").read_bytes().splitlines()); print(\"reports.py: %d/150 physical lines\" % count); assert count <= 150; subprocess.run([\"git\", \"diff\", \"--check\"], check=True)",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-c",
            "from pathlib import Path; import subprocess; count=len(Path(\"jsonldb/reports.py\").read_bytes().splitlines()); print(\"reports.py: %d/150 physical lines\" % count); assert count <= 150; subprocess.run([\"git\", \"diff\", \"--check\"], check=True)"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T15:53:23.964Z",
          "completed_at": "2026-09-05T15:53:24.002Z",
          "duration_ms": 38,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@4d1aad5a625a2b0bfd66af027a75b60067ff0c1a",
          "output": {
            "text": "stdout: reports.py: 124/150 physical lines",
            "truncated": false,
            "captured_bytes": 35
          }
        }
      ]
    }
