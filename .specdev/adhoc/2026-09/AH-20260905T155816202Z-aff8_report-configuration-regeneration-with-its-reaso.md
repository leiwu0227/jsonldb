# Adhoc AH-20260905T155816202Z-aff8

- Scope: Report configuration regeneration with its reason and recover missing table indexes through the shared loader during metadata rebuilds, preserving silent stale-index refreshes
- Title: Report previously silent configuration and index repairs
- Started: 2026-09-05T15:58:16.202Z
- Completed: 2026-09-05T16:00:12.429Z
- Starting working tree: Clean.

## Outcome

Configuration regeneration now emits a structured control_regenerated finding after successful publication, identifying missing timespec or noncanonical format. Metadata rebuilds rely on measurement's shared index loader so missing indexes produce one recovery finding. All 178 focused repair/report/path/metadata/statistics/configuration/folder tests pass, including 38 new cases. Existing stale-index refreshes remain silent, recovered settings and table bytes are preserved, and direct rebuilds do not rewrite report files. folderdb.py is 1209/1250 physical lines and git diff --check passes. Concurrent Discussion state preserved.

## Focused workflow coexistence

No focused Assignment or Mission coexistence was recorded.

## Delivery path facts

### Requested adopted paths

None.

### Committed paths

- `.specdev/adhoc/2026-09/AH-20260905T155816202Z-aff8_report-configuration-regeneration-with-its-reaso.md`
- `jsonldb/folderdb.py`
- `unit_tests/test_repair_reporting.py`

### Rejected paths

None.

### Remaining owned paths

None.

## Verification summary

No manual verification summary was supplied.

## Verification attempt history

- **repair-reporting-regressions: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q unit_tests/test_repair_reporting.py unit_tests/test_reports.py unit_tests/test_report_paths.py unit_tests/test_metadata_upkeep.py unit_tests/test_write_statistics.py unit_tests/test_clear_configuration.py unit_tests/test_folderdb.py` (1755 ms, working-tree@e42b63bcaf3fa5c8607b2c5b4b077f6b151b0746)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 40%]
    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 80%]
    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                        [100%]stdout: 
    178 passed in 1.63s
- **source-cap-and-diff: passed.** `.specdev/cache/bin/python-runtime-env python3 -c from pathlib import Path; import subprocess; count=len(Path("jsonldb/folderdb.py").read_bytes().splitlines()); print("folderdb.py: %d/1250 physical lines" % count); assert count <= 1250; subprocess.run(["git", "diff", "--check"], check=True)` (36 ms, working-tree@e42b63bcaf3fa5c8607b2c5b4b077f6b151b0746)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: folderdb.py: 1209/1250 physical lines

## Current acceptance evidence

- **repair-reporting-regressions: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q unit_tests/test_repair_reporting.py unit_tests/test_reports.py unit_tests/test_report_paths.py unit_tests/test_metadata_upkeep.py unit_tests/test_write_statistics.py unit_tests/test_clear_configuration.py unit_tests/test_folderdb.py` (1755 ms, working-tree@e42b63bcaf3fa5c8607b2c5b4b077f6b151b0746)
- **source-cap-and-diff: passed.** `.specdev/cache/bin/python-runtime-env python3 -c from pathlib import Path; import subprocess; count=len(Path("jsonldb/folderdb.py").read_bytes().splitlines()); print("folderdb.py: %d/1250 physical lines" % count); assert count <= 1250; subprocess.run(["git", "diff", "--check"], check=True)` (36 ms, working-tree@e42b63bcaf3fa5c8607b2c5b4b077f6b151b0746)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: folderdb.py: 1209/1250 physical lines

## Structured verification

    {
      "version": 1,
      "path_facts": {
        "requested": [],
        "committed": [
          ".specdev/adhoc/2026-09/AH-20260905T155816202Z-aff8_report-configuration-regeneration-with-its-reaso.md",
          "jsonldb/folderdb.py",
          "unit_tests/test_repair_reporting.py"
        ],
        "rejected": [],
        "remaining": []
      },
      "attempt_history": [
        {
          "version": 1,
          "id": "V-001",
          "label": "repair-reporting-regressions",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q unit_tests/test_repair_reporting.py unit_tests/test_reports.py unit_tests/test_report_paths.py unit_tests/test_metadata_upkeep.py unit_tests/test_write_statistics.py unit_tests/test_clear_configuration.py unit_tests/test_folderdb.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "unit_tests/test_repair_reporting.py",
            "unit_tests/test_reports.py",
            "unit_tests/test_report_paths.py",
            "unit_tests/test_metadata_upkeep.py",
            "unit_tests/test_write_statistics.py",
            "unit_tests/test_clear_configuration.py",
            "unit_tests/test_folderdb.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T15:59:16.062Z",
          "completed_at": "2026-09-05T15:59:17.817Z",
          "duration_ms": 1755,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@e42b63bcaf3fa5c8607b2c5b4b077f6b151b0746",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 40%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 80%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                        [100%]stdout: \n178 passed in 1.63s",
            "truncated": false,
            "captured_bytes": 260
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "source-cap-and-diff",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -c from pathlib import Path; import subprocess; count=len(Path(\"jsonldb/folderdb.py\").read_bytes().splitlines()); print(\"folderdb.py: %d/1250 physical lines\" % count); assert count <= 1250; subprocess.run([\"git\", \"diff\", \"--check\"], check=True)",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-c",
            "from pathlib import Path; import subprocess; count=len(Path(\"jsonldb/folderdb.py\").read_bytes().splitlines()); print(\"folderdb.py: %d/1250 physical lines\" % count); assert count <= 1250; subprocess.run([\"git\", \"diff\", \"--check\"], check=True)"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T15:59:53.118Z",
          "completed_at": "2026-09-05T15:59:53.155Z",
          "duration_ms": 36,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@e42b63bcaf3fa5c8607b2c5b4b077f6b151b0746",
          "output": {
            "text": "stdout: folderdb.py: 1209/1250 physical lines",
            "truncated": false,
            "captured_bytes": 38
          }
        }
      ],
      "acceptance_evidence": [
        {
          "version": 1,
          "id": "V-001",
          "label": "repair-reporting-regressions",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q unit_tests/test_repair_reporting.py unit_tests/test_reports.py unit_tests/test_report_paths.py unit_tests/test_metadata_upkeep.py unit_tests/test_write_statistics.py unit_tests/test_clear_configuration.py unit_tests/test_folderdb.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "unit_tests/test_repair_reporting.py",
            "unit_tests/test_reports.py",
            "unit_tests/test_report_paths.py",
            "unit_tests/test_metadata_upkeep.py",
            "unit_tests/test_write_statistics.py",
            "unit_tests/test_clear_configuration.py",
            "unit_tests/test_folderdb.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T15:59:16.062Z",
          "completed_at": "2026-09-05T15:59:17.817Z",
          "duration_ms": 1755,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@e42b63bcaf3fa5c8607b2c5b4b077f6b151b0746",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 40%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 80%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                        [100%]stdout: \n178 passed in 1.63s",
            "truncated": false,
            "captured_bytes": 260
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "source-cap-and-diff",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -c from pathlib import Path; import subprocess; count=len(Path(\"jsonldb/folderdb.py\").read_bytes().splitlines()); print(\"folderdb.py: %d/1250 physical lines\" % count); assert count <= 1250; subprocess.run([\"git\", \"diff\", \"--check\"], check=True)",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-c",
            "from pathlib import Path; import subprocess; count=len(Path(\"jsonldb/folderdb.py\").read_bytes().splitlines()); print(\"folderdb.py: %d/1250 physical lines\" % count); assert count <= 1250; subprocess.run([\"git\", \"diff\", \"--check\"], check=True)"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T15:59:53.118Z",
          "completed_at": "2026-09-05T15:59:53.155Z",
          "duration_ms": 36,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@e42b63bcaf3fa5c8607b2c5b4b077f6b151b0746",
          "output": {
            "text": "stdout: folderdb.py: 1209/1250 physical lines",
            "truncated": false,
            "captured_bytes": 38
          }
        }
      ]
    }
