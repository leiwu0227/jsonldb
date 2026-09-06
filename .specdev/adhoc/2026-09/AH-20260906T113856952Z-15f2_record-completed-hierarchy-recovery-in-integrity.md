# Adhoc AH-20260906T113856952Z-15f2

- Scope: Record completed hierarchy recovery in integrity.log
- Title: Log completed hierarchy recovery
- Started: 2026-09-06T11:38:56.952Z
- Completed: 2026-09-06T11:42:11.338Z
- Starting working tree: Clean.

## Outcome

Successful pending hierarchy recovery emits one hierarchy_resumed integrity finding after completion; failed retries and clean opens do not claim recovery. Updated usage documentation. All 76 focused hierarchy/recovery/report tests pass; folderdb.py remains under its 1250-line cap.

## Focused workflow coexistence

No focused Assignment or Mission coexistence was recorded.

## Delivery path facts

### Requested adopted paths

None.

### Committed paths

- `.specdev/adhoc/2026-09/AH-20260906T113856952Z-15f2_record-completed-hierarchy-recovery-in-integrity.md`
- `docs/usage.md`
- `jsonldb/folderdb.py`
- `unit_tests/test_max_hierarchy.py`

### Rejected paths

None.

### Remaining owned paths

None.

## Verification summary

No manual verification summary was supplied.

## Verification attempt history

- **hierarchy-recovery-logging: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest unit_tests/test_max_hierarchy.py unit_tests/test_hierarchy_recovery.py unit_tests/test_reports.py -q --tb=short` (993 ms, working-tree@8d9f976e86861e9e247121ae54ea56cbaae74090)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 94%]
    stdout: .stdout: .stdout: .stdout: .stdout:                                                                      [100%]stdout: 
    76 passed in 0.87s

## Current acceptance evidence

- **hierarchy-recovery-logging: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest unit_tests/test_max_hierarchy.py unit_tests/test_hierarchy_recovery.py unit_tests/test_reports.py -q --tb=short` (993 ms, working-tree@8d9f976e86861e9e247121ae54ea56cbaae74090)

## Structured verification

    {
      "version": 1,
      "path_facts": {
        "requested": [],
        "committed": [
          ".specdev/adhoc/2026-09/AH-20260906T113856952Z-15f2_record-completed-hierarchy-recovery-in-integrity.md",
          "docs/usage.md",
          "jsonldb/folderdb.py",
          "unit_tests/test_max_hierarchy.py"
        ],
        "rejected": [],
        "remaining": []
      },
      "attempt_history": [
        {
          "version": 1,
          "id": "V-001",
          "label": "hierarchy-recovery-logging",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest unit_tests/test_max_hierarchy.py unit_tests/test_hierarchy_recovery.py unit_tests/test_reports.py -q --tb=short",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "unit_tests/test_max_hierarchy.py",
            "unit_tests/test_hierarchy_recovery.py",
            "unit_tests/test_reports.py",
            "-q",
            "--tb=short"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-06T11:39:40.039Z",
          "completed_at": "2026-09-06T11:39:41.032Z",
          "duration_ms": 993,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@8d9f976e86861e9e247121ae54ea56cbaae74090",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 94%]\nstdout: .stdout: .stdout: .stdout: .stdout:                                                                      [100%]stdout: \n76 passed in 0.87s",
            "truncated": false,
            "captured_bytes": 179
          }
        }
      ],
      "acceptance_evidence": [
        {
          "version": 1,
          "id": "V-001",
          "label": "hierarchy-recovery-logging",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest unit_tests/test_max_hierarchy.py unit_tests/test_hierarchy_recovery.py unit_tests/test_reports.py -q --tb=short",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "unit_tests/test_max_hierarchy.py",
            "unit_tests/test_hierarchy_recovery.py",
            "unit_tests/test_reports.py",
            "-q",
            "--tb=short"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-06T11:39:40.039Z",
          "completed_at": "2026-09-06T11:39:41.032Z",
          "duration_ms": 993,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@8d9f976e86861e9e247121ae54ea56cbaae74090",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 94%]\nstdout: .stdout: .stdout: .stdout: .stdout:                                                                      [100%]stdout: \n76 passed in 0.87s",
            "truncated": false,
            "captured_bytes": 179
          }
        }
      ]
    }
