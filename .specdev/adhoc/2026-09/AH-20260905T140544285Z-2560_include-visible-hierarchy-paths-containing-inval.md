# Adhoc AH-20260905T140544285Z-2560

- Scope: Include visible hierarchy paths containing .invalid_tickers while continuing to exclude hidden directories in discovery and reorganization
- Title: Fix visible hierarchy path discovery
- Started: 2026-09-05T14:05:44.285Z
- Completed: 2026-09-05T14:09:36.533Z
- Starting working tree: Clean.

## Outcome

Removed the two .invalid_tickers substring checks from hierarchy discovery and reorganization. Existing hidden-directory pruning continues to exclude quarantine and other hidden trees. Added six regression cases covering visible root, ancestor and child paths, preserved hidden files and indexes, and moved-table reads. All 74 focused hierarchy, FolderDB and configuration tests pass. folderdb.py is 1192/1250 physical lines and whitespace checks pass.

## Focused workflow coexistence

No focused Assignment or Mission coexistence was recorded.

## Delivery path facts

### Requested adopted paths

None.

### Committed paths

- `.specdev/adhoc/2026-09/AH-20260905T140544285Z-2560_include-visible-hierarchy-paths-containing-inval.md`
- `jsonldb/folderdb.py`
- `unit_tests/test_visible_hierarchy_paths.py`

### Rejected paths

None.

### Remaining owned paths

None.

## Verification summary

No manual verification summary was supplied.

## Verification attempt history

- **visible-path-regression: failed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider --tb=short unit_tests/test_visible_hierarchy_paths.py` (719 ms, working-tree@d691e7c51cd3badf912a6d85da848d7324b1d512)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 1
  - Output:

    id_tickers_2026
    _ test_visible_invalid_tickers_substring_and_hidden_exclusion[reorganization-ancestor] _
    unit_tests/test_visible_hierarchy_paths.py:13: in test_visible_invalid_tickers_substring_and_hidden_exclusion
        db = FolderDB(str(root), hierarchy_depth=1)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    jsonldb/folderdb.py:60: in __init__
        raise FileNotFoundError(f"Folder not found: {folder_path}")
    E   FileNotFoundError: Folder not found: /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-717/test_visible_invalid_tickers_s4/backup.invalid_tickers_2026/database
    _ test_visible_invalid_tickers_substring_and_hidden_exclusion[reorganization-child] _
    unit_tests/test_visible_hierarchy_paths.py:13: in test_visible_invalid_tickers_substring_and_hidden_exclusion
        db = FolderDB(str(root), hierarchy_depth=1)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    jsonldb/folderdb.py:60: in __init__
        raise FileNotFoundError(f"Folder not found: {folder_path}")
    E   FileNotFoundError: Folder not found: /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-717/test_visible_invalid_tickers_s5/database
    =========================== short test summary info ============================
    FAILED unit_tests/test_visible_hierarchy_paths.py::test_visible_invalid_tickers_substring_and_hidden_exclusion[discovery-root]
    FAILED unit_tests/test_visible_hierarchy_paths.py::test_visible_invalid_tickers_substring_and_hidden_exclusion[discovery-ancestor]
    FAILED unit_tests/test_visible_hierarchy_paths.py::test_visible_invalid_tickers_substring_and_hidden_exclusion[discovery-child]
    FAILED unit_tests/test_visible_hierarchy_paths.py::test_visible_invalid_tickers_substring_and_hidden_exclusion[reorganization-root]
    FAILED unit_tests/test_visible_hierarchy_paths.py::test_visible_invalid_tickers_substring_and_hidden_exclusion[reorganization-ancestor]
    FAILED unit_tests/test_visible_hierarchy_paths.py::test_visible_invalid_tickers_substring_and_hidden_exclusion[reorganization-child]
    6 failed in 0.56s
- **visible-path-regression: failed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider --tb=short unit_tests/test_visible_hierarchy_paths.py` (487 ms, working-tree@d691e7c51cd3badf912a6d85da848d7324b1d512)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 1
  - Output:

    id_tickers_2026
    _ test_visible_invalid_tickers_substring_and_hidden_exclusion[reorganization-ancestor] _
    unit_tests/test_visible_hierarchy_paths.py:13: in test_visible_invalid_tickers_substring_and_hidden_exclusion
        db = FolderDB(str(root), hierarchy_depth=1)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    jsonldb/folderdb.py:60: in __init__
        raise FileNotFoundError(f"Folder not found: {folder_path}")
    E   FileNotFoundError: Folder not found: /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-718/test_visible_invalid_tickers_s4/backup.invalid_tickers_2026/database
    _ test_visible_invalid_tickers_substring_and_hidden_exclusion[reorganization-child] _
    unit_tests/test_visible_hierarchy_paths.py:13: in test_visible_invalid_tickers_substring_and_hidden_exclusion
        db = FolderDB(str(root), hierarchy_depth=1)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    jsonldb/folderdb.py:60: in __init__
        raise FileNotFoundError(f"Folder not found: {folder_path}")
    E   FileNotFoundError: Folder not found: /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-718/test_visible_invalid_tickers_s5/database
    =========================== short test summary info ============================
    FAILED unit_tests/test_visible_hierarchy_paths.py::test_visible_invalid_tickers_substring_and_hidden_exclusion[discovery-root]
    FAILED unit_tests/test_visible_hierarchy_paths.py::test_visible_invalid_tickers_substring_and_hidden_exclusion[discovery-ancestor]
    FAILED unit_tests/test_visible_hierarchy_paths.py::test_visible_invalid_tickers_substring_and_hidden_exclusion[discovery-child]
    FAILED unit_tests/test_visible_hierarchy_paths.py::test_visible_invalid_tickers_substring_and_hidden_exclusion[reorganization-root]
    FAILED unit_tests/test_visible_hierarchy_paths.py::test_visible_invalid_tickers_substring_and_hidden_exclusion[reorganization-ancestor]
    FAILED unit_tests/test_visible_hierarchy_paths.py::test_visible_invalid_tickers_substring_and_hidden_exclusion[reorganization-child]
    6 failed in 0.37s
- **visible-path-regression: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider --tb=short unit_tests/test_visible_hierarchy_paths.py` (613 ms, working-tree@d691e7c51cd3badf912a6d85da848d7324b1d512)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                                    [100%]stdout: 
    6 passed in 0.50s
- **hierarchy-regression: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider --tb=short unit_tests/test_visible_hierarchy_paths.py unit_tests/test_hierarchy_recovery.py unit_tests/test_folderdb.py unit_tests/test_clear_configuration.py` (566 ms, working-tree@d691e7c51cd3badf912a6d85da848d7324b1d512)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 97%]
    stdout: .stdout: .stdout:                                                                        [100%]stdout: 
    74 passed in 0.45s
- **source-limit-and-whitespace: passed.** `.specdev/cache/bin/python-runtime-env python3 -c from pathlib import Path; import subprocess; count = len(Path("jsonldb/folderdb.py").read_bytes().splitlines()); print("folderdb.py:", count, "/1250"); assert count <= 1250; subprocess.run(["git", "diff", "--check"], check=True)` (38 ms, working-tree@d691e7c51cd3badf912a6d85da848d7324b1d512)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: folderdb.py: 1192 /1250

## Current acceptance evidence

- **visible-path-regression: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider --tb=short unit_tests/test_visible_hierarchy_paths.py` (613 ms, working-tree@d691e7c51cd3badf912a6d85da848d7324b1d512)
- **hierarchy-regression: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider --tb=short unit_tests/test_visible_hierarchy_paths.py unit_tests/test_hierarchy_recovery.py unit_tests/test_folderdb.py unit_tests/test_clear_configuration.py` (566 ms, working-tree@d691e7c51cd3badf912a6d85da848d7324b1d512)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 97%]
    stdout: .stdout: .stdout:                                                                        [100%]stdout: 
    74 passed in 0.45s
- **source-limit-and-whitespace: passed.** `.specdev/cache/bin/python-runtime-env python3 -c from pathlib import Path; import subprocess; count = len(Path("jsonldb/folderdb.py").read_bytes().splitlines()); print("folderdb.py:", count, "/1250"); assert count <= 1250; subprocess.run(["git", "diff", "--check"], check=True)` (38 ms, working-tree@d691e7c51cd3badf912a6d85da848d7324b1d512)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: folderdb.py: 1192 /1250

## Structured verification

    {
      "version": 1,
      "path_facts": {
        "requested": [],
        "committed": [
          ".specdev/adhoc/2026-09/AH-20260905T140544285Z-2560_include-visible-hierarchy-paths-containing-inval.md",
          "jsonldb/folderdb.py",
          "unit_tests/test_visible_hierarchy_paths.py"
        ],
        "rejected": [],
        "remaining": []
      },
      "attempt_history": [
        {
          "version": 1,
          "id": "V-001",
          "label": "visible-path-regression",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider --tb=short unit_tests/test_visible_hierarchy_paths.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "--tb=short",
            "unit_tests/test_visible_hierarchy_paths.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T14:07:21.886Z",
          "completed_at": "2026-09-05T14:07:22.605Z",
          "duration_ms": 719,
          "exit_status": 1,
          "status": "failed",
          "tested_revision": "working-tree@d691e7c51cd3badf912a6d85da848d7324b1d512",
          "output": {
            "text": "stdout: Fstdout: Fstdout: Fstdout: Fstdout: Fstdout: Fstdout:                                                                    [100%]stdout: \n=================================== FAILURES ===================================\n_ test_visible_invalid_tickers_substring_and_hidden_exclusion[discovery-root] __\nunit_tests/test_visible_hierarchy_paths.py:13: in test_visible_invalid_tickers_substring_and_hidden_exclusion\n    db = FolderDB(str(root), hierarchy_depth=1)\n         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\njsonldb/folderdb.py:60: in __init__\n    raise FileNotFoundError(f\"Folder not found: {folder_path}\")\nE   FileNotFoundError: Folder not found: /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-717/test_visible_invalid_tickers_s0/backup.invalid_tickers_2026\n_ test_visible_invalid_tickers_substring_and_hidden_exclusion[discovery-ancestor] _\nunit_tests/test_visible_hierarchy_paths.py:13: in test_visible_invalid_tickers_substring_and_hidden_exclusion\n    db = FolderDB(str(root), hierarchy_depth=1)\n         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\njsonldb/folderdb.py:60: in __init__\n    raise FileNotFoundError(f\"Folder not found: {folder_path}\")\nE   FileNotFoundError: Folder not found: /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-717/test_visible_invalid_tickers_s1/backup.invalid_tickers_2026/database\n_ test_visible_invalid_tickers_substring_and_hidden_exclusion[discovery-child] _\nunit_tests/test_visible_hierarchy_paths.py:13: in test_visible_invalid_tickers_substring_and_hidden_exclusion\n    db = FolderDB(str(root), hierarchy_depth=1)\n         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\njsonldb/folderdb.py:60: in __init__\n    raise FileNotFoundError(f\"Folder not found: {folder_path}\")\nE   FileNotFoundError: Folder not found: /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-717/test_visible_invalid_tickers_s2/database\n_ test_visible_invalid_tickers_substring_and_hidden_exclusion[reorganization-root] _\nunit_tests/test_visible_hierarchy_paths.py:13: in test_visible_invalid_tickers_substring_and_hidden_exclusion\n    db = FolderDB(str(root), hierarchy_depth=1)\n         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\njsonldb/folderdb.py:60: in __init__\n    raise FileNotFoundError(f\"Folder not found: {folder_path}\")\nE   FileNotFoundError: Folder not found: /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-717/test_visible_invalid_tickers_s3/backup.invalid_tickers_2026\n_ test_visible_invalid_tickers_substring_and_hidden_exclusion[reorganization-ancestor] _\nunit_tests/test_visible_hierarchy_paths.py:13: in test_visible_invalid_tickers_substring_and_hidden_exclusion\n    db = FolderDB(str(root), hierarchy_depth=1)\n         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\njsonldb/folderdb.py:60: in __init__\n    raise FileNotFoundError(f\"Folder not found: {folder_path}\")\nE   FileNotFoundError: Folder not found: /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-717/test_visible_invalid_tickers_s4/backup.invalid_tickers_2026/database\n_ test_visible_invalid_tickers_substring_and_hidden_exclusion[reorganization-child] _\nunit_tests/test_visible_hierarchy_paths.py:13: in test_visible_invalid_tickers_substring_and_hidden_exclusion\n    db = FolderDB(str(root), hierarchy_depth=1)\n         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\njsonldb/folderdb.py:60: in __init__\n    raise FileNotFoundError(f\"Folder not found: {folder_path}\")\nE   FileNotFoundError: Folder not found: /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-717/test_visible_invalid_tickers_s5/database\n=========================== short test summary info ============================\nFAILED unit_tests/test_visible_hierarchy_paths.py::test_visible_invalid_tickers_substring_and_hidden_exclusion[discovery-root]\nFAILED unit_tests/test_visible_hierarchy_paths.py::test_visible_invalid_tickers_substring_and_hidden_exclusion[discovery-ancestor]\nFAILED unit_tests/test_visible_hierarchy_paths.py::test_visible_invalid_tickers_substring_and_hidden_exclusion[discovery-child]\nFAILED unit_tests/test_visible_hierarchy_paths.py::test_visible_invalid_tickers_substring_and_hidden_exclusion[reorganization-root]\nFAILED unit_tests/test_visible_hierarchy_paths.py::test_visible_invalid_tickers_substring_and_hidden_exclusion[reorganization-ancestor]\nFAILED unit_tests/test_visible_hierarchy_paths.py::test_visible_invalid_tickers_substring_and_hidden_exclusion[reorganization-child]\n6 failed in 0.56s",
            "truncated": false,
            "captured_bytes": 4479
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "visible-path-regression",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider --tb=short unit_tests/test_visible_hierarchy_paths.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "--tb=short",
            "unit_tests/test_visible_hierarchy_paths.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T14:07:41.527Z",
          "completed_at": "2026-09-05T14:07:42.015Z",
          "duration_ms": 487,
          "exit_status": 1,
          "status": "failed",
          "tested_revision": "working-tree@d691e7c51cd3badf912a6d85da848d7324b1d512",
          "output": {
            "text": "stdout: Fstdout: Fstdout: Fstdout: Fstdout: Fstdout: Fstdout:                                                                    [100%]stdout: \n=================================== FAILURES ===================================\n_ test_visible_invalid_tickers_substring_and_hidden_exclusion[discovery-root] __\nunit_tests/test_visible_hierarchy_paths.py:13: in test_visible_invalid_tickers_substring_and_hidden_exclusion\n    db = FolderDB(str(root), hierarchy_depth=1)\n         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\njsonldb/folderdb.py:60: in __init__\n    raise FileNotFoundError(f\"Folder not found: {folder_path}\")\nE   FileNotFoundError: Folder not found: /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-718/test_visible_invalid_tickers_s0/backup.invalid_tickers_2026\n_ test_visible_invalid_tickers_substring_and_hidden_exclusion[discovery-ancestor] _\nunit_tests/test_visible_hierarchy_paths.py:13: in test_visible_invalid_tickers_substring_and_hidden_exclusion\n    db = FolderDB(str(root), hierarchy_depth=1)\n         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\njsonldb/folderdb.py:60: in __init__\n    raise FileNotFoundError(f\"Folder not found: {folder_path}\")\nE   FileNotFoundError: Folder not found: /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-718/test_visible_invalid_tickers_s1/backup.invalid_tickers_2026/database\n_ test_visible_invalid_tickers_substring_and_hidden_exclusion[discovery-child] _\nunit_tests/test_visible_hierarchy_paths.py:13: in test_visible_invalid_tickers_substring_and_hidden_exclusion\n    db = FolderDB(str(root), hierarchy_depth=1)\n         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\njsonldb/folderdb.py:60: in __init__\n    raise FileNotFoundError(f\"Folder not found: {folder_path}\")\nE   FileNotFoundError: Folder not found: /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-718/test_visible_invalid_tickers_s2/database\n_ test_visible_invalid_tickers_substring_and_hidden_exclusion[reorganization-root] _\nunit_tests/test_visible_hierarchy_paths.py:13: in test_visible_invalid_tickers_substring_and_hidden_exclusion\n    db = FolderDB(str(root), hierarchy_depth=1)\n         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\njsonldb/folderdb.py:60: in __init__\n    raise FileNotFoundError(f\"Folder not found: {folder_path}\")\nE   FileNotFoundError: Folder not found: /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-718/test_visible_invalid_tickers_s3/backup.invalid_tickers_2026\n_ test_visible_invalid_tickers_substring_and_hidden_exclusion[reorganization-ancestor] _\nunit_tests/test_visible_hierarchy_paths.py:13: in test_visible_invalid_tickers_substring_and_hidden_exclusion\n    db = FolderDB(str(root), hierarchy_depth=1)\n         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\njsonldb/folderdb.py:60: in __init__\n    raise FileNotFoundError(f\"Folder not found: {folder_path}\")\nE   FileNotFoundError: Folder not found: /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-718/test_visible_invalid_tickers_s4/backup.invalid_tickers_2026/database\n_ test_visible_invalid_tickers_substring_and_hidden_exclusion[reorganization-child] _\nunit_tests/test_visible_hierarchy_paths.py:13: in test_visible_invalid_tickers_substring_and_hidden_exclusion\n    db = FolderDB(str(root), hierarchy_depth=1)\n         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\njsonldb/folderdb.py:60: in __init__\n    raise FileNotFoundError(f\"Folder not found: {folder_path}\")\nE   FileNotFoundError: Folder not found: /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-718/test_visible_invalid_tickers_s5/database\n=========================== short test summary info ============================\nFAILED unit_tests/test_visible_hierarchy_paths.py::test_visible_invalid_tickers_substring_and_hidden_exclusion[discovery-root]\nFAILED unit_tests/test_visible_hierarchy_paths.py::test_visible_invalid_tickers_substring_and_hidden_exclusion[discovery-ancestor]\nFAILED unit_tests/test_visible_hierarchy_paths.py::test_visible_invalid_tickers_substring_and_hidden_exclusion[discovery-child]\nFAILED unit_tests/test_visible_hierarchy_paths.py::test_visible_invalid_tickers_substring_and_hidden_exclusion[reorganization-root]\nFAILED unit_tests/test_visible_hierarchy_paths.py::test_visible_invalid_tickers_substring_and_hidden_exclusion[reorganization-ancestor]\nFAILED unit_tests/test_visible_hierarchy_paths.py::test_visible_invalid_tickers_substring_and_hidden_exclusion[reorganization-child]\n6 failed in 0.37s",
            "truncated": false,
            "captured_bytes": 4479
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "visible-path-regression",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider --tb=short unit_tests/test_visible_hierarchy_paths.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "--tb=short",
            "unit_tests/test_visible_hierarchy_paths.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T14:08:49.754Z",
          "completed_at": "2026-09-05T14:08:50.368Z",
          "duration_ms": 613,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@d691e7c51cd3badf912a6d85da848d7324b1d512",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                                    [100%]stdout: \n6 passed in 0.50s",
            "truncated": false,
            "captured_bytes": 98
          }
        },
        {
          "version": 1,
          "id": "V-004",
          "label": "hierarchy-regression",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider --tb=short unit_tests/test_visible_hierarchy_paths.py unit_tests/test_hierarchy_recovery.py unit_tests/test_folderdb.py unit_tests/test_clear_configuration.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "--tb=short",
            "unit_tests/test_visible_hierarchy_paths.py",
            "unit_tests/test_hierarchy_recovery.py",
            "unit_tests/test_folderdb.py",
            "unit_tests/test_clear_configuration.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T14:09:07.469Z",
          "completed_at": "2026-09-05T14:09:08.035Z",
          "duration_ms": 566,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@d691e7c51cd3badf912a6d85da848d7324b1d512",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 97%]\nstdout: .stdout: .stdout:                                                                        [100%]stdout: \n74 passed in 0.45s",
            "truncated": false,
            "captured_bytes": 179
          }
        },
        {
          "version": 1,
          "id": "V-005",
          "label": "source-limit-and-whitespace",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -c from pathlib import Path; import subprocess; count = len(Path(\"jsonldb/folderdb.py\").read_bytes().splitlines()); print(\"folderdb.py:\", count, \"/1250\"); assert count <= 1250; subprocess.run([\"git\", \"diff\", \"--check\"], check=True)",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-c",
            "from pathlib import Path; import subprocess; count = len(Path(\"jsonldb/folderdb.py\").read_bytes().splitlines()); print(\"folderdb.py:\", count, \"/1250\"); assert count <= 1250; subprocess.run([\"git\", \"diff\", \"--check\"], check=True)"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T14:09:08.242Z",
          "completed_at": "2026-09-05T14:09:08.281Z",
          "duration_ms": 38,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@d691e7c51cd3badf912a6d85da848d7324b1d512",
          "output": {
            "text": "stdout: folderdb.py: 1192 /1250",
            "truncated": false,
            "captured_bytes": 24
          }
        }
      ],
      "acceptance_evidence": [
        {
          "version": 1,
          "id": "V-003",
          "label": "visible-path-regression",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider --tb=short unit_tests/test_visible_hierarchy_paths.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "--tb=short",
            "unit_tests/test_visible_hierarchy_paths.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T14:08:49.754Z",
          "completed_at": "2026-09-05T14:08:50.368Z",
          "duration_ms": 613,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@d691e7c51cd3badf912a6d85da848d7324b1d512",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                                    [100%]stdout: \n6 passed in 0.50s",
            "truncated": false,
            "captured_bytes": 98
          }
        },
        {
          "version": 1,
          "id": "V-004",
          "label": "hierarchy-regression",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider --tb=short unit_tests/test_visible_hierarchy_paths.py unit_tests/test_hierarchy_recovery.py unit_tests/test_folderdb.py unit_tests/test_clear_configuration.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "--tb=short",
            "unit_tests/test_visible_hierarchy_paths.py",
            "unit_tests/test_hierarchy_recovery.py",
            "unit_tests/test_folderdb.py",
            "unit_tests/test_clear_configuration.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T14:09:07.469Z",
          "completed_at": "2026-09-05T14:09:08.035Z",
          "duration_ms": 566,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@d691e7c51cd3badf912a6d85da848d7324b1d512",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 97%]\nstdout: .stdout: .stdout:                                                                        [100%]stdout: \n74 passed in 0.45s",
            "truncated": false,
            "captured_bytes": 179
          }
        },
        {
          "version": 1,
          "id": "V-005",
          "label": "source-limit-and-whitespace",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -c from pathlib import Path; import subprocess; count = len(Path(\"jsonldb/folderdb.py\").read_bytes().splitlines()); print(\"folderdb.py:\", count, \"/1250\"); assert count <= 1250; subprocess.run([\"git\", \"diff\", \"--check\"], check=True)",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-c",
            "from pathlib import Path; import subprocess; count = len(Path(\"jsonldb/folderdb.py\").read_bytes().splitlines()); print(\"folderdb.py:\", count, \"/1250\"); assert count <= 1250; subprocess.run([\"git\", \"diff\", \"--check\"], check=True)"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T14:09:08.242Z",
          "completed_at": "2026-09-05T14:09:08.281Z",
          "duration_ms": 38,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@d691e7c51cd3badf912a6d85da848d7324b1d512",
          "output": {
            "text": "stdout: folderdb.py: 1192 /1250",
            "truncated": false,
            "captured_bytes": 24
          }
        }
      ]
    }
