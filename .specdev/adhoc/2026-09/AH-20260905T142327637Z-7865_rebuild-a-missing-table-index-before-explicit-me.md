# Adhoc AH-20260905T142327637Z-7865

- Scope: Rebuild a missing table index before explicit metadata refresh computes counts and key bounds, while retaining write-time statistics reuse
- Title: Recover missing indexes during metadata refresh
- Started: 2026-09-05T14:23:27.637Z
- Completed: 2026-09-05T14:25:25.602Z
- Starting working tree: Clean.

## Outcome

Explicit metadata refresh now always loads the table index through existing recovery instead of substituting an empty index when it is missing. Write-time statistics reuse remains unchanged. Eight new regression cases cover empty/populated, slotted/unslotted and flat/hierarchical tables, preserving table bytes and metadata records while rebuilding indexes and reporting accurate counts and bounds. All 141 focused metadata, statistics, index-cache and FolderDB tests passed; folderdb.py is 1191/1250 physical lines and whitespace checks passed.

## Focused workflow coexistence

No focused Assignment or Mission coexistence was recorded.

## Delivery path facts

### Requested adopted paths

None.

### Committed paths

- `.specdev/adhoc/2026-09/AH-20260905T142327637Z-7865_rebuild-a-missing-table-index-before-explicit-me.md`
- `jsonldb/folderdb.py`
- `unit_tests/test_write_statistics.py`

### Rejected paths

None.

### Remaining owned paths

None.

## Verification summary

No manual verification summary was supplied.

## Verification attempt history

- **missing-index-refresh: failed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider --tb=short unit_tests/test_write_statistics.py -k explicit_refresh_rebuilds_missing_index` (338 ms, working-tree@c3d3e9dbfce8d3a106f266fa4867d2f962db418b)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 1
  - Output:

    _________
    unit_tests/test_write_statistics.py:224: in test_explicit_refresh_rebuilds_missing_index
        assert os.path.exists(path + '.idx'), 'refresh must rebuild before measuring'
    E   AssertionError: refresh must rebuild before measuring
    E   assert False
    E    +  where False = <function exists at 0x10077bce0>(('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-721/test_explicit_refresh_rebuilds7/region/region.table.jsonl' + '.idx'))
    E    +    where <function exists at 0x10077bce0> = <module 'posixpath' (frozen)>.exists
    E    +      where <module 'posixpath' (frozen)> = os.path
    ------------------------------ Captured log call -------------------------------
    WARNING  jsonldb.folderdb:folderdb.py:205 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-721/test_explicit_refresh_rebuilds7/h.meta
    WARNING  jsonldb.folderdb:folderdb.py:122 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-721/test_explicit_refresh_rebuilds7/config.meta
    =========================== short test summary info ============================
    FAILED unit_tests/test_write_statistics.py::test_explicit_refresh_rebuilds_missing_index[rows0-False-None]
    FAILED unit_tests/test_write_statistics.py::test_explicit_refresh_rebuilds_missing_index[rows0-False-1]
    FAILED unit_tests/test_write_statistics.py::test_explicit_refresh_rebuilds_missing_index[rows0-True-None]
    FAILED unit_tests/test_write_statistics.py::test_explicit_refresh_rebuilds_missing_index[rows0-True-1]
    FAILED unit_tests/test_write_statistics.py::test_explicit_refresh_rebuilds_missing_index[rows1-False-None]
    FAILED unit_tests/test_write_statistics.py::test_explicit_refresh_rebuilds_missing_index[rows1-False-1]
    FAILED unit_tests/test_write_statistics.py::test_explicit_refresh_rebuilds_missing_index[rows1-True-None]
    FAILED unit_tests/test_write_statistics.py::test_explicit_refresh_rebuilds_missing_index[rows1-True-1]
    8 failed, 49 deselected in 0.20s
- **missing-index-refresh: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider --tb=short unit_tests/test_write_statistics.py -k explicit_refresh_rebuilds_missing_index` (351 ms, working-tree@c3d3e9dbfce8d3a106f266fa4867d2f962db418b)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                                  [100%]stdout: 
    8 passed, 49 deselected in 0.23s
- **metadata-and-statistics-regression: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider --tb=short unit_tests/test_write_statistics.py unit_tests/test_metadata_upkeep.py unit_tests/test_index_cache.py unit_tests/test_folderdb.py` (863 ms, working-tree@c3d3e9dbfce8d3a106f266fa4867d2f962db418b)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 51%]
    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:     [100%]stdout: 
    141 passed in 0.74s
- **source-limit-and-whitespace: passed.** `.specdev/cache/bin/python-runtime-env python3 -c from pathlib import Path; import subprocess; count = len(Path("jsonldb/folderdb.py").read_bytes().splitlines()); print("folderdb.py:", count, "/1250"); assert count <= 1250; subprocess.run(["git", "diff", "--check"], check=True)` (41 ms, working-tree@c3d3e9dbfce8d3a106f266fa4867d2f962db418b)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: folderdb.py: 1191 /1250

## Current acceptance evidence

- **missing-index-refresh: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider --tb=short unit_tests/test_write_statistics.py -k explicit_refresh_rebuilds_missing_index` (351 ms, working-tree@c3d3e9dbfce8d3a106f266fa4867d2f962db418b)
- **metadata-and-statistics-regression: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider --tb=short unit_tests/test_write_statistics.py unit_tests/test_metadata_upkeep.py unit_tests/test_index_cache.py unit_tests/test_folderdb.py` (863 ms, working-tree@c3d3e9dbfce8d3a106f266fa4867d2f962db418b)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 51%]
    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:     [100%]stdout: 
    141 passed in 0.74s
- **source-limit-and-whitespace: passed.** `.specdev/cache/bin/python-runtime-env python3 -c from pathlib import Path; import subprocess; count = len(Path("jsonldb/folderdb.py").read_bytes().splitlines()); print("folderdb.py:", count, "/1250"); assert count <= 1250; subprocess.run(["git", "diff", "--check"], check=True)` (41 ms, working-tree@c3d3e9dbfce8d3a106f266fa4867d2f962db418b)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: folderdb.py: 1191 /1250

## Structured verification

    {
      "version": 1,
      "path_facts": {
        "requested": [],
        "committed": [
          ".specdev/adhoc/2026-09/AH-20260905T142327637Z-7865_rebuild-a-missing-table-index-before-explicit-me.md",
          "jsonldb/folderdb.py",
          "unit_tests/test_write_statistics.py"
        ],
        "rejected": [],
        "remaining": []
      },
      "attempt_history": [
        {
          "version": 1,
          "id": "V-001",
          "label": "missing-index-refresh",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider --tb=short unit_tests/test_write_statistics.py -k explicit_refresh_rebuilds_missing_index",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "--tb=short",
            "unit_tests/test_write_statistics.py",
            "-k",
            "explicit_refresh_rebuilds_missing_index"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T14:24:32.224Z",
          "completed_at": "2026-09-05T14:24:32.563Z",
          "duration_ms": 338,
          "exit_status": 1,
          "status": "failed",
          "tested_revision": "working-tree@c3d3e9dbfce8d3a106f266fa4867d2f962db418b",
          "output": {
            "text": "stdout: Fstdout: Fstdout: Fstdout: Fstdout: Fstdout: Fstdout: Fstdout: Fstdout:                                                                  [100%]stdout: \n=================================== FAILURES ===================================\n________ test_explicit_refresh_rebuilds_missing_index[rows0-False-None] ________\nunit_tests/test_write_statistics.py:224: in test_explicit_refresh_rebuilds_missing_index\n    assert os.path.exists(path + '.idx'), 'refresh must rebuild before measuring'\nE   AssertionError: refresh must rebuild before measuring\nE   assert False\nE    +  where False = <function exists at 0x10077bce0>(('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-721/test_explicit_refresh_rebuilds0/region.table.jsonl' + '.idx'))\nE    +    where <function exists at 0x10077bce0> = <module 'posixpath' (frozen)>.exists\nE    +      where <module 'posixpath' (frozen)> = os.path\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:122 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-721/test_explicit_refresh_rebuilds0/config.meta\nWARNING  jsonldb.folderdb:folderdb.py:139 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-721/test_explicit_refresh_rebuilds0/db.meta\n_________ test_explicit_refresh_rebuilds_missing_index[rows0-False-1] __________\nunit_tests/test_write_statistics.py:224: in test_explicit_refresh_rebuilds_missing_index\n    assert os.path.exists(path + '.idx'), 'refresh must rebuild before measuring'\nE   AssertionError: refresh must rebuild before measuring\nE   assert False\nE    +  where False = <function exists at 0x10077bce0>(('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-721/test_explicit_refresh_rebuilds1/region/region.table.jsonl' + '.idx'))\nE    +    where <function exists at 0x10077bce0> = <module 'posixpath' (frozen)>.exists\nE    +      where <module 'posixpath' (frozen)> = os.path\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:205 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-721/test_explicit_refresh_rebuilds1/h.meta\nWARNING  jsonldb.folderdb:folderdb.py:122 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-721/test_explicit_refresh_rebuilds1/config.meta\n________ test_explicit_refresh_rebuilds_missing_index[rows0-True-None] _________\nunit_tests/test_write_statistics.py:224: in test_explicit_refresh_rebuilds_missing_index\n    assert os.path.exists(path + '.idx'), 'refresh must rebuild before measuring'\nE   AssertionError: refresh must rebuild before measuring\nE   assert False\nE    +  where False = <function exists at 0x10077bce0>(('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-721/test_explicit_refresh_rebuilds2/region.table.jsonl' + '.idx'))\nE    +    where <function exists at 0x10077bce0> = <module 'posixpath' (frozen)>.exists\nE    +      where <module 'posixpath' (frozen)> = os.path\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:122 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-721/test_explicit_refresh_rebuilds2/config.meta\nWARNING  jsonldb.folderdb:folderdb.py:139 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-721/test_explicit_refresh_rebuilds2/db.meta\n__________ test_explicit_refresh_rebuilds_missing_index[rows0-True-1] __________\nunit_tests/test_write_statistics.py:224: in test_explicit_refresh_rebuilds_missing_index\n    assert os.path.exists(path + '.idx'), 'refresh must rebuild before measuring'\nE   AssertionError: refresh must rebuild before measuring\nE   assert False\nE    +  where False = <function exists at 0x10077bce0>(('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-721/test_explicit_refresh_rebuilds3/region/region.table.jsonl' + '.idx'))\nE    +    where <function exists at 0x10077bce0> = <module 'posixpath' (frozen)>.exists\nE    +      where <module 'posixpath' (frozen)> = os.path\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:205 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-721/test_explicit_refresh_rebuilds3/h.meta\nWARNING  jsonldb.folderdb:folderdb.py:122 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-721/test_explicit_refresh_rebuilds3/config.meta\n________ test_explicit_refresh_rebuilds_missing_index[rows1-False-None] ________\nunit_tests/test_write_statistics.py:224: in test_explicit_refresh_rebuilds_missing_index\n    assert os.path.exists(path + '.idx'), 'refresh must rebuild before measuring'\nE   AssertionError: refresh must rebuild before measuring\nE   assert False\nE    +  where False = <function exists at 0x10077bce0>(('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-721/test_explicit_refresh_rebuilds4/region.table.jsonl' + '.idx'))\nE    +    where <function exists at 0x10077bce0> = <module 'posixpath' (frozen)>.exists\nE    +      where <module 'posixpath' (frozen)> = os.path\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:122 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-721/test_explicit_refresh_rebuilds4/config.meta\nWARNING  jsonldb.folderdb:folderdb.py:139 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-721/test_explicit_refresh_rebuilds4/db.meta\n_________ test_explicit_refresh_rebuilds_missing_index[rows1-False-1] __________\nunit_tests/test_write_statistics.py:224: in test_explicit_refresh_rebuilds_missing_index\n    assert os.path.exists(path + '.idx'), 'refresh must rebuild before measuring'\nE   AssertionError: refresh must rebuild before measuring\nE   assert False\nE    +  where False = <function exists at 0x10077bce0>(('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-721/test_explicit_refresh_rebuilds5/region/region.table.jsonl' + '.idx'))\nE    +    where <function exists at 0x10077bce0> = <module 'posixpath' (frozen)>.exists\nE    +      where <module 'posixpath' (frozen)> = os.path\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:205 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-721/test_explicit_refresh_rebuilds5/h.meta\nWARNING  jsonldb.folderdb:folderdb.py:122 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-721/test_explicit_refresh_rebuilds5/config.meta\n________ test_explicit_refresh_rebuilds_missing_index[rows1-True-None] _________\nunit_tests/test_write_statistics.py:224: in test_explicit_refresh_rebuilds_missing_index\n    assert os.path.exists(path + '.idx'), 'refresh must rebuild before measuring'\nE   AssertionError: refresh must rebuild before measuring\nE   assert False\nE    +  where False = <function exists at 0x10077bce0>(('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-721/test_explicit_refresh_rebuilds6/region.table.jsonl' + '.idx'))\nE    +    where <function exists at 0x10077bce0> = <module 'posixpath' (frozen)>.exists\nE    +      where <module 'posixpath' (frozen)> = os.path\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:122 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-721/test_explicit_refresh_rebuilds6/config.meta\nWARNING  jsonldb.folderdb:folderdb.py:139 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-721/test_explicit_refresh_rebuilds6/db.meta\n__________ test_explicit_refresh_rebuilds_missing_index[rows1-True-1] __________\nunit_tests/test_write_statistics.py:224: in test_explicit_refresh_rebuilds_missing_index\n    assert os.path.exists(path + '.idx'), 'refresh must rebuild before measuring'\nE   AssertionError: refresh must rebuild before measuring\nE   assert False\nE    +  where False = <function exists at 0x10077bce0>(('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-721/test_explicit_refresh_rebuilds7/region/region.table.jsonl' + '.idx'))\nE    +    where <function exists at 0x10077bce0> = <module 'posixpath' (frozen)>.exists\nE    +      where <module 'posixpath' (frozen)> = os.path\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:205 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-721/test_explicit_refresh_rebuilds7/h.meta\nWARNING  jsonldb.folderdb:folderdb.py:122 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-721/test_explicit_refresh_rebuilds7/config.meta\n=========================== short test summary info ============================\nFAILED unit_tests/test_write_statistics.py::test_explicit_refresh_rebuilds_missing_index[rows0-False-None]\nFAILED unit_tests/test_write_statistics.py::test_explicit_refresh_rebuilds_missing_index[rows0-False-1]\nFAILED unit_tests/test_write_statistics.py::test_explicit_refresh_rebuilds_missing_index[rows0-True-None]\nFAILED unit_tests/test_write_statistics.py::test_explicit_refresh_rebuilds_missing_index[rows0-True-1]\nFAILED unit_tests/test_write_statistics.py::test_explicit_refresh_rebuilds_missing_index[rows1-False-None]\nFAILED unit_tests/test_write_statistics.py::test_explicit_refresh_rebuilds_missing_index[rows1-False-1]\nFAILED unit_tests/test_write_statistics.py::test_explicit_refresh_rebuilds_missing_index[rows1-True-None]\nFAILED unit_tests/test_write_statistics.py::test_explicit_refresh_rebuilds_missing_index[rows1-True-1]\n8 failed, 49 deselected in 0.20s",
            "truncated": false,
            "captured_bytes": 10419
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "missing-index-refresh",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider --tb=short unit_tests/test_write_statistics.py -k explicit_refresh_rebuilds_missing_index",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "--tb=short",
            "unit_tests/test_write_statistics.py",
            "-k",
            "explicit_refresh_rebuilds_missing_index"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T14:24:53.001Z",
          "completed_at": "2026-09-05T14:24:53.352Z",
          "duration_ms": 351,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@c3d3e9dbfce8d3a106f266fa4867d2f962db418b",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                                  [100%]stdout: \n8 passed, 49 deselected in 0.23s",
            "truncated": false,
            "captured_bytes": 113
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "metadata-and-statistics-regression",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider --tb=short unit_tests/test_write_statistics.py unit_tests/test_metadata_upkeep.py unit_tests/test_index_cache.py unit_tests/test_folderdb.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "--tb=short",
            "unit_tests/test_write_statistics.py",
            "unit_tests/test_metadata_upkeep.py",
            "unit_tests/test_index_cache.py",
            "unit_tests/test_folderdb.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T14:24:53.552Z",
          "completed_at": "2026-09-05T14:24:54.415Z",
          "duration_ms": 863,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@c3d3e9dbfce8d3a106f266fa4867d2f962db418b",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 51%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:     [100%]stdout: \n141 passed in 0.74s",
            "truncated": false,
            "captured_bytes": 180
          }
        },
        {
          "version": 1,
          "id": "V-004",
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
          "started_at": "2026-09-05T14:25:08.568Z",
          "completed_at": "2026-09-05T14:25:08.609Z",
          "duration_ms": 41,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@c3d3e9dbfce8d3a106f266fa4867d2f962db418b",
          "output": {
            "text": "stdout: folderdb.py: 1191 /1250",
            "truncated": false,
            "captured_bytes": 24
          }
        }
      ],
      "acceptance_evidence": [
        {
          "version": 1,
          "id": "V-002",
          "label": "missing-index-refresh",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider --tb=short unit_tests/test_write_statistics.py -k explicit_refresh_rebuilds_missing_index",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "--tb=short",
            "unit_tests/test_write_statistics.py",
            "-k",
            "explicit_refresh_rebuilds_missing_index"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T14:24:53.001Z",
          "completed_at": "2026-09-05T14:24:53.352Z",
          "duration_ms": 351,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@c3d3e9dbfce8d3a106f266fa4867d2f962db418b",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                                  [100%]stdout: \n8 passed, 49 deselected in 0.23s",
            "truncated": false,
            "captured_bytes": 113
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "metadata-and-statistics-regression",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider --tb=short unit_tests/test_write_statistics.py unit_tests/test_metadata_upkeep.py unit_tests/test_index_cache.py unit_tests/test_folderdb.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "--tb=short",
            "unit_tests/test_write_statistics.py",
            "unit_tests/test_metadata_upkeep.py",
            "unit_tests/test_index_cache.py",
            "unit_tests/test_folderdb.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T14:24:53.552Z",
          "completed_at": "2026-09-05T14:24:54.415Z",
          "duration_ms": 863,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@c3d3e9dbfce8d3a106f266fa4867d2f962db418b",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 51%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:     [100%]stdout: \n141 passed in 0.74s",
            "truncated": false,
            "captured_bytes": 180
          }
        },
        {
          "version": 1,
          "id": "V-004",
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
          "started_at": "2026-09-05T14:25:08.568Z",
          "completed_at": "2026-09-05T14:25:08.609Z",
          "duration_ms": 41,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@c3d3e9dbfce8d3a106f266fa4867d2f962db418b",
          "output": {
            "text": "stdout: folderdb.py: 1191 /1250",
            "truncated": false,
            "captured_bytes": 24
          }
        }
      ]
    }
