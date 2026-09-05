# Adhoc AH-20260905T033340272Z-4c89

- Scope: Publish empty db.meta through the ordinary JSONL save path so empty databases immediately have zero-byte metadata and a canonical empty index, including metadata rebuilds after the last table disappears
- Title: Create canonical indexed empty database metadata
- Started: 2026-09-05T03:33:40.272Z
- Completed: 2026-09-05T03:34:58.071Z
- Starting working tree: Clean.

## Outcome

Removed build_dbmeta's direct blank-line writer so empty metadata follows the ordinary JSONL save path. Empty flat and hierarchical databases now immediately contain zero-byte db.meta and a canonical {} db.meta.idx. Rebuilding metadata, reopening after the final table disappears, and clearing a flat database replace leftover indexes correctly. Five new regression cases failed before the change and pass after it, including an initial write without missing-index warnings. Focused metadata upkeep, folder, report, hierarchy recovery, and slot migration suites pass 87 tests; git diff --check passes.

## Focused workflow coexistence

No focused Assignment or Mission coexistence was recorded.

## Delivery path facts

### Requested adopted paths

None.

### Committed paths

- `.specdev/adhoc/2026-09/AH-20260905T033340272Z-4c89_publish-empty-db-meta-through-the-ordinary-jsonl.md`
- `jsonldb/folderdb.py`
- `unit_tests/test_metadata_upkeep.py`

### Rejected paths

None.

### Remaining owned paths

None.

## Verification summary

No manual verification summary was supplied.

## Verification attempt history

- **empty-dbmeta: failed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_metadata_upkeep.py -k empty_database_metadata` (325 ms, working-tree@c22a729951e0770dca28048cee889c8c655e898e)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 1
  - Output:

    leftover_index(tmp_path, operation):
            db = FolderDB(str(tmp_path))
            db.overwrite_dict('last', {'row': {'value': 1}})
            (tmp_path / 'last.jsonl').unlink()
            (tmp_path / 'last.jsonl.idx').unlink()
            assert b'last' in (tmp_path / 'db.meta.idx').read_bytes()
        
            if operation == 'rebuild':
                db.build_dbmeta()
            elif operation == 'reopen':
                os.utime(tmp_path / 'db.meta', ns=(0, 0))
                FolderDB(str(tmp_path))
            else:
                db.clear_folder(force=True)
        
    >       assert (tmp_path / 'db.meta').read_bytes() == b''
    E       AssertionError: assert b'\n' == b''
    E         
    E         Use -v to get more diff
    
    unit_tests/test_metadata_upkeep.py:39: AssertionError
    ------------------------------ Captured log call -------------------------------
    WARNING  jsonldb.folderdb:folderdb.py:122 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-664/test_empty_database_metadata_r2/config.meta
    WARNING  jsonldb.folderdb:folderdb.py:139 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-664/test_empty_database_metadata_r2/db.meta
    WARNING  jsonldb.jsonlfile:jsonlfile.py:148 rebuilt missing index /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-664/test_empty_database_metadata_r2/db.meta.idx
    =========================== short test summary info ============================
    FAILED unit_tests/test_metadata_upkeep.py::test_empty_database_metadata_is_immediately_canonical[None]
    FAILED unit_tests/test_metadata_upkeep.py::test_empty_database_metadata_is_immediately_canonical[2]
    FAILED unit_tests/test_metadata_upkeep.py::test_empty_database_metadata_replaces_leftover_index[rebuild]
    FAILED unit_tests/test_metadata_upkeep.py::test_empty_database_metadata_replaces_leftover_index[reopen]
    FAILED unit_tests/test_metadata_upkeep.py::test_empty_database_metadata_replaces_leftover_index[clear]
    5 failed, 3 deselected in 0.21s
- **empty-dbmeta: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_metadata_upkeep.py -k empty_database_metadata` (263 ms, working-tree@c22a729951e0770dca28048cee889c8c655e898e)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                                     [100%]stdout: 
    5 passed, 3 deselected in 0.15s
- **metadata-regressions: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_metadata_upkeep.py unit_tests/test_folderdb.py unit_tests/test_reports.py unit_tests/test_hierarchy_recovery.py unit_tests/test_metadata_slot_migration.py` (857 ms, working-tree@c22a729951e0770dca28048cee889c8c655e898e)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 82%]
    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                           [100%]stdout: 
    87 passed in 0.74s

## Current acceptance evidence

- **empty-dbmeta: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_metadata_upkeep.py -k empty_database_metadata` (263 ms, working-tree@c22a729951e0770dca28048cee889c8c655e898e)
- **metadata-regressions: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_metadata_upkeep.py unit_tests/test_folderdb.py unit_tests/test_reports.py unit_tests/test_hierarchy_recovery.py unit_tests/test_metadata_slot_migration.py` (857 ms, working-tree@c22a729951e0770dca28048cee889c8c655e898e)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 82%]
    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                           [100%]stdout: 
    87 passed in 0.74s

## Structured verification

    {
      "version": 1,
      "path_facts": {
        "requested": [],
        "committed": [
          ".specdev/adhoc/2026-09/AH-20260905T033340272Z-4c89_publish-empty-db-meta-through-the-ordinary-jsonl.md",
          "jsonldb/folderdb.py",
          "unit_tests/test_metadata_upkeep.py"
        ],
        "rejected": [],
        "remaining": []
      },
      "attempt_history": [
        {
          "version": 1,
          "id": "V-001",
          "label": "empty-dbmeta",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_metadata_upkeep.py -k empty_database_metadata",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_metadata_upkeep.py",
            "-k",
            "empty_database_metadata"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T03:34:25.170Z",
          "completed_at": "2026-09-05T03:34:25.495Z",
          "duration_ms": 325,
          "exit_status": 1,
          "status": "failed",
          "tested_revision": "working-tree@c22a729951e0770dca28048cee889c8c655e898e",
          "output": {
            "text": "stdout: Fstdout: Fstdout: Fstdout: Fstdout: Fstdout:                                                                     [100%]stdout: \n=================================== FAILURES ===================================\n_________ test_empty_database_metadata_is_immediately_canonical[None] __________\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-664/test_empty_database_metadata_i0')\ncaplog = <_pytest.logging.LogCaptureFixture object at 0x10946eb40>\nhierarchy_depth = None\n\n    @pytest.mark.parametrize('hierarchy_depth', [None, 2])\n    def test_empty_database_metadata_is_immediately_canonical(tmp_path, caplog, hierarchy_depth):\n        db = FolderDB(str(tmp_path), hierarchy_depth=hierarchy_depth)\n    \n>       assert (tmp_path / 'db.meta').read_bytes() == b''\nE       AssertionError: assert b'\\n' == b''\nE         \nE         Use -v to get more diff\n\nunit_tests/test_metadata_upkeep.py:13: AssertionError\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:122 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-664/test_empty_database_metadata_i0/config.meta\nWARNING  jsonldb.folderdb:folderdb.py:139 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-664/test_empty_database_metadata_i0/db.meta\n___________ test_empty_database_metadata_is_immediately_canonical[2] ___________\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-664/test_empty_database_metadata_i1')\ncaplog = <_pytest.logging.LogCaptureFixture object at 0x10946ee70>\nhierarchy_depth = 2\n\n    @pytest.mark.parametrize('hierarchy_depth', [None, 2])\n    def test_empty_database_metadata_is_immediately_canonical(tmp_path, caplog, hierarchy_depth):\n        db = FolderDB(str(tmp_path), hierarchy_depth=hierarchy_depth)\n    \n>       assert (tmp_path / 'db.meta').read_bytes() == b''\nE       AssertionError: assert b'\\n' == b''\nE         \nE         Use -v to get more diff\n\nunit_tests/test_metadata_upkeep.py:13: AssertionError\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:205 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-664/test_empty_database_metadata_i1/h.meta\nWARNING  jsonldb.folderdb:folderdb.py:122 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-664/test_empty_database_metadata_i1/config.meta\n________ test_empty_database_metadata_replaces_leftover_index[rebuild] _________\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-664/test_empty_database_metadata_r0')\noperation = 'rebuild'\n\n    @pytest.mark.parametrize('operation', ['rebuild', 'reopen', 'clear'])\n    def test_empty_database_metadata_replaces_leftover_index(tmp_path, operation):\n        db = FolderDB(str(tmp_path))\n        db.overwrite_dict('last', {'row': {'value': 1}})\n        (tmp_path / 'last.jsonl').unlink()\n        (tmp_path / 'last.jsonl.idx').unlink()\n        assert b'last' in (tmp_path / 'db.meta.idx').read_bytes()\n    \n        if operation == 'rebuild':\n            db.build_dbmeta()\n        elif operation == 'reopen':\n            os.utime(tmp_path / 'db.meta', ns=(0, 0))\n            FolderDB(str(tmp_path))\n        else:\n            db.clear_folder(force=True)\n    \n>       assert (tmp_path / 'db.meta').read_bytes() == b''\nE       AssertionError: assert b'\\n' == b''\nE         \nE         Use -v to get more diff\n\nunit_tests/test_metadata_upkeep.py:39: AssertionError\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:122 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-664/test_empty_database_metadata_r0/config.meta\nWARNING  jsonldb.folderdb:folderdb.py:139 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-664/test_empty_database_metadata_r0/db.meta\nWARNING  jsonldb.jsonlfile:jsonlfile.py:148 rebuilt missing index /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-664/test_empty_database_metadata_r0/db.meta.idx\n_________ test_empty_database_metadata_replaces_leftover_index[reopen] _________\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-664/test_empty_database_metadata_r1')\noperation = 'reopen'\n\n    @pytest.mark.parametrize('operation', ['rebuild', 'reopen', 'clear'])\n    def test_empty_database_metadata_replaces_leftover_index(tmp_path, operation):\n        db = FolderDB(str(tmp_path))\n        db.overwrite_dict('last', {'row': {'value': 1}})\n        (tmp_path / 'last.jsonl').unlink()\n        (tmp_path / 'last.jsonl.idx').unlink()\n        assert b'last' in (tmp_path / 'db.meta.idx').read_bytes()\n    \n        if operation == 'rebuild':\n            db.build_dbmeta()\n        elif operation == 'reopen':\n            os.utime(tmp_path / 'db.meta', ns=(0, 0))\n            FolderDB(str(tmp_path))\n        else:\n            db.clear_folder(force=True)\n    \n>       assert (tmp_path / 'db.meta').read_bytes() == b''\nE       AssertionError: assert b'\\n' == b''\nE         \nE         Use -v to get more diff\n\nunit_tests/test_metadata_upkeep.py:39: AssertionError\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:122 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-664/test_empty_database_metadata_r1/config.meta\nWARNING  jsonldb.folderdb:folderdb.py:139 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-664/test_empty_database_metadata_r1/db.meta\nWARNING  jsonldb.jsonlfile:jsonlfile.py:148 rebuilt missing index /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-664/test_empty_database_metadata_r1/db.meta.idx\n_________ test_empty_database_metadata_replaces_leftover_index[clear] __________\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-664/test_empty_database_metadata_r2')\noperation = 'clear'\n\n    @pytest.mark.parametrize('operation', ['rebuild', 'reopen', 'clear'])\n    def test_empty_database_metadata_replaces_leftover_index(tmp_path, operation):\n        db = FolderDB(str(tmp_path))\n        db.overwrite_dict('last', {'row': {'value': 1}})\n        (tmp_path / 'last.jsonl').unlink()\n        (tmp_path / 'last.jsonl.idx').unlink()\n        assert b'last' in (tmp_path / 'db.meta.idx').read_bytes()\n    \n        if operation == 'rebuild':\n            db.build_dbmeta()\n        elif operation == 'reopen':\n            os.utime(tmp_path / 'db.meta', ns=(0, 0))\n            FolderDB(str(tmp_path))\n        else:\n            db.clear_folder(force=True)\n    \n>       assert (tmp_path / 'db.meta').read_bytes() == b''\nE       AssertionError: assert b'\\n' == b''\nE         \nE         Use -v to get more diff\n\nunit_tests/test_metadata_upkeep.py:39: AssertionError\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:122 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-664/test_empty_database_metadata_r2/config.meta\nWARNING  jsonldb.folderdb:folderdb.py:139 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-664/test_empty_database_metadata_r2/db.meta\nWARNING  jsonldb.jsonlfile:jsonlfile.py:148 rebuilt missing index /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-664/test_empty_database_metadata_r2/db.meta.idx\n=========================== short test summary info ============================\nFAILED unit_tests/test_metadata_upkeep.py::test_empty_database_metadata_is_immediately_canonical[None]\nFAILED unit_tests/test_metadata_upkeep.py::test_empty_database_metadata_is_immediately_canonical[2]\nFAILED unit_tests/test_metadata_upkeep.py::test_empty_database_metadata_replaces_leftover_index[rebuild]\nFAILED unit_tests/test_metadata_upkeep.py::test_empty_database_metadata_replaces_leftover_index[reopen]\nFAILED unit_tests/test_metadata_upkeep.py::test_empty_database_metadata_replaces_leftover_index[clear]\n5 failed, 3 deselected in 0.21s",
            "truncated": false,
            "captured_bytes": 8600
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "empty-dbmeta",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_metadata_upkeep.py -k empty_database_metadata",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_metadata_upkeep.py",
            "-k",
            "empty_database_metadata"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T03:34:38.766Z",
          "completed_at": "2026-09-05T03:34:39.029Z",
          "duration_ms": 263,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@c22a729951e0770dca28048cee889c8c655e898e",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                                     [100%]stdout: \n5 passed, 3 deselected in 0.15s",
            "truncated": false,
            "captured_bytes": 112
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "metadata-regressions",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_metadata_upkeep.py unit_tests/test_folderdb.py unit_tests/test_reports.py unit_tests/test_hierarchy_recovery.py unit_tests/test_metadata_slot_migration.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_metadata_upkeep.py",
            "unit_tests/test_folderdb.py",
            "unit_tests/test_reports.py",
            "unit_tests/test_hierarchy_recovery.py",
            "unit_tests/test_metadata_slot_migration.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T03:34:39.183Z",
          "completed_at": "2026-09-05T03:34:40.041Z",
          "duration_ms": 857,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@c22a729951e0770dca28048cee889c8c655e898e",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 82%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                           [100%]stdout: \n87 passed in 0.74s",
            "truncated": false,
            "captured_bytes": 179
          }
        }
      ],
      "acceptance_evidence": [
        {
          "version": 1,
          "id": "V-002",
          "label": "empty-dbmeta",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_metadata_upkeep.py -k empty_database_metadata",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_metadata_upkeep.py",
            "-k",
            "empty_database_metadata"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T03:34:38.766Z",
          "completed_at": "2026-09-05T03:34:39.029Z",
          "duration_ms": 263,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@c22a729951e0770dca28048cee889c8c655e898e",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                                     [100%]stdout: \n5 passed, 3 deselected in 0.15s",
            "truncated": false,
            "captured_bytes": 112
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "metadata-regressions",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_metadata_upkeep.py unit_tests/test_folderdb.py unit_tests/test_reports.py unit_tests/test_hierarchy_recovery.py unit_tests/test_metadata_slot_migration.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_metadata_upkeep.py",
            "unit_tests/test_folderdb.py",
            "unit_tests/test_reports.py",
            "unit_tests/test_hierarchy_recovery.py",
            "unit_tests/test_metadata_slot_migration.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T03:34:39.183Z",
          "completed_at": "2026-09-05T03:34:40.041Z",
          "duration_ms": 857,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@c22a729951e0770dca28048cee889c8c655e898e",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 82%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                           [100%]stdout: \n87 passed in 0.74s",
            "truncated": false,
            "captured_bytes": 179
          }
        }
      ]
    }
