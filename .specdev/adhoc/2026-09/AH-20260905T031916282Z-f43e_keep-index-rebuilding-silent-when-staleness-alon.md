# Adhoc AH-20260905T031916282Z-f43e

- Scope: Keep index rebuilding silent when staleness alone triggers repair in the ordinary loader and lint, while preserving corruption and damaged-row findings
- Title: Silence routine stale-index rebuild warnings
- Started: 2026-09-05T03:19:16.282Z
- Completed: 2026-09-05T03:21:52.333Z
- Starting working tree: Clean.

## Outcome

Suppressed rebuild warnings only when the reason is stale in both ensure_index_exists and the lint index loader. Index rebuilding still runs; missing, empty, and detected corrupt indexes retain their warnings, and skipped or removed damaged rows remain reported. Added 10 integration cases covering open and lint: stale repair yields header-only integrity reports, empty/unparseable/non-object index corruption remains visible, and stale rebuilds preserve damaged-row findings. Four new cases failed before the fix; all 10 pass after it. Focused report, warning-channel, storage, and lint suites pass 89 tests; git diff --check passes and jsonlfile.py remains at its 950-line cap.

## Focused workflow coexistence

No focused Assignment or Mission coexistence was recorded.

## Delivery path facts

### Requested adopted paths

None.

### Committed paths

- `.specdev/adhoc/2026-09/AH-20260905T031916282Z-f43e_keep-index-rebuilding-silent-when-staleness-alon.md`
- `jsonldb/jsonlfile.py`
- `unit_tests/test_reports.py`

### Rejected paths

None.

### Remaining owned paths

None.

## Verification summary

No manual verification summary was supplied.

## Verification attempt history

- **stale-index-reports: failed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_reports.py -k distinguish_staleness or stale_index_rebuild` (527 ms, working-tree@440da7d0e6f44e0d37a965ce4ea41110e6ee55d6)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 1
  - Output:

    sage in caplog.messages)
    E       assert not True
    E        +  where True = any(<generator object test_stale_index_rebuild_still_reports_damaged_rows.<locals>.<genexpr> at 0x1048d25a0>)
    
    unit_tests/test_reports.py:74: AssertionError
    ------------------------------ Captured log call -------------------------------
    WARNING  jsonldb.folderdb:folderdb.py:122 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-659/test_stale_index_rebuild_still1/config.meta
    WARNING  jsonldb.folderdb:folderdb.py:139 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-659/test_stale_index_rebuild_still1/db.meta
    WARNING  jsonldb.jsonlfile:jsonlfile.py:149 rebuilt missing index /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-659/test_stale_index_rebuild_still1/db.meta.idx
    WARNING  jsonldb.jsonlfile:jsonlfile.py:206 rebuilt stale index /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-659/test_stale_index_rebuild_still1/table.jsonl.idx
    WARNING  jsonldb.jsonlfile:jsonlfile.py:248 lint removed 11 bytes from /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-659/test_stale_index_rebuild_still1/table.jsonl at byte 18
    WARNING  jsonldb.jsonlfile:jsonlfile.py:283 lint repaired layout in /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-659/test_stale_index_rebuild_still1/table.jsonl
    =========================== short test summary info ============================
    FAILED unit_tests/test_reports.py::test_index_rebuild_reports_distinguish_staleness_from_corruption[{}-stale-open]
    FAILED unit_tests/test_reports.py::test_index_rebuild_reports_distinguish_staleness_from_corruption[{}-stale-lint]
    FAILED unit_tests/test_reports.py::test_stale_index_rebuild_still_reports_damaged_rows[open]
    FAILED unit_tests/test_reports.py::test_stale_index_rebuild_still_reports_damaged_rows[lint]
    4 failed, 6 passed, 5 deselected in 0.39s
- **stale-index-reports: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_reports.py -k distinguish_staleness or stale_index_rebuild` (464 ms, working-tree@440da7d0e6f44e0d37a965ce4ea41110e6ee55d6)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                                [100%]stdout: 
    10 passed, 5 deselected in 0.34s
- **warning-regressions: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_reports.py unit_tests/test_warning_channels.py unit_tests/test_jsonlfile.py unit_tests/test_lint_integrity.py` (1113 ms, working-tree@440da7d0e6f44e0d37a965ce4ea41110e6ee55d6)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 80%]
    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                         [100%]stdout: 
    89 passed in 0.98s

## Current acceptance evidence

- **stale-index-reports: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_reports.py -k distinguish_staleness or stale_index_rebuild` (464 ms, working-tree@440da7d0e6f44e0d37a965ce4ea41110e6ee55d6)
- **warning-regressions: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_reports.py unit_tests/test_warning_channels.py unit_tests/test_jsonlfile.py unit_tests/test_lint_integrity.py` (1113 ms, working-tree@440da7d0e6f44e0d37a965ce4ea41110e6ee55d6)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 80%]
    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                         [100%]stdout: 
    89 passed in 0.98s

## Structured verification

    {
      "version": 1,
      "path_facts": {
        "requested": [],
        "committed": [
          ".specdev/adhoc/2026-09/AH-20260905T031916282Z-f43e_keep-index-rebuilding-silent-when-staleness-alon.md",
          "jsonldb/jsonlfile.py",
          "unit_tests/test_reports.py"
        ],
        "rejected": [],
        "remaining": []
      },
      "attempt_history": [
        {
          "version": 1,
          "id": "V-001",
          "label": "stale-index-reports",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_reports.py -k distinguish_staleness or stale_index_rebuild",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_reports.py",
            "-k",
            "distinguish_staleness or stale_index_rebuild"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T03:20:51.113Z",
          "completed_at": "2026-09-05T03:20:51.641Z",
          "duration_ms": 527,
          "exit_status": 1,
          "status": "failed",
          "tested_revision": "working-tree@440da7d0e6f44e0d37a965ce4ea41110e6ee55d6",
          "output": {
            "text": "stdout: Fstdout: Fstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: Fstdout: Fstdout:                                                                [100%]stdout: \n=================================== FAILURES ===================================\n_ test_index_rebuild_reports_distinguish_staleness_from_corruption[{}-stale-open] _\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-659/test_index_rebuild_reports_dis0')\ncaplog = <_pytest.logging.LogCaptureFixture object at 0x1048030e0>\noperation = 'open', index_bytes = b'{}', reason = 'stale'\n\n    @pytest.mark.parametrize('operation', ['open', 'lint'])\n    @pytest.mark.parametrize('index_bytes, reason', [\n        (b'{}', 'stale'), (b'', 'empty'), (b'not-json', 'corrupt'), (b'[]', 'corrupt'),\n    ])\n    def test_index_rebuild_reports_distinguish_staleness_from_corruption(\n            tmp_path, caplog, operation, index_bytes, reason):\n        db = FolderDB(str(tmp_path))\n        rows = {'a': {'value': 1}, 'b': {'value': 2}}\n        db.overwrite_dict('table', rows)\n        path = tmp_path / 'table.jsonl'\n        before = path.read_bytes()\n        index_path = tmp_path / 'table.jsonl.idx'\n        index_path.write_bytes(index_bytes)\n        timestamp = 0 if reason == 'stale' else path.stat().st_mtime_ns + 1_000_000_000\n        os.utime(index_path, ns=(timestamp, timestamp))\n        caplog.set_level(logging.WARNING, logger='jsonldb')\n        caplog.clear()\n    \n        if operation == 'open':\n            os.utime(tmp_path / 'db.meta', ns=(0, 0))\n            FolderDB(str(tmp_path))\n        else:\n            db.lint_db()\n    \n        report_path = tmp_path / '.jsonldb' / ('integrity.log' if operation == 'open' else 'lint.log')\n        assert list(orjson.loads(index_path.read_bytes())) == ['a', 'b']\n        assert index_path.stat().st_mtime_ns >= path.stat().st_mtime_ns\n        assert path.read_bytes() == before\n>       assert not any('rebuilt stale index' in message for message in caplog.messages)\nE       assert not True\nE        +  where True = any(<generator object test_index_rebuild_reports_distinguish_staleness_from_corruption.<locals>.<genexpr> at 0x1048d05f0>)\n\nunit_tests/test_reports.py:43: AssertionError\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:122 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-659/test_index_rebuild_reports_dis0/config.meta\nWARNING  jsonldb.folderdb:folderdb.py:139 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-659/test_index_rebuild_reports_dis0/db.meta\nWARNING  jsonldb.jsonlfile:jsonlfile.py:149 rebuilt missing index /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-659/test_index_rebuild_reports_dis0/db.meta.idx\nWARNING  jsonldb.jsonlfile:jsonlfile.py:149 rebuilt stale index /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-659/test_index_rebuild_reports_dis0/table.jsonl.idx\n_ test_index_rebuild_reports_distinguish_staleness_from_corruption[{}-stale-lint] _\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-659/test_index_rebuild_reports_dis1')\ncaplog = <_pytest.logging.LogCaptureFixture object at 0x1048b7b30>\noperation = 'lint', index_bytes = b'{}', reason = 'stale'\n\n    @pytest.mark.parametrize('operation', ['open', 'lint'])\n    @pytest.mark.parametrize('index_bytes, reason', [\n        (b'{}', 'stale'), (b'', 'empty'), (b'not-json', 'corrupt'), (b'[]', 'corrupt'),\n    ])\n    def test_index_rebuild_reports_distinguish_staleness_from_corruption(\n            tmp_path, caplog, operation, index_bytes, reason):\n        db = FolderDB(str(tmp_path))\n        rows = {'a': {'value': 1}, 'b': {'value': 2}}\n        db.overwrite_dict('table', rows)\n        path = tmp_path / 'table.jsonl'\n        before = path.read_bytes()\n        index_path = tmp_path / 'table.jsonl.idx'\n        index_path.write_bytes(index_bytes)\n        timestamp = 0 if reason == 'stale' else path.stat().st_mtime_ns + 1_000_000_000\n        os.utime(index_path, ns=(timestamp, timestamp))\n        caplog.set_level(logging.WARNING, logger='jsonldb')\n        caplog.clear()\n    \n        if operation == 'open':\n            os.utime(tmp_path / 'db.meta', ns=(0, 0))\n            FolderDB(str(tmp_path))\n        else:\n            db.lint_db()\n    \n        report_path = tmp_path / '.jsonldb' / ('integrity.log' if operation == 'open' else 'lint.log')\n        assert list(orjson.loads(index_path.read_bytes())) == ['a', 'b']\n        assert index_path.stat().st_mtime_ns >= path.stat().st_mtime_ns\n        assert path.read_bytes() == before\n>       assert not any('rebuilt stale index' in message for message in caplog.messages)\nE       assert not True\nE        +  where True = any(<generator object test_index_rebuild_reports_distinguish_staleness_from_corruption.<locals>.<genexpr> at 0x1048d1490>)\n\nunit_tests/test_reports.py:43: AssertionError\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:122 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-659/test_index_rebuild_reports_dis1/config.meta\nWARNING  jsonldb.folderdb:folderdb.py:139 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-659/test_index_rebuild_reports_dis1/db.meta\nWARNING  jsonldb.jsonlfile:jsonlfile.py:149 rebuilt missing index /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-659/test_index_rebuild_reports_dis1/db.meta.idx\nWARNING  jsonldb.jsonlfile:jsonlfile.py:206 rebuilt stale index /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-659/test_index_rebuild_reports_dis1/table.jsonl.idx\n__________ test_stale_index_rebuild_still_reports_damaged_rows[open] ___________\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-659/test_stale_index_rebuild_still0')\ncaplog = <_pytest.logging.LogCaptureFixture object at 0x104988200>\noperation = 'open'\n\n    @pytest.mark.parametrize('operation', ['open', 'lint'])\n    def test_stale_index_rebuild_still_reports_damaged_rows(tmp_path, caplog, operation):\n        db = FolderDB(str(tmp_path))\n        db.overwrite_dict('table', {'a': {'value': 1}})\n        path = tmp_path / 'table.jsonl'\n        clean = path.read_bytes()\n        path.write_bytes(clean + b'broken-row\\n')\n        os.utime(tmp_path / 'table.jsonl.idx', ns=(0, 0))\n        caplog.set_level(logging.WARNING, logger='jsonldb')\n        caplog.clear()\n    \n        if operation == 'open':\n            os.utime(tmp_path / 'db.meta', ns=(0, 0))\n            FolderDB(str(tmp_path))\n            report = tmp_path / '.jsonldb' / 'integrity.log'\n            assert 'kind=invalid_json' in report.read_text()\n            assert path.read_bytes() == clean + b'broken-row\\n'\n        else:\n            db.lint_db()\n            report = tmp_path / '.jsonldb' / 'lint.log'\n            assert 'kind=lint_removed' in report.read_text()\n            assert path.read_bytes() == clean\n>       assert not any('rebuilt stale index' in message for message in caplog.messages)\nE       assert not True\nE        +  where True = any(<generator object test_stale_index_rebuild_still_reports_damaged_rows.<locals>.<genexpr> at 0x104985630>)\n\nunit_tests/test_reports.py:74: AssertionError\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:122 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-659/test_stale_index_rebuild_still0/config.meta\nWARNING  jsonldb.folderdb:folderdb.py:139 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-659/test_stale_index_rebuild_still0/db.meta\nWARNING  jsonldb.jsonlfile:jsonlfile.py:149 rebuilt missing index /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-659/test_stale_index_rebuild_still0/db.meta.idx\nWARNING  jsonldb.jsonlfile:jsonlfile.py:149 rebuilt stale index /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-659/test_stale_index_rebuild_still0/table.jsonl.idx\nWARNING  jsonldb.jsonlfile:jsonlfile.py:51 invalid JSON line in /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-659/test_stale_index_rebuild_still0/table.jsonl at byte 18: broken-row\n__________ test_stale_index_rebuild_still_reports_damaged_rows[lint] ___________\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-659/test_stale_index_rebuild_still1')\ncaplog = <_pytest.logging.LogCaptureFixture object at 0x104988110>\noperation = 'lint'\n\n    @pytest.mark.parametrize('operation', ['open', 'lint'])\n    def test_stale_index_rebuild_still_reports_damaged_rows(tmp_path, caplog, operation):\n        db = FolderDB(str(tmp_path))\n        db.overwrite_dict('table', {'a': {'value': 1}})\n        path = tmp_path / 'table.jsonl'\n        clean = path.read_bytes()\n        path.write_bytes(clean + b'broken-row\\n')\n        os.utime(tmp_path / 'table.jsonl.idx', ns=(0, 0))\n        caplog.set_level(logging.WARNING, logger='jsonldb')\n        caplog.clear()\n    \n        if operation == 'open':\n            os.utime(tmp_path / 'db.meta', ns=(0, 0))\n            FolderDB(str(tmp_path))\n            report = tmp_path / '.jsonldb' / 'integrity.log'\n            assert 'kind=invalid_json' in report.read_text()\n            assert path.read_bytes() == clean + b'broken-row\\n'\n        else:\n            db.lint_db()\n            report = tmp_path / '.jsonldb' / 'lint.log'\n            assert 'kind=lint_removed' in report.read_text()\n            assert path.read_bytes() == clean\n>       assert not any('rebuilt stale index' in message for message in caplog.messages)\nE       assert not True\nE        +  where True = any(<generator object test_stale_index_rebuild_still_reports_damaged_rows.<locals>.<genexpr> at 0x1048d25a0>)\n\nunit_tests/test_reports.py:74: AssertionError\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:122 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-659/test_stale_index_rebuild_still1/config.meta\nWARNING  jsonldb.folderdb:folderdb.py:139 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-659/test_stale_index_rebuild_still1/db.meta\nWARNING  jsonldb.jsonlfile:jsonlfile.py:149 rebuilt missing index /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-659/test_stale_index_rebuild_still1/db.meta.idx\nWARNING  jsonldb.jsonlfile:jsonlfile.py:206 rebuilt stale index /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-659/test_stale_index_rebuild_still1/table.jsonl.idx\nWARNING  jsonldb.jsonlfile:jsonlfile.py:248 lint removed 11 bytes from /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-659/test_stale_index_rebuild_still1/table.jsonl at byte 18\nWARNING  jsonldb.jsonlfile:jsonlfile.py:283 lint repaired layout in /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-659/test_stale_index_rebuild_still1/table.jsonl\n=========================== short test summary info ============================\nFAILED unit_tests/test_reports.py::test_index_rebuild_reports_distinguish_staleness_from_corruption[{}-stale-open]\nFAILED unit_tests/test_reports.py::test_index_rebuild_reports_distinguish_staleness_from_corruption[{}-stale-lint]\nFAILED unit_tests/test_reports.py::test_stale_index_rebuild_still_reports_damaged_rows[open]\nFAILED unit_tests/test_reports.py::test_stale_index_rebuild_still_reports_damaged_rows[lint]\n4 failed, 6 passed, 5 deselected in 0.39s",
            "truncated": false,
            "captured_bytes": 12053
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "stale-index-reports",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_reports.py -k distinguish_staleness or stale_index_rebuild",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_reports.py",
            "-k",
            "distinguish_staleness or stale_index_rebuild"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T03:21:10.511Z",
          "completed_at": "2026-09-05T03:21:10.975Z",
          "duration_ms": 464,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@440da7d0e6f44e0d37a965ce4ea41110e6ee55d6",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                                [100%]stdout: \n10 passed, 5 deselected in 0.34s",
            "truncated": false,
            "captured_bytes": 113
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "warning-regressions",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_reports.py unit_tests/test_warning_channels.py unit_tests/test_jsonlfile.py unit_tests/test_lint_integrity.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_reports.py",
            "unit_tests/test_warning_channels.py",
            "unit_tests/test_jsonlfile.py",
            "unit_tests/test_lint_integrity.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T03:21:29.621Z",
          "completed_at": "2026-09-05T03:21:30.733Z",
          "duration_ms": 1113,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@440da7d0e6f44e0d37a965ce4ea41110e6ee55d6",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 80%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                         [100%]stdout: \n89 passed in 0.98s",
            "truncated": false,
            "captured_bytes": 179
          }
        }
      ],
      "acceptance_evidence": [
        {
          "version": 1,
          "id": "V-002",
          "label": "stale-index-reports",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_reports.py -k distinguish_staleness or stale_index_rebuild",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_reports.py",
            "-k",
            "distinguish_staleness or stale_index_rebuild"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T03:21:10.511Z",
          "completed_at": "2026-09-05T03:21:10.975Z",
          "duration_ms": 464,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@440da7d0e6f44e0d37a965ce4ea41110e6ee55d6",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                                [100%]stdout: \n10 passed, 5 deselected in 0.34s",
            "truncated": false,
            "captured_bytes": 113
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "warning-regressions",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_reports.py unit_tests/test_warning_channels.py unit_tests/test_jsonlfile.py unit_tests/test_lint_integrity.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_reports.py",
            "unit_tests/test_warning_channels.py",
            "unit_tests/test_jsonlfile.py",
            "unit_tests/test_lint_integrity.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T03:21:29.621Z",
          "completed_at": "2026-09-05T03:21:30.733Z",
          "duration_ms": 1113,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@440da7d0e6f44e0d37a965ce4ea41110e6ee55d6",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 80%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                         [100%]stdout: \n89 passed in 0.98s",
            "truncated": false,
            "captured_bytes": 179
          }
        }
      ]
    }
