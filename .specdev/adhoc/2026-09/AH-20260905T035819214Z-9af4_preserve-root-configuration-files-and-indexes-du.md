# Adhoc AH-20260905T035819214Z-9af4

- Scope: Preserve root configuration files and indexes during clear_folder(force=True), remove visible table data and indexes, and reset db.meta while keeping live and reopened settings consistent
- Title: Preserve database configuration when clearing tables
- Started: 2026-09-05T03:58:19.214Z
- Completed: 2026-09-05T04:02:38.937Z
- Starting working tree: Clean.

## Outcome

Implemented the approved configuration-preserving clear behavior. clear_folder(force=True) now removes only visible .jsonl tables and .jsonl.idx sidecars, including orphan table indexes, retains config.meta and h.meta with their indexes and all live settings, prunes empty visible branches, and rebuilds canonical empty db.meta. Unrelated files remain intact; the no-force path still only warns. README documents the behavior. Six regression cases cover flat/custom-delimiter hierarchical layouts, enabled/disabled metadata slots, microsecond precision, unknown configuration fields, control-file byte and timestamp preservation, post-clear writes through live and reopened instances, repeated clearing, and no-force immutability. Five cases failed before the fix and all six pass after it; 91 existing focused regressions also pass (97 total), and git diff --check passes. User notes were excluded through a command-scoped Git exclusion without changing repository ignore files.

## Focused workflow coexistence

No focused Assignment or Mission coexistence was recorded.

## Delivery path facts

### Requested adopted paths

None.

### Committed paths

- `.specdev/adhoc/2026-09/AH-20260905T035819214Z-9af4_preserve-root-configuration-files-and-indexes-du.md`
- `README.md`
- `jsonldb/folderdb.py`
- `unit_tests/test_clear_configuration.py`

### Rejected paths

None.

### Remaining owned paths

None.

## Verification summary

No manual verification summary was supplied.

## Verification attempt history

- **clear-configuration: failed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_clear_configuration.py` (379 ms, working-tree@64ae1bb69264f4ec513c894896b4f6ccebea74e0)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 1
  - Output (bounded tail):

    _ _ _ _ _ _ _ _ _ _ 
    
    self = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-670/test_clear_preserves_unrelated0/external/notes.meta')
    mode = 'rb', buffering = -1, encoding = None, errors = None, newline = None
    
        def open(self, mode='r', buffering=-1, encoding=None,
                 errors=None, newline=None):
            """
            Open the file pointed to by this path and return a file object, as
            the built-in open() function does.
            """
            if "b" not in mode:
                encoding = io.text_encoding(encoding)
    >       return io.open(self, mode, buffering, encoding, errors, newline)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    E       FileNotFoundError: [Errno 2] No such file or directory: '/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-670/test_clear_preserves_unrelated0/external/notes.meta'
    
    /opt/homebrew/Cellar/python@3.12/3.12.12_2/Frameworks/Python.framework/Versions/3.12/lib/python3.12/pathlib.py:1013: FileNotFoundError
    ------------------------------ Captured log call -------------------------------
    WARNING  jsonldb.folderdb:folderdb.py:139 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-670/test_clear_preserves_unrelated0/db.meta
    =========================== short test summary info ============================
    FAILED unit_tests/test_clear_configuration.py::test_clear_preserves_configuration_for_live_and_reopened_instances[None-False]
    FAILED unit_tests/test_clear_configuration.py::test_clear_preserves_configuration_for_live_and_reopened_instances[None-True]
    FAILED unit_tests/test_clear_configuration.py::test_clear_preserves_configuration_for_live_and_reopened_instances[160-False]
    FAILED unit_tests/test_clear_configuration.py::test_clear_preserves_configuration_for_live_and_reopened_instances[160-True]
    FAILED unit_tests/test_clear_configuration.py::test_clear_preserves_unrelated_files_and_is_repeatable
    5 failed, 1 passed in 0.24s
- **clear-configuration: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_clear_configuration.py` (282 ms, working-tree@64ae1bb69264f4ec513c894896b4f6ccebea74e0)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                                    [100%]stdout: 
    6 passed in 0.17s
- **clear-regressions: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_folderdb.py unit_tests/test_metadata_upkeep.py unit_tests/test_reports.py unit_tests/test_hierarchy_recovery.py unit_tests/test_metadata_slot_migration.py` (890 ms, working-tree@64ae1bb69264f4ec513c894896b4f6ccebea74e0)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 79%]
    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                       [100%]stdout: 
    91 passed in 0.77s

## Current acceptance evidence

- **clear-configuration: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_clear_configuration.py` (282 ms, working-tree@64ae1bb69264f4ec513c894896b4f6ccebea74e0)
- **clear-regressions: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_folderdb.py unit_tests/test_metadata_upkeep.py unit_tests/test_reports.py unit_tests/test_hierarchy_recovery.py unit_tests/test_metadata_slot_migration.py` (890 ms, working-tree@64ae1bb69264f4ec513c894896b4f6ccebea74e0)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 79%]
    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                       [100%]stdout: 
    91 passed in 0.77s

## Structured verification

    {
      "version": 1,
      "path_facts": {
        "requested": [],
        "committed": [
          ".specdev/adhoc/2026-09/AH-20260905T035819214Z-9af4_preserve-root-configuration-files-and-indexes-du.md",
          "README.md",
          "jsonldb/folderdb.py",
          "unit_tests/test_clear_configuration.py"
        ],
        "rejected": [],
        "remaining": []
      },
      "attempt_history": [
        {
          "version": 1,
          "id": "V-001",
          "label": "clear-configuration",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_clear_configuration.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_clear_configuration.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T04:01:04.480Z",
          "completed_at": "2026-09-05T04:01:04.860Z",
          "duration_ms": 379,
          "exit_status": 1,
          "status": "failed",
          "tested_revision": "working-tree@64ae1bb69264f4ec513c894896b4f6ccebea74e0",
          "output": {
            "text": "_ _ _ _ _ _ _ \nunit_tests/test_clear_configuration.py:54: in <genexpr>\n    assert all((path.read_bytes(), path.stat().st_mtime_ns) == before\n                ^^^^^^^^^^^^^^^^^\n/opt/homebrew/Cellar/python@3.12/3.12.12_2/Frameworks/Python.framework/Versions/3.12/lib/python3.12/pathlib.py:1019: in read_bytes\n    with self.open(mode='rb') as f:\n         ^^^^^^^^^^^^^^^^^^^^\n_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ \n\nself = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-670/test_clear_preserves_configura0/config.meta')\nmode = 'rb', buffering = -1, encoding = None, errors = None, newline = None\n\n    def open(self, mode='r', buffering=-1, encoding=None,\n             errors=None, newline=None):\n        \"\"\"\n        Open the file pointed to by this path and return a file object, as\n        the built-in open() function does.\n        \"\"\"\n        if \"b\" not in mode:\n            encoding = io.text_encoding(encoding)\n>       return io.open(self, mode, buffering, encoding, errors, newline)\n               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\nE       FileNotFoundError: [Errno 2] No such file or directory: '/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-670/test_clear_preserves_configura0/config.meta'\n\n/opt/homebrew/Cellar/python@3.12/3.12.12_2/Frameworks/Python.framework/Versions/3.12/lib/python3.12/pathlib.py:1013: FileNotFoundError\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:139 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-670/test_clear_preserves_configura0/db.meta\n_ test_clear_preserves_configuration_for_live_and_reopened_instances[None-True] _\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-670/test_clear_preserves_configura1')\nhierarchy = True, slot_bytes = None\n\n    @pytest.mark.parametrize('hierarchy', [False, True])\n    @pytest.mark.parametrize('slot_bytes', [None, 160])\n    def test_clear_preserves_configuration_for_live_and_reopened_instances(\n            tmp_path, hierarchy, slot_bytes):\n        db = _configured_db(tmp_path, hierarchy, slot_bytes)\n        key = datetime(2026, 9, 5, 12, 0, 0, 123456)\n        meta = {'owner': 'consumer'} if slot_bytes is not None else None\n        db.overwrite_dict('old-table', {key: {'value': 1}}, meta=meta)\n        extra = tmp_path / 'external' / 'deep' / 'extra.jsonl'\n        extra.parent.mkdir(parents=True)\n        jsonlfile.save_jsonl(str(extra), {'row': {'value': 2}})\n        orphan_index = tmp_path / 'orphan.jsonl.idx'\n        orphan_index.write_bytes(b'{}')\n        control_names = ['config.meta', 'config.meta.idx']\n        if hierarchy:\n            control_names += ['h.meta', 'h.meta.idx']\n        controls = {tmp_path / name: ((tmp_path / name).read_bytes(),\n                                    (tmp_path / name).stat().st_mtime_ns)\n                    for name in control_names}\n        settings = _settings(db)\n    \n        db.clear_folder(force=True)\n    \n        assert _settings(db) == settings\n        assert (tmp_path / 'db.meta').read_bytes() == b''\n        assert (tmp_path / 'db.meta.idx').read_bytes() == b'{}'\n        assert not list(tmp_path.rglob('*.jsonl'))\n        assert not list(tmp_path.rglob('*.jsonl.idx'))\n        assert not (tmp_path / 'external').exists()\n>       assert all((path.read_bytes(), path.stat().st_mtime_ns) == before\n                   for path, before in controls.items())\n\nunit_tests/test_clear_configuration.py:54: \n_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ \nunit_tests/test_clear_configuration.py:54: in <genexpr>\n    assert all((path.read_bytes(), path.stat().st_mtime_ns) == before\n                ^^^^^^^^^^^^^^^^^\n/opt/homebrew/Cellar/python@3.12/3.12.12_2/Frameworks/Python.framework/Versions/3.12/lib/python3.12/pathlib.py:1019: in read_bytes\n    with self.open(mode='rb') as f:\n         ^^^^^^^^^^^^^^^^^^^^\n_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ \n\nself = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-670/test_clear_preserves_configura1/config.meta')\nmode = 'rb', buffering = -1, encoding = None, errors = None, newline = None\n\n    def open(self, mode='r', buffering=-1, encoding=None,\n             errors=None, newline=None):\n        \"\"\"\n        Open the file pointed to by this path and return a file object, as\n        the built-in open() function does.\n        \"\"\"\n        if \"b\" not in mode:\n            encoding = io.text_encoding(encoding)\n>       return io.open(self, mode, buffering, encoding, errors, newline)\n               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\nE       FileNotFoundError: [Errno 2] No such file or directory: '/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-670/test_clear_preserves_configura1/config.meta'\n\n/opt/homebrew/Cellar/python@3.12/3.12.12_2/Frameworks/Python.framework/Versions/3.12/lib/python3.12/pathlib.py:1013: FileNotFoundError\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:139 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-670/test_clear_preserves_configura1/db.meta\n_ test_clear_preserves_configuration_for_live_and_reopened_instances[160-False] _\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-670/test_clear_preserves_configura2')\nhierarchy = False, slot_bytes = 160\n\n    @pytest.mark.parametrize('hierarchy', [False, True])\n    @pytest.mark.parametrize('slot_bytes', [None, 160])\n    def test_clear_preserves_configuration_for_live_and_reopened_instances(\n            tmp_path, hierarchy, slot_bytes):\n        db = _configured_db(tmp_path, hierarchy, slot_bytes)\n        key = datetime(2026, 9, 5, 12, 0, 0, 123456)\n        meta = {'owner': 'consumer'} if slot_bytes is not None else None\n        db.overwrite_dict('old-table', {key: {'value': 1}}, meta=meta)\n        extra = tmp_path / 'external' / 'deep' / 'extra.jsonl'\n        extra.parent.mkdir(parents=True)\n        jsonlfile.save_jsonl(str(extra), {'row': {'value': 2}})\n        orphan_index = tmp_path / 'orphan.jsonl.idx'\n        orphan_index.write_bytes(b'{}')\n        control_names = ['config.meta', 'config.meta.idx']\n        if hierarchy:\n            control_names += ['h.meta', 'h.meta.idx']\n        controls = {tmp_path / name: ((tmp_path / name).read_bytes(),\n                                    (tmp_path / name).stat().st_mtime_ns)\n                    for name in control_names}\n        settings = _settings(db)\n    \n        db.clear_folder(force=True)\n    \n        assert _settings(db) == settings\n        assert (tmp_path / 'db.meta').read_bytes() == b''\n        assert (tmp_path / 'db.meta.idx').read_bytes() == b'{}'\n        assert not list(tmp_path.rglob('*.jsonl'))\n        assert not list(tmp_path.rglob('*.jsonl.idx'))\n        assert not (tmp_path / 'external').exists()\n>       assert all((path.read_bytes(), path.stat().st_mtime_ns) == before\n                   for path, before in controls.items())\n\nunit_tests/test_clear_configuration.py:54: \n_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ \nunit_tests/test_clear_configuration.py:54: in <genexpr>\n    assert all((path.read_bytes(), path.stat().st_mtime_ns) == before\n                ^^^^^^^^^^^^^^^^^\n/opt/homebrew/Cellar/python@3.12/3.12.12_2/Frameworks/Python.framework/Versions/3.12/lib/python3.12/pathlib.py:1019: in read_bytes\n    with self.open(mode='rb') as f:\n         ^^^^^^^^^^^^^^^^^^^^\n_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ \n\nself = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-670/test_clear_preserves_configura2/config.meta')\nmode = 'rb', buffering = -1, encoding = None, errors = None, newline = None\n\n    def open(self, mode='r', buffering=-1, encoding=None,\n             errors=None, newline=None):\n        \"\"\"\n        Open the file pointed to by this path and return a file object, as\n        the built-in open() function does.\n        \"\"\"\n        if \"b\" not in mode:\n            encoding = io.text_encoding(encoding)\n>       return io.open(self, mode, buffering, encoding, errors, newline)\n               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\nE       FileNotFoundError: [Errno 2] No such file or directory: '/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-670/test_clear_preserves_configura2/config.meta'\n\n/opt/homebrew/Cellar/python@3.12/3.12.12_2/Frameworks/Python.framework/Versions/3.12/lib/python3.12/pathlib.py:1013: FileNotFoundError\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:139 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-670/test_clear_preserves_configura2/db.meta\n_ test_clear_preserves_configuration_for_live_and_reopened_instances[160-True] _\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-670/test_clear_preserves_configura3')\nhierarchy = True, slot_bytes = 160\n\n    @pytest.mark.parametrize('hierarchy', [False, True])\n    @pytest.mark.parametrize('slot_bytes', [None, 160])\n    def test_clear_preserves_configuration_for_live_and_reopened_instances(\n            tmp_path, hierarchy, slot_bytes):\n        db = _configured_db(tmp_path, hierarchy, slot_bytes)\n        key = datetime(2026, 9, 5, 12, 0, 0, 123456)\n        meta = {'owner': 'consumer'} if slot_bytes is not None else None\n        db.overwrite_dict('old-table', {key: {'value': 1}}, meta=meta)\n        extra = tmp_path / 'external' / 'deep' / 'extra.jsonl'\n        extra.parent.mkdir(parents=True)\n        jsonlfile.save_jsonl(str(extra), {'row': {'value': 2}})\n        orphan_index = tmp_path / 'orphan.jsonl.idx'\n        orphan_index.write_bytes(b'{}')\n        control_names = ['config.meta', 'config.meta.idx']\n        if hierarchy:\n            control_names += ['h.meta', 'h.meta.idx']\n        controls = {tmp_path / name: ((tmp_path / name).read_bytes(),\n                                    (tmp_path / name).stat().st_mtime_ns)\n                    for name in control_names}\n        settings = _settings(db)\n    \n        db.clear_folder(force=True)\n    \n        assert _settings(db) == settings\n        assert (tmp_path / 'db.meta').read_bytes() == b''\n        assert (tmp_path / 'db.meta.idx').read_bytes() == b'{}'\n        assert not list(tmp_path.rglob('*.jsonl'))\n        assert not list(tmp_path.rglob('*.jsonl.idx'))\n        assert not (tmp_path / 'external').exists()\n>       assert all((path.read_bytes(), path.stat().st_mtime_ns) == before\n                   for path, before in controls.items())\n\nunit_tests/test_clear_configuration.py:54: \n_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ \nunit_tests/test_clear_configuration.py:54: in <genexpr>\n    assert all((path.read_bytes(), path.stat().st_mtime_ns) == before\n                ^^^^^^^^^^^^^^^^^\n/opt/homebrew/Cellar/python@3.12/3.12.12_2/Frameworks/Python.framework/Versions/3.12/lib/python3.12/pathlib.py:1019: in read_bytes\n    with self.open(mode='rb') as f:\n         ^^^^^^^^^^^^^^^^^^^^\n_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ \n\nself = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-670/test_clear_preserves_configura3/config.meta')\nmode = 'rb', buffering = -1, encoding = None, errors = None, newline = None\n\n    def open(self, mode='r', buffering=-1, encoding=None,\n             errors=None, newline=None):\n        \"\"\"\n        Open the file pointed to by this path and return a file object, as\n        the built-in open() function does.\n        \"\"\"\n        if \"b\" not in mode:\n            encoding = io.text_encoding(encoding)\n>       return io.open(self, mode, buffering, encoding, errors, newline)\n               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\nE       FileNotFoundError: [Errno 2] No such file or directory: '/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-670/test_clear_preserves_configura3/config.meta'\n\n/opt/homebrew/Cellar/python@3.12/3.12.12_2/Frameworks/Python.framework/Versions/3.12/lib/python3.12/pathlib.py:1013: FileNotFoundError\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:139 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-670/test_clear_preserves_configura3/db.meta\n____________ test_clear_preserves_unrelated_files_and_is_repeatable ____________\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-670/test_clear_preserves_unrelated0')\n\n    def test_clear_preserves_unrelated_files_and_is_repeatable(tmp_path):\n        db = _configured_db(tmp_path, hierarchy=False, slot_bytes=None)\n        unrelated = tmp_path / 'external'\n        unrelated.mkdir()\n        for name in ('notes.meta', 'notes.idx', 'notes.txt'):\n            (unrelated / name).write_bytes(b'keep')\n        db.overwrite_dict('removed', {'row': {'value': 1}})\n    \n        db.clear_folder(force=True)\n        db.clear_folder(force=True)\n    \n>       assert all((unrelated / name).read_bytes() == b'keep'\n                   for name in ('notes.meta', 'notes.idx', 'notes.txt'))\n\nunit_tests/test_clear_configuration.py:103: \n_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ \nunit_tests/test_clear_configuration.py:103: in <genexpr>\n    assert all((unrelated / name).read_bytes() == b'keep'\n               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n/opt/homebrew/Cellar/python@3.12/3.12.12_2/Frameworks/Python.framework/Versions/3.12/lib/python3.12/pathlib.py:1019: in read_bytes\n    with self.open(mode='rb') as f:\n         ^^^^^^^^^^^^^^^^^^^^\n_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ \n\nself = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-670/test_clear_preserves_unrelated0/external/notes.meta')\nmode = 'rb', buffering = -1, encoding = None, errors = None, newline = None\n\n    def open(self, mode='r', buffering=-1, encoding=None,\n             errors=None, newline=None):\n        \"\"\"\n        Open the file pointed to by this path and return a file object, as\n        the built-in open() function does.\n        \"\"\"\n        if \"b\" not in mode:\n            encoding = io.text_encoding(encoding)\n>       return io.open(self, mode, buffering, encoding, errors, newline)\n               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\nE       FileNotFoundError: [Errno 2] No such file or directory: '/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-670/test_clear_preserves_unrelated0/external/notes.meta'\n\n/opt/homebrew/Cellar/python@3.12/3.12.12_2/Frameworks/Python.framework/Versions/3.12/lib/python3.12/pathlib.py:1013: FileNotFoundError\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:139 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-670/test_clear_preserves_unrelated0/db.meta\n=========================== short test summary info ============================\nFAILED unit_tests/test_clear_configuration.py::test_clear_preserves_configuration_for_live_and_reopened_instances[None-False]\nFAILED unit_tests/test_clear_configuration.py::test_clear_preserves_configuration_for_live_and_reopened_instances[None-True]\nFAILED unit_tests/test_clear_configuration.py::test_clear_preserves_configuration_for_live_and_reopened_instances[160-False]\nFAILED unit_tests/test_clear_configuration.py::test_clear_preserves_configuration_for_live_and_reopened_instances[160-True]\nFAILED unit_tests/test_clear_configuration.py::test_clear_preserves_unrelated_files_and_is_repeatable\n5 failed, 1 passed in 0.24s",
            "truncated": true,
            "captured_bytes": 18528
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "clear-configuration",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_clear_configuration.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_clear_configuration.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T04:01:26.649Z",
          "completed_at": "2026-09-05T04:01:26.932Z",
          "duration_ms": 282,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@64ae1bb69264f4ec513c894896b4f6ccebea74e0",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                                    [100%]stdout: \n6 passed in 0.17s",
            "truncated": false,
            "captured_bytes": 98
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "clear-regressions",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_folderdb.py unit_tests/test_metadata_upkeep.py unit_tests/test_reports.py unit_tests/test_hierarchy_recovery.py unit_tests/test_metadata_slot_migration.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_folderdb.py",
            "unit_tests/test_metadata_upkeep.py",
            "unit_tests/test_reports.py",
            "unit_tests/test_hierarchy_recovery.py",
            "unit_tests/test_metadata_slot_migration.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T04:01:27.091Z",
          "completed_at": "2026-09-05T04:01:27.981Z",
          "duration_ms": 890,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@64ae1bb69264f4ec513c894896b4f6ccebea74e0",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 79%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                       [100%]stdout: \n91 passed in 0.77s",
            "truncated": false,
            "captured_bytes": 179
          }
        }
      ],
      "acceptance_evidence": [
        {
          "version": 1,
          "id": "V-002",
          "label": "clear-configuration",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_clear_configuration.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_clear_configuration.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T04:01:26.649Z",
          "completed_at": "2026-09-05T04:01:26.932Z",
          "duration_ms": 282,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@64ae1bb69264f4ec513c894896b4f6ccebea74e0",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                                    [100%]stdout: \n6 passed in 0.17s",
            "truncated": false,
            "captured_bytes": 98
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "clear-regressions",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_folderdb.py unit_tests/test_metadata_upkeep.py unit_tests/test_reports.py unit_tests/test_hierarchy_recovery.py unit_tests/test_metadata_slot_migration.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_folderdb.py",
            "unit_tests/test_metadata_upkeep.py",
            "unit_tests/test_reports.py",
            "unit_tests/test_hierarchy_recovery.py",
            "unit_tests/test_metadata_slot_migration.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T04:01:27.091Z",
          "completed_at": "2026-09-05T04:01:27.981Z",
          "duration_ms": 890,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@64ae1bb69264f4ec513c894896b4f6ccebea74e0",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 79%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                       [100%]stdout: \n91 passed in 0.77s",
            "truncated": false,
            "captured_bytes": 179
          }
        }
      ]
    }
