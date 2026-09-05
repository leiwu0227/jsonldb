# Adhoc AH-20260905T153252083Z-857b

- Scope: Restrict newly created table names to portable filename characters and safe hierarchy segments, preserve existing table access, and read database metadata names as strings
- Title: Validate new table names and preserve metadata identifiers
- Started: 2026-09-05T15:32:52.083Z
- Completed: 2026-09-05T15:38:32.436Z
- Starting working tree: Clean.

## Outcome

Added portable creation-only FolderDB table-name validation with no mutation on invalid names, preserved access and writes to historical tables, disabled datetime deserialization for all db.meta reads, and documented the policy. All 396 focused naming/folder/metadata/hierarchy/statistics tests pass; initial eight fixture failures were corrected to assert lint status before normal open-time metadata rebuilding. All source-file caps pass, including folderdb.py at 1207/1250 physical lines. Concurrent Discussion state preserved.

## Focused workflow coexistence

No focused Assignment or Mission coexistence was recorded.

## Delivery path facts

### Requested adopted paths

None.

### Committed paths

- `.specdev/adhoc/2026-09/AH-20260905T153252083Z-857b_restrict-newly-created-table-names-to-portable-f.md`
- `README.md`
- `jsonldb/folderdb.py`
- `unit_tests/test_table_names.py`

### Rejected paths

None.

### Remaining owned paths

None.

## Verification summary

No manual verification summary was supplied.

## Verification attempt history

- **table-names-and-folder-regressions: failed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q unit_tests/test_table_names.py unit_tests/test_folderdb.py unit_tests/test_metadata_upkeep.py unit_tests/test_hierarchy_recovery.py unit_tests/test_visible_hierarchy_paths.py unit_tests/test_clear_configuration.py unit_tests/test_write_statistics.py` (1757 ms, working-tree@7086b590fb7ce9c8b19ee19e6585361e403a0b9c)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 1
  - Output:

    meta()
            db = FolderDB(str(tmp_path))
            assert list(db.get_dbmeta()) == [name]
            assert name + ':' in str(db)
            for writer in WRITERS:
                _write(db, writer, name + '.jsonl')
                assert db.get_dict([name])[name] == {'row': {'value': 1}}
            db.lint_db(force=True)
            db.lint_db()
            db = FolderDB(str(tmp_path))
            assert list(db.get_dbmeta()) == [name]
    >       assert db.get_dbmeta()[name]['linted'] is True
    E       assert False is True
    
    unit_tests/test_table_names.py:82: AssertionError
    ------------------------------ Captured log call -------------------------------
    WARNING  jsonldb.folderdb:folderdb.py:206 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-725/test_historical_names_survive_7/h.meta
    WARNING  jsonldb.folderdb:folderdb.py:123 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-725/test_historical_names_survive_7/config.meta
    =========================== short test summary info ============================
    FAILED unit_tests/test_table_names.py::test_historical_names_survive_writes_reopen_lint_and_delete[2024-01-01T00:00:00-None]
    FAILED unit_tests/test_table_names.py::test_historical_names_survive_writes_reopen_lint_and_delete[2024-01-01T00:00:00-1]
    FAILED unit_tests/test_table_names.py::test_historical_names_survive_writes_reopen_lint_and_delete[with space-None]
    FAILED unit_tests/test_table_names.py::test_historical_names_survive_writes_reopen_lint_and_delete[with space-1]
    FAILED unit_tests/test_table_names.py::test_historical_names_survive_writes_reopen_lint_and_delete[CON-None]
    FAILED unit_tests/test_table_names.py::test_historical_names_survive_writes_reopen_lint_and_delete[CON-1]
    FAILED unit_tests/test_table_names.py::test_historical_names_survive_writes_reopen_lint_and_delete[a..b-None]
    FAILED unit_tests/test_table_names.py::test_historical_names_survive_writes_reopen_lint_and_delete[a..b-1]
    8 failed, 388 passed in 1.63s
- **table-names-and-folder-regressions: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q unit_tests/test_table_names.py unit_tests/test_folderdb.py unit_tests/test_metadata_upkeep.py unit_tests/test_hierarchy_recovery.py unit_tests/test_visible_hierarchy_paths.py unit_tests/test_clear_configuration.py unit_tests/test_write_statistics.py` (5336 ms, working-tree@7086b590fb7ce9c8b19ee19e6585361e403a0b9c)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 54%]
    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 72%]
    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 90%]
    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                      [100%]stdout: 
    396 passed in 5.21s
- **source-file-caps: passed.** `.specdev/cache/bin/python-runtime-env python3 -c from pathlib import Path; caps={"folderdb.py":1250,"jsonlfile.py":950,"jsonldf.py":150,"metaslot.py":250,"reports.py":150,"_tabletimezone.py":250}; counts={name:len((Path("jsonldb")/name).read_bytes().splitlines()) for name in caps}; print(counts); assert all(counts[name] <= cap for name,cap in caps.items())` (26 ms, working-tree@7086b590fb7ce9c8b19ee19e6585361e403a0b9c)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: {'folderdb.py': 1207, 'jsonlfile.py': 950, 'jsonldf.py': 144, 'metaslot.py': 179, 'reports.py': 106, '_tabletimezone.py': 146}

## Current acceptance evidence

- **table-names-and-folder-regressions: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q unit_tests/test_table_names.py unit_tests/test_folderdb.py unit_tests/test_metadata_upkeep.py unit_tests/test_hierarchy_recovery.py unit_tests/test_visible_hierarchy_paths.py unit_tests/test_clear_configuration.py unit_tests/test_write_statistics.py` (5336 ms, working-tree@7086b590fb7ce9c8b19ee19e6585361e403a0b9c)
- **source-file-caps: passed.** `.specdev/cache/bin/python-runtime-env python3 -c from pathlib import Path; caps={"folderdb.py":1250,"jsonlfile.py":950,"jsonldf.py":150,"metaslot.py":250,"reports.py":150,"_tabletimezone.py":250}; counts={name:len((Path("jsonldb")/name).read_bytes().splitlines()) for name in caps}; print(counts); assert all(counts[name] <= cap for name,cap in caps.items())` (26 ms, working-tree@7086b590fb7ce9c8b19ee19e6585361e403a0b9c)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: {'folderdb.py': 1207, 'jsonlfile.py': 950, 'jsonldf.py': 144, 'metaslot.py': 179, 'reports.py': 106, '_tabletimezone.py': 146}

## Structured verification

    {
      "version": 1,
      "path_facts": {
        "requested": [],
        "committed": [
          ".specdev/adhoc/2026-09/AH-20260905T153252083Z-857b_restrict-newly-created-table-names-to-portable-f.md",
          "README.md",
          "jsonldb/folderdb.py",
          "unit_tests/test_table_names.py"
        ],
        "rejected": [],
        "remaining": []
      },
      "attempt_history": [
        {
          "version": 1,
          "id": "V-001",
          "label": "table-names-and-folder-regressions",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q unit_tests/test_table_names.py unit_tests/test_folderdb.py unit_tests/test_metadata_upkeep.py unit_tests/test_hierarchy_recovery.py unit_tests/test_visible_hierarchy_paths.py unit_tests/test_clear_configuration.py unit_tests/test_write_statistics.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "unit_tests/test_table_names.py",
            "unit_tests/test_folderdb.py",
            "unit_tests/test_metadata_upkeep.py",
            "unit_tests/test_hierarchy_recovery.py",
            "unit_tests/test_visible_hierarchy_paths.py",
            "unit_tests/test_clear_configuration.py",
            "unit_tests/test_write_statistics.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T15:36:27.747Z",
          "completed_at": "2026-09-05T15:36:29.505Z",
          "duration_ms": 1757,
          "exit_status": 1,
          "status": "failed",
          "tested_revision": "working-tree@7086b590fb7ce9c8b19ee19e6585361e403a0b9c",
          "output": {
            "text": ".stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 90%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                      [100%]stdout: \n=================================== FAILURES ===================================\n_ test_historical_names_survive_writes_reopen_lint_and_delete[2024-01-01T00:00:00-None] _\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-725/test_historical_names_survive_0')\ndepth = None, name = '2024-01-01T00:00:00'\n\n    @pytest.mark.parametrize('depth', [None, 1])\n    @pytest.mark.parametrize('name', ['2024-01-01T00:00:00', 'with space', 'CON', 'a..b'])\n    def test_historical_names_survive_writes_reopen_lint_and_delete(tmp_path, depth, name):\n        db = FolderDB(str(tmp_path), hierarchy_depth=depth)\n        # Seed a historical file through the path-based storage API.\n        path = Path(db._get_file_path(name))\n        path.parent.mkdir(parents=True, exist_ok=True)\n        jsonlfile.save_jsonl(str(path), {'row': {'value': 0}})\n        db.build_dbmeta()\n        db = FolderDB(str(tmp_path))\n        assert list(db.get_dbmeta()) == [name]\n        assert name + ':' in str(db)\n        for writer in WRITERS:\n            _write(db, writer, name + '.jsonl')\n            assert db.get_dict([name])[name] == {'row': {'value': 1}}\n        db.lint_db(force=True)\n        db.lint_db()\n        db = FolderDB(str(tmp_path))\n        assert list(db.get_dbmeta()) == [name]\n>       assert db.get_dbmeta()[name]['linted'] is True\nE       assert False is True\n\nunit_tests/test_table_names.py:82: AssertionError\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:123 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-725/test_historical_names_survive_0/config.meta\nWARNING  jsonldb.folderdb:folderdb.py:140 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-725/test_historical_names_survive_0/db.meta\n_ test_historical_names_survive_writes_reopen_lint_and_delete[2024-01-01T00:00:00-1] _\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-725/test_historical_names_survive_1')\ndepth = 1, name = '2024-01-01T00:00:00'\n\n    @pytest.mark.parametrize('depth', [None, 1])\n    @pytest.mark.parametrize('name', ['2024-01-01T00:00:00', 'with space', 'CON', 'a..b'])\n    def test_historical_names_survive_writes_reopen_lint_and_delete(tmp_path, depth, name):\n        db = FolderDB(str(tmp_path), hierarchy_depth=depth)\n        # Seed a historical file through the path-based storage API.\n        path = Path(db._get_file_path(name))\n        path.parent.mkdir(parents=True, exist_ok=True)\n        jsonlfile.save_jsonl(str(path), {'row': {'value': 0}})\n        db.build_dbmeta()\n        db = FolderDB(str(tmp_path))\n        assert list(db.get_dbmeta()) == [name]\n        assert name + ':' in str(db)\n        for writer in WRITERS:\n            _write(db, writer, name + '.jsonl')\n            assert db.get_dict([name])[name] == {'row': {'value': 1}}\n        db.lint_db(force=True)\n        db.lint_db()\n        db = FolderDB(str(tmp_path))\n        assert list(db.get_dbmeta()) == [name]\n>       assert db.get_dbmeta()[name]['linted'] is True\nE       assert False is True\n\nunit_tests/test_table_names.py:82: AssertionError\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:206 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-725/test_historical_names_survive_1/h.meta\nWARNING  jsonldb.folderdb:folderdb.py:123 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-725/test_historical_names_survive_1/config.meta\n_ test_historical_names_survive_writes_reopen_lint_and_delete[with space-None] _\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-725/test_historical_names_survive_2')\ndepth = None, name = 'with space'\n\n    @pytest.mark.parametrize('depth', [None, 1])\n    @pytest.mark.parametrize('name', ['2024-01-01T00:00:00', 'with space', 'CON', 'a..b'])\n    def test_historical_names_survive_writes_reopen_lint_and_delete(tmp_path, depth, name):\n        db = FolderDB(str(tmp_path), hierarchy_depth=depth)\n        # Seed a historical file through the path-based storage API.\n        path = Path(db._get_file_path(name))\n        path.parent.mkdir(parents=True, exist_ok=True)\n        jsonlfile.save_jsonl(str(path), {'row': {'value': 0}})\n        db.build_dbmeta()\n        db = FolderDB(str(tmp_path))\n        assert list(db.get_dbmeta()) == [name]\n        assert name + ':' in str(db)\n        for writer in WRITERS:\n            _write(db, writer, name + '.jsonl')\n            assert db.get_dict([name])[name] == {'row': {'value': 1}}\n        db.lint_db(force=True)\n        db.lint_db()\n        db = FolderDB(str(tmp_path))\n        assert list(db.get_dbmeta()) == [name]\n>       assert db.get_dbmeta()[name]['linted'] is True\nE       assert False is True\n\nunit_tests/test_table_names.py:82: AssertionError\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:123 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-725/test_historical_names_survive_2/config.meta\nWARNING  jsonldb.folderdb:folderdb.py:140 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-725/test_historical_names_survive_2/db.meta\n__ test_historical_names_survive_writes_reopen_lint_and_delete[with space-1] ___\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-725/test_historical_names_survive_3')\ndepth = 1, name = 'with space'\n\n    @pytest.mark.parametrize('depth', [None, 1])\n    @pytest.mark.parametrize('name', ['2024-01-01T00:00:00', 'with space', 'CON', 'a..b'])\n    def test_historical_names_survive_writes_reopen_lint_and_delete(tmp_path, depth, name):\n        db = FolderDB(str(tmp_path), hierarchy_depth=depth)\n        # Seed a historical file through the path-based storage API.\n        path = Path(db._get_file_path(name))\n        path.parent.mkdir(parents=True, exist_ok=True)\n        jsonlfile.save_jsonl(str(path), {'row': {'value': 0}})\n        db.build_dbmeta()\n        db = FolderDB(str(tmp_path))\n        assert list(db.get_dbmeta()) == [name]\n        assert name + ':' in str(db)\n        for writer in WRITERS:\n            _write(db, writer, name + '.jsonl')\n            assert db.get_dict([name])[name] == {'row': {'value': 1}}\n        db.lint_db(force=True)\n        db.lint_db()\n        db = FolderDB(str(tmp_path))\n        assert list(db.get_dbmeta()) == [name]\n>       assert db.get_dbmeta()[name]['linted'] is True\nE       assert False is True\n\nunit_tests/test_table_names.py:82: AssertionError\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:206 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-725/test_historical_names_survive_3/h.meta\nWARNING  jsonldb.folderdb:folderdb.py:123 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-725/test_historical_names_survive_3/config.meta\n____ test_historical_names_survive_writes_reopen_lint_and_delete[CON-None] _____\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-725/test_historical_names_survive_4')\ndepth = None, name = 'CON'\n\n    @pytest.mark.parametrize('depth', [None, 1])\n    @pytest.mark.parametrize('name', ['2024-01-01T00:00:00', 'with space', 'CON', 'a..b'])\n    def test_historical_names_survive_writes_reopen_lint_and_delete(tmp_path, depth, name):\n        db = FolderDB(str(tmp_path), hierarchy_depth=depth)\n        # Seed a historical file through the path-based storage API.\n        path = Path(db._get_file_path(name))\n        path.parent.mkdir(parents=True, exist_ok=True)\n        jsonlfile.save_jsonl(str(path), {'row': {'value': 0}})\n        db.build_dbmeta()\n        db = FolderDB(str(tmp_path))\n        assert list(db.get_dbmeta()) == [name]\n        assert name + ':' in str(db)\n        for writer in WRITERS:\n            _write(db, writer, name + '.jsonl')\n            assert db.get_dict([name])[name] == {'row': {'value': 1}}\n        db.lint_db(force=True)\n        db.lint_db()\n        db = FolderDB(str(tmp_path))\n        assert list(db.get_dbmeta()) == [name]\n>       assert db.get_dbmeta()[name]['linted'] is True\nE       assert False is True\n\nunit_tests/test_table_names.py:82: AssertionError\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:123 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-725/test_historical_names_survive_4/config.meta\nWARNING  jsonldb.folderdb:folderdb.py:140 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-725/test_historical_names_survive_4/db.meta\n______ test_historical_names_survive_writes_reopen_lint_and_delete[CON-1] ______\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-725/test_historical_names_survive_5')\ndepth = 1, name = 'CON'\n\n    @pytest.mark.parametrize('depth', [None, 1])\n    @pytest.mark.parametrize('name', ['2024-01-01T00:00:00', 'with space', 'CON', 'a..b'])\n    def test_historical_names_survive_writes_reopen_lint_and_delete(tmp_path, depth, name):\n        db = FolderDB(str(tmp_path), hierarchy_depth=depth)\n        # Seed a historical file through the path-based storage API.\n        path = Path(db._get_file_path(name))\n        path.parent.mkdir(parents=True, exist_ok=True)\n        jsonlfile.save_jsonl(str(path), {'row': {'value': 0}})\n        db.build_dbmeta()\n        db = FolderDB(str(tmp_path))\n        assert list(db.get_dbmeta()) == [name]\n        assert name + ':' in str(db)\n        for writer in WRITERS:\n            _write(db, writer, name + '.jsonl')\n            assert db.get_dict([name])[name] == {'row': {'value': 1}}\n        db.lint_db(force=True)\n        db.lint_db()\n        db = FolderDB(str(tmp_path))\n        assert list(db.get_dbmeta()) == [name]\n>       assert db.get_dbmeta()[name]['linted'] is True\nE       assert False is True\n\nunit_tests/test_table_names.py:82: AssertionError\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:206 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-725/test_historical_names_survive_5/h.meta\nWARNING  jsonldb.folderdb:folderdb.py:123 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-725/test_historical_names_survive_5/config.meta\n____ test_historical_names_survive_writes_reopen_lint_and_delete[a..b-None] ____\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-725/test_historical_names_survive_6')\ndepth = None, name = 'a..b'\n\n    @pytest.mark.parametrize('depth', [None, 1])\n    @pytest.mark.parametrize('name', ['2024-01-01T00:00:00', 'with space', 'CON', 'a..b'])\n    def test_historical_names_survive_writes_reopen_lint_and_delete(tmp_path, depth, name):\n        db = FolderDB(str(tmp_path), hierarchy_depth=depth)\n        # Seed a historical file through the path-based storage API.\n        path = Path(db._get_file_path(name))\n        path.parent.mkdir(parents=True, exist_ok=True)\n        jsonlfile.save_jsonl(str(path), {'row': {'value': 0}})\n        db.build_dbmeta()\n        db = FolderDB(str(tmp_path))\n        assert list(db.get_dbmeta()) == [name]\n        assert name + ':' in str(db)\n        for writer in WRITERS:\n            _write(db, writer, name + '.jsonl')\n            assert db.get_dict([name])[name] == {'row': {'value': 1}}\n        db.lint_db(force=True)\n        db.lint_db()\n        db = FolderDB(str(tmp_path))\n        assert list(db.get_dbmeta()) == [name]\n>       assert db.get_dbmeta()[name]['linted'] is True\nE       assert False is True\n\nunit_tests/test_table_names.py:82: AssertionError\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:123 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-725/test_historical_names_survive_6/config.meta\nWARNING  jsonldb.folderdb:folderdb.py:140 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-725/test_historical_names_survive_6/db.meta\n_____ test_historical_names_survive_writes_reopen_lint_and_delete[a..b-1] ______\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-725/test_historical_names_survive_7')\ndepth = 1, name = 'a..b'\n\n    @pytest.mark.parametrize('depth', [None, 1])\n    @pytest.mark.parametrize('name', ['2024-01-01T00:00:00', 'with space', 'CON', 'a..b'])\n    def test_historical_names_survive_writes_reopen_lint_and_delete(tmp_path, depth, name):\n        db = FolderDB(str(tmp_path), hierarchy_depth=depth)\n        # Seed a historical file through the path-based storage API.\n        path = Path(db._get_file_path(name))\n        path.parent.mkdir(parents=True, exist_ok=True)\n        jsonlfile.save_jsonl(str(path), {'row': {'value': 0}})\n        db.build_dbmeta()\n        db = FolderDB(str(tmp_path))\n        assert list(db.get_dbmeta()) == [name]\n        assert name + ':' in str(db)\n        for writer in WRITERS:\n            _write(db, writer, name + '.jsonl')\n            assert db.get_dict([name])[name] == {'row': {'value': 1}}\n        db.lint_db(force=True)\n        db.lint_db()\n        db = FolderDB(str(tmp_path))\n        assert list(db.get_dbmeta()) == [name]\n>       assert db.get_dbmeta()[name]['linted'] is True\nE       assert False is True\n\nunit_tests/test_table_names.py:82: AssertionError\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:206 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-725/test_historical_names_survive_7/h.meta\nWARNING  jsonldb.folderdb:folderdb.py:123 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-725/test_historical_names_survive_7/config.meta\n=========================== short test summary info ============================\nFAILED unit_tests/test_table_names.py::test_historical_names_survive_writes_reopen_lint_and_delete[2024-01-01T00:00:00-None]\nFAILED unit_tests/test_table_names.py::test_historical_names_survive_writes_reopen_lint_and_delete[2024-01-01T00:00:00-1]\nFAILED unit_tests/test_table_names.py::test_historical_names_survive_writes_reopen_lint_and_delete[with space-None]\nFAILED unit_tests/test_table_names.py::test_historical_names_survive_writes_reopen_lint_and_delete[with space-1]\nFAILED unit_tests/test_table_names.py::test_historical_names_survive_writes_reopen_lint_and_delete[CON-None]\nFAILED unit_tests/test_table_names.py::test_historical_names_survive_writes_reopen_lint_and_delete[CON-1]\nFAILED unit_tests/test_table_names.py::test_historical_names_survive_writes_reopen_lint_and_delete[a..b-None]\nFAILED unit_tests/test_table_names.py::test_historical_names_survive_writes_reopen_lint_and_delete[a..b-1]\n8 failed, 388 passed in 1.63s",
            "truncated": false,
            "captured_bytes": 16219
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "table-names-and-folder-regressions",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q unit_tests/test_table_names.py unit_tests/test_folderdb.py unit_tests/test_metadata_upkeep.py unit_tests/test_hierarchy_recovery.py unit_tests/test_visible_hierarchy_paths.py unit_tests/test_clear_configuration.py unit_tests/test_write_statistics.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "unit_tests/test_table_names.py",
            "unit_tests/test_folderdb.py",
            "unit_tests/test_metadata_upkeep.py",
            "unit_tests/test_hierarchy_recovery.py",
            "unit_tests/test_visible_hierarchy_paths.py",
            "unit_tests/test_clear_configuration.py",
            "unit_tests/test_write_statistics.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T15:37:43.693Z",
          "completed_at": "2026-09-05T15:37:49.029Z",
          "duration_ms": 5336,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@7086b590fb7ce9c8b19ee19e6585361e403a0b9c",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 18%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 36%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 54%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 72%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 90%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                      [100%]stdout: \n396 passed in 5.21s",
            "truncated": false,
            "captured_bytes": 500
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "source-file-caps",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -c from pathlib import Path; caps={\"folderdb.py\":1250,\"jsonlfile.py\":950,\"jsonldf.py\":150,\"metaslot.py\":250,\"reports.py\":150,\"_tabletimezone.py\":250}; counts={name:len((Path(\"jsonldb\")/name).read_bytes().splitlines()) for name in caps}; print(counts); assert all(counts[name] <= cap for name,cap in caps.items())",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-c",
            "from pathlib import Path; caps={\"folderdb.py\":1250,\"jsonlfile.py\":950,\"jsonldf.py\":150,\"metaslot.py\":250,\"reports.py\":150,\"_tabletimezone.py\":250}; counts={name:len((Path(\"jsonldb\")/name).read_bytes().splitlines()) for name in caps}; print(counts); assert all(counts[name] <= cap for name,cap in caps.items())"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T15:38:13.016Z",
          "completed_at": "2026-09-05T15:38:13.043Z",
          "duration_ms": 26,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@7086b590fb7ce9c8b19ee19e6585361e403a0b9c",
          "output": {
            "text": "stdout: {'folderdb.py': 1207, 'jsonlfile.py': 950, 'jsonldf.py': 144, 'metaslot.py': 179, 'reports.py': 106, '_tabletimezone.py': 146}",
            "truncated": false,
            "captured_bytes": 127
          }
        }
      ],
      "acceptance_evidence": [
        {
          "version": 1,
          "id": "V-002",
          "label": "table-names-and-folder-regressions",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q unit_tests/test_table_names.py unit_tests/test_folderdb.py unit_tests/test_metadata_upkeep.py unit_tests/test_hierarchy_recovery.py unit_tests/test_visible_hierarchy_paths.py unit_tests/test_clear_configuration.py unit_tests/test_write_statistics.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "unit_tests/test_table_names.py",
            "unit_tests/test_folderdb.py",
            "unit_tests/test_metadata_upkeep.py",
            "unit_tests/test_hierarchy_recovery.py",
            "unit_tests/test_visible_hierarchy_paths.py",
            "unit_tests/test_clear_configuration.py",
            "unit_tests/test_write_statistics.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T15:37:43.693Z",
          "completed_at": "2026-09-05T15:37:49.029Z",
          "duration_ms": 5336,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@7086b590fb7ce9c8b19ee19e6585361e403a0b9c",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 18%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 36%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 54%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 72%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 90%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                      [100%]stdout: \n396 passed in 5.21s",
            "truncated": false,
            "captured_bytes": 500
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "source-file-caps",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -c from pathlib import Path; caps={\"folderdb.py\":1250,\"jsonlfile.py\":950,\"jsonldf.py\":150,\"metaslot.py\":250,\"reports.py\":150,\"_tabletimezone.py\":250}; counts={name:len((Path(\"jsonldb\")/name).read_bytes().splitlines()) for name in caps}; print(counts); assert all(counts[name] <= cap for name,cap in caps.items())",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-c",
            "from pathlib import Path; caps={\"folderdb.py\":1250,\"jsonlfile.py\":950,\"jsonldf.py\":150,\"metaslot.py\":250,\"reports.py\":150,\"_tabletimezone.py\":250}; counts={name:len((Path(\"jsonldb\")/name).read_bytes().splitlines()) for name in caps}; print(counts); assert all(counts[name] <= cap for name,cap in caps.items())"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T15:38:13.016Z",
          "completed_at": "2026-09-05T15:38:13.043Z",
          "duration_ms": 26,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@7086b590fb7ce9c8b19ee19e6585361e403a0b9c",
          "output": {
            "text": "stdout: {'folderdb.py': 1207, 'jsonlfile.py': 950, 'jsonldf.py': 144, 'metaslot.py': 179, 'reports.py': 106, '_tabletimezone.py': 146}",
            "truncated": false,
            "captured_bytes": 127
          }
        }
      ]
    }
