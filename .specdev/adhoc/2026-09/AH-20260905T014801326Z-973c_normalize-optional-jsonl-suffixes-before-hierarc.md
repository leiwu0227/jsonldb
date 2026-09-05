# Adhoc AH-20260905T014801326Z-973c

- Scope: Normalize optional .jsonl suffixes before hierarchy path resolution so custom-delimiter dictionary and DataFrame operations use the same table and refresh metadata successfully
- Title: Fix hierarchy paths for suffixed table names
- Started: 2026-09-05T01:48:01.326Z
- Completed: 2026-09-05T01:49:30.282Z
- Starting working tree: Clean.

## Outcome

Normalized the optional .jsonl suffix in the shared hierarchy-path helper before splitting on the configured delimiter. Both name forms now resolve to the same table for reads, writes, metadata refresh, and deletion. Eight regressions reproduced failures before the fix and now pass for all four dictionary/DataFrame writers with bare and suffixed names; they also verify alternate-name upserts, reopen, metadata counts, missing reads without directory creation, and deletion. Focused folder, metadata upkeep, and metadata slot migration suites pass 41 tests; git diff --check passes.

## Focused workflow coexistence

No focused Assignment or Mission coexistence was recorded.

## Delivery path facts

### Requested adopted paths

None.

### Committed paths

- `.specdev/adhoc/2026-09/AH-20260905T014801326Z-973c_normalize-optional-jsonl-suffixes-before-hierarc.md`
- `jsonldb/folderdb.py`
- `unit_tests/test_folderdb.py`

### Rejected paths

None.

### Remaining owned paths

None.

## Verification summary

No manual verification summary was supplied.

## Verification attempt history

- **custom-delimiter-suffix: failed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_folderdb.py -k custom_delimiter_suffix` (447 ms, working-tree@71c131e0e947c864eb3ebb3766cb11c0d138e62c)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 1
  - Output (bounded tail):

                 if writer.endswith('_df') else rows)
        
            getattr(db, writer)(name, content)
        
            path = tmp_path / 'a' / 'b' / 'a-b.jsonl'
    >       assert path.is_file()
    E       AssertionError: assert False
    E        +  where False = is_file()
    E        +    where is_file = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r7/a/b/a-b.jsonl').is_file
    
    unit_tests/test_folderdb.py:410: AssertionError
    ------------------------------ Captured log call -------------------------------
    WARNING  jsonldb.folderdb:folderdb.py:115 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r7/config.meta
    WARNING  jsonldb.folderdb:folderdb.py:130 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r7/db.meta
    WARNING  jsonldb.jsonlfile:jsonlfile.py:149 rebuilt missing index /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r7/db.meta.idx
    =========================== short test summary info ============================
    FAILED unit_tests/test_folderdb.py::test_custom_delimiter_suffix_round_trip[a-b-overwrite_dict]
    FAILED unit_tests/test_folderdb.py::test_custom_delimiter_suffix_round_trip[a-b-upsert_dict]
    FAILED unit_tests/test_folderdb.py::test_custom_delimiter_suffix_round_trip[a-b-overwrite_df]
    FAILED unit_tests/test_folderdb.py::test_custom_delimiter_suffix_round_trip[a-b-upsert_df]
    FAILED unit_tests/test_folderdb.py::test_custom_delimiter_suffix_round_trip[a-b.jsonl-overwrite_dict]
    FAILED unit_tests/test_folderdb.py::test_custom_delimiter_suffix_round_trip[a-b.jsonl-upsert_dict]
    FAILED unit_tests/test_folderdb.py::test_custom_delimiter_suffix_round_trip[a-b.jsonl-overwrite_df]
    FAILED unit_tests/test_folderdb.py::test_custom_delimiter_suffix_round_trip[a-b.jsonl-upsert_df]
    8 failed, 24 deselected in 0.32s
- **custom-delimiter-suffix: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_folderdb.py -k custom_delimiter_suffix` (374 ms, working-tree@71c131e0e947c864eb3ebb3766cb11c0d138e62c)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                                  [100%]stdout: 
    8 passed, 24 deselected in 0.25s
- **folder-regressions: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_folderdb.py unit_tests/test_metadata_upkeep.py unit_tests/test_metadata_slot_migration.py` (988 ms, working-tree@71c131e0e947c864eb3ebb3766cb11c0d138e62c)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                 [100%]stdout: 
    41 passed in 0.85s

## Current acceptance evidence

- **custom-delimiter-suffix: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_folderdb.py -k custom_delimiter_suffix` (374 ms, working-tree@71c131e0e947c864eb3ebb3766cb11c0d138e62c)
- **folder-regressions: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_folderdb.py unit_tests/test_metadata_upkeep.py unit_tests/test_metadata_slot_migration.py` (988 ms, working-tree@71c131e0e947c864eb3ebb3766cb11c0d138e62c)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                 [100%]stdout: 
    41 passed in 0.85s

## Structured verification

    {
      "version": 1,
      "path_facts": {
        "requested": [],
        "committed": [
          ".specdev/adhoc/2026-09/AH-20260905T014801326Z-973c_normalize-optional-jsonl-suffixes-before-hierarc.md",
          "jsonldb/folderdb.py",
          "unit_tests/test_folderdb.py"
        ],
        "rejected": [],
        "remaining": []
      },
      "attempt_history": [
        {
          "version": 1,
          "id": "V-001",
          "label": "custom-delimiter-suffix",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_folderdb.py -k custom_delimiter_suffix",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_folderdb.py",
            "-k",
            "custom_delimiter_suffix"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T01:48:47.221Z",
          "completed_at": "2026-09-05T01:48:47.668Z",
          "duration_ms": 447,
          "exit_status": 1,
          "status": "failed",
          "tested_revision": "working-tree@71c131e0e947c864eb3ebb3766cb11c0d138e62c",
          "output": {
            "text": "derdb:folderdb.py:130 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r0/db.meta\n___________ test_custom_delimiter_suffix_round_trip[a-b-upsert_dict] ___________\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r1')\nwriter = 'upsert_dict', name = 'a-b'\n\n    @pytest.mark.parametrize('writer', [\n        'overwrite_dict', 'upsert_dict', 'overwrite_df', 'upsert_df',\n    ])\n    @pytest.mark.parametrize('name', ['a-b', 'a-b.jsonl'])\n    def test_custom_delimiter_suffix_round_trip(tmp_path, writer, name):\n        save_jsonl_atomic(str(tmp_path / 'h.meta'), {\n            'hierarchy': {\n                'use_hierarchy': True, 'delimiter': '-', 'hierarchy_depth': 2,\n            },\n        })\n        db = FolderDB(str(tmp_path))\n        rows = {'first': {'value': 1}}\n        content = (pd.DataFrame.from_dict(rows, orient='index')\n                   if writer.endswith('_df') else rows)\n    \n>       getattr(db, writer)(name, content)\n\nunit_tests/test_folderdb.py:407: \n_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ \njsonldb/folderdb.py:535: in upsert_dict\n    self.update_dbmeta(self._get_file_name(name))\njsonldb/folderdb.py:832: in update_dbmeta\n    entry = self._make_meta_entry(meta_key, file_path, linted, lint_time)\n            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\njsonldb/folderdb.py:755: in _make_meta_entry\n    \"size\": os.path.getsize(file_path),\n            ^^^^^^^^^^^^^^^^^^^^^^^^^^\n_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ \n\nfilename = '/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r1/a/b.jsonl/a-b.jsonl'\n\n>   ???\nE   FileNotFoundError: [Errno 2] No such file or directory: '/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r1/a/b.jsonl/a-b.jsonl'\n\n<frozen genericpath>:62: FileNotFoundError\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:115 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r1/config.meta\nWARNING  jsonldb.folderdb:folderdb.py:130 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r1/db.meta\n__________ test_custom_delimiter_suffix_round_trip[a-b-overwrite_df] ___________\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r2')\nwriter = 'overwrite_df', name = 'a-b'\n\n    @pytest.mark.parametrize('writer', [\n        'overwrite_dict', 'upsert_dict', 'overwrite_df', 'upsert_df',\n    ])\n    @pytest.mark.parametrize('name', ['a-b', 'a-b.jsonl'])\n    def test_custom_delimiter_suffix_round_trip(tmp_path, writer, name):\n        save_jsonl_atomic(str(tmp_path / 'h.meta'), {\n            'hierarchy': {\n                'use_hierarchy': True, 'delimiter': '-', 'hierarchy_depth': 2,\n            },\n        })\n        db = FolderDB(str(tmp_path))\n        rows = {'first': {'value': 1}}\n        content = (pd.DataFrame.from_dict(rows, orient='index')\n                   if writer.endswith('_df') else rows)\n    \n>       getattr(db, writer)(name, content)\n\nunit_tests/test_folderdb.py:407: \n_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ \njsonldb/folderdb.py:404: in overwrite_df\n    self.update_dbmeta(self._get_file_name(name))\njsonldb/folderdb.py:832: in update_dbmeta\n    entry = self._make_meta_entry(meta_key, file_path, linted, lint_time)\n            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\njsonldb/folderdb.py:755: in _make_meta_entry\n    \"size\": os.path.getsize(file_path),\n            ^^^^^^^^^^^^^^^^^^^^^^^^^^\n_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ \n\nfilename = '/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r2/a/b.jsonl/a-b.jsonl'\n\n>   ???\nE   FileNotFoundError: [Errno 2] No such file or directory: '/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r2/a/b.jsonl/a-b.jsonl'\n\n<frozen genericpath>:62: FileNotFoundError\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:115 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r2/config.meta\nWARNING  jsonldb.folderdb:folderdb.py:130 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r2/db.meta\n____________ test_custom_delimiter_suffix_round_trip[a-b-upsert_df] ____________\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r3')\nwriter = 'upsert_df', name = 'a-b'\n\n    @pytest.mark.parametrize('writer', [\n        'overwrite_dict', 'upsert_dict', 'overwrite_df', 'upsert_df',\n    ])\n    @pytest.mark.parametrize('name', ['a-b', 'a-b.jsonl'])\n    def test_custom_delimiter_suffix_round_trip(tmp_path, writer, name):\n        save_jsonl_atomic(str(tmp_path / 'h.meta'), {\n            'hierarchy': {\n                'use_hierarchy': True, 'delimiter': '-', 'hierarchy_depth': 2,\n            },\n        })\n        db = FolderDB(str(tmp_path))\n        rows = {'first': {'value': 1}}\n        content = (pd.DataFrame.from_dict(rows, orient='index')\n                   if writer.endswith('_df') else rows)\n    \n>       getattr(db, writer)(name, content)\n\nunit_tests/test_folderdb.py:407: \n_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ \njsonldb/folderdb.py:434: in upsert_df\n    self.update_dbmeta(self._get_file_name(name))\njsonldb/folderdb.py:832: in update_dbmeta\n    entry = self._make_meta_entry(meta_key, file_path, linted, lint_time)\n            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\njsonldb/folderdb.py:755: in _make_meta_entry\n    \"size\": os.path.getsize(file_path),\n            ^^^^^^^^^^^^^^^^^^^^^^^^^^\n_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ \n\nfilename = '/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r3/a/b.jsonl/a-b.jsonl'\n\n>   ???\nE   FileNotFoundError: [Errno 2] No such file or directory: '/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r3/a/b.jsonl/a-b.jsonl'\n\n<frozen genericpath>:62: FileNotFoundError\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:115 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r3/config.meta\nWARNING  jsonldb.folderdb:folderdb.py:130 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r3/db.meta\n______ test_custom_delimiter_suffix_round_trip[a-b.jsonl-overwrite_dict] _______\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r4')\nwriter = 'overwrite_dict', name = 'a-b.jsonl'\n\n    @pytest.mark.parametrize('writer', [\n        'overwrite_dict', 'upsert_dict', 'overwrite_df', 'upsert_df',\n    ])\n    @pytest.mark.parametrize('name', ['a-b', 'a-b.jsonl'])\n    def test_custom_delimiter_suffix_round_trip(tmp_path, writer, name):\n        save_jsonl_atomic(str(tmp_path / 'h.meta'), {\n            'hierarchy': {\n                'use_hierarchy': True, 'delimiter': '-', 'hierarchy_depth': 2,\n            },\n        })\n        db = FolderDB(str(tmp_path))\n        rows = {'first': {'value': 1}}\n        content = (pd.DataFrame.from_dict(rows, orient='index')\n                   if writer.endswith('_df') else rows)\n    \n        getattr(db, writer)(name, content)\n    \n        path = tmp_path / 'a' / 'b' / 'a-b.jsonl'\n>       assert path.is_file()\nE       AssertionError: assert False\nE        +  where False = is_file()\nE        +    where is_file = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r4/a/b/a-b.jsonl').is_file\n\nunit_tests/test_folderdb.py:410: AssertionError\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:115 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r4/config.meta\nWARNING  jsonldb.folderdb:folderdb.py:130 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r4/db.meta\nWARNING  jsonldb.jsonlfile:jsonlfile.py:149 rebuilt missing index /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r4/db.meta.idx\n________ test_custom_delimiter_suffix_round_trip[a-b.jsonl-upsert_dict] ________\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r5')\nwriter = 'upsert_dict', name = 'a-b.jsonl'\n\n    @pytest.mark.parametrize('writer', [\n        'overwrite_dict', 'upsert_dict', 'overwrite_df', 'upsert_df',\n    ])\n    @pytest.mark.parametrize('name', ['a-b', 'a-b.jsonl'])\n    def test_custom_delimiter_suffix_round_trip(tmp_path, writer, name):\n        save_jsonl_atomic(str(tmp_path / 'h.meta'), {\n            'hierarchy': {\n                'use_hierarchy': True, 'delimiter': '-', 'hierarchy_depth': 2,\n            },\n        })\n        db = FolderDB(str(tmp_path))\n        rows = {'first': {'value': 1}}\n        content = (pd.DataFrame.from_dict(rows, orient='index')\n                   if writer.endswith('_df') else rows)\n    \n        getattr(db, writer)(name, content)\n    \n        path = tmp_path / 'a' / 'b' / 'a-b.jsonl'\n>       assert path.is_file()\nE       AssertionError: assert False\nE        +  where False = is_file()\nE        +    where is_file = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r5/a/b/a-b.jsonl').is_file\n\nunit_tests/test_folderdb.py:410: AssertionError\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:115 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r5/config.meta\nWARNING  jsonldb.folderdb:folderdb.py:130 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r5/db.meta\nWARNING  jsonldb.jsonlfile:jsonlfile.py:149 rebuilt missing index /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r5/db.meta.idx\n_______ test_custom_delimiter_suffix_round_trip[a-b.jsonl-overwrite_df] ________\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r6')\nwriter = 'overwrite_df', name = 'a-b.jsonl'\n\n    @pytest.mark.parametrize('writer', [\n        'overwrite_dict', 'upsert_dict', 'overwrite_df', 'upsert_df',\n    ])\n    @pytest.mark.parametrize('name', ['a-b', 'a-b.jsonl'])\n    def test_custom_delimiter_suffix_round_trip(tmp_path, writer, name):\n        save_jsonl_atomic(str(tmp_path / 'h.meta'), {\n            'hierarchy': {\n                'use_hierarchy': True, 'delimiter': '-', 'hierarchy_depth': 2,\n            },\n        })\n        db = FolderDB(str(tmp_path))\n        rows = {'first': {'value': 1}}\n        content = (pd.DataFrame.from_dict(rows, orient='index')\n                   if writer.endswith('_df') else rows)\n    \n        getattr(db, writer)(name, content)\n    \n        path = tmp_path / 'a' / 'b' / 'a-b.jsonl'\n>       assert path.is_file()\nE       AssertionError: assert False\nE        +  where False = is_file()\nE        +    where is_file = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r6/a/b/a-b.jsonl').is_file\n\nunit_tests/test_folderdb.py:410: AssertionError\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:115 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r6/config.meta\nWARNING  jsonldb.folderdb:folderdb.py:130 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r6/db.meta\nWARNING  jsonldb.jsonlfile:jsonlfile.py:149 rebuilt missing index /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r6/db.meta.idx\n_________ test_custom_delimiter_suffix_round_trip[a-b.jsonl-upsert_df] _________\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r7')\nwriter = 'upsert_df', name = 'a-b.jsonl'\n\n    @pytest.mark.parametrize('writer', [\n        'overwrite_dict', 'upsert_dict', 'overwrite_df', 'upsert_df',\n    ])\n    @pytest.mark.parametrize('name', ['a-b', 'a-b.jsonl'])\n    def test_custom_delimiter_suffix_round_trip(tmp_path, writer, name):\n        save_jsonl_atomic(str(tmp_path / 'h.meta'), {\n            'hierarchy': {\n                'use_hierarchy': True, 'delimiter': '-', 'hierarchy_depth': 2,\n            },\n        })\n        db = FolderDB(str(tmp_path))\n        rows = {'first': {'value': 1}}\n        content = (pd.DataFrame.from_dict(rows, orient='index')\n                   if writer.endswith('_df') else rows)\n    \n        getattr(db, writer)(name, content)\n    \n        path = tmp_path / 'a' / 'b' / 'a-b.jsonl'\n>       assert path.is_file()\nE       AssertionError: assert False\nE        +  where False = is_file()\nE        +    where is_file = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r7/a/b/a-b.jsonl').is_file\n\nunit_tests/test_folderdb.py:410: AssertionError\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:115 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r7/config.meta\nWARNING  jsonldb.folderdb:folderdb.py:130 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r7/db.meta\nWARNING  jsonldb.jsonlfile:jsonlfile.py:149 rebuilt missing index /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-649/test_custom_delimiter_suffix_r7/db.meta.idx\n=========================== short test summary info ============================\nFAILED unit_tests/test_folderdb.py::test_custom_delimiter_suffix_round_trip[a-b-overwrite_dict]\nFAILED unit_tests/test_folderdb.py::test_custom_delimiter_suffix_round_trip[a-b-upsert_dict]\nFAILED unit_tests/test_folderdb.py::test_custom_delimiter_suffix_round_trip[a-b-overwrite_df]\nFAILED unit_tests/test_folderdb.py::test_custom_delimiter_suffix_round_trip[a-b-upsert_df]\nFAILED unit_tests/test_folderdb.py::test_custom_delimiter_suffix_round_trip[a-b.jsonl-overwrite_dict]\nFAILED unit_tests/test_folderdb.py::test_custom_delimiter_suffix_round_trip[a-b.jsonl-upsert_dict]\nFAILED unit_tests/test_folderdb.py::test_custom_delimiter_suffix_round_trip[a-b.jsonl-overwrite_df]\nFAILED unit_tests/test_folderdb.py::test_custom_delimiter_suffix_round_trip[a-b.jsonl-upsert_df]\n8 failed, 24 deselected in 0.32s",
            "truncated": true,
            "captured_bytes": 18797
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "custom-delimiter-suffix",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_folderdb.py -k custom_delimiter_suffix",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_folderdb.py",
            "-k",
            "custom_delimiter_suffix"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T01:48:58.882Z",
          "completed_at": "2026-09-05T01:48:59.256Z",
          "duration_ms": 374,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@71c131e0e947c864eb3ebb3766cb11c0d138e62c",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                                  [100%]stdout: \n8 passed, 24 deselected in 0.25s",
            "truncated": false,
            "captured_bytes": 113
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "folder-regressions",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_folderdb.py unit_tests/test_metadata_upkeep.py unit_tests/test_metadata_slot_migration.py",
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
            "unit_tests/test_metadata_slot_migration.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T01:49:09.152Z",
          "completed_at": "2026-09-05T01:49:10.140Z",
          "duration_ms": 988,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@71c131e0e947c864eb3ebb3766cb11c0d138e62c",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                 [100%]stdout: \n41 passed in 0.85s",
            "truncated": false,
            "captured_bytes": 99
          }
        }
      ],
      "acceptance_evidence": [
        {
          "version": 1,
          "id": "V-002",
          "label": "custom-delimiter-suffix",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_folderdb.py -k custom_delimiter_suffix",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_folderdb.py",
            "-k",
            "custom_delimiter_suffix"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T01:48:58.882Z",
          "completed_at": "2026-09-05T01:48:59.256Z",
          "duration_ms": 374,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@71c131e0e947c864eb3ebb3766cb11c0d138e62c",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                                  [100%]stdout: \n8 passed, 24 deselected in 0.25s",
            "truncated": false,
            "captured_bytes": 113
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "folder-regressions",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_folderdb.py unit_tests/test_metadata_upkeep.py unit_tests/test_metadata_slot_migration.py",
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
            "unit_tests/test_metadata_slot_migration.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T01:49:09.152Z",
          "completed_at": "2026-09-05T01:49:10.140Z",
          "duration_ms": 988,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@71c131e0e947c864eb3ebb3766cb11c0d138e62c",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                 [100%]stdout: \n41 passed in 0.85s",
            "truncated": false,
            "captured_bytes": 99
          }
        }
      ]
    }
