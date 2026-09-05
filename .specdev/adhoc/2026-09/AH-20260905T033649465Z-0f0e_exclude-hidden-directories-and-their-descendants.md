# Adhoc AH-20260905T033649465Z-0f0e

- Scope: Exclude hidden directories and their descendants from shared empty-folder pruning while still removing empty visible hierarchy branches after deletes, lint, and clearing
- Title: Preserve hidden directories during empty-folder pruning
- Started: 2026-09-05T03:36:49.465Z
- Completed: 2026-09-05T03:39:04.559Z
- Starting working tree: Clean.

## Outcome

Shared empty-folder pruning now filters hidden directories during a top-down walk, then removes collected visible directories in reverse order. Hidden trees are never entered or removed; nonempty visible parents and the database root remain intact, while empty visible hierarchy branches are pruned. Four new cases reproduced failures before the fix and now pass across direct pruning, table deletion, lint, and clearing, verifying directory traversal, hidden contents, and visible branch removal. Focused folder, report, hierarchy recovery, and metadata upkeep suites pass 85 tests; git diff --check passes.

## Focused workflow coexistence

No focused Assignment or Mission coexistence was recorded.

## Delivery path facts

Two concurrently created user-note files under `.fireplace/notes/` were excluded
from the final delivery and preserved untracked on disk.

### Requested adopted paths

None.

### Committed paths

- `.specdev/adhoc/2026-09/AH-20260905T033649465Z-0f0e_exclude-hidden-directories-and-their-descendants.md`
- `jsonldb/folderdb.py`
- `unit_tests/test_folderdb.py`

### Rejected paths

None.

### Remaining owned paths

None.

## Verification summary

No manual verification summary was supplied.

## Verification attempt history

- **hidden-folder-pruning: failed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_folderdb.py -k empty_folder_pruning` (343 ms, working-tree@193d70d76b8ce68027b36f8759f45b16f6bb7330)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 1
  - Output:

            )]
            for path in hidden_paths:
                path.mkdir(parents=True, exist_ok=True)
            sentinel = tmp_path / '.external' / 'keep.jsonl'
            sentinel.write_bytes(b'{"hidden":{"value":1}}\n')
            scanned = []
            real_scandir = os.scandir
        
            def recording_scandir(path):
                relative = os.path.relpath(path, tmp_path)
                if relative != '.':
                    scanned.append(relative.split(os.sep))
                return real_scandir(path)
        
            monkeypatch.setattr(os, 'scandir', recording_scandir)
            if operation == 'prune':
                db.delete_empty_folders()
            elif operation == 'delete':
                db.delete_file('group.table')
            elif operation == 'lint':
                db.lint_db()
            else:
                db.clear_folder(force=True)
        
    >       assert all(path.is_dir() for path in hidden_paths)
    E       assert False
    E        +  where False = all(<generator object test_empty_folder_pruning_preserves_hidden_trees.<locals>.<genexpr> at 0x1069bdd80>)
    
    unit_tests/test_folderdb.py:494: AssertionError
    ------------------------------ Captured log call -------------------------------
    WARNING  jsonldb.folderdb:folderdb.py:205 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-667/test_empty_folder_pruning_pres3/h.meta
    WARNING  jsonldb.folderdb:folderdb.py:122 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-667/test_empty_folder_pruning_pres3/config.meta
    =========================== short test summary info ============================
    FAILED unit_tests/test_folderdb.py::test_empty_folder_pruning_preserves_hidden_trees[prune]
    FAILED unit_tests/test_folderdb.py::test_empty_folder_pruning_preserves_hidden_trees[delete]
    FAILED unit_tests/test_folderdb.py::test_empty_folder_pruning_preserves_hidden_trees[lint]
    FAILED unit_tests/test_folderdb.py::test_empty_folder_pruning_preserves_hidden_trees[clear]
    4 failed, 32 deselected in 0.22s
- **hidden-folder-pruning: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_folderdb.py -k empty_folder_pruning` (267 ms, working-tree@193d70d76b8ce68027b36f8759f45b16f6bb7330)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout:                                                                      [100%]stdout: 
    4 passed, 32 deselected in 0.15s
- **folder-cleanup-regressions: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_folderdb.py unit_tests/test_reports.py unit_tests/test_hierarchy_recovery.py unit_tests/test_metadata_upkeep.py` (743 ms, working-tree@193d70d76b8ce68027b36f8759f45b16f6bb7330)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 84%]
    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                             [100%]stdout: 
    85 passed in 0.62s

## Current acceptance evidence

- **hidden-folder-pruning: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_folderdb.py -k empty_folder_pruning` (267 ms, working-tree@193d70d76b8ce68027b36f8759f45b16f6bb7330)
- **folder-cleanup-regressions: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_folderdb.py unit_tests/test_reports.py unit_tests/test_hierarchy_recovery.py unit_tests/test_metadata_upkeep.py` (743 ms, working-tree@193d70d76b8ce68027b36f8759f45b16f6bb7330)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 84%]
    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                             [100%]stdout: 
    85 passed in 0.62s

## Structured verification

    {
      "version": 1,
      "path_facts": {
        "requested": [],
        "committed": [
          ".specdev/adhoc/2026-09/AH-20260905T033649465Z-0f0e_exclude-hidden-directories-and-their-descendants.md",
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
          "label": "hidden-folder-pruning",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_folderdb.py -k empty_folder_pruning",
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
            "empty_folder_pruning"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T03:38:20.427Z",
          "completed_at": "2026-09-05T03:38:20.771Z",
          "duration_ms": 343,
          "exit_status": 1,
          "status": "failed",
          "tested_revision": "working-tree@193d70d76b8ce68027b36f8759f45b16f6bb7330",
          "output": {
            "text": "stdout: Fstdout: Fstdout: Fstdout: Fstdout:                                                                      [100%]stdout: \n=================================== FAILURES ===================================\n___________ test_empty_folder_pruning_preserves_hidden_trees[prune] ____________\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-667/test_empty_folder_pruning_pres0')\nmonkeypatch = <_pytest.monkeypatch.MonkeyPatch object at 0x103e35370>\noperation = 'prune'\n\n    @pytest.mark.parametrize('operation', ['prune', 'delete', 'lint', 'clear'])\n    def test_empty_folder_pruning_preserves_hidden_trees(tmp_path, monkeypatch, operation):\n        db = FolderDB(str(tmp_path), hierarchy_depth=2)\n        db.overwrite_dict('group.table', {'row': {'value': 1}})\n        empty_branch = tmp_path / 'unused' / 'branch' / 'deep'\n        empty_branch.mkdir(parents=True)\n        hidden_paths = [tmp_path / relative for relative in (\n            '.invalid_tickers/empty/deep', '.external/cache',\n            '.jsonldb/cache/empty', 'visible/.external/cache',\n        )]\n        for path in hidden_paths:\n            path.mkdir(parents=True, exist_ok=True)\n        sentinel = tmp_path / '.external' / 'keep.jsonl'\n        sentinel.write_bytes(b'{\"hidden\":{\"value\":1}}\\n')\n        scanned = []\n        real_scandir = os.scandir\n    \n        def recording_scandir(path):\n            relative = os.path.relpath(path, tmp_path)\n            if relative != '.':\n                scanned.append(relative.split(os.sep))\n            return real_scandir(path)\n    \n        monkeypatch.setattr(os, 'scandir', recording_scandir)\n        if operation == 'prune':\n            db.delete_empty_folders()\n        elif operation == 'delete':\n            db.delete_file('group.table')\n        elif operation == 'lint':\n            db.lint_db()\n        else:\n            db.clear_folder(force=True)\n    \n>       assert all(path.is_dir() for path in hidden_paths)\nE       assert False\nE        +  where False = all(<generator object test_empty_folder_pruning_preserves_hidden_trees.<locals>.<genexpr> at 0x10692ef60>)\n\nunit_tests/test_folderdb.py:494: AssertionError\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:205 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-667/test_empty_folder_pruning_pres0/h.meta\nWARNING  jsonldb.folderdb:folderdb.py:122 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-667/test_empty_folder_pruning_pres0/config.meta\n___________ test_empty_folder_pruning_preserves_hidden_trees[delete] ___________\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-667/test_empty_folder_pruning_pres1')\nmonkeypatch = <_pytest.monkeypatch.MonkeyPatch object at 0x1069936e0>\noperation = 'delete'\n\n    @pytest.mark.parametrize('operation', ['prune', 'delete', 'lint', 'clear'])\n    def test_empty_folder_pruning_preserves_hidden_trees(tmp_path, monkeypatch, operation):\n        db = FolderDB(str(tmp_path), hierarchy_depth=2)\n        db.overwrite_dict('group.table', {'row': {'value': 1}})\n        empty_branch = tmp_path / 'unused' / 'branch' / 'deep'\n        empty_branch.mkdir(parents=True)\n        hidden_paths = [tmp_path / relative for relative in (\n            '.invalid_tickers/empty/deep', '.external/cache',\n            '.jsonldb/cache/empty', 'visible/.external/cache',\n        )]\n        for path in hidden_paths:\n            path.mkdir(parents=True, exist_ok=True)\n        sentinel = tmp_path / '.external' / 'keep.jsonl'\n        sentinel.write_bytes(b'{\"hidden\":{\"value\":1}}\\n')\n        scanned = []\n        real_scandir = os.scandir\n    \n        def recording_scandir(path):\n            relative = os.path.relpath(path, tmp_path)\n            if relative != '.':\n                scanned.append(relative.split(os.sep))\n            return real_scandir(path)\n    \n        monkeypatch.setattr(os, 'scandir', recording_scandir)\n        if operation == 'prune':\n            db.delete_empty_folders()\n        elif operation == 'delete':\n            db.delete_file('group.table')\n        elif operation == 'lint':\n            db.lint_db()\n        else:\n            db.clear_folder(force=True)\n    \n>       assert all(path.is_dir() for path in hidden_paths)\nE       assert False\nE        +  where False = all(<generator object test_empty_folder_pruning_preserves_hidden_trees.<locals>.<genexpr> at 0x1069bd220>)\n\nunit_tests/test_folderdb.py:494: AssertionError\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:205 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-667/test_empty_folder_pruning_pres1/h.meta\nWARNING  jsonldb.folderdb:folderdb.py:122 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-667/test_empty_folder_pruning_pres1/config.meta\n____________ test_empty_folder_pruning_preserves_hidden_trees[lint] ____________\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-667/test_empty_folder_pruning_pres2')\nmonkeypatch = <_pytest.monkeypatch.MonkeyPatch object at 0x1069cb0b0>\noperation = 'lint'\n\n    @pytest.mark.parametrize('operation', ['prune', 'delete', 'lint', 'clear'])\n    def test_empty_folder_pruning_preserves_hidden_trees(tmp_path, monkeypatch, operation):\n        db = FolderDB(str(tmp_path), hierarchy_depth=2)\n        db.overwrite_dict('group.table', {'row': {'value': 1}})\n        empty_branch = tmp_path / 'unused' / 'branch' / 'deep'\n        empty_branch.mkdir(parents=True)\n        hidden_paths = [tmp_path / relative for relative in (\n            '.invalid_tickers/empty/deep', '.external/cache',\n            '.jsonldb/cache/empty', 'visible/.external/cache',\n        )]\n        for path in hidden_paths:\n            path.mkdir(parents=True, exist_ok=True)\n        sentinel = tmp_path / '.external' / 'keep.jsonl'\n        sentinel.write_bytes(b'{\"hidden\":{\"value\":1}}\\n')\n        scanned = []\n        real_scandir = os.scandir\n    \n        def recording_scandir(path):\n            relative = os.path.relpath(path, tmp_path)\n            if relative != '.':\n                scanned.append(relative.split(os.sep))\n            return real_scandir(path)\n    \n        monkeypatch.setattr(os, 'scandir', recording_scandir)\n        if operation == 'prune':\n            db.delete_empty_folders()\n        elif operation == 'delete':\n            db.delete_file('group.table')\n        elif operation == 'lint':\n            db.lint_db()\n        else:\n            db.clear_folder(force=True)\n    \n>       assert all(path.is_dir() for path in hidden_paths)\nE       assert False\nE        +  where False = all(<generator object test_empty_folder_pruning_preserves_hidden_trees.<locals>.<genexpr> at 0x1069bda40>)\n\nunit_tests/test_folderdb.py:494: AssertionError\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:205 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-667/test_empty_folder_pruning_pres2/h.meta\nWARNING  jsonldb.folderdb:folderdb.py:122 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-667/test_empty_folder_pruning_pres2/config.meta\n___________ test_empty_folder_pruning_preserves_hidden_trees[clear] ____________\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-667/test_empty_folder_pruning_pres3')\nmonkeypatch = <_pytest.monkeypatch.MonkeyPatch object at 0x106a19070>\noperation = 'clear'\n\n    @pytest.mark.parametrize('operation', ['prune', 'delete', 'lint', 'clear'])\n    def test_empty_folder_pruning_preserves_hidden_trees(tmp_path, monkeypatch, operation):\n        db = FolderDB(str(tmp_path), hierarchy_depth=2)\n        db.overwrite_dict('group.table', {'row': {'value': 1}})\n        empty_branch = tmp_path / 'unused' / 'branch' / 'deep'\n        empty_branch.mkdir(parents=True)\n        hidden_paths = [tmp_path / relative for relative in (\n            '.invalid_tickers/empty/deep', '.external/cache',\n            '.jsonldb/cache/empty', 'visible/.external/cache',\n        )]\n        for path in hidden_paths:\n            path.mkdir(parents=True, exist_ok=True)\n        sentinel = tmp_path / '.external' / 'keep.jsonl'\n        sentinel.write_bytes(b'{\"hidden\":{\"value\":1}}\\n')\n        scanned = []\n        real_scandir = os.scandir\n    \n        def recording_scandir(path):\n            relative = os.path.relpath(path, tmp_path)\n            if relative != '.':\n                scanned.append(relative.split(os.sep))\n            return real_scandir(path)\n    \n        monkeypatch.setattr(os, 'scandir', recording_scandir)\n        if operation == 'prune':\n            db.delete_empty_folders()\n        elif operation == 'delete':\n            db.delete_file('group.table')\n        elif operation == 'lint':\n            db.lint_db()\n        else:\n            db.clear_folder(force=True)\n    \n>       assert all(path.is_dir() for path in hidden_paths)\nE       assert False\nE        +  where False = all(<generator object test_empty_folder_pruning_preserves_hidden_trees.<locals>.<genexpr> at 0x1069bdd80>)\n\nunit_tests/test_folderdb.py:494: AssertionError\n------------------------------ Captured log call -------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:205 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-667/test_empty_folder_pruning_pres3/h.meta\nWARNING  jsonldb.folderdb:folderdb.py:122 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-667/test_empty_folder_pruning_pres3/config.meta\n=========================== short test summary info ============================\nFAILED unit_tests/test_folderdb.py::test_empty_folder_pruning_preserves_hidden_trees[prune]\nFAILED unit_tests/test_folderdb.py::test_empty_folder_pruning_preserves_hidden_trees[delete]\nFAILED unit_tests/test_folderdb.py::test_empty_folder_pruning_preserves_hidden_trees[lint]\nFAILED unit_tests/test_folderdb.py::test_empty_folder_pruning_preserves_hidden_trees[clear]\n4 failed, 32 deselected in 0.22s",
            "truncated": false,
            "captured_bytes": 10495
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "hidden-folder-pruning",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_folderdb.py -k empty_folder_pruning",
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
            "empty_folder_pruning"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T03:38:38.803Z",
          "completed_at": "2026-09-05T03:38:39.071Z",
          "duration_ms": 267,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@193d70d76b8ce68027b36f8759f45b16f6bb7330",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout:                                                                      [100%]stdout: \n4 passed, 32 deselected in 0.15s",
            "truncated": false,
            "captured_bytes": 113
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "folder-cleanup-regressions",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_folderdb.py unit_tests/test_reports.py unit_tests/test_hierarchy_recovery.py unit_tests/test_metadata_upkeep.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_folderdb.py",
            "unit_tests/test_reports.py",
            "unit_tests/test_hierarchy_recovery.py",
            "unit_tests/test_metadata_upkeep.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T03:38:39.227Z",
          "completed_at": "2026-09-05T03:38:39.971Z",
          "duration_ms": 743,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@193d70d76b8ce68027b36f8759f45b16f6bb7330",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 84%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                             [100%]stdout: \n85 passed in 0.62s",
            "truncated": false,
            "captured_bytes": 179
          }
        }
      ],
      "acceptance_evidence": [
        {
          "version": 1,
          "id": "V-002",
          "label": "hidden-folder-pruning",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_folderdb.py -k empty_folder_pruning",
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
            "empty_folder_pruning"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T03:38:38.803Z",
          "completed_at": "2026-09-05T03:38:39.071Z",
          "duration_ms": 267,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@193d70d76b8ce68027b36f8759f45b16f6bb7330",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout:                                                                      [100%]stdout: \n4 passed, 32 deselected in 0.15s",
            "truncated": false,
            "captured_bytes": 113
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "folder-cleanup-regressions",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_folderdb.py unit_tests/test_reports.py unit_tests/test_hierarchy_recovery.py unit_tests/test_metadata_upkeep.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_folderdb.py",
            "unit_tests/test_reports.py",
            "unit_tests/test_hierarchy_recovery.py",
            "unit_tests/test_metadata_upkeep.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T03:38:39.227Z",
          "completed_at": "2026-09-05T03:38:39.971Z",
          "duration_ms": 743,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@193d70d76b8ce68027b36f8759f45b16f6bb7330",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 84%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                             [100%]stdout: \n85 passed in 0.62s",
            "truncated": false,
            "captured_bytes": 179
          }
        }
      ]
    }
