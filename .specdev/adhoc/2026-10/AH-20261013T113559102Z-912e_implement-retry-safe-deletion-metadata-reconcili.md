# Adhoc AH-20261013T113559102Z-912e

- Scope: Implement retry-safe deletion metadata reconciliation, optional statistics, and ancestor-only cleanup with focused regression and performance verification
- Title: fix: reconcile deletion retries without unnecessary scans
- Started: 2026-10-13T11:35:59.102Z
- Completed: 2026-10-13T11:39:31.888Z
- Starting working tree: Clean.

## Outcome

Implemented optional deletion statistics, compare-before-refresh metadata reconciliation on no-match retries, and retryable ancestor-only folder cleanup. Preserved already-empty tables, consumer metadata, suffix normalization, hidden trees and the root; unrelated empty folders remain for explicit pruning. Added failure-injection and no-rewrite regression coverage. Focused verification: 349 tests passed on Python 3.11.12 with locked dependencies. Actual before/after medians over 21 randomized repetitions at 10000 rows: public delete 1.2155/1.0566 ms, partial delete 1.4595/1.4711 ms, healthy no-match 0.6478/0.8215 ms, metadata retry 0.6321 ms incorrect/1.0836 ms repaired. Final removal with 200 unrelated folders 8.2887/0.4962 ms. Local warm-filesystem results, not universal performance guarantees. jsonlfile.py is 950 lines and folderdb.py is 1118 lines, within design caps.

## Focused workflow coexistence

No focused Assignment or Mission coexistence was recorded.

## Delivery path facts

### Requested adopted paths

None.

### Committed paths

- `.specdev/adhoc/2026-10/AH-20261013T113559102Z-912e_implement-retry-safe-deletion-metadata-reconcili.md`
- `jsonldb/folderdb.py`
- `jsonldb/jsonlfile.py`
- `unit_tests/test_folderdb.py`
- `unit_tests/test_remove_empty.py`

### Rejected paths

None.

### Remaining owned paths

None.

## Verification summary

No manual verification summary was supplied.

## Verification attempt history

- **deletion-regressions: failed.** `.venv/bin/python -m pytest -q unit_tests/test_remove_empty.py unit_tests/test_metadata_upkeep.py unit_tests/test_folderdb.py unit_tests/test_jsonlfile.py unit_tests/test_index_cache.py unit_tests/test_table_timezone.py unit_tests/test_max_hierarchy.py unit_tests/test_metaslot.py` (2302 ms, working-tree@368bd818cce43adecde771c0fd95d37c27f86781)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 1
  - Output:

    (db, '_update_dbmeta', fail)
    >       delete()  # A healthy retry must not publish metadata again.
            ^^^^^^^^
    
    unit_tests/test_remove_empty.py:138: 
    _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 
    unit_tests/test_remove_empty.py:111: in delete
        db.delete_file_range('err.sample', '2026-01-01', '2026-01-01',
    jsonldb/folderdb.py:872: in delete_file_range
        self._finish_deletion(name, stats, remove_if_empty)
    jsonldb/folderdb.py:840: in _finish_deletion
        self._update_dbmeta(name, stats=stats)
    _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 
    
    args = ('err.sample.jsonl',)
    kwargs = {'stats': {'count': 2, 'max_index': '2026-01-03', 'min_index': '2026-01-02', 'size': 343}}
    
        def fail(*args, **kwargs):
    >       raise OSError('injected metadata refresh')
    E       OSError: injected metadata refresh
    
    unit_tests/test_remove_empty.py:118: OSError
    ------------------------------ Captured log setup ------------------------------
    WARNING  jsonldb.folderdb:folderdb.py:234 recovered hierarchy for /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-429/test_retry_reconciles_metadata3/h.meta: source=missing observed_depths=[] depth=1 delimiter='.' tables=0 maximum=explicit
    WARNING  jsonldb.folderdb:folderdb.py:133 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-429/test_retry_reconciles_metadata3/config.meta
    =========================== short test summary info ============================
    FAILED unit_tests/test_remove_empty.py::test_retry_reconciles_metadata_without_rewriting_healthy_state[False-False]
    FAILED unit_tests/test_remove_empty.py::test_retry_reconciles_metadata_without_rewriting_healthy_state[False-True]
    FAILED unit_tests/test_remove_empty.py::test_retry_reconciles_metadata_without_rewriting_healthy_state[True-False]
    FAILED unit_tests/test_remove_empty.py::test_retry_reconciles_metadata_without_rewriting_healthy_state[True-True]
    4 failed, 345 passed in 1.86s
- **deletion-regressions: passed.** `.venv/bin/python -m pytest -q unit_tests/test_remove_empty.py unit_tests/test_metadata_upkeep.py unit_tests/test_folderdb.py unit_tests/test_jsonlfile.py unit_tests/test_index_cache.py unit_tests/test_table_timezone.py unit_tests/test_max_hierarchy.py unit_tests/test_metaslot.py` (1999 ms, working-tree@368bd818cce43adecde771c0fd95d37c27f86781)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    tdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 41%]
    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 61%]
    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 82%]
    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:             [100%]stdout: 
    349 passed in 1.72s
- **deletion-performance: passed.** `.venv/bin/python .specdev/cache/perf-deletion/actual.py` (1798 ms, working-tree@368bd818cce43adecde771c0fd95d37c27f86781)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: {"case": "public_delete", "median_ms": {"before": 1.2155, "after": 1.0566}}
    stdout: {"case": "partial_delete", "median_ms": {"before": 1.4595, "after": 1.4711}}
    stdout: {"case": "healthy_no_match", "median_ms": {"before": 0.6478, "after": 0.8215}}
    stdout: {"case": "retry_metadata", "median_ms": {"before": 0.6321, "after": 1.0836}}
    stdout: {"case": "remove_last", "median_ms": {"before": 8.2887, "after": 0.4962}}
- **diff-check: passed.** `git diff --check` (12 ms, working-tree@368bd818cce43adecde771c0fd95d37c27f86781)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    (no output)

## Current acceptance evidence

- **deletion-regressions: passed.** `.venv/bin/python -m pytest -q unit_tests/test_remove_empty.py unit_tests/test_metadata_upkeep.py unit_tests/test_folderdb.py unit_tests/test_jsonlfile.py unit_tests/test_index_cache.py unit_tests/test_table_timezone.py unit_tests/test_max_hierarchy.py unit_tests/test_metaslot.py` (1999 ms, working-tree@368bd818cce43adecde771c0fd95d37c27f86781)
- **deletion-performance: passed.** `.venv/bin/python .specdev/cache/perf-deletion/actual.py` (1798 ms, working-tree@368bd818cce43adecde771c0fd95d37c27f86781)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: {"case": "public_delete", "median_ms": {"before": 1.2155, "after": 1.0566}}
    stdout: {"case": "partial_delete", "median_ms": {"before": 1.4595, "after": 1.4711}}
    stdout: {"case": "healthy_no_match", "median_ms": {"before": 0.6478, "after": 0.8215}}
    stdout: {"case": "retry_metadata", "median_ms": {"before": 0.6321, "after": 1.0836}}
    stdout: {"case": "remove_last", "median_ms": {"before": 8.2887, "after": 0.4962}}
- **diff-check: passed.** `git diff --check` (12 ms, working-tree@368bd818cce43adecde771c0fd95d37c27f86781)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    (no output)

## Structured verification

    {
      "version": 1,
      "path_facts": {
        "requested": [],
        "committed": [
          ".specdev/adhoc/2026-10/AH-20261013T113559102Z-912e_implement-retry-safe-deletion-metadata-reconcili.md",
          "jsonldb/folderdb.py",
          "jsonldb/jsonlfile.py",
          "unit_tests/test_folderdb.py",
          "unit_tests/test_remove_empty.py"
        ],
        "rejected": [],
        "remaining": []
      },
      "attempt_history": [
        {
          "version": 1,
          "id": "V-001",
          "label": "deletion-regressions",
          "annotation": null,
          "command": ".venv/bin/python -m pytest -q unit_tests/test_remove_empty.py unit_tests/test_metadata_upkeep.py unit_tests/test_folderdb.py unit_tests/test_jsonlfile.py unit_tests/test_index_cache.py unit_tests/test_table_timezone.py unit_tests/test_max_hierarchy.py unit_tests/test_metaslot.py",
          "argv": [
            ".venv/bin/python",
            "-m",
            "pytest",
            "-q",
            "unit_tests/test_remove_empty.py",
            "unit_tests/test_metadata_upkeep.py",
            "unit_tests/test_folderdb.py",
            "unit_tests/test_jsonlfile.py",
            "unit_tests/test_index_cache.py",
            "unit_tests/test_table_timezone.py",
            "unit_tests/test_max_hierarchy.py",
            "unit_tests/test_metaslot.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-10-13T11:37:34.423Z",
          "completed_at": "2026-10-13T11:37:36.725Z",
          "duration_ms": 2302,
          "exit_status": 1,
          "status": "failed",
          "tested_revision": "working-tree@368bd818cce43adecde771c0fd95d37c27f86781",
          "output": {
            "text": "dout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 41%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 61%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 82%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:             [100%]stdout: \n=================================== FAILURES ===================================\n_ test_retry_reconciles_metadata_without_rewriting_healthy_state[False-False] __\n\ntable = (FolderDB at /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-429/test_retry_reconciles.../fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-429/test_retry_reconciles_metadata0/err/err.sample.jsonl'))\nmonkeypatch = <_pytest.monkeypatch.MonkeyPatch object at 0x104b627d0>\nranged = False, remove_if_empty = False\n\n    @pytest.mark.parametrize('ranged', [False, True])\n    @pytest.mark.parametrize('remove_if_empty', [False, True])\n    def test_retry_reconciles_metadata_without_rewriting_healthy_state(\n            table, monkeypatch, ranged, remove_if_empty):\n        db, path = table\n        def delete():\n            if ranged:\n                db.delete_file_range('err.sample', '2026-01-01', '2026-01-01',\n                                     remove_if_empty=remove_if_empty)\n            else:\n                db.delete_file_keys('err.sample', ['2026-01-01'],\n                                    remove_if_empty=remove_if_empty)\n        original = db._update_dbmeta\n        def fail(*args, **kwargs):\n            raise OSError('injected metadata refresh')\n        monkeypatch.setattr(db, '_update_dbmeta', fail)\n        with pytest.raises(OSError, match='injected'):\n            delete()\n        monkeypatch.setattr(db, '_update_dbmeta', original)\n        calls = []\n        load = jsonlfile.load_index\n        def counted(file):\n            if str(file) == str(path):\n                calls.append(file)\n            return load(file)\n        monkeypatch.setattr(jsonlfile, 'load_index', counted)\n        before = path.read_bytes()\n        delete()\n        assert len(calls) == 1\n        entry = db.get_dbmeta()['err.sample']\n        assert (entry['count'], entry['min_index'], entry['max_index']) == (\n            2, '2026-01-02', '2026-01-03')\n        assert path.read_bytes() == before\n        monkeypatch.setattr(db, '_update_dbmeta', fail)\n>       delete()  # A healthy retry must not publish metadata again.\n        ^^^^^^^^\n\nunit_tests/test_remove_empty.py:138: \n_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ \nunit_tests/test_remove_empty.py:114: in delete\n    db.delete_file_keys('err.sample', ['2026-01-01'],\njsonldb/folderdb.py:854: in delete_file_keys\n    self._finish_deletion(name, stats, remove_if_empty)\njsonldb/folderdb.py:840: in _finish_deletion\n    self._update_dbmeta(name, stats=stats)\n_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ \n\nargs = ('err.sample.jsonl',)\nkwargs = {'stats': {'count': 2, 'max_index': '2026-01-03', 'min_index': '2026-01-02', 'size': 343}}\n\n    def fail(*args, **kwargs):\n>       raise OSError('injected metadata refresh')\nE       OSError: injected metadata refresh\n\nunit_tests/test_remove_empty.py:118: OSError\n------------------------------ Captured log setup ------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:234 recovered hierarchy for /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-429/test_retry_reconciles_metadata0/h.meta: source=missing observed_depths=[] depth=1 delimiter='.' tables=0 maximum=explicit\nWARNING  jsonldb.folderdb:folderdb.py:133 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-429/test_retry_reconciles_metadata0/config.meta\n__ test_retry_reconciles_metadata_without_rewriting_healthy_state[False-True] __\n\ntable = (FolderDB at /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-429/test_retry_reconciles.../fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-429/test_retry_reconciles_metadata1/err/err.sample.jsonl'))\nmonkeypatch = <_pytest.monkeypatch.MonkeyPatch object at 0x104cad510>\nranged = True, remove_if_empty = False\n\n    @pytest.mark.parametrize('ranged', [False, True])\n    @pytest.mark.parametrize('remove_if_empty', [False, True])\n    def test_retry_reconciles_metadata_without_rewriting_healthy_state(\n            table, monkeypatch, ranged, remove_if_empty):\n        db, path = table\n        def delete():\n            if ranged:\n                db.delete_file_range('err.sample', '2026-01-01', '2026-01-01',\n                                     remove_if_empty=remove_if_empty)\n            else:\n                db.delete_file_keys('err.sample', ['2026-01-01'],\n                                    remove_if_empty=remove_if_empty)\n        original = db._update_dbmeta\n        def fail(*args, **kwargs):\n            raise OSError('injected metadata refresh')\n        monkeypatch.setattr(db, '_update_dbmeta', fail)\n        with pytest.raises(OSError, match='injected'):\n            delete()\n        monkeypatch.setattr(db, '_update_dbmeta', original)\n        calls = []\n        load = jsonlfile.load_index\n        def counted(file):\n            if str(file) == str(path):\n                calls.append(file)\n            return load(file)\n        monkeypatch.setattr(jsonlfile, 'load_index', counted)\n        before = path.read_bytes()\n        delete()\n        assert len(calls) == 1\n        entry = db.get_dbmeta()['err.sample']\n        assert (entry['count'], entry['min_index'], entry['max_index']) == (\n            2, '2026-01-02', '2026-01-03')\n        assert path.read_bytes() == before\n        monkeypatch.setattr(db, '_update_dbmeta', fail)\n>       delete()  # A healthy retry must not publish metadata again.\n        ^^^^^^^^\n\nunit_tests/test_remove_empty.py:138: \n_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ \nunit_tests/test_remove_empty.py:111: in delete\n    db.delete_file_range('err.sample', '2026-01-01', '2026-01-01',\njsonldb/folderdb.py:872: in delete_file_range\n    self._finish_deletion(name, stats, remove_if_empty)\njsonldb/folderdb.py:840: in _finish_deletion\n    self._update_dbmeta(name, stats=stats)\n_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ \n\nargs = ('err.sample.jsonl',)\nkwargs = {'stats': {'count': 2, 'max_index': '2026-01-03', 'min_index': '2026-01-02', 'size': 343}}\n\n    def fail(*args, **kwargs):\n>       raise OSError('injected metadata refresh')\nE       OSError: injected metadata refresh\n\nunit_tests/test_remove_empty.py:118: OSError\n------------------------------ Captured log setup ------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:234 recovered hierarchy for /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-429/test_retry_reconciles_metadata1/h.meta: source=missing observed_depths=[] depth=1 delimiter='.' tables=0 maximum=explicit\nWARNING  jsonldb.folderdb:folderdb.py:133 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-429/test_retry_reconciles_metadata1/config.meta\n__ test_retry_reconciles_metadata_without_rewriting_healthy_state[True-False] __\n\ntable = (FolderDB at /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-429/test_retry_reconciles.../fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-429/test_retry_reconciles_metadata2/err/err.sample.jsonl'))\nmonkeypatch = <_pytest.monkeypatch.MonkeyPatch object at 0x104c5b290>\nranged = False, remove_if_empty = True\n\n    @pytest.mark.parametrize('ranged', [False, True])\n    @pytest.mark.parametrize('remove_if_empty', [False, True])\n    def test_retry_reconciles_metadata_without_rewriting_healthy_state(\n            table, monkeypatch, ranged, remove_if_empty):\n        db, path = table\n        def delete():\n            if ranged:\n                db.delete_file_range('err.sample', '2026-01-01', '2026-01-01',\n                                     remove_if_empty=remove_if_empty)\n            else:\n                db.delete_file_keys('err.sample', ['2026-01-01'],\n                                    remove_if_empty=remove_if_empty)\n        original = db._update_dbmeta\n        def fail(*args, **kwargs):\n            raise OSError('injected metadata refresh')\n        monkeypatch.setattr(db, '_update_dbmeta', fail)\n        with pytest.raises(OSError, match='injected'):\n            delete()\n        monkeypatch.setattr(db, '_update_dbmeta', original)\n        calls = []\n        load = jsonlfile.load_index\n        def counted(file):\n            if str(file) == str(path):\n                calls.append(file)\n            return load(file)\n        monkeypatch.setattr(jsonlfile, 'load_index', counted)\n        before = path.read_bytes()\n        delete()\n        assert len(calls) == 1\n        entry = db.get_dbmeta()['err.sample']\n        assert (entry['count'], entry['min_index'], entry['max_index']) == (\n            2, '2026-01-02', '2026-01-03')\n        assert path.read_bytes() == before\n        monkeypatch.setattr(db, '_update_dbmeta', fail)\n>       delete()  # A healthy retry must not publish metadata again.\n        ^^^^^^^^\n\nunit_tests/test_remove_empty.py:138: \n_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ \nunit_tests/test_remove_empty.py:114: in delete\n    db.delete_file_keys('err.sample', ['2026-01-01'],\njsonldb/folderdb.py:854: in delete_file_keys\n    self._finish_deletion(name, stats, remove_if_empty)\njsonldb/folderdb.py:840: in _finish_deletion\n    self._update_dbmeta(name, stats=stats)\n_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ \n\nargs = ('err.sample.jsonl',)\nkwargs = {'stats': {'count': 2, 'max_index': '2026-01-03', 'min_index': '2026-01-02', 'size': 343}}\n\n    def fail(*args, **kwargs):\n>       raise OSError('injected metadata refresh')\nE       OSError: injected metadata refresh\n\nunit_tests/test_remove_empty.py:118: OSError\n------------------------------ Captured log setup ------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:234 recovered hierarchy for /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-429/test_retry_reconciles_metadata2/h.meta: source=missing observed_depths=[] depth=1 delimiter='.' tables=0 maximum=explicit\nWARNING  jsonldb.folderdb:folderdb.py:133 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-429/test_retry_reconciles_metadata2/config.meta\n__ test_retry_reconciles_metadata_without_rewriting_healthy_state[True-True] ___\n\ntable = (FolderDB at /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-429/test_retry_reconciles.../fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-429/test_retry_reconciles_metadata3/err/err.sample.jsonl'))\nmonkeypatch = <_pytest.monkeypatch.MonkeyPatch object at 0x104bd17d0>\nranged = True, remove_if_empty = True\n\n    @pytest.mark.parametrize('ranged', [False, True])\n    @pytest.mark.parametrize('remove_if_empty', [False, True])\n    def test_retry_reconciles_metadata_without_rewriting_healthy_state(\n            table, monkeypatch, ranged, remove_if_empty):\n        db, path = table\n        def delete():\n            if ranged:\n                db.delete_file_range('err.sample', '2026-01-01', '2026-01-01',\n                                     remove_if_empty=remove_if_empty)\n            else:\n                db.delete_file_keys('err.sample', ['2026-01-01'],\n                                    remove_if_empty=remove_if_empty)\n        original = db._update_dbmeta\n        def fail(*args, **kwargs):\n            raise OSError('injected metadata refresh')\n        monkeypatch.setattr(db, '_update_dbmeta', fail)\n        with pytest.raises(OSError, match='injected'):\n            delete()\n        monkeypatch.setattr(db, '_update_dbmeta', original)\n        calls = []\n        load = jsonlfile.load_index\n        def counted(file):\n            if str(file) == str(path):\n                calls.append(file)\n            return load(file)\n        monkeypatch.setattr(jsonlfile, 'load_index', counted)\n        before = path.read_bytes()\n        delete()\n        assert len(calls) == 1\n        entry = db.get_dbmeta()['err.sample']\n        assert (entry['count'], entry['min_index'], entry['max_index']) == (\n            2, '2026-01-02', '2026-01-03')\n        assert path.read_bytes() == before\n        monkeypatch.setattr(db, '_update_dbmeta', fail)\n>       delete()  # A healthy retry must not publish metadata again.\n        ^^^^^^^^\n\nunit_tests/test_remove_empty.py:138: \n_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ \nunit_tests/test_remove_empty.py:111: in delete\n    db.delete_file_range('err.sample', '2026-01-01', '2026-01-01',\njsonldb/folderdb.py:872: in delete_file_range\n    self._finish_deletion(name, stats, remove_if_empty)\njsonldb/folderdb.py:840: in _finish_deletion\n    self._update_dbmeta(name, stats=stats)\n_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ \n\nargs = ('err.sample.jsonl',)\nkwargs = {'stats': {'count': 2, 'max_index': '2026-01-03', 'min_index': '2026-01-02', 'size': 343}}\n\n    def fail(*args, **kwargs):\n>       raise OSError('injected metadata refresh')\nE       OSError: injected metadata refresh\n\nunit_tests/test_remove_empty.py:118: OSError\n------------------------------ Captured log setup ------------------------------\nWARNING  jsonldb.folderdb:folderdb.py:234 recovered hierarchy for /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-429/test_retry_reconciles_metadata3/h.meta: source=missing observed_depths=[] depth=1 delimiter='.' tables=0 maximum=explicit\nWARNING  jsonldb.folderdb:folderdb.py:133 regenerated missing control file /private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-429/test_retry_reconciles_metadata3/config.meta\n=========================== short test summary info ============================\nFAILED unit_tests/test_remove_empty.py::test_retry_reconciles_metadata_without_rewriting_healthy_state[False-False]\nFAILED unit_tests/test_remove_empty.py::test_retry_reconciles_metadata_without_rewriting_healthy_state[False-True]\nFAILED unit_tests/test_remove_empty.py::test_retry_reconciles_metadata_without_rewriting_healthy_state[True-False]\nFAILED unit_tests/test_remove_empty.py::test_retry_reconciles_metadata_without_rewriting_healthy_state[True-True]\n4 failed, 345 passed in 1.86s",
            "truncated": false,
            "captured_bytes": 14720
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "deletion-regressions",
          "annotation": null,
          "command": ".venv/bin/python -m pytest -q unit_tests/test_remove_empty.py unit_tests/test_metadata_upkeep.py unit_tests/test_folderdb.py unit_tests/test_jsonlfile.py unit_tests/test_index_cache.py unit_tests/test_table_timezone.py unit_tests/test_max_hierarchy.py unit_tests/test_metaslot.py",
          "argv": [
            ".venv/bin/python",
            "-m",
            "pytest",
            "-q",
            "unit_tests/test_remove_empty.py",
            "unit_tests/test_metadata_upkeep.py",
            "unit_tests/test_folderdb.py",
            "unit_tests/test_jsonlfile.py",
            "unit_tests/test_index_cache.py",
            "unit_tests/test_table_timezone.py",
            "unit_tests/test_max_hierarchy.py",
            "unit_tests/test_metaslot.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-10-13T11:38:24.304Z",
          "completed_at": "2026-10-13T11:38:26.303Z",
          "duration_ms": 1999,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@368bd818cce43adecde771c0fd95d37c27f86781",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 20%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 41%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 61%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 82%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:             [100%]stdout: \n349 passed in 1.72s",
            "truncated": false,
            "captured_bytes": 420
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "deletion-performance",
          "annotation": null,
          "command": ".venv/bin/python .specdev/cache/perf-deletion/actual.py",
          "argv": [
            ".venv/bin/python",
            ".specdev/cache/perf-deletion/actual.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-10-13T11:38:57.709Z",
          "completed_at": "2026-10-13T11:38:59.507Z",
          "duration_ms": 1798,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@368bd818cce43adecde771c0fd95d37c27f86781",
          "output": {
            "text": "stdout: {\"case\": \"public_delete\", \"median_ms\": {\"before\": 1.2155, \"after\": 1.0566}}\nstdout: {\"case\": \"partial_delete\", \"median_ms\": {\"before\": 1.4595, \"after\": 1.4711}}\nstdout: {\"case\": \"healthy_no_match\", \"median_ms\": {\"before\": 0.6478, \"after\": 0.8215}}\nstdout: {\"case\": \"retry_metadata\", \"median_ms\": {\"before\": 0.6321, \"after\": 1.0836}}\nstdout: {\"case\": \"remove_last\", \"median_ms\": {\"before\": 8.2887, \"after\": 0.4962}}",
            "truncated": false,
            "captured_bytes": 383
          }
        },
        {
          "version": 1,
          "id": "V-004",
          "label": "diff-check",
          "annotation": null,
          "command": "git diff --check",
          "argv": [
            "git",
            "diff",
            "--check"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-10-13T11:39:31.676Z",
          "completed_at": "2026-10-13T11:39:31.688Z",
          "duration_ms": 12,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@368bd818cce43adecde771c0fd95d37c27f86781",
          "output": {
            "text": "",
            "truncated": false,
            "captured_bytes": 0
          }
        }
      ],
      "acceptance_evidence": [
        {
          "version": 1,
          "id": "V-002",
          "label": "deletion-regressions",
          "annotation": null,
          "command": ".venv/bin/python -m pytest -q unit_tests/test_remove_empty.py unit_tests/test_metadata_upkeep.py unit_tests/test_folderdb.py unit_tests/test_jsonlfile.py unit_tests/test_index_cache.py unit_tests/test_table_timezone.py unit_tests/test_max_hierarchy.py unit_tests/test_metaslot.py",
          "argv": [
            ".venv/bin/python",
            "-m",
            "pytest",
            "-q",
            "unit_tests/test_remove_empty.py",
            "unit_tests/test_metadata_upkeep.py",
            "unit_tests/test_folderdb.py",
            "unit_tests/test_jsonlfile.py",
            "unit_tests/test_index_cache.py",
            "unit_tests/test_table_timezone.py",
            "unit_tests/test_max_hierarchy.py",
            "unit_tests/test_metaslot.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-10-13T11:38:24.304Z",
          "completed_at": "2026-10-13T11:38:26.303Z",
          "duration_ms": 1999,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@368bd818cce43adecde771c0fd95d37c27f86781",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 20%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 41%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 61%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 82%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:             [100%]stdout: \n349 passed in 1.72s",
            "truncated": false,
            "captured_bytes": 420
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "deletion-performance",
          "annotation": null,
          "command": ".venv/bin/python .specdev/cache/perf-deletion/actual.py",
          "argv": [
            ".venv/bin/python",
            ".specdev/cache/perf-deletion/actual.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-10-13T11:38:57.709Z",
          "completed_at": "2026-10-13T11:38:59.507Z",
          "duration_ms": 1798,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@368bd818cce43adecde771c0fd95d37c27f86781",
          "output": {
            "text": "stdout: {\"case\": \"public_delete\", \"median_ms\": {\"before\": 1.2155, \"after\": 1.0566}}\nstdout: {\"case\": \"partial_delete\", \"median_ms\": {\"before\": 1.4595, \"after\": 1.4711}}\nstdout: {\"case\": \"healthy_no_match\", \"median_ms\": {\"before\": 0.6478, \"after\": 0.8215}}\nstdout: {\"case\": \"retry_metadata\", \"median_ms\": {\"before\": 0.6321, \"after\": 1.0836}}\nstdout: {\"case\": \"remove_last\", \"median_ms\": {\"before\": 8.2887, \"after\": 0.4962}}",
            "truncated": false,
            "captured_bytes": 383
          }
        },
        {
          "version": 1,
          "id": "V-004",
          "label": "diff-check",
          "annotation": null,
          "command": "git diff --check",
          "argv": [
            "git",
            "diff",
            "--check"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-10-13T11:39:31.676Z",
          "completed_at": "2026-10-13T11:39:31.688Z",
          "duration_ms": 12,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@368bd818cce43adecde771c0fd95d37c27f86781",
          "output": {
            "text": "",
            "truncated": false,
            "captured_bytes": 0
          }
        }
      ]
    }
