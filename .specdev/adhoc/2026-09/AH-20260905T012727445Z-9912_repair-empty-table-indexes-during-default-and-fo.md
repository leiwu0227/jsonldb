# Adhoc AH-20260905T012727445Z-9912

- Scope: Repair empty-table indexes during default and forced lint without rewriting empty data files
- Title: Fix lint index repair for empty tables
- Started: 2026-09-05T01:27:27.445Z
- Completed: 2026-09-05T01:28:41.517Z
- Starting working tree: Clean.

## Outcome

Removed the empty-file early return so default and forced lint validate and repair empty-table indexes through the normal lint path. Added regressions for missing, empty, malformed, non-object, phantom-key, and canonical indexes; all preserve empty data bytes, inode, and modification time. The new regression reproduced six failures before the fix and passes all twelve cases after it; focused lint, compaction, and roadmap compliance suites pass 49 tests.

## Focused workflow coexistence

No focused Assignment or Mission coexistence was recorded.

## Delivery path facts

### Requested adopted paths

None.

### Committed paths

- `.specdev/adhoc/2026-09/AH-20260905T012727445Z-9912_repair-empty-table-indexes-during-default-and-fo.md`
- `jsonldb/jsonlfile.py`
- `unit_tests/test_lint_integrity.py`

### Rejected paths

None.

### Remaining owned paths

None.

## Verification summary

No manual verification summary was supplied.

## Verification attempt history

- **empty-table-lint: failed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_lint_integrity.py -k empty_table_lint` (312 ms, working-tree@b4186ee0537bfe5f60beaa1b0fbb4359380d1adc)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 1
  - Output:

    ithout_rewriting_data[phantom-True] ___
    
    tmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-646/test_empty_table_lint_repairs_9')
    force = True, index_bytes = b'{"ghost":0}'
    
        @pytest.mark.parametrize('force', [False, True])
        @pytest.mark.parametrize('index_bytes', [
            None, b'', b'{', b'[]', b'{"ghost":0}', b'{}',
        ], ids=['missing', 'empty', 'malformed', 'non-object', 'phantom', 'canonical'])
        def test_empty_table_lint_repairs_index_without_rewriting_data(
                tmp_path, force, index_bytes):
            path = tmp_path / 'empty.jsonl'
            path.write_bytes(b'')
            original = path.stat()
            index_path = tmp_path / 'empty.jsonl.idx'
            if index_bytes is not None:
                index_path.write_bytes(index_bytes)
                fresh = original.st_mtime_ns + 1_000_000_000
                os.utime(index_path, ns=(fresh, fresh))
        
            assert jsonlfile.lint_jsonl(str(path), force=force) is True
        
    >       assert index_path.read_bytes() == b'{}'
    E       assert b'{"ghost":0}' == b'{}'
    E         
    E         At index 1 diff: b'"' != b'}'
    E         Use -v to get more diff
    
    unit_tests/test_lint_integrity.py:49: AssertionError
    =========================== short test summary info ============================
    FAILED unit_tests/test_lint_integrity.py::test_empty_table_lint_repairs_index_without_rewriting_data[malformed-False]
    FAILED unit_tests/test_lint_integrity.py::test_empty_table_lint_repairs_index_without_rewriting_data[malformed-True]
    FAILED unit_tests/test_lint_integrity.py::test_empty_table_lint_repairs_index_without_rewriting_data[non-object-False]
    FAILED unit_tests/test_lint_integrity.py::test_empty_table_lint_repairs_index_without_rewriting_data[non-object-True]
    FAILED unit_tests/test_lint_integrity.py::test_empty_table_lint_repairs_index_without_rewriting_data[phantom-False]
    FAILED unit_tests/test_lint_integrity.py::test_empty_table_lint_repairs_index_without_rewriting_data[phantom-True]
    6 failed, 6 passed, 14 deselected in 0.19s
- **empty-table-lint: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_lint_integrity.py -k empty_table_lint` (300 ms, working-tree@b4186ee0537bfe5f60beaa1b0fbb4359380d1adc)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                              [100%]stdout: 
    12 passed, 14 deselected in 0.18s
- **lint-regressions: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_lint_integrity.py unit_tests/test_lint_compaction.py unit_tests/test_roadmap_compliance.py` (552 ms, working-tree@b4186ee0537bfe5f60beaa1b0fbb4359380d1adc)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                         [100%]stdout: 
    49 passed in 0.42s

## Current acceptance evidence

- **empty-table-lint: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_lint_integrity.py -k empty_table_lint` (300 ms, working-tree@b4186ee0537bfe5f60beaa1b0fbb4359380d1adc)
- **lint-regressions: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_lint_integrity.py unit_tests/test_lint_compaction.py unit_tests/test_roadmap_compliance.py` (552 ms, working-tree@b4186ee0537bfe5f60beaa1b0fbb4359380d1adc)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                         [100%]stdout: 
    49 passed in 0.42s

## Structured verification

    {
      "version": 1,
      "path_facts": {
        "requested": [],
        "committed": [
          ".specdev/adhoc/2026-09/AH-20260905T012727445Z-9912_repair-empty-table-indexes-during-default-and-fo.md",
          "jsonldb/jsonlfile.py",
          "unit_tests/test_lint_integrity.py"
        ],
        "rejected": [],
        "remaining": []
      },
      "attempt_history": [
        {
          "version": 1,
          "id": "V-001",
          "label": "empty-table-lint",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_lint_integrity.py -k empty_table_lint",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_lint_integrity.py",
            "-k",
            "empty_table_lint"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T01:27:51.531Z",
          "completed_at": "2026-09-05T01:27:51.843Z",
          "duration_ms": 312,
          "exit_status": 1,
          "status": "failed",
          "tested_revision": "working-tree@b4186ee0537bfe5f60beaa1b0fbb4359380d1adc",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: Fstdout: Fstdout: Fstdout: Fstdout: Fstdout: Fstdout: .stdout: .stdout:                                                              [100%]stdout: \n=================================== FAILURES ===================================\n_ test_empty_table_lint_repairs_index_without_rewriting_data[malformed-False] __\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-646/test_empty_table_lint_repairs_4')\nforce = False, index_bytes = b'{'\n\n    @pytest.mark.parametrize('force', [False, True])\n    @pytest.mark.parametrize('index_bytes', [\n        None, b'', b'{', b'[]', b'{\"ghost\":0}', b'{}',\n    ], ids=['missing', 'empty', 'malformed', 'non-object', 'phantom', 'canonical'])\n    def test_empty_table_lint_repairs_index_without_rewriting_data(\n            tmp_path, force, index_bytes):\n        path = tmp_path / 'empty.jsonl'\n        path.write_bytes(b'')\n        original = path.stat()\n        index_path = tmp_path / 'empty.jsonl.idx'\n        if index_bytes is not None:\n            index_path.write_bytes(index_bytes)\n            fresh = original.st_mtime_ns + 1_000_000_000\n            os.utime(index_path, ns=(fresh, fresh))\n    \n        assert jsonlfile.lint_jsonl(str(path), force=force) is True\n    \n>       assert index_path.read_bytes() == b'{}'\nE       AssertionError: assert b'{' == b'{}'\nE         \nE         Use -v to get more diff\n\nunit_tests/test_lint_integrity.py:49: AssertionError\n__ test_empty_table_lint_repairs_index_without_rewriting_data[malformed-True] __\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-646/test_empty_table_lint_repairs_5')\nforce = True, index_bytes = b'{'\n\n    @pytest.mark.parametrize('force', [False, True])\n    @pytest.mark.parametrize('index_bytes', [\n        None, b'', b'{', b'[]', b'{\"ghost\":0}', b'{}',\n    ], ids=['missing', 'empty', 'malformed', 'non-object', 'phantom', 'canonical'])\n    def test_empty_table_lint_repairs_index_without_rewriting_data(\n            tmp_path, force, index_bytes):\n        path = tmp_path / 'empty.jsonl'\n        path.write_bytes(b'')\n        original = path.stat()\n        index_path = tmp_path / 'empty.jsonl.idx'\n        if index_bytes is not None:\n            index_path.write_bytes(index_bytes)\n            fresh = original.st_mtime_ns + 1_000_000_000\n            os.utime(index_path, ns=(fresh, fresh))\n    \n        assert jsonlfile.lint_jsonl(str(path), force=force) is True\n    \n>       assert index_path.read_bytes() == b'{}'\nE       AssertionError: assert b'{' == b'{}'\nE         \nE         Use -v to get more diff\n\nunit_tests/test_lint_integrity.py:49: AssertionError\n_ test_empty_table_lint_repairs_index_without_rewriting_data[non-object-False] _\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-646/test_empty_table_lint_repairs_6')\nforce = False, index_bytes = b'[]'\n\n    @pytest.mark.parametrize('force', [False, True])\n    @pytest.mark.parametrize('index_bytes', [\n        None, b'', b'{', b'[]', b'{\"ghost\":0}', b'{}',\n    ], ids=['missing', 'empty', 'malformed', 'non-object', 'phantom', 'canonical'])\n    def test_empty_table_lint_repairs_index_without_rewriting_data(\n            tmp_path, force, index_bytes):\n        path = tmp_path / 'empty.jsonl'\n        path.write_bytes(b'')\n        original = path.stat()\n        index_path = tmp_path / 'empty.jsonl.idx'\n        if index_bytes is not None:\n            index_path.write_bytes(index_bytes)\n            fresh = original.st_mtime_ns + 1_000_000_000\n            os.utime(index_path, ns=(fresh, fresh))\n    \n        assert jsonlfile.lint_jsonl(str(path), force=force) is True\n    \n>       assert index_path.read_bytes() == b'{}'\nE       AssertionError: assert b'[]' == b'{}'\nE         \nE         At index 0 diff: b'[' != b'{'\nE         Use -v to get more diff\n\nunit_tests/test_lint_integrity.py:49: AssertionError\n_ test_empty_table_lint_repairs_index_without_rewriting_data[non-object-True] __\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-646/test_empty_table_lint_repairs_7')\nforce = True, index_bytes = b'[]'\n\n    @pytest.mark.parametrize('force', [False, True])\n    @pytest.mark.parametrize('index_bytes', [\n        None, b'', b'{', b'[]', b'{\"ghost\":0}', b'{}',\n    ], ids=['missing', 'empty', 'malformed', 'non-object', 'phantom', 'canonical'])\n    def test_empty_table_lint_repairs_index_without_rewriting_data(\n            tmp_path, force, index_bytes):\n        path = tmp_path / 'empty.jsonl'\n        path.write_bytes(b'')\n        original = path.stat()\n        index_path = tmp_path / 'empty.jsonl.idx'\n        if index_bytes is not None:\n            index_path.write_bytes(index_bytes)\n            fresh = original.st_mtime_ns + 1_000_000_000\n            os.utime(index_path, ns=(fresh, fresh))\n    \n        assert jsonlfile.lint_jsonl(str(path), force=force) is True\n    \n>       assert index_path.read_bytes() == b'{}'\nE       AssertionError: assert b'[]' == b'{}'\nE         \nE         At index 0 diff: b'[' != b'{'\nE         Use -v to get more diff\n\nunit_tests/test_lint_integrity.py:49: AssertionError\n__ test_empty_table_lint_repairs_index_without_rewriting_data[phantom-False] ___\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-646/test_empty_table_lint_repairs_8')\nforce = False, index_bytes = b'{\"ghost\":0}'\n\n    @pytest.mark.parametrize('force', [False, True])\n    @pytest.mark.parametrize('index_bytes', [\n        None, b'', b'{', b'[]', b'{\"ghost\":0}', b'{}',\n    ], ids=['missing', 'empty', 'malformed', 'non-object', 'phantom', 'canonical'])\n    def test_empty_table_lint_repairs_index_without_rewriting_data(\n            tmp_path, force, index_bytes):\n        path = tmp_path / 'empty.jsonl'\n        path.write_bytes(b'')\n        original = path.stat()\n        index_path = tmp_path / 'empty.jsonl.idx'\n        if index_bytes is not None:\n            index_path.write_bytes(index_bytes)\n            fresh = original.st_mtime_ns + 1_000_000_000\n            os.utime(index_path, ns=(fresh, fresh))\n    \n        assert jsonlfile.lint_jsonl(str(path), force=force) is True\n    \n>       assert index_path.read_bytes() == b'{}'\nE       assert b'{\"ghost\":0}' == b'{}'\nE         \nE         At index 1 diff: b'\"' != b'}'\nE         Use -v to get more diff\n\nunit_tests/test_lint_integrity.py:49: AssertionError\n___ test_empty_table_lint_repairs_index_without_rewriting_data[phantom-True] ___\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-646/test_empty_table_lint_repairs_9')\nforce = True, index_bytes = b'{\"ghost\":0}'\n\n    @pytest.mark.parametrize('force', [False, True])\n    @pytest.mark.parametrize('index_bytes', [\n        None, b'', b'{', b'[]', b'{\"ghost\":0}', b'{}',\n    ], ids=['missing', 'empty', 'malformed', 'non-object', 'phantom', 'canonical'])\n    def test_empty_table_lint_repairs_index_without_rewriting_data(\n            tmp_path, force, index_bytes):\n        path = tmp_path / 'empty.jsonl'\n        path.write_bytes(b'')\n        original = path.stat()\n        index_path = tmp_path / 'empty.jsonl.idx'\n        if index_bytes is not None:\n            index_path.write_bytes(index_bytes)\n            fresh = original.st_mtime_ns + 1_000_000_000\n            os.utime(index_path, ns=(fresh, fresh))\n    \n        assert jsonlfile.lint_jsonl(str(path), force=force) is True\n    \n>       assert index_path.read_bytes() == b'{}'\nE       assert b'{\"ghost\":0}' == b'{}'\nE         \nE         At index 1 diff: b'\"' != b'}'\nE         Use -v to get more diff\n\nunit_tests/test_lint_integrity.py:49: AssertionError\n=========================== short test summary info ============================\nFAILED unit_tests/test_lint_integrity.py::test_empty_table_lint_repairs_index_without_rewriting_data[malformed-False]\nFAILED unit_tests/test_lint_integrity.py::test_empty_table_lint_repairs_index_without_rewriting_data[malformed-True]\nFAILED unit_tests/test_lint_integrity.py::test_empty_table_lint_repairs_index_without_rewriting_data[non-object-False]\nFAILED unit_tests/test_lint_integrity.py::test_empty_table_lint_repairs_index_without_rewriting_data[non-object-True]\nFAILED unit_tests/test_lint_integrity.py::test_empty_table_lint_repairs_index_without_rewriting_data[phantom-False]\nFAILED unit_tests/test_lint_integrity.py::test_empty_table_lint_repairs_index_without_rewriting_data[phantom-True]\n6 failed, 6 passed, 14 deselected in 0.19s",
            "truncated": false,
            "captured_bytes": 8477
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "empty-table-lint",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_lint_integrity.py -k empty_table_lint",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_lint_integrity.py",
            "-k",
            "empty_table_lint"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T01:28:17.595Z",
          "completed_at": "2026-09-05T01:28:17.896Z",
          "duration_ms": 300,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@b4186ee0537bfe5f60beaa1b0fbb4359380d1adc",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                              [100%]stdout: \n12 passed, 14 deselected in 0.18s",
            "truncated": false,
            "captured_bytes": 114
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "lint-regressions",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_lint_integrity.py unit_tests/test_lint_compaction.py unit_tests/test_roadmap_compliance.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_lint_integrity.py",
            "unit_tests/test_lint_compaction.py",
            "unit_tests/test_roadmap_compliance.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T01:28:26.636Z",
          "completed_at": "2026-09-05T01:28:27.189Z",
          "duration_ms": 552,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@b4186ee0537bfe5f60beaa1b0fbb4359380d1adc",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                         [100%]stdout: \n49 passed in 0.42s",
            "truncated": false,
            "captured_bytes": 99
          }
        }
      ],
      "acceptance_evidence": [
        {
          "version": 1,
          "id": "V-002",
          "label": "empty-table-lint",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_lint_integrity.py -k empty_table_lint",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_lint_integrity.py",
            "-k",
            "empty_table_lint"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T01:28:17.595Z",
          "completed_at": "2026-09-05T01:28:17.896Z",
          "duration_ms": 300,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@b4186ee0537bfe5f60beaa1b0fbb4359380d1adc",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                              [100%]stdout: \n12 passed, 14 deselected in 0.18s",
            "truncated": false,
            "captured_bytes": 114
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "lint-regressions",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_lint_integrity.py unit_tests/test_lint_compaction.py unit_tests/test_roadmap_compliance.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_lint_integrity.py",
            "unit_tests/test_lint_compaction.py",
            "unit_tests/test_roadmap_compliance.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T01:28:26.636Z",
          "completed_at": "2026-09-05T01:28:27.189Z",
          "duration_ms": 552,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@b4186ee0537bfe5f60beaa1b0fbb4359380d1adc",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                         [100%]stdout: \n49 passed in 0.42s",
            "truncated": false,
            "captured_bytes": 99
          }
        }
      ]
    }
