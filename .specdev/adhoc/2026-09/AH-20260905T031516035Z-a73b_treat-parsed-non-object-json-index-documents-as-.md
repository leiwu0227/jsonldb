# Adhoc AH-20260905T031516035Z-a73b

- Scope: Treat parsed non-object JSON index documents as corrupt and rebuild them through the ordinary index loader before reads and writes
- Title: Rebuild non-object index documents
- Started: 2026-09-05T03:15:16.035Z
- Completed: 2026-09-05T03:17:22.626Z
- Starting working tree: Clean.

## Outcome

The ordinary index loader now returns only dictionary documents. Parsed lists, strings, numbers, booleans, and null fall through to the existing corrupt-index warning and rebuild path. Valid objects, including an empty object, retain existing behavior; deeper offset and row-coverage validation remains with lint. Added regressions for all non-object JSON types, preservation of slotted data during rebuilding, single and range reads, upserts, deletes, and accepting a valid empty index without rebuilding. The new cases reproduced 11 failures before the fix and pass all 12 afterwards. Focused storage, lint, roadmap compliance, and report suites pass 96 tests; git diff --check passes and jsonlfile.py remains within its 950-line cap.

## Focused workflow coexistence

No focused Assignment or Mission coexistence was recorded.

## Delivery path facts

### Requested adopted paths

None.

### Committed paths

- `.specdev/adhoc/2026-09/AH-20260905T031516035Z-a73b_treat-parsed-non-object-json-index-documents-as-.md`
- `jsonldb/jsonlfile.py`
- `unit_tests/test_jsonlfile.py`

### Rejected paths

None.

### Remaining owned paths

None.

## Verification summary

No manual verification summary was supplied.

## Verification attempt history

- **non-object-index: failed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_jsonlfile.py -k non_object or non_objects or valid_empty_object` (445 ms, working-tree@4d4c62b1f6c7928cea5e4e97587d756ae27c665d)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 1
  - Output (bounded tail):

    ject_indexes(tmp_path, operation):
            path = tmp_path / 'rows.jsonl'
            expected = {'a': {'value': 1}, 'z': {'value': 2}}
            save_jsonl(str(path), expected)
            index_path = _write_fresh_index(path, b'[]')
        
            if operation == 'single':
                assert select_line_jsonl(str(path), 'a') == {'a': expected['a']}
            elif operation == 'range':
                assert select_jsonl(str(path), 'a', 'z') == expected
            elif operation == 'upsert':
                update_jsonl(str(path), {'b': {'value': 3}})
                expected['b'] = {'value': 3}
            else:
                delete_jsonl(str(path), ['a'])
                del expected['a']
        
    >       assert load_jsonl(str(path)) == expected
    E       AssertionError: assert {'a': {'value... {'value': 2}} == {'z': {'value': 2}}
    E         
    E         Omitting 1 identical items, use -vv to show
    E         Left contains 1 more item:
    E         {'a': {'value': 1}}
    E         Use -v to get more diff
    
    unit_tests/test_jsonlfile.py:62: AssertionError
    =========================== short test summary info ============================
    FAILED unit_tests/test_jsonlfile.py::test_load_index_rebuilds_non_objects[[]]
    FAILED unit_tests/test_jsonlfile.py::test_load_index_rebuilds_non_objects["key"]
    FAILED unit_tests/test_jsonlfile.py::test_load_index_rebuilds_non_objects[42]
    FAILED unit_tests/test_jsonlfile.py::test_load_index_rebuilds_non_objects[1.5]
    FAILED unit_tests/test_jsonlfile.py::test_load_index_rebuilds_non_objects[true]
    FAILED unit_tests/test_jsonlfile.py::test_load_index_rebuilds_non_objects[false]
    FAILED unit_tests/test_jsonlfile.py::test_load_index_rebuilds_non_objects[null]
    FAILED unit_tests/test_jsonlfile.py::test_operations_recover_non_object_indexes[single]
    FAILED unit_tests/test_jsonlfile.py::test_operations_recover_non_object_indexes[range]
    FAILED unit_tests/test_jsonlfile.py::test_operations_recover_non_object_indexes[upsert]
    FAILED unit_tests/test_jsonlfile.py::test_operations_recover_non_object_indexes[delete]
    11 failed, 1 passed, 34 deselected in 0.31s
- **non-object-index: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_jsonlfile.py -k non_object or non_objects or valid_empty_object` (386 ms, working-tree@4d4c62b1f6c7928cea5e4e97587d756ae27c665d)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                              [100%]stdout: 
    12 passed, 34 deselected in 0.27s
- **index-regressions: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_jsonlfile.py unit_tests/test_lint_integrity.py unit_tests/test_roadmap_compliance.py unit_tests/test_reports.py` (793 ms, working-tree@4d4c62b1f6c7928cea5e4e97587d756ae27c665d)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 75%]
    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                  [100%]stdout: 
    96 passed in 0.66s

## Current acceptance evidence

- **non-object-index: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_jsonlfile.py -k non_object or non_objects or valid_empty_object` (386 ms, working-tree@4d4c62b1f6c7928cea5e4e97587d756ae27c665d)
- **index-regressions: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_jsonlfile.py unit_tests/test_lint_integrity.py unit_tests/test_roadmap_compliance.py unit_tests/test_reports.py` (793 ms, working-tree@4d4c62b1f6c7928cea5e4e97587d756ae27c665d)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 75%]
    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                  [100%]stdout: 
    96 passed in 0.66s

## Structured verification

    {
      "version": 1,
      "path_facts": {
        "requested": [],
        "committed": [
          ".specdev/adhoc/2026-09/AH-20260905T031516035Z-a73b_treat-parsed-non-object-json-index-documents-as-.md",
          "jsonldb/jsonlfile.py",
          "unit_tests/test_jsonlfile.py"
        ],
        "rejected": [],
        "remaining": []
      },
      "attempt_history": [
        {
          "version": 1,
          "id": "V-001",
          "label": "non-object-index",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_jsonlfile.py -k non_object or non_objects or valid_empty_object",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_jsonlfile.py",
            "-k",
            "non_object or non_objects or valid_empty_object"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T03:16:41.975Z",
          "completed_at": "2026-09-05T03:16:42.421Z",
          "duration_ms": 445,
          "exit_status": 1,
          "status": "failed",
          "tested_revision": "working-tree@4d4c62b1f6c7928cea5e4e97587d756ae27c665d",
          "output": {
            "text": "f-leiwu/pytest-656/test_load_index_rebuilds_non_o1/rows.jsonl'))\n\nunit_tests/test_jsonlfile.py:36: AssertionError\n___________________ test_load_index_rebuilds_non_objects[42] ___________________\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-656/test_load_index_rebuilds_non_o2')\ncaplog = <_pytest.logging.LogCaptureFixture object at 0x109386d50>\ncontent = b'42'\n\n    @pytest.mark.parametrize('content', [\n        b'[]', b'\"key\"', b'42', b'1.5', b'true', b'false', b'null',\n    ])\n    def test_load_index_rebuilds_non_objects(tmp_path, caplog, content):\n        path = tmp_path / 'rows.jsonl'\n        save_jsonl(str(path), {'z': {'value': 2}, 'a': {'value': 1}},\n                   slot_bytes=128, meta={'owner': 'kept'})\n        expected = load_index(str(path))\n        before = path.read_bytes()\n        index_path = _write_fresh_index(path, content)\n        caplog.set_level('WARNING', logger='jsonldb.jsonlfile')\n    \n>       assert load_index(str(path)) == expected\nE       AssertionError: assert 42 == {'a': 146, 'z': 128}\nE        +  where 42 = load_index('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-656/test_load_index_rebuilds_non_o2/rows.jsonl')\nE        +    where '/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-656/test_load_index_rebuilds_non_o2/rows.jsonl' = str(PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-656/test_load_index_rebuilds_non_o2/rows.jsonl'))\n\nunit_tests/test_jsonlfile.py:36: AssertionError\n__________________ test_load_index_rebuilds_non_objects[1.5] ___________________\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-656/test_load_index_rebuilds_non_o3')\ncaplog = <_pytest.logging.LogCaptureFixture object at 0x1093c37d0>\ncontent = b'1.5'\n\n    @pytest.mark.parametrize('content', [\n        b'[]', b'\"key\"', b'42', b'1.5', b'true', b'false', b'null',\n    ])\n    def test_load_index_rebuilds_non_objects(tmp_path, caplog, content):\n        path = tmp_path / 'rows.jsonl'\n        save_jsonl(str(path), {'z': {'value': 2}, 'a': {'value': 1}},\n                   slot_bytes=128, meta={'owner': 'kept'})\n        expected = load_index(str(path))\n        before = path.read_bytes()\n        index_path = _write_fresh_index(path, content)\n        caplog.set_level('WARNING', logger='jsonldb.jsonlfile')\n    \n>       assert load_index(str(path)) == expected\nE       AssertionError: assert 1.5 == {'a': 146, 'z': 128}\nE        +  where 1.5 = load_index('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-656/test_load_index_rebuilds_non_o3/rows.jsonl')\nE        +    where '/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-656/test_load_index_rebuilds_non_o3/rows.jsonl' = str(PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-656/test_load_index_rebuilds_non_o3/rows.jsonl'))\n\nunit_tests/test_jsonlfile.py:36: AssertionError\n__________________ test_load_index_rebuilds_non_objects[true] __________________\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-656/test_load_index_rebuilds_non_o4')\ncaplog = <_pytest.logging.LogCaptureFixture object at 0x109386e10>\ncontent = b'true'\n\n    @pytest.mark.parametrize('content', [\n        b'[]', b'\"key\"', b'42', b'1.5', b'true', b'false', b'null',\n    ])\n    def test_load_index_rebuilds_non_objects(tmp_path, caplog, content):\n        path = tmp_path / 'rows.jsonl'\n        save_jsonl(str(path), {'z': {'value': 2}, 'a': {'value': 1}},\n                   slot_bytes=128, meta={'owner': 'kept'})\n        expected = load_index(str(path))\n        before = path.read_bytes()\n        index_path = _write_fresh_index(path, content)\n        caplog.set_level('WARNING', logger='jsonldb.jsonlfile')\n    \n>       assert load_index(str(path)) == expected\nE       AssertionError: assert True == {'a': 146, 'z': 128}\nE        +  where True = load_index('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-656/test_load_index_rebuilds_non_o4/rows.jsonl')\nE        +    where '/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-656/test_load_index_rebuilds_non_o4/rows.jsonl' = str(PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-656/test_load_index_rebuilds_non_o4/rows.jsonl'))\n\nunit_tests/test_jsonlfile.py:36: AssertionError\n_________________ test_load_index_rebuilds_non_objects[false] __________________\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-656/test_load_index_rebuilds_non_o5')\ncaplog = <_pytest.logging.LogCaptureFixture object at 0x109385280>\ncontent = b'false'\n\n    @pytest.mark.parametrize('content', [\n        b'[]', b'\"key\"', b'42', b'1.5', b'true', b'false', b'null',\n    ])\n    def test_load_index_rebuilds_non_objects(tmp_path, caplog, content):\n        path = tmp_path / 'rows.jsonl'\n        save_jsonl(str(path), {'z': {'value': 2}, 'a': {'value': 1}},\n                   slot_bytes=128, meta={'owner': 'kept'})\n        expected = load_index(str(path))\n        before = path.read_bytes()\n        index_path = _write_fresh_index(path, content)\n        caplog.set_level('WARNING', logger='jsonldb.jsonlfile')\n    \n>       assert load_index(str(path)) == expected\nE       AssertionError: assert False == {'a': 146, 'z': 128}\nE        +  where False = load_index('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-656/test_load_index_rebuilds_non_o5/rows.jsonl')\nE        +    where '/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-656/test_load_index_rebuilds_non_o5/rows.jsonl' = str(PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-656/test_load_index_rebuilds_non_o5/rows.jsonl'))\n\nunit_tests/test_jsonlfile.py:36: AssertionError\n__________________ test_load_index_rebuilds_non_objects[null] __________________\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-656/test_load_index_rebuilds_non_o6')\ncaplog = <_pytest.logging.LogCaptureFixture object at 0x1093c3e00>\ncontent = b'null'\n\n    @pytest.mark.parametrize('content', [\n        b'[]', b'\"key\"', b'42', b'1.5', b'true', b'false', b'null',\n    ])\n    def test_load_index_rebuilds_non_objects(tmp_path, caplog, content):\n        path = tmp_path / 'rows.jsonl'\n        save_jsonl(str(path), {'z': {'value': 2}, 'a': {'value': 1}},\n                   slot_bytes=128, meta={'owner': 'kept'})\n        expected = load_index(str(path))\n        before = path.read_bytes()\n        index_path = _write_fresh_index(path, content)\n        caplog.set_level('WARNING', logger='jsonldb.jsonlfile')\n    \n>       assert load_index(str(path)) == expected\nE       AssertionError: assert None == {'a': 146, 'z': 128}\nE        +  where None = load_index('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-656/test_load_index_rebuilds_non_o6/rows.jsonl')\nE        +    where '/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-656/test_load_index_rebuilds_non_o6/rows.jsonl' = str(PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-656/test_load_index_rebuilds_non_o6/rows.jsonl'))\n\nunit_tests/test_jsonlfile.py:36: AssertionError\n______________ test_operations_recover_non_object_indexes[single] ______________\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-656/test_operations_recover_non_ob0')\noperation = 'single'\n\n    @pytest.mark.parametrize('operation', ['single', 'range', 'upsert', 'delete'])\n    def test_operations_recover_non_object_indexes(tmp_path, operation):\n        path = tmp_path / 'rows.jsonl'\n        expected = {'a': {'value': 1}, 'z': {'value': 2}}\n        save_jsonl(str(path), expected)\n        index_path = _write_fresh_index(path, b'[]')\n    \n        if operation == 'single':\n>           assert select_line_jsonl(str(path), 'a') == {'a': expected['a']}\nE           AssertionError: assert {} == {'a': {'value': 1}}\nE             \nE             Right contains 1 more item:\nE             {'a': {'value': 1}}\nE             Use -v to get more diff\n\nunit_tests/test_jsonlfile.py:52: AssertionError\n______________ test_operations_recover_non_object_indexes[range] _______________\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-656/test_operations_recover_non_ob1')\noperation = 'range'\n\n    @pytest.mark.parametrize('operation', ['single', 'range', 'upsert', 'delete'])\n    def test_operations_recover_non_object_indexes(tmp_path, operation):\n        path = tmp_path / 'rows.jsonl'\n        expected = {'a': {'value': 1}, 'z': {'value': 2}}\n        save_jsonl(str(path), expected)\n        index_path = _write_fresh_index(path, b'[]')\n    \n        if operation == 'single':\n            assert select_line_jsonl(str(path), 'a') == {'a': expected['a']}\n        elif operation == 'range':\n>           assert select_jsonl(str(path), 'a', 'z') == expected\nE           AssertionError: assert {} == {'a': {'value... {'value': 2}}\nE             \nE             Right contains 2 more items:\nE             {'a': {'value': 1}, 'z': {'value': 2}}\nE             Use -v to get more diff\n\nunit_tests/test_jsonlfile.py:54: AssertionError\n______________ test_operations_recover_non_object_indexes[upsert] ______________\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-656/test_operations_recover_non_ob2')\noperation = 'upsert'\n\n    @pytest.mark.parametrize('operation', ['single', 'range', 'upsert', 'delete'])\n    def test_operations_recover_non_object_indexes(tmp_path, operation):\n        path = tmp_path / 'rows.jsonl'\n        expected = {'a': {'value': 1}, 'z': {'value': 2}}\n        save_jsonl(str(path), expected)\n        index_path = _write_fresh_index(path, b'[]')\n    \n        if operation == 'single':\n            assert select_line_jsonl(str(path), 'a') == {'a': expected['a']}\n        elif operation == 'range':\n            assert select_jsonl(str(path), 'a', 'z') == expected\n        elif operation == 'upsert':\n>           update_jsonl(str(path), {'b': {'value': 3}})\n\nunit_tests/test_jsonlfile.py:56: \n_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ \n\njsonl_file_path = '/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-656/test_operations_recover_non_ob2/rows.jsonl'\nupdate_dict = {'b': {'value': 3}}, timespec = None, meta = None\n\n    def update_jsonl(jsonl_file_path: str, update_dict: DataDict,\n                     timespec: Optional[str] = None,\n                     meta: Optional[dict] = None) -> None:\n        \"\"\"\n        Update or insert records in a JSONL file.\n    \n        Efficiently handles both updates and inserts:\n        - Updates in place if new record fits in old space\n        - Appends to file if record grows\n        - Maintains index automatically\n    \n        Args:\n            jsonl_file_path: Path to the JSONL file\n            update_dict: Dictionary of records to update/insert\n    \n        Raises:\n            OSError: If file operations fail\n        \"\"\"\n        _validate_row_keys(update_dict, timespec)\n        encoded_meta = None\n        if meta is not None:\n            info = metaslot.inspect_file(jsonl_file_path)\n            if not info.is_slot:\n                raise ValueError(\"metadata slot is not enabled for this file\")\n            encoded_meta = metaslot.encode_slot(meta, info.width)\n    \n        try:\n            # Load index (self-heals an empty/corrupt .idx)\n            index = load_index(jsonl_file_path)\n    \n            updates = []\n            appends = []\n            old_lines = []\n    \n            # Process records\n            with open(jsonl_file_path, 'rb+', buffering=BUFFER_SIZE) as f:\n                f.seek(0, os.SEEK_END)\n                # Heal a missing trailing newline so appends start on a fresh line\n                if f.tell() > 0:\n                    f.seek(-1, os.SEEK_END)\n                    if f.read(1) != b'\\n':\n                        f.write(b'\\n')\n                append_pos = f.tell()\n    \n                for linekey, data in update_dict.items():\n                    linekey = serialize_linekey(linekey, timespec)\n                    new_line = _fast_dumps({linekey: data}).encode('utf-8')\n    \n                    if linekey in index:\n                        f.seek(index[linekey])\n                        old_line = f.readline()\n    \n                        if len(new_line) <= len(old_line):\n                            updates.append((index[linekey], new_line, len(old_line)))\n                        else:\n                            appends.append((linekey, new_line))\n                            old_lines.append((index[linekey], len(old_line)))\n                    else:\n                        appends.append((linekey, new_line))\n    \n                # Apply updates\n                for pos, line, old_len in updates:\n                    if len(line) < old_len:\n                        # Pad before the newline so the record keeps its exact old\n                        # length and the line stays newline-terminated\n                        line = line[:-1] + b' ' * (old_len - len(line)) + b'\\n'\n                    f.seek(pos)\n                    f.write(line)\n    \n                # Apply appends\n                if appends:\n                    f.seek(append_pos)\n                    for linekey, line in appends:\n>                       index[linekey] = f.tell()\n                        ^^^^^^^^^^^^^^\nE                       TypeError: list indices must be integers or slices, not str\n\njsonldb/jsonlfile.py:875: TypeError\n______________ test_operations_recover_non_object_indexes[delete] ______________\n\ntmp_path = PosixPath('/private/var/folders/fk/lczrk_m93674t4hm1g1q60jw0000gn/T/pytest-of-leiwu/pytest-656/test_operations_recover_non_ob3')\noperation = 'delete'\n\n    @pytest.mark.parametrize('operation', ['single', 'range', 'upsert', 'delete'])\n    def test_operations_recover_non_object_indexes(tmp_path, operation):\n        path = tmp_path / 'rows.jsonl'\n        expected = {'a': {'value': 1}, 'z': {'value': 2}}\n        save_jsonl(str(path), expected)\n        index_path = _write_fresh_index(path, b'[]')\n    \n        if operation == 'single':\n            assert select_line_jsonl(str(path), 'a') == {'a': expected['a']}\n        elif operation == 'range':\n            assert select_jsonl(str(path), 'a', 'z') == expected\n        elif operation == 'upsert':\n            update_jsonl(str(path), {'b': {'value': 3}})\n            expected['b'] = {'value': 3}\n        else:\n            delete_jsonl(str(path), ['a'])\n            del expected['a']\n    \n>       assert load_jsonl(str(path)) == expected\nE       AssertionError: assert {'a': {'value... {'value': 2}} == {'z': {'value': 2}}\nE         \nE         Omitting 1 identical items, use -vv to show\nE         Left contains 1 more item:\nE         {'a': {'value': 1}}\nE         Use -v to get more diff\n\nunit_tests/test_jsonlfile.py:62: AssertionError\n=========================== short test summary info ============================\nFAILED unit_tests/test_jsonlfile.py::test_load_index_rebuilds_non_objects[[]]\nFAILED unit_tests/test_jsonlfile.py::test_load_index_rebuilds_non_objects[\"key\"]\nFAILED unit_tests/test_jsonlfile.py::test_load_index_rebuilds_non_objects[42]\nFAILED unit_tests/test_jsonlfile.py::test_load_index_rebuilds_non_objects[1.5]\nFAILED unit_tests/test_jsonlfile.py::test_load_index_rebuilds_non_objects[true]\nFAILED unit_tests/test_jsonlfile.py::test_load_index_rebuilds_non_objects[false]\nFAILED unit_tests/test_jsonlfile.py::test_load_index_rebuilds_non_objects[null]\nFAILED unit_tests/test_jsonlfile.py::test_operations_recover_non_object_indexes[single]\nFAILED unit_tests/test_jsonlfile.py::test_operations_recover_non_object_indexes[range]\nFAILED unit_tests/test_jsonlfile.py::test_operations_recover_non_object_indexes[upsert]\nFAILED unit_tests/test_jsonlfile.py::test_operations_recover_non_object_indexes[delete]\n11 failed, 1 passed, 34 deselected in 0.31s",
            "truncated": true,
            "captured_bytes": 18985
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "non-object-index",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_jsonlfile.py -k non_object or non_objects or valid_empty_object",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_jsonlfile.py",
            "-k",
            "non_object or non_objects or valid_empty_object"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T03:16:59.680Z",
          "completed_at": "2026-09-05T03:17:00.067Z",
          "duration_ms": 386,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@4d4c62b1f6c7928cea5e4e97587d756ae27c665d",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                              [100%]stdout: \n12 passed, 34 deselected in 0.27s",
            "truncated": false,
            "captured_bytes": 114
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "index-regressions",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_jsonlfile.py unit_tests/test_lint_integrity.py unit_tests/test_roadmap_compliance.py unit_tests/test_reports.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_jsonlfile.py",
            "unit_tests/test_lint_integrity.py",
            "unit_tests/test_roadmap_compliance.py",
            "unit_tests/test_reports.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T03:17:09.407Z",
          "completed_at": "2026-09-05T03:17:10.200Z",
          "duration_ms": 793,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@4d4c62b1f6c7928cea5e4e97587d756ae27c665d",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 75%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                  [100%]stdout: \n96 passed in 0.66s",
            "truncated": false,
            "captured_bytes": 179
          }
        }
      ],
      "acceptance_evidence": [
        {
          "version": 1,
          "id": "V-002",
          "label": "non-object-index",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_jsonlfile.py -k non_object or non_objects or valid_empty_object",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_jsonlfile.py",
            "-k",
            "non_object or non_objects or valid_empty_object"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T03:16:59.680Z",
          "completed_at": "2026-09-05T03:17:00.067Z",
          "duration_ms": 386,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@4d4c62b1f6c7928cea5e4e97587d756ae27c665d",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                              [100%]stdout: \n12 passed, 34 deselected in 0.27s",
            "truncated": false,
            "captured_bytes": 114
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "index-regressions",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_jsonlfile.py unit_tests/test_lint_integrity.py unit_tests/test_roadmap_compliance.py unit_tests/test_reports.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_jsonlfile.py",
            "unit_tests/test_lint_integrity.py",
            "unit_tests/test_roadmap_compliance.py",
            "unit_tests/test_reports.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T03:17:09.407Z",
          "completed_at": "2026-09-05T03:17:10.200Z",
          "duration_ms": 793,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@4d4c62b1f6c7928cea5e4e97587d756ae27c665d",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 75%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                  [100%]stdout: \n96 passed in 0.66s",
            "truncated": false,
            "captured_bytes": 179
          }
        }
      ]
    }
