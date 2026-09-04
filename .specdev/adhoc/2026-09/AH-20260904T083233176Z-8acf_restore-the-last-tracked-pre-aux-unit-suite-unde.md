# Adhoc AH-20260904T083233176Z-8acf

- Scope: Restore the last tracked pre-aux unit suite under tests/legacy with its hierarchy assertion aligned to current at-least-depth semantics, and restore the valid profile_test benchmark while keeping Assignment 00008's ignored aux test variants and stale profiler untouched.
- Title: Restore legacy tests and benchmark
- Started: 2026-09-04T08:32:33.176Z
- Completed: 2026-09-04T08:35:08.983Z
- Starting working tree: Clean.

## Outcome

Restored the last tracked pre-aux unit suite as three clean-clone-compatible modules under tests/legacy, aligned two hierarchy error assertions with current at-least-depth semantics, and restored the valid benchmark while leaving the stale profiler and Assignment 00008's ignored aux test variants untouched; 70 restored and 81 total tracked tests pass.

## Focused workflow coexistence

Assignment 00008 remained the focused owner at run assignment-lifecycle-20260729T07073993. Its workflow paths were excluded from this delivery. Post-detour contract revalidation is required before it advances.

## Delivery path facts

### Requested adopted paths

None.

### Committed paths

- `.gitignore`
- `.specdev/adhoc/2026-09/AH-20260904T083233176Z-8acf_restore-the-last-tracked-pre-aux-unit-suite-unde.md`
- `profile_test/benchmark.py`
- `tests/legacy/test_folderdb.py`
- `tests/legacy/test_jsonldf.py`
- `tests/legacy/test_jsonlfile.py`

### Rejected paths

None.

### Remaining owned paths

None.

## Verification summary

No manual verification summary was supplied.

## Verification attempt history

- **restored legacy tests: passed.** `/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/bin/python -m pytest -q tests/legacy` (699 ms, working-tree@4f5f895b301550f70ac4b400bd15e731ab7102bd)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:    [100%]stdout: 
    70 passed in 0.53s
- **complete tracked suite: passed.** `/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/bin/python -m pytest -q` (1329 ms, working-tree@4f5f895b301550f70ac4b400bd15e731ab7102bd)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    /venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85
    ../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85
    ../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85
      /private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85: PyparsingDeprecationWarning: 'parseString' deprecated - use 'parse_string'
        parse = parser.parseString(pattern)
    
    ../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89
    ../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89
    ../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89
    ../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89
    ../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89
    ../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89
      /private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89: PyparsingDeprecationWarning: 'resetCache' deprecated - use 'reset_cache'
        parser.resetCache()
    
    ../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_mathtext.py:45
      /private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_mathtext.py:45: PyparsingDeprecationWarning: 'enablePackrat' deprecated - use 'enable_packrat'
        ParserElement.enablePackrat()
    
    -- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
    81 passed, 14 warnings in 1.08s
- **benchmark and compilation: passed.** `/bin/sh -c /tmp/jsonldb-clean-clone-verify.lNjS5T/venv/bin/python -m py_compile tests/legacy/*.py profile_test/benchmark.py && /tmp/jsonldb-clean-clone-verify.lNjS5T/venv/bin/python -c "import runpy; n=runpy.run_path(\"profile_test/benchmark.py\"); assert len(n[\"generate_data\"](2)) == 2"` (185 ms, working-tree@4f5f895b301550f70ac4b400bd15e731ab7102bd)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    (no output)
- **ownership and whitespace: passed.** `/bin/sh -c test "$(git hash-object unit_tests/test_jsonlfile.py)" = 404df2b071af8a36703472aab58502f3d9e1fbc9 && test "$(git hash-object unit_tests/test_folderdb.py)" = 0247a050372591ac6cc0def600a818962ecb139d && git check-ignore -q profile_test/profile_jsonlfile.py && ! git check-ignore -q profile_test/benchmark.py && ! rg -n "get_aux_path|read_aux|write_aux|remove_aux|[ \\t]+$" tests/legacy profile_test/benchmark.py && git diff --check` (52 ms, working-tree@4f5f895b301550f70ac4b400bd15e731ab7102bd)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    (no output)

## Current acceptance evidence

- **restored legacy tests: passed.** `/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/bin/python -m pytest -q tests/legacy` (699 ms, working-tree@4f5f895b301550f70ac4b400bd15e731ab7102bd)
- **complete tracked suite: passed.** `/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/bin/python -m pytest -q` (1329 ms, working-tree@4f5f895b301550f70ac4b400bd15e731ab7102bd)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    /venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85
    ../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85
    ../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85
      /private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85: PyparsingDeprecationWarning: 'parseString' deprecated - use 'parse_string'
        parse = parser.parseString(pattern)
    
    ../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89
    ../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89
    ../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89
    ../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89
    ../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89
    ../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89
      /private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89: PyparsingDeprecationWarning: 'resetCache' deprecated - use 'reset_cache'
        parser.resetCache()
    
    ../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_mathtext.py:45
      /private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_mathtext.py:45: PyparsingDeprecationWarning: 'enablePackrat' deprecated - use 'enable_packrat'
        ParserElement.enablePackrat()
    
    -- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
    81 passed, 14 warnings in 1.08s
- **benchmark and compilation: passed.** `/bin/sh -c /tmp/jsonldb-clean-clone-verify.lNjS5T/venv/bin/python -m py_compile tests/legacy/*.py profile_test/benchmark.py && /tmp/jsonldb-clean-clone-verify.lNjS5T/venv/bin/python -c "import runpy; n=runpy.run_path(\"profile_test/benchmark.py\"); assert len(n[\"generate_data\"](2)) == 2"` (185 ms, working-tree@4f5f895b301550f70ac4b400bd15e731ab7102bd)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    (no output)
- **ownership and whitespace: passed.** `/bin/sh -c test "$(git hash-object unit_tests/test_jsonlfile.py)" = 404df2b071af8a36703472aab58502f3d9e1fbc9 && test "$(git hash-object unit_tests/test_folderdb.py)" = 0247a050372591ac6cc0def600a818962ecb139d && git check-ignore -q profile_test/profile_jsonlfile.py && ! git check-ignore -q profile_test/benchmark.py && ! rg -n "get_aux_path|read_aux|write_aux|remove_aux|[ \\t]+$" tests/legacy profile_test/benchmark.py && git diff --check` (52 ms, working-tree@4f5f895b301550f70ac4b400bd15e731ab7102bd)
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
          ".gitignore",
          ".specdev/adhoc/2026-09/AH-20260904T083233176Z-8acf_restore-the-last-tracked-pre-aux-unit-suite-unde.md",
          "profile_test/benchmark.py",
          "tests/legacy/test_folderdb.py",
          "tests/legacy/test_jsonldf.py",
          "tests/legacy/test_jsonlfile.py"
        ],
        "rejected": [],
        "remaining": []
      },
      "attempt_history": [
        {
          "version": 1,
          "id": "V-001",
          "label": "restored legacy tests",
          "annotation": null,
          "command": "/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/bin/python -m pytest -q tests/legacy",
          "argv": [
            "/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/bin/python",
            "-m",
            "pytest",
            "-q",
            "tests/legacy"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T08:34:52.838Z",
          "completed_at": "2026-09-04T08:34:53.537Z",
          "duration_ms": 699,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@4f5f895b301550f70ac4b400bd15e731ab7102bd",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:    [100%]stdout: \n70 passed in 0.53s",
            "truncated": false,
            "captured_bytes": 99
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "complete tracked suite",
          "annotation": null,
          "command": "/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/bin/python -m pytest -q",
          "argv": [
            "/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/bin/python",
            "-m",
            "pytest",
            "-q"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T08:34:53.682Z",
          "completed_at": "2026-09-04T08:34:55.012Z",
          "duration_ms": 1329,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@4f5f895b301550f70ac4b400bd15e731ab7102bd",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 88%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                                 [100%]stdout: \n=============================== warnings summary ===============================\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:64\n  /private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:64: PyparsingDeprecationWarning: 'oneOf' deprecated - use 'one_of'\n    prop = Group((name + Suppress(\"=\") + comma_separated(value)) | oneOf(_CONSTANTS))\n\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85\n  /private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85: PyparsingDeprecationWarning: 'parseString' deprecated - use 'parse_string'\n    parse = parser.parseString(pattern)\n\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89\n  /private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89: PyparsingDeprecationWarning: 'resetCache' deprecated - use 'reset_cache'\n    parser.resetCache()\n\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_mathtext.py:45\n  /private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_mathtext.py:45: PyparsingDeprecationWarning: 'enablePackrat' deprecated - use 'enable_packrat'\n    ParserElement.enablePackrat()\n\n-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html\n81 passed, 14 warnings in 1.08s",
            "truncated": false,
            "captured_bytes": 3159
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "benchmark and compilation",
          "annotation": null,
          "command": "/bin/sh -c /tmp/jsonldb-clean-clone-verify.lNjS5T/venv/bin/python -m py_compile tests/legacy/*.py profile_test/benchmark.py && /tmp/jsonldb-clean-clone-verify.lNjS5T/venv/bin/python -c \"import runpy; n=runpy.run_path(\\\"profile_test/benchmark.py\\\"); assert len(n[\\\"generate_data\\\"](2)) == 2\"",
          "argv": [
            "/bin/sh",
            "-c",
            "/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/bin/python -m py_compile tests/legacy/*.py profile_test/benchmark.py && /tmp/jsonldb-clean-clone-verify.lNjS5T/venv/bin/python -c \"import runpy; n=runpy.run_path(\\\"profile_test/benchmark.py\\\"); assert len(n[\\\"generate_data\\\"](2)) == 2\""
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T08:34:55.157Z",
          "completed_at": "2026-09-04T08:34:55.343Z",
          "duration_ms": 185,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@4f5f895b301550f70ac4b400bd15e731ab7102bd",
          "output": {
            "text": "",
            "truncated": false,
            "captured_bytes": 0
          }
        },
        {
          "version": 1,
          "id": "V-004",
          "label": "ownership and whitespace",
          "annotation": null,
          "command": "/bin/sh -c test \"$(git hash-object unit_tests/test_jsonlfile.py)\" = 404df2b071af8a36703472aab58502f3d9e1fbc9 && test \"$(git hash-object unit_tests/test_folderdb.py)\" = 0247a050372591ac6cc0def600a818962ecb139d && git check-ignore -q profile_test/profile_jsonlfile.py && ! git check-ignore -q profile_test/benchmark.py && ! rg -n \"get_aux_path|read_aux|write_aux|remove_aux|[ \\\\t]+$\" tests/legacy profile_test/benchmark.py && git diff --check",
          "argv": [
            "/bin/sh",
            "-c",
            "test \"$(git hash-object unit_tests/test_jsonlfile.py)\" = 404df2b071af8a36703472aab58502f3d9e1fbc9 && test \"$(git hash-object unit_tests/test_folderdb.py)\" = 0247a050372591ac6cc0def600a818962ecb139d && git check-ignore -q profile_test/profile_jsonlfile.py && ! git check-ignore -q profile_test/benchmark.py && ! rg -n \"get_aux_path|read_aux|write_aux|remove_aux|[ \\\\t]+$\" tests/legacy profile_test/benchmark.py && git diff --check"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T08:34:55.483Z",
          "completed_at": "2026-09-04T08:34:55.535Z",
          "duration_ms": 52,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@4f5f895b301550f70ac4b400bd15e731ab7102bd",
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
          "id": "V-001",
          "label": "restored legacy tests",
          "annotation": null,
          "command": "/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/bin/python -m pytest -q tests/legacy",
          "argv": [
            "/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/bin/python",
            "-m",
            "pytest",
            "-q",
            "tests/legacy"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T08:34:52.838Z",
          "completed_at": "2026-09-04T08:34:53.537Z",
          "duration_ms": 699,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@4f5f895b301550f70ac4b400bd15e731ab7102bd",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:    [100%]stdout: \n70 passed in 0.53s",
            "truncated": false,
            "captured_bytes": 99
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "complete tracked suite",
          "annotation": null,
          "command": "/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/bin/python -m pytest -q",
          "argv": [
            "/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/bin/python",
            "-m",
            "pytest",
            "-q"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T08:34:53.682Z",
          "completed_at": "2026-09-04T08:34:55.012Z",
          "duration_ms": 1329,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@4f5f895b301550f70ac4b400bd15e731ab7102bd",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 88%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                                 [100%]stdout: \n=============================== warnings summary ===============================\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:64\n  /private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:64: PyparsingDeprecationWarning: 'oneOf' deprecated - use 'one_of'\n    prop = Group((name + Suppress(\"=\") + comma_separated(value)) | oneOf(_CONSTANTS))\n\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85\n  /private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85: PyparsingDeprecationWarning: 'parseString' deprecated - use 'parse_string'\n    parse = parser.parseString(pattern)\n\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89\n  /private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89: PyparsingDeprecationWarning: 'resetCache' deprecated - use 'reset_cache'\n    parser.resetCache()\n\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_mathtext.py:45\n  /private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_mathtext.py:45: PyparsingDeprecationWarning: 'enablePackrat' deprecated - use 'enable_packrat'\n    ParserElement.enablePackrat()\n\n-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html\n81 passed, 14 warnings in 1.08s",
            "truncated": false,
            "captured_bytes": 3159
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "benchmark and compilation",
          "annotation": null,
          "command": "/bin/sh -c /tmp/jsonldb-clean-clone-verify.lNjS5T/venv/bin/python -m py_compile tests/legacy/*.py profile_test/benchmark.py && /tmp/jsonldb-clean-clone-verify.lNjS5T/venv/bin/python -c \"import runpy; n=runpy.run_path(\\\"profile_test/benchmark.py\\\"); assert len(n[\\\"generate_data\\\"](2)) == 2\"",
          "argv": [
            "/bin/sh",
            "-c",
            "/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/bin/python -m py_compile tests/legacy/*.py profile_test/benchmark.py && /tmp/jsonldb-clean-clone-verify.lNjS5T/venv/bin/python -c \"import runpy; n=runpy.run_path(\\\"profile_test/benchmark.py\\\"); assert len(n[\\\"generate_data\\\"](2)) == 2\""
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T08:34:55.157Z",
          "completed_at": "2026-09-04T08:34:55.343Z",
          "duration_ms": 185,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@4f5f895b301550f70ac4b400bd15e731ab7102bd",
          "output": {
            "text": "",
            "truncated": false,
            "captured_bytes": 0
          }
        },
        {
          "version": 1,
          "id": "V-004",
          "label": "ownership and whitespace",
          "annotation": null,
          "command": "/bin/sh -c test \"$(git hash-object unit_tests/test_jsonlfile.py)\" = 404df2b071af8a36703472aab58502f3d9e1fbc9 && test \"$(git hash-object unit_tests/test_folderdb.py)\" = 0247a050372591ac6cc0def600a818962ecb139d && git check-ignore -q profile_test/profile_jsonlfile.py && ! git check-ignore -q profile_test/benchmark.py && ! rg -n \"get_aux_path|read_aux|write_aux|remove_aux|[ \\\\t]+$\" tests/legacy profile_test/benchmark.py && git diff --check",
          "argv": [
            "/bin/sh",
            "-c",
            "test \"$(git hash-object unit_tests/test_jsonlfile.py)\" = 404df2b071af8a36703472aab58502f3d9e1fbc9 && test \"$(git hash-object unit_tests/test_folderdb.py)\" = 0247a050372591ac6cc0def600a818962ecb139d && git check-ignore -q profile_test/profile_jsonlfile.py && ! git check-ignore -q profile_test/benchmark.py && ! rg -n \"get_aux_path|read_aux|write_aux|remove_aux|[ \\\\t]+$\" tests/legacy profile_test/benchmark.py && git diff --check"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T08:34:55.483Z",
          "completed_at": "2026-09-04T08:34:55.535Z",
          "duration_ms": 52,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@4f5f895b301550f70ac4b400bd15e731ab7102bd",
          "output": {
            "text": "",
            "truncated": false,
            "captured_bytes": 0
          }
        }
      ]
    }
