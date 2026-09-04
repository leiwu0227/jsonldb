# Adhoc AH-20260904T081835643Z-0715

- Scope: Fix visualization edge cases by allowing an empty single-table Bokeh index, labeling plotted index values as byte offsets, and sharing inclusive-start/exclusive-end filtering between the Matplotlib table and database views, with focused tests.
- Title: Fix visualization edge cases
- Started: 2026-09-04T08:18:35.643Z
- Completed: 2026-09-04T08:20:40.738Z
- Starting working tree: Clean.

## Outcome

Single-table Bokeh visualization now handles empty indexes, single-table plots consistently label index values as byte offsets, and both Matplotlib views share inclusive-start/exclusive-end filtering; real-backend regressions cover the empty and range-window cases.

## Focused workflow coexistence

Assignment 00008 remained the focused owner at run assignment-lifecycle-20260729T07073993. Its workflow paths were excluded from this delivery. Post-detour contract revalidation is required before it advances.

## Delivery path facts

### Requested adopted paths

None.

### Committed paths

- `.specdev/adhoc/2026-09/AH-20260904T081835643Z-0715_fix-visualization-edge-cases-by-allowing-an-empt.md`
- `jsonldb/visual.py`
- `tests/test_visualization.py`

### Rejected paths

None.

### Remaining owned paths

None.

## Verification summary

No manual verification summary was supplied.

## Verification attempt history

- **visualization and tracked regressions: passed.** `/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m pytest -q tests` (810 ms, working-tree@ad6ea45aee341a1cba67b595680e6d58ce46e0e1)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    b/_fontconfig_pattern.py:85
    ../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85
    ../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85
    ../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85
      /private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85: PyparsingDeprecationWarning: 'parseString' deprecated - use 'parse_string'
        parse = parser.parseString(pattern)
    
    ../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89
    ../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89
    ../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89
    ../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89
    ../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89
    ../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89
      /private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89: PyparsingDeprecationWarning: 'resetCache' deprecated - use 'reset_cache'
        parser.resetCache()
    
    ../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_mathtext.py:45
      /private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_mathtext.py:45: PyparsingDeprecationWarning: 'enablePackrat' deprecated - use 'enable_packrat'
        ParserElement.enablePackrat()
    
    -- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
    11 passed, 14 warnings in 0.59s
- **Python compilation: passed.** `/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m py_compile jsonldb/visual.py tests/test_visualization.py` (20 ms, working-tree@ad6ea45aee341a1cba67b595680e6d58ce46e0e1)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    (no output)
- **whitespace and source size: passed.** `/bin/sh -c git diff --check && test "$(wc -l < jsonldb/visual.py)" -le 500` (14 ms, working-tree@ad6ea45aee341a1cba67b595680e6d58ce46e0e1)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    (no output)

## Current acceptance evidence

- **visualization and tracked regressions: passed.** `/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m pytest -q tests` (810 ms, working-tree@ad6ea45aee341a1cba67b595680e6d58ce46e0e1)
- **Python compilation: passed.** `/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m py_compile jsonldb/visual.py tests/test_visualization.py` (20 ms, working-tree@ad6ea45aee341a1cba67b595680e6d58ce46e0e1)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    (no output)
- **whitespace and source size: passed.** `/bin/sh -c git diff --check && test "$(wc -l < jsonldb/visual.py)" -le 500` (14 ms, working-tree@ad6ea45aee341a1cba67b595680e6d58ce46e0e1)
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
          ".specdev/adhoc/2026-09/AH-20260904T081835643Z-0715_fix-visualization-edge-cases-by-allowing-an-empt.md",
          "jsonldb/visual.py",
          "tests/test_visualization.py"
        ],
        "rejected": [],
        "remaining": []
      },
      "attempt_history": [
        {
          "version": 1,
          "id": "V-001",
          "label": "visualization and tracked regressions",
          "annotation": null,
          "command": "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m pytest -q tests",
          "argv": [
            "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python",
            "-m",
            "pytest",
            "-q",
            "tests"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T08:20:26.573Z",
          "completed_at": "2026-09-04T08:20:27.384Z",
          "duration_ms": 810,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@ad6ea45aee341a1cba67b595680e6d58ce46e0e1",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                               [100%]stdout: \n=============================== warnings summary ===============================\n../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:64\n  /private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:64: PyparsingDeprecationWarning: 'oneOf' deprecated - use 'one_of'\n    prop = Group((name + Suppress(\"=\") + comma_separated(value)) | oneOf(_CONSTANTS))\n\n../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85\n../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85\n../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85\n../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85\n../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85\n../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85\n  /private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85: PyparsingDeprecationWarning: 'parseString' deprecated - use 'parse_string'\n    parse = parser.parseString(pattern)\n\n../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89\n../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89\n../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89\n../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89\n../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89\n../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89\n  /private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89: PyparsingDeprecationWarning: 'resetCache' deprecated - use 'reset_cache'\n    parser.resetCache()\n\n../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_mathtext.py:45\n  /private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_mathtext.py:45: PyparsingDeprecationWarning: 'enablePackrat' deprecated - use 'enable_packrat'\n    ParserElement.enablePackrat()\n\n-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html\n11 passed, 14 warnings in 0.59s",
            "truncated": false,
            "captured_bytes": 2953
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "Python compilation",
          "annotation": null,
          "command": "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m py_compile jsonldb/visual.py tests/test_visualization.py",
          "argv": [
            "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python",
            "-m",
            "py_compile",
            "jsonldb/visual.py",
            "tests/test_visualization.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T08:20:27.526Z",
          "completed_at": "2026-09-04T08:20:27.547Z",
          "duration_ms": 20,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@ad6ea45aee341a1cba67b595680e6d58ce46e0e1",
          "output": {
            "text": "",
            "truncated": false,
            "captured_bytes": 0
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "whitespace and source size",
          "annotation": null,
          "command": "/bin/sh -c git diff --check && test \"$(wc -l < jsonldb/visual.py)\" -le 500",
          "argv": [
            "/bin/sh",
            "-c",
            "git diff --check && test \"$(wc -l < jsonldb/visual.py)\" -le 500"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T08:20:27.684Z",
          "completed_at": "2026-09-04T08:20:27.698Z",
          "duration_ms": 14,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@ad6ea45aee341a1cba67b595680e6d58ce46e0e1",
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
          "label": "visualization and tracked regressions",
          "annotation": null,
          "command": "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m pytest -q tests",
          "argv": [
            "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python",
            "-m",
            "pytest",
            "-q",
            "tests"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T08:20:26.573Z",
          "completed_at": "2026-09-04T08:20:27.384Z",
          "duration_ms": 810,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@ad6ea45aee341a1cba67b595680e6d58ce46e0e1",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                               [100%]stdout: \n=============================== warnings summary ===============================\n../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:64\n  /private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:64: PyparsingDeprecationWarning: 'oneOf' deprecated - use 'one_of'\n    prop = Group((name + Suppress(\"=\") + comma_separated(value)) | oneOf(_CONSTANTS))\n\n../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85\n../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85\n../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85\n../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85\n../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85\n../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85\n  /private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85: PyparsingDeprecationWarning: 'parseString' deprecated - use 'parse_string'\n    parse = parser.parseString(pattern)\n\n../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89\n../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89\n../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89\n../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89\n../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89\n../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89\n  /private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89: PyparsingDeprecationWarning: 'resetCache' deprecated - use 'reset_cache'\n    parser.resetCache()\n\n../../../../../../private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_mathtext.py:45\n  /private/tmp/jsonldb-lint-verify.93ya2J/venv/lib/python3.9/site-packages/matplotlib/_mathtext.py:45: PyparsingDeprecationWarning: 'enablePackrat' deprecated - use 'enable_packrat'\n    ParserElement.enablePackrat()\n\n-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html\n11 passed, 14 warnings in 0.59s",
            "truncated": false,
            "captured_bytes": 2953
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "Python compilation",
          "annotation": null,
          "command": "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m py_compile jsonldb/visual.py tests/test_visualization.py",
          "argv": [
            "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python",
            "-m",
            "py_compile",
            "jsonldb/visual.py",
            "tests/test_visualization.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T08:20:27.526Z",
          "completed_at": "2026-09-04T08:20:27.547Z",
          "duration_ms": 20,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@ad6ea45aee341a1cba67b595680e6d58ce46e0e1",
          "output": {
            "text": "",
            "truncated": false,
            "captured_bytes": 0
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "whitespace and source size",
          "annotation": null,
          "command": "/bin/sh -c git diff --check && test \"$(wc -l < jsonldb/visual.py)\" -le 500",
          "argv": [
            "/bin/sh",
            "-c",
            "git diff --check && test \"$(wc -l < jsonldb/visual.py)\" -le 500"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T08:20:27.684Z",
          "completed_at": "2026-09-04T08:20:27.698Z",
          "duration_ms": 14,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@ad6ea45aee341a1cba67b595680e6d58ce46e0e1",
          "output": {
            "text": "",
            "truncated": false,
            "captured_bytes": 0
          }
        }
      ]
    }
