# Adhoc AH-20260904T082355821Z-238b

- Scope: Make the tracked test suite reproducible from a clean clone by adding minimal PEP 517 and pytest configuration, a Python-3.8-compatible generated development dependency lock, and setup instructions, without migrating setup.py metadata or touching ignored Assignment 00008 aux tests.
- Title: Make clean-clone tests reproducible
- Started: 2026-09-04T08:23:55.821Z
- Completed: 2026-09-04T08:27:57.910Z
- Starting working tree: Clean.

## Outcome

The 11 tracked tests now have minimal PEP 517 and pytest configuration, a universal hash-locked development environment resolved from setup.py for Python 3.8+, documented fresh-environment install and regeneration commands, and an ignored .venv path; a fresh Python 3.9 environment installed the lock and passed all tests, while ignored Assignment 00008 aux tests remained untouched.

## Focused workflow coexistence

Assignment 00008 remained the focused owner at run assignment-lifecycle-20260729T07073993. Its workflow paths were excluded from this delivery. Post-detour contract revalidation is required before it advances.

## Delivery path facts

### Requested adopted paths

None.

### Committed paths

- `.gitignore`
- `.specdev/adhoc/2026-09/AH-20260904T082355821Z-238b_make-the-tracked-test-suite-reproducible-from-a-.md`
- `README.md`
- `pyproject.toml`
- `requirements-dev.in`
- `requirements-dev.lock`

### Rejected paths

None.

### Remaining owned paths

None.

## Verification summary

No manual verification summary was supplied.

## Verification attempt history

- **locked environment integrity: passed.** `/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/bin/python -m pip check` (242 ms, working-tree@cfaf206392f79a33a4ca83ba7b69bc06a65ebf84)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: No broken requirements found.
- **clean environment tests: passed.** `/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/bin/python -m pytest -q` (808 ms, working-tree@cfaf206392f79a33a4ca83ba7b69bc06a65ebf84)
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
    11 passed, 14 warnings in 0.59s
- **deterministic dependency lock: failed.** `/bin/sh -c before=$(shasum -a 256 requirements-dev.lock | awk "{print \\$1}"); uv pip compile setup.py requirements-dev.in --python-version 3.8 --universal --generate-hashes --output-file requirements-dev.lock --custom-compile-command "uv pip compile setup.py requirements-dev.in --python-version 3.8 --universal --generate-hashes --output-file requirements-dev.lock" >/tmp/jsonldb-uv-lock-verify.log && after=$(shasum -a 256 requirements-dev.lock | awk "{print \\$1}") && test "$before" = "$after"` (174 ms, working-tree@cfaf206392f79a33a4ca83ba7b69bc06a65ebf84)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 2
  - Output:

    stderr: awk: syntax error at source line 1
     context is
    	{print >>>  \ <<< }
    awk: illegal statement at source line 1
    stderr: warning: The requested Python version 3.8 is not available; 3.12.10 will be used to build dependencies instead.
    stderr: Resolved 67 packages in 15ms
    stderr: awk: syntax error at source line 1
     context is
    	{print >>>  \ <<< }
    awk: illegal statement at source line 1
- **configuration ownership and whitespace: passed.** `/bin/sh -c /opt/homebrew/bin/python3 -c "import tomllib; c=tomllib.load(open(\"pyproject.toml\", \"rb\")); assert c[\"build-system\"][\"build-backend\"] == \"setuptools.build_meta\"; assert c[\"tool\"][\"pytest\"][\"ini_options\"][\"testpaths\"] == [\"tests\"]" && test "$(git hash-object unit_tests/test_jsonlfile.py)" = 404df2b071af8a36703472aab58502f3d9e1fbc9 && test "$(git hash-object unit_tests/test_folderdb.py)" = 0247a050372591ac6cc0def600a818962ecb139d && git diff --check` (43 ms, working-tree@cfaf206392f79a33a4ca83ba7b69bc06a65ebf84)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    (no output)
- **deterministic dependency lock: passed.** `/bin/sh -c before=$(shasum -a 256 requirements-dev.lock | cut -d " " -f 1); uv pip compile setup.py requirements-dev.in --python-version 3.8 --universal --generate-hashes --output-file requirements-dev.lock --custom-compile-command "uv pip compile setup.py requirements-dev.in --python-version 3.8 --universal --generate-hashes --output-file requirements-dev.lock" >/tmp/jsonldb-uv-lock-verify.log && after=$(shasum -a 256 requirements-dev.lock | cut -d " " -f 1); test "$before" = "$after"` (173 ms, working-tree@cfaf206392f79a33a4ca83ba7b69bc06a65ebf84)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stderr: warning: The requested Python version 3.8 is not available; 3.12.10 will be used to build dependencies instead.
    stderr: Resolved 67 packages in 15ms
- **configuration ownership and whitespace: passed.** `/bin/sh -c /opt/homebrew/bin/python3 -c "import tomllib; c=tomllib.load(open(\"pyproject.toml\", \"rb\")); assert c[\"build-system\"][\"build-backend\"] == \"setuptools.build_meta\"; assert c[\"tool\"][\"pytest\"][\"ini_options\"][\"testpaths\"] == [\"tests\"]" && git check-ignore -q .venv/probe && test "$(git hash-object unit_tests/test_jsonlfile.py)" = 404df2b071af8a36703472aab58502f3d9e1fbc9 && test "$(git hash-object unit_tests/test_folderdb.py)" = 0247a050372591ac6cc0def600a818962ecb139d && git diff --check` (52 ms, working-tree@cfaf206392f79a33a4ca83ba7b69bc06a65ebf84)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    (no output)

## Current acceptance evidence

- **locked environment integrity: passed.** `/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/bin/python -m pip check` (242 ms, working-tree@cfaf206392f79a33a4ca83ba7b69bc06a65ebf84)
- **clean environment tests: passed.** `/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/bin/python -m pytest -q` (808 ms, working-tree@cfaf206392f79a33a4ca83ba7b69bc06a65ebf84)
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
    11 passed, 14 warnings in 0.59s
- **deterministic dependency lock: passed.** `/bin/sh -c before=$(shasum -a 256 requirements-dev.lock | cut -d " " -f 1); uv pip compile setup.py requirements-dev.in --python-version 3.8 --universal --generate-hashes --output-file requirements-dev.lock --custom-compile-command "uv pip compile setup.py requirements-dev.in --python-version 3.8 --universal --generate-hashes --output-file requirements-dev.lock" >/tmp/jsonldb-uv-lock-verify.log && after=$(shasum -a 256 requirements-dev.lock | cut -d " " -f 1); test "$before" = "$after"` (173 ms, working-tree@cfaf206392f79a33a4ca83ba7b69bc06a65ebf84)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stderr: warning: The requested Python version 3.8 is not available; 3.12.10 will be used to build dependencies instead.
    stderr: Resolved 67 packages in 15ms
- **configuration ownership and whitespace: passed.** `/bin/sh -c /opt/homebrew/bin/python3 -c "import tomllib; c=tomllib.load(open(\"pyproject.toml\", \"rb\")); assert c[\"build-system\"][\"build-backend\"] == \"setuptools.build_meta\"; assert c[\"tool\"][\"pytest\"][\"ini_options\"][\"testpaths\"] == [\"tests\"]" && git check-ignore -q .venv/probe && test "$(git hash-object unit_tests/test_jsonlfile.py)" = 404df2b071af8a36703472aab58502f3d9e1fbc9 && test "$(git hash-object unit_tests/test_folderdb.py)" = 0247a050372591ac6cc0def600a818962ecb139d && git diff --check` (52 ms, working-tree@cfaf206392f79a33a4ca83ba7b69bc06a65ebf84)
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
          ".specdev/adhoc/2026-09/AH-20260904T082355821Z-238b_make-the-tracked-test-suite-reproducible-from-a-.md",
          "README.md",
          "pyproject.toml",
          "requirements-dev.in",
          "requirements-dev.lock"
        ],
        "rejected": [],
        "remaining": []
      },
      "attempt_history": [
        {
          "version": 1,
          "id": "V-001",
          "label": "locked environment integrity",
          "annotation": null,
          "command": "/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/bin/python -m pip check",
          "argv": [
            "/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/bin/python",
            "-m",
            "pip",
            "check"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T08:26:48.262Z",
          "completed_at": "2026-09-04T08:26:48.504Z",
          "duration_ms": 242,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@cfaf206392f79a33a4ca83ba7b69bc06a65ebf84",
          "output": {
            "text": "stdout: No broken requirements found.",
            "truncated": false,
            "captured_bytes": 30
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "clean environment tests",
          "annotation": null,
          "command": "/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/bin/python -m pytest -q",
          "argv": [
            "/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/bin/python",
            "-m",
            "pytest",
            "-q"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T08:26:48.647Z",
          "completed_at": "2026-09-04T08:26:49.456Z",
          "duration_ms": 808,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@cfaf206392f79a33a4ca83ba7b69bc06a65ebf84",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                               [100%]stdout: \n=============================== warnings summary ===============================\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:64\n  /private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:64: PyparsingDeprecationWarning: 'oneOf' deprecated - use 'one_of'\n    prop = Group((name + Suppress(\"=\") + comma_separated(value)) | oneOf(_CONSTANTS))\n\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85\n  /private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85: PyparsingDeprecationWarning: 'parseString' deprecated - use 'parse_string'\n    parse = parser.parseString(pattern)\n\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89\n  /private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89: PyparsingDeprecationWarning: 'resetCache' deprecated - use 'reset_cache'\n    parser.resetCache()\n\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_mathtext.py:45\n  /private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_mathtext.py:45: PyparsingDeprecationWarning: 'enablePackrat' deprecated - use 'enable_packrat'\n    ParserElement.enablePackrat()\n\n-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html\n11 passed, 14 warnings in 0.59s",
            "truncated": false,
            "captured_bytes": 3079
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "deterministic dependency lock",
          "annotation": null,
          "command": "/bin/sh -c before=$(shasum -a 256 requirements-dev.lock | awk \"{print \\\\$1}\"); uv pip compile setup.py requirements-dev.in --python-version 3.8 --universal --generate-hashes --output-file requirements-dev.lock --custom-compile-command \"uv pip compile setup.py requirements-dev.in --python-version 3.8 --universal --generate-hashes --output-file requirements-dev.lock\" >/tmp/jsonldb-uv-lock-verify.log && after=$(shasum -a 256 requirements-dev.lock | awk \"{print \\\\$1}\") && test \"$before\" = \"$after\"",
          "argv": [
            "/bin/sh",
            "-c",
            "before=$(shasum -a 256 requirements-dev.lock | awk \"{print \\\\$1}\"); uv pip compile setup.py requirements-dev.in --python-version 3.8 --universal --generate-hashes --output-file requirements-dev.lock --custom-compile-command \"uv pip compile setup.py requirements-dev.in --python-version 3.8 --universal --generate-hashes --output-file requirements-dev.lock\" >/tmp/jsonldb-uv-lock-verify.log && after=$(shasum -a 256 requirements-dev.lock | awk \"{print \\\\$1}\") && test \"$before\" = \"$after\""
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T08:26:49.600Z",
          "completed_at": "2026-09-04T08:26:49.774Z",
          "duration_ms": 174,
          "exit_status": 2,
          "status": "failed",
          "tested_revision": "working-tree@cfaf206392f79a33a4ca83ba7b69bc06a65ebf84",
          "output": {
            "text": "stderr: awk: syntax error at source line 1\n context is\n\t{print >>>  \\ <<< }\nawk: illegal statement at source line 1\nstderr: warning: The requested Python version 3.8 is not available; 3.12.10 will be used to build dependencies instead.\nstderr: Resolved 67 packages in 15ms\nstderr: awk: syntax error at source line 1\n context is\n\t{print >>>  \\ <<< }\nawk: illegal statement at source line 1",
            "truncated": false,
            "captured_bytes": 357
          }
        },
        {
          "version": 1,
          "id": "V-004",
          "label": "configuration ownership and whitespace",
          "annotation": null,
          "command": "/bin/sh -c /opt/homebrew/bin/python3 -c \"import tomllib; c=tomllib.load(open(\\\"pyproject.toml\\\", \\\"rb\\\")); assert c[\\\"build-system\\\"][\\\"build-backend\\\"] == \\\"setuptools.build_meta\\\"; assert c[\\\"tool\\\"][\\\"pytest\\\"][\\\"ini_options\\\"][\\\"testpaths\\\"] == [\\\"tests\\\"]\" && test \"$(git hash-object unit_tests/test_jsonlfile.py)\" = 404df2b071af8a36703472aab58502f3d9e1fbc9 && test \"$(git hash-object unit_tests/test_folderdb.py)\" = 0247a050372591ac6cc0def600a818962ecb139d && git diff --check",
          "argv": [
            "/bin/sh",
            "-c",
            "/opt/homebrew/bin/python3 -c \"import tomllib; c=tomllib.load(open(\\\"pyproject.toml\\\", \\\"rb\\\")); assert c[\\\"build-system\\\"][\\\"build-backend\\\"] == \\\"setuptools.build_meta\\\"; assert c[\\\"tool\\\"][\\\"pytest\\\"][\\\"ini_options\\\"][\\\"testpaths\\\"] == [\\\"tests\\\"]\" && test \"$(git hash-object unit_tests/test_jsonlfile.py)\" = 404df2b071af8a36703472aab58502f3d9e1fbc9 && test \"$(git hash-object unit_tests/test_folderdb.py)\" = 0247a050372591ac6cc0def600a818962ecb139d && git diff --check"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T08:26:49.916Z",
          "completed_at": "2026-09-04T08:26:49.960Z",
          "duration_ms": 43,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@cfaf206392f79a33a4ca83ba7b69bc06a65ebf84",
          "output": {
            "text": "",
            "truncated": false,
            "captured_bytes": 0
          }
        },
        {
          "version": 1,
          "id": "V-005",
          "label": "deterministic dependency lock",
          "annotation": null,
          "command": "/bin/sh -c before=$(shasum -a 256 requirements-dev.lock | cut -d \" \" -f 1); uv pip compile setup.py requirements-dev.in --python-version 3.8 --universal --generate-hashes --output-file requirements-dev.lock --custom-compile-command \"uv pip compile setup.py requirements-dev.in --python-version 3.8 --universal --generate-hashes --output-file requirements-dev.lock\" >/tmp/jsonldb-uv-lock-verify.log && after=$(shasum -a 256 requirements-dev.lock | cut -d \" \" -f 1); test \"$before\" = \"$after\"",
          "argv": [
            "/bin/sh",
            "-c",
            "before=$(shasum -a 256 requirements-dev.lock | cut -d \" \" -f 1); uv pip compile setup.py requirements-dev.in --python-version 3.8 --universal --generate-hashes --output-file requirements-dev.lock --custom-compile-command \"uv pip compile setup.py requirements-dev.in --python-version 3.8 --universal --generate-hashes --output-file requirements-dev.lock\" >/tmp/jsonldb-uv-lock-verify.log && after=$(shasum -a 256 requirements-dev.lock | cut -d \" \" -f 1); test \"$before\" = \"$after\""
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T08:27:08.377Z",
          "completed_at": "2026-09-04T08:27:08.550Z",
          "duration_ms": 173,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@cfaf206392f79a33a4ca83ba7b69bc06a65ebf84",
          "output": {
            "text": "stderr: warning: The requested Python version 3.8 is not available; 3.12.10 will be used to build dependencies instead.\nstderr: Resolved 67 packages in 15ms",
            "truncated": false,
            "captured_bytes": 141
          }
        },
        {
          "version": 1,
          "id": "V-006",
          "label": "configuration ownership and whitespace",
          "annotation": null,
          "command": "/bin/sh -c /opt/homebrew/bin/python3 -c \"import tomllib; c=tomllib.load(open(\\\"pyproject.toml\\\", \\\"rb\\\")); assert c[\\\"build-system\\\"][\\\"build-backend\\\"] == \\\"setuptools.build_meta\\\"; assert c[\\\"tool\\\"][\\\"pytest\\\"][\\\"ini_options\\\"][\\\"testpaths\\\"] == [\\\"tests\\\"]\" && git check-ignore -q .venv/probe && test \"$(git hash-object unit_tests/test_jsonlfile.py)\" = 404df2b071af8a36703472aab58502f3d9e1fbc9 && test \"$(git hash-object unit_tests/test_folderdb.py)\" = 0247a050372591ac6cc0def600a818962ecb139d && git diff --check",
          "argv": [
            "/bin/sh",
            "-c",
            "/opt/homebrew/bin/python3 -c \"import tomllib; c=tomllib.load(open(\\\"pyproject.toml\\\", \\\"rb\\\")); assert c[\\\"build-system\\\"][\\\"build-backend\\\"] == \\\"setuptools.build_meta\\\"; assert c[\\\"tool\\\"][\\\"pytest\\\"][\\\"ini_options\\\"][\\\"testpaths\\\"] == [\\\"tests\\\"]\" && git check-ignore -q .venv/probe && test \"$(git hash-object unit_tests/test_jsonlfile.py)\" = 404df2b071af8a36703472aab58502f3d9e1fbc9 && test \"$(git hash-object unit_tests/test_folderdb.py)\" = 0247a050372591ac6cc0def600a818962ecb139d && git diff --check"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T08:27:41.545Z",
          "completed_at": "2026-09-04T08:27:41.598Z",
          "duration_ms": 52,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@cfaf206392f79a33a4ca83ba7b69bc06a65ebf84",
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
          "label": "locked environment integrity",
          "annotation": null,
          "command": "/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/bin/python -m pip check",
          "argv": [
            "/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/bin/python",
            "-m",
            "pip",
            "check"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T08:26:48.262Z",
          "completed_at": "2026-09-04T08:26:48.504Z",
          "duration_ms": 242,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@cfaf206392f79a33a4ca83ba7b69bc06a65ebf84",
          "output": {
            "text": "stdout: No broken requirements found.",
            "truncated": false,
            "captured_bytes": 30
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "clean environment tests",
          "annotation": null,
          "command": "/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/bin/python -m pytest -q",
          "argv": [
            "/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/bin/python",
            "-m",
            "pytest",
            "-q"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T08:26:48.647Z",
          "completed_at": "2026-09-04T08:26:49.456Z",
          "duration_ms": 808,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@cfaf206392f79a33a4ca83ba7b69bc06a65ebf84",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                               [100%]stdout: \n=============================== warnings summary ===============================\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:64\n  /private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:64: PyparsingDeprecationWarning: 'oneOf' deprecated - use 'one_of'\n    prop = Group((name + Suppress(\"=\") + comma_separated(value)) | oneOf(_CONSTANTS))\n\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85\n  /private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:85: PyparsingDeprecationWarning: 'parseString' deprecated - use 'parse_string'\n    parse = parser.parseString(pattern)\n\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89\n  /private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_fontconfig_pattern.py:89: PyparsingDeprecationWarning: 'resetCache' deprecated - use 'reset_cache'\n    parser.resetCache()\n\n../../../../../../private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_mathtext.py:45\n  /private/tmp/jsonldb-clean-clone-verify.lNjS5T/venv/lib/python3.9/site-packages/matplotlib/_mathtext.py:45: PyparsingDeprecationWarning: 'enablePackrat' deprecated - use 'enable_packrat'\n    ParserElement.enablePackrat()\n\n-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html\n11 passed, 14 warnings in 0.59s",
            "truncated": false,
            "captured_bytes": 3079
          }
        },
        {
          "version": 1,
          "id": "V-005",
          "label": "deterministic dependency lock",
          "annotation": null,
          "command": "/bin/sh -c before=$(shasum -a 256 requirements-dev.lock | cut -d \" \" -f 1); uv pip compile setup.py requirements-dev.in --python-version 3.8 --universal --generate-hashes --output-file requirements-dev.lock --custom-compile-command \"uv pip compile setup.py requirements-dev.in --python-version 3.8 --universal --generate-hashes --output-file requirements-dev.lock\" >/tmp/jsonldb-uv-lock-verify.log && after=$(shasum -a 256 requirements-dev.lock | cut -d \" \" -f 1); test \"$before\" = \"$after\"",
          "argv": [
            "/bin/sh",
            "-c",
            "before=$(shasum -a 256 requirements-dev.lock | cut -d \" \" -f 1); uv pip compile setup.py requirements-dev.in --python-version 3.8 --universal --generate-hashes --output-file requirements-dev.lock --custom-compile-command \"uv pip compile setup.py requirements-dev.in --python-version 3.8 --universal --generate-hashes --output-file requirements-dev.lock\" >/tmp/jsonldb-uv-lock-verify.log && after=$(shasum -a 256 requirements-dev.lock | cut -d \" \" -f 1); test \"$before\" = \"$after\""
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T08:27:08.377Z",
          "completed_at": "2026-09-04T08:27:08.550Z",
          "duration_ms": 173,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@cfaf206392f79a33a4ca83ba7b69bc06a65ebf84",
          "output": {
            "text": "stderr: warning: The requested Python version 3.8 is not available; 3.12.10 will be used to build dependencies instead.\nstderr: Resolved 67 packages in 15ms",
            "truncated": false,
            "captured_bytes": 141
          }
        },
        {
          "version": 1,
          "id": "V-006",
          "label": "configuration ownership and whitespace",
          "annotation": null,
          "command": "/bin/sh -c /opt/homebrew/bin/python3 -c \"import tomllib; c=tomllib.load(open(\\\"pyproject.toml\\\", \\\"rb\\\")); assert c[\\\"build-system\\\"][\\\"build-backend\\\"] == \\\"setuptools.build_meta\\\"; assert c[\\\"tool\\\"][\\\"pytest\\\"][\\\"ini_options\\\"][\\\"testpaths\\\"] == [\\\"tests\\\"]\" && git check-ignore -q .venv/probe && test \"$(git hash-object unit_tests/test_jsonlfile.py)\" = 404df2b071af8a36703472aab58502f3d9e1fbc9 && test \"$(git hash-object unit_tests/test_folderdb.py)\" = 0247a050372591ac6cc0def600a818962ecb139d && git diff --check",
          "argv": [
            "/bin/sh",
            "-c",
            "/opt/homebrew/bin/python3 -c \"import tomllib; c=tomllib.load(open(\\\"pyproject.toml\\\", \\\"rb\\\")); assert c[\\\"build-system\\\"][\\\"build-backend\\\"] == \\\"setuptools.build_meta\\\"; assert c[\\\"tool\\\"][\\\"pytest\\\"][\\\"ini_options\\\"][\\\"testpaths\\\"] == [\\\"tests\\\"]\" && git check-ignore -q .venv/probe && test \"$(git hash-object unit_tests/test_jsonlfile.py)\" = 404df2b071af8a36703472aab58502f3d9e1fbc9 && test \"$(git hash-object unit_tests/test_folderdb.py)\" = 0247a050372591ac6cc0def600a818962ecb139d && git diff --check"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T08:27:41.545Z",
          "completed_at": "2026-09-04T08:27:41.598Z",
          "duration_ms": 52,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@cfaf206392f79a33a4ca83ba7b69bc06a65ebf84",
          "output": {
            "text": "",
            "truncated": false,
            "captured_bytes": 0
          }
        }
      ]
    }
