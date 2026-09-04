# Adhoc AH-20260904T080207193Z-ae88

- Scope: Clean package hygiene by deleting the stale folderdb.py.tmp copy, removing confirmed unused imports, correcting internal hierarchy typos and docstrings, renaming select_line_jsonl's misleading flag without changing positional behavior, and normalizing affected signatures.
- Title: Clean package hygiene
- Started: 2026-09-04T08:02:07.193Z
- Completed: 2026-09-04T08:06:50.303Z
- Starting working tree: Clean.

## Outcome

Removed the stale packaged folderdb.py.tmp copy and mechanically proven unused imports; corrected hierarchy naming and documentation, standardized affected signatures, renamed select_line_jsonl's flag to auto_deserialize without changing positional behavior, and normalized visual.py line endings and trailing whitespace.

## Focused workflow coexistence

Assignment 00008 remained the focused owner at run assignment-lifecycle-20260729T07073993. Its workflow paths were excluded from this delivery. Post-detour contract revalidation is required before it advances.

## Delivery path facts

### Requested adopted paths

None.

### Committed paths

- `.specdev/adhoc/2026-09/AH-20260904T080207193Z-ae88_clean-package-hygiene-by-deleting-the-stale-fold.md`
- `jsonldb/folderdb.py`
- `jsonldb/folderdb.py.tmp`
- `jsonldb/jsonlfile.py`
- `jsonldb/vercontrol.py`
- `jsonldb/visual.py`

### Rejected paths

None.

### Remaining owned paths

None.

## Verification summary

No manual verification summary was supplied.

## Verification attempt history

- **tracked regressions: passed.** `/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m pytest -q tests` (353 ms, working-tree@e4b1feb45e827cfb59ab40f274e427ac0c4cef0c)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                                 [100%]stdout: 
    9 passed in 0.21s
- **package compilation: passed.** `/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m py_compile jsonldb/__init__.py jsonldb/folderdb.py jsonldb/jsonldf.py jsonldb/jsonlfile.py jsonldb/vercontrol.py jsonldb/visual.py` (25 ms, working-tree@e4b1feb45e827cfb59ab40f274e427ac0c4cef0c)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    (no output)
- **package hygiene: passed.** `/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -c import ast
import inspect
import tempfile
from pathlib import Path
from jsonldb.jsonlfile import save_jsonl, select_line_jsonl
for filename in ("jsonldb/jsonlfile.py", "jsonldb/folderdb.py", "jsonldb/vercontrol.py", "jsonldb/visual.py"):
    tree = ast.parse(Path(filename).read_text())
    imported = {}
    used = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imported[alias.asname or alias.name.split(".")[0]] = alias.name
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                imported[alias.asname or alias.name] = alias.name
    unused = sorted(name for name in imported if name not in used)
    assert not unused, (filename, unused)
with tempfile.TemporaryDirectory() as folder:
    path = str(Path(folder, "records.jsonl"))
    save_jsonl(path, {"key": {"value": 1}})
    assert select_line_jsonl(path, "key", auto_deserialize=False) == {"key": {"value": 1}}
parameters = inspect.signature(select_line_jsonl).parameters
assert "auto_deserialize" in parameters and "auto_serialize" not in parameters
assert not Path("jsonldb/folderdb.py.tmp").exists()` (181 ms, working-tree@e4b1feb45e827cfb59ab40f274e427ac0c4cef0c)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    (no output)
- **whitespace and source size: passed.** `/bin/sh -c git diff --check && test "$(wc -l < jsonldb/folderdb.py)" -le 1000` (15 ms, working-tree@e4b1feb45e827cfb59ab40f274e427ac0c4cef0c)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    (no output)

## Current acceptance evidence

- **tracked regressions: passed.** `/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m pytest -q tests` (353 ms, working-tree@e4b1feb45e827cfb59ab40f274e427ac0c4cef0c)
- **package compilation: passed.** `/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m py_compile jsonldb/__init__.py jsonldb/folderdb.py jsonldb/jsonldf.py jsonldb/jsonlfile.py jsonldb/vercontrol.py jsonldb/visual.py` (25 ms, working-tree@e4b1feb45e827cfb59ab40f274e427ac0c4cef0c)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    (no output)
- **package hygiene: passed.** `/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -c import ast
import inspect
import tempfile
from pathlib import Path
from jsonldb.jsonlfile import save_jsonl, select_line_jsonl
for filename in ("jsonldb/jsonlfile.py", "jsonldb/folderdb.py", "jsonldb/vercontrol.py", "jsonldb/visual.py"):
    tree = ast.parse(Path(filename).read_text())
    imported = {}
    used = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imported[alias.asname or alias.name.split(".")[0]] = alias.name
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                imported[alias.asname or alias.name] = alias.name
    unused = sorted(name for name in imported if name not in used)
    assert not unused, (filename, unused)
with tempfile.TemporaryDirectory() as folder:
    path = str(Path(folder, "records.jsonl"))
    save_jsonl(path, {"key": {"value": 1}})
    assert select_line_jsonl(path, "key", auto_deserialize=False) == {"key": {"value": 1}}
parameters = inspect.signature(select_line_jsonl).parameters
assert "auto_deserialize" in parameters and "auto_serialize" not in parameters
assert not Path("jsonldb/folderdb.py.tmp").exists()` (181 ms, working-tree@e4b1feb45e827cfb59ab40f274e427ac0c4cef0c)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    (no output)
- **whitespace and source size: passed.** `/bin/sh -c git diff --check && test "$(wc -l < jsonldb/folderdb.py)" -le 1000` (15 ms, working-tree@e4b1feb45e827cfb59ab40f274e427ac0c4cef0c)
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
          ".specdev/adhoc/2026-09/AH-20260904T080207193Z-ae88_clean-package-hygiene-by-deleting-the-stale-fold.md",
          "jsonldb/folderdb.py",
          "jsonldb/folderdb.py.tmp",
          "jsonldb/jsonlfile.py",
          "jsonldb/vercontrol.py",
          "jsonldb/visual.py"
        ],
        "rejected": [],
        "remaining": []
      },
      "attempt_history": [
        {
          "version": 1,
          "id": "V-001",
          "label": "tracked regressions",
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
          "started_at": "2026-09-04T08:06:35.952Z",
          "completed_at": "2026-09-04T08:06:36.306Z",
          "duration_ms": 353,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@e4b1feb45e827cfb59ab40f274e427ac0c4cef0c",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                                 [100%]stdout: \n9 passed in 0.21s",
            "truncated": false,
            "captured_bytes": 98
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "package compilation",
          "annotation": null,
          "command": "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m py_compile jsonldb/__init__.py jsonldb/folderdb.py jsonldb/jsonldf.py jsonldb/jsonlfile.py jsonldb/vercontrol.py jsonldb/visual.py",
          "argv": [
            "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python",
            "-m",
            "py_compile",
            "jsonldb/__init__.py",
            "jsonldb/folderdb.py",
            "jsonldb/jsonldf.py",
            "jsonldb/jsonlfile.py",
            "jsonldb/vercontrol.py",
            "jsonldb/visual.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T08:06:36.452Z",
          "completed_at": "2026-09-04T08:06:36.478Z",
          "duration_ms": 25,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@e4b1feb45e827cfb59ab40f274e427ac0c4cef0c",
          "output": {
            "text": "",
            "truncated": false,
            "captured_bytes": 0
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "package hygiene",
          "annotation": null,
          "command": "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -c import ast\nimport inspect\nimport tempfile\nfrom pathlib import Path\nfrom jsonldb.jsonlfile import save_jsonl, select_line_jsonl\nfor filename in (\"jsonldb/jsonlfile.py\", \"jsonldb/folderdb.py\", \"jsonldb/vercontrol.py\", \"jsonldb/visual.py\"):\n    tree = ast.parse(Path(filename).read_text())\n    imported = {}\n    used = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}\n    for node in ast.walk(tree):\n        if isinstance(node, ast.Import):\n            for alias in node.names:\n                imported[alias.asname or alias.name.split(\".\")[0]] = alias.name\n        elif isinstance(node, ast.ImportFrom):\n            for alias in node.names:\n                imported[alias.asname or alias.name] = alias.name\n    unused = sorted(name for name in imported if name not in used)\n    assert not unused, (filename, unused)\nwith tempfile.TemporaryDirectory() as folder:\n    path = str(Path(folder, \"records.jsonl\"))\n    save_jsonl(path, {\"key\": {\"value\": 1}})\n    assert select_line_jsonl(path, \"key\", auto_deserialize=False) == {\"key\": {\"value\": 1}}\nparameters = inspect.signature(select_line_jsonl).parameters\nassert \"auto_deserialize\" in parameters and \"auto_serialize\" not in parameters\nassert not Path(\"jsonldb/folderdb.py.tmp\").exists()",
          "argv": [
            "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python",
            "-c",
            "import ast\nimport inspect\nimport tempfile\nfrom pathlib import Path\nfrom jsonldb.jsonlfile import save_jsonl, select_line_jsonl\nfor filename in (\"jsonldb/jsonlfile.py\", \"jsonldb/folderdb.py\", \"jsonldb/vercontrol.py\", \"jsonldb/visual.py\"):\n    tree = ast.parse(Path(filename).read_text())\n    imported = {}\n    used = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}\n    for node in ast.walk(tree):\n        if isinstance(node, ast.Import):\n            for alias in node.names:\n                imported[alias.asname or alias.name.split(\".\")[0]] = alias.name\n        elif isinstance(node, ast.ImportFrom):\n            for alias in node.names:\n                imported[alias.asname or alias.name] = alias.name\n    unused = sorted(name for name in imported if name not in used)\n    assert not unused, (filename, unused)\nwith tempfile.TemporaryDirectory() as folder:\n    path = str(Path(folder, \"records.jsonl\"))\n    save_jsonl(path, {\"key\": {\"value\": 1}})\n    assert select_line_jsonl(path, \"key\", auto_deserialize=False) == {\"key\": {\"value\": 1}}\nparameters = inspect.signature(select_line_jsonl).parameters\nassert \"auto_deserialize\" in parameters and \"auto_serialize\" not in parameters\nassert not Path(\"jsonldb/folderdb.py.tmp\").exists()"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T08:06:36.616Z",
          "completed_at": "2026-09-04T08:06:36.797Z",
          "duration_ms": 181,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@e4b1feb45e827cfb59ab40f274e427ac0c4cef0c",
          "output": {
            "text": "",
            "truncated": false,
            "captured_bytes": 0
          }
        },
        {
          "version": 1,
          "id": "V-004",
          "label": "whitespace and source size",
          "annotation": null,
          "command": "/bin/sh -c git diff --check && test \"$(wc -l < jsonldb/folderdb.py)\" -le 1000",
          "argv": [
            "/bin/sh",
            "-c",
            "git diff --check && test \"$(wc -l < jsonldb/folderdb.py)\" -le 1000"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T08:06:36.938Z",
          "completed_at": "2026-09-04T08:06:36.952Z",
          "duration_ms": 15,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@e4b1feb45e827cfb59ab40f274e427ac0c4cef0c",
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
          "label": "tracked regressions",
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
          "started_at": "2026-09-04T08:06:35.952Z",
          "completed_at": "2026-09-04T08:06:36.306Z",
          "duration_ms": 353,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@e4b1feb45e827cfb59ab40f274e427ac0c4cef0c",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                                 [100%]stdout: \n9 passed in 0.21s",
            "truncated": false,
            "captured_bytes": 98
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "package compilation",
          "annotation": null,
          "command": "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -m py_compile jsonldb/__init__.py jsonldb/folderdb.py jsonldb/jsonldf.py jsonldb/jsonlfile.py jsonldb/vercontrol.py jsonldb/visual.py",
          "argv": [
            "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python",
            "-m",
            "py_compile",
            "jsonldb/__init__.py",
            "jsonldb/folderdb.py",
            "jsonldb/jsonldf.py",
            "jsonldb/jsonlfile.py",
            "jsonldb/vercontrol.py",
            "jsonldb/visual.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T08:06:36.452Z",
          "completed_at": "2026-09-04T08:06:36.478Z",
          "duration_ms": 25,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@e4b1feb45e827cfb59ab40f274e427ac0c4cef0c",
          "output": {
            "text": "",
            "truncated": false,
            "captured_bytes": 0
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "package hygiene",
          "annotation": null,
          "command": "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -c import ast\nimport inspect\nimport tempfile\nfrom pathlib import Path\nfrom jsonldb.jsonlfile import save_jsonl, select_line_jsonl\nfor filename in (\"jsonldb/jsonlfile.py\", \"jsonldb/folderdb.py\", \"jsonldb/vercontrol.py\", \"jsonldb/visual.py\"):\n    tree = ast.parse(Path(filename).read_text())\n    imported = {}\n    used = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}\n    for node in ast.walk(tree):\n        if isinstance(node, ast.Import):\n            for alias in node.names:\n                imported[alias.asname or alias.name.split(\".\")[0]] = alias.name\n        elif isinstance(node, ast.ImportFrom):\n            for alias in node.names:\n                imported[alias.asname or alias.name] = alias.name\n    unused = sorted(name for name in imported if name not in used)\n    assert not unused, (filename, unused)\nwith tempfile.TemporaryDirectory() as folder:\n    path = str(Path(folder, \"records.jsonl\"))\n    save_jsonl(path, {\"key\": {\"value\": 1}})\n    assert select_line_jsonl(path, \"key\", auto_deserialize=False) == {\"key\": {\"value\": 1}}\nparameters = inspect.signature(select_line_jsonl).parameters\nassert \"auto_deserialize\" in parameters and \"auto_serialize\" not in parameters\nassert not Path(\"jsonldb/folderdb.py.tmp\").exists()",
          "argv": [
            "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python",
            "-c",
            "import ast\nimport inspect\nimport tempfile\nfrom pathlib import Path\nfrom jsonldb.jsonlfile import save_jsonl, select_line_jsonl\nfor filename in (\"jsonldb/jsonlfile.py\", \"jsonldb/folderdb.py\", \"jsonldb/vercontrol.py\", \"jsonldb/visual.py\"):\n    tree = ast.parse(Path(filename).read_text())\n    imported = {}\n    used = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}\n    for node in ast.walk(tree):\n        if isinstance(node, ast.Import):\n            for alias in node.names:\n                imported[alias.asname or alias.name.split(\".\")[0]] = alias.name\n        elif isinstance(node, ast.ImportFrom):\n            for alias in node.names:\n                imported[alias.asname or alias.name] = alias.name\n    unused = sorted(name for name in imported if name not in used)\n    assert not unused, (filename, unused)\nwith tempfile.TemporaryDirectory() as folder:\n    path = str(Path(folder, \"records.jsonl\"))\n    save_jsonl(path, {\"key\": {\"value\": 1}})\n    assert select_line_jsonl(path, \"key\", auto_deserialize=False) == {\"key\": {\"value\": 1}}\nparameters = inspect.signature(select_line_jsonl).parameters\nassert \"auto_deserialize\" in parameters and \"auto_serialize\" not in parameters\nassert not Path(\"jsonldb/folderdb.py.tmp\").exists()"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T08:06:36.616Z",
          "completed_at": "2026-09-04T08:06:36.797Z",
          "duration_ms": 181,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@e4b1feb45e827cfb59ab40f274e427ac0c4cef0c",
          "output": {
            "text": "",
            "truncated": false,
            "captured_bytes": 0
          }
        },
        {
          "version": 1,
          "id": "V-004",
          "label": "whitespace and source size",
          "annotation": null,
          "command": "/bin/sh -c git diff --check && test \"$(wc -l < jsonldb/folderdb.py)\" -le 1000",
          "argv": [
            "/bin/sh",
            "-c",
            "git diff --check && test \"$(wc -l < jsonldb/folderdb.py)\" -le 1000"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T08:06:36.938Z",
          "completed_at": "2026-09-04T08:06:36.952Z",
          "duration_ms": 15,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@e4b1feb45e827cfb59ab40f274e427ac0c4cef0c",
          "output": {
            "text": "",
            "truncated": false,
            "captured_bytes": 0
          }
        }
      ]
    }
