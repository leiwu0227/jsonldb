# Adhoc AH-20260904T075505679Z-bced

- Scope: Rewrite README examples to use the existing FolderDB overwrite, upsert, query, deletion, version-control, metadata, and jsonldb.visual APIs, and align the Bokeh requirement with setup.py.
- Title: Correct README API examples
- Started: 2026-09-04T07:55:05.679Z
- Completed: 2026-09-04T07:58:13.647Z
- Starting working tree: Clean.

## Outcome

README examples now use the implemented FolderDB dictionary, DataFrame, deletion, metadata, lint, version-control, and module-level visualization APIs; key ordering and hard-reset behavior are explicit, and the Bokeh minimum matches setup.py.

## Focused workflow coexistence

Assignment 00008 remained the focused owner at run assignment-lifecycle-20260729T07073993. Its workflow paths were excluded from this delivery. Post-detour contract revalidation is required before it advances.

## Delivery path facts

### Requested adopted paths

None.

### Committed paths

- `.specdev/adhoc/2026-09/AH-20260904T075505679Z-bced_rewrite-readme-examples-to-use-the-existing-fold.md`
- `README.md`

### Rejected paths

None.

### Remaining owned paths

None.

## Verification summary

No manual verification summary was supplied.

## Verification attempt history

- **README API and dependency references: passed.** `/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -c import ast
import re
from pathlib import Path
readme = Path("README.md").read_text()
for block in re.findall(r"```python\n(.*?)```", readme, re.S):
    ast.parse(block)
folderdb = ast.parse(Path("jsonldb/folderdb.py").read_text())
methods = {item.name for node in folderdb.body if isinstance(node, ast.ClassDef) and node.name == "FolderDB" for item in node.body if isinstance(item, ast.FunctionDef)}
expected = {"overwrite_dict", "upsert_dict", "get_dict", "overwrite_dicts", "overwrite_df", "upsert_df", "get_df", "overwrite_dfs", "upsert_dfs", "delete_file_keys", "delete_file_range", "delete_file", "delete_range", "get_dbmeta", "lint_db", "commit", "version", "revert"}
assert expected <= methods
forbidden = re.compile(r"db\.(store|query|update|delete|delete_table|get_metadata|update_metadata|visualize)\b")
assert not forbidden.search(readme)
visual = ast.parse(Path("jsonldb/visual.py").read_text())
functions = {node.name for node in visual.body if isinstance(node, ast.FunctionDef)}
assert {"visualize_folderdb", "visualize_jsonl"} <= functions
assert "bokeh >= 2.0.0" in readme and "\"bokeh>=2.0.0\"" in Path("setup.py").read_text()` (23 ms, working-tree@2fd07f039500f0f98b96af5dff57d24c40363618)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    (no output)
- **README data operation examples: passed.** `/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -c import tempfile
from pathlib import Path
import pandas as pd
from jsonldb import FolderDB
with tempfile.TemporaryDirectory() as folder:
    db = FolderDB(folder)
    db.overwrite_dict("users", {"user-001": {"name": "Ada", "active": True}, "user-002": {"name": "Linus", "active": True}})
    db.upsert_dict("users", {"user-002": {"name": "Linus", "active": False}, "user-003": {"name": "Grace", "active": True}})
    selected_users = db.get_dict(["users"], lower_key="user-001", upper_key="user-002")["users"]
    assert list(selected_users) == ["user-001", "user-002"]
    frame = pd.DataFrame({"value": ["a", "b", "c"]}, index=["row-001", "row-002", "row-003"])
    db.overwrite_df("measurements", frame)
    db.upsert_df("measurements", pd.DataFrame({"value": ["updated"]}, index=["row-002"]))
    selected_frame = db.get_df(["measurements"], lower_key="row-001", upper_key="row-002")["measurements"]
    assert list(selected_frame.index) == ["row-001", "row-002"]
    assert selected_frame.loc["row-002", "value"] == "updated"
    db.delete_file_keys("users", ["user-001"])
    db.delete_file_range("users", "user-002", "user-003")
    db.delete_file("users")
    assert "users" not in db.get_dbmeta()
    assert db.get_dbmeta()["measurements"]["count"] == 3
    assert Path(folder, "measurements.jsonl").exists()` (167 ms, working-tree@2fd07f039500f0f98b96af5dff57d24c40363618)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    (no output)
- **whitespace: passed.** `git diff --check` (10 ms, working-tree@2fd07f039500f0f98b96af5dff57d24c40363618)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    (no output)

## Current acceptance evidence

- **README API and dependency references: passed.** `/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -c import ast
import re
from pathlib import Path
readme = Path("README.md").read_text()
for block in re.findall(r"```python\n(.*?)```", readme, re.S):
    ast.parse(block)
folderdb = ast.parse(Path("jsonldb/folderdb.py").read_text())
methods = {item.name for node in folderdb.body if isinstance(node, ast.ClassDef) and node.name == "FolderDB" for item in node.body if isinstance(item, ast.FunctionDef)}
expected = {"overwrite_dict", "upsert_dict", "get_dict", "overwrite_dicts", "overwrite_df", "upsert_df", "get_df", "overwrite_dfs", "upsert_dfs", "delete_file_keys", "delete_file_range", "delete_file", "delete_range", "get_dbmeta", "lint_db", "commit", "version", "revert"}
assert expected <= methods
forbidden = re.compile(r"db\.(store|query|update|delete|delete_table|get_metadata|update_metadata|visualize)\b")
assert not forbidden.search(readme)
visual = ast.parse(Path("jsonldb/visual.py").read_text())
functions = {node.name for node in visual.body if isinstance(node, ast.FunctionDef)}
assert {"visualize_folderdb", "visualize_jsonl"} <= functions
assert "bokeh >= 2.0.0" in readme and "\"bokeh>=2.0.0\"" in Path("setup.py").read_text()` (23 ms, working-tree@2fd07f039500f0f98b96af5dff57d24c40363618)
- **README data operation examples: passed.** `/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -c import tempfile
from pathlib import Path
import pandas as pd
from jsonldb import FolderDB
with tempfile.TemporaryDirectory() as folder:
    db = FolderDB(folder)
    db.overwrite_dict("users", {"user-001": {"name": "Ada", "active": True}, "user-002": {"name": "Linus", "active": True}})
    db.upsert_dict("users", {"user-002": {"name": "Linus", "active": False}, "user-003": {"name": "Grace", "active": True}})
    selected_users = db.get_dict(["users"], lower_key="user-001", upper_key="user-002")["users"]
    assert list(selected_users) == ["user-001", "user-002"]
    frame = pd.DataFrame({"value": ["a", "b", "c"]}, index=["row-001", "row-002", "row-003"])
    db.overwrite_df("measurements", frame)
    db.upsert_df("measurements", pd.DataFrame({"value": ["updated"]}, index=["row-002"]))
    selected_frame = db.get_df(["measurements"], lower_key="row-001", upper_key="row-002")["measurements"]
    assert list(selected_frame.index) == ["row-001", "row-002"]
    assert selected_frame.loc["row-002", "value"] == "updated"
    db.delete_file_keys("users", ["user-001"])
    db.delete_file_range("users", "user-002", "user-003")
    db.delete_file("users")
    assert "users" not in db.get_dbmeta()
    assert db.get_dbmeta()["measurements"]["count"] == 3
    assert Path(folder, "measurements.jsonl").exists()` (167 ms, working-tree@2fd07f039500f0f98b96af5dff57d24c40363618)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    (no output)
- **whitespace: passed.** `git diff --check` (10 ms, working-tree@2fd07f039500f0f98b96af5dff57d24c40363618)
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
          ".specdev/adhoc/2026-09/AH-20260904T075505679Z-bced_rewrite-readme-examples-to-use-the-existing-fold.md",
          "README.md"
        ],
        "rejected": [],
        "remaining": []
      },
      "attempt_history": [
        {
          "version": 1,
          "id": "V-001",
          "label": "README API and dependency references",
          "annotation": null,
          "command": "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -c import ast\nimport re\nfrom pathlib import Path\nreadme = Path(\"README.md\").read_text()\nfor block in re.findall(r\"```python\\n(.*?)```\", readme, re.S):\n    ast.parse(block)\nfolderdb = ast.parse(Path(\"jsonldb/folderdb.py\").read_text())\nmethods = {item.name for node in folderdb.body if isinstance(node, ast.ClassDef) and node.name == \"FolderDB\" for item in node.body if isinstance(item, ast.FunctionDef)}\nexpected = {\"overwrite_dict\", \"upsert_dict\", \"get_dict\", \"overwrite_dicts\", \"overwrite_df\", \"upsert_df\", \"get_df\", \"overwrite_dfs\", \"upsert_dfs\", \"delete_file_keys\", \"delete_file_range\", \"delete_file\", \"delete_range\", \"get_dbmeta\", \"lint_db\", \"commit\", \"version\", \"revert\"}\nassert expected <= methods\nforbidden = re.compile(r\"db\\.(store|query|update|delete|delete_table|get_metadata|update_metadata|visualize)\\b\")\nassert not forbidden.search(readme)\nvisual = ast.parse(Path(\"jsonldb/visual.py\").read_text())\nfunctions = {node.name for node in visual.body if isinstance(node, ast.FunctionDef)}\nassert {\"visualize_folderdb\", \"visualize_jsonl\"} <= functions\nassert \"bokeh >= 2.0.0\" in readme and \"\\\"bokeh>=2.0.0\\\"\" in Path(\"setup.py\").read_text()",
          "argv": [
            "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python",
            "-c",
            "import ast\nimport re\nfrom pathlib import Path\nreadme = Path(\"README.md\").read_text()\nfor block in re.findall(r\"```python\\n(.*?)```\", readme, re.S):\n    ast.parse(block)\nfolderdb = ast.parse(Path(\"jsonldb/folderdb.py\").read_text())\nmethods = {item.name for node in folderdb.body if isinstance(node, ast.ClassDef) and node.name == \"FolderDB\" for item in node.body if isinstance(item, ast.FunctionDef)}\nexpected = {\"overwrite_dict\", \"upsert_dict\", \"get_dict\", \"overwrite_dicts\", \"overwrite_df\", \"upsert_df\", \"get_df\", \"overwrite_dfs\", \"upsert_dfs\", \"delete_file_keys\", \"delete_file_range\", \"delete_file\", \"delete_range\", \"get_dbmeta\", \"lint_db\", \"commit\", \"version\", \"revert\"}\nassert expected <= methods\nforbidden = re.compile(r\"db\\.(store|query|update|delete|delete_table|get_metadata|update_metadata|visualize)\\b\")\nassert not forbidden.search(readme)\nvisual = ast.parse(Path(\"jsonldb/visual.py\").read_text())\nfunctions = {node.name for node in visual.body if isinstance(node, ast.FunctionDef)}\nassert {\"visualize_folderdb\", \"visualize_jsonl\"} <= functions\nassert \"bokeh >= 2.0.0\" in readme and \"\\\"bokeh>=2.0.0\\\"\" in Path(\"setup.py\").read_text()"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T07:57:58.472Z",
          "completed_at": "2026-09-04T07:57:58.496Z",
          "duration_ms": 23,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@2fd07f039500f0f98b96af5dff57d24c40363618",
          "output": {
            "text": "",
            "truncated": false,
            "captured_bytes": 0
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "README data operation examples",
          "annotation": null,
          "command": "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -c import tempfile\nfrom pathlib import Path\nimport pandas as pd\nfrom jsonldb import FolderDB\nwith tempfile.TemporaryDirectory() as folder:\n    db = FolderDB(folder)\n    db.overwrite_dict(\"users\", {\"user-001\": {\"name\": \"Ada\", \"active\": True}, \"user-002\": {\"name\": \"Linus\", \"active\": True}})\n    db.upsert_dict(\"users\", {\"user-002\": {\"name\": \"Linus\", \"active\": False}, \"user-003\": {\"name\": \"Grace\", \"active\": True}})\n    selected_users = db.get_dict([\"users\"], lower_key=\"user-001\", upper_key=\"user-002\")[\"users\"]\n    assert list(selected_users) == [\"user-001\", \"user-002\"]\n    frame = pd.DataFrame({\"value\": [\"a\", \"b\", \"c\"]}, index=[\"row-001\", \"row-002\", \"row-003\"])\n    db.overwrite_df(\"measurements\", frame)\n    db.upsert_df(\"measurements\", pd.DataFrame({\"value\": [\"updated\"]}, index=[\"row-002\"]))\n    selected_frame = db.get_df([\"measurements\"], lower_key=\"row-001\", upper_key=\"row-002\")[\"measurements\"]\n    assert list(selected_frame.index) == [\"row-001\", \"row-002\"]\n    assert selected_frame.loc[\"row-002\", \"value\"] == \"updated\"\n    db.delete_file_keys(\"users\", [\"user-001\"])\n    db.delete_file_range(\"users\", \"user-002\", \"user-003\")\n    db.delete_file(\"users\")\n    assert \"users\" not in db.get_dbmeta()\n    assert db.get_dbmeta()[\"measurements\"][\"count\"] == 3\n    assert Path(folder, \"measurements.jsonl\").exists()",
          "argv": [
            "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python",
            "-c",
            "import tempfile\nfrom pathlib import Path\nimport pandas as pd\nfrom jsonldb import FolderDB\nwith tempfile.TemporaryDirectory() as folder:\n    db = FolderDB(folder)\n    db.overwrite_dict(\"users\", {\"user-001\": {\"name\": \"Ada\", \"active\": True}, \"user-002\": {\"name\": \"Linus\", \"active\": True}})\n    db.upsert_dict(\"users\", {\"user-002\": {\"name\": \"Linus\", \"active\": False}, \"user-003\": {\"name\": \"Grace\", \"active\": True}})\n    selected_users = db.get_dict([\"users\"], lower_key=\"user-001\", upper_key=\"user-002\")[\"users\"]\n    assert list(selected_users) == [\"user-001\", \"user-002\"]\n    frame = pd.DataFrame({\"value\": [\"a\", \"b\", \"c\"]}, index=[\"row-001\", \"row-002\", \"row-003\"])\n    db.overwrite_df(\"measurements\", frame)\n    db.upsert_df(\"measurements\", pd.DataFrame({\"value\": [\"updated\"]}, index=[\"row-002\"]))\n    selected_frame = db.get_df([\"measurements\"], lower_key=\"row-001\", upper_key=\"row-002\")[\"measurements\"]\n    assert list(selected_frame.index) == [\"row-001\", \"row-002\"]\n    assert selected_frame.loc[\"row-002\", \"value\"] == \"updated\"\n    db.delete_file_keys(\"users\", [\"user-001\"])\n    db.delete_file_range(\"users\", \"user-002\", \"user-003\")\n    db.delete_file(\"users\")\n    assert \"users\" not in db.get_dbmeta()\n    assert db.get_dbmeta()[\"measurements\"][\"count\"] == 3\n    assert Path(folder, \"measurements.jsonl\").exists()"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T07:57:58.637Z",
          "completed_at": "2026-09-04T07:57:58.805Z",
          "duration_ms": 167,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@2fd07f039500f0f98b96af5dff57d24c40363618",
          "output": {
            "text": "",
            "truncated": false,
            "captured_bytes": 0
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "whitespace",
          "annotation": null,
          "command": "git diff --check",
          "argv": [
            "git",
            "diff",
            "--check"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T07:57:58.946Z",
          "completed_at": "2026-09-04T07:57:58.956Z",
          "duration_ms": 10,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@2fd07f039500f0f98b96af5dff57d24c40363618",
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
          "label": "README API and dependency references",
          "annotation": null,
          "command": "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -c import ast\nimport re\nfrom pathlib import Path\nreadme = Path(\"README.md\").read_text()\nfor block in re.findall(r\"```python\\n(.*?)```\", readme, re.S):\n    ast.parse(block)\nfolderdb = ast.parse(Path(\"jsonldb/folderdb.py\").read_text())\nmethods = {item.name for node in folderdb.body if isinstance(node, ast.ClassDef) and node.name == \"FolderDB\" for item in node.body if isinstance(item, ast.FunctionDef)}\nexpected = {\"overwrite_dict\", \"upsert_dict\", \"get_dict\", \"overwrite_dicts\", \"overwrite_df\", \"upsert_df\", \"get_df\", \"overwrite_dfs\", \"upsert_dfs\", \"delete_file_keys\", \"delete_file_range\", \"delete_file\", \"delete_range\", \"get_dbmeta\", \"lint_db\", \"commit\", \"version\", \"revert\"}\nassert expected <= methods\nforbidden = re.compile(r\"db\\.(store|query|update|delete|delete_table|get_metadata|update_metadata|visualize)\\b\")\nassert not forbidden.search(readme)\nvisual = ast.parse(Path(\"jsonldb/visual.py\").read_text())\nfunctions = {node.name for node in visual.body if isinstance(node, ast.FunctionDef)}\nassert {\"visualize_folderdb\", \"visualize_jsonl\"} <= functions\nassert \"bokeh >= 2.0.0\" in readme and \"\\\"bokeh>=2.0.0\\\"\" in Path(\"setup.py\").read_text()",
          "argv": [
            "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python",
            "-c",
            "import ast\nimport re\nfrom pathlib import Path\nreadme = Path(\"README.md\").read_text()\nfor block in re.findall(r\"```python\\n(.*?)```\", readme, re.S):\n    ast.parse(block)\nfolderdb = ast.parse(Path(\"jsonldb/folderdb.py\").read_text())\nmethods = {item.name for node in folderdb.body if isinstance(node, ast.ClassDef) and node.name == \"FolderDB\" for item in node.body if isinstance(item, ast.FunctionDef)}\nexpected = {\"overwrite_dict\", \"upsert_dict\", \"get_dict\", \"overwrite_dicts\", \"overwrite_df\", \"upsert_df\", \"get_df\", \"overwrite_dfs\", \"upsert_dfs\", \"delete_file_keys\", \"delete_file_range\", \"delete_file\", \"delete_range\", \"get_dbmeta\", \"lint_db\", \"commit\", \"version\", \"revert\"}\nassert expected <= methods\nforbidden = re.compile(r\"db\\.(store|query|update|delete|delete_table|get_metadata|update_metadata|visualize)\\b\")\nassert not forbidden.search(readme)\nvisual = ast.parse(Path(\"jsonldb/visual.py\").read_text())\nfunctions = {node.name for node in visual.body if isinstance(node, ast.FunctionDef)}\nassert {\"visualize_folderdb\", \"visualize_jsonl\"} <= functions\nassert \"bokeh >= 2.0.0\" in readme and \"\\\"bokeh>=2.0.0\\\"\" in Path(\"setup.py\").read_text()"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T07:57:58.472Z",
          "completed_at": "2026-09-04T07:57:58.496Z",
          "duration_ms": 23,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@2fd07f039500f0f98b96af5dff57d24c40363618",
          "output": {
            "text": "",
            "truncated": false,
            "captured_bytes": 0
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "README data operation examples",
          "annotation": null,
          "command": "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python -c import tempfile\nfrom pathlib import Path\nimport pandas as pd\nfrom jsonldb import FolderDB\nwith tempfile.TemporaryDirectory() as folder:\n    db = FolderDB(folder)\n    db.overwrite_dict(\"users\", {\"user-001\": {\"name\": \"Ada\", \"active\": True}, \"user-002\": {\"name\": \"Linus\", \"active\": True}})\n    db.upsert_dict(\"users\", {\"user-002\": {\"name\": \"Linus\", \"active\": False}, \"user-003\": {\"name\": \"Grace\", \"active\": True}})\n    selected_users = db.get_dict([\"users\"], lower_key=\"user-001\", upper_key=\"user-002\")[\"users\"]\n    assert list(selected_users) == [\"user-001\", \"user-002\"]\n    frame = pd.DataFrame({\"value\": [\"a\", \"b\", \"c\"]}, index=[\"row-001\", \"row-002\", \"row-003\"])\n    db.overwrite_df(\"measurements\", frame)\n    db.upsert_df(\"measurements\", pd.DataFrame({\"value\": [\"updated\"]}, index=[\"row-002\"]))\n    selected_frame = db.get_df([\"measurements\"], lower_key=\"row-001\", upper_key=\"row-002\")[\"measurements\"]\n    assert list(selected_frame.index) == [\"row-001\", \"row-002\"]\n    assert selected_frame.loc[\"row-002\", \"value\"] == \"updated\"\n    db.delete_file_keys(\"users\", [\"user-001\"])\n    db.delete_file_range(\"users\", \"user-002\", \"user-003\")\n    db.delete_file(\"users\")\n    assert \"users\" not in db.get_dbmeta()\n    assert db.get_dbmeta()[\"measurements\"][\"count\"] == 3\n    assert Path(folder, \"measurements.jsonl\").exists()",
          "argv": [
            "/tmp/jsonldb-lint-verify.93ya2J/venv/bin/python",
            "-c",
            "import tempfile\nfrom pathlib import Path\nimport pandas as pd\nfrom jsonldb import FolderDB\nwith tempfile.TemporaryDirectory() as folder:\n    db = FolderDB(folder)\n    db.overwrite_dict(\"users\", {\"user-001\": {\"name\": \"Ada\", \"active\": True}, \"user-002\": {\"name\": \"Linus\", \"active\": True}})\n    db.upsert_dict(\"users\", {\"user-002\": {\"name\": \"Linus\", \"active\": False}, \"user-003\": {\"name\": \"Grace\", \"active\": True}})\n    selected_users = db.get_dict([\"users\"], lower_key=\"user-001\", upper_key=\"user-002\")[\"users\"]\n    assert list(selected_users) == [\"user-001\", \"user-002\"]\n    frame = pd.DataFrame({\"value\": [\"a\", \"b\", \"c\"]}, index=[\"row-001\", \"row-002\", \"row-003\"])\n    db.overwrite_df(\"measurements\", frame)\n    db.upsert_df(\"measurements\", pd.DataFrame({\"value\": [\"updated\"]}, index=[\"row-002\"]))\n    selected_frame = db.get_df([\"measurements\"], lower_key=\"row-001\", upper_key=\"row-002\")[\"measurements\"]\n    assert list(selected_frame.index) == [\"row-001\", \"row-002\"]\n    assert selected_frame.loc[\"row-002\", \"value\"] == \"updated\"\n    db.delete_file_keys(\"users\", [\"user-001\"])\n    db.delete_file_range(\"users\", \"user-002\", \"user-003\")\n    db.delete_file(\"users\")\n    assert \"users\" not in db.get_dbmeta()\n    assert db.get_dbmeta()[\"measurements\"][\"count\"] == 3\n    assert Path(folder, \"measurements.jsonl\").exists()"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T07:57:58.637Z",
          "completed_at": "2026-09-04T07:57:58.805Z",
          "duration_ms": 167,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@2fd07f039500f0f98b96af5dff57d24c40363618",
          "output": {
            "text": "",
            "truncated": false,
            "captured_bytes": 0
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "whitespace",
          "annotation": null,
          "command": "git diff --check",
          "argv": [
            "git",
            "diff",
            "--check"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-04T07:57:58.946Z",
          "completed_at": "2026-09-04T07:57:58.956Z",
          "duration_ms": 10,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@2fd07f039500f0f98b96af5dff57d24c40363618",
          "output": {
            "text": "",
            "truncated": false,
            "captured_bytes": 0
          }
        }
      ]
    }
