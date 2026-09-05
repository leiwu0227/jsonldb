# Copying, restoring and versioning data

JSONLDB stores data in ordinary files. Copying or archiving a quiescent database
does not require an export service. Git is optional and is operated by the caller;
the library does not automatically initialize repositories, commit or synchronize.

## Copy a complete database

1. Stop writes and maintenance operations for the duration of the copy.
2. Copy the directory, including `config.meta`, `h.meta` when present, and any
   quarantined data under `.invalid_tickers`. An archive must include hidden paths
   if you want those tables and diagnostic reports preserved.
3. Open the destination with its existing hierarchy settings. Refresh `db.meta`
   with `build_dbmeta()` to update informational paths after relocation.

```python
from datetime import datetime
from pathlib import Path
import shutil
from jsonldb import FolderDB

# A self-contained copy example; neither directory should already exist.
source = Path("copy_source")
source.mkdir()
db = FolderDB(str(source))
key = datetime(2026, 1, 5, 9, 30)
db.upsert_dict("sensor.room1", {key: {"temperature": 21.1}})
# No writes occur while this copy runs.
destination = Path("copy_destination")
shutil.copytree(source, destination)
copied = FolderDB(str(destination))
copied.build_dbmeta()
assert copied.get_dict(["sensor.room1"])["sensor.room1"][key]["temperature"] == 21.1
```

Copying a folder during writes is not a transactional snapshot. Plain text makes
the representation portable, but filesystem name/path limits and the installed
Python dependencies still apply. Older table names may not be valid on another
operating system, even though new names follow more portable creation rules.

## Copy one series

A `.jsonl` file carries its observations, consumer metadata and declared timezone.
Precision is still a **database** setting in `config.meta`; a standalone file is
not fully self-describing in that respect. The destination must use compatible
precision and slot configuration. Place it according to the destination's table
name/hierarchy, rebuild its index, and refresh destination statistics. Copying
does not perform timestamp or timezone conversion.

## Decide what Git tracks

Track observations and settings. Derived indexes and frequently rewritten reports
usually create noise rather than useful review history. For a Git repository
whose root is the database directory, a useful `.gitignore` is:

```gitignore
*.idx
/db.meta
/.jsonldb/
```

Keep `config.meta`, `h.meta` if present, and all intended `.jsonl` files tracked.
Do not blanket-ignore every hidden directory: quarantine can contain data.
Ignore patterns do not remove already tracked files from Git's index; review any
such tracking changes separately. If the database is nested in a source repo,
place these rules inside the database directory so their scope is clear.

Stop writes, inspect `git diff`, and commit a deliberate snapshot using ordinary
Git. JSONL rows make many observation changes easy to review, but blank tombstones,
padding and lint reordering can also appear in diffs. If you choose to lint before
a commit, inspect its findings and review compaction separately when it would
obscure meaningful changes. Git history is not a transaction log or retention policy.

## After checkout, merge, restore or external edits

Treat retained table indexes as disposable. Ordinary freshness checks use file
metadata; a restored or externally edited index can look current while pointing
at the wrong bytes. Rebuild instead of assuming timestamps prove correspondence.

The following recipe operates on an existing restored database named
`restored_data`. Stop writers first and resolve all Git conflict markers before
using it. It removes only visible **table index files**, not observations:

```python
from pathlib import Path
from jsonldb import FolderDB

root = Path("restored_data")
assert root.is_dir()
for index_path in root.rglob("*.jsonl.idx"):
    if not any(part.startswith(".") for part in index_path.relative_to(root).parts):
        index_path.unlink()
db = FolderDB(str(root))
db.build_dbmeta()
```

For suspected damage, preserve a copy before `db.lint_db(force=True)` and inspect
`.jsonldb/lint.log` afterward. Lint repairs storage layout and can remove malformed
rows; it cannot decide the intended business meaning of conflicting edits.
Git can produce a textually clean merge that still duplicates a timestamp or
combines incompatible configurations. Check those semantics explicitly.

See [file format](file-format.md) for settings and row interpretation, and
[design decisions](design.md) for the failure model.
