# Version Control

Version control gives a database a history of whole-folder snapshots using Git. The database directory itself becomes the repository, so a snapshot captures every table, index, and control file exactly as they are on disk, and restoring a snapshot restores all of them together.

## Model

- **Repository = database folder.** Initialization happens implicitly on the first commit; a folder that is already a repository is reused with a warning.
- **Version = commit.** A commit stages everything in the folder and records it with a timestamped message. A caller-supplied message is prefixed as a manual commit; an empty one becomes an automatic commit carrying the timestamp alone.
- **Listing versions** returns short commit hashes mapped to their messages, newest first.
- **Revert** resets the working tree to a chosen commit. This is a hard reset: uncommitted changes are discarded, and commits made after the chosen one leave the branch history. It is a rewind, not an undo commit.

## Why whole-folder snapshots

Tables, indexes, and metadata are interdependent. Snapshotting only data files would leave indexes and statistics stale after a revert; snapshotting the whole folder means a restored database is immediately consistent without a lint pass. The cost is that rebuildable index files and control files are stored in history alongside data. No ignore rules are written, so callers who want smaller histories must add their own.

## Boundaries

The module is a thin wrapper over GitPython and is the only place that imports it. `FolderDB` exposes `commit`, `revert`, and `version` and imports this module inside those methods, so version control is an optional capability that costs nothing until used. Git errors are re-raised with a message naming the operation. Remote repositories, branches, merges, and tags are outside scope; the library offers linear local history only.

## Trade-offs

- Git handles large JSONL files adequately for local history but stores each changed file whole, so frequent commits of large tables grow the repository quickly.
- The hard-reset revert is simple and matches the "restore this snapshot" mental model, but it is destructive. Callers wanting to keep later history should commit before reverting and note the hash they left.

## Source target

- `jsonldb/vercontrol.py`: at most 160 lines.
