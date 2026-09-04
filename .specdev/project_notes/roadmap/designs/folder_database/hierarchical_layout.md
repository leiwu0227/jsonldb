# Hierarchical Layout

Hierarchy mode spreads a database's tables across subdirectories so that a folder with tens of thousands of tables stays navigable and directory listings stay fast. It changes only where a table's file lives; the table's logical name, its index, and every data operation are unchanged.

## Naming rule

A table name is split on the database delimiter. In hierarchy mode with depth *d*, the first *d* segments become nested directories and the file keeps its full dotted name inside the deepest one:

```
depth 2, delimiter "."
  region.us.spy   ->  <root>/region/us/region.us.spy.jsonl
  region.eu.dax   ->  <root>/region/eu/region.eu.dax.jsonl
```

Keeping the full name in the filename makes a file self-describing wherever it sits, and discovery can recover the logical name from the filename alone without reconstructing it from the path.

A name is **valid** for depth *d* when it has at least *d* minus one delimiters. In flat mode every name is valid. Reads and writes reject invalid names outright rather than guessing a location.

## Configuration

Hierarchy settings live in `h.meta`: whether the mode is on, the delimiter, and the depth. The file is written only when hierarchy mode is enabled; a database without it is flat. Opening a database with an explicit depth that differs from the stored one triggers a reorganization to the new depth, so changing depth is an ordinary open with a new argument.

## Reorganization

Reorganization is the operation that establishes or changes the hierarchy. It walks every table under the root (skipping hidden directories), classifies each name as valid or invalid for the target depth, and moves files:

- valid tables move to their computed directory, together with their index file;
- invalid tables move to a quarantine folder `.invalid_tickers` at the root, also with their index;
- directories left empty are pruned bottom-up;
- `db.meta` is rebuilt and `h.meta` written.

Quarantine instead of deletion is deliberate: a badly named table is still data, and a later change of depth or delimiter may make it valid. A separate reprocess step re-examines the quarantine folder and moves back any table whose name has become valid, building an index if it lacks one.

Moves are performed file by file and are not transactional. An interruption can leave a table moved without its index, which index self-healing repairs on the next read, or leave a partially reorganized tree, which re-running the open with the same depth completes.

## Discovery under hierarchy

Listing tables walks the whole tree and reports filenames without their extension. Hidden directories, including the quarantine folder and any Git directory, are never entered, so quarantined and version-control files are invisible to data operations. Deleting a table prunes any directories it leaves empty so the tree never accumulates empty branches.

## Trade-offs

- Depth is a database-wide constant. Tables with fewer segments than the depth requires cannot live in that database; they are quarantined.
- Directory placement is derived from the name at write time, so changing the delimiter or depth always requires a reorganization pass.
- Empty-directory pruning runs after deletes and lint and visits every directory in the tree, a cost accepted for keeping the layout canonical.

## Source target

- `jsonldb/folderdb.py`: at most 1000 lines in total, shared with the folder-database and metadata notes.
