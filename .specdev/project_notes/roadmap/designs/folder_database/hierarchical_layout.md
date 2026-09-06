# Hierarchical Layout

Hierarchy organizes a database's tables into directories while preserving each table's logical name and contents. The configured depth is a maximum: tables with different numbers of name segments coexist, and short names do not need padding to fit the database layout.

## Naming rule

Split the table name on the database delimiter after removing an optional `.jsonl` suffix. The final segment identifies the table and never becomes a directory. Use preceding segments, in order, up to the configured maximum. The filename always retains the complete table name.

For a name with *n* segments and maximum depth *m*, directory depth is **min(m, n - 1)**. A single-segment name lives directly at the database root. Names longer than the maximum remain valid; the maximum limits directories, not logical name length.

```
maximum depth 6, delimiter "."
  a.jsonl                 -> <root>/a.jsonl
  a.b.jsonl               -> <root>/a/a.b.jsonl
  a.b.c.jsonl             -> <root>/a/b/a.b.c.jsonl
  a.b.c.d.e.f.g.h.jsonl   -> <root>/a/b/c/d/e/f/a.b.c.d.e.f.g.h.jsonl
```

Keeping the complete name in the filename makes a table identifiable independently of its location. Existing portable-name and safe-directory restrictions continue to apply. Too few segments is never a reason to reject or quarantine a table.

## Configuration and recovery

Hierarchy remains a database-wide setting with a delimiter and a positive maximum depth. The existing `hierarchy_depth` argument and stored depth represent this maximum; a public parameter rename is unnecessary for the concept. Flat mode keeps every table at the root.

Saved hierarchy settings in `h.meta` are authoritative. Opening with a different explicit maximum requests reorganization. Root-level tables may coexist with nested tables in hierarchy mode, so a root-level table does not establish that the database is flat.

When settings are missing or damaged, recovery must account for mixed depths and preserve access to every table. The shallowest observed directory depth does not determine the maximum. Existing tables may not reveal the original configured maximum at all; recovery must distinguish inferred settings from known settings and avoid silently excluding data. The exact fallback policy remains to be settled before implementation.

## Reorganization and compatibility

Reorganization brings visible tables into the layout derived from their names and the target settings. Data and companion indexes move together, embedded metadata remains intact, empty directories are pruned, and database metadata reflects the resulting locations. Short tables remain ordinary database members at every maximum.

The new mapping changes some existing locations even when the configured number stays the same. In particular, a name with exactly that many segments loses its final directory. Compatibility must therefore cover databases opened with unchanged settings as well as explicit depth changes; existing tables must remain accessible during adoption of the new rule. The migration trigger and interrupted-migration behavior must be defined before implementation.

Previously quarantined tables can be reprocessed into the new layout if their names are safe. Restoration must preserve data and must not overwrite another table. Hidden quarantine contents remain outside ordinary discovery until restored.

## Discovery and tradeoffs

Discovery includes root-level and nested tables and derives logical names from filenames. Hidden directories, including quarantine and reports, remain excluded. Deleting a table prunes visible directories left empty.

Mixed depths allow natural names without artificial segments. The maximum bounds nesting but does not guarantee balanced directory sizes. Placement remains deterministic for a given name and configuration; changing the delimiter or maximum can require moving tables. Compatibility with older readers that enforce fixed depth requires a deliberate migration policy.

## Source target

- `jsonldb/folderdb.py`: at most 1250 lines in total, shared with the folder-database, metadata, metadata-slot, and integrity-logs notes.
