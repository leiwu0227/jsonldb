# Proposal: self-heal empty/corrupt `.idx` index files

A `.idx` index that exists but is **empty or unparseable** is never rebuilt.
`ensure_index_exists` only rebuilds when the index is *missing* or *older than
the `.jsonl`* by mtime, so a zero-length `.idx` left behind by an interrupted
write (killed process, upstream 502s) keeps a fresh mtime and is trusted
forever. Every subsequent read does a raw `orjson.loads(f.read())` and crashes
with `Input is a zero-length, empty document`, which a downstream lazy-cache
flush swallows — silently dropping the dirty batch and leaving series stuck at a
stale date (observed in oceandata, root-caused to jsonldb).

The `.idx` is fully derived from the `.jsonl`, so the fix is to treat an empty
or corrupt index exactly like a missing one: rebuild it and continue. We add a
`size==0` trigger to `ensure_index_exists`, introduce a single robust index
loader that rebuilds-and-rereads on a parse failure, route every scattered raw
index read across `jsonlfile.py`, `folderdb.py`, and `visual.py` through it, and
emit a one-line `WARNING` whenever a rebuild was
triggered by corruption (not by a normal stale-mtime rebuild) so a recurrence is
never again silent. The change is purely additive — it only ever turns a current
hard crash into transparent recovery.
