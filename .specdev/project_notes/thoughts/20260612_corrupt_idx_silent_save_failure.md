# Corrupt/empty `.idx` is never rebuilt → `orjson.loads` crash → silent save failure

**Date:** 2026-06-12
**Status:** Thoughts / bug report — pre-design. Found while debugging a
production data-staleness issue in the oceandata consumer; root cause is in
jsonldb's index handling.
**Severity:** High (silent data loss). A stale/empty `.idx` makes every
subsequent `upsert`/meta-rebuild on that file throw, and the oceandata lazy-cache
flush swallows the exception and drops the whole dirty batch without surfacing.

---

## Symptom (as seen downstream)

oceandata's lazy-cache flush (`api_impl_layer._flush_dirty_data`) writes a batch
of dirty tickers via `FolderDB.upsert_dfs(...)` in one call. For ~8 tickers it
logged:

```
ERROR ... Error flushing lazy cache data: Input is a zero-length, empty document: line 1 column 1 (char 0)
```

…then returned "successfully". The generated rows were computed and marked dirty
but **never persisted** — the affected series silently stayed stuck at an old
date while sibling series advanced. Re-running did not help; it recurred every run.

`Input is a zero-length, empty document: line 1 column 1 (char 0)` is the
`orjson.loads(b"")` message — i.e. **an `.idx` file that exists but is empty is
being parsed**.

## Root cause

jsonldb rebuilds an index only when it is **missing** or **older than the
`.jsonl`** — never when it **exists but is empty/corrupt**.

`jsonlfile.ensure_index_exists` (`jsonlfile.py:88-111`):

```python
should_rebuild = False
if not os.path.exists(index_file_path):
    should_rebuild = True
else:
    if os.path.getmtime(jsonl_file_path) > os.path.getmtime(index_file_path):
        should_rebuild = True
if should_rebuild:
    build_jsonl_index(jsonl_file_path)
```

A zero-length `.idx` with an mtime ≥ the `.jsonl` passes both checks → not
rebuilt. Then the read sites parse it unconditionally and crash. The same
"guard existence, then `orjson.loads`" pattern repeats, none validating content:

- `folderdb.py:527-532` (`delete_file_range`): `if not os.path.exists(index_path): build_jsonl_index(...)` then `index = orjson.loads(f.read())`.
- `folderdb.py:569-571` (`_make_meta_entry`): `if os.path.exists(index_file): index = orjson.loads(f.read())`.
- `folderdb.py:164` (`_scan_index_timespecs`): `index = orjson.loads(f.read())`.
- (and the `upsert_df`/`upsert_dfs` read path, `folderdb.py:345/361`.)

**How the `.idx` becomes empty in the first place:** an interrupted/killed write
(e.g. a process terminated mid-flush, or — in our case — runs aborted by upstream
Bloomberg sidecar 502s) can leave a truncated/zero-length `.idx`. Because its
mtime is then fresh, jsonldb treats it as valid forever after.

## Reproduction

```python
# given an existing, non-empty users.jsonl with a valid users.jsonl.idx
open("users.jsonl.idx", "w").close()          # simulate a truncated/empty index
folderdb.upsert_df("users", some_df)          # -> orjson "zero-length empty document"
```

## Proposed fix (self-heal — the `.jsonl` is the source of truth)

The `.idx` is fully derived from the `.jsonl` and is always rebuildable via
`build_jsonl_index`. So treat an **empty or unparseable** index exactly like a
**missing** one: rebuild it and continue. Concretely:

1. **`ensure_index_exists`** — add an empty/parse check to `should_rebuild`:
   ```python
   if not os.path.exists(index_file_path) or os.path.getsize(index_file_path) == 0:
       should_rebuild = True
   elif os.path.getmtime(jsonl) > os.path.getmtime(index_file_path):
       should_rebuild = True
   ```
   (optionally also catch a parse failure and rebuild.)

2. **A single robust loader** to replace the scattered `orjson.loads(f.read())`
   reads — e.g. `_load_index(file_path) -> dict` that: ensures the index exists,
   reads it, and on empty/`JSONDecodeError`/`orjson` error calls
   `build_jsonl_index(file_path)` once and re-reads. Route `delete_file_range`,
   `_make_meta_entry`, `_scan_index_timespecs`, and the upsert read path through
   it. This makes a corrupt index recoverable everywhere, so "reload + save"
   self-heals with no manual intervention.

### Notes
- **Blast radius:** this is a shared library; the change affects every jsonldb
  consumer. It is purely additive (rebuild-on-corruption), so it should only ever
  turn a current hard crash into a transparent recovery.
- **Tests:** empty `.idx` → upsert/read rebuilds and succeeds; truncated/partial
  `.idx` → same; valid `.idx` → unchanged (no spurious rebuilds, mtime path
  intact); a `.jsonl` newer than `.idx` still rebuilds (existing behavior).
- **Today's workaround (no code change):** rewrite the affected tickers so their
  `.idx` is rebuilt (`build_jsonl_index`, or write the file back through
  `upsert_df`). Done manually for the 8 stuck oceandata surfaces.

## Cross-repo companion (oceandata side, separate)

Even with jsonldb self-healing, oceandata's flush should not silently drop a
batch on one bad ticker. Recommended there (mirrors the oceandata BDH-retriever
00049/00050 isolate-and-surface fixes): in `_flush_dirty_data`, write per-ticker
(isolate), and **surface** any genuinely-failed tickers loudly at the end (raise
or summary) instead of logging `str(e)` and returning. That is an oceandata-repo
change, tracked separately; this note covers the jsonldb root fix.
