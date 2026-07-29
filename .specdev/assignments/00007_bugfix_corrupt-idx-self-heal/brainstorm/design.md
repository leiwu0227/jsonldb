# Design: self-heal empty/corrupt `.idx` index files

## Overview

jsonldb stores, for each `foo.jsonl`, a sidecar `foo.jsonl.idx` mapping each
linekey to its byte offset. The index is fully derived from the `.jsonl` and is
always rebuildable via `build_jsonl_index`. Today the index is rebuilt only when
it is **missing** or **older than the `.jsonl`** (mtime gate). An index that
**exists but is empty or unparseable** — e.g. a zero-length file left by a write
interrupted mid-flush — passes both checks (its mtime is fresh) and is trusted
forever, after which every read crashes on `orjson.loads`. This bugfix makes an
empty/corrupt index recover transparently, mirroring the rebuild-and-reread
self-heal already present in `lint_jsonl`'s fast path (`jsonlfile.py:201-207`)
and `_verify_and_compact` (`:134-137`).

## Root Cause

`ensure_index_exists` (`jsonlfile.py:88-111`) decides `should_rebuild` from
existence + mtime only — it never inspects index **content**:

```python
should_rebuild = False
if not os.path.exists(index_file_path):
    should_rebuild = True
else:
    if os.path.getmtime(jsonl) > os.path.getmtime(index_file_path):
        should_rebuild = True
```

A zero-length `.idx` with mtime ≥ the `.jsonl` is therefore never rebuilt. The
read sites then parse it unconditionally with no recovery. The same
"guard existence, then raw `orjson.loads(f.read())`" pattern repeats, none
validating content:

- `jsonlfile.py:451` (`select_jsonl` range), `:521` (`select_line_jsonl`),
  `:565` (`update_jsonl`), `:638` (`delete_jsonl`) — all preceded by
  `ensure_index_exists` but then a **raw** load with no try/except.
- `folderdb.py:164` (`_scan_index_timespecs`), `:532` (`delete_file_range`),
  `:571` (`_make_meta_entry`) — raw loads.
- `visual.py:64, 129, 228, 258, 342, 371` (`visualize_jsonl_bokeh`,
  `visualize_jsonl_matplot`, `visualize_folderdb_bokeh`,
  `visualize_folderdb_matplot`) — raw `json.load(f)`, and worse, they
  `raise FileNotFoundError` instead of building when the index is missing.
  These public visualization paths would still crash on an empty/corrupt index.
- `lint_jsonl` (`:203` fast path, `:219`/`:224` full mmac line-count path) and
  `_verify_and_compact` (`:136`) read the idx during maintenance/compaction.
  The fast path and the `_verify_and_compact` reload **already** rebuild-and-reread
  on failure — the pattern we generalize — but `lint_jsonl(force=True)` reaches a
  **raw** `orjson.loads(f.read())` at `:219`/`:224` that a non-empty/garbage idx
  would still crash. (`load_jsonl`, `:377-419`, does not read the `.idx` at all.)

The `size==0` case is the one seen in production; a non-empty-but-truncated/garbage
index is the rarer sibling. Both must heal.

## Fix Design

Two coordinated changes in `jsonlfile.py`, plus routing all index reads through
one helper.

### 1. `ensure_index_exists` — treat empty as missing, and warn

Add a zero-length trigger to the rebuild condition, and emit a corruption
warning **only** for the empty case (stale-mtime rebuilds stay silent, matching
today's behavior):

```python
should_rebuild = False
corrupt = False
if not os.path.exists(index_file_path):
    should_rebuild = True
elif os.path.getsize(index_file_path) == 0:
    should_rebuild = True
    corrupt = True
elif os.path.getmtime(jsonl_file_path) > os.path.getmtime(index_file_path):
    should_rebuild = True
if should_rebuild:
    if corrupt:
        print(f"WARNING: rebuilt empty index {index_file_path}")
    build_jsonl_index(jsonl_file_path)
```

A valid empty index is `b'{}'` (2 bytes), never 0 bytes, so `size==0` reliably
means truncation/corruption — no false-positive rebuilds.

### 2. `load_index` — single robust loader (parse-failure fallback)

Introduce one helper that becomes the only way index dicts are read:

```python
def load_index(jsonl_file_path: str) -> dict:
    """Load a JSONL file's index, rebuilding it if empty/missing/unparseable."""
    ensure_index_exists(jsonl_file_path)          # heals missing/empty/stale
    index_path = f"{jsonl_file_path}.idx"
    try:
        with open(index_path, 'rb') as f:
            return orjson.loads(f.read())
    except (orjson.JSONDecodeError, OSError):
        print(f"WARNING: rebuilt corrupt index {index_path}")
        build_jsonl_index(jsonl_file_path)
        with open(index_path, 'rb') as f:
            return orjson.loads(f.read())
```

It is named without a leading underscore because it is shared across modules
(`folderdb` already does `import jsonldb.jsonlfile as jsonlfile`).

### 3. Route every raw index read through `load_index`

Replace the `ensure_index_exists(...)` + raw `orjson.loads(f.read())` pairs with
a single `index = jsonlfile.load_index(path)` call at:

- `jsonlfile.py`: `select_jsonl` (range), `select_line_jsonl`, `update_jsonl`,
  `delete_jsonl`, and the maintenance/lint path — `lint_jsonl`'s fast path (`:203`)
  and full-path raw reads (`:219`/`:224`) plus `_verify_and_compact`'s reload
  (`:136`) — consolidating the existing inline rebuild-reread fragments into the
  one `load_index` helper so `lint_jsonl(force=True)` no longer hits a raw load.
- `folderdb.py`: `_scan_index_timespecs:164`, `delete_file_range:532`,
  `_make_meta_entry:571` (the last guarded by `if os.path.exists(index_file)`;
  preserve that guard — `load_index` is only called when an index read is
  actually intended).
- `visual.py`: all six `json.load` reads in `visualize_jsonl_bokeh`,
  `visualize_jsonl_matplot`, `visualize_folderdb_bokeh`,
  `visualize_folderdb_matplot` → `jsonlfile.load_index(jsonl_path)`. This both
  self-heals an empty/corrupt index and replaces the current "raise
  FileNotFoundError when missing" with build-from-`.jsonl` (a benign improvement,
  since the `.jsonl` is the source of truth). The keys returned by `load_index`
  are the same string linekeys these functions already iterate, so downstream
  numeric/datetime conversion is unchanged. The `if not os.path.exists(idx_path):
  raise FileNotFoundError` guards are removed — `load_index` makes a missing index
  recoverable. (Note: `visual.py` currently uses stdlib `json`; switching these
  reads to `load_index` removes that second, divergent index-loading strategy.)

Write paths (`update_jsonl`, `delete_jsonl`) still re-`orjson.dumps` the mutated
index back out unchanged; only the **read** half is swapped for `load_index`.

With these three modules routed through `load_index`, it is genuinely the single
index-read path in the library — no raw `orjson.loads`/`json.load` of an `.idx`
remains outside it.

### Failure semantics / blast radius

- Purely additive: the only behavior change is that a previously-fatal empty or
  corrupt index now triggers a rebuild instead of an exception. Valid indexes are
  untouched (no spurious rebuilds; the mtime fast-path is preserved).
- Shared-library change — affects every jsonldb consumer — but it can only turn a
  current hard crash into transparent recovery, so risk is one-directional.
- Companion oceandata-side change (isolate-per-ticker + surface failed tickers in
  `_flush_dirty_data`) is **out of scope** — tracked in the other repo.

## Non-Goals

- No change to the index file format or to `build_jsonl_index` itself.
- No change to the mtime fast-path / stale-rebuild logic (stays silent).
- No structured logging framework — match the existing `print("WARNING: …")`
  house style.
- No oceandata-side flush changes (separate repo).
- Not attempting to *repair* a partially-written index in place — always a full
  rebuild from the `.jsonl` source of truth.

## Success Criteria

1. Empty `.idx` (zero-length) + `upsert`/`select`/`update`/`delete`/meta-rebuild
   → index is rebuilt and the operation succeeds (no `orjson` crash).
2. Truncated/garbage non-empty `.idx` → same transparent rebuild-and-succeed via
   `load_index`.
3. A corruption-triggered rebuild prints exactly one `WARNING:` line naming the
   index path; a normal stale-mtime rebuild prints nothing.
4. Valid `.idx` → unchanged: no spurious rebuild, mtime fast-path intact, output
   byte-identical to today.
5. `.jsonl` newer than `.idx` → still rebuilds (existing behavior preserved).
6. The folderdb paths (`_scan_index_timespecs`, `delete_file_range`,
   `_make_meta_entry`) recover from a corrupt index instead of throwing.
7. The `visual.py` visualization paths recover from an empty/corrupt index
   instead of throwing, and no raw `orjson.loads`/`json.load` of an `.idx`
   remains anywhere outside `load_index`.
8. `lint_jsonl(force=True)` against a non-empty/garbage idx recovers via
   `load_index` instead of crashing at the raw `:219`/`:224` read.
9. Full existing test suite (82 tests) still passes.

## Testing Approach

TDD, mirroring the existing index/lint tests in `unit_tests/test_jsonlfile.py`
and `unit_tests/test_folderdb.py`:

- **Empty idx**: write a valid file, `open(path+'.idx','w').close()`, assert each
  entry point (select/update/delete/upsert/meta) succeeds and the idx is repopulated.
- **Corrupt idx**: write `b'not json'` (and `os.utime` to a fresh mtime so the
  mtime gate would otherwise trust it), assert `load_index` rebuilds and returns
  the correct dict; include a `lint_jsonl(path, force=True)` case so the
  maintenance full-path read is exercised against the garbage idx.
- **Warning emitted**: capture stdout, assert the `WARNING:` line appears for the
  empty and corrupt cases and is absent for a normal stale rebuild.
- **No false rebuild**: valid idx with fresh mtime — assert `build_jsonl_index` is
  not called (e.g. monkeypatch/spy) and the idx mtime is unchanged.
- **visual.py**: a unit test asserting the visualization functions no longer read
  the index directly — e.g. an empty/corrupt idx is healed (assert the functions'
  index-load step returns a rebuilt dict rather than raising). Heavy bokeh/matplot
  rendering need not be exercised end-to-end; the index-load grep/assertion plus a
  `load_index` call on a corrupt idx is sufficient to prove the second loader is
  gone.
- Run the full suite to confirm no regressions.
