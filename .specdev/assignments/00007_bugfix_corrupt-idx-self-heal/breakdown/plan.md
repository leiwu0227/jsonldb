# Implementation Plan: self-heal empty/corrupt `.idx` index files

**Execution mode:** inline
**Test budget (aggregate):** ≤ 5 new tests across the plan

Source of truth: `brainstorm/design.md`. Two coherent slices — (1) the core
loader + heal in `jsonlfile.py` and its own read sites, (2) routing the
`folderdb.py` and `visual.py` read sites through the same loader.

---

### Task 1: Core self-heal loader + jsonlfile read sites
**Mode:** full
**Skills:** [test-driven-development]
**Files:** jsonldb/jsonlfile.py, unit_tests/test_jsonlfile.py

**Work:**
- `ensure_index_exists`: add `os.path.getsize(index_file_path) == 0` as a rebuild
  trigger (between the missing-check and the mtime-check). Track a `corrupt` flag
  and `print(f"WARNING: rebuilt empty index {index_file_path}")` only for the
  size==0 case; stale-mtime rebuild stays silent.
- Add `load_index(jsonl_file_path) -> dict`: call `ensure_index_exists`, read +
  `orjson.loads`; on `(orjson.JSONDecodeError, OSError)` print
  `WARNING: rebuilt corrupt index {index_path}`, `build_jsonl_index`, re-read.
- Route jsonlfile read sites through `load_index`: `select_jsonl` (range, :451),
  `select_line_jsonl` (:521), `update_jsonl` (:565 read half), `delete_jsonl`
  (:638 read half), `lint_jsonl` fast/full reads (:203/:219/:224), and
  `_verify_and_compact`'s reload (:136). Write paths keep their existing
  `orjson.dumps` of the mutated index.

**Verify:**
- `python -m pytest unit_tests/test_jsonlfile.py -q`

**Test Budget:** +3 in unit_tests/test_jsonlfile.py; focused (<30s) — (a) empty idx
heals on select/update + emits warning, (b) garbage idx heals via
`lint_jsonl(force=True)`, (c) valid idx → no spurious rebuild (mtime unchanged,
no warning). Three because empty vs. garbage vs. no-false-rebuild are distinct
contracts that cannot share a fixture.

**Test Pruning:**
- Reuse existing index/lint fixtures in test_jsonlfile.py; replace any older
  ad-hoc empty-index test rather than adding alongside.

**Commit:** `git commit -m "fix: self-heal empty/corrupt .idx via load_index in jsonlfile"`

---

### Task 2: Route folderdb + visual.py reads through load_index
**Mode:** standard
**Skills:** []
**Files:** jsonldb/folderdb.py, jsonldb/visual.py, unit_tests/test_folderdb.py

**Work:**
- `folderdb.py`: replace raw `orjson.loads(f.read())` at `_scan_index_timespecs`
  (:164), `delete_file_range` (:532), `_make_meta_entry` (:571) with
  `jsonlfile.load_index(path)`. Preserve `_make_meta_entry`'s
  `if os.path.exists(index_file)` guard.
- `visual.py`: replace the six `json.load(f)` reads (`:64, :129, :228, :258,
  :342, :371`) with `jsonlfile.load_index(jsonl_path)`; remove the preceding
  `if not os.path.exists(idx_path): raise FileNotFoundError` guards (load_index
  builds from the `.jsonl`). Drop the now-unused stdlib `json` import if nothing
  else uses it.

**Verify:**
- `python -m pytest unit_tests/test_folderdb.py unit_tests/test_jsonlfile.py -q`
- text-only: `grep -n "orjson.loads(f.read())\|json.load(f)" jsonldb/folderdb.py jsonldb/visual.py` returns no `.idx` read sites.

**Test Budget:** +2 in unit_tests/test_folderdb.py; focused (<30s) — (a) corrupt
idx + meta-rebuild/`delete_file_range` recovers, (b) visual index-load on a
corrupt idx returns a rebuilt dict instead of raising. Two because folderdb and
visual are separate modules/entry points.

**Test Pruning:**
- Check test_folderdb.py for existing meta/timespec idx fixtures and extend them
  rather than duplicating setup.

**Commit:** `git commit -m "fix: route folderdb and visual index reads through load_index"`

---

## Final Verification
- `python -m pytest unit_tests/ -q` — full suite green (82 existing + new).
- `grep -rn "orjson.loads(f.read())\|json.load(f)" jsonldb/*.py` — no remaining
  raw `.idx` read outside `load_index`.
