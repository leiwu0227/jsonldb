# Design: Auto-detect and heal timespec/data mismatches at FolderDB open

## Overview

A database written under the pre-00004 contamination bug can carry a `config.meta` timespec
that disagrees with the actual precision of its datetime keys. Keys then load as strings
instead of `datetime` objects. The fix detects the data's real precision when the database is
opened and self-heals the configuration.

## Goals

- Opening a mismatched database "just works": datetime keys deserialize correctly and
  `config.meta` is corrected on disk, with a visible warning.
- Zero cost for healthy databases beyond one `db.meta` read (already loaded data).
- No behavior change for direct `jsonlfile` users.

## Non-Goals

- **No read-side fallback in `_store_with_key`/`_is_datetime_string`.** Recovering a
  26-char key as a datetime in a seconds-configured DB and later re-serializing it at
  seconds precision would write it back under a *different* key (truncated micros),
  turning an update into a divergent insert. Detection stays strict; the heal happens once,
  at open, where reads and writes can be switched together.
- **No per-table timespec.** Mixed precisions across tables get a warning and keep the
  configured value; supporting heterogeneous precision is a separate design.
- **No data-file (`.jsonl`) scans, and no index scans for healthy databases.** Stage 1 uses
  only `db.meta` key ranges; the full `.idx` key scan (stage 2) runs solely when stage 1 has
  already found a boundary mismatch.

## Design

**`jsonlfile.detect_timespec(linekey: str) -> Optional[str]`** (new helper):
returns `'seconds'` / `'microseconds'` when the string is shape-matched by
`_is_datetime_string` for that precision *and* `datetime.fromisoformat` parses it;
`None` otherwise. Single source of truth for "what precision is this key".

**Two-stage detection** (cheap trigger, full confirmation — healing never fires on a
boundary sample alone):

*Stage 1 — trigger (cheap):* `FolderDB._detect_data_timespec(self) -> Optional[str]` reads
`db.meta` (if present) and runs `detect_timespec` over every entry's `min_index`/`max_index`.
It returns a candidate precision only when at least one datetime-like boundary key disagrees
with `self.timespec`. Healthy and string-keyed databases stop here at zero extra I/O.

*Stage 2 — confirmation (full scan, contaminated DBs only):*
`FolderDB._scan_index_timespecs(self) -> set` reads **every key of every `.idx` file** and
collects the set of detected precisions. The heal proceeds only when the scan finds exactly
one precision and it differs from the configured value:

```python
candidate = self._detect_data_timespec()          # stage 1: boundary keys only
if candidate and candidate != self.timespec:
    found = self._scan_index_timespecs()          # stage 2: all index keys
    if found == {candidate}:
        print(f"WARNING: config.meta timespec '{self.timespec}' does not match "
              f"data ('{candidate}'); auto-correcting config.meta")
        self.timespec = candidate
        self.build_configmeta()
    elif len(found) > 1:
        print(f"WARNING: mixed datetime key precisions {sorted(found)} found in "
              f"{self.folder_path}; keeping timespec '{self.timespec}'")
```

This runs after the existing `db.meta` build/refresh block in `__init__`, so `db.meta`
exists and is current for fresh folders.

**Documented limitation (boundary camouflage):** a table whose `min_index`/`max_index`
match the configured precision but which contains *interior* keys of the other precision
(e.g. `T00:00:00`, `T00:30:00.123456`, `T01:00:00` under a `seconds` config) does not
trigger stage 1, so no scan, no heal, and no warning occur. Those interior keys continue to
load as strings — exactly the pre-guard behavior; the guard never rewrites configuration in
this case and never makes anything worse. Detecting it would require a full index scan on
every open of every healthy database, which contradicts the zero-cost-when-healthy goal.
A test pins this limitation explicitly.

## Key Decisions

- **Two-stage detection:** boundary keys from `db.meta` are the zero-I/O trigger; a full
  index-key scan confirms uniform precision before any heal. Healing on a boundary sample
  alone could mis-convert a mixed table (review finding F1.1); always-scanning would make
  every open O(total keys). The scan cost lands only on databases that already show a
  boundary mismatch — exactly the contaminated ones being repaired, once.
- **Heal persistently (rewrite `config.meta`)** rather than only in-memory: otherwise every
  open re-detects and other processes opening the folder stay broken.
- **Mixed precision → keep configured + warn:** any automatic choice silently breaks the
  other tables; surfacing the conflict is the only safe move.
- **Warning via `print`:** consistent with the library's existing diagnostics.

## Success Criteria

- Heal test: a DB whose `config.meta` says `seconds` but whose table uniformly contains
  microsecond-precision keys opens with `db.timespec == "microseconds"`, `config.meta`
  rewritten to `microseconds`, and `get_dict` returns microsecond `datetime` keys.
- Mixed-precision test: a table containing both precisions **with a mismatched boundary key**
  (stage 1 triggers, stage 2 finds both) keeps the configured timespec, leaves `config.meta`
  untouched, and warns.
- Limitation test (pins the documented narrowing): a table whose boundary keys match the
  config but with an interior other-precision key opens unchanged — no heal, no `config.meta`
  rewrite, interior key loads as a string.
- Healthy databases (matching config, string-keyed, or empty) open with no warning and no
  `config.meta` rewrite (existing `test_init_does_not_rewrite_configmeta_if_unchanged`
  stays green).
- Full suite passes.

## Testing Approach

TDD: write the mismatch-heal test first (construct the contaminated state directly with
`save_jsonl(..., timespec='microseconds')` plus a `seconds` `config.meta`), watch it fail,
then add `detect_timespec`, `_detect_data_timespec`, `_scan_index_timespecs`, and the
`__init__` hook. The mixed and limitation tests follow the same construction pattern.
