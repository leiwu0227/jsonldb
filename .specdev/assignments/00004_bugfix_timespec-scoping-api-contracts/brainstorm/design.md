# Design: Scope timespec per database and fix read-path API contracts

## Overview

Three related defects, all verified against the current code:

1. **Global timespec contamination.** `FolderDB.__init__` sets `jsonlfile.TIME_SPEC` (module
   global) from `config.meta` (`folderdb.py:73`). Confirmed live: open DB-A (microseconds),
   then open fresh DB-B → DB-B inherits `microseconds` and *writes it to disk* in its own
   `config.meta`. Cross-instance state leak with on-disk persistence of the wrong value.
2. **`select_line_jsonl` contract.** Docstring/annotation say `Optional[str]`; the function
   returns `{}` on miss and a `DataDict` on hit. Its `auto_serialize` parameter is also used
   to decide key *de*serialization on the way out (`jsonlfile.py:557`).
3. **Duplicated deserialization.** The "if datetime-looking key, try `fromisoformat`, fall back
   to string" block appears in `load_jsonl`, `select_jsonl`, and `select_line_jsonl` (4 copies).

## Root Cause

The library predates multi-instance use: timespec was modeled as a process-wide constant, and
`FolderDB` repurposed it as per-database config by mutating the module global at init time.
`_is_datetime_string` (exact length 19 vs 26) and `serialize_linekey`
(`isoformat(timespec=...)`) both read the global, so whichever database initialized last wins.
The `select_line_jsonl` issues are simple drift between docstring, annotation, and body.

## Fix Design

**Thread an optional `timespec` keyword through `jsonlfile`, defaulting to the global:**

- `serialize_linekey(linekey, timespec=None)` and `_is_datetime_string(linekey, timespec=None)`
  use `timespec or TIME_SPEC`. Module global stays as the default for direct callers —
  no behavior change for existing code.
- Public read/write functions gain the same trailing keyword and pass it down:
  `save_jsonl`, `load_jsonl`, `select_jsonl`, `select_line_jsonl`, `update_jsonl`,
  `delete_jsonl` (key serialization), plus `jsonldf` wrappers (`save_jsonldf`, `load_jsonldf`,
  `select_jsonldf`, `update_jsonldf`, `delete_jsonldf`) as pure pass-through.

**Make `FolderDB` hold instance state:**

- `self.timespec` = value from `config.meta` if present, else `jsonlfile.TIME_SPEC` default.
- Delete the `jsonlfile.TIME_SPEC = ...` mutation (`folderdb.py:73`).
- `build_configmeta` writes `self.timespec`; the `__init__` rewrite-check compares against
  `self.timespec`. The duplicated `config.meta` read in `__init__` collapses into one block.
- All `FolderDB` methods that call jsonlfile/jsonldf read/write functions pass
  `timespec=self.timespec` where keys are serialized or deserialized.

**Fix `select_line_jsonl` contract:**

- Return type annotation `DataDict`; docstring states "single-record dict, `{}` if not found".
- Keep the `auto_serialize` parameter name (backward compatibility) but document that it
  controls both input-key serialization and output-key deserialization; no behavior change.

**Extract one helper:**

- `_store_with_key(result_dict, linekey, value, auto_deserialize, timespec)` (private, in
  `jsonlfile.py`): performs the datetime-detect/deserialize/fallback and assignment. The four
  duplicated blocks in `load_jsonl`, `select_jsonl`, `select_line_jsonl` call it.

## Key Decisions

- **Optional kwarg + global default, not removal of the global:** removing `TIME_SPEC` breaks
  direct `jsonlfile` users (a documented use mode in `__init__.py`); defaulting preserves
  byte-for-byte behavior when the kwarg is omitted.
- **No rename of `auto_serialize`:** renaming is an API break disproportionate to the bug; the
  contract fix is the return type, docstring, and consistent miss value.
- **Helper lives in `jsonlfile.py` as a private function** — keeps the modules' dependency
  direction unchanged (folderdb → jsonlfile, never the reverse).

## Success Criteria

- New tests pass:
  1. Open a microseconds DB, then a fresh DB → fresh DB's `config.meta` says `seconds` and
     `jsonlfile.TIME_SPEC` is unchanged (`seconds`).
  2. Two DBs with different timespecs in one process round-trip datetime keys correctly
     (microsecond keys survive in the microseconds DB while the seconds DB still works).
  3. `select_line_jsonl` returns a one-record dict on hit and `{}` on miss (typed contract).
- Full existing suite passes unchanged (the kwarg defaults guarantee old call sites behave
  identically).
- `grep` confirms no remaining assignment to `jsonlfile.TIME_SPEC` outside its definition.

## Testing Approach

TDD: the contamination test (converted from the verification script) goes RED first against
current code; then thread the kwarg bottom-up (jsonlfile → jsonldf → folderdb) and turn it
GREEN. Contract/helper changes are covered by the third test plus the existing suite.
