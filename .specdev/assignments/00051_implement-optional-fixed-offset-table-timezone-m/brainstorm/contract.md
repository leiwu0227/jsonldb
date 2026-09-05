# Assignment contract

Kind: change

## Objective and context

Implement optional fixed-offset table timezone metadata and consistent datetime key handling against baseline `f05791e`. Authority is the published design set at that revision: `jsonl_file_store/table_timezone.md`, `jsonl_file_store/metadata_slot.md`, `jsonl_file_store.md`, and `folder_database/metadata_and_timespec.md` under `.specdev/project_notes/roadmap/designs/`.

The bounded knowledge search for `metadata timezone` identified Assignment 00050's outcome. Preserve its validated-key reuse, eager DataFrame conversion, supported-version fallbacks and per-table statistics behavior. Preserve the unknown-envelope fix in `de7217f`. Concurrent Discussion artifacts remain outside delivery ownership.

## Scope and non-goals

Implement explicit timezone access for existing slotted tables, envelope preservation, and table-aware key handling through low-level dictionary operations, DataFrame adapters, FolderDB single/plural writes, reads, lookups, ranges and deletes. Include ordinary lint, slot migration, restart and invalid-input behavior. Provide concise consumer documentation for the new API.

Exclude named geographic zones, daylight-saving rules, automatic timezone inference or conversion between offsets, automatic timezone attachment on reads, populated-table timezone migration, historical `_meta` envelope-version compatibility work, broadening existing datetime recognition, unrelated roadmap gaps and roadmap publication. Populated-table migration remains a separate explicit future operation; this assignment enforces refusal rather than silently relabeling data.

## Expected behavior

The optional version-1 envelope field `_meta.timezone` is independent of `_meta.data`. It describes every datetime linekey in one table. Absence means unspecified and preserves existing undeclared-table behavior. Consumer metadata may independently contain a key named `timezone`; it is not interpreted or reserved.

Add `FolderDB.read_timezone(name)` and `FolderDB.set_timezone(name, timezone)`, with corresponding `jsonlfile.read_jsonl_timezone(path)` and `jsonlfile.write_jsonl_timezone(path, timezone)` entry points. Reads return the canonical string or `None` when no declaration or file exists; a malformed known-version declaration is an error. Setters require an existing known-version slotted table. They do not implicitly create a table or enable slots. Setting or removing a declaration requires an empty table; setting the same normalized offset is an idempotent no-op even when populated. Unknown-version slots are not interpreted or modified by these setters. Existing metadata APIs continue exposing only consumer data.

Accept signed `H:MM` or `HH:MM` numeric offsets, with hours 0 through 23 and minutes 0 through 59, and the spelling `UTC`. Canonical storage is signed `HH:MM`: `+8:00` becomes `+08:00`; `UTC` and signed zero become `+00:00`. `None` requests removal. Reject other spellings, types and out-of-range values before mutation; do not infer a geographic zone. Missing files raise `FileNotFoundError` on setters; invalid declaration, slot state or populated-table changes raise `ValueError`, with wrong argument types raising `TypeError`.

On declared tables, naive datetime keys and matching offset-free timestamp strings represent local time in the declared offset. Aware datetime/Timestamp keys and offset-bearing ISO timestamp strings must match that offset and serialize without a suffix at the selected seconds/microseconds precision. Conflicting offsets and malformed datetime-like offset suffixes are rejected before mutation. Lookup, range and deletion keys use the same normalization, including equal-bound fast paths. Arbitrary string keys remain strings. Normalization preserves existing physical-row and effective-index collision behavior rather than silently pre-collapsing input rows.

Read deserialization remains precision-specific: 19-character seconds or 26-character microseconds keys become naive datetimes. `auto_deserialize=False` returns stored strings. Timezone changes neither row values nor the database timespec. Undeclared tables retain baseline handling of aware inputs and offset-bearing string keys.

## Important decisions

- Retain the published version-1 envelope extension. Replace/clear of consumer data, overwrite/upsert, slot resizing and lint preserve timezone independently, including timezone-only slots. Width checks account for the complete envelope.
- All mutation entry points must inspect and enforce a declared timezone, including low-level bypasses of FolderDB. The atomic control-file writer must not silently erase timezone if used on a declared table; safely reject that unsupported use before replacement if it cannot honor the table contract.
- Establish or retain the timezone declaration before publishing rows that depend on it, including overwrite placeholders. Consumer metadata retains its existing publication-after-rows order. Invalid input and undersized slots leave table, index and configuration bytes unchanged; this is preflight safety, not a new transaction guarantee.
- Historical jsonldb callers and pre-`_meta` databases remain supported. Existing callers require no argument changes, timezone setting, metadata enablement or file migration. Files without metadata slots retain their format and key semantics; this feature must not insert slots, infer timezones, normalize old keys or impose timezone validation unless the caller explicitly opts in. Existing configured metadata behavior is unchanged.
- The user confirms there are no legacy `_meta` consumers. Only historical metadata-envelope migration and compatibility of older binaries with the new `_meta.timezone` field are excluded. This exclusion does not waive compatibility with pre-`_meta` jsonldb callers or files. Use the published version-1 envelope directly.
- Recognizable malformed timezone declarations must not be silently discarded by writes or repair. Preserve recoverable bytes and report refusal. A completely erased declaration is indistinguishable from legitimate absence; document that recovery limitation rather than guessing a timezone or claiming detection of all loss.

## Constraints and invariants

Keep existing public signatures, return shapes, consumer metadata ownership, dependencies and supported Python/pandas declarations. Only the four additive timezone accessors above are authorized public API additions. Preserve unrelated storage/index bytes, row ordering, append/tombstone behavior, cache invalidation, per-table metadata upkeep and plural-operation partial completion. Validation added for declared tables must not change undeclared-table behavior. No global concurrency, fsync or power-loss guarantee is introduced.

Honor all published total physical source-file caps, counting blank lines, comments and docstrings: `jsonldb/metaslot.py` <= 250, `jsonldb/jsonlfile.py` <= 950, `jsonldb/jsonldf.py` <= 150 and `jsonldb/folderdb.py` <= 1250. Readable local consolidation and cohesive private helpers are delegated; any new private module must have an explicit maximum of 250 physical lines recorded in the implementation design. No minification, unrelated relocation or cap increases. No normal Assignment worktree.

## Delegated and reserved authority

After exact contract approval, delegate implementation design, private helper structure, focused verification and supported-runtime checks, independent implementation review and repairs, user-facing API documentation and one final delivery commit. Brainstorm review is optional; implementation review is required, using Claude Opus 5.0 at xhigh effort in accordance with the established review preference.

Reserve populated-table migrations, named-zone support, different serialization/deserialization semantics, public API expansion beyond the four accessors, changes to envelope-version policy, relaxed source caps, full-suite execution and acceptance of material unresolved regressions for the user.

## Risks and assumptions

Timezone is a declared interpretation of naive keys, not evidence of their original provenance. Fixed offsets avoid daylight-saving ambiguity; named zones remain excluded. Completely erased timezone metadata cannot be distinguished from intentional absence. New validation and slot reads must preserve existing undeclared-table fast paths. Historical jsonldb consumers and unslotted databases are numerous and are a required compatibility case; there are no legacy `_meta` consumers to support.

## Verification authority

Focused tests and temporary databases are authorized, including failure injection at relevant publication boundaries and representative available supported pandas runtimes. Representative pre-`_meta` file fixtures and existing-call-pattern comparisons against `f05791e` are required to verify legacy file/API compatibility. Read-only baseline comparisons and bounded same-host timing measurements may verify that undeclared tables retain the delivered fast paths. No package dependency changes or full-suite run are authorized here. Report unavailable runtime coverage and measured limitations honestly.

## Acceptance criteria

- AC-1: The four accessors implement canonical offset normalization, separate timezone/data ownership, empty-table declaration/removal and populated-table idempotence/refusal. Cover signed and zero offsets, limits, invalid strings/types, absent/unslotted/unknown-version/malformed slots, missing files, slot enablement, restart persistence and complete-envelope fit checks. Invalid requests leave existing files and settings unchanged.
- AC-2: Dictionary/DataFrame and FolderDB single/plural writes, full/point/range reads and key/range deletes agree on declared-table key interpretation at both precisions. Verify matching/conflicting aware inputs and ISO strings, naive inputs, mixed arbitrary strings, normalization collisions, equal/open bounds, offsets on opposite sides of UTC and caller-input preservation. Returned keys remain naive or strings according to existing deserialization settings. Undeclared tables and atomic control-file behavior retain baseline compatibility, including custom-key and supported pandas fallbacks.
- AC-3: Timezone survives consumer-data replacement/clearing, row overwrites/upserts, lint, resizing and restart, with correct row bytes, offsets, cache behavior and metadata statistics. Invalid offsets/rows and undersized slots fail before mutation. Failure injection confirms rows are never intentionally published behind a timezone-free placeholder and consumer metadata retains its established ordering. Recognizable malformed declarations are preserved and refused rather than erased. Documentation states the wholly-lost-declaration limitation explicitly; older-binary compatibility with the timezone envelope is not required; pre-`_meta` file/API compatibility is required under AC-5.
- AC-4: Final source files satisfy every applicable physical-line cap, including the four named caps and any new private module's recorded cap. Record counts after review repairs and before delivery; any exceeded limit blocks completion. Focused regression evidence covers existing unknown-envelope preservation and relevant serialization, durability, cache, metadata, DataFrame and validated-key reuse behavior. No material unresolved regression or performance loss is accepted silently.

- AC-5: Existing pre-`_meta` jsonldb callers and unslotted databases work without code changes or migration. Exercise representative historical files and dictionary/DataFrame/FolderDB call patterns across open/reopen, save/upsert, full/point/range reads, key/range deletes and lint. Verify existing signatures/defaults/return shapes and baseline key serialization/deserialization, including naive/aware datetime inputs, offset-bearing strings, custom keys and both precisions. Normal operations on databases without slot enablement keep files unslotted and do not infer timezones or impose new offset validation. New timezone reads report no declaration; setters refuse unslotted files without changing them. Explicit slot enablement and timezone configuration remain opt-in. Record compatibility evidence separately from timezone-feature evidence.
