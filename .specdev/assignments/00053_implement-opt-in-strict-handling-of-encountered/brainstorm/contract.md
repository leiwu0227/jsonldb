# Assignment contract

Kind: change

## Objective and context

Implement the published [Strict Reads](../../../project_notes/roadmap/designs/jsonl_file_store/strict_reads.md) contract from commit `794cbf9`: let consumers opt into exceptions for encountered observation-read errors while retaining permissive defaults and indexed query performance.

The published design and the user's subsequent agreement with OceanData govern scope. The original consumer handoff requested broader completeness guarantees that were explicitly dropped; it is not implementation authority.

## Scope and non-goals

Add keyword-only `strict=False` to single-file full, range, and point row reads, DataFrame load/select, and FolderDB dictionary, DataFrame, and metadata-plus-rows reads. Update public usage/API documentation and focused regression coverage.

Do not add whole-table prevalidation for indexed queries, index-completeness verification or provenance, a validation cache, metadata-envelope strictness, schema validation, repair, transactions, or new dependencies. Existing index maintenance remains unchanged.

## Expected behavior

With `strict=True`, encountered malformed JSON, invalid keyed-row shapes, unreadable indexed observations, and selected-index-key mismatches raise `ValueError` with file/table identity, a byte offset or line location when available, and an informative reason. Filesystem errors retain their existing behavior. A failed read returns no successful partial result; errors propagate from multi-table calls and adapters.

Full reads inspect all physical observation rows. Indexed reads inspect their selections, including equal-bound queries. Unvisited rows, including damage previously excluded by permissive index construction, are outside the guarantee. Existing index rebuilding may still scan and skip damage with warnings. Empty selections do not prove completeness.

Default calls and explicit `strict=False` retain existing behavior and positional compatibility. Missing keys/tables preserve each API's existing results or exceptions. Sequential reads skip tombstones and retain first-line slot classification; an indexed selection pointing to a tombstone or EOF cannot supply its observation and fails in strict mode. Padding, metadata ordering, key conversion, and output ordering retain their existing semantics.

## Important decisions

Strictness belongs to each row-read call and is forwarded through wrappers. It does not change index loading, rebuilding, cache admission, or cache invalidation. Public documentation must state the limitation concerning previously omitted damaged rows.

## Constraints and invariants

Preserve observation bytes during successful and failed reads. Retain Python 3.8 compatibility and the published total source caps: `jsonlfile.py` 950, `jsonldf.py` 150, and `folderdb.py` 1250 lines. Keep the implementation within existing modules; bounded simplification to meet caps is allowed. Published designs and unrelated working-tree artifacts remain untouched.

## Delegated and reserved authority

- Delegated after approval: implementation, bounded simplification, documentation, focused tests, required independent implementation review and in-scope repairs, and the standard final delivery commit.
- Reserved: changes to the agreed guarantee, public defaults, source caps, dependencies, or published designs. No full-suite authorization is implied.

## Risks and assumptions

Index rebuilding can omit damage before selection; this is an accepted limitation, including after permissive reads and with warm caches. No concurrent-read snapshot is promised. Prior read-shape and cache behavior are documented in Assignment 00044 and 00046 outcomes; those are historical implementation context, subordinate to the current design.

## Verification authority

Approval authorizes focused strict-read regressions and affected file-store, DataFrame, FolderDB, metadata, and cache tests, source-cap checks, Python 3.8 syntax checks, and diff checks. Implementation review is required using the configured reviewer. Do not run the full suite without separate authorization.

## Acceptance criteria

- AC-1: Full, range, and point strict reads reject encountered malformed/invalid rows and unreadable or mismatched selected observations with useful location-bearing diagnostics, without modifying observation bytes or returning a partial result.
- AC-2: Every specified wrapper forwards strictness and propagates failures, including metadata-plus-rows and multi-table reads; default/false behavior, positional calls, missing/empty results, slots, tombstones, padding, and ordering remain compatible.
- AC-3: Indexed strict reads perform no additional whole-table validation and do not reject unselected damage. Missing/rebuilt and warm cached indexes retain existing handling; regressions and public documentation explicitly establish that previously excluded damage remains outside the guarantee.
