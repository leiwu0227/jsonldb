# Ordered Reads

Read results represent a table's logical key order independently of its physical layout. Backfills, growing updates, and lint may rearrange physical rows without changing their logical identity. Supplying bounds changes selection, not ordering.

## Default contract

Every row read returns observations within each table in ascending serialized-key order. This is the default behavior; there is no ordering flag or physical-order mode. Existing positional arguments remain valid, and ordering is independent of `strict`.

Ordering uses the same lexical string comparison as indexed selection. Canonical `YYYY-MM-DD` keys therefore sort chronologically, while arbitrary strings such as `"10"` and `"2"` retain lexical order. Serialized keys determine order even when returned keys are deserialized; preserve existing key conversion and duplicate winners.

The contract covers single-file dictionary reads, DataFrame reads, FolderDB dictionary/DataFrame reads, and metadata-plus-rows variants. Wrappers preserve the file-store result order without sorting again. It does not impose ordering on table names in multi-table results or on record fields and DataFrame columns.

## Full reads

When both bounds are omitted, scan every physical observation row sequentially. Apply the existing strict/permissive policy, skip normal tombstones and first-line metadata slots, and reconstruct logical observations. For duplicate serialized keys, the last valid physical occurrence supplies the value. Ordering must not change that resolution or cause an earlier malformed occurrence to escape strict handling.

Check the reconstructed logical key sequence for ascending order using adjacent comparisons. If already ordered, retain its order without sorting or rebuilding the result solely for ordering. Otherwise, stop checking at the first inversion, sort the logical keys, and construct the ordered result while reusing record values. Perform the comparison using serialized keys rather than mixing deserialized key types.

The check uses loaded keys, with no second file read, persistent sorted flag, or validation cache. A linted table normally takes the already-ordered path; later writes may disturb its order. Reads never lint or rewrite the physical file to obtain ordered output.

## Indexed reads

Any supplied bound retains the existing indexed path. A start-only query ends at the index's last key; an end-only query starts at its first key. Both are inclusive. Equal bounds use the point path; direct point reads contain at most one observation.

Indexed ranges select from sorted index keys, read selected rows in physical-offset order for efficient access, and return them in ascending key order. They need no additional whole-table order check or result sort. Existing index recovery and caching remain unchanged.

## Strictness and data fidelity

A full strict read still encounters every physical observation, including damage absent from an existing index. Never replace that scan with an unbounded indexed selection. Indexed strict reads retain their selected-observation guarantee; previously excluded damage remains outside it, as defined by [Strict Reads](strict_reads.md).

Ordering preserves selected values, field presence, nulls, metadata, metadata-before-rows reading, key deserialization, and missing/empty results. It adds no repair, integrity verification, transaction, or persistence format.

## Cost and compatibility

For N reconstructed logical keys, the ascending check takes O(N) comparisons and O(1) additional space. If reordering is needed, sorting takes O(N log N) comparisons in the general case, plus O(N) references and dictionary entries; record values are not deep-copied. The existing sequential scan is required regardless. Indexed selection costs are unchanged.

Full reads intentionally stop exposing physical iteration order. Measure elapsed time and peak memory for already sorted, lightly backfilled, and shuffled representative tables, including the already-ordered path. No universal performance threshold is promised.

## Source targets

- `jsonldb/jsonlfile.py`: at most 950 lines in total, shared with the other file-store notes; logical reconstruction and ordered results.
- `jsonldb/jsonldf.py`: at most 150 lines in total; preserve result order through DataFrame conversion.
- `jsonldb/folderdb.py`: at most 1250 lines in total, shared with the other folder-database notes; preserve per-table order through getters.
