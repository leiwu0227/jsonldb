# Strict Reads

Strict reads let a consumer fail when reading encounters damaged observations, instead of accepting the remaining observations. Strictness controls handling of encountered errors; it does not establish that an index represents every physical observation.

## Public contract

Public row-reading methods accept optional keyword-only `strict=False`, preserving existing positional arguments and default behavior. This applies to single-file full, range, and point reads, DataFrame reads, and FolderDB dictionary, DataFrame, and metadata-plus-rows reads. Adapters forward the option and propagate failures.

With `strict=True`, an encountered malformed JSON row, invalid keyed-row shape, unreadable indexed observation, or mismatch between the selected index key and the row's key raises `ValueError`. The diagnostic identifies the file or table, the byte offset or line location when available, and the reason. Filesystem failures retain their existing exception behavior.

A failed read returns no successful partial result. For a multi-table call, an encountered failure raises from the call; callers needing independent table outcomes issue separate reads.

## Scope of inspection

A full read examines all physical observation rows before [ordering its logical result](ordered_reads.md). Ordering must never substitute an indexed selection for that scan: doing so could hide damage previously omitted from the index. An indexed point or range read, including either one-sided bound, examines its selected rows. Missing keys and empty selections preserve their existing results; strictness does not require an observation to exist. Existing missing-table behavior is preserved at each API layer.

Blank tombstones and trailing-space padding remain normal representation. Sequential reads retain existing first-line metadata-slot classification and exclusion. Strictness adds no metadata-envelope interpretation, consumer schema validation, or datetime-key validation.

An index entry pointing to a blank tombstone or end of file cannot supply its selected observation and fails strict reading. This differs from ordinary sequential traversal past blank storage space.

## Index boundary and cost

Existing index loading, rebuilding, and cache behavior remain unchanged. Index rebuilding may skip damaged observations with a warning; strictness applies to observation retrieval, not the maintenance scan. Rows absent from the resulting index are unvisited by an indexed query and outside its guarantee, even when their omission resulted from corruption. This limitation also applies after a prior permissive read and with a warm cache.

Strict indexed reads require no additional whole-table validation scan or index-completeness evidence. Ordinary index recovery can still incur its existing scan cost. A successful strict indexed read establishes only that the observations it retrieved passed the read checks.

## Boundaries

Strict reads never repair or discard observation bytes. Repair remains explicit. The option adds no transactions, rollback, concurrent-read snapshot, or stronger power-loss guarantee. The default permissive policy remains available to all existing callers.

## Source targets

- `jsonldb/jsonlfile.py`: at most 950 lines in total, shared with the other file-store notes; row-reading policy and diagnostics.
- `jsonldb/jsonldf.py`: at most 150 lines in total; DataFrame propagation.
- `jsonldb/folderdb.py`: at most 1250 lines in total, shared with the other folder-database notes; dictionary, DataFrame, and metadata-plus-rows propagation.
