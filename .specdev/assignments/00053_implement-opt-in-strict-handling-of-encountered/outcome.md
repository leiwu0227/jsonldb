# Outcome

## Delivered behavior

Row reads now accept keyword-only `strict=False`. Strict full, range, and point reads raise location-bearing `ValueError` on encountered malformed/invalid observations, selected-key mismatches, or unreadable selected offsets. DataFrame and FolderDB getters, including metadata-plus-rows, forward the option and propagate failures. Defaults, positional arguments, missing results, metadata classification, and existing index/cache handling remain unchanged.

Usage and API documentation explicitly distinguish encountered-row validation from completeness: indexed reads cannot detect damaged rows previously excluded by index rebuilding. No additional whole-table scan is introduced.

## Deviations

None. Redundant source comments and a slot-migration docstring were shortened to retain the 950-line file-store cap. Initial test-fixture failures were corrected: serializer-representable oversized offsets now exercise read validation, and missing range reads retain their existing OSError behavior.

## Unresolved risks

The agreed limitations remain: index rebuilding may omit damage, and unvisited observations are outside indexed strict reads. Concurrent reads are not transactional snapshots. Source caps remain binding: file store 950/950, adapter 145/150, FolderDB 1104/1250.

| Acceptance | Evidence | Result |
| --- | --- | --- |
| AC-1 | Strict regression cases cover malformed JSON, invalid shapes, UTF-8 damage, key mismatch, tombstone/EOF selections, invalid selected offsets, diagnostics, unchanged observation bytes, and explicit repair followed by success; within 509 passing focused tests. | Passed |
| AC-2 | Focused coverage verifies all nine public signatures, positional calls, wrapper and multi-table failure propagation, missing/empty results, padding/slots/tombstones, key ordering and datetime conversion; existing read/metadata/timezone cases remain passing. | Passed |
| AC-3 | Missing, permissively rebuilt, and warm indexes preserve the omission boundary; warm-read guards prohibit a full load, rebuild, or index reload and count only selected row parses. Public API/usage explain the same limitation. Static checks verify caps, Python 3.8 syntax, whitespace, and unchanged designs. | Passed |
