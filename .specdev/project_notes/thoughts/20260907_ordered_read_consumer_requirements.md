# Consistent read ordering for OceanData

Date: 2026-09-07. Handoff from OceanData's FolderData design review.
Reviewed JSONLDB checkout: `cf3ca1e`.

This note requests support for consistently ordered read results in JSONLDB. Implementation belongs in JSONLDB so OceanData does not repeat sorting at its storage boundary.

## Problem and example

Bounded JSONLDB reads return observations in serialized-key order. Full reads with both bounds omitted follow physical file order. Backfills and updates can therefore make equivalent requests return dictionaries with different iteration orders.

For example, a table initially contains June 6 and June 7. A later backfill appends June 5:

```text
Physical file: June 6, June 7, June 5
Full read:    June 6, June 7, June 5
Bounded read: June 5, June 6, June 7
```

OceanData uses canonical `YYYY-MM-DD` observation keys and dictionary reads. History and chain calculations need chronological order. An ordered result lets these consumers avoid depending on physical layout or sorting the same data again.

## Requested contract

When ordered reading is enabled, return each table's logical observations in ascending serialized-key order, whether bounds are omitted, one-sided, equal, or fully supplied. For OceanData's canonical date keys, that is chronological order.

Use JSONLDB's existing lexical key ordering rather than introducing a new interpretation of arbitrary keys. For example, string keys `"10"` and `"2"` retain lexical ordering. Ordering concerns observations inside each table, not the order of table names in a multi-table result.

Ordering must not change selected values, field presence, nulls, metadata, key deserialization, or missing-table behavior. Preserve existing duplicate-key resolution: full loading selects the last valid physical occurrence before ordering the resulting logical rows. Do not sort duplicate physical lines and accidentally change which value wins.

Expose consistent behavior through FolderDB dictionary reads and metadata-plus-rows reads. Corresponding single-file and DataFrame wrappers should preserve the selected ordering policy rather than independently sorting again.

## Public API choice

OceanData prefers consistent ordered reads owned by JSONLDB. Whether this becomes the default or an explicit keyword-only option should be settled in JSONLDB's implementation review, considering compatibility with callers relying on physical iteration order.

If an opt-in is chosen, a name such as `sort_keys=True` is illustrative; the final name/default is not prescribed here. OceanData will enable it consistently. Preserve existing positional argument compatibility and keep ordering independent of `strict`.

## Preserve strict-read semantics

A full read must still scan every physical observation row and apply the existing `strict` policy. After logical reconstruction, order the result.

Do not implement an ordered full read by substituting an unbounded indexed selection. An earlier permissive index rebuild may have excluded a damaged row; that substitution would prevent a full strict read from encountering it and weaken the recently agreed guarantee.

Point/range strict reads retain their existing selected-observation guarantee. Ordering does not add an integrity scan to indexed reads, repair data, or rewrite the physical file.

## Cost and verification

Sorting N keys costs O(N log N) and additional memory for keys/result ordering. Avoid deep-copying record values and avoid redundant sorting of already ordered indexed selections. A representative large full-read comparison should measure elapsed time and peak memory; no specific performance threshold is imposed by this handoff.

Focused acceptance checks:

1. Out-of-order backfills produce identical ascending logical order for full and equivalent bounded reads.
2. Full strict reads still raise on malformed rows omitted from an existing index.
3. Duplicate resolution, dictionary field presence/nulls, metadata, and key conversion remain unchanged.
4. Empty, single-row, one-sided, point, and multi-table reads behave consistently.
5. Wrappers preserve ordering; reads leave table bytes unchanged.

No new persistence format, background maintenance, transaction mechanism, or OceanData implementation is requested.
