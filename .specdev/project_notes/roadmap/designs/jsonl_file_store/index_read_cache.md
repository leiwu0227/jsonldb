# Index Read Cache

Repeated point and range reads should reuse the parsed index while its files remain unchanged. Loading an entire index for every small query makes the query cost grow with the table even when it returns one row. A private cache removes that repeated work without changing the public interface, storage format, or recovery guarantees.

The cache belongs to the file store. Folder and DataFrame consumers benefit through their existing selectors without managing cache state. Each process owns its cache; nothing additional is persisted.

## Ownership

An entry retains a key-to-offset dictionary and, when range reads need it, an immutable ordered key sequence. It contains no row values or open file handles. Key conversion and row validation remain the responsibility of each read, preserving ordering, datetime behavior, metadata-slot invisibility, and warnings.

Cached indexes are private read snapshots. Public index loads and writers continue receiving independent mutable dictionaries. Read hits neither copy the dictionary nor reconstruct an already retained key sequence. Writers do not populate the cache from their mutable indexes; a read following a write may reload it.

## Trust and recovery

Every hit verifies both the table and its index using file identity, size, and modification/change timestamps. Changed, missing, or unverifiable files invalidate reuse. A time-to-live is insufficient because it permits avoidable stale reads; hashing whole files would defeat the optimization.

Cache misses share the authoritative loading and recovery implementation described in [Index Integrity and Lint](index_integrity_and_lint.md). The cache introduces no second repair policy. A successful load can supply both its parsed index and the evidence needed to retain it, including after repair.

Admission requires stable files around the final successful read and no intervening local invalidation. The loaded index must belong to the file identity being admitted; a replacement during loading cannot attach old offsets to a new fingerprint. When stability is uncertain, return the ordinary loader result without retaining it. Do not add unbounded retries, cache failures, or separately retain missing-key results.

Metadata checks do not guarantee detection of arbitrary edits with indistinguishable filesystem metadata. They also do not create transactional snapshots during concurrent writes. The existing single-writer and non-transactional-read model remains the boundary.

## Mutation and coordination

The file store invalidates affected entries before modifying table or index bytes, so failures cannot leave an entry trusted. This includes ordinary writes, index rebuilding, compaction, slot migration, and metadata-only changes. Invalidation only after successful publication is insufficient. Publication ordering and durability remain unchanged.

Folder operations need no cache-specific behavior. Required file checks detect direct deletion, movement, and replacement before reuse. Entries for paths no longer present may remain retained until lookup or eviction, within the memory budget.

Cache bookkeeping is synchronized briefly; disk reads, parsing, and memory sizing occur outside that synchronization. An invalidated in-flight load cannot repopulate an entry. Concurrent cold readers may duplicate loading rather than require additional coordination. Lint continues its existing verification independently of cached reads, and diagnostic reporting remains intact.

## Bounded memory and performance

Retained entries share a bounded internal memory budget with least-recently-used eviction. Account for dictionary storage, keys, offsets, and any retained key sequence. Size entries on admission rather than on every hit. An oversized index still serves the current read but is not retained; optional range-key storage must also fit the budget.

The budget limits retained cache objects, not total process memory or temporary allocations. Its default must be calibrated against representative table sizes and working sets; no new public setting is required. A fixed entry count cannot provide a meaningful memory bound.

The target is at least a twofold improvement for repeated small indexed reads whose working set fits the budget. Cold admission, reads after writes, and working sets exceeding the budget must be assessed separately; warm performance alone is insufficient. Full-table reads retain their sequential path, and no blanket speedup is promised.

## Source target

- `jsonldb/jsonlfile.py`: at most 950 lines in total, shared with the file-store, index-integrity, and metadata-slot notes.
