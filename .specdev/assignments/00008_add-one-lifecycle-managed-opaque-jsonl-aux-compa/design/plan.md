# Implementation plan

**Implementation Guides:** [api-security]
**Review Guides:** [api-security]

## Tasks

1. **T-1 — Add the canonical opaque companion API (AC-1).**
   Add low-level JSONL helpers and `FolderDB` methods for deriving the sole
   `<ticker>.jsonl.aux` path, reading exact bytes, atomically replacing payload
   bytes from a same-directory temporary file, and idempotently removing the
   optional companion. Validate the public byte payload and JSONL-owner
   boundary, require an owner before publishing, and keep ticker discovery
   unchanged.
2. **T-2 — Integrate companion lifecycle ordering (AC-2).**
   Invalidate an existing companion before every JSONL content mutation,
   compaction, overwrite, deletion, or clear operation. Move the owner first
   and its companion last during hierarchy organization and invalid-ticker
   repair so an interruption can produce an absent companion but cannot attach
   stale content to changed or moved data.
3. **T-3 — Report conservative orphans and verify compatibility (AC-1, AC-2,
   AC-3).**
   Make low-level and folder lint paths report orphan `.jsonl.aux` files without
   deleting, attaching, parsing, or synthesizing them. Add focused tests for
   canonical byte-preserving operations, atomic publication behavior,
   discovery exclusion, mutation/deletion/clear invalidation, hierarchy moves,
   interruption ordering, orphan reporting, and unchanged JSONL/index/metadata
   behavior; run only the affected test modules.
