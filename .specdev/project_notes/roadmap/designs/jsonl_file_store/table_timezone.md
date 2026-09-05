# Table Timezone

A table may declare one timezone for all its datetime linekeys. The declaration gives otherwise offset-free timestamps a shared interpretation while preserving fixed-width keys and their textual ordering. It describes keys only; datetime values inside records remain consumer-owned.

## Ownership and representation

The optional `timezone` field belongs to the library-owned metadata envelope, alongside `v` and `data`. Consumer metadata remains unrestricted inside `data`. A table can have a timezone without a consumer record.

For example, the envelope can contain `"v": 1`, `"timezone": "+08:00"`, and `"data": {"source": "market-feed"}`. Input `+8:00` normalizes to `+08:00`. The initial representation is a fixed UTC offset in signed `HH:MM` form; UTC normalizes to `+00:00`. Invalid offsets are rejected. An offset is never used to infer a geographic timezone.

Fixed offsets have no daylight-saving transitions. Named geographic zones require a separate policy for repeated and nonexistent local times; their support is outside this initial design.

## Key interpretation

Every datetime key in a timezone-declared table represents local time at that offset. Stored keys retain exactly 19 characters for seconds or 26 for microseconds, without an offset suffix. Precision remains a database setting; timezone is a table property, so separate tables may declare different offsets.

Naive datetime inputs and matching timestamp strings are interpreted in the declared timezone. A timezone-aware input must have the declared offset; conflicting offsets are rejected before mutation rather than silently converted. Matching aware inputs serialize as local timestamp text without the redundant offset. Lookup and range-bound handling use the same interpretation as writes.

Automatic deserialization retains its existing precision-specific recognition and returns naive datetime keys. It does not attach timezone information or convert instants. Callers obtain the table timezone separately when they need aware datetimes. With automatic deserialization disabled, keys remain strings. Arbitrary string keys retain their existing meaning.

An absent timezone means unspecified, never inferred UTC or the machine's local zone. Existing undeclared tables retain their behavior, including existing handling of offset-bearing string keys.

## Lifecycle and preservation

Timezone is established explicitly for an empty table. It cannot be silently inferred from the first row. Adding, changing, or removing it on a populated table requires a separate explicit migration that validates existing keys and their interpretation; ordinary metadata edits cannot perform that migration.

All write paths enforce the declared offset. Replacing or clearing consumer metadata affects only `data`. Ordinary row writes, lint, and slot resizing preserve the declaration. An undersized slot is refused before mutation. Timezone access and configuration are separate from the existing consumer metadata API and use the metadata slot's enablement rules.

The declaration must be established before rows relying on it are published. A malformed or lost declaration cannot safely be replaced with a guessed offset; recovery must not silently reinterpret existing keys.

## Compatibility

Timezone extends the known envelope independently of the consumer record. Earlier writers that reconstruct envelopes from `data` can discard this field and therefore cannot safely edit timezone-declared tables. Adding the field to the current envelope does not by itself provide compatibility with those writers; implementation must explicitly address this limitation before delivery.

## Source targets

- `jsonldb/metaslot.py`: at most 250 total lines, shared with the metadata-slot note.
- `jsonldb/jsonlfile.py`: at most 950 total lines, shared with the file-store notes.
- `jsonldb/folderdb.py`: at most 1250 total lines, shared with the folder-database notes.
