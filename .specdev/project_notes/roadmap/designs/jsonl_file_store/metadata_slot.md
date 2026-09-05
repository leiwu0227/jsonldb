# Metadata Slot

A fixed-width slot on line one holds library-owned table properties and an optional opaque consumer record. jsonldb interprets envelope properties, never consumer data. Same-file storage keeps metadata with its rows.

## Contract

Per table: keyed rows and one optional record, both read from the same file, the record writable in place without a rewrite. Ordering rule: the record is written after the rows it describes and read before them, so it is never newer than the rows a reader sees; it may under-describe, never over-describe.

## On-disk form

```
{"_meta": {"v": 1, "timezone": "+08:00", "data": {...}}}····padding····\n
{"2024-07-01T00:00:00": {"mid": 1.0912}}
```

- Line one is a single-key object under `_meta`, padded with trailing spaces to the slot width and ending in a newline; any line reader still sees valid JSON.
- `v` is the envelope version; optional `data` belongs to consumers. Optional `timezone` belongs to jsonldb; [Table Timezone](table_timezone.md) defines its rules.
- JSON serializers escape newlines, so the envelope is one physical line whatever `data` contains.
- Index offsets are absolute and start after the slot.

## Envelope registry

jsonldb alone defines envelope versions. Version 1: key `_meta`; `data` and `timezone` optional, other fields ignored; trailing-space padding; width is the byte length of line one. Versions are added, never removed; files keep the version that wrote them. Consumers version the contents of `data` under their own keys.

## Detection

Line one is classified on every read:

- a valid envelope of a known version: a slot, record present or absent;
- a `_meta` object that is not a valid envelope, or an unknown version: a slot with no record, not a row;
- anything else: a legacy file, line one a row or garbage.

`_meta` is therefore a reserved linekey that writers reject; a pre-existing legacy row with that key is misread, which is accepted.

## Width and enablement

Every slotted file in a folder has the same width, recorded in `config.meta`. One explicit operation on `FolderDB` records it and immediately rewrites every table whose line one is not a slot of that width; this enables a legacy folder or changes a width, the only full-folder rewrite in the design. Shrinking first verifies every record fits and refuses, naming the tables, if any does not. Afterwards new tables get a slot at creation, and lint repairs any file whose line one is missing or of another width, which only an interrupted width operation or tampering produces. Readers still measure line one, since legacy files and torn slots exist; writers and lint use the recorded width.

Default 4096 bytes including the newline: one filesystem block, roughly 3.6 KB of payload. A record that does not fit is refused, naming both sizes; the remedy is the width operation and a re-publish.

## Write path

Publishing rows with a record:

1. in-place overwrites of records that still fit;
2. appends of new keys and grown records;
3. the slot, overwritten in place;
4. blanking of the old copies of grown records;
5. the index.

Consumer-record replacement and clearing preserve timezone. A slot-only write performs step 3 and touches the index modification time; offsets are unchanged. The buffer is flushed before step 3; no fsync is issued. Slots are inserted or resized only by the width operation or lint.

## Failure model

A process crash before step 3 leaves the old record above old rows plus possibly extra rows and a torn last line that lint truncates, the same state as a legacy file; one between steps 3 and 4 leaves a duplicate key lint removes. Power loss is not protected beyond the promise that the database opens and every table is repairable by an ordinary publish; a line one that no longer parses reads as a slot without a record until lint blanks it in place, recording its bytes.

## Legacy

- In a folder that is not slot-enabled every file is legacy: no record on read, record-carrying writes refused, nothing rewritten.
- Control files never carry a slot.
- Old readers see line one as a phantom row keyed `_meta` and do not fail.

## API

On `FolderDB`: `read_meta(name)`; `meta=` on the four write calls, `None` keeping and a dict replacing after the rows; `clear_meta(name)`; `set_meta_slot_bytes(width)`; `get_dict_with_meta` and `get_df_with_meta`, one table, a named tuple of `meta` and `rows`, `None` and an empty container when missing. Timezone access is separate. Opening never rewrites a table. The `_with_meta` pair fixes the reader order and promises nothing stronger.

## Source targets

- `jsonldb/metaslot.py`: at most 250 lines; envelope registry, classification, padding, slot read and write; imports nothing from the package.
- `jsonldb/jsonlfile.py`: at most 950 lines in total, shared with the other file-store notes.
- `jsonldb/folderdb.py`: at most 1250 lines in total, shared with the other folder-database notes.
