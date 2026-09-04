# Core Concepts

JSONLDB is an embedded, file-based database for Python. A database is an ordinary directory; each table inside it is one JSON Lines file; each row is one line. The host process links the library directly and owns the directory; there is no server or daemon.

## Data model

- **Database = folder.** A `FolderDB` opens an existing directory and coordinates every table inside it.
- **Table = `.jsonl` file.** Tables are named by the caller; the name becomes the file stem.
- **Row = one line of the form `{"<linekey>": {record}}`.** A JSON object with exactly one top-level key, the serialized linekey, whose value is the record dictionary. Lines that fail to parse are skipped on read and removed by lint, which records them; other shapes are ignored.
- **Optional record = line one.** A table may reserve its first line as a fixed-width slot holding one opaque metadata record under the reserved key `_meta`. The metadata-slot note owns that design.

The `.jsonl` file is the single source of truth for a table. Everything else on disk is derived from it and can be rebuilt.

## Linekeys

A linekey is the row identity. Callers pass strings or `datetime` objects; other values are coerced through `str`. Datetimes are stored as ISO 8601 text at a precision the database fixes once, the **timespec**: `seconds` or `microseconds`. Reads recognize datetime-looking keys at that precision and hand back `datetime` objects when asked.

Keys are ordered by the lexical order of their serialized text, which is why one timespec is fixed per database: uniform ISO text sorts chronologically, mixed precision does not. Numeric keys are ordered as text too, so `"10"` sorts before `"2"`; callers who need numeric order must zero-pad.

## The derived index

Each table carries a sidecar `<table>.jsonl.idx` mapping every serialized linekey to the byte offset of its line, starting after the slot when one exists. The index is always written with keys sorted, so reading its keys yields the table's logical order without touching the data file. Range queries bisect the sorted keys and seek directly to the selected lines.

The index is disposable. It is rebuilt whenever it is missing, empty, unparseable, or older than its data file, and every read goes through one loader that performs that healing. Consequently the data file's physical order may lag its logical order.

## Mutation strategy

Writes avoid rewriting whole files:

- A full save streams the records in the order given and writes a fresh index.
- An upsert overwrites a record in place when the new line fits, padding with spaces. A record that grows is appended and its old line blanked. New keys are appended.
- A delete blanks the line with spaces and drops the key from the index. The file does not shrink.
- A metadata record is written after the rows and before the index.

Blank lines are tombstones, invisible to index-driven readers, but they accumulate dead space and unsort the physical order. **Linting** reclaims both: it verifies the index against the file, then rewrites the file in sorted key order through a temporary file and an atomic replace whenever the file is not canonical. Data-file rewrites happen only in lint and the slot-width operation; ordinary saves and upserts write in place.

## Layers

```
folderdb    FolderDB: tables in a folder, metadata, hierarchy, git facade
jsonldf     DataFrame <-> record dict adapter
jsonlfile   one JSONL file: format, index, CRUD, lint
metaslot    line one: envelope, classification, slot read and write
```

`jsonlfile` knows nothing about folders. `jsonldf` only translates a DataFrame's index to linekeys. `FolderDB` resolves names to paths, keeps per-database configuration, and forwards every table operation downward with the database's settings.

## Optional capabilities

Git version control and plotting are separate modules. Importing `FolderDB` never loads GitPython, Bokeh, or Matplotlib: the version-control methods import their module on first use, and plotting is reached by importing `jsonldb.visual` explicitly. All three are still install requirements.

## Control and report files

A database folder may hold three JSONL-formatted control files beside its tables: `db.meta` (per-table statistics), `config.meta` (timespec and slot width), and `h.meta` (hierarchy settings, present only in hierarchy mode). They reuse the table format, so each has its own `.idx`, and never carry a slot. A hidden `.jsonldb/` folder holds the two report files that open and lint write.

## Failure model and non-goals

The library targets process crashes: every completed write is visible in order, and a crash leaves a state that reads correctly and is repairable by an ordinary write. Power loss is not protected beyond the promise that the database opens and any table can be overwritten. Concurrency control, transactions, schema enforcement, and record-level validation are outside the library. One process at a time is assumed to write a given database.
