# Core Concepts

JSONLDB is an embedded, file-based database for Python. A database is an ordinary directory; each table inside it is one JSON Lines file; each row is one line. The host process links the library directly and owns the directory. There is no server, no daemon, and no background process.

## Data model

- **Database = folder.** A `FolderDB` opens an existing directory and coordinates every table inside it.
- **Table = `.jsonl` file.** Tables are named by the caller; the name becomes the file stem.
- **Row = one line of the form `{"<linekey>": {record}}`.** Every line is a JSON object with exactly one top-level key, the serialized linekey, whose value is the record dictionary. Lines that fail to parse are reported and skipped; lines of another shape are ignored. Nothing is repaired on read.

The `.jsonl` file is the single source of truth for a table's data. Everything else on disk is derived from it and can be rebuilt.

## Linekeys

A linekey is the row identity. Callers pass strings or `datetime` objects; other values are coerced through `str`. Datetimes are stored as ISO 8601 text at a precision the database fixes once, called the **timespec**: `seconds` or `microseconds`. Reads recognize datetime-looking keys at that precision and hand back `datetime` objects when asked.

Keys are ordered by the lexical order of their serialized text. That is why datetimes are stored as ISO strings and why one timespec is fixed per database: uniform ISO text sorts chronologically, mixed precision does not. Numeric keys are ordered as text too, so `"10"` sorts before `"2"`; callers who need numeric order must zero-pad.

## The derived index

Each table carries a sidecar `<table>.jsonl.idx` mapping every serialized linekey to the byte offset of its line. The index is always written with keys sorted, so reading its keys yields the table's logical order without touching the data file. Range queries bisect the sorted keys and then seek directly to the selected lines.

The index is disposable. It is rebuilt whenever it is missing, empty, unparseable, or older than its data file, and every read path goes through one loader that performs that healing. The accepted consequence is that the data file's physical order may lag its logical order.

## Mutation strategy

Writes are designed to avoid rewriting whole files:

- A full save streams the records in the order given and writes a fresh index.
- An upsert overwrites a record in place when the new line fits in the old one, padding with spaces. A record that grows is blanked and appended at the end. New keys are appended.
- A delete blanks the line with spaces and drops the key from the index. The file does not shrink.

Blank lines are tombstones. They are invisible to readers because readers navigate by index, but they accumulate dead space and leave the physical order unsorted. **Linting** reclaims both: it verifies the index against the file, then rewrites the file in sorted key order through a temporary file and an atomic replace whenever the file is not already sorted and compact. Only the lint rewrite is atomic; ordinary saves and upserts write in place.

## Layers

```
folderdb    FolderDB: tables in a folder, metadata, hierarchy, git facade
jsonldf     DataFrame <-> record dict adapter
jsonlfile   one JSONL file: format, index, CRUD, lint
```

`jsonlfile` knows nothing about folders. `jsonldf` only translates a DataFrame's index to linekeys. `FolderDB` resolves names to paths, keeps per-database configuration, and forwards every table operation downward with the database's timespec.

## Optional capabilities

Git version control and plotting are separate modules. Importing `FolderDB` never loads GitPython, Bokeh, or Matplotlib: the version-control methods on `FolderDB` import their module on first use, and plotting is reached by importing `jsonldb.visual` explicitly. All of these packages are nevertheless declared as install requirements.

## Metadata files

A database folder may hold three JSONL-formatted control files beside its tables: `db.meta` (per-table statistics), `config.meta` (the timespec), and `h.meta` (hierarchy settings, present only when hierarchy mode is on). They reuse the table format, so each has its own `.idx`. They are informational or configurational; no data read depends on `db.meta`.

## Non-goals

Concurrency control, transactions, schema enforcement, and record-level validation are outside the library. One process at a time is assumed to write a given database.
