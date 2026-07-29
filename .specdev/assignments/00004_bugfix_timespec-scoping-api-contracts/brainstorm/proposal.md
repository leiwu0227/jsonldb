# Proposal: Scope timespec per database and fix read-path API contracts

`FolderDB.__init__` mutates the module global `jsonlfile.TIME_SPEC` from its `config.meta`.
A live test confirmed the contamination: opening a microseconds-configured database flips the
global, and a second, fresh database opened afterwards silently inherits `microseconds` — and
persists it into its own newly written `config.meta`. Datetime-key detection
(`_is_datetime_string` length check) and key serialization then misbehave across instances in
the same process.

Alongside this, `select_line_jsonl` has a broken contract (docstring says `Optional[str]`, it
returns a dict; the `auto_serialize` flag silently doubles as a deserialize flag), and the same
datetime-key deserialization block is copy-pasted in four read paths.

This assignment threads an optional `timespec` parameter through the `jsonlfile`/`jsonldf`
read/write functions (defaulting to the module global, so direct callers see no behavior
change), makes `FolderDB` hold its timespec as instance state instead of mutating the global,
fixes the `select_line_jsonl` contract, and extracts one shared deserialization helper.
