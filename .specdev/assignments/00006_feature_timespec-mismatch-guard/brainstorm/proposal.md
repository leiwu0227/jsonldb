# Proposal: Auto-detect and heal timespec/data mismatches at FolderDB open

Before assignment 00004, the old global-timespec contamination bug could produce databases
whose `config.meta` says `seconds` while the files actually contain microsecond-precision
datetime keys (or vice versa). Such a database still loads — nothing is lost — but its
datetime keys deserialize as plain strings because `_is_datetime_string` matches on exact
length per configured precision.

This assignment adds a two-stage guard at `FolderDB.__init__`. Stage 1 is a zero-extra-I/O
trigger: the `min_index`/`max_index` keys already recorded in `db.meta` are checked against
the configured timespec. Only when a boundary key disagrees does stage 2 run: a full scan of
every `.idx` file's keys. The heal — adopt the detected precision, rewrite `config.meta`,
print a warning — fires only when that scan finds exactly one precision. Mixed precisions
keep the configured value and warn (no safe automatic choice exists). One documented
limitation: a table whose boundary keys match the config but which hides other-precision keys
in its interior never triggers stage 1; it behaves exactly as before the guard (those keys
load as strings, nothing is rewritten). The guard keeps reads and writes symmetric — unlike a
read-side-only fallback, which would re-serialize recovered datetime keys at the wrong
precision and write them back under different keys.
