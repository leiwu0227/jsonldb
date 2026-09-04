# Outcome

## Delivered behavior

JSONL reads and index rebuilds now skip and path-log torn rows, append healing keeps record boundaries intact, grown upserts publish and flush replacements before blanking old rows, every index is atomically replaced last, and `config.meta` plus `h.meta` use atomic whole-file replacement while `db.meta` retains in-place data publication.

## Deviations

None.

## Unresolved risks

None within the approved process-crash model; locking, fsync, concurrent writers, and power-loss durability remain explicitly out of scope.

| Acceptance | Evidence | Result |
| --- | --- | --- |
| AC-1 | Focused tests cover all four torn-row paths with path-bearing logs, correct rebuild offsets, and terminal-newline healing. | Passed |
| AC-2 | Injected append-to-blank, pre-index, serialization, and replacement failures retain an old/new row and complete old/new index; normal writers produce compact sorted indexes last. | Passed |
| AC-3 | Destination tracing and injected replacement failures prove atomic `config.meta`/`h.meta` publication and unchanged non-atomic `db.meta` data publication. | Passed |
