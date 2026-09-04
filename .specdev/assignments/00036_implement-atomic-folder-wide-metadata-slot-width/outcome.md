# Outcome

## Delivered behavior

`FolderDB.set_meta_slot_bytes` now defaults to 4096, preserves unrelated
configuration, preflights every table, publishes the target configuration, and
atomically migrates only mismatched table files with their indexes rebuilt
last. Persisted widths configure new dictionary and DataFrame tables, and
interrupted operations converge on retry without replacing conforming tables.

## Deviations

None.

## Unresolved risks

None within the approved process-crash model; locking, fsync, and stronger
power-loss guarantees remain out of scope.

| Acceptance | Evidence | Result |
| --- | --- | --- |
| AC-1 | Focused enablement, safe-resize, byte-preservation, absolute-index, configured-creation, reopen, and no-op tests pass; the default/signature and line-cap audits pass. | Passed |
| AC-2 | Focused shrink preflight names all blockers in deterministic order and proves the complete folder remains byte-for-byte unchanged. | Passed |
| AC-3 | Injected configuration/table data and index publication failures leave complete old/new files; focused retries preserve rows and metadata, repair indexes, and skip conforming table replacement. | Passed |
