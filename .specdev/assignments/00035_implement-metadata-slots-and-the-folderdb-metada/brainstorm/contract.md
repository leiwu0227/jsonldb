# Assignment contract

Kind: feature

## Objective and context

As the second implementation child of `.specdev/missions/M00001_implement-the-five-dependency-ordered-roadmap-wo/brainstorm/contract.md` at approved hash `7e0824300f897190603ccb138e56aaf5254389c0b40f69967908352465ceac4f`, implement line-one metadata slots and the complete `FolderDB` metadata API on the durability guarantees delivered by prerequisite outcome `.specdev/assignments/00034_harden-jsonl-durability-and-atomic-index-and-con/outcome.md`.

## Scope and non-goals

- In scope: version-1 fixed-width slot encoding, legacy/slotted classification, low-level metadata reads and fit-checked writes, metadata-aware row access, and the `FolderDB` metadata API; folder-wide width enablement or resizing, lint repair, reports, logging migration, and all parent Mission non-goals remain out of scope.

## Expected behavior

Legacy files remain ordinary row-only JSONL, while slotted files expose one opaque version-1 `_meta` record in a fixed-width line-one envelope. The metadata API preserves the Mission-defined record-first read and rows-first write ordering, refuses non-fitting records without partial mutation, returns the specified missing-table result, and leaves existing public calls compatible.

## Important decisions

- Readers classify and measure line one from the file rather than consulting folder configuration; slot-only publication leaves row offsets unchanged and completes by updating index freshness after the slot write.

## Constraints and invariants

- Inherit all unchanged Mission constraints and invariants; specifically, reserve `_meta`, keep control files slot-free, preserve absolute row offsets and per-instance timespec behavior, and keep all seven governed source files within their parent-defined logical-line caps.

## Delegated and reserved authority

- Delegated: internal slot helpers, validation structure, and focused test seams within the Mission's published module boundaries; reserved: every authority retained by the Mission, including changes to the version-1 envelope or 4096-byte default and the later children’s folder-wide migration, lint-repair, and reporting behavior.

## Risks and assumptions

- Classification must not confuse legacy rows with valid slots, serialized byte length must govern fit, and sub-second WSL2 mtimes are unsuitable as test assertions; the prerequisite outcome is assumed to provide the required atomic index publication and torn-row tolerance.

## Verification authority

- Run focused slot and `FolderDB` API tests covering legacy/slotted files, ordering, fit refusal, missing tables, absolute offsets, and slot-only index publication, plus the parent-required Python 3.8 audit and seven-file logical-line inventory; the full tracked suite remains reserved for final Mission integration.

## Acceptance criteria

- AC-1: Legacy and slotted files classify correctly; a valid slotted file exposes exactly one opaque version-1 `_meta` record at line one, `_meta` cannot be used as an ordinary row key, and the complete `FolderDB` metadata API returns the specified result for a missing table without breaking existing public calls.
- AC-2: Metadata reads are record-first, metadata writes are rows-first and refuse an oversized record without changing the file, row indexes retain correct absolute offsets, and a successful slot-only write publishes index freshness last without rewriting row data.
