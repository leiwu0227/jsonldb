# Brainstorm Changelog

## Round 1 response

### F1.1 — boundary-only sampling can mis-heal mixed-precision tables

**Accepted; design revised to two-stage detection plus an explicit documented limitation.**

- Healing no longer fires on `min_index`/`max_index` evidence alone. Stage 1 (boundary keys
  from `db.meta`, zero extra I/O) only *triggers*; stage 2 (`_scan_index_timespecs`, every key
  of every `.idx` file) must find exactly one precision — and it differing from the configured
  value — before `config.meta` is rewritten. A scan that finds both precisions keeps the
  configured timespec and warns, satisfying the stated mixed-precision policy for every case
  the guard can see.
- The reviewer's exact example (seconds-shaped boundaries, microsecond interior key) is now a
  **documented limitation** with its own pinning test: stage 1 does not trigger, nothing is
  rewritten, and the interior key loads as a string — identical to pre-guard behavior, so the
  guard never makes a database worse. Detecting that case would require a full index scan on
  every open of every healthy database, contradicting the zero-cost-when-healthy goal; this
  trade-off is now recorded in Key Decisions.
- Success criteria expanded from one test to three: heal (uniform mismatch), mixed-with-
  triggered-scan (keep + warn), and the boundary-camouflage limitation.

## Round 2 response

### F2.1 — proposal.md still described the rejected boundary-only design

**Accepted; proposal.md rewritten to match the revised design:** it now describes the
two-stage guard (boundary trigger from `db.meta`, full `.idx` scan confirmation before any
rewrite), the mixed-precision keep-and-warn policy, and the boundary-camouflage limitation.
No design change in this round — documentation consistency only.

## Round 3 response

### F3.1 — stale Non-Goals line said detection uses only db.meta key ranges

**Accepted; Non-Goals line corrected:** it now states that stage 1 uses `db.meta` key ranges,
that the stage 2 full `.idx` scan runs only after a stage 1 boundary mismatch, and that data
files (`.jsonl`) are never scanned. Wording-only fix; no design change.
