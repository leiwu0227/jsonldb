# Metadata slot: consumer requirements and one request

Date: 2026-09-04
Status: Request against the published Roadmap designs; no implementation started
Origin: OceanData FolderData design discussion (Roadmap lane), 2026-09-04
Supersedes: `20260729_managed_canonical_coverage_sidecar_handoff.md`. The
companion-plus-catalog implementation it led to was rolled back to `fc22cba`
and lives on branch `post-fc22cba`.

## Context

OceanData will store one claim per ticker in the line-one metadata slot exactly
as `designs/jsonl_file_store/metadata_slot.md` defines it, using the API in
`designs/folder_database.md` and the delivery order in `forecast.md` items 5
through 9. The mechanism, the envelope, the width operation, the ordering rule,
and the failure model are all settled by those notes. This note records only
what the consumer needs beyond them, which is one small request, and how the
consumer will use the slot, so the request can be judged against real use.

## The request: `meta=` on the delete calls

Accept `meta=` on `delete_file_keys`, `delete_file_range`, and the plural
`delete_range`, with the record written **before** the tombstones. `None`
preserves the record, as on the write calls.

This generalizes the slot note's ordering rule from "written after the rows it
describes" to: **the record is written after additions and before removals, so
it never over-describes the rows in either direction.**

Why the consumer needs the order. A claim says which days were considered, and
the creator plans its next run from the claim: a covered day is never revisited.
When a creator withdraws coverage from days that still have rows, for example a
restatement from the validity start after a contract change, the rows must go
and the claim must shrink. If the rows are deleted first and the process dies
before the claim shrinks, the claim still covers days that have no rows, the
reader sees "considered, nothing there", and the planner never returns to them.
Data is lost silently. Shrinking the claim first leaves a claim that
under-describes, which the next run heals.

Without the request the consumer does this in two calls: a slot-only write
through `upsert_dict(name, {}, meta=shrunk)`, then the range delete. That is
correct under the designs as written, so nothing is blocked. The request folds
the two into one and puts the ordering guarantee where the tombstones are
written, instead of relying on every consumer to remember it.

Suggested wording for the slot note's Contract section: "The record is written
after additions and before removals: after the rows on save and upsert, before
the tombstones on delete." Forecast item 7 is the natural home.

## How the consumer publishes

For jsonldb's information, the creator-side protocol OceanData will follow:

| Change | Calls |
| --- | --- |
| Add or update rows | `upsert_*(name, rows, meta=claim)` |
| Remove rows, coverage unchanged | `delete_file_keys(name, keys)` |
| Remove rows and withdraw coverage | `delete_*(..., meta=shrunk claim)`, or the two-call form above |
| Publish a covered empty table | `upsert_*(name, {}, meta=claim)` |

Reads use `get_dict_with_meta` and `get_df_with_meta`. A record that
under-describes the rows, after a crash, is handled on the consumer side: rows
outside the claim are present but unconsidered and are re-established by the
creator's next run.

## What the consumer stores in `data`

For orientation only; jsonldb must not interpret it. OceanData namespaces its
record under its own key so the slot can carry other consumer records later:

```text
{"oceandata.claim/1": {"ticker": ..., "creator": {...}, "fields": [...],
                        "covered": [[start, end], ...], "final_through": ...,
                        "replay_from": ..., "revision": ..., "published_at": ...}}
```

Roughly 350 bytes plus about 28 bytes per coverage interval, so the 4096-byte
default holds well over a hundred intervals.

## Facts for sequencing the two repositories

- OceanData's storage adapter and coverage module currently call `read_aux`,
  `write_aux`, `remove_aux`, `replace_df_range`, and `read_family`, none of
  which exist on `master`. The shared OceanScript venv still has the
  pre-rollback build installed as `jsonldb 1.0.0`. Reinstalling from `master`
  before OceanData is adapted breaks those call sites; OceanData adapts after
  forecast items 6 through 8 land.
- Enabling each OceanData folder is one `set_meta_slot_bytes(4096)` call, the
  designed full-folder rewrite. Orphan `.jsonl.aux` companions from the
  pre-rollback build are deleted once, by hand, on the OceanData side; jsonldb
  need not know about them, and the companion concept can leave its designs.
- `todo.md` item 1 applies: the local test modules import the rolled-back aux
  functions and fail at collection on `master`.

## Open question

Whether the plural `delete_range(names, lower, upper, meta=...)` takes one
record for all names or a `{name: record}` mapping. The consumer only needs the
singular forms.
