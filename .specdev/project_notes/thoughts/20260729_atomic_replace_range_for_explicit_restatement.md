# Atomic range replacement for explicit historical restatement

Date: 2026-07-29
Status: Agreed architecture direction; no implementation started
Origin: OceanData ETF corporate-action and canonical-coverage discussion

## Decision summary

Add a public JSONLDB operation for atomically replacing a bounded key range of
one ticker:

```python
FolderDB.replace_range(
    name,
    lower_key,
    upper_key,
    replacement_df,
)
```

OceanData should call this semantic operation when a user explicitly requests a
historical refresh. OceanData should not manipulate `.jsonl`, `.idx`, `.meta`,
temporary files, or call `os.replace()` directly.

Routine `update_to_date(start_date=None)` remains forward-only and does not use
range replacement. An explicit `start_date` authorizes restatement of the
requested historical interval.

JSONLDB owns the filesystem implementation and the lifecycle of the ticker's
opaque `<ticker>.jsonl.aux` companion. The implementation may use an atomic
filesystem replacement primitive internally, but that is a JSONLDB detail.

## Context

OceanData's ETF corporate-action retriever currently treats every refresh as a
complete snapshot:

- it ignores the caller's `start_date`;
- it queries from the configured `valid_start`;
- it constructs complete split and distribution histories; and
- it calls `replace_ticker_data()`, whose current OceanData implementation stages
  JSONL and index files and invokes `os.replace()` directly.

Relevant OceanData locations:

- `oceandata/folderdata/retrievers/retriever_etf_corpact.py`
- `oceandata/folderdata/data_layer.py`
- `oceandata/folderdata/api_abs_layer.py`

The direct replacement was originally introduced because a successful
corporate-action response could contain additions, corrections, and removals.
Ordinary upsert cannot remove an event that disappeared from a later
authoritative response.

The design was reconsidered after clarifying OceanData's user-intent boundary:

```text
update_to_date(start_date=None)
    routine continuation; do not rewrite historical data

update_to_date(start_date=<explicit date>)
    intentional historical refresh; replacement is authorized in that interval
```

Therefore, complete replacement is not appropriate for every routine ETF
update. Bounded replacement remains necessary for an explicit historical
restatement.

## Why the existing JSONLDB operations are insufficient

JSONLDB already supports:

```python
FolderDB.delete_file_keys(name, keys)
FolderDB.delete_file_range(name, lower_key, upper_key)
FolderDB.upsert_df(name, df)
FolderDB.overwrite_df(name, df)
```

These operations do not provide an atomic bounded replacement.

### Delete followed by upsert

```text
delete requested range
→ crash
→ replacement rows are never written
```

Reversing the order has the opposite failure:

```text
upsert replacement rows
→ crash
→ rows absent from the new authoritative result remain stored
```

The user's explicit authorization to revise history does not imply acceptance
of a partially applied revision.

### Existing `overwrite_df`

The current implementation removes the owner and then writes the replacement.
It is a whole-ticker operation, not a range operation, and interruption after
deletion can leave the valid prior ticker unavailable.

### OceanData's direct `os.replace`

The OceanData implementation provides useful single-file publication behavior,
but it crosses the abstraction boundary:

- OceanData accesses private JSONLDB path helpers;
- OceanData manages JSONL and index files itself;
- OceanData manually updates database metadata; and
- direct filesystem replacement bypasses JSONLDB's `.aux` invalidation hooks.

JSONLDB should own all files that constitute a ticker.

## Required semantics

Given:

```text
existing ticker E
inclusive interval [L, U]
validated replacement rows R
```

the committed result must be:

```text
rows from E with key < L
+ rows from R with L <= key <= U
+ rows from E with key > U
```

Rows from the old ticker within `[L, U]` are removed even when `R` is empty.
Rows outside the interval remain byte-equivalent or logically equivalent under
JSONLDB's canonical serialization rules.

The replacement input must be validated before the existing ticker or companion
can change.

Suggested validation:

- `lower_key <= upper_key`;
- replacement keys are unique;
- every replacement key lies within the inclusive interval;
- key serialization uses the database's configured `timespec`; and
- the replacement payload is a supported DataFrame or dictionary form.

## Proposed API

The first API may be DataFrame-specific:

```python
def replace_df_range(
    self,
    name: str,
    lower_key: Any,
    upper_key: Any,
    df: pd.DataFrame,
) -> None:
    ...
```

Alternatively, use the repository's existing naming style:

```python
def replace_range(
    self,
    name: str,
    lower_key: Any,
    upper_key: Any,
    df: pd.DataFrame,
) -> None:
    ...
```

The final name is delegated to the assignment, but it must communicate that
this is authoritative replacement, not upsert.

A low-level dictionary operation may be useful:

```python
jsonlfile.replace_jsonl_range(
    jsonl_file_path,
    lower_key,
    upper_key,
    replacement_dict,
    timespec=None,
)
```

`FolderDB` should remain the public entry point used by OceanData.

## Safe publication ordering

The required invariant is:

> Interruption may discard canonical coverage, but must not leave stale `.aux`
> content attached to changed data or expose a partially constructed range.

A suitable implementation sequence is:

```text
1. Validate L, U, and all replacement keys.
2. Read/stream the existing ticker and construct the complete resulting ticker
   in same-directory staged data and index files.
3. Flush and fsync the staged files as appropriate.
4. Invalidate the target ticker's `.aux`.
5. Publish the staged owner.
6. Publish or rebuild the corresponding index.
7. Update database metadata.
8. Return success with `.aux` absent.
```

OceanData then publishes its new canonical coverage payload through
`FolderDB.write_aux()` only after the domain operation, audit publication, and
semantic validation have completed.

The exact internal filesystem primitive is delegated to JSONLDB. OceanData must
not call it directly.

## `.aux` companion behavior

Assignment 00008 added one lifecycle-managed opaque companion:

```text
<ticker>.jsonl.aux
```

Range replacement must integrate with that contract:

- staging failure leaves the old owner and old `.aux` unchanged;
- immediately before owner mutation, old `.aux` is removed;
- failure after invalidation leaves `.aux` absent;
- failure after owner publication leaves `.aux` absent;
- JSONLDB does not interpret or synthesize replacement `.aux` content; and
- the consumer publishes new opaque content explicitly and last.

An absent companion means unknown coverage. It is safer than retaining coverage
that refers to the previous data revision.

The `replace_range` API should not accept an OceanData coverage schema. JSONLDB
continues treating companion bytes as opaque.

## OceanData behavior after this addition

### Routine corporate-action continuation

```python
update_to_date(start_date=None, to_date=T)
```

OceanData should:

1. read canonical coverage from `.aux`;
2. identify only the uncovered forward interval;
3. fetch and validate that interval;
4. upsert newly observed future events without deleting historical events; and
5. publish merged coverage last.

Corporate actions are sparse, so the latest event date cannot serve as the
query boundary. A ticker may have no events for months even though its source
was successfully queried through today.

### Explicit historical restatement

```python
update_to_date(start_date=S, to_date=T)
```

OceanData should:

1. fetch the complete authoritative result for `[S, T]`;
2. fail without mutation if the source result is incomplete or ambiguous;
3. call JSONLDB's bounded replacement operation;
4. publish audit/provenance artifacts as required; and
5. publish updated canonical coverage last.

An empty successful response intentionally removes all stored events in the
authorized interval.

### Initial build

When no ticker or coverage exists, OceanData may query from the configured
`valid_start` and create the initial owner. No historical data is being revised.

## Failure matrix

| Failure point | Data outcome | `.aux` outcome |
| --- | --- | --- |
| Source query or domain validation | Existing data unchanged | Existing companion unchanged |
| Replacement validation | Existing data unchanged | Existing companion unchanged |
| Staged data/index construction | Existing data unchanged | Existing companion unchanged |
| After companion invalidation, before owner publication | Existing data may remain old | Absent |
| After owner publication, before index/metadata completion | Complete new owner; index must self-heal or finish on recovery | Absent |
| OceanData audit publication | Complete new owner | Absent |
| New companion publication | Complete new owner | Old companion must never reappear |
| Complete success | Complete bounded replacement | New companion published |

## Suggested acceptance criteria

1. A public `FolderDB` range-replacement operation replaces exactly the
   inclusive requested interval, removes missing old rows within it, accepts an
   empty replacement, and preserves rows outside it.
2. Invalid bounds, duplicate keys, out-of-range replacement keys, and
   serialization failures leave the owner, index, metadata, and existing
   companion unchanged.
3. Once owner mutation can begin, the old companion is invalidated first; every
   tested interruption afterward leaves the companion absent rather than stale.
4. Readers observe a complete old or complete new logical ticker, with existing
   index self-healing behavior covering any unavoidable data/index publication
   window.
5. Ticker discovery and existing `.jsonl`, `.idx`, `.meta`, `.aux`, upsert,
   delete, and overwrite behavior remain compatible.

## Focused test cases

- Replace a populated middle interval while preserving both outer regions.
- Replace the beginning or end of a ticker.
- Replace the entire ticker range.
- Replace with zero rows and remove every prior row in the interval.
- Insert rows into a previously empty interval.
- Correct existing rows with identical keys.
- Reject a replacement key outside the authorized interval.
- Reject duplicate replacement keys.
- Preserve old owner and `.aux` when staging or serialization fails.
- Remove old `.aux` immediately before owner publication.
- Simulate interruption after companion invalidation.
- Simulate interruption after owner publication and verify index recovery.
- Verify database metadata after success.
- Verify hierarchy-aware paths.
- Verify `.aux` remains excluded from ticker discovery.

The tests must be version-controlled. Assignment 00008's companion tests are
currently under the repository's ignored `unit_tests/` directory, so the new
assignment should ensure its regression tests are actually committed or amend
the ignore policy narrowly.

## Non-goals

- Defining OceanData's canonical coverage JSON schema.
- Encoding Bloomberg or ETF semantics in JSONLDB.
- Automatically rewriting historical data during routine updates.
- Supporting arbitrary companion suffixes or multiple companions.
- Making data, index, metadata, and companion publication one impossible
  cross-file atomic transaction.
- Adding dataset release/version-pinning architecture.

## Cross-repository delivery order

1. Open a JSONLDB SpecDev assignment for atomic bounded replacement.
2. Install the delivered JSONLDB build into `lib/oceanscript/venv-macos`.
3. Open an OceanData assignment that:
   - restores `update_to_date` intent for ETF corporate actions;
   - uses forward-only updates when `start_date=None`;
   - uses JSONLDB range replacement only for explicit historical starts;
   - defines and publishes canonical coverage through `.aux`; and
   - removes direct `os.replace()` and private JSONLDB path handling from
     `data_layer.py`.
4. Rebuild the experimental ETF dataset and verify both forward continuation
   and explicit restatement behavior.

## Handoff conclusion

The desired operation is not generic deletion and not unconditional whole-file
replacement. It is an explicit, bounded, authoritative restatement with safe
publication and managed companion invalidation.

OceanData should request that operation. JSONLDB should implement it.
