# Managed canonical coverage sidecar — discussion handoff

Date: 2026-07-29
Status: Architecture discussion; no implementation started
Origin: OceanData SpecDev Discussion D00008

## Executive summary

OceanData needs a canonical way to distinguish:

1. data that has not been queried;
2. data that was queried successfully and was absent; and
3. a query that failed, was incomplete, or had an ambiguous empty response.

The current proposal is a per-ticker coverage receipt, tentatively named:

```text
<ticker>.jsonl.cvg
```

For example:

```text
ref.cdr.datetime.etf.spy_arcx_usd.split.jsonl
ref.cdr.datetime.etf.spy_arcx_usd.split.jsonl.cvg
```

Git will version this filename, and current JSONLDB discovery will correctly avoid
treating it as a data ticker. However, JSONLDB currently does not manage such a
sidecar through delete, clear, move, hierarchy lint, or repair operations. Adding
`.cvg` only in OceanData would therefore allow stale or orphaned coverage to survive
after its corresponding data has changed.

The recommended direction is:

- JSONLDB owns the lifecycle mechanics for a managed canonical companion file.
- OceanData owns the coverage schema and the meaning of its contents.
- `.config` remains immutable build input.
- `.state` remains disposable audit output and is never consumed for query or data
  decisions.
- Canonical coverage is published only after a complete, validated source response
  and the corresponding data write have succeeded.

This needs a new JSONLDB SpecDev assignment before implementation, followed by an
OceanData assignment that uses the resulting companion-file contract.

## Related work

The originating OceanData artifacts are:

- `lib/oceandata/.specdev/discussions/D00008_define-oceandata-coverage-semantics-that-disting/brainstorm/proposal.md`
- `lib/oceandata/.specdev/discussions/D00008_define-oceandata-coverage-semantics-that-disting/brainstorm/design.md`

Relevant OceanData assignments:

- `00077`: ETF prices and corporate-action retrievers/generators.
- `00078`: explicit ETF action tickers and `dist_type`.

The immediate example is ETF splits and cash distributions, but the missing-data
distinction is a general OceanData architecture concern.

## The problem

An empty JSONL file or an absent row does not tell a consumer whether:

- the source was queried successfully and had no events;
- the source was unreachable;
- authentication failed;
- the response was partial or truncated;
- the response could not be validated;
- the date range was never queried; or
- data was deleted or reorganized after a prior query.

A mutable audit log cannot safely resolve this ambiguity. In particular, a failed
first request must not create an "empty" sentinel that prevents a later successful
request:

```text
attempt 1: Bloomberg unreachable
           -> no canonical coverage
attempt 2: Bloomberg reachable
           -> query again, write data, then publish coverage
```

The updater must retry attempt 2 because canonical coverage is absent. It must not
consult `.state` to make that decision.

## Architectural invariants

### `.config`

`.config` is the immutable, declarative build specification. A dataset must remain
rebuildable from versioned configuration. Runtime updates must not rewrite it.

### `.state`

`.state` is mutable and disposable audit information. It may record attempts,
failures, timings, diagnostics, or run reports, but:

- consumers must not depend on it;
- retrievers and generators must not use it to decide what to query;
- deleting it must not change dataset behavior or results; and
- it is not canonical evidence that a range is empty.

### Canonical coverage

Canonical coverage is part of the dataset contract. It is a successful-query
receipt, not an inferred sentinel.

- Only a successful, complete, validated response may publish coverage.
- Source unavailability, authentication failures, timeouts, ambiguous empties,
  partial responses, truncation, and validation failures publish no coverage.
- Absence of coverage means unknown, never "known empty."
- A zero-row successful result may publish coverage.
- Coverage is usable only when its producer contract is compatible and it is fresh
  under the configured policy.

## Proposed on-disk form

The short suffix under discussion is `.cvg`. The full companion path would retain
the data suffix:

```text
<ticker>.jsonl.cvg
```

Illustrative contents:

```json
{
  "kind": "oceandata.coverage",
  "version": 1,
  "ticker": "ref.cdr.datetime.etf.spy_arcx_usd.split",
  "fields": ["split_factor"],
  "producer_contract": "sha256:<contract-hash>",
  "complete_intervals": [
    {
      "start": "2000-01-01",
      "end": "2026-07-29",
      "observed_at": "2026-07-29T08:32:10Z",
      "row_count": 0,
      "content_hash": "sha256:<data-hash>"
    }
  ]
}
```

The exact schema remains an OceanData design decision. At minimum, it should make
these properties explicit:

- schema kind and version;
- ticker identity;
- covered fields or event family;
- producer/semantic contract identity;
- complete time intervals;
- source observation time;
- row count, including zero; and
- a binding to the data content or revision.

The coverage file is authoritative metadata. Unlike `.idx`, it cannot be recreated
from the JSONL rows alone because a missing row does not prove a successful empty
query.

## Publication rules

The source adapter should return an explicit result envelope rather than collapsing
all outcomes into a list of rows:

| Source outcome | Write data | Publish coverage | Audit in `.state` |
| --- | --- | --- | --- |
| Complete, validated, populated | Yes | Yes | Optional |
| Complete, validated, zero rows | Preserve valid data semantics | Yes | Optional |
| Source unreachable or timeout | No | No | Yes |
| Authentication or permission failure | No | No | Yes |
| Partial or truncated response | No canonical replacement | No | Yes |
| Ambiguous empty response | No canonical replacement | No | Yes |
| Schema or validation failure | No | No | Yes |

For a successful update, the safe publication order is:

1. validate the complete source result;
2. prepare the new data and coverage in temporary files;
3. publish the data atomically;
4. publish coverage atomically and last.

For destructive or in-place data mutations, existing coverage must be invalidated
before the data can diverge from it. A crash may leave coverage absent, which means
"unknown" and causes a safe re-query. It must not leave old coverage claiming that
new or partially written data is complete.

## Compatibility scan performed on 2026-07-29

### Git behavior

No Git change is required for the `.cvg` suffix.

- Git tracks arbitrary non-ignored filenames.
- The development dataset `.gitignore` contains only `.tmp_*` and `.old_*` patterns.
- `git check-ignore` found no rule matching a sample `probe.jsonl.cvg`.
- JSONLDB's Git helper currently calls `repo.index.add("*")` in
  `jsonldb/vercontrol.py`, so a non-ignored companion file is included by that
  workflow.

Therefore, `.cvg` can be version controlled normally.

### JSONLDB discovery

Current discovery ignores `.cvg`, which is desirable:

- `FolderDB.get_file_list()` in `jsonldb/folderdb.py` includes only filenames ending
  exactly in `.jsonl`.
- `build_dbmeta()` likewise processes exact `.jsonl` files.

Consequently, `ticker.jsonl.cvg` does not appear as a second ticker and does not need
to be added to ticker discovery.

### JSONLDB lifecycle gap

Current JSONLDB operations manage `.jsonl`, `.idx`, and sometimes `.meta`, but know
nothing about `.cvg`:

- `clear_folder()` removes `.idx`, `.jsonl`, and `.meta`, leaving `.cvg`.
- `delete_file()` removes the `.jsonl` and `.idx`, leaving `.cvg`.
- `lint_hierarchy()` moves `.jsonl` and `.idx`, stranding `.cvg`.
- `reprocess_invalid_tickers()` moves `.jsonl` and `.idx`, stranding `.cvg`.

Other reorganize, repair, and mutation paths should be audited in the new
assignment. These gaps make an OceanData-only sidecar unsafe: stale canonical
coverage could survive deletion or movement of the data it describes.

### OceanData Hub transport

The current Go CLI full-dataset archive recursively packs every regular file, so a
`.cvg` companion would be included in a full dataset push.

The config-only push explicitly archives `.config`, so coverage is correctly
excluded from that path.

A separate policy issue was found: the full archive currently follows filesystem
contents rather than `.gitignore` semantics and therefore also includes `.state`.
The future Hub contract should explicitly:

- include canonical `.cvg` files; and
- exclude disposable `.state` files.

That Hub policy is related but should not be silently folded into the JSONLDB
assignment.

## Recommended JSONLDB responsibility

JSONLDB should provide lifecycle-safe support for a managed companion file without
owning ETF or Bloomberg semantics.

Minimum capabilities:

1. Resolve the canonical companion path for a ticker.
2. Read and atomically write the companion.
3. Publish a companion last in a coordinated data update.
4. Remove or invalidate it before data mutations that can make it stale.
5. Delete it when the ticker is deleted.
6. Clear it when its folder/database is cleared.
7. Move it with the ticker during hierarchy lint and repair.
8. Move it during invalid-ticker reprocessing.
9. Detect and report orphan companions.
10. Provide a repair policy that is conservative: do not synthesize authoritative
    coverage from data rows.
11. Add deterministic tests for normal operations and interruption points.

One possible API shape is a managed-companion abstraction:

```python
folderdb.companion_path(ticker, kind="coverage")
folderdb.read_companion(ticker, kind="coverage")
folderdb.write_companion_atomic(ticker, kind="coverage", payload=...)
folderdb.remove_companion(ticker, kind="coverage")
```

However, a fully generic abstraction must still encode lifecycle policy. Coverage
is authoritative and must be invalidated conservatively, whereas `.idx` is derived
and rebuildable. Treating every suffix identically would hide this important
difference.

An explicit coverage companion may therefore be safer for the first iteration,
unless a generic API exposes policies such as:

```text
derived/rebuildable
authoritative/invalidate-on-mutation
audit/disposable
```

## Recommended division of ownership

JSONLDB should own:

- companion path and filesystem lifecycle;
- atomicity primitives;
- deletion, clear, move, and repair integration;
- orphan detection; and
- low-level consistency hooks.

OceanData should own:

- coverage JSON schema;
- source result classification;
- producer contract hashing;
- interval union and freshness rules;
- decisions about whether a range needs querying;
- semantic validation of source completeness; and
- the rule that `.state` is never a decision input.

The Hub/client layer should own:

- transfer inclusion policy;
- `.cvg` inclusion in full dataset archives; and
- `.state` exclusion from canonical distribution.

## Concrete ETF examples

### Successful empty split history

Bloomberg returns a complete, validated split-history response with zero rows for
the requested interval.

Result:

- the split JSONL has no event rows for that interval;
- `.cvg` records the successfully covered interval with `row_count: 0`; and
- future updates may avoid re-querying that interval while the receipt remains
  compatible and fresh.

### First request is unreachable

Bloomberg cannot be reached.

Result:

- existing canonical data is not replaced with empty data;
- no `.cvg` interval is published;
- `.state` may record the failed attempt; and
- the next update queries again because coverage is absent.

### Later request succeeds

Bloomberg later returns a complete response containing events.

Result:

- data is validated and written;
- `.cvg` is published last and binds the interval to that result; and
- the failed `.state` record has no effect on consumption or future query planning.

## Alternatives considered

### Put coverage into `.config`

Rejected. Runtime observations would mutate the declarative build specification and
break reproducible configuration.

### Put coverage into `.state`

Rejected. `.state` is audit-only and disposable. Depending on it would make deleting
audit history change dataset behavior.

### Use only empty sentinel rows

Rejected. A sentinel written after a failed or ambiguous query can falsely suppress
future requests, and a sentinel is awkward to bind to producer semantics and
freshness.

### Add custom coverage to database metadata

Not currently preferred. Existing metadata rebuild paths may discard custom state,
and a central mutable document creates coordination and merge pressure.

### Store coverage as an internal JSONL ticker

Not currently preferred. It exposes control-plane records through normal data
discovery and risks consumers treating them as market data.

### Maintain a separate coverage database

Possible, but it creates a second database whose atomic relationship with the data
must still be solved. A managed per-ticker companion keeps ownership and movement
local.

## Suggested JSONLDB assignment scope

The next session can open a focused SpecDev assignment with this objective:

> Add lifecycle-safe managed canonical coverage companions for JSONL tickers, without
> defining OceanData's domain-specific coverage schema.

Suggested acceptance criteria:

- `.jsonl.cvg` is never exposed as a ticker.
- Git-backed saves include non-ignored `.cvg` files.
- ticker deletion and folder clearing remove the companion.
- every supported ticker move/reorganization moves the companion.
- data mutations cannot leave a previously valid companion falsely authoritative.
- writes use crash-safe ordering and atomic replacement where the platform permits.
- orphan companions are reported by lint/validation and handled conservatively.
- JSONLDB never manufactures coverage merely from existing JSONL rows.
- tests cover populated, zero-row, failure, deletion, move, and interrupted-update
  cases.
- existing `.jsonl`, `.idx`, and `.meta` behavior remains compatible.

Out of scope for that assignment:

- Bloomberg response semantics;
- ETF split or distribution field selection;
- OceanData interval/freshness policy;
- `.state` run-report design; and
- Hub archive policy changes.

## Cross-repository delivery order

1. Open and complete the JSONLDB assignment for managed companion lifecycle.
2. Open or resume an OceanData assignment to define the coverage schema and source
   result envelope.
3. Integrate ETF retrievers/generators using canonical coverage.
4. Update Hub archive policy to include `.cvg` and exclude `.state`.
5. Rebuild the experimental dataset and verify:
   - known-empty ranges;
   - failed/unreachable queries;
   - successful later retries;
   - populated corporate-action ranges; and
   - rebuild behavior after deleting `.state`.

## Open questions for the next discussion

1. Should JSONLDB expose an explicit coverage API first, or a generic companion API
   with authoritative/derived/disposable lifecycle policies?
2. Which JSONLDB mutation APIs can alter ticker contents, and exactly when must each
   invalidate coverage?
3. Should database metadata report only companion presence/version, or remain
   entirely unaware of companions?
4. How should a multi-file data-plus-coverage update recover after a crash between
   the two atomic renames?
5. Should `.cvg` be JSON, JSONL, or a versioned envelope with a content checksum?
6. Is a per-ticker lock sufficient, or does publication need a folder/database
   transaction boundary?
7. What is the desired treatment of orphan `.cvg` files: report only, quarantine, or
   delete under an explicit repair command?

## Handoff conclusion

`.cvg` is a workable short filename and Git will version it. Current JSONLDB will
ignore it during ticker discovery, but that alone is not enough. Canonical coverage
must be managed as part of the ticker's lifecycle or it can become stale and unsafe.

The next implementation step belongs in JSONLDB: establish the companion lifecycle
contract first. OceanData can then rely on that contract while retaining ownership
of coverage semantics.
