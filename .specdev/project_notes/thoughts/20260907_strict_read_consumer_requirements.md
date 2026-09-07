# Strict reads for OceanData consumers

Date: 2026-09-07. Handoff from OceanData's FolderData design review.
Reviewed JSONLDB checkout: `4779cac`.

This note requests an opt-in strict-read capability. It records consumer requirements and a suggested interface; implementation remains in JSONLDB.

## Problem and concrete example

OceanData records coverage separately from observation rows. A missing observation within a covered period can be legitimate, such as a holiday. A malformed stored row must instead be a storage read failure.

Suppose a raw ticker has coverage for June 5–10 and its June 8 JSON line is damaged. JSONLDB currently skips malformed rows and returns the other observations. OceanData then sees June 8 as absent. If its validation recipe permits absence, cleaning could publish successfully despite storage corruption.

A detection algorithm cannot reliably catch this: the damaged row has disappeared before the algorithm receives its input. OceanData must receive a failed read rather than a successful partial result. The affected ticker operation fails; the operator repairs or regenerates data and reruns. Other ticker operations can proceed independently.

## Suggested public interface

Add an optional keyword-only `strict=False` argument to public row-reading methods, preserving existing positional arguments and default behavior. For example:

```python
rows = db.get_dict(
    ["_raw.fx.or.lvl.eurusd.nyc.spot"],
    lower_key="2026-06-05",
    upper_key="2026-06-10",
    auto_deserialize=False,
    strict=True,
)
```

Provide consistent behavior through FolderDB dictionary and DataFrame reads, their `_with_meta` variants, and the corresponding single-file full, range, and point reads. Wrappers should pass the option through to shared reading logic.

OceanData will enable strict reads in its JSONL FolderDataAPI implementation. It should not need its own JSONL parser or a logging handler that converts warnings into exceptions.

## Required semantics

- Default reads retain current permissive behavior.
- Strict reads raise on malformed JSON, invalid keyed-row shapes, or unreadable/mismatched indexed observations. They must not return the remaining rows as a successful result.
- Legitimately absent observations remain absent. Preserve documented missing-table behavior; strictness is not a requirement that observations exist.
- Blank tombstones and padding are normal storage representation. A valid first-line metadata slot is not an observation.
- Include the file/table identity, byte offset or line location when available, and an informative reason in the exception. A normal `ValueError` is sufficient; a new exception hierarchy is unnecessary.
- Reading does not repair or discard damaged observations. Existing recoverable index maintenance may remain, provided it cannot hide the failure.

## Index and cache correctness

Merely raising inside the current row parser is insufficient. A permissive index rebuild may already have excluded a malformed physical row. A later strict point/range read could therefore return an empty result without examining that row or emitting a warning.

Strictness must hold with existing, missing, rebuilt, and cached indexes, including after a prior permissive read. An empty selection is not evidence that the underlying data is sound.

Suggested simple initial policy: a malformed physical observation makes strict reads of that table fail, even for a bounded request, because an unreadable key may not be assignable to a date range. This conservative scope is a proposal to settle during implementation. Optimize validation reuse only if it remains correct when the data file changes; the consumer does not require a particular scanning or caching mechanism.

## Focused acceptance checks

1. Full, bounded, and point reads fail on damaged observations with useful diagnostics.
2. They still fail after permissive index rebuilding has excluded the damaged row, including with a warm index cache.
3. Missing observations, empty tables, valid slots, and deletion tombstones preserve normal behavior.
4. Dictionary, DataFrame, and metadata-plus-rows wrappers propagate failures consistently.
5. Default permissive reads remain compatible; strict failures leave observation bytes unchanged.
6. After explicit repair and index refresh, rerunning succeeds.

## Boundaries

This request concerns malformed observation reads. Strict metadata-envelope interpretation is a separate review issue. It does not request transactions, rollback, automatic data repair, schema validation, coverage inference, stronger power-loss guarantees, or changes to OceanData's human validation workflow.
