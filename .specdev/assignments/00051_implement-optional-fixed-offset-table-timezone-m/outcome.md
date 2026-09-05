# Outcome

## Delivered behavior

Optional v1 `_meta.timezone` stores canonical fixed offsets separately from consumer data. Four additive accessors configure existing empty slotted tables; populated changes are refused and equal offsets are no-ops. Declared-table writes and query/delete keys validate offsets and store suffix-free timestamps. Returned keys remain naive or strings. Consumer metadata replacement/clearing, overwrite/upsert, lint and resizing preserve timezone. Overwrite placeholders retain the declaration before dependent rows.

## Deviations

None. Historical `_meta` consumers are excluded by user direction; pre-`_meta` jsonldb caller/file compatibility is explicitly covered. A cohesive new private helper stays within its delegated 250-line cap. No public signature, dependency declaration or roadmap file was changed except for the four approved additive accessors.

## Unresolved risks

Claude Opus 5.0 xhigh implementation review approved the corrected candidate (Attempt-00085), with all five acceptance criteria passed. Strict offset component validation now rejects Python normalization of overflowing components and loss of fractional zero offsets; timestamp-like ordinary labels remain strings. No populated-table timezone migration or named-zone support is provided, as scoped. A wholly erased declaration is indistinguishable from absence; recognizable damage is preserved and refused. Python 3.8 syntax was checked, but its runtime and pandas 1.3 were unavailable; tested runtimes are Python 3.12/pandas 3.0.5 and Python 3.11/pandas 1.5.3 and 2.3.3. Full-suite execution was not performed. Seven alternating timing rounds show undeclared-table median overhead of 0.5–6.5% (about 0.5–13 microseconds on small operations), with no material observed regression.

| Acceptance | Evidence | Result |
| --- | --- | --- |
| AC-1 | Offset/API, slot/state, physical-emptiness and refusal tests in test_table_timezone.py; restart and independent consumer data | Passed |
| AC-2 | 855 focused tests, including strict suffix/refusal cases; 512 checks on each of pandas 1.5.3 and 2.3.3 | Passed |
| AC-3 | test_timezone_recovery.py failure injection and malformed/undersize refusal; unknown-envelope and existing durability/cache/metadata regressions | Passed |
| AC-4 | line-counts.json: metaslot 179/250, jsonlfile 950/950, jsonldf 144/150, folderdb 1198/1250, _tabletimezone 146/250; Python 3.8 syntax passes | Passed |
| AC-5 | 8 historical file/caller tests; verify_compatibility.py compares 66 signatures, 40 snapshots and 18 historical observations; bounded legacy-performance.json | Passed |
