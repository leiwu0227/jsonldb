# Outcome

## Delivered behavior

Ordinary datetime/Timestamp dictionary inputs reuse stable formatted keys from
pre-mutation validation during save, atomic save and upsert. Public APIs and
DataFrame/FolderDB wrappers preserve eager conversion and existing publication
behavior. Prepared keys retain row order and collisions; custom/unsupported
keys, timezones, mappings and other ineligible inputs use original conversion.

## Deviations

None. Named timezones conservatively fall back under delegated eligibility.
Existing dependencies and supported-version declarations are unchanged. Local
save-slot consolidation keeps jsonlfile.py at exactly 950 physical lines.
The independently published roadmap update and D00003 remain outside delivery.

## Unresolved risks

No unresolved implementation blocker. Gains are workload-specific: tested 100k
DataFrame overwrites improve 1.20–1.31x, dictionary overwrites 1.44x and large
upserts 1.13x. Small upserts and fallback writes remain near baseline. Retained
keys add ~0.17 MB to full-write peak at 20k unique rows and ~1.37 MB when all keys
collide, matching the accepted O(input rows) tradeoff. Local noisy samples and
follow-ups are retained in performance.md and raw reports. Python 3.8/pandas 1.3
were not runtime-tested; syntax and representative newer runtime branches were
checked. Full-suite execution was not performed.

| Acceptance | Evidence | Result |
| --- | --- | --- |
| AC-1 | 606 main-runtime focused tests, 256 on each of four additional pandas versions; 66 public signatures and 40 baseline storage snapshots identical. New tests cover keys, collisions, timezones, mappings, warnings/errors, slots, caller frames and plural completion. | Passed |
| AC-2 | Baseline conversion passes match all three writers; custom mapping lookup timing stays at the original file boundary. Durability, slot, cache, metadata, payload/error and publication checks pass. | Passed |
| AC-3 | Three alternating full matrices show five intended gains. Dense and confirmation samples, fallback costs, unique/collision allocation, source provenance and fresh-checkout benchmark execution are retained and verified. | Passed |
| AC-4 | line-counts.json and check_evidence.py verify total physical counts: jsonlfile.py 950/950, jsonldf.py 144/150, folderdb.py 1188/1250. No new product module, cap increase or out-of-scope relocation. | Passed |
