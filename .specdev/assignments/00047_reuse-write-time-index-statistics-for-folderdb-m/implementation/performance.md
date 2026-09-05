# Write-statistics performance evidence

Baseline: commit `58bdf0d7da136b8cc279ca863115734d68dc4410`, extracted with
`git archive` into an ignored source directory, without a worktree. Candidate:
this Assignment's three changed product modules. Each raw report records all
three source SHA-256 hashes, Python/platform identity and individual samples.

The same shipped harness runs both packages in separate processes. Four pairs
alternate baseline/candidate and candidate/baseline. Each operation has one
warmup and eleven timed samples per process; the table reports the median of
four process medians. OS caches are warm; setup and fixture construction are
excluded, but DataFrame conversion and synchronous db.meta publication are
included. Upserts replace one existing row without growing the table; size zero
is a no-op empty upsert. Creation removes the table and resets db.meta before
each timed operation. These results describe these fixtures, not all workloads.

## Final measurements

| Rows / operation | Baseline ms | Candidate ms | Speedup |
| --- | ---: | ---: | ---: |
| 0/dict/upsert | 0.4174 | 0.3837 | 1.088x |
| 0/dict/overwrite | 0.3763 | 0.3451 | 1.090x |
| 0/dict/create | 0.4044 | 0.3656 | 1.106x |
| 0/df/upsert | 0.5916 | 0.5518 | 1.072x |
| 0/df/overwrite | 0.4225 | 0.4028 | 1.049x |
| 0/df/create | 0.4481 | 0.4083 | 1.097x |
| 0/lowlevel/upsert | 0.2045 | 0.2059 | 0.993x |
| 0/lowlevel/overwrite | 0.1812 | 0.1550 | 1.169x |
| 10/dict/upsert | 0.4309 | 0.5221 | 0.825x |
| 10/dict/overwrite | 0.4221 | 0.3961 | 1.066x |
| 10/dict/create | 0.4167 | 0.3819 | 1.091x |
| 10/df/upsert | 0.5308 | 0.4981 | 1.066x |
| 10/df/overwrite | 0.5285 | 0.4974 | 1.063x |
| 10/df/create | 0.5063 | 0.4613 | 1.098x |
| 10/lowlevel/upsert | 0.1977 | 0.1911 | 1.035x |
| 10/lowlevel/overwrite | 0.1914 | 0.1997 | 0.958x |
| 1000/dict/upsert | 0.6737 | 0.4989 | 1.350x |
| 1000/dict/overwrite | 0.8747 | 0.8138 | 1.075x |
| 1000/dict/create | 0.8639 | 0.7997 | 1.080x |
| 1000/df/upsert | 0.6350 | 0.5716 | 1.111x |
| 1000/df/overwrite | 1.4302 | 1.3509 | 1.059x |
| 1000/df/create | 1.3915 | 1.3673 | 1.018x |
| 1000/lowlevel/upsert | 0.2734 | 0.2709 | 1.009x |
| 1000/lowlevel/overwrite | 0.5871 | 0.5907 | 0.994x |
| 100000/dict/upsert | 15.7787 | 10.5776 | 1.492x |
| 100000/dict/overwrite | 46.8063 | 40.5853 | 1.153x |
| 100000/dict/create | 46.8736 | 40.5174 | 1.157x |
| 100000/df/upsert | 15.7261 | 10.2185 | 1.539x |
| 100000/df/overwrite | 96.6538 | 90.4573 | 1.069x |
| 100000/df/create | 96.3768 | 89.9712 | 1.071x |
| 100000/lowlevel/upsert | 8.1672 | 8.1838 | 0.998x |
| 100000/lowlevel/overwrite | 39.1186 | 38.5254 | 1.015x |

Summary derivation alone costs about 0.002 ms for empty/10-row indexes, 0.020 ms
for 1,000 rows, and 1.864 ms for 100,000 rows. It performs two C-level key scans
(min/max), a length lookup and a size stat. Ordinary low-level writes skip it.
The 100,000-row upsert gains are 1.492x dictionary and 1.539x DataFrame; large
creates/overwrites improve about 1.07–1.16x. No twofold gain is claimed.

## Small-write investigation and limits

The initial implementation removed the empty-save branch and inadvertently used
the large buffer for empty legacy tables. Preliminary four-pair results showed
about 21% more latency for empty low-level overwrite (0.1532 to 0.1858 ms).
`preliminary/` preserves that evidence and the earlier candidate source hashes.
The final code retains the default buffer for empty legacy writes.

Four additional alternating pairs with 101 samples per operation are preserved
as `small-*.json`. Empty low-level overwrite is 0.1542 to 0.1565 ms (+1.5%);
10-row low-level overwrite is 0.1914 to 0.1948 ms (+1.7%). FolderDB operations
improve 1.07–1.11x in that denser sample. Large low-level writes remain near
baseline (0.2% slower upsert, 1.5% faster overwrite in the final matrix).

The short final matrix nevertheless contains a 10-row dictionary-upsert anomaly
(0.4309 to 0.5221 ms). It is retained above, not omitted. A subsequent two-pair
AB/BA follow-up with 201 samples per operation (`followup-*.json`) measures
0.4393 to 0.4032 ms (1.089x) for that operation. Other follow-up FolderDB costs
range from 1.006x to 1.079x faster; low-level changes are under 0.005 ms.
The denser results do not reproduce a material small-write regression. Sub-ms
filesystem measurements remain noisy; the reports preserve all samples.

## Reproduction

From the repository root, with the repository's runtime dependencies available:

```sh
baseline_dir="$(mktemp -d)"
git archive 58bdf0d7da136b8cc279ca863115734d68dc4410 jsonldb | tar -x -C "$baseline_dir"
.specdev/cache/bin/python-runtime-env python3 profile_test/benchmark_write_statistics.py --source-root "$baseline_dir" --save /tmp/jsonldb-write-baseline.json
.specdev/cache/bin/python-runtime-env python3 profile_test/benchmark_write_statistics.py --save /tmp/jsonldb-write-candidate.json --compare /tmp/jsonldb-write-baseline.json
```

Repeat in alternating order for four pairs. For denser small-table checks add
`--sizes 0 10 --repeats 101`; the final 10-row follow-up uses `--sizes 10
--repeats 201`. The wrapper can be replaced by a Python interpreter with the
project dependencies installed. The benchmark helper has an explicit gitignore
exception. Its actual execution on an extracted staged snapshot succeeded with
`--sizes 0 10 --repeats 2` (tree `b2bc46729902c2592399364a5ae3c6bb51b17f1f`).

## Compatibility evidence

295 focused tests passed, including 49 new write-statistics cases. An independent
baseline comparison (`verify_compatibility.py BASELINE_ROOT`) found all 66 public
function signatures unchanged and all 40 storage snapshots byte-identical across
dictionary/DataFrame writes, slots, unsorted/Unicode/colliding keys and empties.
Source sizes are 950/950 file store, 134/150 DataFrame adapter and 1188/1250
FolderDB. Full-suite execution was neither needed nor performed.
