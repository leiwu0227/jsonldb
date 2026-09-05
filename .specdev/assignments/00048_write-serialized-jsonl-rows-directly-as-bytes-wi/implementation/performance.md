# Direct byte serialization: performance and compatibility

Baseline is commit `784b038`, whose product bytes include Assignment 00047
(`d6e3de4`). Both variants run the same shipped harness in separate processes;
the baseline package is extracted from Git into an ignored directory, with no
worktree. Raw reports record Python/platform, three module hashes and every
sample. Four pairs alternate AB/BA order, each with one warmup and eleven samples
per operation. Reported values are medians of four process medians. OS caches are
warm; fixture construction is excluded; actual serialization, DataFrame conversion,
file/index publication and FolderDB metadata upkeep are included where applicable.

## Main results

| 100,000 rows / operation | Baseline ms | Candidate ms | Speedup |
| --- | ---: | ---: | ---: |
| dict/upsert | 10.141 | 10.288 | 0.986x |
| dict/overwrite | 40.375 | 34.312 | 1.177x |
| dict/create | 40.216 | 34.272 | 1.173x |
| df/upsert | 10.084 | 10.073 | 1.001x |
| df/overwrite | 89.744 | 82.647 | 1.086x |
| df/create | 88.950 | 82.551 | 1.078x |
| lowlevel/upsert | 8.122 | 8.116 | 1.001x |
| lowlevel/overwrite | 39.009 | 32.195 | 1.212x |
| summary_only | 1.813 | 1.811 | 1.001x |
| serialization_only | 15.850 | 9.663 | 1.640x |

End-to-end low-level save is 1.212x faster, satisfying AC-3; the isolated serializer
is 1.640x faster. Large FolderDB creates/overwrites improve about 1.08–1.18x.
One-row upserts into a large table remain near baseline because index and metadata
work dominate that operation. These are synthetic small-record fixtures, not a
universal speedup or a twofold guarantee. Serialization loops use each revision's
actual writer representation: the baseline re-encodes the helper's text result;
the candidate consumes the bytes result directly. Neither measured loop performs
file I/O or preconstructs serialized output.

## Small operations and timing variation

`benchmark-summary.json` retains the entire empty/10/1,000/100,000-row matrix,
including negative short-run results. The short matrix showed empty low-level
save at 0.8599x and 10-row DataFrame upsert at 0.7924x. Follow-up AB/BA pairs use
201 samples per operation (`small-*.json`), keeping all results:

| Operation | Baseline ms | Candidate ms | Speedup |
| --- | ---: | ---: | ---: |
| 0/dict/upsert | 0.38140 | 0.40323 | 0.946x |
| 0/dict/overwrite | 0.34719 | 0.36210 | 0.959x |
| 0/dict/create | 0.38312 | 0.38508 | 0.995x |
| 0/df/upsert | 0.44135 | 0.43615 | 1.012x |
| 0/df/overwrite | 0.39804 | 0.40329 | 0.987x |
| 0/df/create | 0.42481 | 0.42412 | 1.002x |
| 0/lowlevel/upsert | 0.18315 | 0.20083 | 0.912x |
| 0/lowlevel/overwrite | 0.15548 | 0.15192 | 1.023x |
| 0/summary_only | 0.00229 | 0.00198 | 1.158x |
| 0/serialization_only | 0.00006 | 0.00006 | 1.000x |
| 10/dict/upsert | 0.41308 | 0.40300 | 1.025x |
| 10/dict/overwrite | 0.42469 | 0.40873 | 1.039x |
| 10/dict/create | 0.42873 | 0.41458 | 1.034x |
| 10/df/upsert | 0.50927 | 0.47702 | 1.068x |
| 10/df/overwrite | 0.50660 | 0.47662 | 1.063x |
| 10/df/create | 0.52685 | 0.47335 | 1.113x |
| 10/lowlevel/upsert | 0.20100 | 0.19804 | 1.015x |
| 10/lowlevel/overwrite | 0.19431 | 0.19804 | 0.981x |
| 10/summary_only | 0.00219 | 0.00221 | 0.991x |
| 10/serialization_only | 0.00167 | 0.00104 | 1.600x |
| control/atomic_save | 0.27569 | 0.27527 | 1.002x |

The two earlier slowdowns do not reproduce: empty low-level save is 1.023x and
10-row DataFrame upsert 1.068x. Atomic control save is essentially unchanged at
0.2757 versus 0.2753 ms. Some follow-up empty operations vary by up to 0.022 ms
(+5.7% for FolderDB dictionary upsert, +9.7% for empty low-level upsert). The empty
low-level operation serializes no rows, and its executed path is unchanged, so
that difference is consistent with measurement noise rather than added work.
10-row low-level overwrite differs by about 0.004 ms (+1.9%). These observations
remain disclosed; no material regression is established by this evidence.

## Reproduction

From the repository root:

```sh
baseline_dir="$(mktemp -d)"
git archive 784b038 jsonldb | tar -x -C "$baseline_dir"
.specdev/cache/bin/python-runtime-env python3 profile_test/benchmark_write_statistics.py --serialization --source-root "$baseline_dir" --save /tmp/jsonldb-bytes-baseline.json
.specdev/cache/bin/python-runtime-env python3 profile_test/benchmark_write_statistics.py --serialization --save /tmp/jsonldb-bytes-candidate.json --compare /tmp/jsonldb-bytes-baseline.json
```

Alternate order across four pairs. For dense small-table evidence use `--sizes
0 10 --repeats 201` across two AB/BA pairs. A Python interpreter with the project
dependencies may replace the local runtime wrapper. The tracked benchmark was
actually executed from extracted staged tree `12d08ee10cf741dae613d92f99b333c3e98942c1`
with `--serialization --sizes 0 10 --repeats 2`. No ignored helper is required.

## Correctness and integrity

330 focused tests passed in 2.02 seconds: 25 new byte-serialization cases, existing
durability/atomicity coverage and the prior metadata/cache/integration selection.
The new cases compare physical records/indexes with the former serializer,
exercise Unicode padding/appends and slots, and compare exceptions/partial-write
boundaries for unsupported objects, invalid Unicode and non-contiguous NumPy data.

The durable compatibility script from Assignment 00047, run against this pinned
baseline, confirms 66 public signatures and 40 storage snapshots unchanged.
`check_evidence.py` verifies raw samples, medians, source hashes, the save benefit
and the 950-line cap. `checks.json` contains timed verification receipts. Full
suite execution was not performed. Publication and failure guarantees remain
those of the existing single-writer durability model.
