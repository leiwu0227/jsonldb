# DataFrame conversion performance and compatibility

Baseline: commit `5276e8e`, including Assignment 00048. The same tracked benchmark
runs each package in a separate process; historical product source is extracted
from Git into ignored scratch, without a worktree. Raw reports retain Python,
pandas/NumPy, three module hashes and all samples. Three pairs alternate AB, BA,
AB order, with one warmup and seven samples per operation; reported values are
medians of the three process medians. Inputs and OS caches are warm. Input-frame
construction is excluded; conversion, storage and synchronous metadata publication
are included in complete writes. Conversion-result disposal is excluded from
conversion-only timings, as stated in each report.

## Final large-table results

| 100,000-row shape / operation | Baseline ms | Candidate ms | Speedup |
| --- | ---: | ---: | ---: |
| numeric_1/conversion | 45.841 | 30.408 | 1.508x |
| numeric_1/overwrite | 81.248 | 66.568 | 1.221x |
| numeric_1/upsert | 10.421 | 10.639 | 0.980x |
| numeric_8/conversion | 86.868 | 69.095 | 1.257x |
| numeric_8/overwrite | 141.749 | 124.396 | 1.139x |
| numeric_8/upsert | 11.027 | 10.851 | 1.016x |
| mixed_8/conversion | 139.365 | 98.397 | 1.416x |
| mixed_8/overwrite | 184.949 | 141.302 | 1.309x |
| mixed_8/upsert | 10.687 | 10.710 | 0.998x |
| datetime_8/conversion | 116.134 | 114.913 | 1.011x |
| datetime_8/overwrite | 315.591 | 313.950 | 1.005x |
| datetime_8/upsert | 11.168 | 11.061 | 1.010x |
| fallback_object_index/conversion | 74.859 | 73.891 | 1.013x |
| fallback_object_index/overwrite | 130.390 | 128.431 | 1.015x |
| fallback_object_index/upsert | 10.900 | 10.722 | 1.017x |

The validated string-indexed overwrite targets improve 1.220x for one numeric
column, 1.139x for eight numeric columns, and 1.309x for seven numeric plus one
text column. Conversion-only gains are 1.26–1.51x. Single-row upserts into large
tables remain near baseline because the changed conversion sees just one row.
Datetime and explicit object-index frames take the existing path; their large
write measurements remain near baseline. No universal or twofold gain is claimed.

## Eligibility and compatibility

Acceleration is restricted to pandas 3.0.x, exact DataFrame/Index classes,
Python-backed pandas StringDtype indexes with no missing keys, nonempty frames
and columns, and unique row/column labels. Subclasses, unusual index classes,
object indexes, missing keys, duplicate labels, empty axes, other string storage
and other pandas branches retain the original index-oriented conversion. Each
entry point's original duplicate-index validation behavior is preserved.

The main runtime (pandas 3.0.5) passes 411 focused tests, including 81 new
conversion cases. Isolated pandas 1.5.3, 2.0.3, 2.3.3 and 3.0.0 environments each
pass 142 focused conversion/adapter/metadata tests. Their Python is 3.11.12;
NumPy versions are respectively 1.24.4, 1.24.4, 2.2.6 and 2.4.6. No project
requirements or lockfiles changed. Installation of pandas 1.3.5 on the available
Python 3.9.6 had no usable wheel and was not forced into a source build. The
1.3 branch remains supported through the original path, with a deterministic
version-guard test; it is not claimed to have been runtime-tested here.

Tests compare keys, serialized values, warnings, exceptions, caller frames and
physical storage against index-oriented pandas. They exercise numeric, mixed,
nullable, object, categorical, nested and unsupported values, datetime/MultiIndex/
range/mixed/missing/object indexes, duplicate labels, empty axes, subclasses,
eager conversion failure and plural completion ordering. Existing durability,
cache, slot, byte-serialization and metadata cases remain green. Independent
baseline comparison confirms 66 public signatures and 40 storage snapshots
unchanged. Source lengths are adapter 144/150, file store 950/950 and FolderDB
1188/1250. Full-suite execution was not performed.

## Small operations and allocation

`benchmark-summary.json` contains the complete 0/10/1,000/100,000-row matrix,
including negative short-run samples. In particular, 10-row eight-column numeric
overwrite measured 0.783x and 1,000-row datetime upsert 0.829x in the short matrix.
Two denser AB/BA pairs with 101 samples per operation (`small-*.json`) measure
1.033x and 0.980x respectively. Empty mixed-frame upsert changes from a short
0.922x to 0.986x. The slowest dense result is empty mixed overwrite, 0.964x,
about 0.019 ms more; remaining negative deltas are smaller. Empty and datetime
paths retain their original conversion. No material slowdown is established.

Allocation was measured separately with tracemalloc on 20,000-row frames, after
input construction and outside latency sampling. This measures peak traced
allocation, not process RSS. Values below are decimal MB.

| Shape | Conversion baseline / candidate | Whole write baseline / candidate |
| --- | ---: | ---: |
| numeric_1 | 4.730 / 5.269 | 58.222 / 58.222 |
| numeric_8 | 9.706 / 10.243 | 63.195 / 63.195 |
| mixed_8 | 10.287 / 10.825 | 63.777 / 63.778 |
| datetime_8 | 12.364 / 12.364 | 66.970 / 66.970 |
| fallback_object_index | 9.705 / 9.705 | 63.195 / 63.195 |

The fast path's temporary key/record lists add roughly 0.54 MB to conversion peak
(5–11% in these fixtures); full-write peak is essentially unchanged because the
lists are released before the large I/O buffer is allocated. This is a measured
tradeoff, not a zero-allocation claim or a bound for arbitrary table sizes.

## Reproduction

```sh
baseline_dir="$(mktemp -d)"
git archive 5276e8e jsonldb | tar -x -C "$baseline_dir"
.specdev/cache/bin/python-runtime-env python3 profile_test/benchmark_dataframe.py --source-root "$baseline_dir" --save /tmp/jsonldb-df-baseline.json
.specdev/cache/bin/python-runtime-env python3 profile_test/benchmark_dataframe.py --save /tmp/jsonldb-df-candidate.json --compare /tmp/jsonldb-df-baseline.json
```

Alternate order over three pairs. Dense checks add `--sizes 0 10 1000 --repeats
101`; allocation checks add `--sizes 20000 --repeats 1 --memory`. A Python runtime
with project dependencies can replace the local wrapper. The new helper has an
explicit ignore exception and was actually executed from extracted staged tree
`d0d4528c533e926bf02dde946663039776c8112c` with `--sizes 0 10 --repeats 2`.
`check_evidence.py` independently checks source identities, samples, aggregates,
acceptance gains and line caps; `checks.json` retains timed verification receipts.
