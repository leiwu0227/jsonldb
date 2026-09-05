# Datetime write performance and compatibility

Baseline product: `0c54d29`, including Assignment 00049. The implementation
boundary `5f56588` additionally contains the separately published file-store
note; product bytes are identical between those commits. Benchmark processes
load the baseline extracted directly from Git or the candidate source root.
Reports retain interpreter/dependency versions, platform and source hashes.

## Repeated complete writes

Three alternating AB/BA/AB process pairs, one warmup and seven timed samples
per operation; figures are medians of process medians. Inputs and OS caches are
warm. Input construction is excluded; DataFrame conversion and synchronous
table/index/FolderDB metadata publication are included. Dictionary fixtures use
ordinary Python datetime keys; DataFrame fixtures use pandas Timestamp keys.

| 100,000-row operation | Baseline ms | Candidate ms | Speedup |
| --- | ---: | ---: | ---: |
| One numeric column DataFrame overwrite | 255.771 | 195.247 | 1.310x |
| Eight numeric columns DataFrame overwrite | 314.896 | 255.025 | 1.235x |
| Seven numeric + text columns DataFrame overwrite | 359.576 | 299.516 | 1.201x |
| Datetime dictionary overwrite | 120.248 | 83.569 | 1.439x |
| Eight numeric columns DataFrame, 100k-row upsert | 564.799 | 501.979 | 1.125x |
| String-indexed eight-column overwrite (fallback) | 125.176 | 125.127 | 1.000x |
| Named-timezone eight-column overwrite (fallback) | 676.449 | 667.573 | 1.013x |

One-row upserts into a 100k-row table are near baseline (11.611 vs 11.661 ms,
0.996x); 1,000-row upserts measure 16.361 vs 15.845 ms (1.033x). The optimization
reduces formatting of the supplied keys; unchanged index I/O dominates small
changes. No universal or twofold benefit is claimed. `main-*.json` retains all
samples, including negative small/empty results.

## Eligibility and source constraints

Reuse applies to exact dictionaries starting with an exact Python datetime or
pandas Timestamp, with ordinary timespec/default values. Every key must be an
ordinary string or an exact datetime/Timestamp whose timezone is absent or an
exact `datetime.timezone`. If any key is custom or has another timezone, the
entire input uses its original two conversion passes. Named zones deliberately
fall back; the exploratory prototype admitted ZoneInfo but this narrower guard
avoids new version-specific imports and leaves those workloads near baseline.
String-leading/mixed inputs and custom mappings also retain the original path.

The existing pandas dependency supplies exact Timestamp recognition under a
private import alias. No dependency or public interface changes. A deferred items
factory preserves custom mapping lookup timing inside the original writer
boundary; it does not defer eager DataFrame conversion or record/key validation.
Prepared keys form an ordered list rather than a normalized-key dictionary, so
colliding input rows remain physical rows with the baseline effective index.

Local consolidation of metadata-slot setup and redundant writer descriptions
keeps the file store at 950 physical lines. The DataFrame adapter is 144/150 and
FolderDB 1188/1250. Counts include comments, docstrings and blank lines. Explicit
AC-4 evidence is in `line-counts.json`, verified by `check_evidence.py`, and must
be checked again after review before delivery. No logic was moved to a new module.

## Compatibility evidence

606 focused tests pass on the main pandas 3.0.5 runtime, including 195 new tests.
A 256-test key/adapter/metadata subset passes separately on pandas 1.5.3, 2.0.3,
2.3.3 and 3.0.0 using Python 3.11.12. Main-runtime Python/version details and
NumPy versions are recorded in benchmark reports; existing environments and
dependency versions from Assignment 00049 were reused without dependency changes.
The Python 3.8 syntax floor is checked by AST parsing; Python 3.8 and pandas 1.3
were not runtime-tested. No full suite was executed.

Differential checks compare the former validation/conversion passes with current
writers across all three low-level entry points, precision/timezone/collision
cases, invalid input, custom key/mapping/timezone side effects, errors and warnings.
FolderDB/DataFrame cases cover slots, metadata, cache reads and shorter/longer
rows; plural failures preserve prior completion. A direct custom-mapping lookup
assertion checks that the second items lookup still occurs after file opening.
The existing durability, metadata, cache, slot migration and serialization tests
remain green. An independent baseline-package comparison verifies 66 public
signatures and 40 physical storage snapshots unchanged, covering shared save-slot
setup as well as row behavior.

## Reproduction

From the repository root, with a Python environment containing project dependencies:

```sh
baseline_dir="$(mktemp -d)"
git archive 0c54d29 jsonldb | tar -x -C "$baseline_dir"
python3 profile_test/benchmark_datetime_keys.py --source-root "$baseline_dir" --save /tmp/datetime-baseline.json
python3 profile_test/benchmark_datetime_keys.py --save /tmp/datetime-candidate.json --compare /tmp/datetime-baseline.json
```

Alternate process order over three pairs. Add `--sizes 0 10 1000 --repeats 101`
for dense small-workload samples; use `--sizes 20000 --repeats 1 --memory` for
separate allocation traces. The tracked helper is explicitly included by
`.gitignore`. `check_evidence.py` verifies all source identities, samples,
aggregates, intended benefits and source limits. Allocation and denser results are reported below.

## Peak allocation and the accepted tradeoff

Single allocation traces run separately from latency at 20,000 rows. Input frame
allocation is excluded; full writes include conversion. This is traced Python
allocation, not RSS. Validation-only measurements retain the returned factory and
its key list through peak observation, so deferred ownership is not omitted.

| Input | Whole-write baseline bytes | Candidate bytes | Added bytes |
| --- | ---: | ---: | ---: |
| One numeric column, unique datetime | 61,996,913 | 62,170,225 | 173,312 |
| Eight numeric columns, unique datetime | 66,970,259 | 67,143,571 | 173,312 |
| Mixed columns, unique datetime | 67,551,987 | 67,725,299 | 173,312 |
| Eight numeric columns, all keys collide | 64,715,451 | 66,088,643 | 1,373,192 |
| String fallback | 63,195,177 | 63,195,417 | 240 |
| Named-timezone fallback | 67,090,908 | 67,093,706 | 2,798 |

Datetime validation peak is 723 vs 1,373,759 bytes. Unique-key strings are later
needed by the index, so retaining them early adds little beyond list references
to the full-write peak. Colliding keys retain strings not kept by the final index,
so their overhead is larger. Both match the exploratory ~0.17 MB / ~1.37 MB
tradeoff; neither is an arbitrary-size upper bound. Inputs with a late custom key
may briefly accumulate a partial list before falling back; the O(input rows)
qualification includes this case.

## Small-workload follow-up and fresh-checkout verification

The initial seven-sample matrix showed empty dictionary overwrites at 0.819x.
Two AB/BA pairs with 101 samples restore that case to 1.007x. That denser matrix
also contains one candidate process with three larger outliers: empty string
frames 0.883x, ten-row mixed frames 0.881x and ten-row dictionaries 0.842x when
aggregated with the second pair. The second pair alone improves all three.

Three additional alternating process pairs with 301 samples for sizes 0 and 10
measure those cases at 0.992x, 1.012x and 0.976x respectively. The largest remaining
negative absolute delta in this confirmation is about 0.010 ms for the ten-row
dictionary. The 1,000-row, one-row-upsert dense sample is 0.954x (+0.031 ms),
while its initial matrix was 1.061x; no material recurring slowdown is established.
All earlier negative samples and all follow-ups are retained; no sample was
removed. These are local filesystem benchmarks, not a universal latency bound.

The delivered helper was run from fresh staged tree
`8a30d2caf24af1f626ad6b35a61aa673a5c14ddf` with `--sizes 0 10 --repeats 2`.
`fresh-checkout.json` retains its actual output and duration. Main benchmark
runtime is Python 3.12.12, pandas 3.0.5 and NumPy 2.5.2.
