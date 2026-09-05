# Index cache performance evidence

Baseline: b744cbd, source SHA-256 `661c9331b5c4f298fd836721fda985c6098534ef26b1b68374d010ffea86d4d6`.
Candidate: working tree, source SHA-256 `1f9c6c4ee3bd99b139808028443ed607b540b34e00da2948808cffecceea00cf`.
Host: macOS-15.3-arm64-arm-64bit; Python 3.12.12.

Both runs used the same benchmark, data shape and machine. Each operation has one warmup and nine samples, with equivalent results checked on every iteration. Cold means empty application cache; OS cache remains warm. No cold-storage claim is made. The baseline was loaded as an isolated module from the exact pre-change source; package dependencies are unchanged. Raw timing samples and memory observations are in benchmark-results.json.

| Workload | Before ms | After ms | Speedup |
| --- | ---: | ---: | ---: |
| 1000/cold_point | 0.078 | 0.098 | 0.79x |
| 1000/warm_point | 0.079 | 0.017 | 4.74x |
| 1000/cold_range | 0.139 | 0.164 | 0.85x |
| 1000/warm_range | 0.140 | 0.074 | 1.90x |
| 1000/read_after_write | 0.083 | 0.106 | 0.79x |
| 1000/write_and_read | 0.349 | 0.389 | 0.90x |
| 1000/full_load | 0.507 | 0.518 | 0.98x |
| 100000/cold_point | 6.802 | 7.046 | 0.97x |
| 100000/warm_point | 6.765 | 0.108 | 62.84x |
| 100000/cold_range | 7.502 | 7.235 | 1.04x |
| 100000/warm_range | 7.734 | 0.146 | 53.06x |
| 100000/read_after_write | 6.631 | 6.825 | 0.97x |
| 100000/write_and_read | 15.628 | 16.318 | 0.96x |
| 100000/full_load | 56.741 | 56.608 | 1.00x |
| 600000/cold_point | 53.289 | 65.945 | 0.81x |
| 600000/warm_point | 52.391 | 54.531 | 0.96x |
| 600000/cold_range | 55.586 | 56.283 | 0.99x |
| 600000/warm_range | 54.949 | 54.857 | 1.00x |
| pressure/cycle_six_tables | 39.913 | 46.253 | 0.86x |

## Interpretation and memory choice

The two required 100,000-row warm workloads exceed 2x (about 63x point and 53x range). Cold point/read-after-write latency increases by roughly 0.2 ms; complete write-plus-read adds about 0.7 ms. These modest, disclosed admission costs are amortized by a subsequent cache hit. A 1,000-row first read adds about 20 microseconds; its 100-row warm range improves less than 2x, outside the specified 100,000-row target.

A six-table cyclic working set exceeds the 64 MiB budget. All revisited indexes have been evicted: the cycle costs about 46 ms versus 40 ms (16% slower), with four entries retained and about 58 MiB conservatively charged. This bounded-memory tradeoff is documented, not described as a speedup. The default stays 64 MiB rather than being raised to make an over-budget benchmark appear fast.

The 600,000-row index is not retained. The sequential benchmark's cold point median was about 66 ms versus 53 ms, while its later point/range medians were close to baseline. Because this discrepancy could indicate a material regression, a separate paired experiment alternated baseline/candidate execution order over ten samples after two warmups, clearing the application cache before each call. The paired medians were 52.98 ms baseline and 53.66 ms candidate (1.3% overhead), with zero retained entries. Both the original unfavorable measurement and all paired samples are retained in benchmark-results.json; the evidence supports modest bypass overhead rather than a reliable 24% regression. No cold-storage or universal timing guarantee is made.

A 100,000-row entry with range keys charges 16,022,388 bytes. The estimate bounds dictionary storage, key text/header storage and 64-bit integer offsets from parsed object count and encoded byte length; non-ASCII or escaped keys use a conservative four-byte-per-character bound. Unsupported nested/noninteger offsets are never cached, and oversized entries bypass further admission work. Unicode/escaped-key and integer-boundary tests compare the charge with actual decoded object sizes. Accounting includes path, entry and fingerprint overhead, plus the lazy tuple, but excludes transient buffers, allocator overhead and active-query references. This is not an RSS cap.

An initial per-object sizing implementation substantially regressed cold/pressure paths. It was replaced within approved implementation authority by conservative accounting and a single-byte escape check. Final published samples correspond only to the final source hash above. No contract, module cap, public setting or dependency changed.

## Reproduction

```sh
git show b744cbd:jsonldb/jsonlfile.py > .specdev/cache/index-cache-baseline-source.py
.specdev/cache/bin/python-runtime-env python3 profile_test/benchmark.py --index-cache --source .specdev/cache/index-cache-baseline-source.py --save .specdev/cache/index-cache-baseline-final.json
.specdev/cache/bin/python-runtime-env python3 profile_test/benchmark.py --index-cache --save .specdev/cache/index-cache-final.json --compare .specdev/cache/index-cache-baseline-final.json
```
