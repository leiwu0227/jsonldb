# Ordered-read performance

Baseline: `a55e844`. Final package code measured in fresh processes, five alternating baseline/final trials per layout. OS file cache warmed before reads. Times and whole-process peak RSS are medians; RSS is sampled before post-timing validation. Values/key conversion and final ascending order were checked after timing, and table byte hashes remained unchanged. Raw samples are in `benchmark-results.json`; rerun `benchmark.py` through the repository Python runtime.

Runtime: 3.12.12 (main, Oct  9 2025, 11:07:00) [Clang 17.0.0 (clang-1700.6.3.2)]; macOS-15.3-arm64-arm-64bit.

| Keys / rows | Layout | Baseline ms | Final ms | Change | Baseline peak MiB | Final peak MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| date / 1,000,000 | sorted | 557.0 | 594.2 | 6.7% | 477.8 | 477.7 |
| date / 1,000,000 | backfill_1pct | 561.9 | 737.5 | 31.2% | 477.8 | 485.9 |
| date / 1,000,000 | shuffled | 567.9 | 952.8 | 67.8% | 477.8 | 496.6 |
| datetime / 100,000 | sorted | 70.2 | 81.7 | 16.4% | 107.7 | 122.2 |
| datetime / 100,000 | backfill_1pct | 70.0 | 91.5 | 30.7% | 107.7 | 127.3 |
| datetime / 100,000 | shuffled | 70.3 | 104.8 | 49.1% | 107.7 | 127.3 |

Date keys use auto_deserialize=False; datetime keys use auto_deserialize=True. Every record has value, close, and a null missing field. File sizes are about 57.1 MiB and 6.5 MiB respectively. These are synthetic, warm-cache workloads, not universal forecasts.

Converted datetime keys retain original serialized spellings to preserve lexical order and existing collision winners. That reconstruction bookkeeping adds memory even on already-ordered full reads; the adjacent-key check itself uses constant extra space. String-only reads avoid that retained spelling map. Unsorted results also allocate key references and a replacement dictionary; record values are reused. Indexed/wrapper read paths gained no sorting or full-read check.
