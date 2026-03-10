## Round 1

- [F1.1] **Addressed.** Updated Design §3 (batch sequential reads): reads are done in offset order for I/O throughput, but results are re-inserted into the result dict in sorted key order to preserve the API contract.
- [F1.2] **Addressed.** Updated Design §2 (bisect replacement): documented that each JSONL file has homogeneous key types in practice. Added `sorted(keys, key=str)` as a safety fallback to handle any unexpected mixed-type edge cases without raising `TypeError`.
- [F1.3] **Addressed.** Updated Design §4 (stream-lint): after the line-count check passes, additionally spot-check the first and last index entries by seeking to their byte offsets and verifying the parsed key matches. This catches offset corruption at O(2) extra reads. If spot-check fails, fall back to full index rebuild.
