# Implementation plan

**Implementation Guides:** api-security, frontend
**Review Guides:** none

Authority: approved contract `bc000917a44fd2a394f0d82ffa040ce61f8d749106d3e8ace4fe8e8cca242658` and the published strict-read design.

1. **T-1 (AC-1, AC-3)** Add per-read strict handling and diagnostics in the three file-store read paths; retain index recovery/cache behavior. Preserve full-load and equal-bound forwarding and selected-offset error locations. Shorten redundant documentation within the file-store module to remain within 950 lines.
2. **T-2 (AC-2)** Forward keyword-only strict through DataFrame load/select and the four FolderDB getters without changing positional meanings, metadata ordering, missing results, or permissive calls. Retain 150/1250-line caps.
3. **T-3 (AC-1, AC-2, AC-3)** Add focused regressions for bad encountered rows, mismatched/blank/EOF selections, full/range/point delegation, wrappers, legitimate absence, slots, tombstones, file preservation, and recovery after repair. Prove unvisited/previously omitted damage is outside indexed reads and warm reads add no full scan. Update public API and usage documentation with that limitation.
4. **T-4 (AC-1, AC-2, AC-3)** Run focused affected read, cache, metadata, and timezone tests plus Python 3.8 parsing, source caps, and diff checks. Record acceptance evidence and run the required independent implementation review, fixing in-scope findings before delivery.

Historical context: Assignment 00044 outcome records shared keyed-row shape validation and permissive damage skipping; Assignment 00046 outcome records index reuse and invalidation. Current code confirms both; neither expands strictness into index verification.
