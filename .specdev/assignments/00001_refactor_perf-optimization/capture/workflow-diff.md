# Workflow Diff — Performance Optimization
**Date:** 2026-03-10  |  **Assignment:** 00001_refactor_perf-optimization

## What Worked
- Brainstorm phase identified all 5 bottlenecks accurately; no surprises during implementation
- TDD discipline caught regressions early (especially around bisect replacing Numba)
- Codex review caught real issues: leftover json.load in folderdb.py and missing benchmark comparison
- The 10-task breakdown was well-scoped; each task was completable in a single pass
- Stream-lint design with line-count + spot-check heuristic works well for the common case

## What Didn't
- The stream-lint approach has a 23x regression on unsorted 100K files — the design didn't surface this tradeoff clearly enough. A hybrid threshold (in-memory for small files, stream for large) would have been better to design upfront
- WSL2 filesystem (Windows /mnt/h/) has 1-second mtime resolution, which broke mtime-based tests — had to switch to content comparison. This is a general constraint worth documenting
- Running baseline benchmarks required a separate git worktree setup, which was cumbersome. The --save/--compare flags on benchmark.py solve this for future use
