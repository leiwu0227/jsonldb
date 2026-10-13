# Adhoc AH-20261013T114218892Z-33a6

- Scope: Simplify deletion control flow, benchmark iterator intersection before adoption, and consolidate repeated deletion test instrumentation without behavior changes or new classes
- Title: refactor: simplify deletion flow and regression helpers
- Started: 2026-10-13T11:42:18.892Z
- Completed: 2026-10-13T11:44:06.830Z
- Starting working tree: Clean.

## Outcome

Simplified deletion to one optional-statistics return while retaining final-removal early return and changed status. Consolidated three index-load spies and two metadata failure callbacks into test helpers; all assertions and cases preserved. No classes added; roadmap retry, empty-table, index reuse and pruning contracts unchanged. 349 focused tests passed. Iterator intersection experiment rejected: 10000-key selection 390.6 us versus 335.2 us for existing set path. Actual final before/after medians over 21 randomized repetitions: public delete 0.9713/0.9757 ms, partial delete 1.4358/1.3847 ms, healthy no-match 0.8157/0.8243 ms, retry 1.0433/1.0457 ms, final removal 0.4280/0.4151 ms. No material regression observed on this local workload. Corrected obsolete benchmark assertion for previously fixed retry behavior. Source remains within design line caps.

## Focused workflow coexistence

No focused Assignment or Mission coexistence was recorded.

## Delivery path facts

### Requested adopted paths

None.

### Committed paths

- `.specdev/adhoc/2026-10/AH-20261013T114218892Z-33a6_simplify-deletion-control-flow-benchmark-iterato.md`
- `jsonldb/jsonlfile.py`
- `unit_tests/test_remove_empty.py`

### Rejected paths

None.

### Remaining owned paths

None.

## Verification summary

No manual verification summary was supplied.

## Verification attempt history

- **shared-return: passed.** `.venv/bin/python -m pytest -q unit_tests/test_remove_empty.py unit_tests/test_jsonlfile.py unit_tests/test_table_timezone.py` (1283 ms, working-tree@5e0cb43280bc4e73ea938ea6787398ad6664cb60)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 32%]
    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 64%]
    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 96%]
    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                                 [100%]stdout: 
    225 passed in 1.02s
- **intersection-experiment: passed.** `.venv/bin/python .specdev/cache/perf-deletion/intersection.py` (1867 ms, working-tree@5e0cb43280bc4e73ea938ea6787398ad6664cb60)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: {"keys": 1, "microseconds": {"set": 0.15334051568061113, "iterator": 0.15249999705702066}}
    {"keys": 100, "microseconds": {"set": 3.850829671137035, "iterator": 3.2591598574072123}}
    {"keys": 10000, "microseconds": {"set": 335.21207980811596, "iterator": 390.5654198024422}}
    {"keys": 20000, "microseconds": {"set": 659.2966703465208, "iterator": 644.0887501230463}}
    Iterator consumption: ['a', 'b', 'tail']
- **final-performance: failed.** `.venv/bin/python .specdev/cache/perf-deletion/simplification.py` (997 ms, working-tree@5e0cb43280bc4e73ea938ea6787398ad6664cb60)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 1
  - Output:

    stdout: {"case": "public_delete", "median_ms": {"before": 1.123, "after": 1.1051}}
    stdout: {"case": "partial_delete", "median_ms": {"before": 1.5428, "after": 1.5972}}
    stdout: {"case": "healthy_no_match", "median_ms": {"before": 0.9114, "after": 0.928}}
    stderr: Traceback (most recent call last):
      File "/Users/leiwu/code/oceanwave/lib/jsonldb/.specdev/cache/perf-deletion/simplification.py", line 60, in <module>
    stderr:     assert db.get_dbmeta()['group.branch.table']['count'] == expected
    stderr:            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    AssertionError
- **final-performance: passed.** `.venv/bin/python .specdev/cache/perf-deletion/simplification.py` (1538 ms, working-tree@5e0cb43280bc4e73ea938ea6787398ad6664cb60)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: {"case": "public_delete", "median_ms": {"before": 0.9713, "after": 0.9757}}
    stdout: {"case": "partial_delete", "median_ms": {"before": 1.4358, "after": 1.3847}}
    stdout: {"case": "healthy_no_match", "median_ms": {"before": 0.8157, "after": 0.8243}}
    stdout: {"case": "retry_metadata", "median_ms": {"before": 1.0433, "after": 1.0457}}
    stdout: {"case": "remove_last", "median_ms": {"before": 0.428, "after": 0.4151}}
- **final-regressions: passed.** `.venv/bin/python -m pytest -q unit_tests/test_remove_empty.py unit_tests/test_metadata_upkeep.py unit_tests/test_folderdb.py unit_tests/test_jsonlfile.py unit_tests/test_index_cache.py unit_tests/test_table_timezone.py unit_tests/test_max_hierarchy.py unit_tests/test_metaslot.py` (1965 ms, working-tree@5e0cb43280bc4e73ea938ea6787398ad6664cb60)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    tdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 41%]
    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 61%]
    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 82%]
    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:             [100%]stdout: 
    349 passed in 1.63s
- **diff-check: passed.** `git diff --check` (11 ms, working-tree@5e0cb43280bc4e73ea938ea6787398ad6664cb60)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    (no output)

## Current acceptance evidence

- **shared-return: passed.** `.venv/bin/python -m pytest -q unit_tests/test_remove_empty.py unit_tests/test_jsonlfile.py unit_tests/test_table_timezone.py` (1283 ms, working-tree@5e0cb43280bc4e73ea938ea6787398ad6664cb60)
- **intersection-experiment: passed.** `.venv/bin/python .specdev/cache/perf-deletion/intersection.py` (1867 ms, working-tree@5e0cb43280bc4e73ea938ea6787398ad6664cb60)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: {"keys": 1, "microseconds": {"set": 0.15334051568061113, "iterator": 0.15249999705702066}}
    {"keys": 100, "microseconds": {"set": 3.850829671137035, "iterator": 3.2591598574072123}}
    {"keys": 10000, "microseconds": {"set": 335.21207980811596, "iterator": 390.5654198024422}}
    {"keys": 20000, "microseconds": {"set": 659.2966703465208, "iterator": 644.0887501230463}}
    Iterator consumption: ['a', 'b', 'tail']
- **final-performance: passed.** `.venv/bin/python .specdev/cache/perf-deletion/simplification.py` (1538 ms, working-tree@5e0cb43280bc4e73ea938ea6787398ad6664cb60)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: {"case": "public_delete", "median_ms": {"before": 0.9713, "after": 0.9757}}
    stdout: {"case": "partial_delete", "median_ms": {"before": 1.4358, "after": 1.3847}}
    stdout: {"case": "healthy_no_match", "median_ms": {"before": 0.8157, "after": 0.8243}}
    stdout: {"case": "retry_metadata", "median_ms": {"before": 1.0433, "after": 1.0457}}
    stdout: {"case": "remove_last", "median_ms": {"before": 0.428, "after": 0.4151}}
- **final-regressions: passed.** `.venv/bin/python -m pytest -q unit_tests/test_remove_empty.py unit_tests/test_metadata_upkeep.py unit_tests/test_folderdb.py unit_tests/test_jsonlfile.py unit_tests/test_index_cache.py unit_tests/test_table_timezone.py unit_tests/test_max_hierarchy.py unit_tests/test_metaslot.py` (1965 ms, working-tree@5e0cb43280bc4e73ea938ea6787398ad6664cb60)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    tdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 41%]
    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 61%]
    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 82%]
    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:             [100%]stdout: 
    349 passed in 1.63s
- **diff-check: passed.** `git diff --check` (11 ms, working-tree@5e0cb43280bc4e73ea938ea6787398ad6664cb60)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    (no output)

## Structured verification

    {
      "version": 1,
      "path_facts": {
        "requested": [],
        "committed": [
          ".specdev/adhoc/2026-10/AH-20261013T114218892Z-33a6_simplify-deletion-control-flow-benchmark-iterato.md",
          "jsonldb/jsonlfile.py",
          "unit_tests/test_remove_empty.py"
        ],
        "rejected": [],
        "remaining": []
      },
      "attempt_history": [
        {
          "version": 1,
          "id": "V-001",
          "label": "shared-return",
          "annotation": null,
          "command": ".venv/bin/python -m pytest -q unit_tests/test_remove_empty.py unit_tests/test_jsonlfile.py unit_tests/test_table_timezone.py",
          "argv": [
            ".venv/bin/python",
            "-m",
            "pytest",
            "-q",
            "unit_tests/test_remove_empty.py",
            "unit_tests/test_jsonlfile.py",
            "unit_tests/test_table_timezone.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-10-13T11:42:29.059Z",
          "completed_at": "2026-10-13T11:42:30.342Z",
          "duration_ms": 1283,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@5e0cb43280bc4e73ea938ea6787398ad6664cb60",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 32%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 64%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 96%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                                 [100%]stdout: \n225 passed in 1.02s",
            "truncated": false,
            "captured_bytes": 340
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "intersection-experiment",
          "annotation": null,
          "command": ".venv/bin/python .specdev/cache/perf-deletion/intersection.py",
          "argv": [
            ".venv/bin/python",
            ".specdev/cache/perf-deletion/intersection.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-10-13T11:42:44.111Z",
          "completed_at": "2026-10-13T11:42:45.978Z",
          "duration_ms": 1867,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@5e0cb43280bc4e73ea938ea6787398ad6664cb60",
          "output": {
            "text": "stdout: {\"keys\": 1, \"microseconds\": {\"set\": 0.15334051568061113, \"iterator\": 0.15249999705702066}}\n{\"keys\": 100, \"microseconds\": {\"set\": 3.850829671137035, \"iterator\": 3.2591598574072123}}\n{\"keys\": 10000, \"microseconds\": {\"set\": 335.21207980811596, \"iterator\": 390.5654198024422}}\n{\"keys\": 20000, \"microseconds\": {\"set\": 659.2966703465208, \"iterator\": 644.0887501230463}}\nIterator consumption: ['a', 'b', 'tail']",
            "truncated": false,
            "captured_bytes": 405
          }
        },
        {
          "version": 1,
          "id": "V-003",
          "label": "final-performance",
          "annotation": null,
          "command": ".venv/bin/python .specdev/cache/perf-deletion/simplification.py",
          "argv": [
            ".venv/bin/python",
            ".specdev/cache/perf-deletion/simplification.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-10-13T11:43:18.228Z",
          "completed_at": "2026-10-13T11:43:19.226Z",
          "duration_ms": 997,
          "exit_status": 1,
          "status": "failed",
          "tested_revision": "working-tree@5e0cb43280bc4e73ea938ea6787398ad6664cb60",
          "output": {
            "text": "stdout: {\"case\": \"public_delete\", \"median_ms\": {\"before\": 1.123, \"after\": 1.1051}}\nstdout: {\"case\": \"partial_delete\", \"median_ms\": {\"before\": 1.5428, \"after\": 1.5972}}\nstdout: {\"case\": \"healthy_no_match\", \"median_ms\": {\"before\": 0.9114, \"after\": 0.928}}\nstderr: Traceback (most recent call last):\n  File \"/Users/leiwu/code/oceanwave/lib/jsonldb/.specdev/cache/perf-deletion/simplification.py\", line 60, in <module>\nstderr:     assert db.get_dbmeta()['group.branch.table']['count'] == expected\nstderr:            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\nAssertionError",
            "truncated": false,
            "captured_bytes": 538
          }
        },
        {
          "version": 1,
          "id": "V-004",
          "label": "final-performance",
          "annotation": null,
          "command": ".venv/bin/python .specdev/cache/perf-deletion/simplification.py",
          "argv": [
            ".venv/bin/python",
            ".specdev/cache/perf-deletion/simplification.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-10-13T11:43:43.504Z",
          "completed_at": "2026-10-13T11:43:45.042Z",
          "duration_ms": 1538,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@5e0cb43280bc4e73ea938ea6787398ad6664cb60",
          "output": {
            "text": "stdout: {\"case\": \"public_delete\", \"median_ms\": {\"before\": 0.9713, \"after\": 0.9757}}\nstdout: {\"case\": \"partial_delete\", \"median_ms\": {\"before\": 1.4358, \"after\": 1.3847}}\nstdout: {\"case\": \"healthy_no_match\", \"median_ms\": {\"before\": 0.8157, \"after\": 0.8243}}\nstdout: {\"case\": \"retry_metadata\", \"median_ms\": {\"before\": 1.0433, \"after\": 1.0457}}\nstdout: {\"case\": \"remove_last\", \"median_ms\": {\"before\": 0.428, \"after\": 0.4151}}",
            "truncated": false,
            "captured_bytes": 382
          }
        },
        {
          "version": 1,
          "id": "V-005",
          "label": "final-regressions",
          "annotation": null,
          "command": ".venv/bin/python -m pytest -q unit_tests/test_remove_empty.py unit_tests/test_metadata_upkeep.py unit_tests/test_folderdb.py unit_tests/test_jsonlfile.py unit_tests/test_index_cache.py unit_tests/test_table_timezone.py unit_tests/test_max_hierarchy.py unit_tests/test_metaslot.py",
          "argv": [
            ".venv/bin/python",
            "-m",
            "pytest",
            "-q",
            "unit_tests/test_remove_empty.py",
            "unit_tests/test_metadata_upkeep.py",
            "unit_tests/test_folderdb.py",
            "unit_tests/test_jsonlfile.py",
            "unit_tests/test_index_cache.py",
            "unit_tests/test_table_timezone.py",
            "unit_tests/test_max_hierarchy.py",
            "unit_tests/test_metaslot.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-10-13T11:43:51.651Z",
          "completed_at": "2026-10-13T11:43:53.615Z",
          "duration_ms": 1965,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@5e0cb43280bc4e73ea938ea6787398ad6664cb60",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 20%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 41%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 61%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 82%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:             [100%]stdout: \n349 passed in 1.63s",
            "truncated": false,
            "captured_bytes": 420
          }
        },
        {
          "version": 1,
          "id": "V-006",
          "label": "diff-check",
          "annotation": null,
          "command": "git diff --check",
          "argv": [
            "git",
            "diff",
            "--check"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-10-13T11:44:06.648Z",
          "completed_at": "2026-10-13T11:44:06.660Z",
          "duration_ms": 11,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@5e0cb43280bc4e73ea938ea6787398ad6664cb60",
          "output": {
            "text": "",
            "truncated": false,
            "captured_bytes": 0
          }
        }
      ],
      "acceptance_evidence": [
        {
          "version": 1,
          "id": "V-001",
          "label": "shared-return",
          "annotation": null,
          "command": ".venv/bin/python -m pytest -q unit_tests/test_remove_empty.py unit_tests/test_jsonlfile.py unit_tests/test_table_timezone.py",
          "argv": [
            ".venv/bin/python",
            "-m",
            "pytest",
            "-q",
            "unit_tests/test_remove_empty.py",
            "unit_tests/test_jsonlfile.py",
            "unit_tests/test_table_timezone.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-10-13T11:42:29.059Z",
          "completed_at": "2026-10-13T11:42:30.342Z",
          "duration_ms": 1283,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@5e0cb43280bc4e73ea938ea6787398ad6664cb60",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 32%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 64%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 96%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                                 [100%]stdout: \n225 passed in 1.02s",
            "truncated": false,
            "captured_bytes": 340
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "intersection-experiment",
          "annotation": null,
          "command": ".venv/bin/python .specdev/cache/perf-deletion/intersection.py",
          "argv": [
            ".venv/bin/python",
            ".specdev/cache/perf-deletion/intersection.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-10-13T11:42:44.111Z",
          "completed_at": "2026-10-13T11:42:45.978Z",
          "duration_ms": 1867,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@5e0cb43280bc4e73ea938ea6787398ad6664cb60",
          "output": {
            "text": "stdout: {\"keys\": 1, \"microseconds\": {\"set\": 0.15334051568061113, \"iterator\": 0.15249999705702066}}\n{\"keys\": 100, \"microseconds\": {\"set\": 3.850829671137035, \"iterator\": 3.2591598574072123}}\n{\"keys\": 10000, \"microseconds\": {\"set\": 335.21207980811596, \"iterator\": 390.5654198024422}}\n{\"keys\": 20000, \"microseconds\": {\"set\": 659.2966703465208, \"iterator\": 644.0887501230463}}\nIterator consumption: ['a', 'b', 'tail']",
            "truncated": false,
            "captured_bytes": 405
          }
        },
        {
          "version": 1,
          "id": "V-004",
          "label": "final-performance",
          "annotation": null,
          "command": ".venv/bin/python .specdev/cache/perf-deletion/simplification.py",
          "argv": [
            ".venv/bin/python",
            ".specdev/cache/perf-deletion/simplification.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-10-13T11:43:43.504Z",
          "completed_at": "2026-10-13T11:43:45.042Z",
          "duration_ms": 1538,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@5e0cb43280bc4e73ea938ea6787398ad6664cb60",
          "output": {
            "text": "stdout: {\"case\": \"public_delete\", \"median_ms\": {\"before\": 0.9713, \"after\": 0.9757}}\nstdout: {\"case\": \"partial_delete\", \"median_ms\": {\"before\": 1.4358, \"after\": 1.3847}}\nstdout: {\"case\": \"healthy_no_match\", \"median_ms\": {\"before\": 0.8157, \"after\": 0.8243}}\nstdout: {\"case\": \"retry_metadata\", \"median_ms\": {\"before\": 1.0433, \"after\": 1.0457}}\nstdout: {\"case\": \"remove_last\", \"median_ms\": {\"before\": 0.428, \"after\": 0.4151}}",
            "truncated": false,
            "captured_bytes": 382
          }
        },
        {
          "version": 1,
          "id": "V-005",
          "label": "final-regressions",
          "annotation": null,
          "command": ".venv/bin/python -m pytest -q unit_tests/test_remove_empty.py unit_tests/test_metadata_upkeep.py unit_tests/test_folderdb.py unit_tests/test_jsonlfile.py unit_tests/test_index_cache.py unit_tests/test_table_timezone.py unit_tests/test_max_hierarchy.py unit_tests/test_metaslot.py",
          "argv": [
            ".venv/bin/python",
            "-m",
            "pytest",
            "-q",
            "unit_tests/test_remove_empty.py",
            "unit_tests/test_metadata_upkeep.py",
            "unit_tests/test_folderdb.py",
            "unit_tests/test_jsonlfile.py",
            "unit_tests/test_index_cache.py",
            "unit_tests/test_table_timezone.py",
            "unit_tests/test_max_hierarchy.py",
            "unit_tests/test_metaslot.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-10-13T11:43:51.651Z",
          "completed_at": "2026-10-13T11:43:53.615Z",
          "duration_ms": 1965,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@5e0cb43280bc4e73ea938ea6787398ad6664cb60",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 20%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 41%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 61%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: . [ 82%]\nstdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:             [100%]stdout: \n349 passed in 1.63s",
            "truncated": false,
            "captured_bytes": 420
          }
        },
        {
          "version": 1,
          "id": "V-006",
          "label": "diff-check",
          "annotation": null,
          "command": "git diff --check",
          "argv": [
            "git",
            "diff",
            "--check"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-10-13T11:44:06.648Z",
          "completed_at": "2026-10-13T11:44:06.660Z",
          "duration_ms": 11,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@5e0cb43280bc4e73ea938ea6787398ad6664cb60",
          "output": {
            "text": "",
            "truncated": false,
            "captured_bytes": 0
          }
        }
      ]
    }
