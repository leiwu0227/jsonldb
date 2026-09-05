# Adhoc AH-20260905T032539911Z-ac7b

- Scope: Preserve well-formed unknown-version first-line _meta envelopes during lint and row compaction; resize padding without interpreting their contents or refuse before mutation when the envelope cannot fit
- Title: Preserve unknown metadata envelopes during lint
- Started: 2026-09-05T03:25:39.911Z
- Completed: 2026-09-05T03:32:14.136Z
- Starting working tree: Clean.

## Outcome

Moved lint slot selection into metaslot and preserved well-formed unknown integer-version envelopes under the first-line reserved _meta key. Ordinary lint and compaction retain their raw bytes; configured-width mismatches adjust only padding when the opaque envelope fits, otherwise raise before changing the table. Missing trailing newlines are healed, malformed envelopes remain repairable, and padding-only repairs do not report the preserved envelope contents as removed. Reads still return no decoded record for unknown envelope versions. Regression coverage confirms that consumer v fields in ordinary rows and inside _meta.data remain opaque. All 21 new cases and 70 focused metadata, lint, migration, compliance, and report regressions pass (91 total); git diff --check passes and source caps remain satisfied (metaslot 135/250, jsonlfile 940/950 lines).

## Focused workflow coexistence

No focused Assignment or Mission coexistence was recorded.

## Delivery path facts

### Requested adopted paths

None.

### Committed paths

- `.specdev/adhoc/2026-09/AH-20260905T032539911Z-ac7b_preserve-well-formed-unknown-version-first-line-.md`
- `jsonldb/jsonlfile.py`
- `jsonldb/metaslot.py`
- `unit_tests/test_unknown_metadata_lint.py`

### Rejected paths

None.

### Remaining owned paths

None.

## Verification summary

No manual verification summary was supplied.

## Verification attempt history

- **unknown-envelope-lint: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_unknown_metadata_lint.py` (635 ms, working-tree@82856fa49392924c30dac0714cfffd071e7109f8)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                     [100%]stdout: 
    21 passed in 0.50s
- **metadata-lint-regressions: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_metaslot.py unit_tests/test_lint_integrity.py unit_tests/test_metadata_slot_migration.py unit_tests/test_roadmap_compliance.py unit_tests/test_reports.py` (1143 ms, working-tree@82856fa49392924c30dac0714cfffd071e7109f8)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:    [100%]stdout: 
    70 passed in 1.00s

## Current acceptance evidence

- **unknown-envelope-lint: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_unknown_metadata_lint.py` (635 ms, working-tree@82856fa49392924c30dac0714cfffd071e7109f8)
- **metadata-lint-regressions: passed.** `.specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_metaslot.py unit_tests/test_lint_integrity.py unit_tests/test_metadata_slot_migration.py unit_tests/test_roadmap_compliance.py unit_tests/test_reports.py` (1143 ms, working-tree@82856fa49392924c30dac0714cfffd071e7109f8)
  - Working directory: `/Users/leiwu/code/oceanwave/lib/jsonldb`
  - Exit status: 0
  - Output:

    stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:    [100%]stdout: 
    70 passed in 1.00s

## Structured verification

    {
      "version": 1,
      "path_facts": {
        "requested": [],
        "committed": [
          ".specdev/adhoc/2026-09/AH-20260905T032539911Z-ac7b_preserve-well-formed-unknown-version-first-line-.md",
          "jsonldb/jsonlfile.py",
          "jsonldb/metaslot.py",
          "unit_tests/test_unknown_metadata_lint.py"
        ],
        "rejected": [],
        "remaining": []
      },
      "attempt_history": [
        {
          "version": 1,
          "id": "V-001",
          "label": "unknown-envelope-lint",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_unknown_metadata_lint.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_unknown_metadata_lint.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T03:30:25.386Z",
          "completed_at": "2026-09-05T03:30:26.022Z",
          "duration_ms": 635,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@82856fa49392924c30dac0714cfffd071e7109f8",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                     [100%]stdout: \n21 passed in 0.50s",
            "truncated": false,
            "captured_bytes": 99
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "metadata-lint-regressions",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_metaslot.py unit_tests/test_lint_integrity.py unit_tests/test_metadata_slot_migration.py unit_tests/test_roadmap_compliance.py unit_tests/test_reports.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_metaslot.py",
            "unit_tests/test_lint_integrity.py",
            "unit_tests/test_metadata_slot_migration.py",
            "unit_tests/test_roadmap_compliance.py",
            "unit_tests/test_reports.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T03:31:10.903Z",
          "completed_at": "2026-09-05T03:31:12.047Z",
          "duration_ms": 1143,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@82856fa49392924c30dac0714cfffd071e7109f8",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:    [100%]stdout: \n70 passed in 1.00s",
            "truncated": false,
            "captured_bytes": 99
          }
        }
      ],
      "acceptance_evidence": [
        {
          "version": 1,
          "id": "V-001",
          "label": "unknown-envelope-lint",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_unknown_metadata_lint.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_unknown_metadata_lint.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T03:30:25.386Z",
          "completed_at": "2026-09-05T03:30:26.022Z",
          "duration_ms": 635,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@82856fa49392924c30dac0714cfffd071e7109f8",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:                                                     [100%]stdout: \n21 passed in 0.50s",
            "truncated": false,
            "captured_bytes": 99
          }
        },
        {
          "version": 1,
          "id": "V-002",
          "label": "metadata-lint-regressions",
          "annotation": null,
          "command": ".specdev/cache/bin/python-runtime-env python3 -m pytest -q -p no:cacheprovider unit_tests/test_metaslot.py unit_tests/test_lint_integrity.py unit_tests/test_metadata_slot_migration.py unit_tests/test_roadmap_compliance.py unit_tests/test_reports.py",
          "argv": [
            ".specdev/cache/bin/python-runtime-env",
            "python3",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "unit_tests/test_metaslot.py",
            "unit_tests/test_lint_integrity.py",
            "unit_tests/test_metadata_slot_migration.py",
            "unit_tests/test_roadmap_compliance.py",
            "unit_tests/test_reports.py"
          ],
          "working_directory": "/Users/leiwu/code/oceanwave/lib/jsonldb",
          "started_at": "2026-09-05T03:31:10.903Z",
          "completed_at": "2026-09-05T03:31:12.047Z",
          "duration_ms": 1143,
          "exit_status": 0,
          "status": "passed",
          "tested_revision": "working-tree@82856fa49392924c30dac0714cfffd071e7109f8",
          "output": {
            "text": "stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout: .stdout:    [100%]stdout: \n70 passed in 1.00s",
            "truncated": false,
            "captured_bytes": 99
          }
        }
      ]
    }
