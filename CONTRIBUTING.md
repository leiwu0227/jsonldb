# Developing JSONLDB

Start with the [README](README.md) for the project's time-series focus and
[design decisions](docs/design.md) for storage and failure boundaries. Changes
should preserve readable files, portable interpretation and historical callers.

## Local setup

From a clone of this repository:

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements-dev.lock
python -m pip install --no-deps -e .
```

On Windows, activate the environment using the appropriate script under
`.venv\Scripts`. `requirements-dev.lock` is generated from the declared package
dependencies and `requirements-dev.in`. To regenerate it after a dependency change:

```bash
uv pip compile setup.py requirements-dev.in \
    --python-version 3.8 --universal --generate-hashes \
    --output-file requirements-dev.lock
```

The package declares Python >= 3.8 and minimum pandas/NumPy/orjson versions in
`setup.py`. A declared minimum is not a claim that every runtime combination has
been tested for every change. Record the Python and dependency versions exercised,
and identify any unavailable coverage in the change's verification notes.

## Verification

Run focused tests appropriate to the change, for example:

```bash
python -m pytest -q unit_tests/test_jsonlfile.py unit_tests/test_folderdb.py
```

Storage changes often need failure-injection cases as well as successful CRUD
checks: verify publication order, index rebuilding, byte preservation, missing
files, metadata/precision handling and existing caller behavior where relevant.
Use temporary databases; do not run destructive examples against real data.

For an explicitly requested full repository check, use `python -m pytest -q`.
Agents must follow the repository's verification authorization rules instead of
automatically expanding a narrow check to the full suite.

## Performance

The tracked benchmark entry point supports before/after measurements:

```bash
python profile_test/benchmark.py --save baseline.json
python profile_test/benchmark.py --compare baseline.json
python profile_test/benchmark.py --index-cache --save cache-baseline.json
```

Use the same environment and hardware, and distinguish cold reads, warm reads,
writes and cache pressure. `profile_test/` also contains focused DataFrame,
datetime-key and write-statistics benchmarks. Timings are workload evidence,
not universal speed guarantees. Keep generated measurement output outside commits
unless it is deliberately included as review evidence.

## Repository orientation

| Path | Purpose |
| --- | --- |
| `jsonldb/` | Installable library. |
| `unit_tests/` | Regression and storage-behavior tests. |
| `docs/` | Current public usage, API, format and design guides. |
| `examples/` | Two maintained, self-contained notebooks using temporary datasets. |
| `profile_test/` | Tracked benchmark programs and local measurement output. |
| `.specdev/` | Maintainer workflow, specifications and delivery history. |
| `.codex/` | Repository agent skills. |

For agent-assisted work, read repository instructions and `.specdev/_main.md`.
Do not treat old proposals or notebook output as the current public contract.
Keep public documentation examples consistent with the implementation. Source
changes must honor applicable physical-line caps from approved specifications.

When proposing a change, state the user-visible problem, the resulting behavior,
the focused verification performed, and any remaining limitations. Separate
behavior changes from generated artifacts so the diff is reviewable.
