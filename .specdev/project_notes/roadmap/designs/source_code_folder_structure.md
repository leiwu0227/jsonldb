# Source Code Folder Structure

```
jsonldb/                      repository root
  jsonldb/                    the installable package
    __init__.py               exports FolderDB; names the submodules
    metaslot.py               line one: envelope registry, classification, slot read and write
    jsonlfile.py              single-file JSONL store: format, index, CRUD, lint
    jsonldf.py                pandas DataFrame adapter over jsonlfile
    reports.py                logger capture and the two report-file writers
    folderdb.py               FolderDB: multi-table folder database
  unit_tests/                 pytest suites, one per core module
    test_jsonlfile.py
    test_jsonldf.py
    test_folderdb.py
  profile_test/               benchmark and profiling scripts
    benchmark.py              timed scenarios with --save / --compare
    profile_jsonlfile.py      cProfile harness for the file layer
  examples/                   Jupyter notebooks grouped by module
    example_jsonlfile/  example_jsonldf/  example_folderdb/
  setup.py                    packaging metadata and install requirements
  README.md                   user-facing overview
  .prodinclude                allowlist of paths that ship to production
  .specdev/                   SpecDev workflow, project notes, roadmap
```

## Package layout rules

- The package is flat: one module per concern, no subpackages. Dependencies point downward only. `folderdb` imports `jsonldf`, `jsonlfile`, and `reports`; `jsonldf` imports `jsonlfile`; `jsonlfile` imports `metaslot`; `metaslot` and `reports` import nothing from the package.
- `metaslot` owns everything about line one and knows nothing about rows, indexes, or folders. `jsonlfile` calls it to classify, read, and write the slot. This keeps the file store focused on rows and lets the envelope evolve in one place.
- `reports` owns the capture handler and the writers for the two report files and knows nothing about tables. `folderdb` calls it at open and at lint.
- Heavy third-party imports live where they are used. `pandas` and `orjson` are core dependencies imported by the layers that need them.
- `__init__` exposes `FolderDB` eagerly and lists the remaining submodule names.

## What is tracked and what ships

- `.prodinclude` defines the production surface: the package, `setup.py`, `README.md`, and the ignore file. Tests, profiling scripts, notebooks, and workflow folders are development-only.
- Build artefacts (`build/`, `dist/`, `*.egg-info/`) may exist locally and are never edited by hand.

## Where new code goes

- Behaviour of a single JSONL file, including index and lint rules, belongs in `jsonlfile`. Anything about the envelope on line one belongs in `metaslot`.
- Anything that needs a folder, a table name, or database configuration belongs in `FolderDB`. Report formatting and capture belong in `reports`.
- Each core module keeps a matching `unit_tests/test_<module>.py`, and performance-sensitive changes add a scenario to `profile_test/benchmark.py`.
