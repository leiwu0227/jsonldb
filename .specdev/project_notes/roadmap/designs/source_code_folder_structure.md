# Source Code Folder Structure

```
jsonldb/                      repository root
  jsonldb/                    the installable package
    __init__.py               exports FolderDB; names the submodules
    jsonlfile.py              single-file JSONL store: format, index, CRUD, lint
    jsonldf.py                pandas DataFrame adapter over jsonlfile
    folderdb.py               FolderDB: multi-table folder database
    vercontrol.py             Git wrapper (GitPython)
    visual.py                 linekey distribution plots (Matplotlib, Bokeh)
  unit_tests/                 pytest suites, one per core module
    test_jsonlfile.py
    test_jsonldf.py
    test_folderdb.py
  profile_test/               benchmark and profiling scripts
    benchmark.py              timed scenarios with --save / --compare
    profile_jsonlfile.py      cProfile harness for the file layer
  examples/                   Jupyter notebooks grouped by module
    example_jsonlfile/  example_jsonldf/  example_folderdb/
    example_vercontrol/ example_visual/
  setup.py                    packaging metadata and install requirements
  README.md                   user-facing overview
  .prodinclude                allowlist of paths that ship to production
  .specdev/                   SpecDev workflow, project notes, roadmap
```

## Package layout rules

- The package is flat: one module per concern, no subpackages. Dependencies point downward only. `folderdb` imports `jsonldf` and `jsonlfile`; `jsonldf` imports `jsonlfile`; `jsonlfile` imports nothing from the package.
- `vercontrol` and `visual` are leaves. `vercontrol` depends on GitPython alone; `visual` depends on `jsonlfile` and `folderdb`. Neither is imported by `__init__` or by the core modules at import time.
- Heavy third-party imports live where they are used. `git` is imported only in `vercontrol`; Bokeh and Matplotlib only in `visual`; `pandas` and `orjson` are core dependencies imported by the file store.
- `__init__` exposes `FolderDB` eagerly and lists the submodule names so that callers import optional capabilities by module path.

## What is tracked and what ships

- `.prodinclude` defines the production surface: the package, `setup.py`, `README.md`, and the ignore file. Tests, profiling scripts, notebooks, and workflow folders are development-only.
- `unit_tests/`, `profile_test/`, `.journal/`, build outputs, and agent configuration are excluded from Git by the ignore file. The tracked tree is essentially the package, packaging files, `examples/`, and `.specdev/`.
- Build artefacts (`build/`, `dist/`, `*.egg-info/`) may exist locally and are never edited by hand.

## Where new code goes

- Behaviour of a single JSONL file, including index and lint rules, belongs in `jsonlfile`.
- Anything that needs a folder, a table name, or database configuration belongs in `FolderDB`.
- A new optional capability with its own heavy dependency becomes a new leaf module that is imported lazily, following the `vercontrol` pattern.
- Each core module keeps a matching `unit_tests/test_<module>.py`, and performance-sensitive changes add a scenario to `profile_test/benchmark.py`.
