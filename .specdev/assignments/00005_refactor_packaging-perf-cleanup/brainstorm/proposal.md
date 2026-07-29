# Proposal: Packaging, performance, and cleanup sweep

The verification pass that produced assignments 00003/00004 also confirmed a set of
lower-severity but real defects. The worst for users: `jsonldb/jsonldf.py` imports `numba`
(never used, not in `install_requires`), so `from jsonldb import FolderDB` crashes on any
fresh install — it only works locally because numba happens to be installed. `setup.py`
also declares a console script (`jsonldb.cli:main`) that doesn't exist, omits matplotlib
while `visual.py` uses it, and disagrees with `__init__.py` about the version. The
`__init__.py` docstring promises modular imports, yet importing `FolderDB` eagerly loads
gitpython.

On top of that: every index write sorts twice (Python-side `dict(sorted(...))` plus orjson's
`OPT_SORT_KEYS`), `select_jsonl` does O(n) `min`/`max` over already-sorted keys, `save_jsonl`
buffers every encoded line in memory before writing, and there is verified dead code
(`check_dict_format` is never called; `deleted_line` in `delete_jsonl` is computed and
discarded) plus three near-identical copies of the metadata-entry builder in `folderdb.py`.

All changes are behavior-preserving; the existing 78-test suite is the safety net, plus one
new test asserting the import contract (no numba/git modules loaded by the core import).
