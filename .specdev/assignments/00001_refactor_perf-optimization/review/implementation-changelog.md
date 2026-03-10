## Round 1

- [F1.1] **Addressed.** Replaced all three `json.load` calls in `folderdb.py` (`delete_file_range`, `build_dbmeta`, `update_dbmeta`) with `orjson.loads(f.read())` using binary file mode. Also replaced `import json` with `import orjson` in folderdb.py. All 61 tests pass.
- [F1.2] **Addressed.** Added `--save FILE` and `--compare FILE` flags to `benchmark.py`. `--save` writes results to a JSON file; `--compare` loads a baseline and prints a side-by-side before/after comparison table with speedup ratios.
