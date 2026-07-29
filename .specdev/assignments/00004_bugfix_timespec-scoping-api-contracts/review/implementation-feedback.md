## Round 1

**Verdict:** approved

### Findings
1. (none)

### Addressed from changelog
- (none -- first round)

### Verification
- `python -m pytest unit_tests/test_jsonlfile.py -k timespec -x -q` -> 1 passed, 36 deselected
- `python -m pytest unit_tests/test_folderdb.py -k timespec -x -q` -> 2 passed, 27 deselected
- `python -m pytest unit_tests/ -q` -> 78 passed
- `grep -n "TIME_SPEC = " jsonldb/folderdb.py` -> no matches
