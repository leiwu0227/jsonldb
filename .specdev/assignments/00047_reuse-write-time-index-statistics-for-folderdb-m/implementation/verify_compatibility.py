"""Compare public signatures and deterministic storage bytes with a baseline root."""
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


CASE = r'''
import hashlib, inspect, json, sys
from datetime import datetime
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from jsonldb import FolderDB, jsonlfile, jsonldf
import pandas as pd
root = Path(sys.argv[2])
signatures = {}
for owner in (jsonlfile, jsonldf, FolderDB):
    for name, value in vars(owner).items():
        if not name.startswith('_') and inspect.isfunction(value):
            signatures[owner.__name__ + '.' + name] = str(inspect.signature(value))
jsonlfile.save_jsonl(str(root / 'config.meta'), {'config': {'timespec': 'microseconds'}})
db = FolderDB(str(root), hierarchy_depth=1)
snapshots = []
def snapshot():
    snapshots.append({str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                      for p in root.rglob('*') if p.is_file() and '.jsonldb' not in p.parts})
for slot in (False, True):
    if slot:
        db.set_meta_slot_bytes(256)
    for kind in ('dict', 'df'):
        for operation in ('overwrite', 'upsert'):
            for rows in ({'z': {'v': 1}, 'a': {'v': 2}},
                         {'m': {'v': 'longer row'}, 'zz': {'v': 3}}, {},
                         {datetime(2026, 9, 5): {'v': 4}, '2026-09-05T00:00:00.000000': {'v': 5}},
                         {'\uffff': {'v': 6}, '\U00010000': {'v': 7}}):
                data = (pd.DataFrame(list(rows.values()), index=pd.Index(list(rows), dtype=object))
                        if kind == 'df' else rows)
                assert getattr(db, operation + '_' + kind)('region.table', data,
                       meta={'version': 1} if slot else None) is None
                snapshot()
print(json.dumps({'signatures': signatures, 'snapshots': snapshots}, sort_keys=True))
'''


def main():
    baseline = Path(sys.argv[1]).resolve()
    candidate = Path.cwd()
    with tempfile.TemporaryDirectory(prefix='jsonldb-compatibility-') as temp:
        fixture = Path(temp) / 'fixture'
        results = []
        for source in (baseline, candidate):
            fixture.mkdir()
            output = subprocess.check_output([sys.executable, '-c', CASE,
                                              str(source), str(fixture)], text=True)
            results.append(json.loads(output))
            shutil.rmtree(fixture)
    assert results[0]['signatures'] == results[1]['signatures'], 'public signature changed'
    assert results[0]['snapshots'] == results[1]['snapshots'], 'storage bytes changed'
    print(json.dumps({'public_signatures_equal': len(results[0]['signatures']),
                      'storage_snapshots_equal': len(results[0]['snapshots'])}))


if __name__ == '__main__':
    main()
