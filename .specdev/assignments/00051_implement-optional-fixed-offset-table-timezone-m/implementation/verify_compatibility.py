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
# Pre-metadata fixtures: preserve raw string/naive/aware keys and CRUD semantics.
from datetime import timezone, timedelta
legacy_results = []
legacy = root / 'historical.jsonl'
fixture = b'{"2020-01-01T00:00:00":{"v":1}}\n{"2020-01-01T00:00:01+08:00":{"v":2}}\n{"plain":{"v":3}}\n'
for spec in ('seconds', 'microseconds'):
    legacy.write_bytes(fixture)
    if Path(str(legacy)+'.idx').exists():
        Path(str(legacy)+'.idx').unlink()
    legacy_results.append(repr(jsonlfile.load_jsonl(str(legacy), timespec=spec)))
    legacy_results.append(repr(jsonldf.load_jsonldf(str(legacy), timespec=spec)))
    key = datetime(2020,1,1,0,0,1,tzinfo=timezone(timedelta(hours=8)))
    for lower, upper in ((None,None),(key,key),(None,key),(key,None),('plain','plain')):
        legacy_results.append(repr(jsonlfile.select_jsonl(str(legacy),lower,upper,True,spec)))
    jsonlfile.update_jsonl(str(legacy), {key: {'v': 7}, 'new': {'v': 8}}, timespec=spec)
    jsonlfile.delete_jsonl(str(legacy), ['plain'], timespec=spec)
    jsonlfile.lint_jsonl(str(legacy), force=True)
    legacy_results.append(legacy.read_bytes().hex())
    legacy_results.append(Path(str(legacy)+'.idx').read_bytes().hex())
    jsonlfile.save_jsonl(str(legacy), {'old-style': {'v': 9}}, timespec=spec)
    assert not legacy.read_bytes().startswith(b'{"_meta"')
print(json.dumps({'signatures': signatures, 'snapshots': snapshots, 'legacy': legacy_results}, sort_keys=True))
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
    added = {'jsonldb.jsonlfile.read_jsonl_timezone', 'jsonldb.jsonlfile.write_jsonl_timezone',
             'FolderDB.read_timezone', 'FolderDB.set_timezone'}
    assert set(results[1]['signatures']) - set(results[0]['signatures']) == added
    assert all(results[1]['signatures'].get(k) == v for k, v in results[0]['signatures'].items()), 'public signature changed'
    assert results[0]['snapshots'] == results[1]['snapshots'], 'storage bytes changed'
    assert results[0]['legacy'] == results[1]['legacy'], 'historical file behavior changed'
    print(json.dumps({'public_signatures_equal': len(results[0]['signatures']),
                      'storage_snapshots_equal': len(results[0]['snapshots']),
                      'legacy_observations_equal': len(results[0]['legacy']), 'additive_timezone_apis': 4}))


if __name__ == '__main__':
    main()
