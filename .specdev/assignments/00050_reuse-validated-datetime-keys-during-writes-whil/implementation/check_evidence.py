"""Verify benchmark provenance, exact aggregates, source hashes and line caps."""
import ast
import hashlib
import json
from pathlib import Path
import statistics
import subprocess

root = Path(__file__).resolve().parent
for path in root.glob('*.json'):
    run = json.loads(path.read_text())
    if 'operations' not in run:
        continue
    for name, digest in run['source_hashes'].items():
        data = (subprocess.check_output(['git','show','0c54d29:jsonldb/'+name])
                if 'baseline' in path.name else Path('jsonldb',name).read_bytes())
        assert hashlib.sha256(data).hexdigest() == digest, path
    for value in run['operations'].values():
        assert len(value['samples_ms']) == run['repeats']
        assert statistics.median(value['samples_ms']) == value['median_ms']
for kind, pairs in [('main',3), ('small',2), ('memory',1), ('confirmation',3)]:
    summary = json.loads((root/f'{kind}-summary.json').read_text())
    for variant in ('baseline','candidate'):
        runs = [json.loads((root/f'{kind}-{variant}-{i+1}.json').read_text()) for i in range(pairs)]
        for op in runs[0]['operations']:
            assert summary[op][variant] == statistics.median(r['operations'][op]['median_ms'] for r in runs)
    for v in summary.values():
        assert v['speedup'] == v['baseline']/v['candidate']
summary = json.loads((root/'main-summary.json').read_text())
for op in ['datetime_1/overwrite', 'datetime_8/overwrite', 'datetime_mixed/overwrite',
           'datetime_dict/overwrite', 'datetime_8/upsert_100000']:
    assert summary['100000/'+op]['speedup'] > 1, op
counts = json.loads((root/'line-counts.json').read_text())
for name, cap in [('jsonlfile.py',950), ('jsonldf.py',150), ('folderdb.py',1250)]:
    path = Path('jsonldb',name)
    actual = len(path.read_text().splitlines())
    assert actual <= cap
    assert counts[str(path)] == {'lines':actual, 'limit':cap}
for name,digest in json.loads((root/'source-hashes.json').read_text()).items():
    data = Path(name).read_bytes()
    assert hashlib.sha256(data).hexdigest() == digest, name
    if name.endswith('.py'):
        ast.parse(data.decode(), feature_version=(3,8))
print('PASS: baseline/source identity, all sample aggregates, five performance targets, physical line caps and Python 3.8 syntax.')
