"""Check sample provenance, aggregates, target gains and source constraints."""
import hashlib
import json
from pathlib import Path
import statistics
import subprocess


def main():
    root = Path(__file__).resolve().parent
    for path in root.glob('*.json'):
        run = json.loads(path.read_text())
        if 'operations' not in run:
            continue
        for name, digest in run['source_hashes'].items():
            data = (subprocess.check_output(['git', 'show', '5276e8e:jsonldb/'+name])
                    if 'baseline' in path.name else Path('jsonldb', name).read_bytes())
            assert hashlib.sha256(data).hexdigest() == digest
        for value in run['operations'].values():
            assert len(value['samples_ms']) == run['repeats']
            assert statistics.median(value['samples_ms']) == value['median_ms']
    for prefix, pairs, name in [('', 3, 'benchmark-summary.json'), ('small-', 2, 'small-summary.json')]:
        summary = json.loads((root/name).read_text())
        for variant in ('baseline', 'candidate'):
            runs = [json.loads((root/f'{prefix}{variant}-{i+1}.json').read_text()) for i in range(pairs)]
            for operation in runs[0]['operations']:
                assert summary[operation][variant] == statistics.median(
                    run['operations'][operation]['median_ms'] for run in runs)
        for value in summary.values():
            assert value['speedup'] == value['baseline']/value['candidate']
    summary = json.loads((root/'benchmark-summary.json').read_text())
    for shape in ('numeric_1', 'numeric_8', 'mixed_8'):
        assert summary[f'100000/{shape}/overwrite']['speedup'] > 1
    for name, limit in [('jsonldf.py',150), ('jsonlfile.py',950), ('folderdb.py',1250)]:
        assert len(Path('jsonldb',name).read_text().splitlines()) <= limit
    print('Benchmark source identities, samples, aggregates, target gains and line caps verified.')


if __name__ == '__main__':
    main()
