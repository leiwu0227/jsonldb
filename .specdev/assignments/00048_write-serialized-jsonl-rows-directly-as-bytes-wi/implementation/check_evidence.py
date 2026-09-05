"""Validate durable benchmark samples, identities, aggregates and acceptance."""
import hashlib
import json
from pathlib import Path
import statistics
import subprocess


def main():
    root = Path(__file__).resolve().parent
    baseline = '784b038'
    for prefix, pairs, summary_name in [('', 4, 'benchmark-summary.json'),
                                         ('small-', 2, 'small-summary.json')]:
        summary = json.loads((root / summary_name).read_text())
        for variant in ('baseline', 'candidate'):
            runs = [json.loads((root / f'{prefix}{variant}-{i+1}.json').read_text())
                    for i in range(pairs)]
            for run in runs:
                for name, digest in run['source_hashes'].items():
                    source = (Path('jsonldb', name).read_bytes() if variant == 'candidate'
                              else subprocess.check_output(['git', 'show', f'{baseline}:jsonldb/{name}']))
                    assert hashlib.sha256(source).hexdigest() == digest
                for result in run['operations'].values():
                    assert len(result['samples_ms']) == run['repeats']
                    assert statistics.median(result['samples_ms']) == result['median_ms']
            for name in runs[0]['operations']:
                assert summary[name][variant] == statistics.median(
                    run['operations'][name]['median_ms'] for run in runs)
        for value in summary.values():
            assert value['speedup'] == value['baseline'] / value['candidate']
    summary = json.loads((root / 'benchmark-summary.json').read_text())
    assert summary['100000/lowlevel/overwrite']['speedup'] > 1
    assert len(Path('jsonldb/jsonlfile.py').read_text().splitlines()) <= 950
    print('Benchmark source identities, samples, aggregates, save benefit and line cap verified.')


if __name__ == '__main__':
    main()
