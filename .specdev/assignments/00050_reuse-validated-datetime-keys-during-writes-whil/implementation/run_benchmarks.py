"""Alternate independent baseline/candidate processes and retain all samples."""
import json
from pathlib import Path
import statistics
import subprocess
import sys
import time

root = Path(__file__).resolve().parent
helper = Path('profile_test/benchmark_datetime_keys.py')
baseline = Path('.specdev/cache/datetime-baseline')
kind = sys.argv[1] if len(sys.argv)>1 else 'main'
pairs, options = {'main': (3, []), 'small': (2, ['--sizes', '0', '10', '1000', '--repeats', '101']),
                  'confirmation': (3, ['--sizes', '0', '10', '--repeats', '301']),
                  'memory': (1, ['--sizes', '20000', '--repeats', '1', '--memory'])}[kind]
start = time.perf_counter_ns()
for i in range(pairs):
    for variant in (['baseline', 'candidate'] if i%2==0 else ['candidate', 'baseline']):
        path = root/f'{kind}-{variant}-{i+1}.json'
        command = [sys.executable, str(helper), '--save', str(path), *options]
        if variant == 'baseline':
            command += ['--source-root', str(baseline)]
        run = subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        print(kind, i+1, variant, 'exit', run.returncode, flush=True)
        if run.returncode:
            print(run.stdout, flush=True)
            sys.exit(run.returncode)
summary = {}
for variant in ('baseline', 'candidate'):
    runs = [json.loads((root/f'{kind}-{variant}-{i+1}.json').read_text()) for i in range(pairs)]
    for op in runs[0]['operations']:
        summary.setdefault(op,{})[variant] = statistics.median(r['operations'][op]['median_ms'] for r in runs)
for op,v in summary.items():
    v['speedup'] = v['baseline']/v['candidate']
(root/f'{kind}-summary.json').write_text(json.dumps(summary,indent=2)+'\n')
(root/f'{kind}-run.json').write_text(json.dumps({'duration_ms': round((time.perf_counter_ns()-start)/1e6), 'pairs': pairs, 'options':options},indent=2)+'\n')
for op,v in summary.items():
    print(op, round(v['baseline'],3), round(v['candidate'],3), round(v['speedup'],3), flush=True)
