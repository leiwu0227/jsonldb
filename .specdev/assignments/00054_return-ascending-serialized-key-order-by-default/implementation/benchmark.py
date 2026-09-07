"""Compare baseline and final full reads in isolated, cache-warm processes."""
import datetime as dt
import gc
import hashlib
import importlib.util
import json
from pathlib import Path
import platform
import random
import resource
import statistics
import subprocess
import sys
import time

sys.path.insert(0, str(Path.cwd()))
from jsonldb import jsonlfile as current
import orjson

HERE = Path(__file__).resolve().parent
CACHE = Path('.specdev/cache/ordered-read-final')
BASELINE = 'a55e844fe9f372bf04c49f93ccf3757d3f5bbd24'


def worker(path, method, kind):
    reader = current
    if method == 'baseline':
        spec = importlib.util.spec_from_file_location('jsonldb._ordering_baseline', CACHE / 'baseline.py')
        reader = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(reader)
    gc.collect()
    start = time.perf_counter()
    rows = reader.load_jsonl(path, auto_deserialize=(kind == 'datetime'))
    elapsed = (time.perf_counter() - start) * 1000
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024**2 if sys.platform == 'darwin' else 1024)
    result = dict(ms=elapsed, peak_mib=peak, count=len(rows))
    # Verification follows timing and memory capture.
    previous = None
    for key, value in rows.items():
        assert value['missing'] is None and value['close'] == 1.25
        expected = ((dt.datetime(2000, 1, 1) + dt.timedelta(seconds=value['value']))
                    if kind == 'datetime' else
                    (dt.date(2000, 1, 1) + dt.timedelta(days=value['value'])).isoformat())
        assert key == expected
        if method == 'final':
            assert previous is None or previous < key
            previous = key
    print(json.dumps(result))


def main():
    CACHE.mkdir(parents=True, exist_ok=True)
    (CACHE / 'baseline.py').write_bytes(subprocess.check_output(['git', 'show', BASELINE + ':jsonldb/jsonlfile.py']))
    rng = random.Random(731)
    raw = []
    summaries = []
    for kind, count in [('date', 1_000_000), ('datetime', 100_000)]:
        rows = []
        for i in range(count):
            key = ((dt.datetime(2000, 1, 1) + dt.timedelta(seconds=i)).isoformat()
                   if kind == 'datetime' else
                   (dt.date(2000, 1, 1) + dt.timedelta(days=i)).isoformat())
            rows.append(orjson.dumps({key: {'value': i, 'close': 1.25, 'missing': None}}) + b'\n')
        for layout in ('sorted', 'backfill_1pct', 'shuffled'):
            path = CACHE / f'{kind}_{layout}.jsonl'
            if layout == 'sorted':
                indices = range(count)
            elif layout == 'backfill_1pct':
                indices = list(range(count//100, count)) + list(range(count//100))
            else:
                indices = list(range(count))
                rng.shuffle(indices)
            with path.open('wb') as stream:
                stream.writelines(rows[i] for i in indices)
            before = hashlib.sha256(path.read_bytes()).hexdigest()  # Warm OS cache.
            for trial in range(5):
                for method in (('baseline', 'final') if trial % 2 == 0 else ('final', 'baseline')):
                    result = json.loads(subprocess.check_output([
                        sys.executable, __file__, '--worker', str(path), method, kind], text=True))
                    assert result['count'] == count
                    raw.append(dict(kind=kind, layout=layout, trial=trial, method=method, **result))
            summary = dict(kind=kind, layout=layout, count=count, file_mib=path.stat().st_size/1024**2)
            for method in ('baseline', 'final'):
                sample = [r for r in raw if r['kind']==kind and r['layout']==layout and r['method']==method]
                summary[method] = {k: statistics.median(r[k] for r in sample) for k in ('ms', 'peak_mib')}
            summary['change_pct'] = (summary['final']['ms']/summary['baseline']['ms']-1)*100
            summaries.append(summary)
            print(json.dumps(summary), flush=True)
            assert hashlib.sha256(path.read_bytes()).hexdigest() == before
            path.unlink()
        del rows, indices
    evidence = dict(baseline=BASELINE, python=sys.version, platform=platform.platform(),
                    repetitions=5, cache='warm OS cache', memory='whole-process maximum RSS before validation',
                    summaries=summaries, runs=raw)
    (HERE/'benchmark-results.json').write_text(json.dumps(evidence, indent=2)+'\n')


if __name__ == '__main__':
    if len(sys.argv)>1:
        worker(*sys.argv[2:])
    else:
        main()
