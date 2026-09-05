"""Repeated-read cache benchmark, including admission and eviction costs.

Run directly or via benchmark.py --index-cache. OS caches are not flushed:
"cold" means an empty application index cache, not cold storage.
"""
import argparse
import hashlib
import importlib.util
import json
import logging
import os
from pathlib import Path
import platform
import statistics
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from jsonldb import jsonlfile as jf


def clear_cache():
    clear = getattr(jf, '_invalidate_index_cache', None)
    if clear:
        clear(None)


def measure(fn, prepare=lambda: None, repeats=9):
    prepare()
    expected = fn()
    samples = []
    for _ in range(repeats):
        prepare()
        start = time.perf_counter_ns()
        value = fn()
        samples.append((time.perf_counter_ns() - start) / 1e6)
        assert value == expected
    return {'median_ms': statistics.median(samples), 'samples_ms': samples}


def run():
    logging.getLogger('jsonldb').setLevel(logging.ERROR)
    results = {'python': platform.python_version(), 'platform': platform.platform(),
               'source_sha256': hashlib.sha256(Path(jf.__file__).read_bytes()).hexdigest(),
               'revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
               'method': 'One warmup, nine samples; OS cache warm; cold means empty application cache.',
               'budget_bytes': getattr(jf, '_INDEX_CACHE_LIMIT', 0), 'operations': {}}
    ops = results['operations']
    with tempfile.TemporaryDirectory(prefix='jsonldb-cache-bench-') as folder:
        for n in (1000, 100_000, 600_000):
            data = {f'key_{i:08d}': {'value': i, 'name': f'item_{i}'} for i in range(n)}
            path = os.path.join(folder, f'{n}.jsonl')
            jf.save_jsonl(path, data)
            key = f'key_{n // 2:08d}'
            high = f'key_{n // 2 + 99:08d}'
            point = lambda: jf.select_line_jsonl(path, key)
            narrow = lambda: jf.select_jsonl(path, key, high)
            assert point() == {key: data[key]}
            assert list(narrow()) == [f'key_{i:08d}' for i in range(n // 2, n // 2 + 100)]
            ops[f'{n}/cold_point'] = measure(point, clear_cache)
            ops[f'{n}/warm_point'] = measure(point)
            ops[f'{n}/cold_range'] = measure(narrow, clear_cache)
            ops[f'{n}/warm_range'] = measure(narrow)
            results[f'{n}/retained_bytes'] = getattr(jf, '_INDEX_CACHE_BYTES', 0)
            results[f'{n}/retained_entries'] = len(getattr(jf, '_INDEX_CACHE', {}))
            if n <= 100_000:
                ops[f'{n}/read_after_write'] = measure(
                    point, lambda: jf.update_jsonl(path, {key: data[key]}))
                def write_read():
                    jf.update_jsonl(path, {key: data[key]})
                    return point()
                ops[f'{n}/write_and_read'] = measure(write_read)
                ops[f'{n}/full_load'] = measure(lambda: len(jf.load_jsonl(path)))
            del data
            clear_cache()
        # Six 100k indexes exceed the initial 64 MiB retained-object budget.
        paths = []
        data = {f'key_{i:08d}': {'value': i} for i in range(100_000)}
        for i in range(6):
            path = os.path.join(folder, f'pressure_{i}.jsonl')
            jf.save_jsonl(path, data)
            paths.append(path)
        del data
        def pressure():
            return [jf.select_line_jsonl(p, 'key_00050000') for p in paths]
        clear_cache()
        ops['pressure/cycle_six_tables'] = measure(pressure)
        results['pressure/retained_bytes'] = getattr(jf, '_INDEX_CACHE_BYTES', 0)
        results['pressure/retained_entries'] = len(getattr(jf, '_INDEX_CACHE', {}))
        # Deterministic reuse/eviction observation outside measured intervals.
        cache = getattr(jf, '_INDEX_CACHE', {})
        observations = []
        for path in paths:
            before = set(cache)
            was_retained = os.path.abspath(path) in cache
            jf.select_line_jsonl(path, 'key_00050000')
            observations.append({'retained_before': was_retained,
                                 'evicted_entries': len(before - set(cache))})
        results['pressure/observations'] = observations
    clear_cache()
    return results


def main():
    global jf
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--save')
    parser.add_argument('--compare')
    parser.add_argument('--source', help='Optional historical jsonlfile.py for an isolated baseline')
    args = parser.parse_args()
    if args.source:
        spec = importlib.util.spec_from_file_location('jsonldb._benchmark_baseline', args.source)
        jf = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(jf)
    result = run()
    if args.compare:
        baseline = json.loads(Path(args.compare).read_text())
        for name, value in result['operations'].items():
            value['speedup'] = baseline['operations'][name]['median_ms'] / value['median_ms']
    if args.save:
        Path(args.save).write_text(json.dumps(result, indent=2) + '\n')
    for name, value in result['operations'].items():
        print(f"{name:34} {value['median_ms']:9.3f} ms", end='')
        print(f"  {value['speedup']:.2f}x" if 'speedup' in value else '')
    print('Retained memory:', {k: v for k, v in result.items() if 'retained' in k})


if __name__ == '__main__':
    main()
