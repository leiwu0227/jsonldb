"""Eager DataFrame conversion, FolderDB writes and traced allocation benchmark.

Use --source-root for an extracted historical package, --save/--compare for
before/after results, and --memory for allocation outside timed intervals.
"""
import argparse
import gc
import hashlib
import json
import logging
from pathlib import Path
import platform
import statistics
import sys
import tempfile
import time
import tracemalloc


def measure(fn, repeats):
    fn()
    samples = []
    for _ in range(repeats):
        start = time.perf_counter_ns()
        value = fn()
        samples.append((time.perf_counter_ns() - start) / 1e6)
        del value
    return dict(median_ms=statistics.median(samples), samples_ms=samples)


def peak(fn):
    gc.collect()
    tracemalloc.start()
    value = fn()
    result = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()
    del value
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-root', type=Path)
    parser.add_argument('--save', type=Path)
    parser.add_argument('--compare', type=Path)
    parser.add_argument('--sizes', type=int, nargs='+', default=[0, 10, 1000, 100000])
    parser.add_argument('--repeats', type=int, default=7)
    parser.add_argument('--memory', action='store_true')
    args = parser.parse_args()
    root = (args.source_root or Path(__file__).resolve().parents[1]).resolve()
    sys.path.insert(0, str(root))
    import numpy as np
    import pandas as pd
    from jsonldb import FolderDB, jsonldf as jdf

    logging.getLogger('jsonldb').setLevel(logging.ERROR)
    result = dict(python=platform.python_version(), platform=platform.platform(),
                  pandas=pd.__version__, numpy=np.__version__, repeats=args.repeats,
                  source_hashes={name: hashlib.sha256((root/'jsonldb'/name).read_bytes()).hexdigest()
                                 for name in ('jsonlfile.py', 'jsonldf.py', 'folderdb.py')},
                  method='One warmup, repeated samples; median; warm input and OS caches. '
                         'Input construction excluded; conversion and synchronous metadata included in writes. '
                         'Upsert replaces one existing row; zero rows is empty upsert. '
                         'Conversion-result disposal excluded from conversion timings.',
                  operations={}, peak_traced_bytes={})
    with tempfile.TemporaryDirectory(prefix='jsonldb-dataframe-bench-') as folder:
        db = FolderDB(folder)
        for n in args.sizes:
            keys = [f'k{i:08d}' for i in range(n)]
            values = {f'c{i}': np.arange(n, dtype='float64') for i in range(8)}
            frames = {
                'numeric_1': pd.DataFrame({'v': np.arange(n)}, index=keys),
                'numeric_8': pd.DataFrame(values, index=keys),
                'mixed_8': pd.DataFrame({**{f'c{i}': np.arange(n) for i in range(7)}, 'text': ['value']*n}, index=keys),
                'datetime_8': pd.DataFrame(values, index=pd.date_range('2026-01-01', periods=n, freq='s')),
                'fallback_object_index': pd.DataFrame(values, index=pd.Index(keys, dtype=object)),
            }
            for name, df in frames.items():
                key = f'{n}/{name}'
                result['operations'][key+'/conversion'] = measure(lambda: jdf._df_records(df), args.repeats)
                result['operations'][key+'/overwrite'] = measure(lambda: db.overwrite_df('table', df), args.repeats)
                change = df.iloc[[n//2]] if n else df
                result['operations'][key+'/upsert'] = measure(lambda: db.upsert_df('table', change), args.repeats)
                assert db.get_dbmeta()['table']['count'] == n
                if args.memory:
                    result['peak_traced_bytes'][key+'/conversion'] = peak(lambda: jdf._df_records(df))
                    result['peak_traced_bytes'][key+'/overwrite'] = peak(lambda: db.overwrite_df('table', df))
    if args.compare:
        old = json.loads(args.compare.read_text())
        for name, value in result['operations'].items():
            value['speedup'] = old['operations'][name]['median_ms'] / value['median_ms']
    if args.save:
        args.save.write_text(json.dumps(result, indent=2)+'\n')
    for name, value in result['operations'].items():
        print(f"{name:45} {value['median_ms']:9.3f} ms", end='')
        print(f"  {value['speedup']:.2f}x" if 'speedup' in value else '')
    if args.memory:
        print('Peak traced bytes:', result['peak_traced_bytes'])


if __name__ == '__main__':
    main()
