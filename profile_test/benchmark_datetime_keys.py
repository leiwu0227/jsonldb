"""Compare complete datetime writes and allocation using --source-root/--save."""
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
        fn()
        samples.append((time.perf_counter_ns()-start)/1e6)
    return {'median_ms': statistics.median(samples), 'samples_ms': samples}


def peak(fn):
    gc.collect()
    tracemalloc.start()
    result = fn()
    size = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()
    del result
    return size


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-root', type=Path)
    parser.add_argument('--save', type=Path)
    parser.add_argument('--compare', type=Path)
    parser.add_argument('--sizes', nargs='+', type=int, default=[0, 10, 1000, 100000])
    parser.add_argument('--repeats', type=int, default=7)
    parser.add_argument('--memory', action='store_true')
    args = parser.parse_args()
    root = (args.source_root or Path(__file__).resolve().parents[1]).resolve()
    sys.path.insert(0, str(root))
    import numpy as np
    import pandas as pd
    from jsonldb import FolderDB, jsonldf as jdf, jsonlfile as jf
    logging.getLogger('jsonldb').setLevel(logging.ERROR)
    result = {'python': platform.python_version(), 'pandas': pd.__version__, 'numpy': np.__version__,
              'platform': platform.platform(), 'repeats': args.repeats,
              'source_hashes': {n: hashlib.sha256((root/'jsonldb'/n).read_bytes()).hexdigest() for n in ('jsonlfile.py', 'jsonldf.py', 'folderdb.py')},
              'method': 'One warmup then repeated samples; warm input/OS cache; full FolderDB conversion/table/index/metadata writes included; input construction excluded. Upsert replaces existing rows. Separate allocation traces exclude input frame but include conversion and full write; validation retains its result through peak measurement. Not RSS.',
              'operations': {}, 'memory': {}}
    with tempfile.TemporaryDirectory(prefix='jsonldb-datetime-bench-') as folder:
        db = FolderDB(folder)
        for n in args.sizes:
            dates = pd.date_range('2026-01-01', periods=n, freq='s')
            numeric = {f'c{i}': np.arange(n, dtype='float64') for i in range(8)}
            frames = {'datetime_1': pd.DataFrame({'v': np.arange(n)}, index=dates),
                      'datetime_8': pd.DataFrame(numeric, index=dates),
                      'datetime_mixed': pd.DataFrame({**{f'c{i}': np.arange(n) for i in range(7)}, 'text': ['value']*n}, index=dates),
                      'string_8': pd.DataFrame(numeric, index=[f'k{i:08d}' for i in range(n)]),
                      'named_timezone_8': pd.DataFrame(numeric, index=dates.tz_localize('America/New_York'))}
            if args.memory:
                frames['collision_8'] = pd.DataFrame(numeric, index=pd.date_range('2026-01-01', periods=n, freq='ns'))
            for name, frame in frames.items():
                key = f'{n}/{name}'
                result['operations'][key+'/overwrite'] = measure(lambda: db.overwrite_df('table', frame), args.repeats)
                expected = (1 if n else 0) if name == 'collision_8' else n
                assert db.get_dbmeta()['table']['count'] == expected
                if args.memory:
                    rows = jdf._df_records(frame)
                    def validate():
                        # Baseline validator has no reuse keyword.
                        if 'reuse' in (jf._validate_row_keys.__kwdefaults__ or {}):
                            return jf._validate_row_keys(rows, reuse=True)
                        return jf._validate_row_keys(rows)
                    result['memory'][key] = {'whole_write': peak(lambda: db.overwrite_df('table', frame)), 'validation': peak(validate)}
            db.overwrite_df('table', frames['datetime_8'])
            for size in sorted({0, min(n, 1), min(n, 1000), n}):
                change = frames['datetime_8'].iloc[:size] + 1
                result['operations'][f'{n}/datetime_8/upsert_{size}'] = measure(lambda: db.upsert_df('table', change), args.repeats)
            rows = {key.to_pydatetime(): {'v': i} for i, key in enumerate(dates)}
            result['operations'][f'{n}/datetime_dict/overwrite'] = measure(lambda: db.overwrite_dict('table', rows), args.repeats)
    if args.compare:
        old = json.loads(args.compare.read_text())
        for name, value in result['operations'].items():
            value['speedup'] = old['operations'][name]['median_ms']/value['median_ms']
    if args.save:
        args.save.write_text(json.dumps(result, indent=2)+'\n')
    for name, value in result['operations'].items():
        print(name, round(value['median_ms'], 4), 'ms', round(value['speedup'], 3) if 'speedup' in value else '')
    if args.memory:
        print('Peak traced bytes:', result['memory'])

if __name__ == '__main__':
    main()
