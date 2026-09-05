"""FolderDB write-statistics benchmark on disposable, OS-cache-warm fixtures.

Run with --source-root PATH to measure an extracted historical package with the
same harness. Use --save FILE and --compare FILE for repeated-run evidence.
"""
import argparse
import hashlib
import json
import logging
import os
from pathlib import Path
import platform
import statistics
import sys
import tempfile
import time


def measure(fn, prepare=lambda: None, repeats=11):
    prepare()
    fn()
    samples = []
    for _ in range(repeats):
        prepare()
        start = time.perf_counter_ns()
        fn()
        samples.append((time.perf_counter_ns() - start) / 1e6)
    return {'median_ms': statistics.median(samples), 'samples_ms': samples}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-root', type=Path)
    parser.add_argument('--save', type=Path)
    parser.add_argument('--compare', type=Path)
    parser.add_argument('--sizes', type=int, nargs='+', default=[0, 10, 1000, 100000])
    parser.add_argument('--repeats', type=int, default=11)
    args = parser.parse_args()
    root = (args.source_root or Path(__file__).resolve().parents[1]).resolve()
    sys.path.insert(0, str(root))
    from jsonldb import FolderDB, jsonlfile as jf
    import pandas as pd

    logging.getLogger('jsonldb').setLevel(logging.ERROR)
    result = {'python': platform.python_version(), 'platform': platform.platform(),
              'source_root': str(root),
              'source_hashes': {name: hashlib.sha256((root / 'jsonldb' / name).read_bytes()).hexdigest()
                                for name in ('jsonlfile.py', 'jsonldf.py', 'folderdb.py')},
              'repeats': args.repeats,
              'method': 'One warmup, repeated samples, median; warm OS cache; preparation excluded. '
                        'Upsert replaces one existing row without growth; size zero is an empty upsert. '
                        'DataFrame conversion is included; input construction is excluded.',
              'operations': {}}
    operations = result['operations']
    def timed(fn, prepare=lambda: None):
        return measure(fn, prepare, args.repeats)
    with tempfile.TemporaryDirectory(prefix='jsonldb-write-statistics-') as folder:
        db = FolderDB(folder)
        for size in args.sizes:
            rows = {f'k{i:08d}': {'v': i} for i in range(size)}
            updates = {f'k{size // 2:08d}': {'v': size // 2}} if size else {}
            for kind in ('dict', 'df'):
                data = pd.DataFrame.from_dict(rows, orient='index') if kind == 'df' else rows
                change = pd.DataFrame.from_dict(updates, orient='index') if kind == 'df' else updates
                overwrite, upsert = getattr(db, 'overwrite_' + kind), getattr(db, 'upsert_' + kind)
                path = db._get_file_path('table')
                overwrite('table', data)
                operations[f'{size}/{kind}/upsert'] = timed(lambda: upsert('table', change))
                operations[f'{size}/{kind}/overwrite'] = timed(lambda: overwrite('table', data))

                def remove_table():
                    for file in (path, path + '.idx'):
                        if os.path.exists(file):
                            os.unlink(file)
                    jf.save_jsonl(db.dbmeta_path, {})

                operations[f'{size}/{kind}/create'] = timed(
                    lambda: upsert('table', data), remove_table)
                entry = db.get_dbmeta()['table']
                assert entry['count'] == size
                assert entry['size'] == os.path.getsize(path)
            path = os.path.join(folder, 'lowlevel.jsonl')
            jf.save_jsonl(path, rows)
            operations[f'{size}/lowlevel/upsert'] = timed(lambda: jf.update_jsonl(path, updates))
            operations[f'{size}/lowlevel/overwrite'] = timed(lambda: jf.save_jsonl(path, rows))
            if hasattr(jf, '_index_stats'):
                index = jf.load_index(path)
                operations[f'{size}/summary_only'] = timed(lambda: jf._index_stats(path, index))

    if args.compare:
        baseline = json.loads(args.compare.read_text())['operations']
        for name, value in operations.items():
            if name in baseline:
                value['speedup'] = baseline[name]['median_ms'] / value['median_ms']
    if args.save:
        args.save.write_text(json.dumps(result, indent=2) + '\n')
    for name, value in operations.items():
        print(f"{name:30} {value['median_ms']:9.3f} ms", end='')
        print(f"  {value['speedup']:.2f}x" if 'speedup' in value else '')


if __name__ == '__main__':
    main()
