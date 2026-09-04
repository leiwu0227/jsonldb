#!/usr/bin/env python
"""Benchmark script for JSONLDB performance.

Usage:
    python benchmark.py                  # Run benchmarks, print results
    python benchmark.py --save baseline.json   # Run and save results to file
    python benchmark.py --compare baseline.json  # Run and compare against saved baseline
    python benchmark.py --large          # Include 1M record benchmarks
"""

import os
import sys
import time
import shutil
import tempfile
import argparse
import json

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jsonldb.jsonlfile import save_jsonl, load_jsonl, select_jsonl, lint_jsonl, delete_jsonl, update_jsonl
from jsonldb.folderdb import FolderDB

# Collect results for save/compare
_results = {}


def generate_data(n):
    """Generate n records with string keys."""
    return {f"key_{i:08d}": {"value": i, "name": f"item_{i}"} for i in range(n)}


def generate_unsorted_data(n):
    """Generate n records in reverse order (unsorted)."""
    return {f"key_{i:08d}": {"value": i, "name": f"item_{i}"} for i in range(n - 1, -1, -1)}


def bench(label, fn, *args, **kwargs):
    """Time a function call and print the result."""
    start = time.perf_counter()
    result = fn(*args, **kwargs)
    elapsed = time.perf_counter() - start
    print(f"  {label:<50s} {elapsed:8.3f}s")
    _results[label] = elapsed
    return elapsed, result


def benchmark_jsonlfile(sizes):
    """Benchmark core jsonlfile operations."""
    print("\n=== jsonlfile benchmarks ===\n")
    print(f"  {'Operation':<50s} {'Time':>8s}")
    print(f"  {'-'*50} {'-'*8}")

    for n in sizes:
        tmp = tempfile.mkdtemp()
        fpath = os.path.join(tmp, "bench.jsonl")
        data = generate_data(n)

        try:
            # save
            bench(f"save_jsonl ({n:,} records)", save_jsonl, fpath, data)

            # load
            bench(f"load_jsonl ({n:,} records)", load_jsonl, fpath)

            # select range (middle 10%)
            keys = sorted(data.keys())
            lo = keys[len(keys) // 2 - len(keys) // 20]
            hi = keys[len(keys) // 2 + len(keys) // 20]
            bench(f"select_jsonl range ({n:,}, ~10%)", select_jsonl, fpath, lo, hi)

            # lint (already sorted — should skip)
            bench(f"lint_jsonl skip path ({n:,})", lint_jsonl, fpath)

            # lint unsorted
            os.remove(fpath)
            os.remove(fpath + ".idx")
            unsorted = generate_unsorted_data(n)
            save_jsonl(fpath, unsorted)
            bench(f"lint_jsonl unsorted ({n:,})", lint_jsonl, fpath)

            # lint with dead lines
            save_jsonl(fpath, data)
            del_keys = keys[:n // 10]  # delete first 10%
            delete_jsonl(fpath, del_keys)
            bench(f"lint_jsonl dead lines ({n:,}, 10% deleted)", lint_jsonl, fpath)

            print()
        finally:
            shutil.rmtree(tmp)


def benchmark_folderdb(num_files=50, records_per_file=1000):
    """Benchmark FolderDB operations."""
    print(f"\n=== FolderDB benchmarks ({num_files} files, {records_per_file} records each) ===\n")
    print(f"  {'Operation':<50s} {'Time':>8s}")
    print(f"  {'-'*50} {'-'*8}")

    tmp = tempfile.mkdtemp()

    try:
        # Create files
        db = FolderDB(tmp)
        for i in range(num_files):
            data = generate_data(records_per_file)
            db.upsert_dict(f"table_{i:04d}", data)

        # Benchmark __init__ (re-opening existing db)
        bench(f"FolderDB.__init__ ({num_files} files)", FolderDB, tmp)

        # Benchmark lint_db
        db2 = FolderDB(tmp)
        bench(f"lint_db ({num_files} files)", db2.lint_db)

    finally:
        shutil.rmtree(tmp)


def print_comparison(baseline, current):
    """Print a before/after comparison table."""
    print("\n" + "=" * 78)
    print("BEFORE / AFTER COMPARISON")
    print("=" * 78)
    print(f"  {'Operation':<42s} {'Before':>8s} {'After':>8s} {'Speedup':>8s}")
    print(f"  {'-'*42} {'-'*8} {'-'*8} {'-'*8}")

    for label in current:
        before = baseline.get(label)
        after = current[label]
        if before is not None:
            speedup = before / after if after > 0 else float('inf')
            marker = " <--" if speedup < 0.8 else ""
            print(f"  {label:<42s} {before:7.3f}s {after:7.3f}s {speedup:7.1f}x{marker}")
        else:
            print(f"  {label:<42s} {'N/A':>8s} {after:7.3f}s {'N/A':>8s}")

    print()


def main():
    parser = argparse.ArgumentParser(description="JSONLDB Performance Benchmarks")
    parser.add_argument("--large", action="store_true", help="Include 1M record benchmarks")
    parser.add_argument("--save", metavar="FILE", help="Save results to JSON file for later comparison")
    parser.add_argument("--compare", metavar="FILE", help="Compare current run against a saved baseline")
    args = parser.parse_args()

    sizes = [1_000, 10_000, 100_000]
    if args.large:
        sizes.append(1_000_000)

    print("JSONLDB Performance Benchmark")
    print("=" * 60)

    benchmark_jsonlfile(sizes)
    benchmark_folderdb()

    # Save results if requested
    if args.save:
        with open(args.save, 'w') as f:
            json.dump(_results, f, indent=2)
        print(f"\nResults saved to {args.save}")

    # Compare against baseline if requested
    if args.compare:
        if not os.path.exists(args.compare):
            print(f"\nBaseline file not found: {args.compare}")
        else:
            with open(args.compare) as f:
                baseline = json.load(f)
            print_comparison(baseline, _results)

    print("\nDone.")


if __name__ == "__main__":
    main()
