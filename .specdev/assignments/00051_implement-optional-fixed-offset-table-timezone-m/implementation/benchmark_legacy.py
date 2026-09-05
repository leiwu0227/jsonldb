"""Alternate baseline/candidate processes; report undeclared-table latency."""
import json
from pathlib import Path
import statistics
import subprocess
import sys
import time

CASE = r'''
import datetime as dt, json, logging, sys, tempfile, time
sys.path.insert(0, sys.argv[1])
from jsonldb import jsonlfile as jf
logging.disable(logging.CRITICAL)
with tempfile.TemporaryDirectory() as root:
    path = root+'/table.jsonl'
    start = dt.datetime(2026,1,1)
    rows = {start+dt.timedelta(seconds=i): {'v': i} for i in range(100000)}
    jf.save_jsonl(path, rows)
    result = {}
    def measure(name, action, repeats):
        action()
        begin = time.perf_counter_ns()
        for _ in range(repeats): action()
        result[name] = (time.perf_counter_ns()-begin)/repeats/1000
    measure('large_datetime_save_us', lambda: jf.save_jsonl(path, rows), 3)
    measure('point_read_us', lambda: jf.select_line_jsonl(path, start), 1500)
    small = root+'/small.jsonl'
    jf.save_jsonl(small, {'a': {'v': 1}})
    measure('small_upsert_us', lambda: jf.update_jsonl(small, {'a': {'v': 1}}), 150)
    measure('small_save_us', lambda: jf.save_jsonl(small, {'a': {'v': 1}}), 150)
    print(json.dumps(result))
'''


def main():
    begin = time.perf_counter()
    sources = [str(Path(sys.argv[1]).resolve()), str(Path.cwd())]
    samples = [[], []]
    for round_number in range(7):
        for index in ([0, 1] if round_number % 2 == 0 else [1, 0]):
            raw = subprocess.check_output([sys.executable, '-c', CASE, sources[index]], text=True)
            samples[index].append(json.loads(raw))
    summary = {}
    for name in samples[0][0]:
        medians = [statistics.median(sample[name] for sample in group) for group in samples]
        summary[name] = {'baseline': medians[0], 'candidate': medians[1], 'ratio': medians[1]/medians[0]}
    result = {'samples': samples, 'medians': summary, 'duration_ms': round((time.perf_counter()-begin)*1000)}
    destination = Path(__file__).with_name('legacy-performance.json')
    destination.write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
