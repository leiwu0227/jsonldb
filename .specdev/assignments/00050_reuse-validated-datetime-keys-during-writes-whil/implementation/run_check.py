"""Record one focused command's actual result and duration."""
import json
from pathlib import Path
import subprocess
import sys
import time

label, *command = sys.argv[1:]
start = time.perf_counter_ns()
run = subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
receipt = {'command': command, 'duration_ms': round((time.perf_counter_ns()-start)/1e6),
           'status': 'passed' if run.returncode == 0 else 'failed', 'exit_code': run.returncode,
           'output': run.stdout}
Path(__file__).with_name(label+'.json').write_text(json.dumps(receipt, indent=2)+'\n')
print(run.stdout)
sys.exit(run.returncode)
