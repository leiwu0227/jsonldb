"""Run the delivered benchmark command from the exact staged tree."""
import io
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

tree = subprocess.check_output(['git','write-tree'],text=True).strip()
raw = subprocess.check_output(['git','archive',tree])
with tempfile.TemporaryDirectory(prefix='jsonldb-datetime-checkout-') as tmp:
    with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
        archive.extractall(tmp, filter='data')
    subprocess.run([sys.executable,'profile_test/benchmark_datetime_keys.py',
                    '--sizes','0','10','--repeats','2'],cwd=tmp,check=True)
print('Fresh staged tree:', tree)
