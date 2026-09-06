"""Check the changed notebook, Python compatibility, source cap and whitespace."""
import ast
import json
from pathlib import Path
import subprocess

source = Path('jsonldb/folderdb.py').read_text()
assert len(source.splitlines()) <= 1250
ast.parse(source, feature_version=(3, 8))
path = Path('examples/02_portable_datasets.ipynb')
notebook = json.loads(path.read_text())
for cell in notebook['cells']:
    if cell['cell_type'] == 'code':
        compile(''.join(cell['source']), str(path), 'exec')
subprocess.run(['git', 'diff', '--check'], check=True)
print(f'Passed: {len(source.splitlines())} source lines, Python 3.8 syntax, notebook JSON/code syntax, whitespace.')
