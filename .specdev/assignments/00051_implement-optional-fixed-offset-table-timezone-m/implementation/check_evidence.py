"""Recheck source budgets, Python 3.8 syntax and legacy comparison artifacts."""
import ast
import json
from pathlib import Path
import subprocess
import time

start = time.perf_counter()
root = Path(__file__).parent
limits = {'jsonldb/metaslot.py': 250, 'jsonldb/jsonlfile.py': 950,
          'jsonldb/jsonldf.py': 150, 'jsonldb/folderdb.py': 1250,
          'jsonldb/_tabletimezone.py': 250}
rows = []
for name, limit in limits.items():
    source = Path(name).read_text()
    count = len(source.splitlines())
    assert count <= limit, (name, count, limit)
    ast.parse(source, filename=name, feature_version=(3, 8))
    rows.append({'path': name, 'physical_lines': count, 'limit': limit})
(root/'line-counts.json').write_text(json.dumps(rows, indent=2)+'\n')
subprocess.run(['git', 'diff', '--check'], check=True)
print(json.dumps({'line_caps': rows, 'python38_syntax': 'passed',
                  'duration_ms': round((time.perf_counter()-start)*1000)}))
