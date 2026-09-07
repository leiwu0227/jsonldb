"""Bounded static acceptance checks for the strict-read assignment."""
import ast
from pathlib import Path
import subprocess

for name, cap in [('jsonlfile', 950), ('jsonldf', 150), ('folderdb', 1250)]:
    path = Path('jsonldb') / (name + '.py')
    source = path.read_text()
    lines = len(source.splitlines())
    assert lines <= cap, (path, lines, cap)
    ast.parse(source, filename=str(path), feature_version=(3, 8))
    print(f'{path}: {lines}/{cap} lines; Python 3.8 syntax passes')
subprocess.run(['git', 'diff', '--check'], check=True)
subprocess.run(['git', 'diff', '--exit-code', '794cbf9', '--',
                '.specdev/project_notes/roadmap/designs'], check=True)
print('Whitespace and published design preservation pass')
