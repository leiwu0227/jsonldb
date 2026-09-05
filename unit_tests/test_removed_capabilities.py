import ast
import importlib.util
import os
from pathlib import Path
import subprocess
import sys

import jsonldb
from jsonldb import FolderDB


REMOVED_MODULES = ("vercontrol", "visual")
REMOVED_METHODS = ("commit", "revert", "version")
REMOVED_DEPENDENCIES = ("gitpython", "bokeh", "matplotlib")


def test_removed_modules_are_absent_from_source_and_import_inventory():
    package_dir = Path(jsonldb.__file__).parent

    for module_name in REMOVED_MODULES:
        assert not (package_dir / (module_name + ".py")).exists()
        assert importlib.util.find_spec("jsonldb." + module_name) is None
        assert module_name not in jsonldb.__all__


def test_folderdb_has_no_version_control_facade():
    for method_name in REMOVED_METHODS:
        assert not hasattr(FolderDB, method_name)


def test_remaining_package_inventory_is_explicit():
    assert jsonldb.__all__ == [
        "FolderDB", "jsonlfile", "jsonldf", "metaslot", "reports"]


def test_setup_declares_only_remaining_runtime_dependencies():
    setup_path = Path(__file__).parents[1] / "setup.py"
    tree = ast.parse(setup_path.read_text(encoding="utf-8"))
    setup_call = next(
        node for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "setup")
    requirements_node = next(
        keyword.value for keyword in setup_call.keywords
        if keyword.arg == "install_requires")
    requirements = [
        item.value.lower() for item in requirements_node.elts]

    assert any(item.startswith("numpy") for item in requirements)
    assert all(
        not item.startswith(removed)
        for item in requirements
        for removed in REMOVED_DEPENDENCIES)


def test_core_operates_when_removed_library_imports_are_blocked(tmp_path):
    code = """
import builtins
import tempfile
from pathlib import Path

real_import = builtins.__import__

def deny_removed(name, *args, **kwargs):
    if name.split('.')[0] in {'git', 'bokeh', 'matplotlib'}:
        raise AssertionError('removed dependency imported: ' + name)
    return real_import(name, *args, **kwargs)

builtins.__import__ = deny_removed
from jsonldb import FolderDB
root = Path(tempfile.mkdtemp(prefix='jsonldb-no-removed-deps-'))
db = FolderDB(str(root))
db.overwrite_dict('records', {'a': {'value': 1}})
assert db.get_dict(['records']) == {'records': {'a': {'value': 1}}}
"""
    environment = os.environ.copy()
    package_root = str(Path(__file__).parents[1])
    environment["PYTHONPATH"] = os.pathsep.join(filter(None, (
        package_root, environment.get("PYTHONPATH", ""))))
    subprocess.run(
        [sys.executable, "-c", code],
        cwd=tmp_path,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
