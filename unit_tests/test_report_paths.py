"""Persistent reports scope findings by file ownership, independent of spelling."""
import logging
import os

import pytest

from jsonldb import FolderDB, reports


@pytest.mark.parametrize('operation', ['open', 'lint'])
@pytest.mark.parametrize('spelling', ['dot', 'relative', 'dot-relative', 'parent', 'absolute'])
def test_reports_capture_repairs_for_all_database_path_spellings(
        tmp_path, monkeypatch, operation, spelling):
    root = tmp_path / 'database with spaces'
    root.mkdir()
    child = root / 'child'
    child.mkdir()
    if spelling == 'dot':
        monkeypatch.chdir(root)
        argument = '.'
    elif spelling == 'parent':
        monkeypatch.chdir(child)
        argument = '..'
    else:
        monkeypatch.chdir(tmp_path)
        argument = {'relative': root.name, 'dot-relative': './' + root.name,
                    'absolute': str(root)}[spelling]
    db = FolderDB(argument)
    initial = (root / '.jsonldb/integrity.log').read_text()
    assert 'kind=control_regenerated' in initial
    db.overwrite_dict('table', {'a': {'value': 1}})
    table = root / 'table.jsonl'
    clean = table.read_bytes()
    damaged = clean + b'{torn\n'
    table.write_bytes(damaged)
    (root / 'table.jsonl.idx').write_bytes(b'corrupt')
    # Force open to inspect the table; retain the corrupt-index diagnostic.
    os.utime(root / 'db.meta', ns=(0, 0))
    future = table.stat().st_mtime_ns + 1_000_000_000
    os.utime(root / 'table.jsonl.idx', ns=(future, future))
    hook = '_open' if operation == 'open' else '_lint_db'
    original = getattr(FolderDB, hook)
    foreign = root.with_name(root.name + '-sibling') / 'foreign.jsonl.idx'

    def with_unrelated_findings(self, *args, **kwargs):
        logger = logging.getLogger('jsonldb.jsonlfile')
        logger.warning('rebuilt corrupt index %s', os.path.relpath(foreign))
        logger.warning('foreign finding mentioning %s', table,
                       extra={'jsonldb_file': str(foreign)})
        return original(self, *args, **kwargs)

    monkeypatch.setattr(FolderDB, hook, with_unrelated_findings)
    if operation == 'open':
        FolderDB(argument)
        report = (root / '.jsonldb/integrity.log').read_text()
        assert 'kind=invalid_json' in report
        assert table.read_bytes() == damaged
    else:
        db.lint_db(force=True)
        report = (root / '.jsonldb/lint.log').read_text()
        assert 'kind=lint_removed' in report
        assert 'kind=layout_repaired' in report
        assert table.read_bytes() == clean
    assert 'kind=index_rebuilt' in report
    assert 'detail=corrupt' in report
    assert 'foreign' not in report


@pytest.mark.parametrize('legacy', [False, True])
def test_capture_normalizes_paths_and_excludes_siblings_and_parent_traversal(
        tmp_path, monkeypatch, legacy):
    root = tmp_path / 'database'
    root.mkdir()
    monkeypatch.chdir(root)
    logger = logging.getLogger('jsonldb.jsonlfile')
    with reports.capture_report('.', 'probe', 'integrity.log'):
        for path in ('./inside.jsonl.idx', str(root / 'absolute.jsonl.idx'),
                     '../database/nested/../normalized.jsonl.idx',
                     '../database-sibling/foreign.jsonl.idx',
                     './nested/../../foreign.jsonl.idx'):
            if legacy:
                logger.warning('rebuilt corrupt index %s', path)
            else:
                # Structured ownership must work without a path in the message.
                logger.warning('diagnostic', extra={'jsonldb_file': path})
    lines = (root / '.jsonldb/integrity.log').read_text().splitlines()
    assert len(lines) == 4
    assert 'inside.jsonl.idx' in lines[1]
    assert 'absolute.jsonl.idx' in lines[2]
    assert 'normalized.jsonl.idx' in lines[3]
    assert 'foreign' not in '\n'.join(lines)


@pytest.mark.parametrize('message', [
    'invalid JSON line in %s at byte 7: damaged',
    'lint removed 5 bytes from %s at byte 7',
    'could not move invalid file %s: failed',
    'could not move valid file %s: failed',
    'could not move file %s: failed',
    'file not found: %s',
])
def test_legacy_path_fallback_is_scoped(tmp_path, monkeypatch, message):
    monkeypatch.chdir(tmp_path)
    logger = logging.getLogger('jsonldb.jsonlfile')
    with reports.capture_report('.', 'probe', 'integrity.log'):
        logger.warning(message, './local.jsonl')
        logger.warning(message, '../foreign.jsonl')
    lines = (tmp_path / '.jsonldb/integrity.log').read_text().splitlines()
    assert len(lines) == 2
    assert 'local.jsonl' in lines[1]
    assert 'foreign' not in lines[1]
