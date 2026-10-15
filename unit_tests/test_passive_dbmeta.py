"""Passive reads expose published statistics without recovery side effects."""
import builtins
from pathlib import Path

import pytest

from jsonldb import FolderDB, jsonlfile


@pytest.mark.parametrize('missing', [False, True])
def test_passive_read_never_opens_tables_or_builds_indexes(tmp_path, monkeypatch, missing):
    db = FolderDB(str(tmp_path))
    db.overwrite_dict('ticker', {'a': {'v': 1}, 'z': {'v': 2}})
    db.overwrite_dict('empty', {})
    expected = db.get_dbmeta()
    if missing:
        Path(db.dbmeta_path).unlink()
    for path in tmp_path.glob('*.idx'):
        path.unlink()
    before = {p.relative_to(tmp_path): p.read_bytes()
              for p in tmp_path.rglob('*') if p.is_file()}
    original_open = builtins.open

    def forbidden(*args, **kwargs):
        pytest.fail('Passive read attempted recovery')

    def metadata_only(file, mode='r', *args, **kwargs):
        assert str(file) == db.dbmeta_path
        assert mode == 'rb'
        return original_open(file, mode, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(db, 'build_dbmeta', forbidden)
        patch.setattr(jsonlfile, 'load_index', forbidden)
        patch.setattr(jsonlfile, 'build_jsonl_index', forbidden)
        patch.setattr(builtins, 'open', metadata_only)
        result = db.get_dbmeta(build_if_missing=False)
    assert result == (None if missing else expected)
    if not missing:
        assert result['ticker']['count'] == 2
        assert (result['ticker']['min_index'], result['ticker']['max_index']) == ('a', 'z')
        assert result['empty']['count'] == 0
        assert result['empty']['min_index'] is result['empty']['max_index'] is None
        assert 'unknown' not in result
    assert before == {p.relative_to(tmp_path): p.read_bytes()
                      for p in tmp_path.rglob('*') if p.is_file()}


def test_default_still_builds_missing_metadata(tmp_path):
    db = FolderDB(str(tmp_path))
    db.overwrite_dict('ticker', {'a': {'v': 1}})
    Path(db.dbmeta_path).unlink()
    assert db.get_dbmeta(build_if_missing=False) is None
    assert db.get_dbmeta()['ticker']['count'] == 1
    assert db.get_dbmeta(build_if_missing=False) == db.get_dbmeta()


@pytest.mark.parametrize('damage', [b'{broken\n', b'[]\n', b'{"bad":null}\n'])
def test_passive_damage_raises_while_default_remains_tolerant(tmp_path, damage):
    db = FolderDB(str(tmp_path))
    db.overwrite_dict('ticker', {'a': {'v': 1}})
    expected = db.get_dbmeta()
    path = Path(db.dbmeta_path)
    path.write_bytes(path.read_bytes() + damage)
    before = path.read_bytes()
    with pytest.raises(ValueError):
        db.get_dbmeta(build_if_missing=False)
    assert db.get_dbmeta() == expected
    assert path.read_bytes() == before


def test_passive_unreadable_metadata_raises(tmp_path, monkeypatch):
    db = FolderDB(str(tmp_path))
    assert db.get_dbmeta(build_if_missing=False) == {}

    def unreadable(*args, **kwargs):
        raise PermissionError('metadata access denied')

    monkeypatch.setattr(builtins, 'open', unreadable)
    with pytest.raises(OSError, match='metadata access denied'):
        db.get_dbmeta(build_if_missing=False)
