"""Behavioral coverage for private index reuse and its invalidation boundaries."""
import os
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event

import orjson
import pandas as pd
import pytest

from jsonldb import jsonlfile as jf
from jsonldb.folderdb import FolderDB
from jsonldb.jsonldf import select_jsonldf


@pytest.fixture(autouse=True)
def isolated_cache(monkeypatch):
    jf._invalidate_index_cache(None)
    monkeypatch.setattr(jf, '_INDEX_CACHE_LIMIT', 64 * 1024 * 1024)
    yield
    jf._invalidate_index_cache(None)


@pytest.fixture
def table(tmp_path):
    path = str(tmp_path / 'table.jsonl')
    jf.save_jsonl(path, {'c': {'v': 3}, 'a': {'v': 1}}, slot_bytes=128,
                  meta={'consumer': 'unchanged'})
    return path


def ranged(path):
    return jf.select_jsonl(path, 'a', 'z', auto_deserialize=False)


def count_parses(monkeypatch):
    calls = []
    original = jf._read_index_snapshot
    def counted(path):
        calls.append(path)
        return original(path)
    monkeypatch.setattr(jf, '_read_index_snapshot', counted)
    return calls


def test_reads_reuse_index_and_lazy_keys_without_lending_public_dict(table, monkeypatch):
    calls = count_parses(monkeypatch)
    assert jf.select_line_jsonl(table, 'a') == {'a': {'v': 1}}
    entry = jf._INDEX_CACHE[table]
    assert entry.keys is None
    assert jf.select_line_jsonl(table, 'missing') == {}
    assert jf.select_line_jsonl(table, 'c') == {'c': {'v': 3}}
    assert list(ranged(table)) == ['a', 'c']
    warmed = jf._INDEX_CACHE[table]
    assert warmed.index is entry.index
    assert ranged(table) == {'a': {'v': 1}, 'c': {'v': 3}}
    assert jf._INDEX_CACHE[table].keys is warmed.keys
    assert calls == [table]
    public = jf.load_index(table)
    public.clear()
    assert jf.load_index(table) == warmed.index
    assert ranged(table) == {'a': {'v': 1}, 'c': {'v': 3}}
    assert jf.select_line_jsonl(table, 'missing') == {}
    assert len(jf._INDEX_CACHE) == 1


@pytest.mark.parametrize('mutation', [
    'fit', 'grow', 'insert', 'delete', 'save', 'atomic_save', 'slot',
    'migration', 'rebuild', 'lint',
])
def test_mutators_invalidate_before_next_read(table, mutation):
    ranged(table)
    if mutation == 'fit':
        jf.update_jsonl(table, {'a': {'v': 2}})
    elif mutation == 'grow':
        jf.update_jsonl(table, {'a': {'v': 'large' * 30}})
    elif mutation == 'insert':
        jf.update_jsonl(table, {'b': {'v': 2}})
    elif mutation == 'delete':
        jf.delete_jsonl(table, ['a'])
    elif mutation == 'save':
        jf.save_jsonl(table, {'b': {'v': 2}})
    elif mutation == 'atomic_save':
        jf.save_jsonl_atomic(table, {'b': {'v': 2}})
    elif mutation == 'slot':
        jf.write_jsonl_meta(table, {'v': 2})
    elif mutation == 'migration':
        jf.migrate_jsonl_slot(table, 256)
    elif mutation == 'rebuild':
        jf.build_jsonl_index(table)
    else:
        jf.lint_jsonl(table, force=True)
    assert table not in jf._INDEX_CACHE
    expected = dict(sorted(jf.load_jsonl(table, auto_deserialize=False).items()))
    assert ranged(table) == expected


@pytest.mark.parametrize('damage', ['missing', 'empty', 'malformed', 'array', 'stale'])
def test_repaired_index_is_admitted_from_final_parse(table, damage, monkeypatch, caplog):
    expected = ranged(table)
    index_path = Path(table + '.idx')
    if damage == 'missing':
        index_path.unlink()
    elif damage == 'stale':
        st = os.stat(table)
        os.utime(index_path, ns=(st.st_atime_ns, st.st_mtime_ns - 1_000_000_000))
    else:
        index_path.write_bytes({'empty': b'', 'malformed': b'{', 'array': b'[]'}[damage])
    calls = count_parses(monkeypatch)
    assert ranged(table) == expected
    first = list(calls)
    first_warnings = len(caplog.records)
    assert first
    assert ranged(table) == expected
    assert calls == first
    assert len(caplog.records) == first_warnings
    assert bool(caplog.records) == (damage != 'stale')


def test_unverifiable_files_bypass_cache(table, monkeypatch):
    expected = ranged(table)
    monkeypatch.setattr(jf, '_index_fingerprint', lambda path: None)
    assert ranged(table) == expected
    assert not jf._INDEX_CACHE


def test_failed_write_drops_trusted_entry_before_mutation(table, monkeypatch):
    ranged(table)
    original = jf._write_index
    def failed_publish(path, index):
        assert path not in jf._INDEX_CACHE
        raise OSError('injected failure after row publication')
    monkeypatch.setattr(jf, '_write_index', failed_publish)
    with pytest.raises(OSError, match='injected failure'):
        jf.save_jsonl(table, {'b': {'v': 9}})
    assert table not in jf._INDEX_CACHE
    monkeypatch.setattr(jf, '_write_index', original)
    jf.build_jsonl_index(table)
    assert ranged(table) == {'b': {'v': 9}}


def test_external_same_size_edit_with_restored_mtime_is_observed(table):
    ranged(table)
    old = jf._INDEX_CACHE[table]
    stat = os.stat(table)
    data = Path(table).read_bytes().replace(b'"v":1', b'"v":9')
    Path(table).write_bytes(data)
    os.utime(table, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    assert ranged(table)['a'] == {'v': 9}
    # APFS supplies nanosecond ctime. Row bytes are read afresh regardless.
    if os.stat(table).st_ctime_ns != stat.st_ctime_ns:
        assert jf._INDEX_CACHE[table] is not old


def test_replacement_uses_new_identity_even_with_preserved_mtime(table):
    ranged(table)
    old_stat = os.stat(table)
    replacement = Path(table + '.new')
    replacement.write_bytes(Path(table).read_bytes().replace(b'"a"', b'"b"'))
    os.utime(replacement, ns=(old_stat.st_atime_ns, old_stat.st_mtime_ns))
    replacement.replace(table)
    Path(table + '.idx').unlink()
    assert ranged(table) == {'b': {'v': 1}, 'c': {'v': 3}}


def test_index_replaced_during_parse_is_not_admitted(table, monkeypatch):
    original = jf.orjson.loads
    replaced = False
    def loads(data):
        nonlocal replaced
        value = original(data)
        if not replaced:
            replaced = True
            replacement = Path(table + '.newidx')
            replacement.write_bytes(data)
            replacement.replace(table + '.idx')
        return value
    monkeypatch.setattr(jf.orjson, 'loads', loads)
    assert ranged(table) == {'a': {'v': 1}, 'c': {'v': 3}}
    assert table not in jf._INDEX_CACHE
    assert ranged(table) == {'a': {'v': 1}, 'c': {'v': 3}}
    assert table in jf._INDEX_CACHE


def test_invalidation_during_admission_prevents_repopulation(table, monkeypatch):
    original = jf._read_index_snapshot
    def invalidated(path):
        result = original(path)
        jf._invalidate_index_cache(path)
        return result
    monkeypatch.setattr(jf, '_read_index_snapshot', invalidated)
    assert ranged(table) == {'a': {'v': 1}, 'c': {'v': 3}}
    assert not jf._INDEX_CACHE


def test_lru_budget_keys_and_oversized_bypass(tmp_path, monkeypatch):
    paths = [str(tmp_path / f'{i}.jsonl') for i in range(3)]
    for path in paths:
        jf.save_jsonl(path, {'a': {'v': 1}})
    for path in paths:
        jf.select_line_jsonl(path, 'a')
    largest = max(e.size for e in jf._INDEX_CACHE.values())
    jf._invalidate_index_cache(None)
    monkeypatch.setattr(jf, '_INDEX_CACHE_LIMIT', 2 * largest)
    for path in paths[:2]:
        jf.select_line_jsonl(path, 'a')
    jf.select_line_jsonl(paths[0], 'a')
    jf.select_line_jsonl(paths[2], 'a')
    assert list(jf._INDEX_CACHE) == [paths[0], paths[2]]
    assert jf._INDEX_CACHE_BYTES == sum(e.size for e in jf._INDEX_CACHE.values())
    assert jf._INDEX_CACHE_BYTES <= jf._INDEX_CACHE_LIMIT
    monkeypatch.setattr(jf, '_INDEX_CACHE_LIMIT', 1)
    jf._invalidate_index_cache(None)
    assert ranged(paths[0]) == {'a': {'v': 1}}
    assert not jf._INDEX_CACHE
    assert jf._INDEX_CACHE_BYTES == 0


def test_lazy_key_budget_and_unrelated_write_epoch(table, tmp_path, monkeypatch):
    jf.select_line_jsonl(table, 'a')
    base = jf._INDEX_CACHE[table].size
    jf.save_jsonl(str(tmp_path / 'other.jsonl'), {'z': {'v': 9}})
    assert list(ranged(table)) == ['a', 'c']
    assert jf._INDEX_CACHE[table].keys == ('a', 'c')
    key_size = jf._INDEX_CACHE[table].size - base
    assert key_size > 0
    jf._invalidate_index_cache(None)
    monkeypatch.setattr(jf, '_INDEX_CACHE_LIMIT', base)
    jf.select_line_jsonl(table, 'a')
    assert list(ranged(table)) == ['a', 'c']
    assert jf._INDEX_CACHE[table].keys is None
    assert jf._INDEX_CACHE_BYTES == base


def test_nested_malformed_offset_is_not_retained(table):
    Path(table + '.idx').write_bytes(orjson.dumps({'a': {'nested': [1, 2, 3]}}))
    # Existing loaders accept object-shaped indexes; lint owns offset fidelity.
    assert jf.load_index(table) == {'a': {'nested': [1, 2, 3]}}
    assert jf._read_cached_index(table)[0] == {'a': {'nested': [1, 2, 3]}}
    assert not jf._INDEX_CACHE


def test_parallel_cold_reads_do_not_hold_lock_during_io(table, monkeypatch):
    entered, release = Event(), Event()
    original = jf._read_index_snapshot
    def paused(path):
        value = original(path)
        entered.set()
        assert release.wait(5)
        return value
    monkeypatch.setattr(jf, '_read_index_snapshot', paused)
    with ThreadPoolExecutor(max_workers=2) as pool:
        reader = pool.submit(ranged, table)
        assert entered.wait(5)
        invalidation = pool.submit(jf._invalidate_index_cache, table)
        try:
            invalidation.result(timeout=2)
        finally:
            release.set()
        assert reader.result(timeout=5) == {'a': {'v': 1}, 'c': {'v': 3}}
    assert not jf._INDEX_CACHE


def test_concurrent_readers_keep_accounting_consistent(table):
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(ranged, [table] * 40))
    assert all(value == {'a': {'v': 1}, 'c': {'v': 3}} for value in results)
    assert len(jf._INDEX_CACHE) == 1
    assert jf._INDEX_CACHE_BYTES == jf._INDEX_CACHE[table].size


def test_folder_dataframe_and_key_conversion(tmp_path, monkeypatch):
    db = FolderDB(str(tmp_path))
    db.set_meta_slot_bytes(256)
    key = pd.Timestamp('2024-01-01T00:00:00')
    db.overwrite_df('table', pd.DataFrame({'v': [1]}, index=[key]))
    path = str(tmp_path / 'table.jsonl')
    calls = count_parses(monkeypatch)
    first = db.get_df(['table'], key, key)
    second = db.get_df(['table'], key, key)
    pd.testing.assert_frame_equal(first['table'], second['table'])
    assert calls == [path]
    raw = select_jsonldf(path, key, key, auto_deserialize=False)
    assert list(raw.index) == ['2024-01-01T00:00:00']
    assert calls == [path]
    db.delete_file('table')
    with pytest.raises(FileNotFoundError):
        jf.select_line_jsonl(path, key)
    assert path not in jf._INDEX_CACHE


def test_alias_and_external_move_are_checked(table, tmp_path):
    alias = str(tmp_path / 'alias.jsonl')
    os.symlink(table, alias)
    os.symlink(table + '.idx', alias + '.idx')
    assert ranged(alias) == ranged(table)
    jf.update_jsonl(table, {'a': {'v': 'expanded' * 20}})
    assert ranged(alias) == ranged(table)
    moved = str(tmp_path / 'moved.jsonl')
    os.rename(table, moved)
    os.rename(table + '.idx', moved + '.idx')
    with pytest.raises(OSError):
        ranged(alias)
    assert ranged(moved)['a'] == {'v': 'expanded' * 20}


def test_full_load_does_not_use_index_cache(table, monkeypatch):
    monkeypatch.setattr(jf, '_read_cached_index', lambda *a, **kw: pytest.fail('sequential read used cache'))
    assert jf.select_jsonl(table) == jf.load_jsonl(table)


@pytest.mark.parametrize('escaped', [False, True])
@pytest.mark.parametrize('keys', [['a', 'long' * 50], ['é', '€', '𝄞'], ['\\u1234', '\n']])
def test_admission_weight_bounds_decoded_objects(table, keys, escaped):
    offsets = dict(zip(keys, [-(1 << 63), (1 << 64) - 1, 42]))
    raw = json.dumps(offsets, ensure_ascii=escaped).encode()
    Path(table + '.idx').write_bytes(raw)
    index, ordered = jf._read_cached_index(table, with_keys=True)
    assert index == offsets
    entry = jf._INDEX_CACHE[table]
    actual = (sys.getsizeof(index) + sum(map(sys.getsizeof, index))
              + sum(map(sys.getsizeof, index.values())) + sys.getsizeof(ordered)
              + sys.getsizeof(entry) + sys.getsizeof(table))
    assert entry.size >= actual


@pytest.mark.parametrize('operation', ['clear', 'hierarchy'])
def test_folder_mutations_need_no_cache_hooks(tmp_path, operation):
    db = FolderDB(str(tmp_path), hierarchy_depth=1)
    db.overwrite_dict('A.B.table', {'a': {'v': 1}})
    old_path = db._get_file_path('A.B.table')
    assert db.get_dict(['A.B.table'], 'a', 'z') == {'A.B.table': {'a': {'v': 1}}}
    if operation == 'clear':
        db.clear_folder(force=True)
        assert db.get_dict(['A.B.table'], 'a', 'z') == {}
    else:
        db.lint_hierarchy(2)
        assert db.get_dict(['A.B.table'], 'a', 'z') == {'A.B.table': {'a': {'v': 1}}}
    with pytest.raises(FileNotFoundError):
        jf.select_line_jsonl(old_path, 'a')
    assert old_path not in jf._INDEX_CACHE


def test_cached_offsets_still_parse_and_warn_on_damaged_row(table, monkeypatch, caplog):
    ranged(table)
    old_stamp = jf._index_fingerprint(table)
    raw = Path(table).read_bytes().replace(b'"v":1', b'"v":!')
    Path(table).write_bytes(raw)
    # Model undetectable metadata changes: cached offsets still validate rows.
    monkeypatch.setattr(jf, '_index_fingerprint', lambda path: old_stamp)
    assert jf.select_line_jsonl(table, 'a') == {}
    assert ranged(table) == {'c': {'v': 3}}
    assert len(caplog.records) == 2
    assert all('invalid JSON line' in record.message for record in caplog.records)


def test_path_objects_preserve_indexed_read_and_write_compatibility(table):
    path = Path(table)
    assert jf.load_index(path) == jf.load_index(table)
    assert ranged(path) == {'a': {'v': 1}, 'c': {'v': 3}}
    assert jf.select_line_jsonl(path, 'a') == {'a': {'v': 1}}
    jf.update_jsonl(path, {'a': {'v': 'expanded' * 10}})
    assert jf.select_line_jsonl(path, 'a') == {'a': {'v': 'expanded' * 10}}
    jf.delete_jsonl(path, ['a'])
    assert ranged(path) == {'c': {'v': 3}}
    assert jf._INDEX_CACHE[str(path)].index == jf.load_index(path)
