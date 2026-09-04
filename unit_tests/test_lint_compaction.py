import pytest

from jsonldb.jsonlfile import delete_jsonl, lint_jsonl, load_jsonl, save_jsonl, update_jsonl


@pytest.mark.parametrize("force", [False, True])
def test_lint_reclaims_middle_record_tombstone(tmp_path, force):
    path = tmp_path / "ticker.jsonl"
    save_jsonl(str(path), {
        "a": {"value": "first"},
        "b": {"value": "middle"},
        "c": {"value": "last"},
    })
    delete_jsonl(str(path), ["b"])
    size_with_tombstone = path.stat().st_size

    assert lint_jsonl(str(path), force=force) is True

    assert path.stat().st_size < size_with_tombstone
    assert load_jsonl(str(path)) == {
        "a": {"value": "first"},
        "c": {"value": "last"},
    }
    assert path.read_bytes().count(b"\n") == 2


@pytest.mark.parametrize("force", [False, True])
def test_lint_reclaims_last_record_growth_tombstone(tmp_path, force):
    path = tmp_path / "ticker.jsonl"
    save_jsonl(str(path), {
        "a": {"value": "first"},
        "b": {"value": "middle"},
        "c": {"value": "short"},
    })
    replacement = {"value": "expanded-" + "x" * 256}
    update_jsonl(str(path), {"c": replacement})
    size_with_tombstone = path.stat().st_size

    assert lint_jsonl(str(path), force=force) is True

    assert path.stat().st_size < size_with_tombstone
    assert load_jsonl(str(path))["c"] == replacement
    assert path.read_bytes().count(b"\n") == 3
