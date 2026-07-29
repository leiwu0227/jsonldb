import os
from datetime import datetime

import pandas as pd
import pytest

from jsonldb import FolderDB
from jsonldb import jsonlfile


def _frame(records):
    return pd.DataFrame.from_dict(records, orient="index")


def _owner(db, name="ticker"):
    return db._get_file_path(name)


def _artifacts(db, name="ticker"):
    owner = _owner(db, name)
    paths = (owner, owner + ".idx", db.dbmeta_path, owner + ".aux")
    return {
        path: open(path, "rb").read() if os.path.exists(path) else None
        for path in paths
    }


@pytest.fixture
def ticker_db(tmp_path):
    db = FolderDB(str(tmp_path))
    db.upsert_df(
        "ticker",
        _frame({
            "a": {"value": 1},
            "b": {"value": 2},
            "c": {"value": 3},
            "d": {"value": 4},
            "e": {"value": 5},
        }),
    )
    return db


@pytest.mark.parametrize(
    ("lower", "upper", "replacement", "expected"),
    [
        (
            "b",
            "d",
            {"b": {"value": 20}, "d": {"value": 40}},
            {"a": 1, "b": 20, "d": 40, "e": 5},
        ),
        (
            "a",
            "b",
            {"a": {"value": 10}},
            {"a": 10, "c": 3, "d": 4, "e": 5},
        ),
        (
            "d",
            "e",
            {"e": {"value": 50}},
            {"a": 1, "b": 2, "c": 3, "e": 50},
        ),
        (
            "a",
            "e",
            {"c": {"value": 30}},
            {"c": 30},
        ),
        (
            "b",
            "d",
            {},
            {"a": 1, "e": 5},
        ),
        (
            "bb",
            "bc",
            {"bb": {"value": 22}},
            {"a": 1, "b": 2, "bb": 22, "c": 3, "d": 4, "e": 5},
        ),
        (
            "c",
            "c",
            {"c": {"value": 33}},
            {"a": 1, "b": 2, "c": 33, "d": 4, "e": 5},
        ),
    ],
)
def test_replace_df_range_has_exact_inclusive_semantics(
    ticker_db, lower, upper, replacement, expected
):
    replacement_df = (
        _frame(replacement)
        if replacement
        else pd.DataFrame(columns=["value"])
    )

    ticker_db.replace_df_range("ticker", lower, upper, replacement_df)

    result = ticker_db.get_df(["ticker"])["ticker"]
    assert result["value"].to_dict() == expected
    metadata = ticker_db.get_dbmeta()["ticker"]
    assert metadata["count"] == len(expected)
    assert metadata["min_index"] == next(iter(expected))
    assert metadata["max_index"] == next(reversed(expected))


def test_success_stages_everything_then_invalidates_aux_and_publishes_in_order(
    ticker_db, monkeypatch
):
    owner = _owner(ticker_db)
    ticker_db.write_aux("ticker", b"old-derived")
    events = []
    real_remove = jsonlfile.remove_aux
    real_replace = jsonlfile.os.replace

    def recording_remove(path):
        if path == owner:
            events.append("aux")
        return real_remove(path)

    def recording_replace(source, target):
        if target == owner:
            assert os.path.isfile(source)
            assert os.path.isfile(source + ".idx")
            assert jsonlfile.load_jsonl(source, auto_deserialize=False) == {
                "a": {"value": 1},
                "c": {"value": 30},
                "e": {"value": 5},
            }
            events.append("owner")
        elif target == owner + ".idx":
            events.append("index")
        return real_replace(source, target)

    monkeypatch.setattr(jsonlfile, "remove_aux", recording_remove)
    monkeypatch.setattr(jsonlfile.os, "replace", recording_replace)

    ticker_db.replace_df_range(
        "ticker",
        "b",
        "d",
        _frame({"c": {"value": 30}}),
    )

    assert events[-3:] == ["aux", "owner", "index"]
    assert not os.path.exists(owner + ".aux")


@pytest.mark.parametrize(
    ("operation", "error"),
    [
        ("missing", FileNotFoundError),
        ("reversed", ValueError),
        ("duplicate-index", ValueError),
        ("out-of-range", ValueError),
        ("empty-payload", ValueError),
        ("unserializable", TypeError),
        ("wrong-type", TypeError),
    ],
)
def test_prepublication_validation_leaves_every_artifact_unchanged(
    ticker_db, operation, error
):
    ticker_db.write_aux("ticker", b"still-current")
    before = _artifacts(ticker_db)

    if operation == "missing":
        call = lambda: ticker_db.replace_df_range(
            "missing", "a", "b", _frame({"a": {"value": 1}})
        )
    elif operation == "reversed":
        call = lambda: ticker_db.replace_df_range(
            "ticker", "d", "b", _frame({"c": {"value": 3}})
        )
    elif operation == "duplicate-index":
        frame = pd.DataFrame({"value": [1, 2]}, index=["b", "b"])
        call = lambda: ticker_db.replace_df_range("ticker", "b", "c", frame)
    elif operation == "out-of-range":
        call = lambda: ticker_db.replace_df_range(
            "ticker", "b", "c", _frame({"d": {"value": 4}})
        )
    elif operation == "empty-payload":
        call = lambda: ticker_db.replace_df_range(
            "ticker", "b", "c", pd.DataFrame(index=["b"])
        )
    elif operation == "unserializable":
        call = lambda: ticker_db.replace_df_range(
            "ticker", "b", "c", _frame({"b": {"value": object()}})
        )
    else:
        call = lambda: ticker_db.replace_df_range("ticker", "b", "c", {})

    with pytest.raises(error):
        call()

    assert _artifacts(ticker_db) == before
    assert not os.path.exists(_owner(ticker_db, "missing"))


def test_timespec_serialization_collision_is_rejected_before_publication(
    tmp_path
):
    db = FolderDB(str(tmp_path))
    db.upsert_df(
        "ticker",
        _frame({datetime(2024, 1, 1): {"value": 1}}),
    )
    db.write_aux("ticker", b"still-current")
    before = _artifacts(db)
    colliding = pd.DataFrame(
        {"value": [2, 3]},
        index=[
            datetime(2024, 1, 2, 0, 0, 0, 1),
            datetime(2024, 1, 2, 0, 0, 0, 2),
        ],
    )

    with pytest.raises(ValueError, match="collide"):
        db.replace_df_range(
            "ticker",
            datetime(2024, 1, 2),
            datetime(2024, 1, 2, 0, 0, 1),
            colliding,
        )

    assert _artifacts(db) == before


@pytest.mark.parametrize("failure_point", ["stage", "sync", "aux", "owner"])
def test_failures_through_owner_publication_never_expose_partial_data(
    ticker_db, monkeypatch, failure_point
):
    owner = _owner(ticker_db)
    ticker_db.write_aux("ticker", b"old-derived")
    before = _artifacts(ticker_db)

    if failure_point == "stage":
        real_save = jsonlfile.save_jsonl

        def fail_stage(path, records, timespec=None):
            if path.endswith(".range.tmp"):
                raise OSError("stage failure")
            return real_save(path, records, timespec)

        monkeypatch.setattr(jsonlfile, "save_jsonl", fail_stage)
    elif failure_point == "sync":
        monkeypatch.setattr(
            jsonlfile,
            "_sync_file",
            lambda path: (_ for _ in ()).throw(OSError("sync failure")),
        )
    elif failure_point == "aux":
        real_remove = jsonlfile.remove_aux

        def fail_aux(path):
            if path == owner:
                raise OSError("aux failure")
            return real_remove(path)

        monkeypatch.setattr(jsonlfile, "remove_aux", fail_aux)
    else:
        real_replace = jsonlfile.os.replace

        def fail_owner(source, target):
            if target == owner:
                raise OSError("owner failure")
            return real_replace(source, target)

        monkeypatch.setattr(jsonlfile.os, "replace", fail_owner)

    with pytest.raises(OSError):
        ticker_db.replace_df_range(
            "ticker",
            "b",
            "d",
            _frame({"c": {"value": 30}}),
        )

    after = _artifacts(ticker_db)
    assert after[owner] == before[owner]
    assert after[owner + ".idx"] == before[owner + ".idx"]
    assert after[ticker_db.dbmeta_path] == before[ticker_db.dbmeta_path]
    if failure_point == "owner":
        assert after[owner + ".aux"] is None
    else:
        assert after[owner + ".aux"] == before[owner + ".aux"]


def test_index_publication_failure_recovers_reads_and_metadata(
    ticker_db, monkeypatch
):
    owner = _owner(ticker_db)
    ticker_db.write_aux("ticker", b"old-derived")
    real_replace = jsonlfile.os.replace

    def fail_index(source, target):
        if target == owner + ".idx":
            raise OSError("index publication failure")
        return real_replace(source, target)

    monkeypatch.setattr(jsonlfile.os, "replace", fail_index)
    with pytest.raises(OSError):
        ticker_db.replace_df_range(
            "ticker",
            "b",
            "d",
            _frame({"bb": {"value": 22}}),
        )
    monkeypatch.setattr(jsonlfile.os, "replace", real_replace)

    assert jsonlfile.load_jsonl(owner, auto_deserialize=False) == {
        "a": {"value": 1},
        "bb": {"value": 22},
        "e": {"value": 5},
    }
    assert not os.path.exists(owner + ".aux")
    assert jsonlfile.select_line_jsonl(owner, "bb") == {
        "bb": {"value": 22}
    }
    assert ticker_db.get_dbmeta()["ticker"]["count"] == 3
    assert FolderDB(ticker_db.folder_path).get_dbmeta()["ticker"]["count"] == 3


def test_metadata_publication_failure_repairs_current_and_reopened_instances(
    ticker_db, monkeypatch
):
    ticker_db.write_aux("ticker", b"old-derived")
    real_update = ticker_db.update_dbmeta

    monkeypatch.setattr(
        ticker_db,
        "update_dbmeta",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            OSError("metadata publication failure")
        ),
    )
    with pytest.raises(OSError):
        ticker_db.replace_df_range(
            "ticker",
            "b",
            "d",
            _frame({"bb": {"value": 22}}),
        )
    monkeypatch.setattr(ticker_db, "update_dbmeta", real_update)

    assert not os.path.exists(_owner(ticker_db) + ".aux")
    assert ticker_db.get_dbmeta()["ticker"]["count"] == 3
    reopened = FolderDB(ticker_db.folder_path)
    assert reopened.get_dbmeta()["ticker"]["count"] == 3
    assert reopened.get_df(["ticker"])["ticker"]["value"].to_dict() == {
        "a": 1,
        "bb": 22,
        "e": 5,
    }


@pytest.mark.parametrize("read_kind", ["line", "range"])
def test_indexed_reads_retry_an_owner_replaced_after_old_index_load(
    ticker_db, monkeypatch, read_kind
):
    owner = _owner(ticker_db)
    real_load_index = jsonlfile.load_index
    old_index = real_load_index(owner)
    raced = False

    def racing_load_index(path):
        nonlocal raced
        if path == owner and not raced:
            raced = True
            jsonlfile.replace_jsonl_range(
                owner,
                "b",
                "d",
                {"bb": {"value": 22}, "cc": {"value": 33}},
            )
            return old_index
        return real_load_index(path)

    monkeypatch.setattr(jsonlfile, "load_index", racing_load_index)

    if read_kind == "line":
        result = jsonlfile.select_line_jsonl(owner, "bb")
        assert result == {"bb": {"value": 22}}
    else:
        result = jsonlfile.select_jsonl(owner, "a", "e")
        assert result == {
            "a": {"value": 1},
            "bb": {"value": 22},
            "cc": {"value": 33},
            "e": {"value": 5},
        }
    assert raced is True


def test_existing_hierarchy_discovery_mutation_lint_and_repair_remain_compatible(
    tmp_path
):
    db = FolderDB(str(tmp_path), hierarchy_depth=2)
    db.upsert_df("region.ticker", _frame({"a": {"value": 1}, "c": {"value": 3}}))
    db.upsert_dict("region.other", {"x": {"value": 1}})

    db.replace_df_range(
        "region.ticker",
        "b",
        "b",
        _frame({"b": {"value": 2}}),
    )
    db.upsert_dict("region.other", {"y": {"value": 2}})
    db.delete_file_keys("region.other", ["x"])

    owner = _owner(db, "region.ticker")
    with open(owner + ".idx", "wb") as corrupt_index:
        corrupt_index.write(b"not-json")

    assert jsonlfile.select_line_jsonl(owner, "b") == {"b": {"value": 2}}
    db.lint_db(force=True)
    assert sorted(db.get_file_list()) == ["region.other", "region.ticker"]
    assert db.get_df(["region.ticker"])["region.ticker"]["value"].to_dict() == {
        "a": 1,
        "b": 2,
        "c": 3,
    }
    assert db.get_dict("region.other")["region.other"] == {
        "y": {"value": 2}
    }


def test_microsecond_timespec_round_trip_remains_isolated(tmp_path):
    micro_path = tmp_path / "micro"
    second_path = tmp_path / "second"
    micro_path.mkdir()
    second_path.mkdir()
    jsonlfile.save_jsonl(
        str(micro_path / "config.meta"),
        {"timespec": "microseconds"},
    )
    micro = FolderDB(str(micro_path))
    second = FolderDB(str(second_path))
    first = datetime(2024, 1, 1, 0, 0, 0, 100)
    replacement_key = datetime(2024, 1, 1, 0, 0, 0, 250)
    micro.upsert_df("ticker", _frame({first: {"value": 1}}))
    second.upsert_df("ticker", _frame({datetime(2024, 1, 1): {"value": 9}}))

    micro.replace_df_range(
        "ticker",
        datetime(2024, 1, 1, 0, 0, 0, 200),
        datetime(2024, 1, 1, 0, 0, 0, 300),
        _frame({replacement_key: {"value": 2}}),
    )

    assert micro.get_df(["ticker"])["ticker"]["value"].to_dict() == {
        first: 1,
        replacement_key: 2,
    }
    assert second.timespec == "seconds"
    assert jsonlfile.TIME_SPEC == "seconds"
