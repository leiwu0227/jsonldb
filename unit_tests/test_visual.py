import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from jsonldb import FolderDB
from jsonldb.jsonlfile import save_jsonl
from jsonldb.visual import (
    visualize_folderdb_matplot,
    visualize_jsonl_bokeh,
    visualize_jsonl_matplot,
)


def test_empty_jsonl_bokeh_returns_an_empty_byte_offset_plot(tmp_path):
    path = tmp_path / "empty.jsonl"
    save_jsonl(str(path), {})

    plot = visualize_jsonl_bokeh(str(path))

    assert plot.yaxis[0].axis_label == "Byte Offset"
    assert plot.renderers[0].data_source.data == {"x": [], "y": []}


def test_matplotlib_views_share_inclusive_exclusive_key_window(tmp_path):
    db = FolderDB(str(tmp_path))
    db.overwrite_dict("records", {
        "1": {"value": 1},
        "2": {"value": 2},
        "3": {"value": 3},
        "4": {"value": 4},
    })

    table_figure, table_axes = visualize_jsonl_matplot(
        str(tmp_path / "records.jsonl"), start_index=2, end_index=4
    )
    database_figure, database_axes = visualize_folderdb_matplot(
        db, start_index=2, end_index=4
    )

    try:
        assert table_axes.get_ylabel() == "Byte Offset"
        assert table_axes.collections[0].get_offsets()[:, 0].tolist() == [2.0, 3.0]
        assert database_axes.collections[0].get_offsets()[:, 0].tolist() == [2.0, 3.0]
    finally:
        plt.close(table_figure)
        plt.close(database_figure)
