import logging

import git
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from jsonldb import FolderDB, vercontrol
from jsonldb.jsonlfile import save_jsonl
from jsonldb.visual import (
    visualize_folderdb_bokeh,
    visualize_folderdb_matplot,
)


def test_gitpython_init_logs_repository_path_without_stdio(tmp_path, caplog, capsys):
    repository = tmp_path / "repository"
    caplog.set_level(logging.INFO, logger="jsonldb.vercontrol")

    result = vercontrol.init_folder(str(repository))

    repo = git.Repo(str(repository))
    assert result is None
    assert vercontrol.is_versioned(str(repository))
    assert repo.working_tree_dir == str(repository.resolve())
    assert any(
        record.getMessage() == "initialized git repository in %s" % repository
        for record in caplog.records
    )
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_real_visualizers_log_discovery_and_empty_data_without_stdio(
    tmp_path, caplog, capsys
):
    db = FolderDB(str(tmp_path))
    db.overwrite_dict("records", {"1": {"value": 1}})
    table = tmp_path / "empty.jsonl"
    save_jsonl(str(table), {})
    caplog.set_level(logging.INFO, logger="jsonldb.visual")

    bokeh_plot = visualize_folderdb_bokeh(db)
    matplotlib_figure, matplotlib_axes = visualize_folderdb_matplot(db)

    try:
        assert bokeh_plot.title.text == "FolderDB Line Keys Distribution"
        assert bokeh_plot.renderers[0].data_source.data == {
            "x": [1.0],
            "y": ["records"],
            "filename": ["records"],
        }
        assert matplotlib_axes.get_title() == "FolderDB Line Keys Distribution"
        assert matplotlib_axes.collections[0].get_offsets()[:, 0].tolist() == [1.0]

        messages = [record.getMessage() for record in caplog.records]
        discovery = "found 2 JSONL files in %s" % tmp_path
        empty_index = "empty index for %s" % table
        assert messages.count(discovery) == 2
        assert messages.count(empty_index) == 2

        captured = capsys.readouterr()
        assert captured.out == ""
        assert captured.err == ""
    finally:
        plt.close(matplotlib_figure)
