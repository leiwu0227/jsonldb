# Visualization

Visualization answers one operational question: how are a table's keys distributed, and how does that distribution compare across tables? It reads only index files, never data, so it is cheap even on large databases and works on any table that has, or can rebuild, an index.

## What is plotted

- **Single table.** A scatter of key value against the record's byte position in the file. A monotonic diagonal means the file is physically sorted; scattered points reveal appends and out-of-order writes; vertical gaps are dead space. This is a quick visual lint.
- **Whole database.** One row per table, key value along the horizontal axis. Rows with the same span show aligned coverage; short or sparse rows reveal tables missing data. A name prefix narrows the set, and figure height is adjustable for databases with many tables.

Keys are interpreted for plotting by trying, in order: a number, an ISO datetime, and finally a hash of the text so that arbitrary string keys still spread across the axis. The horizontal axis becomes a time axis when the first key is a datetime (for the database view, when every table's first key is); otherwise it is linear.

## Two backends

Matplotlib is the default and returns a figure and axes suitable for notebooks and saved images. Bokeh is selectable and returns an interactive figure with pan, zoom, and hover. The Matplotlib paths additionally accept an inclusive start and exclusive end key to window the plot; the Bokeh paths leave windowing to their interactive tools. Height is given in pixels for Bokeh and inches for Matplotlib, with a heuristic conversion when one value is reused across backends.

## Boundaries

Both plotting libraries are imported when this module is imported, so it is reached explicitly and never from the core package. Index reads go through the file store's single loader, so a missing or corrupt index is healed rather than plotted as empty. A database with no tables raises; a table whose index is empty is skipped with a warning rather than failing the whole plot.

## Trade-offs

- Plotting byte position rather than row number keeps the single-table view honest about dead space, at the cost of a less intuitive vertical axis.
- Hashing string keys gives a spread but not an order; for string-keyed tables the horizontal axis is only meaningful when names are numeric or datetime-like.
- Legends are hidden in the database view because with many tables they overwhelm the plot; the categorical vertical axis carries the names instead.

## Source target

- `jsonldb/visual.py`: at most 500 lines.
