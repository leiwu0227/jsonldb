"""
JSONLDB - A simple file-based database that stores data in JSONL format.

Modular import structure - import only what you need to avoid loading
unnecessary dependencies (e.g., git for vercontrol, bokeh/matplotlib for visual).

Example usage:
    from jsonldb import FolderDB  # Core functionality only
    from jsonldb.vercontrol import commit  # Only if you need version control
    from jsonldb.visual import visualize_folderdb  # Only if you need visualization
"""

from .folderdb import FolderDB
from .catalog import (
    CatalogBusyError,
    CatalogFilesystemError,
    CatalogRecoveryError,
    FolderCatalogEntry,
    FolderCatalogSnapshot,
    UnsupportedCatalogVersionError,
)

__version__ = "1.0.0"
__all__ = [
    "FolderDB", "FolderCatalogEntry", "FolderCatalogSnapshot",
    "CatalogBusyError", "CatalogFilesystemError", "CatalogRecoveryError",
    "UnsupportedCatalogVersionError", "visual", "vercontrol", "jsonlfile",
    "jsonldf",
]
