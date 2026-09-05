"""
JSONLDB - A simple file-based database that stores data in JSONL format.

FolderDB is imported by default. Supporting storage, DataFrame, metadata, and
report modules remain available through the package namespace.

Example usage:
    from jsonldb import FolderDB
"""

from .folderdb import FolderDB

__version__ = "1.0.0"
__all__ = ["FolderDB", "jsonlfile", "jsonldf", "metaslot", "reports"]
