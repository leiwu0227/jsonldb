"""
JSONLDB - A simple file-based database that stores data in JSONL format.
"""

from .folderdb import FolderDB
from . import visual
from . import vercontrol
from . import jsonlfile
from . import jsonldf

__version__ = "0.1.0"
__all__ = ["FolderDB", "visual", "vercontrol", "jsonlfile", "jsonldf"] 