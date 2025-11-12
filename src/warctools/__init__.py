"""Warctools package - re-exports from hanzo for compatibility."""

# Import everything from hanzo to maintain backward compatibility
import sys
from pathlib import Path

# Add src/hanzo to path so we can import it
src_path = Path(__file__).parent.parent
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from hanzo import warctools
from hanzo.warctools import ArchiveRecord, ArcRecord, MixedRecord, WarcRecord, expand_files

__all__ = [
    "WarcRecord",
    "ArcRecord",
    "MixedRecord",
    "ArchiveRecord",
    "expand_files",
    "warctools",
]
