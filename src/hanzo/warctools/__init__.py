"""Main warctools package - provides WARC and ARC file handling.

WARC Format Specification References:
- WARC 1.1 Annotated (primary): https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/
"""

from . import arc, record, s3, warc
from .arc import ArcRecord
from .mixed import MixedRecord
from .record import ArchiveRecord
from .s3 import list_files
from .warc import WarcRecord


def expand_files(files):
    """Expand file patterns, including S3 URLs, into individual file paths.

    Handles both local file paths and S3 URLs. For S3 URLs, lists all
    matching objects in the bucket.

    Args:
        files: Iterable of file paths or S3 URLs

    Yields:
        str: Individual file paths

    Example:
        >>> list(expand_files(['file.warc', 's3://bucket/prefix']))
        ['file.warc', 's3://bucket/prefix/file1.warc', 's3://bucket/prefix/file2.warc']
    """
    for file in files:
        if file.startswith("s3:"):
            yield from list_files(file)
        else:
            yield file


__all__ = [
    "MixedRecord",
    "ArchiveRecord",
    "ArcRecord",
    "WarcRecord",
    "record",
    "warc",
    "arc",
    "s3",
    "expand_files",
]
