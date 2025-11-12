"""Archive format detection and registration.

This module provides utilities for detecting WARC and ARC file formats
and registering custom record type parsers.
"""

import gzip

archive_types = []


def is_gzip_file(file_handle):
    """Check if a file handle points to a gzip-compressed file.

    Detects gzip files by reading the magic number (0x1f 0x8b).
    The file position is restored after checking.

    Args:
        file_handle: File-like object to check

    Returns:
        bool: True if the file appears to be gzip-compressed
    """
    signature = file_handle.read(2)
    file_handle.seek(-len(signature), 1)
    return signature == b"\x1f\x8b"


def guess_record_type(file_handle):
    """Guess the archive record type from file content.

    Attempts to detect whether the file contains WARC or ARC records
    by reading the first line and matching against registered patterns.
    Handles both compressed (gzip) and uncompressed files.

    Args:
        file_handle: File-like object to inspect

    Returns:
        ArchiveRecord class or None: The record class if detected, None otherwise

    See:
        WARC 1.1 Section 4: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#file-and-record-model
    """
    offset = file_handle.tell()
    if is_gzip_file(file_handle):
        nfh = gzip.GzipFile(fileobj=file_handle)
    else:
        nfh = file_handle

    line = nfh.readline()
    file_handle.seek(offset)
    for rx, record in archive_types:
        if rx.match(line):
            return record

    else:
        return None


def register_record_type(rx, record):
    """Register a record type pattern for format detection.

    Registers a regex pattern and corresponding record class for use
    in format detection. Patterns are checked in registration order.

    Args:
        rx: Compiled regex pattern to match against first line of file
        record: ArchiveRecord class to return when pattern matches

    Example:
        register_record_type(version_rx, WarcRecord)
    """
    archive_types.append((rx, record))
