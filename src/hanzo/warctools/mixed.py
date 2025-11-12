"""Mixed WARC/ARC record parser.

This module provides support for files containing both WARC and ARC records,
allowing automatic detection and parsing of mixed archive formats.
"""

from hanzo.warctools.arc import ArcParser
from hanzo.warctools.record import ArchiveParser, ArchiveRecord
from hanzo.warctools.warc import WarcParser


class MixedRecord(ArchiveRecord):
    """Archive record that can represent either WARC or ARC format records.

    Used when the archive format is unknown or when processing files
    containing both WARC and ARC records.
    """

    @classmethod
    def make_parser(cls):
        """Create a parser for mixed WARC/ARC records."""
        return MixedParser()


class MixedParser(ArchiveParser):
    """Parser that automatically detects and parses WARC or ARC records.

    Detects record type by examining the first line:
    - Lines starting with "WARC" are parsed as WARC records
    - Other non-empty lines are parsed as ARC records
    - Empty lines are skipped

    See:
        WARC 1.1 Section 4: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#file-and-record-model
    """

    def __init__(self):
        """Initialize parser with both ARC and WARC parsers."""
        self.arc = ArcParser()
        self.warc = WarcParser()

    def parse(self, stream, offset=None, line=None):
        """Parse a record from the stream, detecting format automatically.

        Args:
            stream: File-like object to read from
            offset: Optional byte offset of record start
            line: Optional first line (if already read)

        Returns:
            tuple: (record, errors, offset) where record is None if parsing failed
        """
        if line is None:
            line = stream.readline()

        while line:
            if line.startswith(b"WARC"):
                return self.warc.parse(stream, offset, line=line)
            elif line not in (b"\n", b"\r\n", b"\r"):
                return self.arc.parse(stream, offset, line=line)

            line = stream.readline()
        return None, (), offset
