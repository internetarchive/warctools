"""Read records from normal file and compressed file

WARC Format Specification References:
- WARC 1.1 Annotated (primary): https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/
- Compression: See Annex D "Compression recommendations"
  https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#annex-d-informative-compression-recommendations
"""

import gzip
import re

from hanzo.warctools.archive_detect import guess_record_type, is_gzip_file


def open_record_stream(
    record_class=None,
    filename=None,
    file_handle=None,
    mode="rb",
    gzip="auto",
    offset=None,
    length=None,
):
    """Open an archive file and return a RecordStream for reading records.

    Factory function that creates an appropriate RecordStream based on
    the file format and compression. Supports local files, S3 URLs, and
    automatic format/compression detection.

    Args:
        record_class: Optional ArchiveRecord class (auto-detected if None)
        filename: Path to archive file or S3 URL (s3://bucket/key)
        file_handle: Optional file-like object (takes precedence over filename)
        mode: File open mode (default: "rb")
        gzip: Compression mode - "auto" (detect), "record" (per-record gzip),
              "file" (file-level gzip), or None (uncompressed)
        offset: Optional byte offset to seek to before reading
        length: Optional length limit for S3 requests

    Returns:
        RecordStream: Stream for reading archive records

    Raises:
        Exception: If format detection fails or file cannot be opened

    See:
        WARC 1.1 Annex D: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#annex-d-informative-compression-recommendations

    Example:
        >>> stream = open_record_stream(filename="archive.warc.gz")
        >>> for record in stream:
        ...     print(record.type)
    """

    if file_handle is None:
        if filename.startswith("s3://"):
            from . import s3

            file_handle = s3.open_url(filename, offset=offset, length=length)
        else:
            file_handle = open(filename, mode=mode)
            if offset is not None:
                file_handle.seek(offset)

    if record_class is None:
        record_class = guess_record_type(file_handle)

    if record_class is None:
        raise Exception("Failed to guess compression")

    record_parser = record_class.make_parser()

    if gzip == "auto":
        if (filename and filename.endswith(".gz")) or is_gzip_file(file_handle):
            gzip = "record"
            # Record-at-a-time compression per WARC 1.1 Annex D.2
            # https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#record-at-time-compression
        else:
            # assume uncompressed file
            gzip = None

    if gzip == "record":
        # Record-at-a-time compression: each WARC record is a separate gzip member
        # See Annex D.2: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#record-at-time-compression
        return GzipRecordStream(file_handle, record_parser)
    elif gzip == "file":
        # File-level compression: entire WARC file is one gzip stream
        return GzipFileStream(file_handle, record_parser)
    else:
        # Uncompressed WARC file
        return RecordStream(file_handle, record_parser)


class RecordStream:
    """A readable/writable stream of Archive Records. Can be iterated over
    or read_records can give more control, and potentially offset information.
    """

    def __init__(self, file_handle, record_parser):
        self.fh = file_handle
        self.record_parser = record_parser

        # Number of bytes until the end of the record's content, if known.
        # Normally set by the record parser based on the Content-Length header.
        self.bytes_to_eoc = None

    def seek(self, offset, pos=0):
        """Same as a seek on a file"""
        self.fh.seek(offset, pos)

    def read_records(self, limit=1, offsets=True):
        """Yield a tuple of (offset, record, errors) where
        Offset is either a number or None.
        Record is an object and errors is an empty list
        or record is none and errors is a list"""
        nrecords = 0
        while limit is None or nrecords < limit:
            offset, record, errors = self._read_record(offsets)
            nrecords += 1
            yield (offset, record, errors)
            if not record:
                break

    def __iter__(self):
        while True:
            _, record, errors = self._read_record(offsets=False)
            if record:
                yield record
            elif errors:
                error_str = ",".join(str(error) for error in errors)
                raise Exception(f"Errors while decoding {error_str}")
            else:
                break

    def _read_record(self, offsets):
        """overridden by sub-classes to read individual records"""
        if self.bytes_to_eoc is not None:
            self._skip_to_eoc()  # skip to end of previous record
        self.bytes_to_eoc = None

        # Capture offset before reading (for first record, this should be 0)
        offset = self.fh.tell() if offsets else None

        # handle any sort of valid or invalid record terminator
        while True:
            line = self.fh.readline()
            if not re.match(rb"^[\r\n]+$", line):
                # Update offset to current position before the actual record starts
                if offsets and offset is not None:
                    # Offset should point to start of this line (the actual record)
                    offset = self.fh.tell() - len(line)
                break
            elif offsets and offset is not None:
                # Update offset as we skip empty lines
                offset += len(line)

        record, errors, offset = self.record_parser.parse(self, offset, line)
        return offset, record, errors

    def write(self, record):
        """Writes an archive record to the stream"""
        record.write_to(self)

    def close(self):
        """Close the underlying file handle."""
        self.fh.close()

    def _skip_to_eoc(self):
        if self.bytes_to_eoc is None:
            raise Exception("bytes_to_eoc is unset, cannot skip to end")

        while self.bytes_to_eoc > 0:
            read_size = min(CHUNK_SIZE, self.bytes_to_eoc)
            buf = self._read(read_size)
            if len(buf) < read_size:
                raise Exception(f"expected {read_size} bytes but only read {len(buf)}")

    def _read(self, count=None):
        """Raw read, will read into next record if caller isn't careful"""
        if count is not None:
            result = self.fh.read(count)
        else:
            result = self.fh.read()

        if self.bytes_to_eoc is not None:
            self.bytes_to_eoc -= len(result)

        return result

    def read(self, count=None):
        """Safe read for reading content, will not read past the end of the
        payload, assuming self.bytes_to_eoc is set. The record's trailing
        bytes, \\r\\n\\r\\n for warcs or \\n for arcs, will remain when this
        method returns "".
        """
        if self.bytes_to_eoc is not None and count is not None:
            read_size = min(count, self.bytes_to_eoc)
        elif self.bytes_to_eoc is not None:
            read_size = self.bytes_to_eoc
        elif count is not None:
            read_size = count
        else:
            read_size = None

        return self._read(read_size)

    # XXX dumb implementation to support python3 http.client
    def readinto(self, b):
        tmp = self.read(count=len(b))
        b[: len(tmp)] = tmp
        return len(tmp)

    def readline(self, maxlen=None):
        """Safe readline for reading content, will not read past the end of the
        payload, assuming self.bytes_to_eoc is set. The record's trailing
        bytes, \\r\\n\\r\\n for valid warcs or \\n for valid arcs, will remain
        when this method returns "".
        """
        if self.bytes_to_eoc is not None and maxlen is not None:
            lim = min(maxlen, self.bytes_to_eoc)
        elif self.bytes_to_eoc is not None:
            lim = self.bytes_to_eoc
        elif maxlen is not None:
            lim = maxlen
        else:
            lim = None

        if lim is not None:
            result = self.fh.readline(lim)
        else:
            result = self.fh.readline()

        if self.bytes_to_eoc is not None:
            self.bytes_to_eoc -= len(result)
        return result


CHUNK_SIZE = 8192  # the size to read in, make this bigger things go faster.


class GeeZipFile(gzip.GzipFile):
    """Extends gzip.GzipFile to remember self.member_offset, the raw file
    offset of the current gzip member."""

    def __init__(self, filename=None, mode=None, compresslevel=9, fileobj=None, mtime=None):
        gzip.GzipFile.__init__(
            self,
            filename=filename,
            mode=mode,
            compresslevel=compresslevel,
            fileobj=fileobj,
        )
        self.member_offset = 0  # First record starts at offset 0

    def _read_gzip_header(self):
        """This is called at the beginning of each gzip member.
        We can capture the raw file's current position."""
        self.member_offset = self.fileobj.tell()
        return super()._read_gzip_header()


class GzipRecordStream(RecordStream):
    """A stream to read/write concatenated file made up of gzipped archive records.

    Implements record-at-a-time compression per WARC 1.1 Annex D.2:
    https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#record-at-time-compression

    Each WARC record is compressed as a separate gzip member, allowing random access
    to individual records via offset tracking. This preserves the ability to seek
    to specific records by offset, unlike file-level compression.

    File naming convention: .warc.gz suffix per Annex D.3
    https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#annex-d-informative-compression-recommendations
    """

    def __init__(self, file_handle, record_parser):
        RecordStream.__init__(self, GeeZipFile(fileobj=file_handle), record_parser)
        self.raw_fh = file_handle

    def _read_record(self, offsets):
        if self.bytes_to_eoc is not None:
            self._skip_to_eoc()  # skip to end of previous record
        self.bytes_to_eoc = None

        # Before reading, capture the current member_offset.
        # It will be 0 for the first record, and the start of the member for subsequent ones.
        offset = self.fh.member_offset if offsets else None

        # handle any sort of valid or invalid record terminator
        while True:
            line = self.fh.readline()
            if not re.match(rb"^[\r\n]+$", line):
                break
            if not line:  # EOF
                return None, None, offset

        if not line:
            return None, None, offset

        # After readline, member_offset should be updated if a new member was crossed
        if offsets and self.fh.member_offset is not None:
            offset = self.fh.member_offset

        record, errors, _ = self.record_parser.parse(self, offset, line)

        return offset, record, errors

    def seek(self, offset, pos=0):
        """Same as a seek on a file"""
        self.raw_fh.seek(offset, pos)
        # trick to avoid closing and recreating GzipFile, does it always work?
        self.fh._new_member = True


class GzipFileStream(RecordStream):
    """A stream to read/write gzipped file made up of all archive records.

    Implements file-level compression where the entire WARC file is compressed
    as a single gzip stream. This is more efficient for storage but does not
    support offset tracking for individual records since the file is one
    continuous compressed stream.

    Note: Record-at-a-time compression (GzipRecordStream) is recommended per
    WARC 1.1 Annex D.2 as it preserves random access capabilities.

    See:
        WARC 1.1 Annex D: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#annex-d-informative-compression-recommendations
    """

    def __init__(self, file_handle, record):
        RecordStream.__init__(self, gzip.GzipFile(fileobj=file_handle), record)

    def _read_record(self, offsets):
        # no useful offsets in a gzipped file
        if self.bytes_to_eoc is not None:
            self._skip_to_eoc()  # skip to end of previous record
        self.bytes_to_eoc = None

        # handle any sort of valid or invalid record terminator
        while True:
            line = self.fh.readline()
            if not re.match(rb"^[\r\n]+$", line):
                break

        record, errors, _offset = self.record_parser.parse(self, offset=None, line=line)

        return _offset, record, errors
