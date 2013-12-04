"""Read records from normal file and compressed file"""

import gzip
import re

from hanzo.warctools.archive_detect import is_gzip_file, guess_record_type

def open_record_stream(record_class=None, filename=None, file_handle=None,
                       mode="rb", gzip="auto", offset=None, length=None):
    """Can take a filename or a file_handle. Normally called
    indirectly from A record class i.e WarcRecord.open_archive. If the
    first parameter is None, will try to guess"""

    if file_handle is None:
        if filename.startswith('s3://'):
            from . import s3
            file_handle = s3.open_url(filename, offset=offset, length=length)
        else:
            file_handle = open(filename, mode=mode)
            if offset is not None:
                file_handle.seek(offset)

    if record_class == None:
        record_class = guess_record_type(file_handle)

    if record_class == None:
        raise Exception('Failed to guess compression')

    record_parser = record_class.make_parser()

    if gzip == 'auto':
        if (filename and filename.endswith('.gz')) or is_gzip_file(file_handle):
            gzip = 'record'
            #debug('autodetect: record gzip')
        else:
            # assume uncompressed file
            #debug('autodetected: uncompressed file')
            gzip = None

    if gzip == 'record':
        return GzipRecordStream(file_handle, record_parser)
    elif gzip == 'file':
        return GzipFileStream(file_handle, record_parser)
    else:
        return RecordStream(file_handle, record_parser)


class RecordStream(object):
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
                raise Exception("Errors while decoding %s" % error_str)
            else:
                break

    def _read_record(self, offsets):
        """overridden by sub-classes to read individual records"""
        if self.bytes_to_eoc is not None:
            self._skip_to_eoc()  # skip to end of previous record
        self.bytes_to_eoc = None

        # handle any sort of valid or invalid record terminator
        while True:
            offset = self.fh.tell() if offsets else None
            line = self.fh.readline()
            if not re.match(br'^[\r\n]+$', line):
                break

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
            raise Exception('bytes_to_eoc is unset, cannot skip to end')

        while self.bytes_to_eoc > 0:
            read_size = min(CHUNK_SIZE, self.bytes_to_eoc)
            buf = self._read(read_size)
            if len(buf) < read_size:
                raise Exception('expected {} bytes but only read {}'.format(read_size, len(buf)))

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
        b[:len(tmp)] = tmp
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

CHUNK_SIZE = 8192 # the size to read in, make this bigger things go faster.

class GeeZipFile(gzip.GzipFile):
    """Extends gzip.GzipFile to remember self.member_offset, the raw file
    offset of the current gzip member."""

    def __init__(self, filename=None, mode=None,
                 compresslevel=9, fileobj=None, mtime=None):
        # ignore mtime for python 2.6
        gzip.GzipFile.__init__(self, filename=filename, mode=mode, compresslevel=compresslevel, fileobj=fileobj)
        self.member_offset = None

    # hook in to the place we seem to be able to reliably get the raw gzip
    # member offset
    def _read(self, size=1024):
        if self._new_member:
            try:
                # works for python3.2
                self.member_offset = self.fileobj.tell() - self.fileobj._length + (self.fileobj._read or 0)
            except AttributeError:
                # works for python2.7
                self.member_offset = self.fileobj.tell()

        return gzip.GzipFile._read(self, size)

class GzipRecordStream(RecordStream):
    """A stream to read/write concatted file made up of gzipped
    archive records"""
    def __init__(self, file_handle, record_parser):
        RecordStream.__init__(self, GeeZipFile(fileobj=file_handle), record_parser)
        self.raw_fh = file_handle

    def _read_record(self, offsets):
        if self.bytes_to_eoc is not None:
            self._skip_to_eoc()  # skip to end of previous record
        self.bytes_to_eoc = None

        # handle any sort of valid or invalid record terminator
        while True:
            line = self.fh.readline()
            if not re.match(br'^[\r\n]+$', line):
                break

        record, errors, _offset = \
            self.record_parser.parse(self, offset=None, line=line)

        offset = self.fh.member_offset

        return offset, record, errors

    def seek(self, offset, pos=0):
        """Same as a seek on a file"""
        self.raw_fh.seek(offset, pos)
        # trick to avoid closing and recreating GzipFile, does it always work?
        self.fh._new_member = True

class GzipFileStream(RecordStream):
    """A stream to read/write gzipped file made up of all archive records"""
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
            if not re.match(br'^[\r\n]+$', line):
                break

        record, errors, _offset = \
            self.record_parser.parse(self, offset=None, line=line)

        return offset, record, errors

