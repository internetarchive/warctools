"""Read records from normal file and compressed file"""

import zlib
import gzip
import re

from hanzo.warctools.archive_detect import is_gzip_file, guess_record_type

def open_record_stream(record_class=None, filename=None, file_handle=None,
                       mode="rb+", gzip="auto", offset=None, length=None):
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
        raise StandardError('Failed to guess compression')

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
        self.record_fh = None


    def seek(self, offset, pos=0):
        """Same as a seek on a file"""
        self.fh.seek(offset, pos)

    def read_records(self, limit=1, offsets=True):
        """Yield a tuple of (offset, record, errors) where
        Offset is either a number or None.
        Record is an object and errors is an empty list
        or record is none and errors is a list"""
        nrecords = 0
        while nrecords < limit or limit is None:
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
                raise StandardError("Errors while decoding %s" % error_str)
            else:
                break

    def _read_record(self, offsets):
        """overridden by sub-classes to read individual records"""
        if self.record_fh is not None:
            self.record_fh.skip_to_end()
        offset = self.fh.tell() if offsets else None
        self.record_fh = RecordFile(self.fh)
        record, errors, offset = self.record_parser.parse(self.record_fh, offset)
        return offset, record, errors

    def write(self, record):
        """Writes an archive record to the stream"""
        record.write_to(self)

    def close(self):
        """Close the underlying file handle."""
        self.fh.close()

class GzipRecordStream(RecordStream):
    """A stream to read/write concatted file made up of gzipped
    archive records"""
    def __init__(self, file_handle, record_parser):
        RecordStream.__init__(self, file_handle, record_parser)
        self.record_fh = None

    def _read_record(self, offsets):
        errors = []
        if self.record_fh is not None:
            self.record_fh.skip_to_end()
            self.record_fh.close()

        offset = self.fh.tell() if offsets else None
        self.record_fh = GzipRecordFile(self.fh)
        record, r_errors, _offset = \
            self.record_parser.parse(self.record_fh, offset=None)
        errors.extend(r_errors)
        return offset, record, errors


class GzipFileStream(RecordStream):
    """A stream to read/write gzipped file made up of all archive records"""
    def __init__(self, file_handle, record):
        RecordStream.__init__(self, gzip.GzipFile(fileobj=file_handle), record)

    def _read_record(self, offsets):
        # no real offsets in a gzipped file (no seperate records)
        return RecordStream._read_record(self, False)

### record-gzip handler, based on zlib
### implements readline() access over a a single
### gzip-record. must be re-created to read another record


CHUNK_SIZE = 8192 # the size to read in, make this bigger things go faster.
line_rx = re.compile('^(?P<line>^[^\r\n]*(?:\r\n|\r(?!\n)|\n))(?P<tail>.*)$',
                     re.DOTALL)

class RecordFile(object):
    def __init__(self, fh):
        self.fh = fh
        self.expected_bytes_left = None

    def readline(self):
        output = self.fh.readline()
        if self.expected_bytes_left is not None:
            self.expected_bytes_left -= len(output)
        return output

    def read(self, count):
        output = self.fh.read(count)
        if self.expected_bytes_left is not None:
            self.expected_bytes_left -= len(output)
        return output

    def skip_to_end(self):
        if self.expected_bytes_left is None:
            raise Exception('expected_bytes_left is unset, cannot skip to end')

        while self.expected_bytes_left > 0:
            read_size = min(CHUNK_SIZE, self.expected_bytes_left)
            buf = self.read(read_size)
            if len(buf) < read_size:
                raise Exception('expected {} more bytes but only read {}'.format(self.expected_bytes_left, len(buf)))


class GzipRecordFile(RecordFile):
    """A file like class providing 'readline' over catted gzip'd records"""
    def __init__(self, fh):
        RecordFile.__init__(self, fh)
        self.buffer = ""
        self.z = zlib.decompressobj(16+zlib.MAX_WBITS)
        self.done = False
        self.expected_bytes_left = None


    def _getline(self):
        if self.buffer:
            #a,nl,b
            match = line_rx.match(self.buffer)
            #print match
            # print 'split:', split[0],split[1], len(split[2])
            if match:
                output = match.group('line')

                self.buffer = ""+match.group('tail')
                return output

            elif self.done:
                output = self.buffer
                self.buffer = ""

                return output

    def _read_chunk(self):
        chunk = self.fh.read(CHUNK_SIZE)
        out = self.z.decompress(chunk)

        # if we hit a \r on reading a chunk boundary, read a little more
        # in case there is a following \n
        while out.endswith('\r') and not self.z.unused_data:
            chunk = self.fh.read(CHUNK_SIZE)
            if not chunk:
                break
            tail = self.z.decompress(chunk)
            if tail:
                out += tail
                break

        if self.z.unused_data:
            self.fh.seek(-len(self.z.unused_data), 1)
            self.done = True

        if not chunk:
            self.done = True

        return out

    def readline(self):
        while True:
            output = self._getline()
            if output:
                if self.expected_bytes_left is not None:
                    self.expected_bytes_left -= len(output)
                return output

            if self.done:
                return ""

            chunk = self._read_chunk()
            if chunk:
                self.buffer += chunk

    def read(self, count):
        """Reads `count` bytes from the stream. The output will be truncated if
        there are less than `count` bytes remaining in the stream."""
        length = len(self.buffer)
        chunks = []
        while not self.done and length < count:
            chunk = self._read_chunk()
            if not chunk:
                break
            length += len(chunk)
            chunks.append(chunk)
        self.buffer += "".join(chunks)

        output = self.buffer[:count]
        self.buffer = self.buffer[count:]
        if self.expected_bytes_left is not None:
            self.expected_bytes_left -= len(output)
        return output

    def close(self):
        if self.z:
            self.z.flush()
