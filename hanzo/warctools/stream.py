"""Read records from normal file and compressed file"""

import zlib
import gzip
import re

from .log import debug
from .archive_detect import is_gzip_file, guess_record_type

def open_record_stream(record_class=None, filename=None, file_handle=None, mode="rb+", gzip="auto"):
        """Can take a filename or a file_handle. Normally called indirectly from
        A record class i.e WarcRecord.open_archive. If the first parameter is None, will try to guess"""

        if file_handle is None:
            file_handle = open(filename, mode=mode)
        else:
            if not filename:
                filename = file_handle.name

        if record_class == None:
            record_class = guess_record_type(file_handle)

        if record_class == None:
            raise StandardError('Failed to guess compression')

        record_parser = record_class.make_parser()

        if gzip == 'auto':
            if is_gzip_file(file_handle):
                gzip = 'record'
                #debug('autodetect: record gzip')
            else:
                # assume uncompressed file
                #debug('autodetected: uncompressed file')
                gzip = None


        if gzip=='record':
            return GzipRecordStream(file_handle, record_parser)
        elif gzip=='file':
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
        self._parser = None

    def seek(self, offset, pos=0):
        """Same as a seek on a file"""
        self.fh.seek(offset,pos)

    def read_records(self, limit=1, offsets=True):
        """Yield a tuple of (offset, record, errors) where
        Offset is either a number or None.
        Record is an object and errors is an empty list
        or record is none and errors is a list"""

        nrecords = 0
        while nrecords < limit or limit is None:
            offset, record, errors = self._read_record(offsets)
            nrecords+=1
            yield (offset, record,errors)
            if not record:
                break

    def __iter__(self):
        while True:
            offset, record, errors = self._read_record(offsets=False)
            if record:
                yield record
            elif errors:
                raise StandardError('Errors while decoding '+",".join(errors))
            else:
                break

    def _read_record(self, offsets):
        """overridden by sub-classes to read individual records"""
        offset = self.fh.tell() if offsets else None
        record, errors, offset = self.record_parser.parse(self.fh, offset)
        if record:
            #TODO: use compressed_record_size to store size of UNcompressed record for now, rename later
            record.compressed_record_size = self.fh.tell() - offset

        return offset, record, errors

    def write(self, record):
        record.write_to(self)

    def close(self):
        self.fh.close()

class GzipRecordStream(RecordStream):
    """A stream to read/write concatted file made up of gzipped archive records"""
    def __init__(self, file_handle, record_parser):
        RecordStream.__init__(self,file_handle, record_parser)
        self.gz = None

        ###rajbot
        self.next_record = None
        self.next_errors = None
        self.next_offset = None

    def _read_record(self, offsets):
        """rajbot: restructure this function to call parse() twice, once
        to read the record, and once to read possible trailing bytes.
        This is to ensure that the compressed_record_size is correct.
        """
        errors = []

        if self.next_record:
            record   = self.next_record
            errors   = self.next_errors
            offset   = self.next_offset if offsets else None

            self.next_record = None
            self.next_errors = None
            self.next_offset = None

            return offset, record, errors


        offset_ = self.fh.tell()
        self.gz = GzipRecordFile(self.fh)
        record, r_errors, tmp_offset = self.record_parser.parse(self.gz, offset=None)

        next_offset_ = self.fh.tell()
        next_record, next_errors, tmp_next_offset = self.record_parser.parse(self.gz, offset=None)

        if not next_record:
            if record:
                record.compressed_record_size = self.fh.tell() - offset_
        else:
            next_record.error('multiple warc records in gzip record file')
            if record:
                record.compressed_record_size = next_offset_ - offset_
            next_record.compressed_record_size = self.fh.tell() - next_offset_
            self.next_record = next_record
            self.next_errors = next_errors
            self.next_offset = next_offset_

        self.gz.close()

        offset  = offset_ if offsets else None
        errors.extend(r_errors)
        return offset, record, errors


class GzipFileStream(RecordStream):
    """A stream to read/write gzipped file made up of all archive records"""
    def __init__(self, file_handle, record):
        RecordStream.__init__(self,gzip.GzipFile(fileobj=file_handle), record)
    def _read_record(self, offsets):
        # no real offsets in a gzipped file (no seperate records)
        return RecordStream._read_record(self, False)



### record-gzip handler, based on zlib
### implements readline() access over a a single
### gzip-record. must be re-created to read another record


CHUNK_SIZE=1024 # the size to read in, make this bigger things go faster.
line_rx=re.compile('^(?P<line>^[^\r\n]*(?:\r\n|\r(?!\n)|\n))(?P<tail>.*)$',re.DOTALL)

class GzipRecordFile(object):
    """A file like class providing 'readline' over catted gzip'd records"""
    def __init__(self, fh):
        self.fh = fh
        self.buffer=""
        self.z = zlib.decompressobj(16+zlib.MAX_WBITS)
        self.done = False


    def _getline(self):
            if self.buffer:
                #a,nl,b
                match=line_rx.match(self.buffer)
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

    def readline(self):
        while True:
            output = self._getline()
            if output:
                    return output

            if self.done:
                return ""

            #print 'read chunk at', self.fh.tell(), self.done
            chunk = self.fh.read(CHUNK_SIZE)
            out = self.z.decompress(chunk)

            if out:
                #rajbot: if the decompressed string ends with a \r, then
                #read another chunk in case a \n is next. We need to do
                #this because the line_rx pattern allows \r to be a valid
                #newline, which causes the following \n to become a blank
                #line which might prematurely end parsing of warc headers.
                if out.endswith('\r') and not self.z.unused_data:
                    next_chunk = self.fh.read(CHUNK_SIZE)
                    next_out   = self.z.decompress(next_chunk)
                    if next_out is not None:
                        out += next_out

                self.buffer+=out

            if self.z.unused_data:
                #print 'unused', len(self.z.unused_data)
                self.fh.seek(-len(self.z.unused_data),1)
                self.done=True
                continue
            if not chunk:
                self.done = True
                continue

    def read(self, num_bytes):
        # add bytes to self.buff if the current buffer doesn't have enough
        # data in it, and we have not exhaused the compressed stream
        while (len(self.buffer) < num_bytes) and not self.done:
            chunk = self.fh.read(CHUNK_SIZE)
            d_bytes = self.z.decompress(chunk)
            if d_bytes:
                self.buffer += d_bytes

            if self.z.unused_data:
                #print 'unused', len(self.z.unused_data)
                self.fh.seek(-len(self.z.unused_data),1)
                self.done=True
                break
            if not chunk:
                self.done = True
                break

        out = self.buffer[:num_bytes]
        self.buffer = self.buffer[num_bytes:]

        return out

    def close(self):
        if self.z:
            self.z.flush()
