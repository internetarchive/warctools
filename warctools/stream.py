"""Read records from normal file and compressed file"""

import zlib
import gzip
import re

class RecordStream(object):
    def __init__(self, file_handle, record):
        self.fh = file_handle
        self.record = record

    def seek(self, offset):
        self.fh.seek(offset)

    def read_records(self, limit=1):
        
        nrecords = 0
        while nrecords < limit or limit is None:
            offset, record, errors = self._read_record()
            nrecords+=1
            yield (offset, record,errors)
            if not record: 
                break

    def __iter__(self):
        while True:
            record, errors = self._read_record()
            if record:
                yield record
            else:
                break
            
    def _read_record(self):
        offset = self.fh.tell()
        record, errors= self.record.parse(self.fh)
        return offset, record, errors

    def index(self):
        pass

    def write(self, record):
        pass

class GzipRecordStream(RecordStream):
    def __init__(self, file_handle, record):
        RecordStream.__init__(self,file_handle, record)
    def _read_record(self):
        offset = self.fh.tell()
        gz = GzipRecordFile(self.fh)
        record, errors= self.record.parse(gz)
        if not gz.done:
            nlines = 0
            while gz.readline():
                nlines+=1
            if nlines:
                e = ("trailing data in gzipped record, try 'file' mode, at gzip archive offset", offset," lines ignored", nlines)
                if record:
                    record.error(*e)
                else:
                    errors.append(e)
                
        return offset, record, errors

class GzipFileStream(RecordStream):
    def __init__(self, file_handle, record):
        RecordStream.__init__(self,gzip.GzipFile(fileobj=file_handle), record)
    def _read_record(self):
        # no real offsets in a gzipped file (no seperate records)
        offset, record, errors = RecordStream._read_record(self)
        return None, record, errors


def open_record_stream(record, filename=None, file_handle=None, mode="rb+", gzip=None):
        if file_handle is None:
            file_handle = open(filename, mode=mode)
        else:
            if not filename:
                filename = file_handle.name

        if gzip=='record':
            return GzipRecordStream(file_handle, record)
        elif gzip=='file':
            return GzipFileStream(file_handle, record)
        else:
            return RecordStream(file_handle, record)
        
    

CHUNK_SIZE=1024
line_rx=re.compile('^(?P<line>^[^\r\n]*(?:\r\n|\r(?!\n)|\n))(?P<tail>.*)$',re.DOTALL)

class GzipRecordFile(object):
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
            if out: self.buffer+=out

            if self.z.unused_data:
                #print 'unused', len(self.z.unused_data)
                self.fh.seek(-len(self.z.unused_data),1)
                self.done=True
                continue
            if not chunk:
                self.done = True
                continue
            
                
    
            
                
