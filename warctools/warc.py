"""An object to represent warc records"""

import re

from warctools.record import ArchiveRecord
from warctools.stream import open_record_stream

bad_lines = 5 # when to give up looking for the version stamp

@ArchiveRecord.HEADERS(
    DATE='WARC-Date',
    TYPE = 'WARC-Type',
    ID = 'WARC-Record-ID',
    CONTENT_LENGTH = 'Content-Length',
    CONTENT_TYPE = 'Content-Type',
)
class WarcRecord(ArchiveRecord):
    def __init__(self, version=None, headers=None, content=None, errors=None):
        ArchiveRecord.__init__(self,headers,content,errors) 
        self.version = version

    @property
    def type(self):
        return self.headers[self.TYPE]

    @property
    def id(self):
        return self.headers[self.ID]

    @property
    def content_type(self):
        return self.headers[self.CONTENT_TYPE]

    def _write_to(self, out, nl):  
        out.write(self.version)
        out.write(nl)
        for k,v in self.headers:
            out.write(k)
            out.write(": ")
            out.write(v)
            out.write(nl)
        content_type, buffer = self.content
        if content_type:
            out.write(self.CONTENT_TYPE)
            out.write(": ")
            out.write(content_type)
            out.write(nl)
        if buffer:
            content_length = len(buffer)
            out.write(self.CONTENT_LENGTH)
            out.write(": ")
            out.write(content_length)
            out.write(nl)
        out.write(nl)
        if buffer:
            out.write(buffer)
        out.write(nl)
        out.write(nl)

    @classmethod
    def open_archive(cls , filename=None, file_handle=None, mode="rb+", gzip=None): 
        return open_record_stream(cls, filename, file_handle, mode, gzip)

    def repair(self):
        pass

    def validate(self):
        return self.errors

def rx(pat):
    return re.compile(pat,flags=re.IGNORECASE)

version_rx = rx(r'^(?P<prefix>.*?)(?P<version>\s*WARC/.*)' '(?:\r\n|\r|\n)$')
# a header is key: <ws> value plus any following lines with leading whitespace
header_rx = rx(r'^(?P<name>.*?):\s?(?P<value>.*)' '(?:\r\n|\r|\n)$')
value_rx = rx(r'^\s+(.*) ' '\n$')
nl_rx=rx('^\r\n|\r|\n$')
length_rx = rx('^'+WarcRecord.CONTENT_LENGTH+'$')
type_rx = rx('^'+WarcRecord.CONTENT_TYPE+'$')


def parse(stream):
    errors = []
    # find WARC/.*
    while True:   
        line = stream.readline()
        match = version_rx.match(line)

        if match or not line:
            break
        elif not nl_rx.match(line):
            errors.append(('ignored line', line)) 
            if len(errors) > bad_lines:
                raise ValueError, errors  
        
    if line:
        content_length = 0
        content_type = None

        record = WarcRecord(errors=errors)

        record.version = match.group('version').strip()

        prefix = match.group('prefix')

        if prefix:
            record.error('bad prefix on WARC version header', prefix)
        
        #Read headers
        line = stream.readline()
        while line and not nl_rx.match(line):       
          
            #print 'header', line
            match = header_rx.match(line)
            if match:
                name = match.group('name').strip()
                value = [match.group('value').strip()]
                #print 'match',name, value

                line = stream.readline()
                match = value_rx.match(line)
                while match:
                    value.append(match.group().strip())
                    line = stream.readline()
                    match = value_rx.match(line)

                value = " ".join(value)
                
                if type_rx.match(name):
                    if value:
                        content_type = value
                    else:
                        record.error('invalid header',name,value) 
                elif length_rx.match(name):
                    try:
                        #print name, value
                        content_length = int(value)
                        #print content_length
                    except ValueError:
                        record.error('invalid header',name,value) 
                else:
                    record.headers.append((name,value))

        # have read blank line following headers
        
        # read content
        if content_length:
            content=[]
            length = 0
            while length <= content_length:
                line = stream.readline()
                if not line:
                       # print 'no more data' 
                        break
                content.append(line)
                length+=len(line)
            content="".join(content)
            content, line = content[0:content_length], content[content_length+1:]
            if len(content)!= content_length:
                record.error('content length mismatch (is, claims)', len(content), content_length)
            record.content = (content_type, content)
        else:   
            record.error('missing header', WarcRecord.CONTENT_LENGTH)

        # read trailing newlines
        
        newlines = 0
        while line:
            if nl_rx.match(line):
                newlines+=1
                #print 'newline'
                if newlines == 2:
                    break

            else:
                #print 'line', line, newlines
                newlines = 0
                record.error('trailing data after content', line)
            line = stream.readline()

        if newlines < 2:
            record.error('less than two terminating newlines at end of record', newlines)

        return (record, ())
    
    else:
        return (None, errors)
    
                    
            
                


WarcRecord.parse = staticmethod(parse)
