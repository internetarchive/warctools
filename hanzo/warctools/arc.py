"""An object to represent arc records"""

import sys
import re

from .record import ArchiveRecord,ArchiveParser
from .stream import open_record_stream
from .archive_detect import register_record_type

# URL<sp>IP-address<sp>Archive-date<sp>Content-type<sp>
#Result-code<sp>Checksum<sp>Location<sp> Offset<sp>Filename<sp>
#Archive-length<nl> 
# 
@ArchiveRecord.HEADERS(
    URL='URL',
    IP='IP-address',
    DATE='Archive-date',
    CONTENT_TYPE = 'Content-type',
    CONTENT_LENGTH = 'Archive-length',
    RESULT_CODE = 'Result-code',
    CHECKSUM = 'Checksum',
    LOCATION = 'Location',
    OFFSET = 'Offset',
    FILENAME = 'Filename',
)
class ArcRecord(ArchiveRecord):
    def __init__(self, headers=None, content=None, errors=None):
        ArchiveRecord.__init__(self,headers,content,errors) 

    @property
    def type(self):
        return "response"

    def _write_to(self, out, nl):  
        pass

    @classmethod
    def make_parser(self):
        return ArcParser()


class ArcRecordHeader(ArcRecord):
    def __init__(self, headers=None, content=None, errors=None, version=None, raw_headers=None):
        ArcRecord.__init__(self,headers,content,errors) 
        self.version = version
        self.raw_headers = raw_headers
    @property
    def type(self):
        return "filedesc"
    def raw(self):
        return "".join(self.raw_headers) + self.content[1]

def rx(pat):
    return re.compile(pat,flags=re.IGNORECASE)

nl_rx=rx('^\r\n|\r|\n$')
length_rx = rx('^'+ArcRecord.CONTENT_LENGTH+'$')
type_rx = rx('^'+ArcRecord.CONTENT_TYPE+'$')

class ArcParser(ArchiveParser):
    def __init__(self):
        self.version = 0
        # we don't know which version to parse initially - a v1 or v2 file
        # so we read the filedesc because the order and number of the headers change
        # between versions.

        # question? will we get arc fragments?
        # should we store both headers & detect records by header length?
        # if we don't know 

        self.headers = []
        self.trailing_newlines = 0

    def parse(self, stream, offset):
        record = None
        content_type = None
        content_length = None
        line = stream.readline()
        while not line.rstrip():
            if not line:
                return (None,(), offset)
            self.trailing_newlines-=1
            line = stream.readline()


        if line.startswith('filedesc:'):
            raw_headers = []
            raw_headers.append(line)
            # read headers named in body of record
            # to assign names to header, to read body of record
            arc_version_line = stream.readline()
            raw_headers.append(arc_version_line)
            arc_names_line = stream.readline()
            raw_headers.append(arc_names_line)

            arc_version=arc_version_line.strip()

            # configure parser instance
            self.version = arc_version.split()[0]
            self.headers = arc_names_line.strip().split()
            
            # now we have read header field in record body
            # we can extract the headers from the current record,
            # and read the length field

            # which is in a different place with v1 and v2
        
            # read headers 
            arc_headers = self.get_header_list(line.strip().split())
            
            # extract content, ignoring header lines parsed already
            content_type, content_length, errors = self.get_content_headers(arc_headers)

            content_length = content_length - len(arc_version_line) - len(arc_names_line)

            record = ArcRecordHeader(headers = arc_headers, version=arc_version, errors=errors, raw_headers=raw_headers)

        else:
            if not self.headers:
                raise StandardHeader('missing filedesc')
            headers = self.get_header_list(line.strip().split())
            content_type, content_length, errors = self.get_content_headers(headers)

            record = ArcRecord(headers = headers, errors=errors)

        line = None

        if content_length:
            content=[]
            length = 0
            while length < content_length:
                line = stream.readline()
                if not line:
                       # print 'no more data' 
                        break
                content.append(line)
                length+=len(line)
            content="".join(content)
            content, line = content[0:content_length], content[content_length+1:]
            record.content = (content_type, content)


        if line:
            record.error('trailing data at end of record', line)
        if  line == '':
            self.trailing_newlines = 1

        return (record, (), offset)

    def trim(self, stream):
        return ()

    def get_header_list(self, values):
        return zip(self.headers, values)


    @staticmethod
    def get_content_headers(headers):
        content_type = None
        content_length = None
        errors = []

        for name,value in headers:
            if type_rx.match(name):
                if value:
                    content_type = value
                else:
                    errors.append(('invalid header',name,value)) 
            elif length_rx.match(name):
                try:
                    content_length = int(value)
                except ValueError:
                    errors.append(('invalid header',name,value)) 

        return content_type, content_length, errors


register_record_type(re.compile('^filedesc://'), ArcRecord)
