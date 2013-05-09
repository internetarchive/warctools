"""An object to represent arc records"""

import sys
import re
import base64
import hashlib
import zlib

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

        #raj: alexa arc files don't always have content-type in header
        self.short_headers = [ArcRecord.URL, ArcRecord.IP, ArcRecord.DATE, ArcRecord.CONTENT_LENGTH]

    def parse(self, stream, offset):
        record = None
        content_type = None
        content_length = None
        try:
            line = stream.readline()
        except zlib.error:
            #raj: some ARC files contain trailing padding zeros
            #see DR_crawl22.20030622054102-c/DR_crawl22.20030622142039.arc.gz for an example
            return (None,(), offset)
        while not line.rstrip():
            if not line:
                return (None,(), offset)
            self.trailing_newlines-=1
            if offset:
                offset += len(line) #rajbot
            line = stream.readline()

        while line.endswith('\r'):
            #raj: some ARC record headers contain a url with a '\r' character.
            #The record header should end in \n, but we may also encounter
            #malformed header lines that end with \r, so we need to see of we
            #read the whole header, or just part of the url.
            if not re.search('(?:\d{1,3}\.){3}\d{1,3} \d{14} \S+ \d+$', line):
                line += stream.readline()
            else:
                break

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

            # raj: some v1 ARC files are incorrectly sending a v2 header names line
            if arc_names_line == 'URL IP-address Archive-date Content-type Result-code Checksum Location Offset Filepath Archive-length\n':
                if arc_version == '1 0 InternetArchive' and 5 == len(line.split(' ')):
                    self.headers = ['URL', 'IP-address', 'Archive-date', 'Content-type', 'Archive-length']

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
                #raj: some arc files are missing the filedesc:// line
                #raise StandardHeader('missing filedesc')
                self.version = '1'
                self.headers = ['URL', 'IP-address', 'Archive-date', 'Content-type', 'Archive-length']

            #raj: change the call to split below to only split on space (some arcs have a \x0c formfeed character in the url)
            headers = self.get_header_list(line.strip().split(' '))
            content_type, content_length, errors = self.get_content_headers(headers)

            record = ArcRecord(headers = headers, errors=errors)

        ### raj:
        ### We do this because we don't want to read large records into memory,
        ### since this was exhasting memory and crashing for large payloads.
        sha1_digest = None
        if record.url.startswith('http'):
            parsed_http_header = False
            sha1_digest = hashlib.sha1()
        else:
            #This isn't a http response so pretend we already parsed the http header
            parsed_http_header = True

        line = None

        if content_length > 0: ###raj: some arc files have a negative content_length and no payload.
            content=[]
            length = 0

            should_skip_content = False
            if content_length > ArchiveParser.content_length_limit:
                should_skip_content = True

            while length < content_length:
                if not parsed_http_header:
                    line = stream.readline()
                else:
                    bytes_to_read = min(content_length-length, 1024)
                    line = stream.read(bytes_to_read) #TODO: rename variable. may be more than just one line
                if not line:
                    #print 'no more data'
                    break

                if should_skip_content:
                    if not parsed_http_header:
                        content.append(line)
                else:
                    content.append(line)

                length+=len(line)

                if sha1_digest:
                    if parsed_http_header:
                        if length <= content_length:
                            sha1_digest.update(line)
                        else:
                            sha1_digest.update(line[:-(length-content_length)])

                if not parsed_http_header:
                    if nl_rx.match(line):
                        parsed_http_header = True

            if sha1_digest:
                sha1_str = 'sha1:'+base64.b32encode(sha1_digest.digest())
                record.headers.append(('WARC-Payload-Digest', sha1_str))

            content="".join(content)

            ### note the content_length+1 below, which is not in the WARC parser
            ### the +1 might be a bug
            #content, line = content[0:content_length], content[content_length+1:]
            content = content[0:content_length]
            if length > content_length:
                #line is the last line we read
                trailing_chars = line[-(length-content_length):] #note that we removed the +1 from above
            else:
                trailing_chars = ''

            record.content = (content_type, content)

            if trailing_chars:
                record.error('trailing data at end of record', line)
            if  trailing_chars == '':
                self.trailing_newlines = 1

        return (record, (), offset)

    def trim(self, stream):
        return ()

    def get_header_list(self, values):
        num_values = len(values)

        #raj: some headers contain urls with unescaped spaces
        if num_values > 5:
            if re.match('^(?:\d{1,3}\.){3}\d{1,3}$', values[-4]) and re.match('^\d{14}$', values[-3]) and re.match('^\d+$', values[-1]):
                values = ['%20'.join(values[0:-4]), values[-4], values[-3], values[-2], values[-1]]
                num_values = len(values)

        if 4 == num_values:
            #raj: alexa arc files don't always have content-type in header
            return zip(self.short_headers, values)
        elif 5 == num_values:
            #normal case
            #raj: some old alexa arcs have ip-address and date transposed in the header
            if re.match('^\d{14}$', values[1]) and re.match('^(?:\d{1,3}\.){3}\d{1,3}$', values[2]):
                values[1], values[2] = values[2], values[1]

            return zip(self.headers, values)
        elif 6 == num_values:
            #raj: some old alexa arcs have "content-type; charset" in the header
            v = values[0:4]+values[5:]
            v[3] = v[3].rstrip(';')
            return zip(self.headers, v)
        else:
            raise Exception('invalid number of header fields')

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

#raj: some arc files are missing the filedesc:// line
url_record_regex = re.compile('^https?://\S+ (?:\d{1,3}\.){3}\d{1,3} \d{14} \S+ \d+$')
register_record_type(url_record_regex, ArcRecord)
