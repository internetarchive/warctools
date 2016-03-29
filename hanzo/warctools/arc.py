"""An object to represent arc records
http://archive.org/web/researcher/ArcFileFormat.php
"""

import re

from hanzo.warctools.record import ArchiveRecord, ArchiveParser
from hanzo.warctools.archive_detect import register_record_type

# URL<sp>IP-address<sp>Archive-date<sp>Content-type<sp>
#Result-code<sp>Checksum<sp>Location<sp> Offset<sp>Filename<sp>
#Archive-length<nl> 
# 
@ArchiveRecord.HEADERS(
    URL = b'URL',
    IP = b'IP-address',
    DATE = b'Archive-date',
    CONTENT_TYPE = b'Content-type',
    CONTENT_LENGTH = b'Archive-length',
    RESULT_CODE = b'Result-code',
    CHECKSUM = b'Checksum',
    LOCATION = b'Location',
    OFFSET = b'Offset',
    FILENAME = b'Filename',
)
class ArcRecord(ArchiveRecord):

    TRAILER = b'\n'  # an ARC record is trailed by single unix newline

    """Represents a record in an arc file."""
    def __init__(self, headers=None, content=None, errors=None):
        ArchiveRecord.__init__(self, headers, content, errors) 

    @property
    def type(self):
        return b"response"

    def _write_to(self, out, nl):
        #TODO: empty method?
        pass

    @classmethod
    def make_parser(cls):
        """Constructs a parser for arc records."""
        return ArcParser()

class ArcRecordHeader(ArcRecord):
    """Represents the headers in an arc record."""
    def __init__(self, headers=None, content=None, errors=None, version=None,
                 raw_headers=None):
        ArcRecord.__init__(self, headers, content, errors) 
        self.version = version
        self.raw_headers = raw_headers

    @property
    def type(self):
        return b"filedesc"

    def raw(self):
        """Return the raw representation of this record."""
        return b"".join(self.raw_headers) + self.content[1]

def rx(pat):
    """Helper function to compile a regular expression with the IGNORECASE
    flag."""
    return re.compile(pat, flags=re.IGNORECASE)

nl_rx = rx('^\r\n|\r|\n$')
length_rx = rx(b'^' + ArcRecord.CONTENT_LENGTH + b'$') #pylint: disable-msg=E1101
type_rx = rx(b'^' + ArcRecord.CONTENT_TYPE + b'$')     #pylint: disable-msg=E1101
#raj/noah: change the call to split below to only split on space (some arcs
#have a \x0c formfeed character in the url)
# SPLIT = re.compile(br'\b\s|\s\b').split
SPLIT = re.compile(br'\b | \b').split

class ArcParser(ArchiveParser):
    """A parser for arc archives."""


    def __init__(self):
        self.version = 0
        # we don't know which version to parse initially - a v1 or v2 file so
        # we read the filedesc because the order and number of the headers
        # change between versions.

        # question? will we get arc fragments?
        # should we store both headers & detect records by header length?
        # if we don't know 

        self.headers = []

    def parse(self, stream, offset, line=None):
        """Parses a stream as an arc archive and returns an Arc record along
        with the offset in the stream of the end of the record."""
        record = None
        content_type = None
        content_length = None
        if line is None:
            line = stream.readline()

        while not line.rstrip():
            if not line:
                return (None, (), offset)
            line = stream.readline()

        if line.startswith(b'filedesc:'):
            raw_headers = []
            raw_headers.append(line)
            # read headers named in body of record
            # to assign names to header, to read body of record
            arc_version_line = stream.readline()
            raw_headers.append(arc_version_line)
            arc_names_line = stream.readline()
            raw_headers.append(arc_names_line)

            arc_version = arc_version_line.strip()

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
            arc_headers = self.parse_header_list(line)

            # extract content, ignoring header lines parsed already
            content_type, content_length, errors = \
                self.get_content_headers(arc_headers)

            content_length = content_length \
                - len(arc_version_line) \
                - len(arc_names_line)

            record = ArcRecordHeader(headers=arc_headers,
                                     version=arc_version,
                                     errors=errors,
                                     raw_headers=raw_headers)
        else:
            if not self.headers:
                #raj: some arc files are missing the filedesc:// line
                #raise Exception('missing filedesc')
                self.version = '1'
                self.headers = ['URL', 'IP-address', 'Archive-date', 'Content-type', 'Archive-length']

            headers = self.parse_header_list(line)
            content_type, content_length, errors = \
                self.get_content_headers(headers)

            record = ArcRecord(headers = headers, errors=errors)

        line = None

        record.content_file = stream
        record.content_file.bytes_to_eoc = content_length

        return (record, (), offset)

    def trim(self, stream):
        return ()

    def parse_header_list(self, line):
        values = line.strip().split(b' ')
        num_values = len(values)

        #raj: some headers contain urls with unescaped spaces
        if num_values > 5:
            if re.match(b'^(?:\d{1,3}\.){3}\d{1,3}$', values[-4]) and re.match('^\d{14}$', values[-3]) and re.match('^\d+$', values[-1]):
                values = [b'%20'.join(values[0:-4]), values[-4], values[-3], values[-2], values[-1]]
                num_values = len(values)

        if 4 == num_values:
            #raj: alexa arc files don't always have content-type in header
            return list(zip(self.short_headers, values))
        elif 5 == num_values:
            #normal case
            #raj: some old alexa arcs have ip-address and date transposed in the header
            if re.match(b'^\d{14}$', values[1]) and re.match(b'^(?:\d{1,3}\.){3}\d{1,3}$', values[2]):
                values[1], values[2] = values[2], values[1]

            return list(zip(self.headers, values))
        elif 6 == num_values:
            #raj: some old alexa arcs have "content-type; charset" in the header
            v = values[0:4]+values[5:]
            v[3] = v[3].rstrip(';')
            return list(zip(self.headers, v))
        else:
            raise Exception('invalid number of header fields')

    @staticmethod
    def get_content_headers(headers):
        content_type = None
        content_length = None
        errors = []

        for name, value in headers:
            if type_rx.match(name):
                if value:
                    content_type = value
                else:
                    errors.append(('invalid header', name, value))
            elif length_rx.match(name):
                try:
                    content_length = int(value)
                except ValueError:
                    errors.append(('invalid header', name, value))

        return content_type, content_length, errors


register_record_type(re.compile(br'^filedesc://'), ArcRecord)

#raj: some arc files are missing the filedesc:// line
url_record_regex = re.compile('^https?://\S+ (?:\d{1,3}\.){3}\d{1,3} \d{14} \S+ \d+$')
register_record_type(url_record_regex, ArcRecord)

