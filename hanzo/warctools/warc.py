"""An object to represent warc records, using the abstract record in
record.py"""

import re
import hashlib
from hanzo.warctools.record import ArchiveRecord, ArchiveParser
from hanzo.warctools.archive_detect import register_record_type
import uuid

bad_lines = 5 # when to give up looking for the version stamp


@ArchiveRecord.HEADERS(
    DATE=b'WARC-Date',
    TYPE=b'WARC-Type',
    ID=b'WARC-Record-ID',
    CONCURRENT_TO=b'WARC-Concurrent-To',
    REFERS_TO=b'WARC-Refers-To',
    REFERS_TO_TARGET_URI=b'WARC-Refers-To-Target-URI',
    REFERS_TO_DATE=b'WARC-Refers-To-Date',
    CONTENT_LENGTH=b'Content-Length',
    CONTENT_TYPE=b'Content-Type',
    URL=b'WARC-Target-URI',
    BLOCK_DIGEST=b'WARC-Block-Digest',
    PAYLOAD_DIGEST=b'WARC-Payload-Digest',
    IP_ADDRESS=b'WARC-IP-Address',
    FILENAME=b'WARC-Filename',
    WARCINFO_ID=b'WARC-Warcinfo-ID',
    PROFILE=b'WARC-Profile'
)
class WarcRecord(ArchiveRecord):

    # Pylint is very bad at decorators, E1101 is the message that says
    # a member variable does not exist

    # pylint: disable-msg=E1101

    VERSION = b"WARC/1.0"
    VERSION18 = b"WARC/0.18"
    VERSION17 = b"WARC/0.17"
    RESPONSE = b"response"
    RESOURCE = b"resource"
    REQUEST = b"request"
    REVISIT = b"revisit"
    METADATA = b"metadata"
    CONVERSION = b"conversion"
    WARCINFO = b"warcinfo"

    PROFILE_IDENTICAL_PAYLOAD_DIGEST = b"http://netpreserve.org/warc/1.0/revisit/identical-payload-digest"

    TRAILER = b'\r\n\r\n'

    def __init__(self, version=VERSION, headers=None, content=None,
                 errors=None, content_file=None):
        """
        WarcRecord constructor.

        Either content or content_file must be provided, but not both. If
        content, which is a tuple (content_type, content_buffer), is provided,
        when writing the warc record, any Content-Type and Content-Length that
        appear in the supplied headers are ignored, and the values content[0]
        and len(content[1]), respectively, are used. 

        When reading, the caller can stream content_file or use content, which is
        lazily filled using content_file, and after which content_file is
        unavailable.
        """
        ArchiveRecord.__init__(self, headers, content, errors)
        self.version = version
        self.content_file = content_file

    @property
    def id(self):
        return self.get_header(self.ID)

    def _write_to(self, out, nl):
        """WARC Format:
            VERSION NL
            (Key: Value NL)*
            NL
            CONTENT NL
            NL

            don't write multi line headers
        """
        out.write(self.version)
        out.write(nl)
        for k, v in self.headers:
            if self.content_file is not None or k not in (self.CONTENT_TYPE, self.CONTENT_LENGTH):
                out.write(k)
                out.write(b": ")
                out.write(v)
                out.write(nl)

        if self.content_file is not None:
            out.write(nl) # end of header blank nl
            while True:
                buf = self.content_file.read(8192)
                if buf == b'': break
                out.write(buf)
        else:
            # if content tuple is provided, set Content-Type and
            # Content-Length based on the values in the tuple
            content_type, content_buffer = self.content

            if content_type:
                out.write(self.CONTENT_TYPE)
                out.write(b": ")
                out.write(content_type)
                out.write(nl)
            if content_buffer is None:
                content_buffer = b""

            content_length = len(content_buffer)
            out.write(self.CONTENT_LENGTH)
            out.write(b": ")
            out.write(str(content_length).encode('ascii'))
            out.write(nl)

            out.write(nl) # end of header blank nl
            if content_buffer:
                out.write(content_buffer)
     
        # end of record nl nl
        out.write(nl)
        out.write(nl)
        out.flush()

    def repair(self):
        pass

    def validate(self):
        return self.errors

    @classmethod
    def make_parser(self):
        return WarcParser()

    def block_digest(self, content_buffer):
        block_hash = hashlib.sha256()
        block_hash.update(content_buffer)

        digest = "sha256:%s" % block_hash.hexdigest()
        return digest

    @staticmethod
    def warc_uuid(text):
        return "<urn:uuid:{}>".format(uuid.UUID(hashlib.sha1(text).hexdigest()[0:32])).encode('ascii')

    @staticmethod
    def random_warc_uuid():
        return "<urn:uuid:{}>".format(uuid.uuid4()).encode('ascii')


def rx(pat):
    """Helper to compile regexps with IGNORECASE option set."""
    return re.compile(pat, flags=re.IGNORECASE)

version_rx = rx(br'^(?P<prefix>.*?)(?P<version>\s*WARC/(?P<number>.*?))'
                b'(?P<nl>\r\n|\r|\n)\\Z')
# a header is key: <ws> value plus any following lines with leading whitespace
header_rx = rx(br'^(?P<name>.*?):\s?(?P<value>.*?)' b'(?P<nl>\r\n|\r|\n)\\Z')
value_rx = rx(br'^\s+(?P<value>.+?)' b'(?P<nl>\r\n|\r|\n)\\Z')
nl_rx = rx(b'^(?P<nl>\r\n|\r|\n\\Z)')
length_rx = rx(b'^' + WarcRecord.CONTENT_LENGTH + b'$' ) # pylint: disable-msg=E1101
type_rx = rx(b'^' + WarcRecord.CONTENT_TYPE + b'$')     # pylint: disable-msg=E1101

required_headers = set((
        WarcRecord.TYPE.lower(),           # pylint: disable-msg=E1101
        WarcRecord.ID.lower(),             # pylint: disable-msg=E1101
        WarcRecord.CONTENT_LENGTH.lower(), # pylint: disable-msg=E1101
        WarcRecord.DATE.lower(),           # pylint: disable-msg=E1101
        ))


class WarcParser(ArchiveParser):
    KNOWN_VERSIONS = set((b'1.0', b'0.17', b'0.18'))

    def parse(self, stream, offset, line=None):
        """Reads a warc record from the stream, returns a tuple
        (record, errors).  Either records is null or errors is
        null. Any record-specific errors are contained in the record -
        errors is only used when *nothing* could be parsed"""
        # pylint: disable-msg=E1101
        errors = []
        version = None
        # find WARC/.*
        if line is None:
            line = stream.readline()

        while line:
            match = version_rx.match(line)

            if match:
                version = match.group('version')
                if offset is not None:
                    offset += len(match.group('prefix'))
                break
            else:
                if offset is not None:
                    offset += len(line)
                if not nl_rx.match(line):
                    errors.append(('ignored line', line))
                    if len(errors) > bad_lines:
                        errors.append(('too many errors, giving up hope',))
                        return (None, errors, offset)
                line = stream.readline()
        if not line:
            if version:
                errors.append(('warc version but no headers', version))
            return (None, errors, offset)
        if line:
            content_length = 0
            content_type = None

            record = WarcRecord(errors=errors, version=version)

            if match.group('nl') != b'\x0d\x0a':
                record.error('incorrect newline in version', match.group('nl'))

            if match.group('number') not in self.KNOWN_VERSIONS:
                record.error('version field is not known (%s)'
                             % (",".join(self.KNOWN_VERSIONS)),
                             match.group('number'))

            prefix = match.group('prefix')

            if prefix:
                record.error('bad prefix on WARC version header', prefix)

            #Read headers
            line = stream.readline()
            while line and not nl_rx.match(line):

                #print 'header', repr(line)
                match = header_rx.match(line)
                if match:
                    if match.group('nl') != b'\x0d\x0a':
                        record.error('incorrect newline in header',
                                     match.group('nl'))
                    name = match.group('name').strip()
                    value = [match.group('value').strip()]
                    #print 'match',name, value

                    line = stream.readline()
                    match = value_rx.match(line)
                    while match:
                        #print 'follow', repr(line)
                        if match.group('nl') != b'\x0d\x0a':
                            record.error('incorrect newline in follow header',
                                         line, match.group('nl'))
                        value.append(match.group('value').strip())
                        line = stream.readline()
                        match = value_rx.match(line)

                    value = b" ".join(value)

                    record.headers.append((name, value))

                    if type_rx.match(name):
                        if value:
                            content_type = value
                        else:
                            record.error('invalid header', name, value)
                    elif length_rx.match(name):
                        try:
                            #print name, value
                            content_length = int(value)
                            #print content_length
                        except ValueError:
                            record.error('invalid header', name, value)

            # have read blank line following headers

            record.content_file = stream
            record.content_file.bytes_to_eoc = content_length

            # check mandatory headers 
            # WARC-Type WARC-Date WARC-Record-ID Content-Length

            return (record, (), offset)


blank_rx = rx(br'^$')
register_record_type(version_rx, WarcRecord)
register_record_type(blank_rx, WarcRecord)


def make_response(id, date, url, content, request_id):
    # pylint: disable-msg=E1101
    headers = [
            (WarcRecord.TYPE, WarcRecord.RESPONSE),
            (WarcRecord.ID, id),
            (WarcRecord.DATE, date),
            (WarcRecord.URL, url),

    ]
    if request_id:
        headers.append((WarcRecord.CONCURRENT_TO, request_id))

    record = WarcRecord(headers=headers, content=content)

    return record


def make_request(request_id, date, url, content, response_id):
    # pylint: disable-msg=E1101
    headers = [
            (WarcRecord.TYPE, WarcRecord.REQUEST),
            (WarcRecord.ID, request_id),
            (WarcRecord.DATE, date),
            (WarcRecord.URL, url),

    ]
    if response_id:
        headers.append((WarcRecord.CONCURRENT_TO, response_id))

    record = WarcRecord(headers=headers, content=content)

    return record


def make_metadata(meta_id, date, content, concurrent_to=None, url=None):
    # pylint: disable-msg=E1101
    headers = [
            (WarcRecord.TYPE, WarcRecord.METADATA),
            (WarcRecord.ID, meta_id),
            (WarcRecord.DATE, date),

    ]
    if concurrent_to:
        headers.append((WarcRecord.CONCURRENT_TO, concurrent_to))

    if url:
        headers.append((WarcRecord.URL, url))

    record = WarcRecord(headers=headers, content=content)

    return record


def make_conversion(conv_id, date, content, refers_to=None, url=None):
    # pylint: disable-msg=E1101
    headers = [
            (WarcRecord.TYPE, WarcRecord.CONVERSION),
            (WarcRecord.ID, conv_id),
            (WarcRecord.DATE, date),

    ]
    if refers_to:
        headers.append((WarcRecord.REFERS_TO, refers_to))

    if url:
        headers.append((WarcRecord.URL, url))

    record = WarcRecord(headers=headers, content=content)

    return record


def warc_datetime_str(d):
    s = d.isoformat()
    if '.' in s:
        s = s[:s.find('.')]
    return (s + 'Z').encode('utf-8')
