"""A set of stream oriented parsers for http requests and responses, inline
with the current draft recommendations from the http working group.

http://tools.ietf.org/html/draft-ietf-httpbis-p1-messaging-17

Unlike other libraries, this is for clients, servers and proxies.

Missing:
    comma parsing/header folding

"""
from gzip import GzipFile
import re
import zlib
from io import BytesIO


class ParseError(Exception):
    """Baseclass for all http parsing errors"""
    pass

from hanzo.httptools.semantics import Codes, Methods

NEWLINES = (b'\r\n', b'\n')


class BrokenChunks(Exception):
    pass

class HTTPMessage(object):
    """A stream based parser for http like messages"""

    CONTENT_TYPE = b"application/http"

    def __init__(self, header, buf=None, offset=0):
        self.buffer = buf if buf is not None else bytearray()
        self.offset = offset
        self.header = header
        self.body_chunks = []
        self.mode = 'start'
        self.body_reader = None

    @property
    def url(self):
        return self.header.url

    @property
    def scheme(self):
        return self.header.scheme

    @property
    def method(self):
        return self.header.method

    @property
    def host(self):
        return self.header.host

    @property
    def port(self):
        return self.header.port

    def feed_fd(self, fd):
        while True:
            length, terminator = self.feed_predict()
            if length == 0:
                return  ''
            elif terminator == '\r\n':
                text = fd.readLine()
            elif length < 0:
                text = fd.read()
            elif length > 0:
                text = fd.read(length)
            unread = self.feed(text)
            if unread:
                return unread

    def feed_predict(self):
        """returns size, terminator request for input. size is 0 means end. """
        if self.mode == 'start':
            return None, '\r\n'
        elif self.mode == 'headers':
            return None, '\r\n'
        elif self.mode == 'body':
            if self.body_reader is not None:
                return self.body_reader.feed_predict()
            else:
                # connection close
                return -1, None
        if self.mode == 'end':
            return 0, None
        if self.mode == 'incomplete':
            return 0, None

    def feed(self, text):
        """Push more text from the input stream into the parser."""
        if text and self.mode == 'start':
            text = self.feed_start(text)

        if text and self.mode == 'headers':
            text = self.feed_headers(text)
            if self.mode == 'body':
                if not self.header.has_body():
                    self.mode = 'end'
                else:
                    if self.header.body_is_chunked():
                        self.body_reader = ChunkReader()
                    else:
                        length = self.header.body_length()
                        if length is not None:
                            encoding = self.header.encoding

                            if encoding and encoding.endswith(b'gzip'):
                                self.body_reader = ZipLengthReader(length,
                                                                   text)
                            else:
                                self.body_reader = LengthReader(length)
                            length = self.body_reader.remaining
                            self.body_chunks = [(self.offset, length)]
                            if length == 0:
                                self.mode = 'end'
                        else:
                            self.body_chunks = [(self.offset, 0)]
                            self.body_reader = None

        if text and self.mode == 'body':
            if self.body_reader is not None:
                try:
                    text = self.body_reader.feed(self, text)
                except BrokenChunks:
                    self.body_reader = None
                    self.body_chunks = [(self.offset, 0)]
            if self.body_reader is None:
                ((offset, length),) = self.body_chunks
                self.buffer.extend(text)
                self.offset = len(self.buffer)
                self.body_chunks = ((offset, length + len(text)),)
                text = ''

        return text

    def close(self):
        """Mark the end of the input stream and finish parsing."""
        if (self.body_reader is None and self.mode == 'body'):
            self.mode = 'end'

        elif self.mode != 'end':
            if self.body_chunks:
                # check for incomplete in body_chunks
                offset, length = self.body_chunks.pop()
                position = len(self.buffer)
                length = min(length, position - offset)
                self.body_chunks.append((offset, length))
            self.mode = 'incomplete'

    def headers_complete(self):
        """Check whether the input stream has finished supplying headers."""
        return self.mode in ('end', 'body')

    def complete(self):
        """Checks whether the input stream is at the end, i.e. if the parser
        is expecting no more input."""

        return self.mode == 'end'

    def feed_line(self, text):
        """Feed text into the buffer, returning the first line found (if found
        yet)"""
        self.buffer.extend(text)
        pos = self.buffer.find(b'\n', self.offset)
        if pos > -1:
            pos += 1
            text = bytes(self.buffer[pos:])
            del self.buffer[pos:]
            line = bytes(self.buffer[self.offset:])
            self.offset = len(self.buffer)
        else:
            line = None
            text = b''
        return line, text

    def feed_length(self, text, remaining):
        """Feed (at most remaining bytes) text to buffer, returning
        leftovers."""
        body, text = text[:remaining], text[remaining:]
        remaining -= len(body)
        self.buffer.extend(body)
        self.offset = len(self.buffer)
        return remaining, text

    def feed_start(self, text):
        """Feed text to the parser while it is in the 'start' state."""
        line, text = self.feed_line(text)
        if line is not None:
            if line not in NEWLINES:
                self.header.set_start_line(line)
                self.mode = 'headers'

        return text

    def feed_headers(self, text):
        """Feed text to the parser while it is in the 'headers'
        state."""
        while text:
            line, text = self.feed_line(text)
            if line is not None:
                self.header.add_header_line(line)
                if line in NEWLINES:
                    self.mode = 'body'
                    break

        return text

    def get_message(self):
        """Returns the contents of the input buffer."""
        return bytes(self.buffer)

    def get_decoded_message(self):
        """Return the input stream reconstructed from the parsed
        data."""
        buf = bytearray()
        self.write_decoded_message(buf)
        return bytes(buf)

    def write_message(self, buf):
        #TODO: No idea what this does, looks broken
        self.header.write(buf)
        buf.extend(b'\r\n')
        self.write_body(buf)

    def write_decoded_message(self, buf):
        """Writes the parsed data to the buffer passed."""
        self.header.write_decoded(buf)
        if self.header.has_body():
            length = sum(l for o, l in self.body_chunks)
            buf.extend(b'Content-Length: ' + str(length).encode('ascii') + b'\r\n')
        body = self.get_body()
        if self.header.encoding and body:
            try:
                body = zlib.decompress(body)
            except zlib.error:
                try:
                    body = zlib.decompress(body, 16 + zlib.MAX_WBITS)
                except zlib.error:
                    encoding_header = b"Content-Encoding: " + self.header.encoding + b"\r\n"
                    buf.extend(encoding_header)
        buf.extend(b'\r\n')
        try:
            buf.extend(body)
        except Exception as e:
            raise Exception('buf={} body={} e={}'.format(repr(buf), repr(body), e))

    def get_body(self):
        """Returns the body of the HTTP message."""
        buf = bytearray()
        self.write_body(buf)
        return bytes(buf)

    def write_body(self, buf):
        """Writes the body of the HTTP message to the passed
        buffer."""
        for offset, length in self.body_chunks:
            buf.extend(self.buffer[offset:offset + length])


class ChunkReader(object):
    """Reads the body of a HTTP message with chunked encoding."""

    def __init__(self):
        self.mode = "start"
        self.start = True
        self.remaining = 0

    def feed_predict(self):
        if self.mode == 'start':
            return None, '\r\n'
        elif self.mode == 'chunk':
            if self.remaining == 0:
                return None, '\r\n'
            else:
                return self.remaining, None
        elif self.mode == 'trailer':
            return None, '\r\n'
        elif self.mode == 'end':
            return 0, None

    def feed_start(self, parser, text):
        """Feed text into the ChunkReader when the mode is 'start'."""
        pos = len(parser.buffer)
        line, text = parser.feed_line(text)
        offset = len(parser.buffer)

        if line is not None:
            try:
                chunk = int(line.split(b';', 1)[0], 16)
            except ValueError:
                # ugh, this means the chunk is probably not a chunk
                if self.start:
                    # undo, stip text from buffer
                    del parser.buffer[pos:]
                    parser.offset = len(parser.buffer)
                    raise BrokenChunks()
                else:
                    raise

            parser.body_chunks.append((offset, chunk))
            self.remaining = chunk
            if chunk == 0:
                self.mode = 'trailer'
            else:
                self.mode = 'chunk'
        self.start = False
        return text

    def feed_chunk(self, parser, text):
        """Feed text into the ChunkReader when the mode is 'chunk'."""
        if self.remaining > 0:
            self.remaining, text = parser.feed_length(text, self.remaining)
        if self.remaining == 0:
            end_of_chunk, text = parser.feed_line(text)
            if end_of_chunk:
                self.mode = 'start'

        return text

    def feed_trailer(self, parser, text):
        """Feed text into the ChunkReader when the mode is
        'trailer'."""
        line, text = parser.feed_line(text)
        if line is not None:
            parser.header.add_trailer_line(line)
            if line in NEWLINES:
                self.mode = 'end'

        return text

    def feed(self, parser, text):
        """Feed text into the ChunkReader."""
        while text:
            if self.mode == 'start':
                text = self.feed_start(parser, text)

            if text and self.mode == 'chunk':
                text = self.feed_chunk(parser, text)

            if text and self.mode == 'trailer':
                text = self.feed_trailer(parser, text)

            if self.mode == 'end':
                parser.mode = 'end'
                break

        return text


class LengthReader(object):

    def __init__(self, length):
        self.remaining = length

    def feed_predict(self):
        return self.remaining, None

    def feed(self, parser, text):
        if self.remaining > 0:
            self.remaining, text = parser.feed_length(text, self.remaining)
        if self.remaining <= 0:
            parser.mode = 'end'
        return text


class ZipLengthReader(LengthReader):
    """
    Tries to read the body as gzip according to length. In case that fails, it
    disregards the Content-Length and reads it normally.
    """
    def __init__(self, length, text):
        # TODO test if this works with gzipped responses in WARC
        try:
            self._file = GzipFile(fileobj=BytesIO(text[:length]), mode='rb')
            self._text = self._file.read()
            super(ZipLengthReader, self).__init__(len(self._text))
        except IOError:
            self._file = None
            super(ZipLengthReader, self).__init__(len(text))

    def __del__(self):
        if self._file:
            self._file.close()

    def feed(self, parser, text):
        """Parse the body according to remaining length"""
        if self.remaining > 0:
            if self._file:
                text = self._text
            self.remaining, text = parser.feed_length(text, self.remaining)
        if self.remaining <= 0:
            parser.mode = 'end'
        return text


class HTTPHeader(object):
    STRIP_HEADERS = [n.lower() for n in (b'Content-Length',
                     b'Transfer-Encoding', b'Content-Encoding',
                     b'TE', b'Expect', b'Trailer')]

    def __init__(self, ignore_headers):
        self.headers = []
        self.keep_alive = False
        self.mode = 'close'
        self.content_length = None
        self.encoding = None
        self.trailers = []
        self.expect_continue = False
        self.ignore_headers = set(x.lower() for x in ignore_headers)

    def has_body(self):
        pass

    def set_start_line(self, line):
        pass

    def write_decoded(self, buf):
        self.write_decoded_start(buf)
        strip_headers = self.STRIP_HEADERS if self.has_body() else ()
        self.write_headers(buf, strip_headers)

    def write_decoded_start(self, buf):
        pass

    def write_headers(self, buf, strip_headers=()):
        for k, v in self.headers:
            if k.lower() not in strip_headers:
                buf.extend(k + b': ' + v + b'\r\n')
        for k, v in self.trailers:
            if k.lower() not in strip_headers:
                buf.extend(k + b': ' + v + b'\r\n')

    def add_trailer_line(self, line):
        if line.startswith(b' ') or line.startswith(b'\t'):
            k, v = self.trailers.pop()
            line = line.strip()
            v = v + b' ' + line
            self.trailers.append((k, v))
        elif line in NEWLINES:
            pass
        else:
            name, value = line.split(b':', 1)
            name = name.strip()
            value = value.strip()
            self.trailers.append((name, value))

    def add_header(self, name, value):
        self.headers.append((name, value))

    def add_header_line(self, line):
        if line.startswith(b' ') or line.startswith(b'\t'):
            k, v = self.headers.pop()
            line = line.strip()
            v = v + b' ' + line
            self.add_header(k, v)

        elif line in NEWLINES:
            for name, value in self.headers:
                name = name.lower()
                value = value.lower()

                # todo handle multiple instances
                # of these headers
                if name in self.ignore_headers:
                    #print >> sys.stderr, 'ignore', name
                    pass
                elif name == b'expect':
                    if b'100-continue' in value:
                        self.expect_continue = True
                elif name == b'content-length':
                    if self.mode == 'close':
                        self.content_length = int(value)
                        self.mode = 'length'

                elif name == b'transfer-encoding':
                    if b'chunked' in value:
                        self.mode = 'chunked'

                elif name == b'content-encoding':
                    self.encoding = value

                elif name == b'connection':
                    if b'keep-alive' in value:
                        self.keep_alive = True
                    elif b'close' in value:
                        self.keep_alive = False

        else:
            name, value = line.split(b':', 1)
            name = name.strip()
            value = value.strip()
            self.add_header(name, value)

    def body_is_chunked(self):
        return self.mode == 'chunked'

    def body_length(self):
        if self.mode == 'length':
            return self.content_length

url_rx = re.compile(
    b'(?P<scheme>https?)://(?P<authority>(?P<host>[^:/]+)(?::(?P<port>\\d+))?)'
    b'(?P<path>.*)',
    re.I)


class RequestHeader(HTTPHeader):

    def __init__(self, ignore_headers=()):
        HTTPHeader.__init__(self, ignore_headers=ignore_headers)
        self.method = ''
        self.target_uri = ''
        self.version = ''
        self.host = ''
        self.scheme = 'http'
        self.port = 80
        self.host = ''
    
    @property
    def url(self):
        if (self.scheme == 'http' and self.port == 80)\
        or (self.scheme == 'https' and self.port == 80):
            return "%s://%s%s"%(self.scheme, self.host, self.target_uri)
        else:
            return "%s://%s:%s%s"%(self.scheme, self.host, self.port,  self.target_uri)


    def add_header(self, name, value):

        if name.lower() == b'host':
            if b':' in value:
                self.host, self.port = value.split(b':',1)
            else:
                self.host = value

        return HTTPHeader.add_header(self, name, value)

    def set_start_line(self, line):
        self.method, self.target_uri, self.version = \
            line.rstrip().split(b' ', 2)

        if self.method.upper() == b"CONNECT":
            # target_uri = host:port
            self.host, self.port = self.target_uri.split(b':')
        else:
            match = url_rx.match(self.target_uri)
            if match:
                #self.add_header('Host', match.group('authority'))
                self.target_uri = match.group('path')
                self.host = match.group('host')
                port = match.group('port')
                self.port = int(port) if port else 80

                self.scheme = match.group('scheme')
                if not self.target_uri:
                    if self.method.upper() == 'OPTIONS':
                        self.target_uri = '*'
                    else:
                        self.target_uri = '/'

        if self.version == 'HTTP/1.0':
            self.keep_alive = False

    def has_body(self):
        return self.mode in ('chunked', 'length')

    def write_decoded_start(self, buf):
        buf.extend(self.method + b' ' + self.target_uri + b' ' + self.version + b'\r\n')


class ResponseHeader(HTTPHeader):

    def __init__(self, request=None, ignore_headers=()):
        HTTPHeader.__init__(self, ignore_headers=ignore_headers)
        self.request = request
        self.version = b"HTTP/1.1"
        self.code = 0
        self.phrase = "Empty Response"

    @property
    def method(self):
        return self.request.method

    @property
    def url(self):
        return self.request.url

    @property
    def host(self):
        return self.request.host

    @property
    def port(self):
        return self.request.port

    @property
    def scheme(self):
        return self.request.scheme

    def set_start_line(self, line):
        parts = line.rstrip().split(b' ', 2)
        self.version, self.code = parts[:2]
        self.phrase = parts[2] if len(parts) >= 3 else b""

        self.code = int(self.code)
        if self.version == b'HTTP/1.0':
            self.keep_alive = False

    def has_body(self):
        if self.request and self.request.method in Methods.no_body:
            return False
        elif self.code in Codes.no_body:
            return False

        return True

    def write_decoded_start(self, buf):
        buf.extend(self.version + b' ' + str(self.code).encode('ascii') + b' ' + self.phrase + b'\r\n')


class RequestMessage(HTTPMessage):
    CONTENT_TYPE = HTTPMessage.CONTENT_TYPE + b";msgtype=request"

    def __init__(self, ignore_headers=()):
        HTTPMessage.__init__(self,
                             RequestHeader(ignore_headers=ignore_headers))


class ResponseMessage(HTTPMessage):
    CONTENT_TYPE = HTTPMessage.CONTENT_TYPE + b";msgtype=response"

    def __init__(self, request, ignore_headers=()):
        self.interim = []
        HTTPMessage.__init__(self,
                             ResponseHeader(request.header,
                                            ignore_headers=ignore_headers))

    def got_continue(self):
        return bool(self.interim)

    @property
    def code(self):
        return self.header.code

    def feed(self, text):
        text = HTTPMessage.feed(self, text)
        if self.complete() and self.header.code == Codes.Continue:
            self.interim.append(self.header)
            self.header = ResponseHeader(self.header.request)
            self.body_chunks = []
            self.mode = 'start'
            self.body_reader = None
            text = HTTPMessage.feed(self, text)
        return text

    def as_http09(self):
        return HTTP09Response(self)

class HTTP09ResponseHeader(HTTPHeader):
    def __init__(self, request=None, ignore_headers=()):
        HTTPHeader.__init__(self, ignore_headers=ignore_headers)
        self.request = request
        self.version = "HTTP/0.9"
        self.code = 200
        self.phrase = ""

    @property
    def method(self):
        return self.request.method

    @property
    def url(self):
        return self.request.url

    @property
    def host(self):
        return self.request.host

    @property
    def port(self):
        return self.request.port

    @property
    def scheme(self):
        return self.request.scheme

    def has_body(self):
        return True

class HTTP09Response(HTTPMessage):
    CONTENT_TYPE = "%s;msgtype=response;version=0.9" % HTTPMessage.CONTENT_TYPE
    def __init__(self, response):
        header= HTTP09ResponseHeader(response.header.request)
        HTTPMessage.__init__(self, header, buf=response.buffer, offset=response.offset)
        self.mode = 'body'

    @property
    def code(self):
        return self.header.code

    def feed_predict(self):
        """returns size, terminator request for input. size is 0 means end. """
        return -1, None

    def feed(self, text):
        """Push more text from the input stream into the parser."""
        self.buffer.extend(text)
        return ''

    def close(self):
        """Mark the end of the input stream and finish parsing."""
        self.mode = 'end'

    def get_message(self):
        """Returns the contents of the input buffer."""
        return bytes(self.buffer)

    def get_decoded_message(self):
        """Return the input stream reconstructed from the parsed
        data."""
        return bytes(self.buffer)

    def write_decoded_message(self, buf):
        """Writes the parsed data to the buffer passed."""
        buf.extend(self.buffer)

    def get_body(self):
        """Returns the body of the HTTP message."""
        return bytes(self.buffer)

    def write_body(self, buf):
        buf.extend(self.buffer)


