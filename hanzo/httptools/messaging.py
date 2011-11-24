""" A set of stream oriented parsers for http requests and responses, 
inline with the current draft recommendations from the http working group.

http://tools.ietf.org/html/draft-ietf-httpbis-p1-messaging-17

Unlike other libraries, this is for clients, servers and proxies.
"""
import re

from StringIO import StringIO
class ParseError(StandardError):
    pass

from .semantics import Codes, Methods

CRLF = '\r\n'

"""
Missing:
    comma parsing/header folding

"""

class HTTPMessage(object):
    CONTENT_TYPE="application/http"
    """A stream based parser for http like messages"""
    def __init__(self, buffer, header):
        self.buffer = buffer
        self.offset = self.buffer.tell()
        self.header = header
        
        self.body_chunks = []
        self.mode = 'start'
        self.body_reader = None


    def feed_fd(self, fd):
        return self.feed(fd.read())

    def feed(self, text):
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
                        if length >= 0:
                            self.body_reader = LengthReader(length)
                            self.body_chunks = [(self.offset, length)]
                            if length == 0:
                                self.mode = 'end'
                        else:
                            self.body_chunks = [(self.offset, 0)]
                            self.body_reader = None

        if text and self.mode == 'body':
            if self.body_reader is not None:
                 text = self.body_reader.feed(self, text)
            else:
                ( (offset, length), ) = self.body_chunks
                self.buffer.write(text)
                self.body_chunks = ( (offset, length+len(text)), )
                text = ''

        return text

    def close(self):
        if self.mode =='start' or (self.body_reader is None and self.mode == 'body'):
            self.mode = 'end'
        
        elif self.mode != 'end':
            if self.body_chunks:
                # check for incomplete in body_chunks
                offset, length = self.body_chunks.pop()
                position = self.buffer.tell()
                length = min(length, position-offset)
                self.body_chunks.append((offset, length))
            self.mode = 'incomplete'


    def headers_complete(self):
        return self.mode in ('end', 'body')

    def complete(self):
        return self.mode =='end'

    def feed_line(self, text):
        """ feed text into the buffer, returning the first line found (if found yet)"""
        #print 'feed line', repr(text)
        line = None
        nl= text.find(CRLF)
        if nl > -1:
            nl+=2
            self.buffer.write(text[:nl])
            self.buffer.seek(self.offset)
            line = self.buffer.readline()
            self.offset = self.buffer.tell()
            text = text[nl:]
        else:
            self.buffer.write(text)
            text = ''
        #print 'feed line', repr(line), repr(text)
        return line, text

    def feed_length(self, text, remaining):
        """ feed (at most remaining bytes) text to buffer, returning leftovers """
        body, text = text[:remaining], text[remaining:]
        remaining -= len(body)
        self.buffer.write(body)
        self.offset = self.buffer.tell()
        return remaining, text

    def feed_start(self, text):
        line, text = self.feed_line(text)
        if line is not None:
            if line != CRLF: # skip leading newlines
                self.header.set_start_line(line)
                self.mode = 'headers'

        return text
                
    def feed_headers(self, text):
        while text:
            line, text = self.feed_line(text)
            if line is not None:
                self.header.add_header_line(line)
                if line == CRLF:
                    self.mode = 'body'
                    break

        return text

    def get_decoded_message(self):
        buf = StringIO()
        self.write_decoded_message(buf)
        return buf.getvalue()

    def write_decoded_message(self, buf):
        self.header.write_decoded(buf)
        if self.header.has_body():
            length = sum(l for o,l in self.body_chunks)
            buf.write('Content-Length: %d\r\n'%length)
        body = self.get_body()
        if self.header.encoding and body:
            try: 
                data=zlib.decompress(data)
            except zlib.error:
                 try:
                     data=zlib.decompress(data,16+zlib.MAX_WBITS)
                 except zlib.error:
                    buf.write('Content-Encoding: %s\r\n'%self.header.encoding)
        buf.write('\r\n')
        buf.write(body)


    def get_body(self):
        body = StringIO()
        self.write_body(body)
        return body.getvalue()

    def write_body(self, body):
        current = self.buffer.tell()
        for offset, length in self.body_chunks:
            self.buffer.seek(offset)
            body.write(self.buffer.read(length))
        self.buffer.seek(current)

            
class ChunkReader(object):
    def __init__(self):
        self.mode = "start"
        self.remaining = 0

    def feed(self, parser, text):
        while text:
            if self.mode == 'start':
                #print self.mode, repr(text)
                
                line, text = parser.feed_line(text)
                offset = parser.buffer.tell()

                if line is not None:
                    chunk = int(line.split(';',1)[0], 16)
                    parser.body_chunks.append((offset, chunk))
                    self.remaining = chunk
                    if chunk == 0:
                        self.mode = 'trailer'
                    else:
                        self.mode = 'chunk'
                #print self.mode, repr(text)

            if text and self.mode == 'chunk':
                #print self.mode, repr(text), self.remaining
                if self.remaining > 0: 
                    self.remaining, text = parser.feed_length(text, self.remaining)
                if self.remaining == 0:
                    end_of_chunk, text = parser.feed_line(text)
                    #print 'end',end_of_chunk
                    if end_of_chunk:
                        #print 'ended'
                        self.mode = 'start'
                #print self.mode, repr(text)

            if text and self.mode == 'trailer':
                line, text = parser.feed_line(text)
                if line is not None:
                    parser.header.add_trailer_line(line)
                    if line == CRLF:
                        self.mode = 'end'

            if self.mode == 'end':
                parser.mode ='end'
                break

        return text

class LengthReader(object):
    def __init__(self, length):
        self.remaining = length

    def feed(self, parser, text):
        if self.remaining > 0: 
            self.remaining, text = parser.feed_length(text, self.remaining)
        if self.remaining <= 0:
            parser.mode ='end'
        return text
            

class HTTPHeader(object):
    STRIP_HEADERS = ('Content-Length', 'Transfer-Encoding', 'Content-Encoding', 'TE', 'Expect', 'Trailer')
    def __init__(self):
        self.headers = []
        self.keep_alive = False
        self.mode = 'close'
        self.content_length = None
        self.encoding = None
        self.trailers = []
        self.expect_continue=False

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
        for k,v in self.headers:
            if k not in strip_headers:
                buf.write('%s: %s\r\n'%(k,v))
        for k,v in self.trailers:
            if k not in strip_headers:
                buf.write('%s: %s\r\n'%(k,v))

    def add_trailer_line(self, line):
        if line.startswith(' ') or line.startswith('\t'):
            k,v = self.trailers.pop()
            line = line.strip()
            v = "%s %s"%(v, line)
            self.trailers.append((k,v))
        elif line == '\r\n':
            pass
        else:
            name, value = line.split(':',1)
            name = name.strip()
            value = value.strip()
            self.trailers.append((name, value))

    def add_header(self, name, value):
        self.headers.append((name, value))
        
    def add_header_line(self, line):
        if line.startswith(' ') or line.startswith('\t'):
            k,v = self.headers.pop()
            line = line.strip()
            v = "%s %s"%(v, line)
            self.add_header(k, v)
        
        elif line == '\r\n':
            for name, value in self.headers:
                name = name.lower()
                value = value.lower()

                
                # todo handle multiple instances
                # of these headers
                if name == 'expect':
                    if '100-continue' in value:
                        self.expect_continue = True
                if name == 'content-length':
                    if self.mode == 'close':
                        self.content_length = int(value)
                        self.mode = 'length'
                        
                if name == 'transfer-encoding':
                    if 'chunked' in value:
                        self.mode = 'chunked'

                if name == 'content-encoding':
                    self.encoding = value

                if name == 'connection':
                    if 'keep-alive' in value:
                        self.keep_alive = True
                    elif 'close' in value:
                        self.keep_alive = False

        else:
            #print line
            name, value = line.split(':',1)
            name = name.strip()
            value = value.strip()
            self.add_header(name, value)

    
    def body_is_chunked(self):
        return self.mode == 'chunked'

    def body_length(self):
        if self.mode == 'length':
            return self.content_length


url_rx = re.compile('https?://(?P<authority>(?P<host>[^:/]+)(?::(?P<port>\d+)))?(?P<path>.*)', re.I)
class RequestHeader(HTTPHeader):
    def __init__(self):
        HTTPHeader.__init__(self)
        self.method = ''
        self.target_uri = ''
        self.version = ''
        self.host = ''

    def set_start_line(self, line):
        self.method, self.target_uri, self.version = line.rstrip().split(' ',2)
        if self.method.upper() == "CONNECT":
            # target_uri = host:port
            pass
        else:
            match = url_rx.match(self.target_uri)
            if match:
                self.add_header('Host', match.group('authority'))
                self.target_uri = match.group('path')
                if not path:
                    if self.method.upper() == 'OPTIONS':
                        self.target_uri = '*'
                    else:
                        self.target_uri = '/'

            
        if self.version =='HTTP/1.0':
            self.keep_alive = False


    def has_body(self):
        return self.mode in ('chunked', 'length')

    def write_decoded_start(self, buf):
        buf.write('%s %s %s\r\n'%(self.method, self.target_uri, self.version))

class ResponseHeader(HTTPHeader):
    def __init__(self, request):
        HTTPHeader.__init__(self)
        self.request = request
        self.version = None
        self.code = None
        self.phrase = None

    def set_start_line(self, line):
        self.version, self.code, self.phrase = line.rstrip().split(' ',2)
        self.code = int(self.code)
        if self.version =='HTTP/1.0':
            self.keep_alive = False

    def has_body(self):
        if self.request.method in Methods.no_body:
            return False
        elif self.code in Codes.no_body:
            return False

        return True

    def write_decoded_start(self, buf):
        buf.write('%s %d %s\r\n'%(self.version, self.code, self.phrase))


class RequestMessage(HTTPMessage):
    CONTENT_TYPE="%s;msgtype=request"%HTTPMessage.CONTENT_TYPE
    def __init__(self, buffer):
        HTTPMessage.__init__(self, buffer, RequestHeader())

class ResponseMessage(HTTPMessage):
    CONTENT_TYPE="%s;msgtype=response"%HTTPMessage.CONTENT_TYPE
    def __init__(self, buffer, request_header):
        self.interim = []
        HTTPMessage.__init__(self, buffer, ResponseHeader(request_header))

    def got_continue(self):
        return bool(self.interim)

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
            
