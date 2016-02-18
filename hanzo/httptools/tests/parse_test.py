"""Tests for http parsing."""
import unittest

# want unittest2 for python2.6
try:
    unittest.TestCase.assertIsNone
except AttributeError:
    import unittest2
    unittest = unittest2

from hanzo.httptools.messaging import \
    RequestMessage, \
    ResponseMessage

get_request_lines = [
        b"GET / HTTP/1.1",
        b"Host: example.org",
        b"",
        b"",
        ]
get_request = b"\r\n".join(get_request_lines)
get_response_lines = [
        b"HTTP/1.1 200 OK",
        b"Host: example.org",
        b"Content-Length: 5",
        b"",
        b"tests",
        ]
get_response = b"\r\n".join(get_response_lines)


class GetChar(unittest.TestCase):
    """Test basic GET request parsing. Single byte at a time."""

    def runTest(self):
        """Attempts to parse the contents of get_request and
        get_response."""
        p = RequestMessage()
        for t in get_request:
            if isinstance(t, int): t = bytes([t]) # python3
            text = p.feed(t)
            self.assertEqual(text, b'')

        self.assertTrue(p.headers_complete())
        self.assertTrue(p.complete())

        self.assertEqual(get_request, p.get_decoded_message())

        p = ResponseMessage(p)
        for char in get_response:
            if isinstance(char, int): char = bytes([char]) # python3
            text = p.feed(char)
            self.assertEqual(text, b'')

        self.assertTrue(p.headers_complete())
        self.assertTrue(p.complete())
        self.assertEqual(get_response, p.get_decoded_message())
        self.assertEqual(b"tests", p.get_body())


class GetLines(unittest.TestCase):
    """Test basic GET request parsing. Single line at a time."""

    def runTest(self):
        """Attempts to parse get_request_lines, i.e. get_request line
        at a time."""

        p = RequestMessage()
        for line in get_request_lines[:-1]:
            text = p.feed(line)
            self.assertEqual(text, b"")
            text = p.feed(b"\r\n")
            self.assertEqual(text, b"")
        text = p.feed(get_request_lines[-1])
        self.assertEqual(text, b"")

        self.assertTrue(p.headers_complete())
        self.assertTrue(p.complete())

        self.assertEqual(get_request, p.get_decoded_message())

        p = ResponseMessage(p)
        for line in get_response_lines[:-1]:
            text = p.feed(line)
            self.assertEqual(text, b"")
            text = p.feed(b"\r\n")
            self.assertEqual(text, b"")
        text = p.feed(get_response_lines[-1])

        self.assertEqual(text, b"")

        self.assertTrue(p.headers_complete())
        self.assertTrue(p.complete())

        self.assertEqual(get_response, p.get_decoded_message())

        self.assertEqual(p.code, 200)
        self.assertEqual(p.header.version, b"HTTP/1.1")
        self.assertEqual(p.header.phrase, b"OK")


head_request = b"\r\n".join([
    b"HEAD / HTTP/1.1",
    b"Host: example.org",
    b"",
    b"",
])
head_response = b"\r\n".join([
    b"HTTP/1.1 200 OK",
    b"Host: example.org",
    b"Content-Length: 5",
    b"",
    b"",
])


class HeadTest(unittest.TestCase):
    """Tests parsing of HEAD requests and responses."""

    def runTest(self):
        """Constructs a RequestMessage and ResponseMessage and uses them to
        parse HEAD messages."""
        p = RequestMessage()
        text = p.feed(head_request)

        self.assertEqual(text, b'')
        self.assertTrue(p.complete())
        self.assertEqual(head_request, p.get_decoded_message())

        p = ResponseMessage(p)
        text = p.feed(head_response)

        self.assertEqual(text, b'')
        self.assertTrue(p.complete())
        self.assertEqual(head_response, p.get_decoded_message())
        self.assertEqual(p.code, 200)
        self.assertEqual(p.header.version, b"HTTP/1.1")
        self.assertEqual(p.header.phrase, b"OK")


class PostTestChunked(unittest.TestCase):
    """Tests the parser with a POST request with chunked encoding."""
    post_request = b"\r\n".join([
            b"POST / HTTP/1.1",
            b"Host: example.org",
            b"Transfer-Encoding: chunked",
            b"",
            b"8",
            b"abcdefgh",
            b"0",
            b"",
            b"",
            ])
    post_response = b"\r\n".join([
            b"HTTP/1.1 100 Continue",
            b"Host: example.org",
            b"",
            b"HTTP/1.0 204 No Content",
            b"Date: now!",
            b"",
            b"",
            ])

    def runTest(self):
        """Tests parsing of POST requests and responses."""
        p = RequestMessage()
        text = p.feed(self.post_request)

        self.assertEqual(text, b'')
        self.assertTrue(p.complete())

        p = ResponseMessage(p)
        text = p.feed(self.post_response)

        self.assertEqual(text, b'')
        self.assertTrue(p.complete())
        self.assertEqual(p.code, 204)
        self.assertEqual(p.header.version, b"HTTP/1.0")
        self.assertEqual(p.header.phrase, b"No Content")


class PostTestChunkedEmpty(unittest.TestCase):
    """Tests the parser with a POST request with chunked encoding and
    an empty body."""
    post_request = b"\r\n".join([
            b"POST / HTTP/1.1",
            b"Host: example.org",
            b"Transfer-Encoding: chunked",
            b"",
            b"0",
            b"",
            b"",
            ])
    post_response = b"\r\n".join([
            b"HTTP/1.1 100 Continue",
            b"Host: example.org",
            b"",
            b"HTTP/1.0 204 No Content",
            b"Date: now!",
            b"",
            b"",
            ])

    def runTest(self):
        """Tests parsing of POST requests and responses."""
        p = RequestMessage()
        text = p.feed(self.post_request)

        self.assertEqual(text, b'')
        self.assertTrue(p.complete())

        p = ResponseMessage(p)
        text = p.feed(self.post_response)

        self.assertEqual(text, b'')
        self.assertTrue(p.complete())
        self.assertEqual(p.code, 204)
        self.assertEqual(p.header.version, b"HTTP/1.0")
        self.assertEqual(p.header.phrase, b"No Content")


class TestTwoPartStatus(unittest.TestCase):
    """This is a request taken from the wild that broke the crawler. The main
    part being tested is the status line without a message."""

    request = b"\r\n".join([
            b"GET / HTTP/1.1",
            b"Host: example.org", # Name changed to protect the guilty
            b"Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            b"Accept-Charset: ISO-8859-1,utf-8;q=0.7,*;q=0.3",
            b"Accept-Encoding: gzip,deflate,sdch",
            b"Accept-Language: en-US,en;q=0.8",
            b"Connection: keep-alive",
            b"Host: example.org",
            b"User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_2) AppleWebKit/535.7 (KHTML, like Gecko) Chrome/16.0.912.77 Safari/535.7",
            b"",
            b"",
            ])
    response = b"\r\n".join([
            b"HTTP/1.1 404",
            b"Cache-Control: no-cache",
            b"Content-Length: 0",
            b"Content-Type:image/gif",
            b"Pragma:no-cache",
            b"nnCoection: close",
            b"",
            b"",
            ])

    def runTest(self):
        """Tests parsing of a broken response."""
        p = RequestMessage()
        text = p.feed(self.request)

        self.assertEqual(text, b'')
        self.assertTrue(p.complete())

        p = ResponseMessage(p)
        text = p.feed(self.response)

        self.assertEqual(text, b'')
        self.assertTrue(p.complete())
        self.assertEqual(p.code, 404)
        self.assertEqual(p.header.version, b"HTTP/1.1")


class PostTestPseudoGzipped(unittest.TestCase):
    """Tests the parser with a POST request with gzip encoding."""
    post_request = b"\r\n".join([
        b"GET / HTTP/1.1",
        b"Host: example.org",
        b"",
        b"",
        b"",
    ])
    post_response = b"\r\n".join([
        b"HTTP/1.1 200 OK",
        b"Host: example.org",
        b"Content-Encoding: gzip",
        b"Content-Length: 7",
        b"",
        b"text",
        b""
    ])

    def runTest(self):
        """Tests parsing of POST response."""
        request = RequestMessage()
        response = ResponseMessage(request)
        text = response.feed(self.post_response)

        self.assertEqual(text, b'')
        self.assertTrue(response.complete())
        self.assertEqual(response.code, 200)
        self.assertEqual(response.header.version, b"HTTP/1.1")


class PostTestGzipped(unittest.TestCase):
    """Tests the parser with a POST request with gzip encoding."""
    post_request = b"\r\n".join([
        b"GET / HTTP/1.1",
        b"Host: example.org",
        b"",
        b"",
        b"",
    ])

    def runTest(self):
        """Tests parsing of POST response."""
        response_list = [
            b"HTTP/1.1 200 OK",
            b"Host: example.org",
            b"Content-Encoding: gzip",
            b"Content-Length: 30",
            b""
        ]
        gfile = open('hanzo/httptools/tests/test.gz', 'rb')

        response_list.append(gfile.read())
        gfile.close()

        self.post_response = b"\r\n".join(response_list)
        request = RequestMessage()
        response = ResponseMessage(request)
        text = response.feed(self.post_response)

        self.assertEqual(text, b'')
        self.assertTrue(response.complete())
        self.assertEqual(response.code, 200)
        self.assertEqual(response.header.version, b"HTTP/1.1")


if __name__ == '__main__':
    unittest.main()
