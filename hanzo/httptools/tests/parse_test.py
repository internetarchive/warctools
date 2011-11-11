import unittest2


from StringIO import StringIO
from hanzo.httptools.messaging import RequestParser, ResponseParser


get_request = "\r\n".join( [
    "GET / HTTP/1.1",
    "Host: example.org",
    "",
    "",
])
get_response = "\r\n".join( [
    "HTTP/1.1 200 OK",
    "Host: example.org",
    "Content-Length:5",
    "",
    "tests",
])


class GetTest(unittest2.TestCase):

    def runTest(self):
        print repr(get_request)
        buffer = StringIO()
        p = RequestParser(buffer)
        text = p.feed(get_request)

        self.assertEqual(text, '')
        self.assertEqual(get_request, buffer.getvalue())
        self.assertTrue(p.complete())

        print repr(get_response)

        
        buffer = StringIO()
        p = ResponseParser(buffer, p.header)
        text = p.feed(get_response)

        self.assertEqual(text, '')
        self.assertEqual(get_response, buffer.getvalue())
        self.assertTrue(p.complete())

head_request = "\r\n".join( [
    "HEAD / HTTP/1.1",
    "Host: example.org",
    "",
    "",
])
head_response = "\r\n".join( [
    "HTTP/1.1 200 OK",
    "Host: example.org",
    "Content-Length:5",
    "",
    "",
])


class HeadTest(unittest2.TestCase):

    def runTest(self):
        print repr(head_request)
        buffer = StringIO()
        p = RequestParser(buffer)
        text = p.feed(head_request)

        self.assertEqual(text, '')
        self.assertEqual(head_request, buffer.getvalue())
        self.assertTrue(p.complete())

        print repr(head_response)

        
        buffer = StringIO()
        p = ResponseParser(buffer, p.header)
        text = p.feed(head_response)

        self.assertEqual(text, '')
        self.assertEqual(head_response, buffer.getvalue())
        self.assertTrue(p.complete())
post_request = "\r\n".join( [
    "POST / HTTP/1.1",
    "Host: example.org",
    "Transfer-Encoding: chunked",
    "",
    "8",
    "abcdefgh",
    "0",
    "",
    "",
])
post_response = "\r\n".join( [
    "HTTP/1.1 100 Continue",
    "Host: example.org",
    "",
    "",
    "HTTP/1.0 204 No Content",
    "Date: now!",
    "",
    "",
])


class HeadTest(unittest2.TestCase):

    def runTest(self):
        print repr(post_request)
        buffer = StringIO()
        p = RequestParser(buffer)
        text = p.feed(post_request)

        print p.mode

        self.assertEqual(text, '')
        self.assertEqual(post_request, buffer.getvalue())
        self.assertTrue(p.complete())

        print repr(post_response)

        
        buffer = StringIO()
        p = ResponseParser(buffer, p.header)
        text = p.feed(post_response)
        

        self.assertEqual(text, '')
        self.assertEqual(post_response, buffer.getvalue())
        self.assertTrue(p.complete())


if __name__ == '__main__':
    unittest2.main()


