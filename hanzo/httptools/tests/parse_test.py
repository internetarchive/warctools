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
    "butts",
])


class ParseRequestTest(unittest2.TestCase):

    def runTest(self):
        print repr(get_request)
        buffer = StringIO()
        p = RequestParser(buffer)
        text = p.feed(get_request)

        self.assertEqual(text, '')
        self.assertEqual(get_request, buffer.getvalue())
        self.assertTrue(p.complete)

        print repr(get_response)

        
        buffer = StringIO()
        p = ResponseParser(buffer, p.header)
        text = p.feed(get_response)

        self.assertEqual(text, '')
        self.assertEqual(get_response, buffer.getvalue())
        self.assertTrue(p.complete)

if __name__ == '__main__':
    unittest2.main()


