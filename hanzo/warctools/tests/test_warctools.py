# vim: set sw=4 et:

import unittest2
import StringIO
import tempfile
import gzip
from hanzo import warctools

class ArcRecordTerminatorTest(unittest2.TestCase):
    REC1_CONTENT = (b'1 0 InternetArchive\n'
                  + b'URL IP-address Archive-date Content-type Archive-length\n'
                  + b'Here is some funky arc header content!\n')
    RECORD1 = b'filedesc://ArcRecordTerminatorTest.arc 0.0.0.0 20131113000000 text/plain {}\n{}'.format(len(REC1_CONTENT), REC1_CONTENT)

    REC2_CONTENT = (b'HTTP/1.1 200 OK\r\n'
                  + b'Content-Type: text/plain\r\n'
                  + b'Content-Length: 12\r\n'
                  + b'\r\n'
                  + b'01234567890\r\n')
    RECORD2 = b'http://example.org/ 192.168.1.1 20131113000000 text/plain {}\n{}'.format(len(REC2_CONTENT), REC2_CONTENT)

    def _arc_gz(self, terminator=b'\r\n\r\n'):
        # r+b instead of w+b to avoid http://bugs.python.org/issue18323
        f = tempfile.TemporaryFile(mode='r+b')

        g = gzip.GzipFile(fileobj=f, mode='wb')
        g.write(self.RECORD1)
        g.write(terminator)
        g.close()

        g = gzip.GzipFile(fileobj=f, mode='wb')
        g.write(self.RECORD2)
        g.write(terminator)
        g.close()

        f.seek(0)
        return f

    def _arc(self, terminator):
        s = self.RECORD1 + terminator + self.RECORD2 + terminator
        f = StringIO.StringIO(s)
        return f

    def _test_terminator(self, terminator):
        # print('testing warc with record terminator {}'.format(repr(terminator)))
        fin = self._arc(terminator)
        self._run_checks(fin, terminator, False)
        
        fin = self._arc_gz(terminator)
        self._run_checks(fin, terminator, True)

    def _run_checks(self, fin, terminator, gzipped):
        fh = warctools.ArchiveRecord.open_archive(file_handle=fin)
        try:
            i = 0
            for (offset, record, errors) in fh.read_records(limit=None, offsets=True):
                if i == 0:
                    self.assertEqual(offset, 0)
                    self.assertEqual(type(record), warctools.arc.ArcRecordHeader)
                    self.assertEqual(record.type, 'filedesc')
                    self.assertEqual(record.content_type, 'text/plain')
                    # content_length != len(record.content[1]) here because
                    # ArcParser reads and parses part of the "content" of the
                    # arc header record 
                    self.assertEqual(record.content_length, 115)
                    self.assertEqual(record.content[1], b'Here is some funky arc header content!\n')
                elif i == 1:
                    if not gzipped:
                        self.assertEqual(offset, len(self.RECORD1) + len(terminator))
                    else:
                        self.assertLess(offset, len(self.RECORD1) + len(terminator))
                    self.assertEqual(type(record), warctools.arc.ArcRecord)
                    self.assertEqual(record.type, 'response')
                    self.assertEqual(record.content_type, 'text/plain')
                    self.assertEqual(record.content_length, 78)
                    self.assertEqual(record.content[1], b'HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 12\r\n\r\n01234567890\r\n')
                elif i == 2:
                    if not gzipped:
                        self.assertEqual(offset, len(self.RECORD1) + len(self.RECORD2) + 2 * len(terminator))
                    else:
                        self.assertLess(offset, len(self.RECORD1) + len(self.RECORD2) + 2 * len(terminator))
                    self.assertIsNone(record)
                else:
                    self.fail('this line should not be reached')

                i += 1
        finally:
            fh.close()

    def runTest(self):
        # anything works as long as it contains only \r and \n and ends with \n
        self._test_terminator(b'\n') # the good one
        self._test_terminator(b'\r\n\r\n') 
        self._test_terminator(b'\r\n')
        self._test_terminator(b'\n\r\n')
        self._test_terminator(b'\n\n\r\n')
        self._test_terminator(b'\r\n\n')
        self._test_terminator(b'\r\n\r\n\r\n')
        self._test_terminator(b'\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n')
        self._test_terminator(b'\n\n')
        self._test_terminator(b'\n\n\n')
        self._test_terminator(b'\n\n\n\n')
        self._test_terminator(b'\r\n\n\r\n\n')
        self._test_terminator(b'\r\r\r\r\r\r\n')
        self._test_terminator(b'\r\r\r\r\r\r\n\n')
        self._test_terminator(b'\r\r\r\r\r\r\n\n\n')

class WarcRecordTerminatorTest(unittest2.TestCase):
    RECORD1 = (b'WARC/1.0\r\n'
             + b'WARC-Record-ID: <urn:uuid:00000000-0000-0000-0000-000000000000>\r\n'
             + b'WARC-Type: warcinfo\r\n'
             + b'Content-Type: application/warc-fields\r\n'
             + b'Content-Length: 30\r\n'
             + b'\r\n'
             + b'format: WARC File Format 1.0\r\n')

    RECORD2 = (b'WARC/1.0\r\n'
             + b'WARC-Type: response\r\n'
             + b'WARC-Record-ID: <urn:uuid:00000000-0000-0000-0000-000000000001>\r\n'
             + b'WARC-Target-URI: http://example.org/\r\n'
             + b'Content-Type: application/http;msgtype=response\r\n'
             + b'Content-Length: 78\r\n'
             + b'\r\n'
             + b'HTTP/1.1 200 OK\r\n'
             + b'Content-Type: text/plain\r\n'
             + b'Content-Length: 12\r\n'
             + b'\r\n'
             + b'01234567890\r\n')

    def _warc_gz(self, terminator=b'\r\n\r\n'):
        # r+b instead of w+b to avoid http://bugs.python.org/issue18323
        f = tempfile.TemporaryFile(mode='r+b')

        g = gzip.GzipFile(fileobj=f, mode='wb')
        g.write(self.RECORD1)
        g.write(terminator)
        g.close()

        g = gzip.GzipFile(fileobj=f, mode='wb')
        g.write(self.RECORD2)
        g.write(terminator)
        g.close()

        f.seek(0)
        return f

    def _warc(self, terminator):
        s = self.RECORD1 + terminator + self.RECORD2 + terminator
        f = StringIO.StringIO(s)
        return f

    def _test_terminator(self, terminator):
        # print('testing warc with record terminator {}'.format(repr(terminator)))
        fin = self._warc(terminator)
        self._run_checks(fin, terminator, False)
        
        fin = self._warc_gz(terminator)
        self._run_checks(fin, terminator, True)

    def _run_checks(self, fin, terminator, gzipped):
        fh = warctools.ArchiveRecord.open_archive(file_handle=fin)
        try:
            i = 0
            for (offset, record, errors) in fh.read_records(limit=None, offsets=True):
                if i == 0:
                    self.assertEqual(offset, 0)
                    self.assertEqual(type(record), warctools.warc.WarcRecord)
                    self.assertEqual(record.type, 'warcinfo')
                    self.assertEqual(record.content_type, 'application/warc-fields')
                    self.assertEqual(record.content_length, 30)
                    self.assertEqual(record.content[1], b'format: WARC File Format 1.0\r\n')
                elif i == 1:
                    if not gzipped:
                        self.assertEqual(offset, len(self.RECORD1) + len(terminator))
                    else:
                        self.assertLess(offset, len(self.RECORD1) + len(terminator))
                    self.assertEqual(type(record), warctools.warc.WarcRecord)
                    self.assertEqual(record.type, 'response')
                    self.assertEqual(record.content_type, 'application/http;msgtype=response')
                    self.assertEqual(record.content_length, 78)
                    self.assertEqual(record.content[1], b'HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 12\r\n\r\n01234567890\r\n')
                elif i == 2:
                    if not gzipped:
                        self.assertEqual(offset, len(self.RECORD1) + len(self.RECORD2) + 2 * len(terminator))
                    else:
                        self.assertLess(offset, len(self.RECORD1) + len(self.RECORD2) + 2 * len(terminator))
                    self.assertIsNone(record)
                else:
                    self.fail('this line should not be reached')

                i += 1
        finally:
            fh.close()

    def runTest(self):
        # anything works as long as it contains only \r and \n and ends with \n
        self._test_terminator(b'\r\n\r\n') # the good one
        self._test_terminator(b'\r\n')
        self._test_terminator(b'\n\r\n')
        self._test_terminator(b'\n\n\r\n')
        self._test_terminator(b'\r\n\n')
        self._test_terminator(b'\r\n\r\n\r\n')
        self._test_terminator(b'\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n')
        self._test_terminator(b'\n')
        self._test_terminator(b'\n\n')
        self._test_terminator(b'\n\n\n')
        self._test_terminator(b'\n\n\n\n')
        self._test_terminator(b'\r\n\n\r\n\n')
        self._test_terminator(b'\r\r\r\r\r\r\n')
        self._test_terminator(b'\r\r\r\r\r\r\n\n')
        self._test_terminator(b'\r\r\r\r\r\r\n\n\n')

if __name__ == '__main__':
    unittest2.main()
