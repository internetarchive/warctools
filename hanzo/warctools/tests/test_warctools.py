# vim: set sw=4 et:

import unittest2
from StringIO import StringIO
import tempfile
import gzip
from hanzo import warctools, httptools

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
        f = StringIO(s)
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
        f = StringIO(s)
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


class WarcWritingTest(unittest2.TestCase):

    # XXX should this a part of the library?
    def build_warc_record(self, url, warc_date=None, content_buffer=None,
            content_file=None, content_length=None, concurrent_to=None,
            warc_type=None, content_type=None, remote_ip=None, profile=None,
            refers_to=None, refers_to_target_uri=None, refers_to_date=None,
            record_id=None, block_digest=None, payload_digest=None):

        if warc_date is None:
            warc_date = warctools.warc.warc_datetime_str(datetime.now())

        if record_id is None:
            record_id = warctools.WarcRecord.random_warc_uuid()

        headers = []
        if warc_type is not None:
            headers.append((warctools.WarcRecord.TYPE, warc_type))
        headers.append((warctools.WarcRecord.ID, record_id))
        headers.append((warctools.WarcRecord.DATE, warc_date))
        headers.append((warctools.WarcRecord.URL, url))
        if remote_ip is not None:
            headers.append((warctools.WarcRecord.IP_ADDRESS, remote_ip))
        if profile is not None:
            headers.append((warctools.WarcRecord.PROFILE, profile))
        if refers_to is not None:
            headers.append((warctools.WarcRecord.REFERS_TO, refers_to))
        if refers_to_target_uri is not None:
            headers.append((warctools.WarcRecord.REFERS_TO_TARGET_URI, refers_to_target_uri))
        if refers_to_date is not None:
            headers.append((warctools.WarcRecord.REFERS_TO_DATE, refers_to_date))
        if concurrent_to is not None:
            headers.append((warctools.WarcRecord.CONCURRENT_TO, concurrent_to))
        if content_type is not None:
            headers.append((warctools.WarcRecord.CONTENT_TYPE, content_type))
        if content_length is not None:
            headers.append((warctools.WarcRecord.CONTENT_LENGTH, content_length))
        if block_digest is not None:
            headers.append((warctools.WarcRecord.BLOCK_DIGEST, block_digest))
        if payload_digest is not None:
            headers.append((warctools.WarcRecord.BLOCK_DIGEST, payload_digest))

        if content_file is not None:
            assert content_buffer is None
            assert content_length is not None
            record = warctools.WarcRecord(headers=headers, content_file=content_file)
        else:
            assert content_buffer is not None
            content_tuple = (content_type, content_buffer)
            record = warctools.WarcRecord(headers=headers, content=content_tuple)

        return record

    def build_record_using_tuple(self):
        content_buffer = 'Luke, I am your payload'
        record = self.build_warc_record(url='http://example.org/',
                content_buffer=content_buffer,
                record_id='<urn:uuid:00000000-0000-0000-0000-000000000000>',
                warc_date='2013-11-15T00:00:00Z',
                warc_type=warctools.WarcRecord.RESPONSE,
                content_type=httptools.RequestMessage.CONTENT_TYPE)
        return record

    def build_record_using_stream(self):
        content_buffer = 'Shmuke, I gam four snayglob'
        fh = StringIO(content_buffer)
        record = self.build_warc_record(url='http://example.org/',
                content_file=fh, content_length=str(len(content_buffer)),
                record_id='<urn:uuid:00000000-0000-0000-0000-000000000000>',
                warc_date='2013-11-15T00:00:00Z',
                warc_type=warctools.WarcRecord.RESPONSE,
                content_type=httptools.RequestMessage.CONTENT_TYPE)
        return record


    def test_write_using_tuple(self):
        record = self.build_record_using_tuple()

        f = StringIO()
        record.write_to(f)
        self.assertEqual(f.getvalue(), 
                'WARC/1.0\r\nWARC-Type: response\r\nWARC-Record-ID: <urn:uuid:00000000-0000-0000-0000-000000000000>\r\nWARC-Date: 2013-11-15T00:00:00Z\r\nWARC-Target-URI: http://example.org/\r\nContent-Type: application/http;msgtype=request\r\nContent-Length: 23\r\n\r\nLuke, I am your payload\r\n\r\n')
        f.close()

        # should work again if we do it again
        f = StringIO()
        record.write_to(f)
        self.assertEqual(f.getvalue(), 
                'WARC/1.0\r\nWARC-Type: response\r\nWARC-Record-ID: <urn:uuid:00000000-0000-0000-0000-000000000000>\r\nWARC-Date: 2013-11-15T00:00:00Z\r\nWARC-Target-URI: http://example.org/\r\nContent-Type: application/http;msgtype=request\r\nContent-Length: 23\r\n\r\nLuke, I am your payload\r\n\r\n')
        f.close()


    def test_write_using_tuple_gz(self):
        record = self.build_record_using_tuple()

        f = StringIO()
        record.write_to(f, gzip=True)
        f.seek(0)
        g = gzip.GzipFile(fileobj=f, mode='rb')
        self.assertEqual(g.read(), 'WARC/1.0\r\nWARC-Type: response\r\nWARC-Record-ID: <urn:uuid:00000000-0000-0000-0000-000000000000>\r\nWARC-Date: 2013-11-15T00:00:00Z\r\nWARC-Target-URI: http://example.org/\r\nContent-Type: application/http;msgtype=request\r\nContent-Length: 23\r\n\r\nLuke, I am your payload\r\n\r\n')
        g.close()
        f.close()

        # should work again if we do it again
        f = StringIO()
        record.write_to(f, gzip=True)
        f.seek(0)
        g = gzip.GzipFile(fileobj=f, mode='rb')
        self.assertEqual(g.read(), 'WARC/1.0\r\nWARC-Type: response\r\nWARC-Record-ID: <urn:uuid:00000000-0000-0000-0000-000000000000>\r\nWARC-Date: 2013-11-15T00:00:00Z\r\nWARC-Target-URI: http://example.org/\r\nContent-Type: application/http;msgtype=request\r\nContent-Length: 23\r\n\r\nLuke, I am your payload\r\n\r\n')
        g.close()
        f.close()


    def test_write_using_stream(self):
        record = self.build_record_using_stream()

        f = StringIO()
        record.write_to(f)
        self.assertEqual(f.getvalue(), 
                'WARC/1.0\r\nWARC-Type: response\r\nWARC-Record-ID: <urn:uuid:00000000-0000-0000-0000-000000000000>\r\nWARC-Date: 2013-11-15T00:00:00Z\r\nWARC-Target-URI: http://example.org/\r\nContent-Type: application/http;msgtype=request\r\nContent-Length: 27\r\n\r\nShmuke, I gam four snayglob\r\n\r\n')
        f.close()

        # throws exception because record.content_file position has advanced
        f = StringIO()
        with self.assertRaises(Exception):
            record.write_to(f)
        f.close()


    def test_write_using_stream_gz(self):
        record = self.build_record_using_stream()

        f = StringIO()
        record.write_to(f, gzip=True)
        f.seek(0)
        g = gzip.GzipFile(fileobj=f, mode='rb')
        self.assertEqual(g.read(), 'WARC/1.0\r\nWARC-Type: response\r\nWARC-Record-ID: <urn:uuid:00000000-0000-0000-0000-000000000000>\r\nWARC-Date: 2013-11-15T00:00:00Z\r\nWARC-Target-URI: http://example.org/\r\nContent-Type: application/http;msgtype=request\r\nContent-Length: 27\r\n\r\nShmuke, I gam four snayglob\r\n\r\n')
        g.close()
        f.close()

        # throws exception because record.content_file position has advanced
        f = StringIO()
        with self.assertRaises(Exception):
            record.write_to(f, gzip=True)
        f.close()


if __name__ == '__main__':
    unittest2.main()
