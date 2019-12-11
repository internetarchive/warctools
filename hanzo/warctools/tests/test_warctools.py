# vim: set sw=4 et:

import gzip
import unittest

from datetime import datetime
from hanzo import warctools, httptools

try:
    from io import BytesIO
except ImportError:
    from StringIO import StringIO
    BytesIO = StringIO

try:
    unittest.TestCase.assertIsNone
except AttributeError:
    import unittest2
    unittest = unittest2


class ArcRecordTerminatorTest(unittest.TestCase):
    REC1_CONTENT = (b'1 0 InternetArchive\n'
                    b'URL IP-address Archive-date Content-type Archive-length\n'
                    b'Here is some funky arc header content!\n')
    RECORD1 = b'filedesc://ArcRecordTerminatorTest.arc 0.0.0.0 20131113000000 text/plain ' + str(len(REC1_CONTENT)).encode('ascii') + b'\n' + REC1_CONTENT

    REC2_CONTENT = (b'HTTP/1.1 200 OK\r\n'
                    b'Content-Type: text/plain\r\n'
                    b'Content-Length: 12\r\n'
                    b'\r\n'
                    b'01234567890\r\n')
    RECORD2 = b'http://example.org/ 192.168.1.1 20131113000000 text/plain ' + str(len(REC2_CONTENT)).encode('ascii') + b'\n' + REC2_CONTENT

    REC1_GZ = b"\x1f\x8b\x08\x00\xbf\xa9\x99R\x02\xff=NK\x0e\x820\x14\xdc\xf7\x14\xcf\x03\xf0\xa9\xc4\x8d;\xe3F\x12\x17\x86\xe0\x01\x9av\x90Fh\xc9\xeb\xd3\xc8\xedE4\xce\xec\xe6\x97\xe9\xfc\x00\x87d\xf7Eq`\xdb\xc0Fv-x\xf4\xc1H\xe4\x16Ir\xc3\x96\xca|%mK]i\xad\xabr\x05\t^RL\x83\xf1\x81\xb4\xde)M%\xd5A\xc0\x01\xb2\xac\xf5\xfe\tum\xceT_2\xe3\x1c#%\xfa\xc9\x993\x02:\xc6%\x1c$\x93y\xc2\xdf\x19\x10n\xd2\xab\x13\x18\xe4\x13\xa58\x82\xbaG\xb8\xcf\xf49\xd2\xc380\xd9os\xa3\xd4\x1b\xa0\xa9\x1c5\xc1\x00\x00\x00"
    REC2_GZ = b"\x1f\x8b\x08\x00\xbf\xa9\x99R\x02\xffM\xca1\x0e\xc20\x0c@\xd1\xddR\xee\xe0\x0b\x10\xdb\t\xb4iV\x16$\x90`\xc8\x05:X-RI#\xe4\xa1\xdc\x1e\t\x06\xf8\xeb\x7f\xb3Y\xcbD\xba\x8d\x8f\xb6\xa8_\x9f\x13\xa1\x0c\xc1K\x97\xbcx\xc1\xc0\x12E$\xf2'4\xdd\x8c\xda2\xde+\xf6\tN\xa5\xdc\xe8\xab\x18\xafg\x07\xc7\xb5\x9aV\xdb\x95W\xd3\xfc\x87\x7f\xe7\xa2u\xb29\xa3\x04\x07\x0eXB\xdc\x1f\xba>\r\xec\x00\xde#Pz\x9d\x8c\x00\x00\x00"

    def _arc_gz(self, terminator=b'\r\n\r\n'):
        return BytesIO(self.REC1_GZ + self.REC2_GZ)

    def _arc(self, terminator):
        s = self.RECORD1 + terminator + self.RECORD2 + terminator
        f = BytesIO(s)
        return f

    def _test_terminator(self, terminator):
        # print('testing warc with record terminator {}'.format(repr(terminator)))
        fin = self._arc(terminator)
        try:
            self._run_checks(fin, terminator, False)
        finally:
            fin.close()

        fin = self._arc_gz(terminator)
        try:
            self._run_checks(fin, terminator, True)
        finally:
            fin.close()

    def _run_checks(self, fin, terminator, gzipped):
        fh = warctools.ArchiveRecord.open_archive(file_handle=fin)
        try:
            i = 0
            for (offset, record, errors) in fh.read_records(limit=None, offsets=True):
                if i == 0:
                    self.assertEqual(offset, 0)
                    self.assertEqual(type(record), warctools.arc.ArcRecordHeader)
                    self.assertEqual(record.type, b'filedesc')
                    self.assertEqual(record.content_type, b'text/plain')
                    # content_length != len(record.content[1]) here because
                    # ArcParser reads and parses part of the "content" of the
                    # arc header record 
                    self.assertEqual(record.content_length, 115)
                    self.assertEqual(record.content[1], b'Here is some funky arc header content!\n')
                elif i == 1:
                    if not gzipped:
                        self.assertEqual(offset, len(self.RECORD1) + len(terminator))
                    else:
                        self.assertEqual(offset, len(self.REC1_GZ))
                    self.assertEqual(type(record), warctools.arc.ArcRecord)
                    self.assertEqual(record.type, b'response')
                    self.assertEqual(record.content_type, b'text/plain')
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

        # added this to get stdout from test runner when tests pass
        # raise ValueError('[DEBUG] ArcRecordTerminatortest Done.')


class WarcRecordTerminatorTest(unittest.TestCase):
    RECORD1 = (b'WARC/1.0\r\n'
               b'WARC-Record-ID: <urn:uuid:00000000-0000-0000-0000-000000000000>\r\n'
               b'WARC-Type: warcinfo\r\n'
               b'Content-Type: application/warc-fields\r\n'
               b'Content-Length: 30\r\n'
               b'\r\n'
               b'format: WARC File Format 1.0\r\n')

    RECORD2 = (b'WARC/1.0\r\n'
               b'WARC-Type: response\r\n'
               b'WARC-Record-ID: <urn:uuid:00000000-0000-0000-0000-000000000001>\r\n'
               b'WARC-Target-URI: http://example.org/\r\n'
               b'Content-Type: application/http;msgtype=response\r\n'
               b'Content-Length: 78\r\n'
               b'\r\n'
               b'HTTP/1.1 200 OK\r\n'
               b'Content-Type: text/plain\r\n'
               b'Content-Length: 12\r\n'
               b'\r\n'
               b'01234567890\r\n')

    RECORD1_GZ = b'\x1f\x8b\x08\x00\xce\xae\x99R\x02\xff\x0bw\x0cr\xd67\xd43\xe0\xe5\n\x07\xb2t\x83R\x93\xf3\x8bRt=]\xac\x14lJ\x8b\xf2\xacJK3S\xac\x0c\xa0@\x17\x0b\x01\x03vP\x03B*\x0bR\xad\x14\xca\x13\x8b\x923\xf3\xd2\xf2y\xb9\x9c\xf3\xf3JR\xf3J\xa0\xe2\x89\x05\x059\x99\xc9\x89%\x99\xf9y\xfa 5\xbai\x99\xa99)\xc5\x08e>\xa9y\xe9%\x19V\n\xc6@\x07\xf1r\xa5\xe5\x17\xe5&\x96X)\x80LVp\xcb\xccIUp\x03\x8b(\x80\x1d\x0c\x82\x00\x04h\xbe\xd2\xbf\x00\x00\x00'
    RECORD2_GZ = b'\x1f\x8b\x08\x00\xce\xae\x99R\x02\xffm\x8f\xc9\n\xc20\x10\x86\xef\x81\xbcC^\xa0MR\x97j\\@\xeaAQPJ\xa5\xe7\xa0C-\xd4$\xa4S\xd0\xb7\xb7\x85\x16A\xfd\x0f\xc3\xac\xdf\xcc\xe4\x9b4\xe12\x14\x94\xe4\xad\x17d/\x07\x8ay\xa8\x9d55\xf4\xc9\x14\xae\xd6\xdf\x82\xfdV\xb1e\xe3\x8dj\x9a\xf2\xa6D\xaf\xe0\x8f\xe9%\xd7\x03U\xfb\x020\xb8\xa4{\xc5\xee\x88Nq\x0eO\xfdp\x15\x84\xd6\x17\x9c\x92\xc4\x1a\x04\x83\xfdz\xed\\U^5\x96\xd6\xf0\xae}\xf1\xa8\x0bl+\xab\xcf]\xc3\xc0\x11L\x81w\xc5\xe2\x19%\x94\xec\xb2\xec\xdc>#Y$\x04;\x1d\xbe\xb9\x08O\xe4\xae\xd2\xa5\xf9\x05\xc8\xa8\x03\x08\x19\x8d\xc6\x93i<\x9b\x8b.\xa4\xe4\rV`\x1c`\x1f\x01\x00\x00'

    def _warc_gz(self, terminator=b'\r\n\r\n'):
        return BytesIO(self.RECORD1_GZ + self.RECORD2_GZ)

    def _warc(self, terminator):
        s = self.RECORD1 + terminator + self.RECORD2 + terminator
        f = BytesIO(s)
        return f

    def _test_terminator(self, terminator):
        # print('testing warc with record terminator {}'.format(repr(terminator)))
        fin = self._warc(terminator)
        try:
            self._run_checks(fin, terminator, False)
        finally:
            fin.close()

        fin = self._warc_gz(terminator)
        try:
            self._run_checks(fin, terminator, True)
        finally:
            fin.close()

    def _run_checks(self, fin, terminator, gzipped):
        fh = warctools.ArchiveRecord.open_archive(file_handle=fin)
        try:
            i = 0
            for (offset, record, errors) in fh.read_records(limit=None, offsets=True):
                if i == 0:
                    self.assertEqual(offset, 0)
                    self.assertEqual(type(record), warctools.warc.WarcRecord)
                    self.assertEqual(record.type, b'warcinfo')
                    self.assertEqual(record.content_type, b'application/warc-fields')
                    self.assertEqual(record.content_length, 30)
                    self.assertEqual(record.content[1], b'format: WARC File Format 1.0\r\n')
                elif i == 1:
                    if not gzipped:
                        self.assertEqual(offset, len(self.RECORD1) + len(terminator))
                    else:
                        self.assertEqual(offset, len(self.RECORD1_GZ))
                    self.assertEqual(type(record), warctools.warc.WarcRecord)
                    self.assertEqual(record.type, b'response')
                    self.assertEqual(record.content_type, b'application/http;msgtype=response')
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
        self._test_terminator(b'\r\n\r\n')  # the good one
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

        # added this to get stdout from test runner when tests pass
        # raise ValueError('[DEBUG] WarcRecordTerminatortest Done.')


class WarcWritingTest(unittest.TestCase):

    # XXX should this a part of the library?
    def build_warc_record(
            self, url, warc_date=None, content_buffer=None,
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
        content_buffer = b'Luke, I am your payload'
        record = self.build_warc_record(
            url=b'http://example.org/',
            content_buffer=content_buffer,
            record_id=b'<urn:uuid:00000000-0000-0000-0000-000000000000>',
            warc_date=b'2013-11-15T00:00:00Z',
            warc_type=warctools.WarcRecord.RESPONSE,
            content_type=httptools.RequestMessage.CONTENT_TYPE)
        return record

    def build_record_using_stream(self):
        content_buffer = b'Shmuke, I gam four snayglob'
        fh = BytesIO(content_buffer)
        record = self.build_warc_record(
            url=b'http://example.org/',
            content_file=fh, content_length=str(len(content_buffer)).encode('ascii'),
            record_id=b'<urn:uuid:00000000-0000-0000-0000-000000000000>',
            warc_date=b'2013-11-15T00:00:00Z',
            warc_type=warctools.WarcRecord.RESPONSE,
            content_type=httptools.RequestMessage.CONTENT_TYPE)
        return record

    def test_write_using_tuple(self):
        record = self.build_record_using_tuple()

        f = BytesIO()
        record.write_to(f)
        self.assertEqual(f.getvalue(), 
                         b'WARC/1.0\r\nWARC-Type: response\r\nWARC-Record-ID: <urn:uuid:00000000-0000-0000-0000-000000000000>\r\nWARC-Date: 2013-11-15T00:00:00Z\r\nWARC-Target-URI: http://example.org/\r\nContent-Type: application/http;msgtype=request\r\nContent-Length: 23\r\n\r\nLuke, I am your payload\r\n\r\n')
        f.close()

        # should work again if we do it again
        f = BytesIO()
        record.write_to(f)
        self.assertEqual(f.getvalue(),
                         b'WARC/1.0\r\nWARC-Type: response\r\nWARC-Record-ID: <urn:uuid:00000000-0000-0000-0000-000000000000>\r\nWARC-Date: 2013-11-15T00:00:00Z\r\nWARC-Target-URI: http://example.org/\r\nContent-Type: application/http;msgtype=request\r\nContent-Length: 23\r\n\r\nLuke, I am your payload\r\n\r\n')
        f.close()

    def test_write_using_tuple_gz(self):
        record = self.build_record_using_tuple()

        f = BytesIO()
        record.write_to(f, gzip=True)
        f.seek(0)
        g = gzip.GzipFile(fileobj=f, mode='rb')
        self.assertEqual(g.read(), b'WARC/1.0\r\nWARC-Type: response\r\nWARC-Record-ID: <urn:uuid:00000000-0000-0000-0000-000000000000>\r\nWARC-Date: 2013-11-15T00:00:00Z\r\nWARC-Target-URI: http://example.org/\r\nContent-Type: application/http;msgtype=request\r\nContent-Length: 23\r\n\r\nLuke, I am your payload\r\n\r\n')
        g.close()
        f.close()

        # should work again if we do it again
        f = BytesIO()
        record.write_to(f, gzip=True)
        f.seek(0)
        g = gzip.GzipFile(fileobj=f, mode='rb')
        self.assertEqual(g.read(), b'WARC/1.0\r\nWARC-Type: response\r\nWARC-Record-ID: <urn:uuid:00000000-0000-0000-0000-000000000000>\r\nWARC-Date: 2013-11-15T00:00:00Z\r\nWARC-Target-URI: http://example.org/\r\nContent-Type: application/http;msgtype=request\r\nContent-Length: 23\r\n\r\nLuke, I am your payload\r\n\r\n')
        g.close()
        f.close()

    def test_write_using_stream(self):
        record = self.build_record_using_stream()

        f = BytesIO()
        record.write_to(f)
        self.assertEqual(
            f.getvalue(),
            b'WARC/1.0\r\nWARC-Type: response\r\nWARC-Record-ID: <urn:uuid:00000000-0000-0000-0000-000000000000>\r\nWARC-Date: 2013-11-15T00:00:00Z\r\nWARC-Target-URI: http://example.org/\r\nContent-Type: application/http;msgtype=request\r\nContent-Length: 27\r\n\r\nShmuke, I gam four snayglob\r\n\r\n')
        f.close()

        # throws exception because record.content_file position has advanced
        f = BytesIO()
        with self.assertRaises(Exception):
            record.write_to(f)
        f.close()

    def test_write_using_stream_gz(self):
        record = self.build_record_using_stream()

        f = BytesIO()
        record.write_to(f, gzip=True)
        f.seek(0)
        g = gzip.GzipFile(fileobj=f, mode='rb')
        self.assertEqual(g.read(), b'WARC/1.0\r\nWARC-Type: response\r\nWARC-Record-ID: <urn:uuid:00000000-0000-0000-0000-000000000000>\r\nWARC-Date: 2013-11-15T00:00:00Z\r\nWARC-Target-URI: http://example.org/\r\nContent-Type: application/http;msgtype=request\r\nContent-Length: 27\r\n\r\nShmuke, I gam four snayglob\r\n\r\n')
        g.close()
        f.close()

        # throws exception because record.content_file position has advanced
        f = BytesIO()
        with self.assertRaises(Exception):
            record.write_to(f, gzip=True)
        f.close()


if __name__ == '__main__':
    unittest.main()
