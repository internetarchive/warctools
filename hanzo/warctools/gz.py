import struct
import sys
import os
import zlib
import io
import gzip

class MultiMemberGzipReader(object):
    class InputBuffer(object):
        MIN_CHUNK_SIZE = 1024
        READ_SIZE = 8192

        def __init__(self, fileobj):
            self.fileobj = fileobj
            self._offset = 0
            self._buf = b''
            self._buf_offset = 0

        def _refill(self):
            bytes_read = self.fileobj.read(self.READ_SIZE)
            self._offset += len(bytes_read)
            self._buf = self._buf[self._buf_offset:] + bytes_read
            self._buf_offset = 0

        def next_bytes(self, size):
            """size is required here"""
            if self._buf_offset + size - 1 >= len(self._buf):
                self._refill()
            try:
                return self._buf[self._buf_offset:self._buf_offset+size]
            finally:
                self._buf_offset += size

        def next_chunk(self):
            if len(self._buf) - self._buf_offset < self.MIN_CHUNK_SIZE:
                self._refill()
            try:
                return self._buf[self._buf_offset:]
            finally:
                self._buf_offset = len(self._buf)

        def rewind(self, n):
            if n < 0 or n > self._buf_offset:
                raise IndexError
            self._buf_offset -= n

        def tell(self):
            return self._offset - len(self._buf) + self._buf_offset

    class GzipMemberReader(object):
        def __init__(self, parent, member_offset):
            self._parent = parent
            self.eof = False
            self.member_offset = member_offset

        def _read_chunk(self, size=-1, delim=None):
            if self.eof:
                return b''

            res = self._parent._decompress_until(size, delim)
            if self._parent._new_member:
                self.eof = True

            return res

        def readline(self, size=-1):
            return self._read_chunk(size, b'\n')

        def read(self, size=-1):
            return self._read_chunk(size)

        def __iter__(self):
            return iter(self.readline, b'')

        def close(self):
            self.eof = True

    def __init__(self, fileobj):
        self._cbuf = self.InputBuffer(fileobj)
        self._decompressor = zlib.decompressobj(-zlib.MAX_WBITS)
        self._dbuf = b''
        self._new_member = True
        self._cbuf_new_member = True
        self._member_offset = 0

    def __iter__(self):
        return self

    def _decompress_until(self, size=-1, delim=None):
        """Decompresses within until delim is found, size is reached, or the
        end of the member. After the end of the member is reached, subsequent
        calls return b'' (until the next call to self.__next__())."""
        if self._new_member:
            return b''
        while True:
            end = None
            if delim is not None:
                delim_offset = self._dbuf.find(delim, 0, size)
                if delim_offset >= 0:
                    end = delim_offset + len(delim)
            if end is None and size >= 0 and size < len(self._dbuf):
                end = size
            if end is None and self._cbuf_new_member:
                end = len(self._dbuf)

            if end == len(self._dbuf) and self._cbuf_new_member:
                self._new_member = True

            if end is not None:
                res = self._dbuf[:end]
                self._dbuf = self._dbuf[end:]
                return res

            tmp_cbuf = self._cbuf.next_chunk()
            if tmp_cbuf == b'':
                raise EOFError(
                        'Compressed file ended before the end-of-stream '
                        'marker was reached')
            self._dbuf += self._decompressor.decompress(tmp_cbuf)
            if self._decompressor.unused_data != b'':
                self._cbuf.rewind(len(self._decompressor.unused_data))
                self._skip_eof()
                self._cbuf_new_member = True
                self._member_offset = self._cbuf.tell()
                self._decompressor = zlib.decompressobj(-zlib.MAX_WBITS)

    def __next__(self):
        while not self._new_member:
            self._decompress_until(8192)

        if self._cbuf.next_bytes(1) != b'':
            self._cbuf.rewind(1)
            res = self.GzipMemberReader(self, self._member_offset)
            self._skip_gzip_header()
            self._cbuf_new_member = False
            self._new_member = False
            return res
        else:
            raise StopIteration

    # python2
    def next(self):
        return self.__next__()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

    def _skip_gzip_header(self):
        magic = self._cbuf.next_bytes(2)

        if magic != b'\037\213':
            raise OSError('Not a gzipped file (%r)' % magic)

        (method, flag, self._last_mtime) = struct.unpack(
                "<BBIxx", self._cbuf.next_bytes(8))
        if method != 8:
            raise OSError('Unknown compression method')

        if flag & gzip.FEXTRA:
            # Read & discard the extra field, if present
            extra_len, = struct.unpack("<H", self._cbuf.next_bytes(2))
            self._cbuf.next_bytes(extra_len)

        if flag & gzip.FNAME:
            # Read and discard a null-terminated string containing the filename
            while True:
                s = self._cbuf.next_bytes(1)
                if not s or s == b'\000':
                    break

        if flag & gzip.FCOMMENT:
            # Read and discard a null-terminated string containing a comment
            while True:
                s = self._cbuf.next_bytes(1)
                if not s or s==b'\000':
                    break

        if flag & gzip.FHCRC:
            s = self._cbuf.next_bytes(2)

        return True

    def _skip_eof(self):
        crc32, isize = struct.unpack("<II", self._cbuf.next_bytes(8))
        # if crc32 != self._crc:
        #     raise OSError("CRC check failed %s != %s" % (hex(crc32),
        #                                                  hex(self._crc)))
        # elif isize != (self._stream_size & 0xffffffff):
        #     raise OSError("Incorrect length of data produced")

        while True:
            bite = self._cbuf.next_bytes(1)
            if bite != b'\0':
                self._cbuf.rewind(1)
                break

