"""Extends gzip.GzipFile for raw gzip offset in python 2 and 3"""

import gzip
import io

try:
    import builtins
except ImportError:
    pass

try:

    # this branch contributed by Kenji:
    # https://github.com/kngenie/warctools/commit/159bfdfa45cc0b51ed4a4a4d7d744ef7bf82ae23

    # Python 3.5 got a major change to gzip module. Essential gunzip
    # work is now implemented in _GzipReader and GzipFile simply wraps
    # around it.

    class _GeeZipReader(gzip._GzipReader):

        """Extends python 3.5 gzip._GzipReader"""

        def _read_gzip_header(self):
            pos = self._raw_pos()
            has_record = super(_GeeZipReader, self)._read_gzip_header()

            if has_record:
                self.member_offset = pos

            return has_record

        def _raw_pos(self):
            """Return offset in raw gzip file corresponding to this
            object's state."""

            # _fp is PaddedFile object with prepend method. it doesn't have
            # tell(). It has seek(), but it's useless as a replacement for
            # tell(). We need to compute offset from internal attributes.

            pos = self._fp.file.tell()

            if self._fp._read is not None:
                pos -= (self._fp._length - self._fp._read)

            return pos

    class GeeZipFile(gzip.GzipFile):

        def __init__(self, filename=None, mode=None, fileobj=None):
            if mode is None:
                mode = getattr(fileobj, 'mode', 'rb')

            if mode.startswith('r'):
                if mode and 'b' not in mode:
                    mode += 'b'
                if fileobj is None:
                    fileobj = self.myfileobj = builtins.open(
                        filename, mode or 'rb')
                if filename is None:
                    filename = getattr(fileobj, 'name', '')
                    if not isinstance(filename, (str, bytes)):
                        filename = ''
                self.mode = gzip.READ
                raw = _GeeZipReader(fileobj)
                self._buffer = io.BufferedReader(raw)
                self.name = filename
                self.fileobj = fileobj
                self._raw = raw
            else:
                super(GeeZipFile, self).__init__(filename, mode, fileobj)

        @property
        def member_offset(self):
            return self._raw.member_offset

except AttributeError:

    # this branch falls back to python 2.7+

    class GeeZipFile(gzip.GzipFile):
        """Extends gzip.GzipFile to remember self.member_offset, the raw
        file offset of the current gzip member."""

        def __init__(self, filename=None, mode=None,
                     compresslevel=9, fileobj=None, mtime=None):
            print('[DEBUG] GeeZipFile(gzip.GzipFile)')
            print('[DEBUG] => fileobj', fileobj)

            # ignore mtime for python 2.6
            gzip.GzipFile.__init__(self, filename=filename, mode=mode,
                                   compresslevel=compresslevel,
                                   fileobj=fileobj)

            self.member_offset = None
            print('[DEBUG] => member_offset', self.member_offset)

        # hook in to the place we seem to be able to reliably get the raw gzip
        # member offset
        def _read(self, size=1024):
            if self._new_member:
                try:
                    # works for python3.2
                    self.member_offset = (self.fileobj.tell()
                                          - self.fileobj._length
                                          + (self.fileobj._read or 0))
                except AttributeError:
                    # works for python2.7
                    self.member_offset = self.fileobj.tell()

            return gzip.GzipFile._read(self, size)
