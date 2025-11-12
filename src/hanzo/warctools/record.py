"""Base classes for archive records (WARC and ARC formats).

WARC Format Specification References:
- WARC 1.1 Annotated (primary): https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/
- File and record model: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#file-and-record-model
"""

import re
from gzip import GzipFile

from hanzo.warctools.stream import open_record_stream

strip = re.compile(rb"[^\w\t \|\\\/]")


def add_headers(**kwargs):
    """Decorator helper for defining header name constants in record formats.

    This decorator sets class attributes for header names and maintains
    a list of header names in the _HEADERS attribute.

    Args:
        **kwargs: Header name to constant value mappings (e.g., TYPE=b"WARC-Type")

    Returns:
        Decorator function that adds header constants to a class

    Example:
        @add_headers(
            TYPE=b"WARC-Type",
            DATE=b"WARC-Date",
        )
        class WarcRecord(ArchiveRecord):
            pass
    """

    def _add_headers(cls):
        for k, v in kwargs.items():
            setattr(cls, k, v)
        cls._HEADERS = list(kwargs.keys())
        return cls

    return _add_headers


class ArchiveParser:
    """Base class for archive record parsers.

    Parsers read archive records from streams and return record objects.
    Subclasses must implement the parse() method.

    See:
        WARC 1.1 Section 4: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#file-and-record-model
    """

    pass


@add_headers(
    DATE=b"Date",
    CONTENT_TYPE=b"Type",
    CONTENT_LENGTH=b"Length",
    TYPE=b"Type",
    URL=b"Url",
)
class ArchiveRecord:
    """An archive record has some headers, maybe some content and
    a list of errors encountered. record.headers is a list of tuples (name,
    value). errors is a list, and content is a tuple of (type, data)"""

    # pylint: disable-msg=e1101

    def __init__(self, headers=None, content=None, errors=None):
        self.headers = headers if headers else []
        self.errors = errors if errors else []
        self._content = content

    HEADERS = staticmethod(add_headers)

    @property
    def date(self):
        return self.get_header(self.DATE)

    def error(self, *args):
        self.errors.append(args)

    @property
    def type(self):
        return self.get_header(self.TYPE)

    @property
    def content_file(self):
        """
        File handle for streaming the payload.

        If the record has been read from a RecordStream, content_file wraps the
        same underlying file handle as the RecordStream itself. This has
        important implications. Results are undefined if you try to read from
        content_file after reading the next record from RecordStream; and
        closing content_file will close the RecordStream, and vice versa.
        But if you avoid these caveats, content_file takes care to bound itself
        within the content-length specified in the warc record, so that reading
        to the end of content_file will bring you only to the end of the
        record's payload.

        When creating a record for writing and supplying content_file, the
        record can only be written once, since writing the record entails
        reading content_file and advancing the file position. Subsequent
        attempts to write using content_file will throw an exception.
        """
        return self._content_file

    @content_file.setter
    def content_file(self, fh):
        self._content_file = fh
        self._content_file_valid = fh is not None

    @property
    def content(self):
        """A tuple (content_type, content). When first referenced, content[0]
        is populated from the Content-Type header, and content[1] by reading
        self.content_file."""
        if self._content is None:
            content_type = self.get_header(self.CONTENT_TYPE)
            try:
                content = self.content_file.read()
                self._content = (content_type, content)
            finally:
                self.content_file = None

        return self._content

    @property
    def content_type(self):
        """If self.content tuple was supplied, or has already been snarfed, or
        we don't have a Content-Type header, return self.content[0]. Otherwise,
        return the value of the Content-Type header."""
        if self._content is None:
            content_type = self.get_header(self.CONTENT_TYPE)
            if content_type is not None:
                return content_type

        return self.content[0]

    @property
    def content_length(self):
        """Get Content-Length header value.

        If self.content tuple was supplied, or has already been snarfed, or
        we don't have a Content-Length header, return len(self.content[1]).
        Otherwise, return the value of the Content-Length header.

        See WARC 1.1 Section 5.5: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#content-length
        """
        if self._content is None:
            content_length = self.get_header(self.CONTENT_LENGTH)
            if content_length is not None:
                return int(content_length)

        return len(self.content[1])

    @property
    def url(self):
        return self.get_header(self.URL)

    def get_header(self, name):
        """Returns value of first header found matching name, case insensitively.

        Field names are case-insensitive per WARC 1.1 Section 4.
        https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#file-and-record-model

        Args:
            name: Header name to search for (bytes)

        Returns:
            bytes or None: Header value if found, None otherwise
        """
        for k, v in self.headers:
            if name.lower() == k.lower():
                return v

    def get_all_headers(self, name):
        """Returns all header values matching name, case insensitively.

        Some WARC fields may appear multiple times (e.g., WARC-Concurrent-To).
        This method returns all matching values.

        Args:
            name: Header name to search for (bytes)

        Returns:
            list: List of header values (bytes), empty list if none found

        See:
            WARC 1.1 Section 5.7: WARC-Concurrent-To may be repeated
            https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#warc-concurrent-to
        """
        values = []
        for k, v in self.headers:
            if name.lower() == k.lower():
                values.append(v)
        return values

    def set_header(self, name, value):
        self.headers = [(k, v) for (k, v) in self.headers if k != name]
        self.headers.append((name, value))

    def dump(self, content=True):
        print("Headers:")
        for h, v in self.headers:
            print("\t{}:{}".format(h.decode("latin1"), v.decode("latin1")))
        if content and self.content:
            print("Content Headers:")
            content_type, content_body = self.content
            print(
                "\t" + self.CONTENT_TYPE.decode("latin1"),
                ":",
                content_type.decode("latin1"),
            )
            print("\t" + self.CONTENT_LENGTH.decode("latin1"), ":", len(content_body))
            print("Content:")
            ln = min(1024, len(content_body))
            abbr_strp_content = strip.sub(
                lambda x: (f"\\x{ord(x.group()):0X}").encode("ascii"),
                content_body[:ln],
            )
            print("\t" + abbr_strp_content.decode("ascii"))
            print("\t...")
            print()
        else:
            print("Content: none")
            print()
            print()
        if self.errors:
            print("Errors:")
            for e in self.errors:
                print("\t" + e)

    def write_to(self, out, newline=b"\x0d\x0a", gzip=False):
        if self.content_file is not None:
            if not self._content_file_valid:
                raise Exception("cannot write record because content_file has already been used")

        if gzip:
            if hasattr(out, "mode"):
                out = GzipFile(fileobj=out)
            else:
                out = GzipFile(fileobj=out, mode="ab")

        self._write_to(out, newline)

        if gzip:
            out.flush()
            out.close()

        if self.content_file is not None:
            self._content_file_valid = False

    def _write_to(self, out, newline):
        raise AssertionError("this is bad")

    ### class methods for parsing
    @classmethod
    def open_archive(
        cls,
        filename=None,
        file_handle=None,
        mode="rb",
        gzip="auto",
        offset=None,
        length=None,
    ):
        """Generically open an archive - magic autodetect"""
        if cls is ArchiveRecord:
            cls = None  # means guess
        return open_record_stream(cls, filename, file_handle, mode, gzip, offset, length)

    @classmethod
    def make_parser(cls):
        """Reads a (w)arc record from the stream, returns a tuple (record,
        errors).  Either records is null or errors is null. Any
        record-specific errors are contained in the record - errors is only
        used when *nothing* could be parsed"""
        raise Exception()
