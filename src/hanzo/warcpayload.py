#!/usr/bin/env python
"""warcpayload - extract payload from WARC record"""

import sys
from contextlib import closing

import click

from .warctools import WarcRecord

try:
    from http.client import HTTPResponse
except ImportError:
    from httplib import HTTPResponse  # type: ignore


def dump_payload_from_file(
    filename: str, offset: int | None = None, length: int | None = None
) -> None:
    """Dump payload from a WARC file at the specified offset."""
    with closing(
        WarcRecord.open_archive(filename=filename, gzip="auto", offset=offset, length=length)
    ) as fh:
        dump_payload_from_stream(fh, filename)


def dump_payload_from_stream(fh, name: str = "-") -> None:
    """Dump payload from a WARC stream."""
    out = sys.stdout.buffer

    for offset, record, errors in fh.read_records(limit=1, offsets=False):
        if record:
            if record.type == WarcRecord.RESPONSE and record.content_type.startswith(
                b"application/http"
            ):
                f = FileHTTPResponse(record.content_file)
                f.begin()
            else:
                f = record.content_file

            buf = f.read(8192)
            while buf != b"":
                out.write(buf)
                buf = f.read(8192)

        elif errors:
            print(
                f"warc errors at {name}:{offset if offset else 0}",
                file=sys.stderr,
            )
            for e in errors:
                print("\t", e, file=sys.stderr)


class FileHTTPResponse(HTTPResponse):
    """HTTPResponse subclass that reads from the supplied fileobj instead of
    from a socket."""

    def __init__(self, fileobj, debuglevel=0, strict=0, method=None, buffering=False):
        self.fp = fileobj

        # We can't call HTTPResponse.__init__(self, ...) because it will try to
        # call sock.makefile() and we have no sock. So we have to copy and
        # paste the rest of the constructor below.

        self.debuglevel = debuglevel
        self.strict = strict
        self._method = method

        self.headers = self.msg = None

        # from the Status-Line of the response
        self.version = "UNKNOWN"  # HTTP-Version
        self.status = "UNKNOWN"  # Status-Code
        self.reason = "UNKNOWN"  # Reason-Phrase

        self.chunked = "UNKNOWN"  # is "chunked" being used?
        self.chunk_left = "UNKNOWN"  # bytes left to read in current chunk
        self.length = "UNKNOWN"  # number of bytes left in response
        self.will_close = "UNKNOWN"  # conn will close at end of response


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.argument("warc_offset", required=True)
def main(warc_offset: str) -> None:
    """Extract payload from WARC record at specified offset.

    WARC_OFFSET format: filename:offset or filename:offset,length
    """
    filename, offset_str = warc_offset.rsplit(":", 1)
    if "," in offset_str:
        offset, length = [int(n) for n in offset_str.split(",", 1)]
    else:
        offset = int(offset_str)
        length = None  # unknown

    dump_payload_from_file(filename, offset, length)


def run() -> None:
    """Entry point for the command-line interface."""
    main()


if __name__ == "__main__":
    run()
