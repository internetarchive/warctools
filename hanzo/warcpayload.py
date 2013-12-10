#!/usr/bin/env python

from __future__ import print_function

import os
import sys
try:
    from http.client import HTTPResponse
except ImportError:
    from httplib import HTTPResponse


from optparse import OptionParser
from contextlib import closing

from .warctools import WarcRecord

parser = OptionParser(usage="%prog warc:offset")

parser.set_defaults(output_directory=None, limit=None, log_level="info")

def main(argv):
    (options, args) = parser.parse_args(args=argv[1:])

    filename, offset = args[0].rsplit(':',1)
    if ',' in offset:
        offset, length = [int(n) for n in offset.split(',',1)]
    else:
        offset = int(offset)
        length = None # unknown

    dump_payload_from_file(filename, offset, length)

def dump_payload_from_file(filename, offset=None, length=None):
    with closing(WarcRecord.open_archive(filename=filename, gzip="auto", offset=offset, length=length)) as fh:
        return dump_payload_from_stream(fh)

def dump_payload_from_stream(fh):
    try: # python3
        out = sys.stdout.buffer
    except AttributeError: # python2
        out = sys.stdout

    for (offset, record, errors) in fh.read_records(limit=1, offsets=False):
        if record:
            if (record.type == WarcRecord.RESPONSE 
                    and record.content_type.startswith(b'application/http')):
                f = FileHTTPResponse(record.content_file)
                f.begin()
            else:
                f = record.content_file

            buf = f.read(8192) 
            while buf != b'':
                out.write(buf)
                buf = f.read(8192)

        elif errors:
            print("warc errors at %s:%d"%(name, offset if offset else 0), file=sys.stderr)
            for e in errors:
                print('\t', e)

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
        self.version = 'UNKNOWN' # HTTP-Version
        self.status = 'UNKNOWN'  # Status-Code
        self.reason = 'UNKNOWN'  # Reason-Phrase

        self.chunked = 'UNKNOWN'         # is "chunked" being used?
        self.chunk_left = 'UNKNOWN'      # bytes left to read in current chunk
        self.length = 'UNKNOWN'          # number of bytes left in response
        self.will_close = 'UNKNOWN'      # conn will close at end of response


def run():
    sys.exit(main(sys.argv))


if __name__ == '__main__':  
    run()


