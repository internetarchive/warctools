#!/usr/bin/env python

import os
import sys

import sys
import os.path
from cStringIO import StringIO

from optparse import OptionParser
from contextlib import closing

from .warctools import WarcRecord
from .httptools import RequestMessage, ResponseMessage

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
    for (offset, record, errors) in fh.read_records(limit=3, offsets=False):
        if record:
            # if record.type == WarcRecord.RESPONSE and content_type.startswith('application/http'):
            #     content = parse_http_response(record)
            bytes_read = 0
            content_length = int(record.get_header(WarcRecord.CONTENT_LENGTH))
            while bytes_read < content_length:
                read_size = min(65536, content_length - bytes_read)
                buf = record.content_file.read(read_size)
                sys.stdout.write(buf)
                bytes_read += len(buf)
                if len(buf) < read_size:
                    raise Exception('content length mismatch (is, claims)',
                                 bytes_read, content_length)
                    break

        elif errors:
            print >> sys.stderr, "warc errors at %s:%d"%(name, offset if offset else 0)
            for e in errors:
                print '\t', e

def parse_http_response(record):
    message = ResponseMessage(RequestMessage())
    remainder = message.feed(record.content[1])
    message.close()
    if remainder or not message.complete():
        if remainder:
            print >>sys.stderr, 'warning: trailing data in http response for', record.url
        if not message.complete():
            print >>sys.stderr, 'warning: truncated http response for', record.url

    return message.get_body()



def run():
    sys.exit(main(sys.argv))


if __name__ == '__main__':  
    run()


