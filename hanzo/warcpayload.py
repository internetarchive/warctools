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

    out = sys.stdout

    filename, offset = args[0].rsplit(':',1)
    if ',' in offset:
        offset, length = [int(n) for n in offset.split(',',1)]
    else:
        offset = int(offset)
        length = None # unknown

    payload = extract_payload_from_file(filename, offset, length)
    out.write(payload)

def extract_payload_from_str(contents, gzip="record"):
    with closing(StringIO(contents)) as stream, closing(WarcRecord.open_archive(file_handle=stream, gzip=gzip)) as fh:
        return extract_payload_from_stream(fh)

def extract_payload_from_file(filename, offset=None, length=None):
    with closing(WarcRecord.open_archive(filename=filename, gzip="auto", offset=offset, length=length)) as fh:
        return extract_payload_from_stream(fh)

def extract_payload_from_stream(fh):
    content = ""
    for (offset, record, errors) in fh.read_records(limit=1, offsets=False):
        if record:
            content_type, content = record.content
            if record.type == WarcRecord.RESPONSE and content_type.startswith('application/http'):
                content = parse_http_response(record)
        elif errors:
            print >> sys.stderr, "warc errors at %s:%d"%(name, offset if offset else 0)
            for e in errors:
                print '\t', e

        return content

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


