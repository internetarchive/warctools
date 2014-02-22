#!/usr/bin/env python
"""warcfilter - prints warcs in that match regexp, by default searches all headers"""

import os
import sys

import re

from optparse import OptionParser

from .warctools import WarcRecord, expand_files
from .httptools import RequestMessage, ResponseMessage

parser = OptionParser(usage="%prog [options] pattern warc warc warc")

parser.add_option("-l", "--limit", dest="limit", help="limit (ignored)")
parser.add_option("-I", "--input", dest="input_format", help="input format (ignored)")
parser.add_option("-i", "--invert", dest="invert",action="store_true", help="invert match")
parser.add_option("-U", "--url", dest="url",action="store_true", help="match on url")
parser.add_option("-T", "--type", dest="type",action="store_true", help="match on (warc) record type")
parser.add_option("-C", "--content-type", dest="content_type",action="store_true", help="match on (warc) record content type")
parser.add_option("-H", "--http-content-type", dest="http_content_type",action="store_true", help="match on http payload content type")
parser.add_option("-D", "--warc-date", dest="warc_date",action="store_true", help="match on WARC-Date header")
parser.add_option("-L", "--log-level", dest="log_level", help="log level(ignored)")

parser.set_defaults(output_directory=None, limit=None, log_level="info", invert=False, url=None, content_type=None, type=None)

def parse_http_response(record):
    message = ResponseMessage(RequestMessage())
    remainder = message.feed(record.content[1])
    message.close()
    if remainder or not message.complete():
        if remainder:
            logging.warning('trailing data in http response for %s'% record.url)
        if not message.complete():
            logging.warning('truncated http response for %s'%record.url)

    header = message.header

    mime_type = [v for k,v in header.headers if k.lower() == b'content-type']
    if mime_type:
        mime_type = mime_type[0].split(b';')[0]
    else:
        mime_type = None

    return header.code, mime_type, message

def main(argv):
    (options, input_files) = parser.parse_args(args=argv[1:])

    try: # python3
        out = sys.stdout.buffer
    except AttributeError: # python2
        out = sys.stdout

    if len(input_files) < 1:
        parser.error("no pattern")

        
    pattern, input_files = input_files[0].encode(), input_files[1:]


    invert = options.invert
    pattern = re.compile(pattern)
    if not input_files:
            fh = WarcRecord.open_archive(file_handle=sys.stdin, gzip=None)
            filter_archive(fh, options, pattern, out)
    else:
        for name in expand_files(input_files):
            fh = WarcRecord.open_archive(name, gzip="auto")
            filter_archive(fh, options, pattern,out)
            fh.close()



    return 0

def filter_archive(fh, options, pattern, out):
        invert = options.invert
        for record in fh:
            if options.url:
                if bool(record.url and pattern.search(record.url)) ^ invert :
                    record.write_to(out)

            elif options.type:
                if bool(record.type and pattern.search(record.type)) ^ invert:
                    record.write_to(out)

            elif options.content_type:
                if bool(record.content_type and pattern.search(record.content_type)) ^ invert:
                    record.write_to(out)

            elif options.http_content_type:
                if record.type == WarcRecord.RESPONSE and record.content_type.startswith(b'application/http'):
                    code, content_type, message = parse_http_response(record)

                    if bool(content_type and pattern.search(content_type)) ^ invert:
                        record.write_to(out)

            elif options.warc_date:
                if bool(record.date and pattern.search(record.date)) ^ invert:
                    record.write_to(out)

            else:
                found = False
                for name, value in record.headers:
                    if pattern.search(value):
                        found = True
                        break

                content_type, content = record.content
                if not found:
                    found = bool(pattern.search(content))
                        

                if found ^ invert:
                    record.write_to(out)


def run():
    sys.exit(main(sys.argv))


if __name__ == '__main__':  
    run()


