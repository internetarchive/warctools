#!/usr/bin/env python
"""warcfilter - prints warcs in that match regexp, by default searches all headers"""

import os
import sys

import re

from optparse import OptionParser

from hanzo.warctools import ArchiveRecord

parser = OptionParser(usage="%prog [options] pattern warc warc warc")

parser.add_option("-l", "--limit", dest="limit", help="limit (ignored)")
parser.add_option("-I", "--input", dest="input_format", help="input format (ignored)")
parser.add_option("-i", "--invert", dest="invert",action="store_true", help="invert match")
parser.add_option("-U", "--url", dest="url",action="store_true", help="match on url")
parser.add_option("-T", "--type", dest="type",action="store_true", help="match on (warc) record type")
parser.add_option("-C", "--content-type", dest="content_type",action="store_true", help="match on (warc) record type")
parser.add_option("-L", "--log-level", dest="log_level", help="log level(ignored)")

parser.set_defaults(output_directory=None, limit=None, log_level="info", invert=False, url=None, content_type=None, type=None)

def main(argv):
    (options, input_files) = parser.parse_args(args=argv[1:])

    out = sys.stdout
    if len(input_files) < 1:
        parser.error("no pattern")

        
    pattern, input_files = input_files[0], input_files[1:]


    invert = options.invert
    out = sys.stdout
    pattern = re.compile(pattern)
    if not input_files:
            fh = ArchiveRecord.open_archive(file_handle=sys.stdin, gzip=None)
            filter_archive(fh, options, pattern, out)
    else:
        for name in input_files:
            fh = ArchiveRecord.open_archive(name, gzip="auto")
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

            else:
                found = False
                for name, value in record.headers:
                    if pattern.search(value):
                        found = True
                        break

                if found ^ invert:
                    record.write_to(out)

if __name__ == '__main__':
    sys.exit(main(sys.argv))



