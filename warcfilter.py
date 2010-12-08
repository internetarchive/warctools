#!/usr/bin/env python
"""warcfilter - prints warcs in that match regexp"""

import os
import sys

import re

from optparse import OptionParser

from warctools import ArchiveRecord

parser = OptionParser(usage="%prog [options] pattern warc warc warc")

parser.add_option("-l", "--limit", dest="limit")
parser.add_option("-I", "--input", dest="input_format")
parser.add_option("-i", "--invert", dest="invert",action="store_true")
parser.add_option("-L", "--log-level", dest="log_level")

parser.set_defaults(output_directory=None, limit=None, log_level="info", invert=None)

def main(argv):
    (options, input_files) = parser.parse_args(args=argv[1:])

    out = sys.stdout
    if len(input_files) < 2:
        parser.error("no imput warc file(s)")
        
    pattern, input_files = input_files[0], input_files[1:]

    pattern = re.compile(pattern)
    for name in input_files:
        fh = ArchiveRecord.open_archive(name, gzip="auto")

        for record in fh:
            for name, value in record.headers:
                if pattern.search(value):
                    record.write_to(out)
        fh.close()



    return 0

if __name__ == '__main__':
    sys.exit(main(sys.argv))



