#!/usr/bin/env python
"""warc2warc - convert one warc to another, can be used to re-compress things"""

import os
import sys

import sys
import os.path

from optparse import OptionParser

from warctools import WarcRecord

parser = OptionParser(usage="%prog [options] url (url ...)")

parser.add_option("-o", "--output", dest="output",
                       help="output warc file")
parser.add_option("-l", "--limit", dest="limit")
parser.add_option("-I", "--input", dest="input_format (ignored)")
parser.add_option("-Z", "--gzip", dest="gzip", action="store_true", help="compress output")
parser.add_option("-L", "--log-level", dest="log_level")

parser.set_defaults(output_directory=None, limit=None, log_level="info", gzip=False)

def main(argv):
    (options, input_files) = parser.parse_args(args=argv[1:])

    out = sys.stdout
    if len(input_files) < 1:
        fh = WarcRecord.open_archive(file_handle=sys.stdin, gzip=None)

        for record in fh:
            record.write_to(out, gzip=options.gzip)
    else:
        for name in input_files:
            fh = WarcRecord.open_archive(name, gzip="auto")

            for record in fh:
                record.write_to(out, gzip=options.gzip)


            fh.close()



    return 0

if __name__ == '__main__':
    sys.exit(main(sys.argv))



