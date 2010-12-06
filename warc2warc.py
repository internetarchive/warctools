#!/usr/bin/env python
"""warc2warc - convert one warc to another"""

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
parser.add_option("-I", "--input", dest="input_format")
parser.add_option("-L", "--log-level", dest="log_level")

parser.set_defaults(output_directory=None, limit=None, log_level="info")

def main(argv):
    (options, input_files) = parser.parse_args(args=argv[1:])

    out = sys.stdout
    if len(input_files) < 1:
        parser.error("no imput warc file(s)")
        
    for name in input_files:
        fh = WarcRecord.open_archive(name, gzip="auto")

        for record in fh:
            record.write_to(out)


        fh.close()



    return 0

if __name__ == '__main__':
    sys.exit(main(sys.argv))



