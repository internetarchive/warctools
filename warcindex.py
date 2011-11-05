#!/usr/bin/env python
"""warcindex - dump warc index"""

import os
import sys

import sys
import os.path

from optparse import OptionParser

from hanzo.warctools import ArchiveRecord

parser = OptionParser(usage="%prog [options] warc warc warc")

parser.add_option("-l", "--limit", dest="limit")
parser.add_option("-O", "--output-format", dest="output_format", help="output format (ignored)")
parser.add_option("-o", "--output", dest="output_format", help="output file (ignored)")

parser.add_option("-L", "--log-level", dest="log_level")

parser.set_defaults(output=None, limit=None, log_level="info")

def main(argv):
    (options, input_files) = parser.parse_args(args=argv[1:])

    out = sys.stdout
    if len(input_files) < 1:
        parser.error("no imput warc file(s)")
        
    print '#WARC filename offset warc-type warc-subject-uri warc-record-id content-type content-length'
    for name in input_files:
        fh = ArchiveRecord.open_archive(name, gzip="auto")

        for (offset, record, errors) in fh.read_records(limit=None):
            if record:
                print name, offset, record.type, record.url, record.id, record.content_type, record.content_length
            elif errors:
                pass
                # ignore
            else:
                pass
                # no errors at tail




        fh.close()



    return 0

if __name__ == '__main__':
    sys.exit(main(sys.argv))



