#!/usr/bin/env python
"""warcindex - dump warc index"""

import os
import sys

import sys
import os.path

from optparse import OptionParser

from .warctools import WarcRecord, expand_files

parser = OptionParser(usage="%prog [options] warc warc warc")

parser.add_option("-l", "--limit", dest="limit")
parser.add_option("-O", "--output-format", dest="output_format", help="output format (ignored)")
parser.add_option("-o", "--output", dest="output_format", help="output file (ignored)")

parser.add_option("-L", "--log-level", dest="log_level")

parser.set_defaults(output=None, limit=None, log_level="info")

def main(argv):
    (options, input_files) = parser.parse_args(args=argv[1:])

    try: # python3
        out = sys.stdout.buffer
    except AttributeError: # python2
        out = sys.stdout

    if len(input_files) < 1:
        parser.error("no imput warc file(s)")
        
    out.write(b'#WARC filename offset warc-type warc-subject-uri warc-record-id content-type content-length\n')
    for name in expand_files(input_files):
        fh = WarcRecord.open_archive(name, gzip="auto")

        try:
            for (offset, record, errors) in fh.read_records(limit=None):
                if record:
                    fields = [name.encode('utf-8'), 
                            str(offset).encode('utf-8'),
                            record.type or b'-', 
                            record.url or b'-', 
                            record.id or b'-', 
                            record.content_type or b'-',
                            str(record.content_length).encode('utf-8')]
                    out.write(b' '.join(fields) + b'\n')
                elif errors:
                    pass
                    # ignore
                else:
                    pass
                    # no errors at tail

        finally:
            fh.close()

    return 0


def run():
    sys.exit(main(sys.argv))


if __name__ == '__main__':  
    run()


