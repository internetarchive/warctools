#!/usr/bin/env python
"""warcextract - dump warc record context to standard out"""

from __future__ import print_function

import os
import sys

import sys
import os.path

from optparse import OptionParser
from contextlib import closing

from .warctools import WarcRecord

parser = OptionParser(usage="%prog [options] warc offset")

#parser.add_option("-l", "--limit", dest="limit")
parser.add_option("-I", "--input", dest="input_format")
parser.add_option("-L", "--log-level", dest="log_level")

parser.set_defaults(output_directory=None, limit=None, log_level="info")

def main(argv):
    (options, args) = parser.parse_args(args=argv[1:])

    try: # python3
        out = sys.stdout.buffer
    except AttributeError: # python2
        out = sys.stdout

    if len(args) < 1:
        # dump the first record on stdin
        with closing(WarcRecord.open_archive(file_handle=sys.stdin, gzip=None)) as fh:
            dump_record(fh, out)
        
    else:
        # dump a record from the filename, with optional offset
        filename = args[0]
        if len(args) > 1:
            offset = int(args[1])
        else:
            offset = 0

        with closing(WarcRecord.open_archive(filename=filename, gzip="auto")) as fh:
            fh.seek(offset)
            dump_record(fh, out)


    return 0

def dump_record(fh, out):
    for (offset, record, errors) in fh.read_records(limit=1, offsets=False):
        if record:
            out.write(record.content[1])
        elif errors:
            print("warc errors at %s:%d"%(name, offset if offset else 0), file=sys.stderr)
            for e in errors:
                print('\t', e)
        break # only use one (I'm terrible)


def run():
    sys.exit(main(sys.argv))


if __name__ == '__main__':  
    run()


