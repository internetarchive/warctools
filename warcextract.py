#!/usr/bin/env python
"""warcextract - dump warc record context to standard out"""

import os
import sys

import sys
import os.path

from optparse import OptionParser
from contextlib import closing

from hanzo.warctools import ArchiveRecord, WarcRecord

parser = OptionParser(usage="%prog [options] warc offset")

#parser.add_option("-l", "--limit", dest="limit")
parser.add_option("-I", "--input", dest="input_format")
parser.add_option("-L", "--log-level", dest="log_level")

parser.set_defaults(output_directory=None, limit=None, log_level="info")

def main(argv):
    (options, args) = parser.parse_args(args=argv[1:])

    out = sys.stdout
    if len(args) < 1:
        # dump the first record on stdin
        with closing(WarcRecord.open_archive(file_handle=sys.stdin, gzip=None)) as fh:
            dump_record(fh)
        
    else:
        # dump a record from the filename, with optional offset
        filename = args[0]
        if len(args) > 1:
            offset = int(args[1])
        else:
            offset = 0

        with closing(ArchiveRecord.open_archive(filename=filename, gzip="auto")) as fh:
            fh.seek(offset)
            dump_record(fh)


    return 0

def dump_record(fh):
    for (offset, record, errors) in fh.read_records(limit=1, offsets=False):
        if record:
            sys.stdout.write(record.content[1])
        elif errors:
            print >> sys.stderr, "warc errors at %s:%d"%(name, offset if offset else 0)
            for e in errors:
                print '\t', e
        break # only use one (I'm terrible)


if __name__ == '__main__':
    sys.exit(main(sys.argv))



