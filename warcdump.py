#!/usr/bin/env python
"""warcdump - dump warcs in a slightly more humane format"""

import os
import sys

import sys
import os.path

from optparse import OptionParser

from hanzo.warctools import ArchiveRecord, WarcRecord

parser = OptionParser(usage="%prog [options] warc warc warc")

parser.add_option("-l", "--limit", dest="limit")
parser.add_option("-I", "--input", dest="input_format")
parser.add_option("-L", "--log-level", dest="log_level")

parser.set_defaults(output_directory=None, limit=None, log_level="info")

def main(argv):
    (options, input_files) = parser.parse_args(args=argv[1:])

    out = sys.stdout
    if len(input_files) < 1:
        dump_archive(WarcRecord.open_archive(file_handle=sys.stdin, gzip=None), name="-",offsets=False)
        
    else:
        for name in input_files:
            fh = ArchiveRecord.open_archive(name, gzip="auto")
            dump_archive(fh,name)

            fh.close()


    return 0

def dump_archive(fh, name, offsets=True):
    for (offset, record, errors) in fh.read_records(limit=None, offsets=offsets):
        if record:
            print "archive record at %s:%s"%(name,offset)
            record.dump(content=True)
        elif errors:
            print "warc errors at %s:%d"%(name, offset if offset else 0)
            for e in errors:
                print '\t', e
        else:
            print
            print 'note: no errors encountered in tail of file'


if __name__ == '__main__':
    sys.exit(main(sys.argv))



