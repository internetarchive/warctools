#!/usr/bin/env python
"""warcdump - dump warcs in a slightly more humane format"""

import os
import sys

import sys
import os.path

from optparse import OptionParser

from warctools import ArchiveRecord

parser = OptionParser(usage="%prog [options] warc warc warc")

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
        fh = ArchiveRecord.open_archive(name, gzip="auto")

        for (offset, record, errors) in fh.read_records(limit=None):
            if record:
                print "archive record at %s:%d"%(name,offset)
                record.dump(content=True)
            elif errors:
                print "warc errors at %s:%d"%(name, offset)
                for e in errors:
                    print '\t', e
            else:
                print
                print 'note: no errors encountered in tail of file'




        fh.close()



    return 0

if __name__ == '__main__':
    sys.exit(main(sys.argv))



