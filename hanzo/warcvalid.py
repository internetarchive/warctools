#!/usr/bin/env python
"""warcvalid - check a warc is ok"""

import os
import sys

import sys
import os.path

from optparse import OptionParser

from .warctools import WarcRecord, expand_files

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
        

    correct=True
    fh=None
    try:
        for name in expand_files(input_files):
            fh = WarcRecord.open_archive(name, gzip="auto")

            for (offset, record, errors) in fh.read_records(limit=None):
                if errors:
                    print  >> sys.stderr, "warc errors at %s:%d"%(name, offset)
                    print >> sys.stderr,  errors
                    correct=False

                    break
                elif record is not None and record.validate(): # ugh name, returns errorsa
                    print  >> sys.stderr, "warc errors at %s:%d"%(name, offset)
                    print >> sys.stderr, record.validate()
                    correct=False
                    break
                

    except StandardError, e:
        correct=False
    finally:
        if fh: fh.close()
    
    if correct:
        return 0
    else:
        return -1 # failure code


def run():
    sys.exit(main(sys.argv))


if __name__ == '__main__':  
    run()


