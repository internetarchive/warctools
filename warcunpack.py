#!/usr/bin/env python
"""warcextract - dump warc record context to standard out"""

import os
import sys

import sys
import os.path

from optparse import OptionParser
from contextlib import closing

from hanzo.warctools import ArchiveRecord, WarcRecord
from hanzo.httptools import RequestMessage, ResponseMessage

parser = OptionParser(usage="%prog [options] warc offset")

#parser.add_option("-l", "--limit", dest="limit")
parser.add_option("-o", "--output", dest="output")
parser.add_option("-l", "--log", dest="logfile")
parser.add_option("-L", "--log-level", dest="log_level")

parser.set_defaults(output=None, log_file=None, log_level="info")

def main(argv):
    (options, args) = parser.parse_args(args=argv[1:])

    out = sys.stdout
    if options.output:
        if not os.path.exists(options.output):
            os.makedirs(options.output)
        output_dir =  options.output
    else:
        output_dir  = os.getcwd()

    print options, args

    if len(args) < 1:
        # dump the first record on stdin
        with closing(WarcRecord.open_archive(file_handle=sys.stdin, gzip=None)) as fh:
            unpack_records(fh, output_dir)
        
    else:
        # dump a record from the filename, with optional offset
        for filename in args:
            print args
            with closing(ArchiveRecord.open_archive(filename=filename, gzip="auto")) as fh:
                print fh
                unpack_records(fh, output_dir)


    return 0

def unpack_records(fh, output_dir):
    for (offset, record, errors) in fh.read_records(limit=None):
        print >> sys.stderr, offset, record, record.content[0]
        if record:
            content_type, content = record.content

            if record.type == WarcRecord.WARCINFO:
                pass
            if record.type == WarcRecord.RESPONSE and content_type.startswith('application/http'):
                message = ResponseMessage(RequestMessage())
                remainder = message.feed(content)
                message.close()
                if remainder or not message.complete():
                    pass # error 

                header = message.header
                if 200 <= header.code < 300: 
                    print >>sys.stderr, 'writing', record.url,  header.headers

            
        elif errors:
            print >> sys.stderr, "warc errors at %s:%d"%(name, offset if offset else 0)
            for e in errors:
                print '\t', e


if __name__ == '__main__':
    sys.exit(main(sys.argv))



