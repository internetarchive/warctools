#!/usr/bin/env python
"""warc2warc - convert one warc to another, can be used to re-compress things"""

import os
import sys

import sys
import os.path

from optparse import OptionParser

from hanzo.warctools import WarcRecord
from hanzo.httptools import RequestMessage, ResponseMessage

parser = OptionParser(usage="%prog [options] url (url ...)")

parser.add_option("-o", "--output", dest="output",
                       help="output warc file")
parser.add_option("-l", "--limit", dest="limit")
parser.add_option("-I", "--input", dest="input_format", help="(ignored)")
parser.add_option("-Z", "--gzip", dest="gzip", action="store_true", help="compress output")
parser.add_option("-D", "--decode_http", dest="decode_http", action="store_true", help="decode http messages (strip chunks, gzip)")
parser.add_option("-L", "--log-level", dest="log_level")

parser.set_defaults(output_directory=None, limit=None, log_level="info", gzip=False, decode_http=False)

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
                if options.decode_http:
                    if record.type == WarcRecord.RESPONSE:
                        content_type, content = record.content
                        message = None
                        if content_type == ResponseMessage.CONTENT_TYPE:
                            # technically, a http request needs to know the request to be parsed
                            # because responses to head requests don't have a body.
                            # we assume we don't store 'head' responses, and plough on 
                            message = ResponseMessage(RequestMessage())
                        if content_type == RequestMessage.CONTENT_TYPE:
                            message = RequestMessage()

                        if message:
                            message.feed(content)
                            content = message.get_decoded_message()
                            record.content = content_type, content

                record.write_to(out, gzip=options.gzip)


            fh.close()



    return 0

if __name__ == '__main__':
    sys.exit(main(sys.argv))



