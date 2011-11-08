#!/usr/bin/env python
"""arc2warc - convert one arc to a new warc"""

import os
import sys
import hashlib
import uuid

import sys
import os.path


from optparse import OptionParser

from hanzo.warctools import ArcRecord,WarcRecord

parser = OptionParser(usage="%prog [options] arc (arc ...)")

parser.add_option("-o", "--output", dest="output",
                       help="output warc file")
parser.add_option("-l", "--limit", dest="limit")
parser.add_option("-Z", "--gzip", dest="gzip", action="store_true", help="compress")
parser.add_option("-L", "--log-level", dest="log_level")

parser.set_defaults(output_directory=None, limit=None, log_level="info", gzip=False)

def main(argv):
    (options, input_files) = parser.parse_args(args=argv[1:])

    out = sys.stdout
    if options.output:
        out = open(options.output, 'ab')
    if len(input_files) < 1:
        parser.error("no imput warc file(s)")
        
    for name in input_files:
        fh = ArcRecord.open_archive(name, gzip="auto")

        for record in fh:
            content = record.content
            headers = [
                (WarcRecord.TYPE, "response"),
                (WarcRecord.ID, "<urn:uuid:%s>"%uuid.UUID(hashlib.sha1(record.url+record.date).hexdigest()[0:32])),
            ]
            version = "WARC/1.0"

            url = record.url
            if url:
                headers.append((WarcRecord.URL,url))
            date = record.date
            if date:
                headers.append((WarcRecord.DATE,date))
            
            warcrecord = WarcRecord(headers=headers, content="application/http;msgtype=response", version=version)

            warcrecord.write_to(out, gzip=options.gzip)


        fh.close()



    return 0

if __name__ == '__main__':
    sys.exit(main(sys.argv))



