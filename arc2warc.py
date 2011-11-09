#!/usr/bin/env python
"""arc2warc - convert one arc to a new warc"""

import os
import sys
import hashlib
import uuid

import sys
import os.path
import datetime


from optparse import OptionParser

from hanzo.warctools import ArcRecord,WarcRecord
from hanzo.warctools.warc import warc_datetime_str

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

        filedesc = None
        for record in fh:

            # todo :convert filedesc record to warcinfo record
            #       add arc filename to warcinfo record, from filedesc
            #       add warc conversion record with original filedesc

            #       add warcinfo field to every warc after a warcinfo record
            #       i.e. deal with concatted arc files
            #       documentation, options

            # corrent content types - http for urlsinherit content-type for everything else?

            content_type, content  = record.content

            if record.url.startswith('http'):
                content_type="application/http;msgtype=response"
            headers = [
                (WarcRecord.TYPE, WarcRecord.METADATA if record.type == 'filedesc' else WarcRecord.RESPONSE ),
                (WarcRecord.ID, "<urn:uuid:%s>"%uuid.UUID(hashlib.sha1(record.url+record.date).hexdigest()[0:32])),
            ]
            version = "WARC/1.0"

            url = record.url
            if url:
                headers.append((WarcRecord.URL,url))
            date = record.date
            if date:
                date = datetime.datetime.strptime(date,'%Y%m%d%H%M%S')
                headers.append((WarcRecord.DATE, warc_datetime_str(date)))
            
            warcrecord = WarcRecord(headers=headers, content=(content_type, content), version=version)

            warcrecord.write_to(out, gzip=options.gzip)


        fh.close()



    return 0

if __name__ == '__main__':
    sys.exit(main(sys.argv))



