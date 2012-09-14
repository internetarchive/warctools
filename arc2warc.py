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

from hanzo.warctools import ArcRecord,WarcRecord, MixedRecord
from hanzo.warctools.warc import warc_datetime_str

parser = OptionParser(usage="%prog [options] arc (arc ...)")

parser.add_option("-o", "--output", dest="output",
                       help="output warc file")
parser.add_option("-l", "--limit", dest="limit")
parser.add_option("-Z", "--gzip", dest="gzip", action="store_true", help="compress")
parser.add_option("-L", "--log-level", dest="log_level")

parser.set_defaults(output_directory=None, limit=None, log_level="info", gzip=False)

def make_warc_uuid(text):
    return "<urn:uuid:%s>"%uuid.UUID(hashlib.sha1(text).hexdigest()[0:32])


class ArcTransformer(object):
    def __init__(self):
        self.warcinfo_id = None

    def convert(self, record):

        version = "WARC/1.0"

        #print >> sys.stderr, record.headers

        warc_id = make_warc_uuid(record.url+record.date)
        headers = [
            (WarcRecord.ID, warc_id),
        ]
        if record.date:
            date = datetime.datetime.strptime(record.date,'%Y%m%d%H%M%S')
            headers.append((WarcRecord.DATE, warc_datetime_str(date)))


        if record.type == 'filedesc':
            self.warcinfo_id = warc_id

            warcinfo_headers = list(headers)
            warcinfo_headers.append((WarcRecord.FILENAME, record.url[11:]))
            warcinfo_headers.append((WarcRecord.TYPE, WarcRecord.WARCINFO))

            warcinfo_content = ('application/warc-fields', 'software: hanzo.arc2warc\r\n')

            inforecord = WarcRecord(headers=warcinfo_headers, content=warcinfo_content, version=version)

            warc_id = make_warc_uuid(record.url+record.date+"-meta")
            warcmeta_headers = [
                (WarcRecord.TYPE, WarcRecord.METADATA),
                (WarcRecord.CONCURRENT_TO, self.warcinfo_id),
                (WarcRecord.ID, warc_id),
                (WarcRecord.URL, record.url),
                (WarcRecord.DATE, inforecord.date),
                (WarcRecord.WARCINFO_ID, self.warcinfo_id),
            ]
            warcmeta_content =('application/arc', record.raw())

            metarecord = WarcRecord(headers=warcmeta_headers, content=warcmeta_content, version=version)
            return inforecord, metarecord
        else:
            content_type, content = record.content
            if record.url.startswith('http'):
                # don't promote content-types for http urls,
                # they contain headers in the body.
                content_type="application/http;msgtype=response"

            headers.extend([
                (WarcRecord.TYPE, WarcRecord.RESPONSE ),
                (WarcRecord.URL,record.url),
                (WarcRecord.WARCINFO_ID, self.warcinfo_id),
            ])
        
            warcrecord = WarcRecord(headers=headers, content=(content_type, content), version=version)

            return warcrecord,

        return True

def main(argv):
    (options, input_files) = parser.parse_args(args=argv[1:])

    out = sys.stdout
    if options.output:
        out = open(options.output, 'ab')
        if options.output.endswith('.gz'):
            options.gzip = True
    if len(input_files) < 1:
        parser.error("no imput warc file(s)")
        
    for name in input_files:
        fh = MixedRecord.open_archive(filename=name, gzip="auto")
        arc = ArcTransformer()
        try:
            for record in fh:
                if isinstance(record, WarcRecord):
                    print >> sys.stderr, '   WARC', record.url
                    warcs = [record]
                else:
                    print >> sys.stderr, 'ARC    ', record.url
                    warcs = arc.convert(record)

                for warcrecord in warcs:
                    warcrecord.write_to(out, gzip=options.gzip)
        finally:
            fh.close()

    return 0



if __name__ == '__main__':
    sys.exit(main(sys.argv))



