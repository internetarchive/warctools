#!/usr/bin/env python
"""arc2warc - convert one arc to a new warc"""

from __future__ import print_function

import os
import sys
import hashlib
import uuid

import sys
import os.path
import datetime
import socket

from optparse import OptionParser

from .warctools import ArcRecord,WarcRecord, MixedRecord, expand_files
from .warctools.warc import warc_datetime_str

from .httptools import ResponseMessage, RequestMessage

parser = OptionParser(usage="%prog [options] arc (arc ...)")

parser.add_option("-o", "--output", dest="output",
                       help="output warc file")
parser.add_option("-l", "--limit", dest="limit")
parser.add_option("-Z", "--gzip", dest="gzip", action="store_true", help="compress")
parser.add_option("-L", "--log-level", dest="log_level")
parser.add_option("--description", dest="description")
parser.add_option("--operator", dest="operator")
parser.add_option("--publisher", dest="publisher")
parser.add_option("--audience", dest="audience")
parser.add_option("--resource", dest="resource", action="append")
parser.add_option("--response", dest="response", action="append")

parser.set_defaults(
    output_directory=None, limit=None, log_level="info", gzip=False,
    description="", operator="", publisher="", audience="",
    resource = [], response=[],
    
)

def is_http_response(content):
    message = ResponseMessage(RequestMessage())
    remainder = message.feed(content)
    message.close()
    return message.complete() and not remainder


class ArcTransformer(object):
    def __init__(self, output_filename=None, warcinfo_fields=b'software: hanzo.arc2warc\r\n', resources=(), responses=()):
        self.warcinfo_id = None
        self.output_filename = output_filename
        self.version = b"WARC/1.0"
        self.warcinfo_fields = warcinfo_fields
        self.resources = resources
        self.responses = responses

    @staticmethod
    def make_warc_uuid(text):
        return ("<urn:uuid:%s>"%uuid.UUID(hashlib.sha1(text).hexdigest()[0:32])).encode('ascii')

    def convert(self, record):

        if record.type == b'filedesc':
            return self.convert_filedesc(record)
        else:
            return self.convert_record(record)
        
    def convert_filedesc(self, record):
        # todo - filedesc might have missing url?
        warcinfo_date = warc_datetime_str(datetime.datetime.now())
        warcinfo_id = self.make_warc_uuid(record.url+warcinfo_date)

        warcinfo_headers = [
            (WarcRecord.TYPE, WarcRecord.WARCINFO),
            (WarcRecord.ID, warcinfo_id),
            (WarcRecord.DATE, warcinfo_date),
        ]

        if self.output_filename:
            warcinfo_headers.append((WarcRecord.FILENAME, self.output_filename))

        warcinfo_content = (b'application/warc-fields', self.warcinfo_fields)

        inforecord = WarcRecord(headers=warcinfo_headers, content=warcinfo_content, version=self.version)

        if record.date:
            if len(record.date) >= 14:
                warcmeta_date = datetime.datetime.strptime(record.date[:14].decode('ascii'),'%Y%m%d%H%M%S')
            else:
                warcmeta_date = datetime.datetime.strptime(record.date[:8].decode('ascii'),'%Y%m%d')

            warcmeta_date = warc_datetime_str(warcmeta_date)
        else:
            warcmeta_date = warcinfo_date


        warcmeta_id = self.make_warc_uuid(record.url+record.date+b"-meta")
        warcmeta_url = record.url
        if warcmeta_url.startswith(b'filedesc://'):
            warcmeta_url = warcmeta_url[11:]
        warcmeta_headers = [
            (WarcRecord.TYPE, WarcRecord.METADATA),
            (WarcRecord.CONCURRENT_TO, warcinfo_id),
            (WarcRecord.ID, warcmeta_id),
            (WarcRecord.URL, warcmeta_url),
            (WarcRecord.DATE, warcmeta_date),
            (WarcRecord.WARCINFO_ID, warcinfo_id),
        ]
        warcmeta_content =(b'application/arc', record.raw())

        metarecord = WarcRecord(headers=warcmeta_headers, content=warcmeta_content, version=self.version)

        self.warcinfo_id = warcinfo_id

        return inforecord, metarecord

    def convert_record(self, record):

        warc_id = self.make_warc_uuid(record.url+record.date)
        headers = [
            (WarcRecord.ID, warc_id),
            (WarcRecord.URL,record.url),
            (WarcRecord.WARCINFO_ID, self.warcinfo_id),
        ]

        if record.date:
            try:
                date = datetime.datetime.strptime(record.date.decode('ascii'),'%Y%m%d%H%M%S')
            except ValueError:
                date = datetime.datetime.strptime(record.date.decode('ascii'),'%Y%m%d')

        else:
            date = datetime.datetime.now()

        ip = record.get_header(ArcRecord.IP)
        if ip:
            ip = ip.strip()
            if ip != b"0.0.0.0":
                headers.append((WarcRecord.IP_ADDRESS, ip))
            

        headers.append((WarcRecord.DATE, warc_datetime_str(date)))

        content_type, content = record.content

        if not content_type.strip():
            content_type = b'application/octet-stream'

        url = record.url.lower()


        if any(url.startswith(p) for p in self.resources):
            record_type = WarcRecord.RESOURCE
        elif any(url.startswith(p) for p in self.responses):
            record_type = WarcRecord.RESPONSE
        elif url.startswith(b'http'):
            if is_http_response(content):
                content_type=b"application/http;msgtype=response"
                record_type = WarcRecord.RESPONSE
            else:
                record_type = WarcRecord.RESOURCE
        elif url.startswith(b'dns'):
            if content_type.startswith(b'text/dns') and str(content.decode('ascii', 'ignore')) == content:
                record_type = WarcRecord.RESOURCE
            else:
                record_type = WarcRecord.RESPONSE
        else:
            # unknown protocol
            record_type = WarcRecord.RESPONSE
          
        headers.append((WarcRecord.TYPE, record_type))

        warcrecord = WarcRecord(headers=headers, content=(content_type, content), version=self.version)

        return warcrecord,

def warcinfo_fields(description="", operator="", publisher="", audience=""):
    return "\r\n".join([
        "software: hanzo.arc2warc",
        "hostname: %s"%socket.gethostname(),
        "description: %s"%description,
        "operator: %s"%operator,
        "publisher: %s"%publisher,
        "audience: %s"%audience,
    ]).encode('utf-8')

## todo
"""
    move arctransformer into mixed.py
    move output file into arc2warc loop

"""
def main(argv):
    (options, input_files) = parser.parse_args(args=argv[1:])

    try: # python3
        out = sys.stdout.buffer
    except AttributeError: # python2
        out = sys.stdout

    if options.output:
        out = open(options.output, 'ab')
        if options.output.endswith('.gz'):
            options.gzip = True
    if len(input_files) < 1:
        parser.error("no imput warc file(s)")
        
    warcinfo = warcinfo_fields(
        description = options.description,
        operator = options.operator,
        publisher = options.publisher,
        audience = options.audience,
    )
    arc = ArcTransformer(options.output, warcinfo, options.resource, options.response)
    for name in expand_files(input_files):
        fh = MixedRecord.open_archive(filename=name, gzip="auto")
        try:
            for record in fh:
                if isinstance(record, WarcRecord):
                    print('   WARC', record.url, file=sys.stderr)
                    warcs = [record]
                else:
                    print('ARC    ', record.url, file=sys.stderr)
                    warcs = arc.convert(record)

                for warcrecord in warcs:
                    warcrecord.write_to(out, gzip=options.gzip)
        finally:
            fh.close()

    return 0

def run():
    sys.exit(main(sys.argv))


if __name__ == '__main__':
    run()



