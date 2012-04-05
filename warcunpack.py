#!/usr/bin/env python
"""warcextract - dump warc record context to directory"""

import os
import uuid

import sys
import os.path

from optparse import OptionParser
from contextlib import closing
from urlparse import urlparse

import mimetypes

from hanzo.warctools import ArchiveRecord, WarcRecord
from hanzo.httptools import RequestMessage, ResponseMessage

mimetypes.add_type('text/javascript', 'js')

parser = OptionParser(usage="%prog [options] warc offset")

parser.add_option("-D", "--default-name", dest="default_name")
parser.add_option("-o", "--output", dest="output")
parser.add_option("-l", "--log", dest="log_file")

parser.set_defaults(output=None, log_file=None, default_name='index')


def log_headers(log_file):
    print >> log_file, 'warc_file warc_id warc_type warc_content_length warc_uri_date warc_subject_uri uri_content_type outfile'

def log_entry(log_file, input_file, record, content_type, output_file):
    print >> log_file, input_file, record.id, record.type, record.content_length, record.date, record.url, content_type, output_file

def main(argv):
    (options, args) = parser.parse_args(args=argv[1:])

    out = sys.stdout
    if options.output:
        if not os.path.exists(options.output):
            os.makedirs(options.output)
        output_dir =  options.output
    else:
        output_dir  = os.getcwd()


    log_file = sys.stdout if not options.log_file else open(options.log_file, 'ab')

    log_headers(log_file)
    if len(args) < 1:
        # dump the first record on stdin
        
        with closing(WarcRecord.open_archive(file_handle=sys.stdin, gzip=None)) as fh:
            unpack_records('<stdin>', fh, output_dir, options.default_name, log_file)
        
    else:
        # dump a record from the filename, with optional offset
        for filename in args:
            try:
                with closing(ArchiveRecord.open_archive(filename=filename, gzip="auto")) as fh:
                    unpack_records(filename, fh, output_dir, options.default_name, log_file)

            except StandardError, e:
                print >> sys.stderr, "exception in handling", filename, e


    return 0

def unpack_records(name, fh, output_dir, default_name, output_log):
    for (offset, record, errors) in fh.read_records(limit=None):
        if record:
            try:
                content_type, content = record.content

                if record.type == WarcRecord.WARCINFO:
                    pass
                if record.type == WarcRecord.RESPONSE and content_type.startswith('application/http'):

                    code, mime_type, message = parse_http_response(record)

                    if 200 <= code < 300: 
                        filename, collision = output_file(output_dir, record.url, mime_type, default_name)


                        with open(filename, 'wb') as out:
                            out.write(message.get_body())
                            log_entry(output_log, name, record, mime_type, filename)

            except StandardError, e:
                print >> sys.stderr, "exception in handling record", e

        elif errors:
            print >> sys.stderr, "warc errors at %s:%d"%(name, offset if offset else 0),
            for e in errors:
                print >> sys.stderr , e,
            print >>sys.stderr

def parse_http_response(record):
    message = ResponseMessage(RequestMessage())
    remainder = message.feed(record.content[1])
    message.close()
    if remainder or not message.complete():
        if remainder:
            print >>sys.stderr, 'warning: trailing data in http response for', record.url
        if not message.complete():
            print >>sys.stderr, 'warning: truncated http response for', record.url

    header = message.header

    mime_type = [v for k,v in header.headers if k.lower() =='content-type']
    if mime_type:
        mime_type = mime_type[0].split(';')[0]
    else:
        mime_type = None

    return header.code, mime_type, message


def output_file(output_dir, url, mime_type, default_name='index'):
    clean_url = "".join((c if c.isalpha() or c.isdigit() or c in '_-/.' else '_') for c in url.replace('://','/',1))

    parts = clean_url.split('/')
    directories, filename = parts[:-1], parts[-1]


    path = [output_dir]
    for d in directories:
        if d:
            path.append(d)

    if filename:
        name, ext = os.path.splitext(filename)
    else:
        name, ext = default_name, ''

    if mime_type:
        guess_type = mimetypes.guess_type(url)
        # preserve variant file extensions, rather than clobber with default for mime type
        if not ext or guess_type != mime_type: 
            mime_ext = mimetypes.guess_extension(mime_type)
            if mime_ext:
                ext = mime_ext

    directory =  os.path.normpath(os.path.join(*path))
    directory = directory[:200]
    
    if not os.path.exists(directory):
        os.makedirs(directory)

    filename = name[:45-len(ext)] + ext

    fullname = os.path.join(directory, filename)

    collision = False

    while os.path.exists(fullname):
        collision = True
        u = str(uuid.uuid4())[:8]

        filename = name[:45-len(ext)] + '_R'+ u + ext

        fullname = os.path.join(directory, filename)

    return os.path.realpath(os.path.normpath(fullname)), collision
    
if __name__ == '__main__':
    sys.exit(main(sys.argv))



