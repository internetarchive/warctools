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
parser.add_option("-l", "--log", dest="logfile")

parser.set_defaults(output=None, log_file=None, default_name='index')


def main(argv):
    (options, args) = parser.parse_args(args=argv[1:])

    out = sys.stdout
    if options.output:
        if not os.path.exists(options.output):
            os.makedirs(options.output)
        output_dir =  options.output
    else:
        output_dir  = os.getcwd()


    if len(args) < 1:
        # dump the first record on stdin
        with closing(WarcRecord.open_archive(file_handle=sys.stdin, gzip=None)) as fh:
            unpack_records(fh, output_dir, default_name)
        
    else:
        # dump a record from the filename, with optional offset
        for filename in args:
            with closing(ArchiveRecord.open_archive(filename=filename, gzip="auto")) as fh:
                unpack_records(fh, output_dir, default_name)


    return 0

def unpack_records(fh, output_dir, default_name):
    for (offset, record, errors) in fh.read_records(limit=None):
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

                    filename = output_file(output_dir, record.url, message.header, default_name)

                    print >>sys.stderr, 'writing', record.url,  filename
                    with open(filename, 'wb') as out:
                        out.write(message.get_body())

            
        elif errors:
            print >> sys.stderr, "warc errors at %s:%d"%(name, offset if offset else 0)
            for e in errors:
                print '\t', e

def output_file(output_dir, url, http_header, default_name='index'):
    clean_url = "".join((c if c.isalpha() or c.isdigit() or c in '_-/.' else '_') for c in url.replace('://','/',1))

    parts = clean_url.split('/')
    directories, filename = parts[:-1], parts[-1]

    mime_type = [v for k,v in http_header.headers if k.lower() =='content-type']
    if mime_type:
        mime_type = mime_type[0].split(';')[0]
    else:
        mime_type = None

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

    while os.path.exists(fullname):
        u = uuid.uuid4()[:8]

        filename = name[:45-len(ext)] + u + ext

        fullname = os.path.join(directory, filename)

    return fullname
    
if __name__ == '__main__':
    sys.exit(main(sys.argv))



