#!/usr/bin/env python
"""warcextract - dump warc record context to directory"""

import os
import sys
import os.path
import uuid
import mimetypes
import shlex

from optparse import OptionParser
from contextlib import closing
from urlparse import urlparse


from hanzo.warctools import ArchiveRecord, WarcRecord
from hanzo.httptools import RequestMessage, ResponseMessage

mimetypes.add_type('text/javascript', 'js')

parser = OptionParser(usage="%prog [options] warc offset")

parser.add_option("-D", "--default-name", dest="default_name")
parser.add_option("-o", "--output", dest="output")
parser.add_option("-l", "--log", dest="log_file")
parser.add_option("-W", "--wayback_prefix", dest="wayback")

parser.set_defaults(output=None, log_file=None, default_name='crawlerdefault', wayback="http://wayback.archive-it.org/")


def log_headers(log_file):
    print >> log_file, '>>warc_file\twarc_id\twarc_type\twarc_content_length\twarc_uri_date\twarc_subject_uri\turi_content_type\toutfile\twayback_uri'

def log_entry(log_file, input_file, record, content_type, output_file, wayback_uri):
    log = (input_file, record.id, record.type, record.content_length, record.date, record.url, content_type, output_file, wayback_uri)
    print >> log_file, "\t".join(str(s) for s in log)

def main(argv):
    (options, args) = parser.parse_args(args=argv[1:])

    out = sys.stdout
    if options.output:
        if not os.path.exists(options.output):
            os.makedirs(options.output)
        output_dir =  options.output
    else:
        output_dir  = os.getcwd()

    collisions = 0


    if len(args) < 1:
        log_file = sys.stdout if not options.log_file else open(options.log_file, 'wb')
        log_headers(log_file)
        
        with closing(WarcRecord.open_archive(file_handle=sys.stdin, gzip=None)) as fh:
            collisions += unpack_records('<stdin>', fh, output_dir, options.default_name, log_file, options.wayback)
        
    else:
        for filename in args:
            
            log_file = os.path.join(output_dir, os.path.basename(filename)+ '.index.txt') if not options.log_file else options.log_file
            log_file = open(log_file, 'wb')
            log_headers(log_file)
            try:
                with closing(ArchiveRecord.open_archive(filename=filename, gzip="auto")) as fh:
                    collisions+=unpack_records(filename, fh, output_dir, options.default_name, log_file, options.wayback)

            except StandardError, e:
                print >> sys.stderr, "exception in handling", filename, e
    if collisions:
        print >> sys.stderr, collisions, "filenames that collided"
        

    return 0

def unpack_records(name, fh, output_dir, default_name, output_log, wayback_prefix):
    collectionId = ''
    collisions = 0
    for (offset, record, errors) in fh.read_records(limit=None):
        if record:
            try:
                content_type, content = record.content

                if record.type == WarcRecord.WARCINFO:
                    info = parse_warcinfo(record)
                    for entry in shlex.split(info.get('description', "")):
                        if entry.startswith('collectionId'):
                            collectionId = entry.split('=',1)[1].split(',')[0]
                    if not collectionId:
                        filename = record.get_header("WARC-Filename")
                        if filename:
                            collectionId = filename.split(r'-')[1]
                        elif '-' in name:
                            collectionId = name.split(r'-')[1]



                if record.type == WarcRecord.RESPONSE and content_type.startswith('application/http'):

                    code, mime_type, message = parse_http_response(record)

                    if 200 <= code < 300: 
                        filename, collision = output_file(output_dir, record.url, mime_type, default_name)
                        if collision:
                            collisions+=1

                        wayback_uri = ''
                        if collectionId:
                            wayback_date = record.date.translate(None,r'TZ:-')
                            wayback_uri = wayback_prefix + collectionId + '/' + wayback_date + '/' + record.url

                        with open(filename, 'wb') as out:
                            out.write(message.get_body())
                            log_entry(output_log, name, record, mime_type, filename, wayback_uri)

            except StandardError, e:
                import traceback; traceback.print_exc()
                print >> sys.stderr, "exception in handling record", e

        elif errors:
            print >> sys.stderr, "warc errors at %s:%d"%(name, offset if offset else 0),
            for e in errors:
                print >> sys.stderr , e,
            print >>sys.stderr
    return collisions

def parse_warcinfo(record):
    info = {}
    try:
        for line in record.content[1].split('\n'):
            line = line.strip()
            if line:
                try:
                    key, value =line.split(':',1)
                    info[key]=value
                except StandardError, e:
                        print >>sys.stderr, 'malformed warcinfo line', line
    except StandardError, e:
            print >>sys.stderr, 'exception reading warcinfo record', e
    return info

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


def output_file(output_dir, url, mime_type, default_name):
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
    elif not ext:
        ext = '.html' # no mime time, no extension

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



