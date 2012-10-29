from urlparse import urlparse

from boto.s3.connection import S3Connection, Location
from boto.s3.key import Key

import tempfile
from cStringIO import StringIO


def open_url(url):
    p = urlparse(url)
    bucket_name = p.netloc
    key = p.path[1:]
    conn = S3Connection()
    b= conn.get_bucket(bucket_name)
    k = Key(b)
    k.key = key
    s = StringIO()
    k.get_contents_to_file(s)
    s.seek(0)
    return s

def list_files(prefix):
    p = urlparse(prefix)
    bucket_name = p.netloc
    prefix = p.path[1:]

    conn = S3Connection()

    b= conn.get_bucket(bucket_name)
    complete  = False
    marker = ''

    while not complete:
        rs = b.get_all_keys(prefix=prefix, marker=marker, delimiter='')
        for k in rs:
            yield 's3://%s/%s'%(bucket_name, k.key)
            marker = k.key

        complete = not rs.is_truncated
