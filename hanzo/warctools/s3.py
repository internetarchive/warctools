from urlparse import urlparse

from cStringIO import StringIO

try:
    from boto.s3.connection import S3Connection
    from boto.s3.key import Key
except ImportError:
    def open_url(url, offset=None, length=None):
        raise ImportError('boto')

    def list_files(prefix):
        raise ImportError('boto')
else:
    def open_url(url, offset=None, length=None):
        p = urlparse(url)
        bucket_name = p.netloc
        key = p.path[1:]
        conn = S3Connection()
        bucket = conn.get_bucket(bucket_name)
        k = Key(bucket)
        k.key = key
        if offset is not None and length is not None:
            headers = {'Range': 'bytes=%d-%d' % (offset, offset + length)}
        elif offset is not None:
            headers = {'Range': 'bytes=%d-' % offset}
        else:
            headers = {}

        s = StringIO()
        k.get_contents_to_file(s, headers=headers)
        s.seek(0)
        return s

    def list_files(prefix):
        p = urlparse(prefix)
        bucket_name = p.netloc
        prefix = p.path[1:]

        conn = S3Connection()

        bucket = conn.get_bucket(bucket_name)
        complete  = False
        marker = ''

        while not complete:
            rs = bucket.get_all_keys(prefix=prefix, marker=marker, delimiter='')
            for k in rs:
                yield 's3://%s/%s' % (bucket_name, k.key)
                marker = k.key

            complete = not rs.is_truncated
