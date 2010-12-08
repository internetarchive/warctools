"""a skeleton class for archive records"""

from warctools.stream import open_record_stream

def add_headers(**kwargs):
    """a useful helper for defining header names in record formats"""
    def _add_headers(cls):
        for k,v in kwargs.iteritems():
            setattr(cls,k,v)
        cls._HEADERS = kwargs.keys()
        return cls
    return _add_headers



@add_headers(DATE='Date', CONTENT_TYPE='Type', CONTENT_LENGTH='Length')
class ArchiveRecord(object):
    """An archive record has some headers, maybe some content and
    a list of errors encountered. record.headers is a list of tuples (name,
    value). errors is a list, and content is a tuple of (type, data)"""
    def __init__(self,  headers=None, content=None, errors=None):
        self.headers = headers if headers else []
        self.content = content if content else (None, "")
        self.errors = errors if errors else []

    HEADERS=staticmethod(add_headers)

    @property
    def date(self):
        return self.headers[self.DATE]

    def error(self, *args):
        self.errors.append(args)

    def dump(self, content=True):
        print 'Headers:'
        for (h,v) in self.headers:
            print '\t%s:%s'%(h,v)
        if content and self.content:
            print 'Content Headers:'
            content_type, content_body = self.content
            print '\t',self.CONTENT_TYPE,': ',content_type
            print '\t',self.CONTENT_LENGTH,': ',len(content_body)
            print 'Content:'
            for _,line in zip(range(20),content_body.split('\n')):
                print '\t', line
            print '\t...'
            print 
        else:
            print 'Content: none'
            print
            print
        if self.errors:
            print 'Errors:'
            for e in self.errors:
                print '\t', e

    def write_to(self, out, newline='\x0D\x0A', gzip=None):  
        self._write_to(out, newline, gzip)


    def _write_to(self, out, newline, gzip):  
        raise AssertionError, 'this is bad'


    ### class methods for parsing
    @classmethod
    def open_archive(cls , filename=None, file_handle=None, mode="rb+", gzip="auto"):
        """Generically open an archive - magic autodetect""" 
        return open_record_stream(None, filename, file_handle, mode, gzip)

    @classmethod
    def make_parser(self):
        """Reads a (w)arc record from the stream, returns a tuple (record, errors). 
        Either records is null or errors is null. Any record-specific errors are 
        contained in the record - errors is only used when *nothing* could be parsed"""
        raise StandardError


    



