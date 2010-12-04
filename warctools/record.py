"""a skeleton class for archive records"""


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

    def write_to(self, out, newline='\x0D\x0A'):  
        self._write_to(out, newline)


    def _write_to(self, out, newline):  
        raise AssertionError, 'this is bad'

    HEADERS=staticmethod(add_headers)
    






