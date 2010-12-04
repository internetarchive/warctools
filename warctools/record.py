

class AbstractRecord(object):
    date_field='Date'

    def __init__(self, buffer=None, fields=None, block=None, errors=None):
        self.buffer = buffer
        self._fields = fields
        self._block = block
        self.errors = errors if errors else []

    @property
    def fields(self):
        """returns list fields for this record (i.e headers) of tuples (name, value)"""
        if self._fields is None:
            self._parse()
        return self._fields

    @fields.setter
    def fields(self, value):
        self._fields = value


    @property
    def block()
        """returns the block of the record (i.e the payload) as a tuple (type, string)"""
        if self._block is None:
            self._parse()
        return self._block

    @block.setter
    def block(self,value):
        self._block = value

    @property
    def date(self):
        return self.fields[self.date_field]


    def dump(self):
        print 'Headers:'
        for (h,v) in self.fields:
            print '%t%s:%s'%(h,v)
        print
        print 'Payload:',
        if self.block:
            content_type, buffer = self.block
            print 'length', len(self.block),'content-type:',content_type
        else:
            print 'none'
        print

    def write_to(self, out, newline):  
        self._write_to(out, newline)


    def _write_to(self, out, newline):  
        raise AssertionError, 'this is bad'
    def _parse(self):
        raise AssertionError, 'this is bad'

        

def WarcRecord(AbstractRecord):
    date_field = 'WARC-Date'
    type_field = 'WARC-Type'
    id_field = 'WARC-Record-ID'
    content_length_field = 'Content-Length'
    content_type_field = 'Content-Type'
    

    def __init__(self, buffer=None, version=None, fields=None, block=None, errors=None):
        AbstractRecord.__init__(self,buffer,fields,block,errors) 
        self.version = version

    @property
    def type(self):
        return self.fields[self.type_field]

    @property
    def id(self):
        return self.fields[self.id_field]

    @property
    def content_type(self):
        return self.fields[self.content_type_field]

    def _write_to(self, out, newline):  
        out.write(self.version)
        out.write(nl)
        for k,v in self.fields:
            out.write(k)
            out.write(": ")
            out.write(v)
            out.write(nl)
        content_type, buffer = self.block
        if content_type:
            out.write(self.content_type_field)
            out.write(": ")
            out.write(content_type)
            out.write(nl)
        if buffer:
            content_length = len(buffer)
            out.write(self.content_length_field)
            out.write(": ")
            out.write(content_length)
            out.write(nl)
        out.write(nl)
        if buffer:
            out.write(buffer)
        out.write(nl)
        out.write(nl)


    def _parse(self):
        b = self.buffer
        self.version=b.readline()
        # break out the res etc.... 
        

    def validate(self):
        return self.errors
