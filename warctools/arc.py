"""An object to represent arc records"""

import re

from warctools.record import ArchiveRecord
from warctools.stream import open_record_stream

# URL<sp>IP-address<sp>Archive-date<sp>Content-type<sp>
#Result-code<sp>Checksum<sp>Location<sp> Offset<sp>Filename<sp>
#Archive-length<nl> 
# 
@ArchiveRecord.HEADERS(
    URL='URL',
    IP='IP-address',
    DATE='Archive-date',
    CONTENT_TYPE = 'Content-type',
    CONTENT_LENGTH = 'Archive-length',
    RESULT_CODE = 'Result-code',
    CHECKSUM = 'Checksum',
    LOCATION = 'Location',
    OFFSET = 'Offset',
    FILENAME = 'Filename',
)
class ArcRecord(ArchiveRecord):
    def __init__(self, headers=None, content=None, errors=None):
        AbstractRecord.__init__(self,headers,content,errors) 

    @property
    def url(self):
        return self.headers[self.URL]

    def _write_to(self, out, nl):  
        pass



