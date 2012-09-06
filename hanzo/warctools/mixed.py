
from hanzo.warctools.record import ArchiveRecord, ArchiveParser
from hanzo.warctools.warc import WarcParser
from hanzo.warctools.arc import ArcParser


class MixedRecord(ArchiveRecord):
    @classmethod
    def make_parser(self):
        return MixedParser()

class MixedParser(ArchiveParser):
    def __init__(self):
        self.arc = ArcParser()
        self.warc = WarcParser()

    def parse(self, stream, offset=None):
        line = stream.readline()
        if line.startswith('WARC'):
            record, errors, offset = self.warc.parse(stream, offset, line=line)
            errors = list(errors) + list(self.warc.trim(stream))
        else:
            record, errors, offset = self.arc.parse(stream, offset, line=line)

        return record, errors,offset

