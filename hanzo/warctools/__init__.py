from .record import ArchiveRecord
from .warc import WarcRecord
from .arc import ArcRecord
from .mixed import MixedRecord
from .s3 import list_files
from . import record, warc, arc, s3

def expand_files(files):
    for file in files:
        if file.startswith('s3:'):
            for f in list_files(file):
                yield f
        else:
            yield file

__all__= [
    'MixedRecord',
    'ArchiveRecord',
    'ArcRecord',
    'WarcRecord',
    'record',
    'warc',
    'arc',
    'expand_files',
]
