from .record import ArchiveRecord
from .warc import WarcRecord
from .arc import ArcRecord
from .mixed import MixedRecord
from . import record, warc, arc

__all__= [
    'MixedRecord',
    'ArchiveRecord',
    'ArcRecord',
    'WarcRecord',
    'record',
    'warc',
    'arc'
]
