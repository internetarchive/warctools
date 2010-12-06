import sys

from warctools import WarcRecord


gz= '/Dropbox/v/old-warc-tools-reference-only/app/wdata/testwfile/newonerec.warc.gz'
gz='/work/hanzo-warc-tools/hooray.gz'
'/Users/tef/Downloads/IAH-20080430204825-00000-blackbook.warc.gz'
gz=sys.argv[1]

fh = WarcRecord.open_archive(gz, gzip=("auto" if len(sys.argv) >= 2 else sys.argv[2]))

for (offset, record, errors) in fh.read_records(limit=10):
    if record:
        print "Offset:",offset
        record.dump(content=True)
    elif errors:
        print "Offset:",offset
        print 'Errors:'
        for e in errors:
            print '\t', e
    else:
        print
        print 'End'
