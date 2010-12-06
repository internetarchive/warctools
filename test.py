
import warctools

fh = open('/Users/tef/Downloads/IAH-20080430204825-00000-blackbook.warc')

records, errors =  warctools.WarcRecord.parse(fh, limit=3)

for a in records:
    a.dump()
print errors
