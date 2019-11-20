Warctools
=========

WARC (Web ARChive) file tools for python 2/3 based on the
[WARC 1.0 spec](https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.0/)
and compatible with the Internet Archive's
[ARC File Format](https://archive.org/web/researcher/ArcFileFormat.php)
originally developed by Hanzo Archives.


Install
-------

```
pip install warctools
```


Python Usage
------------

```
from hanzo import warctools
```


Python Examples
---------------

Write a WARC file:

```
import os

from hanzo import warctools


def write():
    headers = [
        (b'WARC-Type', b'warcinfo'),
        (b'WARC-Date', b'2019-11-19T23:08:51.182451Z'),
        (b'WARC-Filename', b'CRAWL-20191119230851-00000-hostname.warc.gz'),
        (b'WARC-Record-ID', b'<urn:uuid:8cc5dcae-0b21-11ea-842b-525476278032>')
    ]
    content_type = b'application/warc-fields'
    content = 'This\nis\nonly\na\ntest\n'.encode()
    fname = 'test.warc.gz'

    mode = 'ab'
    if not os.path.exists(fname):
        mode = 'wb'

    with open(fname, mode) as _fh:
        content = (content_type, content)
        record = warctools.WarcRecord(headers=headers, content=content)
        record.write_to(_fh, gzip="record")
```


Command-line Usage
------------------

### warcvalid

Returns 0 if the arguments are all valid W/ARC files, non-zero on
error.

```
[warctools] $ warcvalid -h
Usage: warcvalid [options] warc warc warc

Options:
  -h, --help            show this help message and exit
  -l LIMIT, --limit=LIMIT
  -I INPUT_FORMAT, --input=INPUT_FORMAT
  -L LOG_LEVEL, --log-level=LOG_LEVEL
```

### warcdump

Writes human readable summary of warcfiles. Autodetects input format
when filenames are passed, i.e recordgzip vs plaintext, WARC vs
ARC. Assumes uncompressed warc on stdin if no args.

```
[warctools] $ warcdump -h
Usage: warcdump [options] warc warc warc

Options:
  -h, --help            show this help message and exit
  -l LIMIT, --limit=LIMIT
  -I INPUT_FORMAT, --input=INPUT_FORMAT
  -L LOG_LEVEL, --log-level=LOG_LEVEL
```

### warcfilter

Searches all headers for regex pattern. Autodetects and stdin like
warcdump. Prints out a WARC format by default. Use -i to invert
search. Use -U to constrain to url. Use -T to constrain to record
type. Use -C to constrain to content-type.

```
$ warcfilter -h
Usage: warcfilter [options] pattern warc warc warc

Options:
  -h, --help            show this help message and exit
  -l LIMIT, --limit=LIMIT
                        limit (ignored)
  -I INPUT_FORMAT, --input=INPUT_FORMAT
                        input format (ignored)
  -i, --invert          invert match
  -U, --url             match on url
  -T, --type            match on (warc) record type
  -C, --content-type    match on (warc) record content type
  -H, --http-content-type
                        match on http payload content type
  -D, --warc-date       match on WARC-Date header
  -L LOG_LEVEL, --log-level=LOG_LEVEL
                        log level(ignored)
```

### warc2warc

Autodetects compression on file args. Assumes uncompressed stdin if
none. Use -Z to write compressed output, i.e warc2warc -Z input >
input.gz. Should ignore buggy records in input.

```
[warctools] $ warc2warc -h
Usage: warc2warc [options] url (url ...)

Options:
  -h, --help            show this help message and exit
  -o OUTPUT, --output=OUTPUT
                        output warc file
  -l LIMIT, --limit=LIMIT
  -I INPUT_FORMAT, --input=INPUT_FORMAT
                        (ignored)
  -Z, --gzip            compress output, record by record
  -D, --decode_http     decode http messages (strip chunks, gzip)
  -L LOG_LEVEL, --log-level=LOG_LEVEL
  --wget-chunk-fix      skip transfer-encoding headers in http records, when
                        decoding them (-D)
```

### arc2warc

Creates a crappy WARC file from arc files on input. A handful of
headers are preserved. Use -Z to write compressed output, i.e arc2warc
-Z input.arc > input.warc.gz

```
[warctools] $ arc2warc -h
Usage: arc2warc [options] arc (arc ...)

Options:
  -h, --help            show this help message and exit
  -o OUTPUT, --output=OUTPUT
                        output warc file
  -l LIMIT, --limit=LIMIT
  -Z, --gzip            compress
  -L LOG_LEVEL, --log-level=LOG_LEVEL
  --description=DESCRIPTION
  --operator=OPERATOR
  --publisher=PUBLISHER
  --audience=AUDIENCE
  --resource=RESOURCE
  --response=RESPONSE
```

### warcindex

DEPRECATED, use `CDX-writer` branch.

```
#WARC-filename offset warc-type warc-subject-uri warc-record-id content-type content-length
warccrap/mywarc.warc 1196018 request /images/slides/hanzo_markm__wwwoh.pdf <urn:uuid:fd1255a8-d07c-11df-b125-12313b0a18c6> application/http;msgtype=request 193
warccrap/mywarc.warc 1196631 response http://www.hanzoarchives.com/images/slides/hanzo_markm__wwwoh.pdf <urn:uuid:fd2614f8-d07c-11df-b125-12313b0a18c6> application/http;msgtype=response 3279474
```


Notes
-----

1. arc2warc uses the conversion rules from the earlier arc2warc.c as a
   starter for converting the headers
2. I haven't profiled the code yet (and don't plan to until it falls
   over)
3. Warcvalid barely skirts some of the iso standard, missing things:
    * strict whitespace
    * required headers check
    * mime quoted printable header encoding
    * treating headers as utf8


ToDo
----

1. Lots more testing
2. Support pre-1.0 WARC files
3. Add more documentation
4. Support more commandline options for output and filenames
5. S3 urls


Credits
-------

Originally developed by "tef" `thomas.figg@hanzoarchives.com`.


@internetarchive
