This is a fork of Hanzo Archives' warc tools package. It was forked after
[changeset 1897e2bc9d29](http://code.hanzoarchives.com/warc-tools/changeset/1897e2bc9d29)
and has not yet been merged upstream.

This fork contains the following changes to support creating CDX indexes
of archives used by the Internet Archive's Wayback Machine.

Changes:

- ArchiveRecord base class now contains a compressed_record_size field:
changesets [941b79f83d6c](https://bitbucket.org/rajbot/warc-tools/changeset/941b79f83d6c) and
[f0a98baef67b](https://bitbucket.org/rajbot/warc-tools/changeset/f0a98baef67b)
- The WARC-Payload-Digest is fabricated if not present. This allows us to
include the sha1 hash in cdx files without having to store the entire
payload in memory. Changeset [0bedbca70d7c](https://bitbucket.org/rajbot/warc-tools/changeset/0bedbca70d7c)
- Do not store payloads larger than 5MB due to memory issues:
changesets [f412b98c5575](https://bitbucket.org/rajbot/warc-tools/changeset/f412b98c5575) and
[d3767f736601](https://bitbucket.org/rajbot/warc-tools/changeset/d3767f736601)
- Bugfix for parsing warc headers with a CRLF at 1K compressed chunk boundary:
changeset [25595bef06b0](https://bitbucket.org/rajbot/warc-tools/changeset/25595bef06b0)

----

dependencies
	setuptools
	unittest2
	python 2.6

hanzo warc tools:

    warcvalid.py
        returns 0 if the arguments are all valid arc/warc files
        non zero on error

    warcdump.py - writes human readable summary of warcfiles:
        usage: python warcdump.py foo.warc foo.warc.gz
        autodetects input format when filenames are passed
        i.e recordgzip vs plaintext, warc vs arc

        assumes uncompressed warc on stdin if no args

    warcfilter.py
        python warcfilter.py pattern file file file
            searches all headers for regex pattern
        use -i to invert search
        use -U to constrain to url
        use -T to constrain to record type
        use -C to constrain to content-type

        autodetects and stdin like warcdump

        prints out a warc format by default.

    warc2warc.py:
        python warc2warc <input files>

        autodetects compression on file
        args, assumes uncompressed stdin if none

        use -Z to write compressed output

        i.e warc2warc -Z input > input.gz

        should ignore buggy records in input

    arc2warc.py
        creates a crappy warc file from arc files on input
        a handful of headers are preserved
        use -Z to write compressed output
        i.e arc2warc -Z input.arc > input.warc.gz

    warcindex.py
        spits out an index like this:
#WARC filename offset warc-type warc-subject-uri warc-record-id content-type content-length
warccrap/mywarc.warc 1196018 request /images/slides/hanzo_markm__wwwoh.pdf <urn:uuid:fd1255a8-d07c-11df-b125-12313b0a18c6> application/http;msgtype=request 193
warccrap/mywarc.warc 1196631 response http://www.hanzoarchives.com/images/slides/hanzo_markm__wwwoh.pdf <urn:uuid:fd2614f8-d07c-11df-b125-12313b0a18c6> application/http;msgtype=response 3279474
        not great, but a start

notes:

    arc2warc uses the conversion rules from the earlier arc2warc.c
    as a starter for converting the headers

    I haven't profiled the code yet (and don't plan to until it falls over)

    warcvalid barely skirts some of the iso standard:
        missing things: strict whitespace, required headers check...
	mime quoted printable header encoding
	treating headers as utf8

things left to do (in no order):
    lots more testing.
    supporting pre 1.0 warc files
    add more documentation
    support more commandline options for output and filenames
    s3 urls


-- tef thomas.figg@hanzoarchives.com

