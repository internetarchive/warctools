#!/usr/bin/env python
"""arc2warc - convert ARC format files to WARC format

WARC Format Specification References:
- WARC 1.1 Annotated (primary): https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/
- ARC Format: http://archive.org/web/researcher/ArcFileFormat.php
- WARC Record Types: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#warc-record-types
"""

import datetime
import hashlib
import socket
import sys
import uuid

import click

from .httptools import RequestMessage, ResponseMessage
from .warctools import ArcRecord, MixedRecord, WarcRecord, expand_files
from .warctools.warc import warc_datetime_str


def is_http_response(content):
    message = ResponseMessage(RequestMessage())
    remainder = message.feed(content)
    message.close()
    return message.complete() and not remainder


class ArcTransformer:
    def __init__(
        self,
        output_filename=None,
        warcinfo_fields=b"software: hanzo.arc2warc\r\n",
        resources=(),
        responses=(),
    ):
        self.warcinfo_id = None
        self.output_filename = output_filename
        self.version = b"WARC/1.0"
        self.warcinfo_fields = warcinfo_fields
        self.resources = resources
        self.responses = responses

    @staticmethod
    def make_warc_uuid(text: bytes) -> bytes:
        """Generate a WARC UUID from text."""
        return (f"<urn:uuid:{uuid.UUID(hashlib.sha1(text).hexdigest()[:32])}>").encode("ascii")

    def convert(self, record):
        if record.type == b"filedesc":
            return self.convert_filedesc(record)
        else:
            return self.convert_record(record)

    def convert_filedesc(self, record):
        # todo - filedesc might have missing url?
        warcinfo_date = warc_datetime_str(datetime.datetime.now())
        warcinfo_id = self.make_warc_uuid(record.url + warcinfo_date)

        warcinfo_headers = [
            (WarcRecord.TYPE, WarcRecord.WARCINFO),
            (WarcRecord.ID, warcinfo_id),
            (WarcRecord.DATE, warcinfo_date),
        ]

        if self.output_filename:
            warcinfo_headers.append((WarcRecord.FILENAME, self.output_filename))

        warcinfo_content = (b"application/warc-fields", self.warcinfo_fields)

        inforecord = WarcRecord(
            headers=warcinfo_headers, content=warcinfo_content, version=self.version
        )

        if record.date:
            if len(record.date) >= 14:
                warcmeta_date = datetime.datetime.strptime(
                    record.date[:14].decode("ascii"), "%Y%m%d%H%M%S"
                )
            else:
                warcmeta_date = datetime.datetime.strptime(
                    record.date[:8].decode("ascii"), "%Y%m%d"
                )

            warcmeta_date = warc_datetime_str(warcmeta_date)
        else:
            warcmeta_date = warcinfo_date

        warcmeta_id = self.make_warc_uuid(record.url + record.date + b"-meta")
        warcmeta_url = record.url
        if warcmeta_url.startswith(b"filedesc://"):
            warcmeta_url = warcmeta_url[11:]
        warcmeta_headers = [
            (WarcRecord.TYPE, WarcRecord.METADATA),
            (WarcRecord.CONCURRENT_TO, warcinfo_id),
            (WarcRecord.ID, warcmeta_id),
            (WarcRecord.URL, warcmeta_url),
            (WarcRecord.DATE, warcmeta_date),
            (WarcRecord.WARCINFO_ID, warcinfo_id),
        ]
        warcmeta_content = (b"application/arc", record.raw())

        metarecord = WarcRecord(
            headers=warcmeta_headers, content=warcmeta_content, version=self.version
        )

        self.warcinfo_id = warcinfo_id

        return inforecord, metarecord

    def convert_record(self, record):
        warc_id = self.make_warc_uuid(record.url + record.date)
        headers = [
            (WarcRecord.ID, warc_id),
            (WarcRecord.URL, record.url),
            (WarcRecord.WARCINFO_ID, self.warcinfo_id),
        ]

        if record.date:
            try:
                date = datetime.datetime.strptime(record.date.decode("ascii"), "%Y%m%d%H%M%S")
            except ValueError:
                date = datetime.datetime.strptime(record.date.decode("ascii"), "%Y%m%d")

        else:
            date = datetime.datetime.now()

        ip = record.get_header(ArcRecord.IP)
        if ip:
            ip = ip.strip()
            if ip != b"0.0.0.0":
                headers.append((WarcRecord.IP_ADDRESS, ip))

        headers.append((WarcRecord.DATE, warc_datetime_str(date)))

        content_type, content = record.content

        if not content_type.strip():
            content_type = b"application/octet-stream"

        url = record.url.lower()

        if any(url.startswith(p) for p in self.resources):
            record_type = WarcRecord.RESOURCE
        elif any(url.startswith(p) for p in self.responses):
            record_type = WarcRecord.RESPONSE
        elif url.startswith(b"http"):
            if is_http_response(content):
                content_type = b"application/http;msgtype=response"
                record_type = WarcRecord.RESPONSE
            else:
                record_type = WarcRecord.RESOURCE
        elif url.startswith(b"dns"):
            if (
                content_type.startswith(b"text/dns")
                and str(content.decode("ascii", "ignore")) == content
            ):
                record_type = WarcRecord.RESOURCE
            else:
                record_type = WarcRecord.RESPONSE
        else:
            # unknown protocol
            record_type = WarcRecord.RESPONSE

        headers.append((WarcRecord.TYPE, record_type))

        warcrecord = WarcRecord(
            headers=headers, content=(content_type, content), version=self.version
        )

        return (warcrecord,)


def warcinfo_fields(
    description: str = "",
    operator: str = "",
    publisher: str = "",
    audience: str = "",
) -> bytes:
    """Generate WARC info fields."""
    return "\r\n".join(
        [
            "software: hanzo.arc2warc",
            f"hostname: {socket.gethostname()}",
            f"description: {description}",
            f"operator: {operator}",
            f"publisher: {publisher}",
            f"audience: {audience}",
        ]
    ).encode("utf-8")


## todo
"""
    move arctransformer into mixed.py
    move output file into arc2warc loop

"""


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "-o",
    "--output",
    "output",
    help="output warc file",
    type=click.Path(),
    default=None,
)
@click.option(
    "-l",
    "--limit",
    "limit",
    help="Limit number of records (ignored, kept for compatibility)",
    default=None,
)
@click.option(
    "-Z",
    "--gzip",
    "gzip",
    is_flag=True,
    help="compress",
    default=False,
)
@click.option(
    "-L",
    "--log-level",
    "log_level",
    help="Log level (ignored, kept for compatibility)",
    default="info",
)
@click.option("--description", "description", help="WARC description", default="")
@click.option("--operator", "operator", help="WARC operator", default="")
@click.option("--publisher", "publisher", help="WARC publisher", default="")
@click.option("--audience", "audience", help="WARC audience", default="")
@click.option(
    "--resource",
    "resource",
    multiple=True,
    help="URL prefix to treat as resource (can be specified multiple times)",
    default=[],
)
@click.option(
    "--response",
    "response",
    multiple=True,
    help="URL prefix to treat as response (can be specified multiple times)",
    default=[],
)
@click.argument("arc_files", nargs=-1, required=True, type=click.Path(exists=True))
def main(
    output: str | None,
    limit: str | None,
    gzip: bool,
    log_level: str,
    description: str,
    operator: str,
    publisher: str,
    audience: str,
    resource: tuple[str, ...],
    response: tuple[str, ...],
    arc_files: tuple[str, ...],
) -> None:
    """Convert ARC files to WARC format."""
    out = sys.stdout.buffer

    if output:
        out = open(output, "ab")
        if output.endswith(".gz"):
            gzip = True

    warcinfo = warcinfo_fields(
        description=description,
        operator=operator,
        publisher=publisher,
        audience=audience,
    )
    arc = ArcTransformer(
        output, warcinfo, tuple(r.encode() for r in resource), tuple(r.encode() for r in response)
    )
    for name in expand_files(arc_files):
        fh = MixedRecord.open_archive(filename=name, gzip="auto")
        try:
            for record in fh:
                if isinstance(record, WarcRecord):
                    print(f"   WARC {record.url}", file=sys.stderr)
                    warcs = [record]
                else:
                    print(f"ARC     {record.url}", file=sys.stderr)
                    warcs = arc.convert(record)

                for warcrecord in warcs:
                    warcrecord.write_to(out, gzip=gzip)
        finally:
            fh.close()

    if output and out != sys.stdout.buffer:
        out.close()


def run() -> None:
    """Entry point for the command-line interface."""
    main()


if __name__ == "__main__":
    run()
