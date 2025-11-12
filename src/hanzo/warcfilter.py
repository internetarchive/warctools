#!/usr/bin/env python
"""warcfilter - prints warcs in that match regexp, by default searches all headers"""

import logging
import re
import sys
from re import Pattern

import click

from .httptools import RequestMessage, ResponseMessage
from .warctools import WarcRecord, expand_files


def parse_http_response(record):
    """Parse HTTP response from WARC record."""
    message = ResponseMessage(RequestMessage())
    remainder = message.feed(record.content[1])
    message.close()
    if remainder or not message.complete():
        if remainder:
            logging.warning(f"trailing data in http response for {record.url}")
        if not message.complete():
            logging.warning(f"truncated http response for {record.url}")

    header = message.header

    mime_type = [v for k, v in header.headers if k.lower() == b"content-type"]
    if mime_type:
        mime_type = mime_type[0].split(b";")[0]
    else:
        mime_type = None

    return header.code, mime_type, message


def filter_archive(
    fh,
    pattern: Pattern[bytes],
    out,
    invert: bool,
    url: bool,
    type_flag: bool,
    content_type: bool,
    http_content_type: bool,
    warc_date: bool,
) -> None:
    """Filter archive records based on pattern."""
    for record in fh:
        if url:
            if bool(record.url and pattern.search(record.url)) ^ invert:
                record.write_to(out)

        elif type_flag:
            if bool(record.type and pattern.search(record.type)) ^ invert:
                record.write_to(out)

        elif content_type:
            if bool(record.content_type and pattern.search(record.content_type)) ^ invert:
                record.write_to(out)

        elif http_content_type:
            if record.type == WarcRecord.RESPONSE and record.content_type.startswith(
                b"application/http"
            ):
                code, content_type_val, message = parse_http_response(record)

                if bool(content_type_val and pattern.search(content_type_val)) ^ invert:
                    record.write_to(out)

        elif warc_date:
            if bool(record.date and pattern.search(record.date)) ^ invert:
                record.write_to(out)

        else:
            found = False
            for _name, value in record.headers:
                if pattern.search(value):
                    found = True
                    break

            content_type_val, content = record.content
            if not found:
                found = bool(pattern.search(content))

            if found ^ invert:
                record.write_to(out)


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "-l",
    "--limit",
    "limit",
    help="Limit number of records (ignored, kept for compatibility)",
    default=None,
)
@click.option(
    "-I",
    "--input",
    "input_format",
    help="Input format (ignored, kept for compatibility)",
    default=None,
)
@click.option(
    "-i",
    "--invert",
    "invert",
    is_flag=True,
    help="invert match",
    default=False,
)
@click.option(
    "-U",
    "--url",
    "url",
    is_flag=True,
    help="match on url",
    default=False,
)
@click.option(
    "-T",
    "--type",
    "type_flag",
    is_flag=True,
    help="match on (warc) record type",
    default=False,
)
@click.option(
    "-C",
    "--content-type",
    "content_type",
    is_flag=True,
    help="match on (warc) record content type",
    default=False,
)
@click.option(
    "-H",
    "--http-content-type",
    "http_content_type",
    is_flag=True,
    help="match on http payload content type",
    default=False,
)
@click.option(
    "-D",
    "--warc-date",
    "warc_date",
    is_flag=True,
    help="match on WARC-Date header",
    default=False,
)
@click.option(
    "-L",
    "--log-level",
    "log_level",
    help="Log level (ignored, kept for compatibility)",
    default="info",
)
@click.argument("pattern", required=True)
@click.argument("warc_files", nargs=-1, type=click.Path(exists=True))
def main(
    limit: str | None,
    input_format: str | None,
    invert: bool,
    url: bool,
    type_flag: bool,
    content_type: bool,
    http_content_type: bool,
    warc_date: bool,
    log_level: str,
    pattern: str,
    warc_files: tuple[str, ...],
) -> None:
    """Filter WARC files by regex pattern."""
    out = sys.stdout.buffer

    pattern_bytes = pattern.encode()
    pattern_re = re.compile(pattern_bytes)

    if not warc_files:
        fh = WarcRecord.open_archive(file_handle=sys.stdin, gzip=None)
        filter_archive(
            fh,
            pattern_re,
            out,
            invert,
            url,
            type_flag,
            content_type,
            http_content_type,
            warc_date,
        )
    else:
        for name in expand_files(warc_files):
            fh = WarcRecord.open_archive(name, gzip="auto")
            filter_archive(
                fh,
                pattern_re,
                out,
                invert,
                url,
                type_flag,
                content_type,
                http_content_type,
                warc_date,
            )
            fh.close()


def run() -> None:
    """Entry point for the command-line interface."""
    main()


if __name__ == "__main__":
    run()
