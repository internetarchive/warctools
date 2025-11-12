#!/usr/bin/env python
"""warc2warc - convert one warc to another, can be used to re-compress things"""

import sys

import click

from .httptools import RequestMessage, ResponseMessage
from .warctools import WarcRecord, expand_files

WGET_IGNORE_HEADERS = ["Transfer-Encoding"]


def process(record, out, gzip: bool, decode_http: bool, wget_workaround: bool) -> None:
    """Process a single WARC record."""
    ignore_headers = WGET_IGNORE_HEADERS if wget_workaround else ()
    if decode_http:
        if record.type == WarcRecord.RESPONSE:
            content_type, content = record.content
            message = None
            if content_type == ResponseMessage.CONTENT_TYPE:
                # technically, a http request needs to know the request to be parsed
                # because responses to head requests don't have a body.
                # we assume we don't store 'head' responses, and plough on
                message = ResponseMessage(RequestMessage(), ignore_headers=ignore_headers)
            if content_type == RequestMessage.CONTENT_TYPE:
                message = RequestMessage(ignore_headers=ignore_headers)

            if message:
                leftover = message.feed(content)
                message.close()
                if not leftover and message.complete():
                    content = message.get_decoded_message()
                    record.content = content_type, content
                else:
                    error = []
                    if leftover:
                        error.append(f"{len(leftover)} bytes unparsed")
                    if not message.complete():
                        error.append(
                            f"incomplete message (at {message.mode}, {message.header.mode})"
                        )
                    print(
                        f"errors decoding http in record {record.id} {','.join(error)}",
                        file=sys.stderr,
                    )

    record.write_to(out, gzip=gzip)


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
    "-I",
    "--input",
    "input_format",
    help="Input format (ignored, kept for compatibility)",
    default=None,
)
@click.option(
    "-Z",
    "--gzip",
    "gzip",
    is_flag=True,
    help="compress output, record by record",
    default=False,
)
@click.option(
    "-D",
    "--decode_http",
    "decode_http",
    is_flag=True,
    help="decode http messages (strip chunks, gzip)",
    default=False,
)
@click.option(
    "-L",
    "--log-level",
    "log_level",
    help="Log level (ignored, kept for compatibility)",
    default="info",
)
@click.option(
    "--wget-chunk-fix",
    "wget_workaround",
    is_flag=True,
    help="skip transfer-encoding headers in http records, when decoding them (-D)",
    default=False,
)
@click.argument("warc_files", nargs=-1, type=click.Path(exists=True))
def main(
    output: str | None,
    limit: str | None,
    input_format: str | None,
    gzip: bool,
    decode_http: bool,
    log_level: str,
    wget_workaround: bool,
    warc_files: tuple[str, ...],
) -> None:
    """Convert one WARC to another, can be used to re-compress things."""
    out = sys.stdout.buffer
    if output:
        out = open(output, "wb")

    try:
        if len(warc_files) < 1:
            fh = WarcRecord.open_archive(file_handle=sys.stdin, gzip=None)
            for record in fh:
                process(record, out, gzip, decode_http, wget_workaround)
        else:
            for name in expand_files(warc_files):
                fh = WarcRecord.open_archive(name, gzip="auto")
                for record in fh:
                    process(record, out, gzip, decode_http, wget_workaround)
                fh.close()
    finally:
        if output and out != sys.stdout.buffer:
            out.close()


def run() -> None:
    """Entry point for the command-line interface."""
    main()


if __name__ == "__main__":
    run()
