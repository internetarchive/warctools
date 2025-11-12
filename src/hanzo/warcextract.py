#!/usr/bin/env python
"""warcextract - dump warc record context to standard out"""

import sys
from contextlib import closing

import click

from .warctools import WarcRecord


def dump_record(fh, out, name: str = "-") -> None:
    """Dump a single record to output."""
    for offset, record, errors in fh.read_records(limit=1, offsets=False):
        if record:
            out.write(record.content[1])
        elif errors:
            print(
                f"warc errors at {name}:{offset if offset else 0}",
                file=sys.stderr,
            )
            for e in errors:
                print("\t", e, file=sys.stderr)
        break  # only use one record


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "-I",
    "--input",
    "input_format",
    help="Input format (ignored, kept for compatibility)",
    default=None,
)
@click.option(
    "-L",
    "--log-level",
    "log_level",
    help="Log level (ignored, kept for compatibility)",
    default="info",
)
@click.argument("warc_file", required=False, type=click.Path(exists=True))
@click.argument("offset", required=False, type=int, default=0)
def main(
    input_format: str | None,
    log_level: str,
    warc_file: str | None,
    offset: int,
) -> None:
    """Extract WARC record content to stdout."""
    out = sys.stdout.buffer

    if warc_file is None:
        # dump the first record on stdin
        with closing(WarcRecord.open_archive(file_handle=sys.stdin, gzip=None)) as fh:
            dump_record(fh, out, name="-")
    else:
        # dump a record from the filename, with optional offset
        with closing(WarcRecord.open_archive(filename=warc_file, gzip="auto")) as fh:
            fh.seek(offset)
            dump_record(fh, out, name=warc_file)


def run() -> None:
    """Entry point for the command-line interface."""
    main()


if __name__ == "__main__":
    run()
