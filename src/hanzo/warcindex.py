#!/usr/bin/env python
"""warcindex - dump warc index

This tool outputs a simple index format with offsets for random access to WARC records.
WARC Format Specification: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/
"""

import sys

import click

from .warctools import WarcRecord, expand_files


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "-l",
    "--limit",
    "limit",
    help="Limit number of records (ignored, kept for compatibility)",
    default=None,
)
@click.option(
    "-O",
    "--output-format",
    "output_format",
    help="Output format (ignored, kept for compatibility)",
    default=None,
)
@click.option(
    "-o",
    "--output",
    "output_file",
    help="Output file (ignored, kept for compatibility)",
    default=None,
)
@click.option(
    "-L",
    "--log-level",
    "log_level",
    help="Log level (ignored, kept for compatibility)",
    default="info",
)
@click.argument("warc_files", nargs=-1, required=True, type=click.Path(exists=True))
def main(
    limit: str | None,
    output_format: str | None,
    output_file: str | None,
    log_level: str,
    warc_files: tuple[str, ...],
) -> None:
    """Dump WARC index."""
    out = sys.stdout.buffer

    out.write(
        b"#WARC filename offset warc-type warc-subject-uri warc-record-id content-type content-length\n"
    )
    for name in expand_files(warc_files):
        fh = WarcRecord.open_archive(name, gzip="auto")

        try:
            for offset, record, _errors in fh.read_records(limit=None):
                if record:
                    fields = [
                        name.encode("utf-8"),
                        str(offset).encode("utf-8"),
                        record.type or b"-",
                        record.url or b"-",
                        record.id or b"-",
                        record.content_type or b"-",
                        str(record.content_length).encode("utf-8"),
                    ]
                    out.write(b" ".join(fields) + b"\n")
                # ignore errors and tail

        finally:
            fh.close()


def run() -> None:
    """Entry point for the command-line interface."""
    main()


if __name__ == "__main__":
    run()
