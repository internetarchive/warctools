#!/usr/bin/env python
"""warcdump - dump warcs in a slightly more humane format"""

import sys

import click

from .warctools import WarcRecord, expand_files


def dump_archive(fh, name: str, offsets: bool = True) -> None:
    """Dump archive records to stdout."""
    for offset, record, errors in fh.read_records(limit=None, offsets=offsets):
        if record:
            print(f"archive record at {name}:{offset}")
            record.dump(content=True)
        elif errors:
            print(f"warc errors at {name}:{offset if offset else 0}")
            for e in errors:
                print("\t", e)
        else:
            print()
            print("note: no errors encountered in tail of file")


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
    "-L",
    "--log-level",
    "log_level",
    help="Log level (ignored, kept for compatibility)",
    default="info",
)
@click.argument("warc_files", nargs=-1, type=click.Path(exists=True))
def main(
    limit: str | None,
    input_format: str | None,
    log_level: str,
    warc_files: tuple[str, ...],
) -> None:
    """Dump WARC files in a human-readable format."""
    if len(warc_files) < 1:
        dump_archive(
            WarcRecord.open_archive(file_handle=sys.stdin, gzip=None),
            name="-",
            offsets=False,
        )
    else:
        for name in expand_files(warc_files):
            fh = WarcRecord.open_archive(name, gzip="auto")
            dump_archive(fh, name)
            fh.close()


def run() -> None:
    """Entry point for the command-line interface."""
    main()


if __name__ == "__main__":
    run()
