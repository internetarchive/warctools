#!/usr/bin/env python
"""warcvalid - check a warc is ok"""

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
@click.argument("warc_files", nargs=-1, required=True, type=click.Path(exists=True))
def main(
    limit: str | None,
    input_format: str | None,
    log_level: str,
    warc_files: tuple[str, ...],
) -> None:
    """Validate WARC files."""
    correct = True
    fh = None
    try:
        for name in expand_files(warc_files):
            fh = WarcRecord.open_archive(name, gzip="auto")

            for offset, record, errors in fh.read_records(limit=None):
                if errors:
                    print(f"warc errors at {name}:{offset}", file=sys.stderr)
                    print(errors, file=sys.stderr)
                    correct = False
                    break
                elif record is not None and record.validate():
                    # validate() returns errors if any
                    print(f"warc errors at {name}:{offset}", file=sys.stderr)
                    print(record.validate(), file=sys.stderr)
                    correct = False
                    break

    except Exception as e:
        print(f"Exception: {str(e)}", file=sys.stderr)
        correct = False
    finally:
        if fh:
            fh.close()

    sys.exit(0 if correct else -1)


def run() -> None:
    """Entry point for the command-line interface."""
    main()


if __name__ == "__main__":
    run()
