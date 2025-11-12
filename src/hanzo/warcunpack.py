#!/usr/bin/env python
"""warcunpack - unpack WARC records to directory structure"""

import mimetypes
import os
import os.path
import shlex
import sys
import uuid
from pathlib import Path

import click

from .httptools import RequestMessage, ResponseMessage
from .warctools import ArchiveRecord, WarcRecord

mimetypes.add_type("text/javascript", ".js")


def log_headers(log_file):
    """Write log file header."""
    print(
        ">>warc_file\twarc_id\twarc_type\twarc_content_length\twarc_uri_date\twarc_subject_uri\turi_content_type\toutfile\twayback_uri",
        file=log_file,
    )


def log_entry(log_file, input_file, record, content_type, output_file, wayback_uri):
    """Write a log entry for an unpacked record."""
    log = (
        input_file,
        record.id.decode("utf-8", errors="replace") if record.id else "",
        record.type.decode("utf-8", errors="replace") if record.type else "",
        record.content_length,
        record.date.decode("utf-8", errors="replace") if record.date else "",
        record.url.decode("utf-8", errors="replace") if record.url else "",
        content_type or "",
        output_file,
        wayback_uri,
    )
    print("\t".join(str(s) for s in log), file=log_file)


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "-D",
    "--default-name",
    "default_name",
    help="Default filename for records without URL",
    default="crawlerdefault",
)
@click.option(
    "-o",
    "--output",
    "output",
    help="Output directory (default: current directory)",
    type=click.Path(),
    default=None,
)
@click.option(
    "-l",
    "--log",
    "log_file",
    help="Log file path (default: auto-generated or stdout)",
    type=click.Path(),
    default=None,
)
@click.option(
    "-W",
    "--wayback-prefix",
    "wayback_prefix",
    help="Wayback URL prefix",
    default="http://wayback.archive-it.org/",
)
@click.argument("warc_files", nargs=-1, type=click.Path(exists=True))
def main(
    default_name: str,
    output: str | None,
    log_file: str | None,
    wayback_prefix: str,
    warc_files: tuple[str, ...],
) -> None:
    """Unpack WARC records to directory structure.

    Extracts HTTP response records from WARC files and writes them to a directory
    structure based on the URL. Creates a log file with metadata about each
    extracted record.

    If no WARC files are provided, reads from stdin.
    """
    if output:
        output_dir = Path(output)
        output_dir.mkdir(parents=True, exist_ok=True)
    else:
        output_dir = Path.cwd()

    collisions = 0

    if len(warc_files) < 1:
        # Read from stdin
        log_fh = sys.stdout if not log_file else open(log_file, "w", encoding="utf-8")
        log_headers(log_fh)

        fh = WarcRecord.open_archive(file_handle=sys.stdin, gzip=None)
        try:
            collisions += unpack_records(
                "<stdin>",
                fh,
                output_dir,
                default_name,
                log_fh,
                wayback_prefix,
            )
        finally:
            fh.close()

        if log_file:
            log_fh.close()
    else:
        # Process each WARC file
        for filename in warc_files:
            if log_file:
                log_path = Path(log_file)
            else:
                log_path = output_dir / f"{Path(filename).stem}.index.txt"

            with open(log_path, "w", encoding="utf-8") as log_fh:
                log_headers(log_fh)
                try:
                    fh = ArchiveRecord.open_archive(filename=filename, gzip="auto")
                    try:
                        collisions += unpack_records(
                            filename,
                            fh,
                            output_dir,
                            default_name,
                            log_fh,
                            wayback_prefix,
                        )
                    finally:
                        fh.close()
                except Exception as e:
                    print(f"exception in handling {filename}: {e}", file=sys.stderr)
                    raise

    if collisions:
        print(f"{collisions} filenames that collided", file=sys.stderr)

    sys.exit(0 if collisions == 0 else 1)


def unpack_records(name, fh, output_dir, default_name, output_log, wayback_prefix):
    """Unpack records from archive to directory structure."""
    collection_id = ""
    collisions = 0
    for offset, record, errors in fh.read_records(limit=None):
        if record:
            try:
                content_type, content = record.content

                if record.type == WarcRecord.WARCINFO:
                    info = parse_warcinfo(record)
                    for entry in shlex.split(info.get("description", "")):
                        if entry.startswith("collectionId"):
                            collection_id = entry.split("=", 1)[1].split(",")[0]
                    if not collection_id:
                        filename_header = record.get_header(b"WARC-Filename")
                        if filename_header:
                            filename = filename_header.decode("utf-8", errors="replace")
                            parts = filename.split("-")
                            if len(parts) > 1:
                                collection_id = parts[1]
                        elif "-" in name:
                            parts = name.split("-")
                            if len(parts) > 1:
                                collection_id = parts[1]

                if (
                    record.type == WarcRecord.RESPONSE
                    and content_type
                    and content_type.startswith(b"application/http")
                ):
                    code, mime_type, message = parse_http_response(record)

                    if 200 <= code < 300:
                        url = record.url.decode("utf-8", errors="replace") if record.url else ""
                        filename, collision = output_file(output_dir, url, mime_type, default_name)
                        if collision:
                            collisions += 1

                        wayback_uri = ""
                        if collection_id:
                            date_str = (
                                record.date.decode("utf-8", errors="replace") if record.date else ""
                            )
                            # Remove T, Z, :, - from date for wayback format
                            wayback_date = date_str.translate(str.maketrans("", "", "TZ:-"))
                            wayback_uri = f"{wayback_prefix}{collection_id}/{wayback_date}/{url}"

                        with open(filename, "wb") as out:
                            out.write(message.get_body())
                            log_entry(
                                output_log,
                                name,
                                record,
                                mime_type,
                                str(filename),
                                wayback_uri,
                            )

            except Exception as e:
                import traceback

                traceback.print_exc()
                print(f"exception in handling record: {e}", file=sys.stderr)

        elif errors:
            print(
                f"warc errors at {name}:{offset if offset else 0}",
                end=" ",
                file=sys.stderr,
            )
            for e in errors:
                print(e, end=" ", file=sys.stderr)
            print(file=sys.stderr)
    return collisions


def parse_warcinfo(record):
    """Parse warcinfo record content into dictionary."""
    info = {}
    try:
        content_bytes = record.content[1]
        if isinstance(content_bytes, bytes):
            content_text = content_bytes.decode("utf-8", errors="replace")
        else:
            content_text = content_bytes
        for line in content_text.split("\n"):
            line = line.strip()
            if line:
                try:
                    key, value = line.split(":", 1)
                    info[key.strip()] = value.strip()
                except Exception:
                    print(f"malformed warcinfo line: {line}", file=sys.stderr)
    except Exception as e:
        print(f"exception reading warcinfo record: {e}", file=sys.stderr)
    return info


def parse_http_response(record):
    """Parse HTTP response from WARC record."""
    message = ResponseMessage(RequestMessage())
    content_bytes = record.content[1]
    remainder = message.feed(content_bytes)
    message.close()
    if remainder or not message.complete():
        url = record.url.decode("utf-8", errors="replace") if record.url else "unknown"
        if remainder:
            print(
                f"warning: trailing data in http response for {url}",
                file=sys.stderr,
            )
        if not message.complete():
            print(f"warning: truncated http response for {url}", file=sys.stderr)

    header = message.header

    mime_type = None
    for k, v in header.headers:
        if k.lower() == b"content-type":
            mime_type = v.decode("utf-8", errors="replace").split(";")[0].strip()
            break

    return header.code, mime_type, message


def output_file(output_dir, url, mime_type, default_name):
    """Generate output filename from URL and MIME type."""
    # Clean URL for filesystem
    clean_url = "".join(
        (c if c.isalnum() or c in "_-/." else "_") for c in url.replace("://", "/", 1)
    )

    parts = clean_url.split("/")
    directories, filename = parts[:-1], parts[-1]

    path = [output_dir]
    for d in directories:
        if d:
            path.append(d)

    if filename:
        name, ext = os.path.splitext(filename)
    else:
        name, ext = default_name, ""

    if mime_type:
        guess_type, _ = mimetypes.guess_type(url)
        # Preserve variant file extensions, rather than clobber with default for mime type
        if not ext or guess_type != mime_type:
            mime_ext = mimetypes.guess_extension(mime_type)
            if mime_ext:
                ext = mime_ext
    elif not ext:
        ext = ".html"  # no mime type, no extension

    directory = os.path.normpath(os.path.join(*path))
    # Limit directory path length
    directory = directory[:200]

    os.makedirs(directory, exist_ok=True)

    # Limit filename length (45 chars for name + extension)
    filename = name[: 45 - len(ext)] + ext

    fullname = os.path.join(directory, filename)

    collision = False

    while os.path.exists(fullname):
        collision = True
        u = str(uuid.uuid4())[:8]

        filename = name[: 45 - len(ext)] + "_R" + u + ext

        fullname = os.path.join(directory, filename)

    return os.path.realpath(os.path.normpath(fullname)), collision


def run() -> None:
    """Entry point for the command-line interface."""
    main()


if __name__ == "__main__":
    run()
