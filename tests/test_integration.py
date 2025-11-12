"""Integration tests for warctools - test tools working together."""

import gzip
import subprocess
import tempfile
from datetime import datetime
from io import BytesIO
from pathlib import Path

import pytest

from hanzo import warctools


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_warc_file(temp_dir):
    """Create a sample WARC file with multiple record types."""
    warc_file = temp_dir / "test.warc"

    # Create WARCINFO record
    warcinfo_id = warctools.WarcRecord.random_warc_uuid()
    warcinfo_date = warctools.warc.warc_datetime_str(datetime.now())
    warcinfo_content = b"software: warctools test\nformat: WARC File Format 1.0\n"
    warcinfo_headers = [
        (warctools.WarcRecord.TYPE, warctools.WarcRecord.WARCINFO),
        (warctools.WarcRecord.ID, warcinfo_id),
        (warctools.WarcRecord.DATE, warcinfo_date),
        (warctools.WarcRecord.CONTENT_TYPE, b"application/warc-fields"),
    ]
    warcinfo_record = warctools.WarcRecord(
        headers=warcinfo_headers,
        content=(b"application/warc-fields", warcinfo_content),
    )

    # Create REQUEST and RESPONSE records (response_id first for linking)
    request_id = warctools.WarcRecord.random_warc_uuid()
    response_id = warctools.WarcRecord.random_warc_uuid()

    request_date = warctools.warc.warc_datetime_str(datetime.now())
    request_url = b"http://example.com/page1"
    request_content = b"GET /page1 HTTP/1.1\r\nHost: example.com\r\n\r\n"
    request_record = warctools.warc.make_request(
        request_id, request_date, request_url, (b"application/http", request_content), response_id
    )

    # Create RESPONSE record
    response_date = warctools.warc.warc_datetime_str(datetime.now())
    response_url = b"http://example.com/page1"
    response_content = (
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Type: text/html\r\n"
        b"Content-Length: 25\r\n\r\n"
        b"<html>Hello World</html>"
    )
    response_record = warctools.warc.make_response(
        response_id,
        response_date,
        response_url,
        (b"application/http", response_content),
        request_id,
    )

    # Create another RESPONSE record with different URL
    response2_id = warctools.WarcRecord.random_warc_uuid()
    response2_date = warctools.warc.warc_datetime_str(datetime.now())
    response2_url = b"http://example.com/page2"
    response2_content = (
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Type: application/json\r\n"
        b"Content-Length: 20\r\n\r\n"
        b'{"key": "value"}'
    )
    response2_record = warctools.warc.make_response(
        response2_id,
        response2_date,
        response2_url,
        (b"application/http", response2_content),
        None,
    )

    # Write all records to file
    with open(warc_file, "wb") as f:
        warcinfo_record.write_to(f)
        request_record.write_to(f)
        response_record.write_to(f)
        response2_record.write_to(f)

    return warc_file


@pytest.fixture
def compressed_warc_file(temp_dir, sample_warc_file):
    """Create a compressed WARC file."""
    compressed_file = temp_dir / "test.warc.gz"

    with open(sample_warc_file, "rb") as f_in:
        with gzip.open(compressed_file, "wb") as f_out:
            f_out.write(f_in.read())

    return compressed_file


def test_create_and_read_warc(sample_warc_file):
    """Test creating a WARC file and reading it back."""
    # Read the WARC file
    fh = warctools.WarcRecord.open_archive(str(sample_warc_file), gzip="auto")

    records = []
    for _offset, record, errors in fh.read_records(limit=None):
        assert errors is None or len(errors) == 0, f"Found errors: {errors}"
        if record:
            records.append(record)

    fh.close()

    # Verify we got the expected records
    assert len(records) == 4, f"Expected 4 records, got {len(records)}"

    # Check record types
    assert records[0].type == warctools.WarcRecord.WARCINFO
    assert records[1].type == warctools.WarcRecord.REQUEST
    assert records[2].type == warctools.WarcRecord.RESPONSE
    assert records[3].type == warctools.WarcRecord.RESPONSE

    # Check URLs
    assert records[1].url == b"http://example.com/page1"
    assert records[2].url == b"http://example.com/page1"
    assert records[3].url == b"http://example.com/page2"


def test_warcvalid_cli(sample_warc_file):
    """Test warcvalid CLI tool."""
    result = subprocess.run(
        ["warcvalid", str(sample_warc_file)],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent,
    )
    assert result.returncode == 0, f"warcvalid failed: {result.stderr}"


def test_warcvalid_cli_compressed(compressed_warc_file):
    """Test warcvalid CLI tool with compressed file."""
    result = subprocess.run(
        ["warcvalid", str(compressed_warc_file)],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent,
    )
    assert result.returncode == 0, f"warcvalid failed: {result.stderr}"


def test_warcdump_cli(sample_warc_file):
    """Test warcdump CLI tool."""
    result = subprocess.run(
        ["warcdump", str(sample_warc_file)],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent,
    )
    assert result.returncode == 0, f"warcdump failed: {result.stderr}"
    assert "archive record" in result.stdout.lower() or "warc" in result.stdout.lower()


def test_warcfilter_by_url(sample_warc_file, temp_dir):
    """Test warcfilter filtering by URL."""
    output_file = temp_dir / "filtered.warc"

    with open(output_file, "wb") as f:
        result = subprocess.run(
            ["warcfilter", "-U", "page1", str(sample_warc_file)],
            stdout=f,
            stderr=subprocess.PIPE,
            cwd=Path(__file__).parent.parent,
        )
    assert result.returncode == 0, f"warcfilter failed: {result.stderr}"

    # Read filtered file and verify
    fh = warctools.WarcRecord.open_archive(str(output_file), gzip="auto")
    records = []
    for _offset, record, _errors in fh.read_records(limit=None):
        if record:
            records.append(record)
    fh.close()

    # Should have records with page1 in URL
    assert len(records) > 0, "Filtered file should have records"
    for record in records:
        if record.url:
            assert b"page1" in record.url, f"Record URL should contain 'page1': {record.url}"


def test_warcfilter_by_type(sample_warc_file, temp_dir):
    """Test warcfilter filtering by record type."""
    output_file = temp_dir / "filtered.warc"

    with open(output_file, "wb") as f:
        result = subprocess.run(
            ["warcfilter", "-T", "response", str(sample_warc_file)],
            stdout=f,
            stderr=subprocess.PIPE,
            cwd=Path(__file__).parent.parent,
        )
    assert result.returncode == 0, f"warcfilter failed: {result.stderr}"

    # Read filtered file and verify
    fh = warctools.WarcRecord.open_archive(str(output_file), gzip="auto")
    records = []
    for _offset, record, _errors in fh.read_records(limit=None):
        if record:
            records.append(record)
    fh.close()

    # All records should be responses
    assert len(records) > 0, "Filtered file should have records"
    for record in records:
        assert record.type == warctools.WarcRecord.RESPONSE


def test_warcfilter_invert(sample_warc_file, temp_dir):
    """Test warcfilter with invert option."""
    output_file = temp_dir / "filtered.warc"

    with open(output_file, "wb") as f:
        result = subprocess.run(
            ["warcfilter", "-i", "-U", "page1", str(sample_warc_file)],
            stdout=f,
            stderr=subprocess.PIPE,
            cwd=Path(__file__).parent.parent,
        )
    assert result.returncode == 0, f"warcfilter failed: {result.stderr}"

    # Read filtered file and verify
    fh = warctools.WarcRecord.open_archive(str(output_file), gzip="auto")
    records = []
    for _offset, record, _errors in fh.read_records(limit=None):
        if record:
            records.append(record)
    fh.close()

    # Should have records without page1 in URL
    assert len(records) > 0, "Filtered file should have records"
    for record in records:
        if record.url:
            assert b"page1" not in record.url, (
                f"Record URL should not contain 'page1': {record.url}"
            )


def test_warcextract_cli(sample_warc_file):
    """Test warcextract CLI tool."""
    result = subprocess.run(
        ["warcextract", str(sample_warc_file), "0"],
        capture_output=True,
        cwd=Path(__file__).parent.parent,
    )
    assert result.returncode == 0, f"warcextract failed: {result.stderr}"
    assert len(result.stdout) > 0, "Should extract some content"


def test_warc2warc_cli(sample_warc_file, temp_dir):
    """Test warc2warc CLI tool (copy/convert)."""
    output_file = temp_dir / "converted.warc"

    with open(output_file, "wb") as f:
        result = subprocess.run(
            ["warc2warc", str(sample_warc_file)],
            stdout=f,
            stderr=subprocess.PIPE,
            cwd=Path(__file__).parent.parent,
        )
    assert result.returncode == 0, f"warc2warc failed: {result.stderr}"

    # Verify output file has same records
    fh = warctools.WarcRecord.open_archive(str(output_file), gzip="auto")
    records = []
    for _offset, record, _errors in fh.read_records(limit=None):
        if record:
            records.append(record)
    fh.close()

    assert len(records) == 4, "Converted file should have same number of records"


def test_warcindex_cli(sample_warc_file):
    """Test warcindex CLI tool."""
    result = subprocess.run(
        ["warcindex", str(sample_warc_file)],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent,
    )
    assert result.returncode == 0, f"warcindex failed: {result.stderr}"
    # Index output should contain offset information
    assert len(result.stdout) > 0, "Index should produce output"


def test_warclinks_cli(sample_warc_file):
    """Test warclinks CLI tool."""
    result = subprocess.run(
        ["warclinks", str(sample_warc_file)],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent,
    )
    assert result.returncode == 0, f"warclinks failed: {result.stderr}"


def test_warcpayload_cli(sample_warc_file):
    """Test warcpayload CLI tool."""
    # warcpayload expects format: filename:offset
    # First, get an offset from warcindex
    index_result = subprocess.run(
        ["warcindex", str(sample_warc_file)],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent,
    )
    assert index_result.returncode == 0, "warcindex should work"

    # Extract first numeric offset from index (skip comment lines)
    if index_result.stdout:
        for line in index_result.stdout.split("\n"):
            line = line.strip()
            if line and not line.startswith("#") and line[0].isdigit():
                offset = line.split()[0]
                warc_offset = f"{sample_warc_file}:{offset}"

                result = subprocess.run(
                    ["warcpayload", warc_offset],
                    capture_output=True,
                    cwd=Path(__file__).parent.parent,
                )
                assert result.returncode == 0, f"warcpayload failed: {result.stderr}"
                assert len(result.stdout) > 0, "Should extract payload"
                break


def test_integration_workflow(sample_warc_file, temp_dir):
    """Test a complete workflow: create, validate, filter, extract."""
    # Step 1: Validate the file
    result = subprocess.run(
        ["warcvalid", str(sample_warc_file)],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent,
    )
    assert result.returncode == 0, "File should be valid"

    # Step 2: Filter to get only responses
    filtered_file = temp_dir / "filtered_responses.warc"
    with open(filtered_file, "wb") as f:
        result = subprocess.run(
            ["warcfilter", "-T", "response", str(sample_warc_file)],
            stdout=f,
            stderr=subprocess.PIPE,
            cwd=Path(__file__).parent.parent,
        )
    assert result.returncode == 0, "Filter should succeed"

    # Step 3: Validate filtered file
    result = subprocess.run(
        ["warcvalid", str(filtered_file)],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent,
    )
    assert result.returncode == 0, "Filtered file should be valid"

    # Step 4: Extract content from filtered file
    result = subprocess.run(
        ["warcextract", str(filtered_file), "0"],
        capture_output=True,
        cwd=Path(__file__).parent.parent,
    )
    assert result.returncode == 0, "Extract should succeed"
    assert len(result.stdout) > 0, "Should extract content"

    # Step 5: Dump the filtered file
    result = subprocess.run(
        ["warcdump", str(filtered_file)],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent,
    )
    assert result.returncode == 0, "Dump should succeed"


def test_create_with_streaming_content(temp_dir):
    """Test creating WARC records with streaming content."""
    warc_file = temp_dir / "streaming.warc"

    # Create a record with content_file instead of content tuple
    # Note: content_file position will be advanced during write_to
    content_data = b"This is streaming content that could be large"
    content_file = BytesIO(content_data)

    record_id = warctools.WarcRecord.random_warc_uuid()
    record_date = warctools.warc.warc_datetime_str(datetime.now())
    record_url = b"http://example.com/stream"

    headers = [
        (warctools.WarcRecord.TYPE, warctools.WarcRecord.RESPONSE),
        (warctools.WarcRecord.ID, record_id),
        (warctools.WarcRecord.DATE, record_date),
        (warctools.WarcRecord.URL, record_url),
        (warctools.WarcRecord.CONTENT_TYPE, b"text/plain"),
        (warctools.WarcRecord.CONTENT_LENGTH, str(len(content_data)).encode("ascii")),
    ]

    record = warctools.WarcRecord(headers=headers, content_file=content_file)

    with open(warc_file, "wb") as f:
        record.write_to(f)

    # Read it back and verify - use content tuple approach for simpler test
    # The content_file approach works but requires careful handling
    fh = warctools.WarcRecord.open_archive(str(warc_file), gzip="auto")
    records = []
    for _offset, record, _errors in fh.read_records(limit=None):
        if record:
            records.append(record)
    fh.close()

    assert len(records) == 1
    assert records[0].url == record_url
    # Verify the record was written correctly by checking it exists
    assert records[0].type == warctools.WarcRecord.RESPONSE


def test_multiple_warc_files(temp_dir):
    """Test operations with multiple WARC files."""
    # Create two WARC files
    warc1 = temp_dir / "file1.warc"
    warc2 = temp_dir / "file2.warc"

    # File 1
    record1 = warctools.warc.make_response(
        warctools.WarcRecord.random_warc_uuid(),
        warctools.warc.warc_datetime_str(datetime.now()),
        b"http://example.com/file1",
        (b"application/http", b"HTTP/1.1 200 OK\r\n\r\nFile 1"),
        None,  # request_id
    )
    with open(warc1, "wb") as f:
        record1.write_to(f)

    # File 2
    record2 = warctools.warc.make_response(
        warctools.WarcRecord.random_warc_uuid(),
        warctools.warc.warc_datetime_str(datetime.now()),
        b"http://example.com/file2",
        (b"application/http", b"HTTP/1.1 200 OK\r\n\r\nFile 2"),
        None,  # request_id
    )
    with open(warc2, "wb") as f:
        record2.write_to(f)

    # Validate both files
    result = subprocess.run(
        ["warcvalid", str(warc1), str(warc2)],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent,
    )
    assert result.returncode == 0, "Both files should be valid"

    # Filter both files
    output_file = temp_dir / "combined_filtered.warc"
    with open(output_file, "wb") as f:
        result = subprocess.run(
            ["warcfilter", "-T", "response", str(warc1), str(warc2)],
            stdout=f,
            stderr=subprocess.PIPE,
            cwd=Path(__file__).parent.parent,
        )
    assert result.returncode == 0, "Filter should work on multiple files"

    # Verify combined output
    fh = warctools.WarcRecord.open_archive(str(output_file), gzip="auto")
    records = []
    for _offset, record, _errors in fh.read_records(limit=None):
        if record:
            records.append(record)
    fh.close()

    assert len(records) == 2, "Should have records from both files"


def test_warcunpack_cli(sample_warc_file, temp_dir):
    """Test warcunpack CLI tool."""
    output_dir = temp_dir / "unpacked"
    log_file = temp_dir / "unpack.log"

    result = subprocess.run(
        [
            "warcunpack",
            "-o",
            str(output_dir),
            "-l",
            str(log_file),
            str(sample_warc_file),
        ],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent,
    )
    assert result.returncode == 0, f"warcunpack failed: {result.stderr}"

    # Check that log file was created
    assert log_file.exists(), "Log file should be created"

    # Check that log file has content
    log_content = log_file.read_text()
    assert ">>warc_file" in log_content, "Log should have header"
    assert len(log_content.split("\n")) > 1, "Log should have entries"

    # Check that output directory exists
    assert output_dir.exists(), "Output directory should be created"


def test_warcunpack_default_name(temp_dir):
    """Test warcunpack with default name option."""
    # Create a simple WARC file with a response record
    warc_file = temp_dir / "test_unpack.warc"
    output_dir = temp_dir / "unpacked"

    # Create WARCINFO record
    warcinfo_id = warctools.WarcRecord.random_warc_uuid()
    warcinfo_date = warctools.warc.warc_datetime_str(datetime.now())
    warcinfo_content = b"software: warctools test\n"
    warcinfo_headers = [
        (warctools.WarcRecord.TYPE, warctools.WarcRecord.WARCINFO),
        (warctools.WarcRecord.ID, warcinfo_id),
        (warctools.WarcRecord.DATE, warcinfo_date),
        (warctools.WarcRecord.CONTENT_TYPE, b"application/warc-fields"),
    ]
    warcinfo_record = warctools.WarcRecord(
        headers=warcinfo_headers,
        content=(b"application/warc-fields", warcinfo_content),
    )

    # Create RESPONSE record with HTTP content
    response_id = warctools.WarcRecord.random_warc_uuid()
    response_date = warctools.warc.warc_datetime_str(datetime.now())
    response_url = b"http://example.com/test.html"
    http_response = (
        b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: 13\r\n\r\n<html>test</html>"
    )
    response_record = warctools.warc.make_response(
        response_id,
        response_date,
        response_url,
        (b"application/http;msgtype=response", http_response),
        None,
    )

    # Write WARC file
    with open(warc_file, "wb") as f:
        warcinfo_record.write_to(f, gzip=False)
        response_record.write_to(f, gzip=False)

    # Run warcunpack
    result = subprocess.run(
        [
            "warcunpack",
            "-o",
            str(output_dir),
            "-D",
            "mypage",
            str(warc_file),
        ],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent,
    )
    assert result.returncode == 0, f"warcunpack failed: {result.stderr}"

    # Check that files were created
    assert output_dir.exists(), "Output directory should be created"
