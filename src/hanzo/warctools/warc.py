"""An object to represent warc records, using the abstract record in
record.py

WARC Format Specification References:
- WARC 1.1 Annotated (primary): https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/
- WARC 1.1: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/
- WARC 1.0: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.0/
"""

import hashlib
import re
import uuid

from hanzo.warctools.archive_detect import register_record_type
from hanzo.warctools.record import ArchiveParser, ArchiveRecord

bad_lines = 5  # when to give up looking for the version stamp


# WARC Named Fields - See WARC 1.1 Section 5 "Named fields"
# https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#named-fields
@ArchiveRecord.HEADERS(
    # Mandatory fields (Section 5.2-5.5):
    DATE=b"WARC-Date",  # Section 5.3: WARC-Date (mandatory)
    TYPE=b"WARC-Type",  # Section 5.4: WARC-Type (mandatory)
    ID=b"WARC-Record-ID",  # Section 5.2: WARC-Record-ID (mandatory)
    CONTENT_LENGTH=b"Content-Length",  # Section 5.5: Content-Length (mandatory)
    # Optional fields:
    CONTENT_TYPE=b"Content-Type",  # Section 5.6: Content-Type
    CONCURRENT_TO=b"WARC-Concurrent-To",  # Section 5.7: WARC-Concurrent-To
    REFERS_TO=b"WARC-Refers-To",  # Section 5.8: WARC-Refers-To
    REFERS_TO_TARGET_URI=b"WARC-Refers-To-Target-URI",  # Section 5.9: WARC-Refers-To-Target-URI (WARC 1.1)
    REFERS_TO_DATE=b"WARC-Refers-To-Date",  # Section 5.10: WARC-Refers-To-Date (WARC 1.1)
    URL=b"WARC-Target-URI",  # Section 5.13: WARC-Target-URI
    BLOCK_DIGEST=b"WARC-Block-Digest",  # Section 5.9: WARC-Block-Digest
    PAYLOAD_DIGEST=b"WARC-Payload-Digest",  # Section 5.10: WARC-Payload-Digest
    IP_ADDRESS=b"WARC-IP-Address",  # Section 5.11: WARC-IP-Address
    FILENAME=b"WARC-Filename",  # Section 5.12: WARC-Filename
    WARCINFO_ID=b"WARC-Warcinfo-ID",  # Section 5.14: WARC-Warcinfo-ID
    PROFILE=b"WARC-Profile",  # Section 5.15: WARC-Profile
)
class WarcRecord(ArchiveRecord):
    # Pylint is very bad at decorators, E1101 is the message that says
    # a member variable does not exist

    # pylint: disable-msg=E1101

    # WARC Version Line - See WARC 1.1 Section 4 "File and record model"
    # https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#file-and-record-model
    VERSION = b"WARC/1.0"  # Also supports WARC/1.1
    VERSION11 = b"WARC/1.1"  # WARC 1.1 format
    VERSION18 = b"WARC/0.18"
    VERSION17 = b"WARC/0.17"

    # WARC Record Types - See WARC 1.1 Section 6 "WARC Record Types"
    # https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#warc-record-types
    # All 8 record types defined in WARC 1.1 Section 6:
    WARCINFO = b"warcinfo"  # Section 6.2: 'warcinfo' record - describes following records
    RESPONSE = b"response"  # Section 6.3: 'response' record - complete scheme-specific response
    RESOURCE = b"resource"  # Section 6.4: 'resource' record - resource without full protocol info
    REQUEST = b"request"  # Section 6.5: 'request' record - complete scheme-specific request
    METADATA = b"metadata"  # Section 6.6: 'metadata' record - describes/explains another record
    REVISIT = b"revisit"  # Section 6.7: 'revisit' record - revisitation with abbreviated content
    CONVERSION = b"conversion"  # Section 6.8: 'conversion' record - alternative version of content
    CONTINUATION = (
        b"continuation"  # Section 6.9: 'continuation' record - segmented record continuation
    )

    # Revisit Profiles - See WARC 1.1 Section 6.7 "revisit" record
    # https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#revisit
    # Profile: Identical Payload Digest (Section 6.7.2)
    # https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#profile-identical-payload-digest
    PROFILE_IDENTICAL_PAYLOAD_DIGEST = (
        b"http://netpreserve.org/warc/1.1/revisit/identical-payload-digest"
    )
    # Profile: Server Not Modified (Section 6.7.3)
    # https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#profile-server-not-modified
    PROFILE_SERVER_NOT_MODIFIED = b"http://netpreserve.org/warc/1.1/revisit/server-not-modified"
    # Also see: WARC Deduplication spec for recording arbitrary duplicates
    # https://iipc.github.io/warc-specifications/specifications/warc-deduplication/recording-arbitrary-duplicates-1.0/

    TRAILER = b"\r\n\r\n"

    def __init__(
        self,
        version=VERSION,
        headers=None,
        content=None,
        errors=None,
        content_file=None,
    ):
        """WarcRecord constructor.

        Creates a WARC record. Either content or content_file must be provided,
        but not both.

        If content (a tuple (content_type, content_buffer)) is provided, when
        writing the WARC record, any Content-Type and Content-Length that appear
        in the supplied headers are ignored, and the values content[0] and
        len(content[1]), respectively, are used.

        When reading, the caller can stream content_file or use content, which is
        lazily filled using content_file, and after which content_file is
        unavailable.

        Args:
            version: WARC version (default: WARC/1.0, also supports WARC/1.1)
            headers: List of (name, value) tuples for WARC named fields
            content: Tuple (content_type, content_buffer) or None
            errors: List of error tuples or None
            content_file: File-like object for streaming content or None

        See:
            WARC 1.1 Section 4: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#file-and-record-model
        """
        ArchiveRecord.__init__(self, headers, content, errors)
        self.version = version
        self.content_file = content_file

    @property
    def id(self):
        """Get WARC-Record-ID header value.

        See WARC 1.1 Section 5.2:
        https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#warc-record-id
        """
        return self.get_header(self.ID)

    def get_concurrent_to(self):
        """Get all WARC-Concurrent-To header values.

        WARC-Concurrent-To may appear multiple times per WARC 1.1 Section 5.7
        (exception to the no-repeat rule). This method returns all instances.

        Returns:
            list: List of WARC-Record-IDs (bytes), empty list if none found

        See:
            WARC 1.1 Section 5.7: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#warc-concurrent-to
        """
        return self.get_all_headers(self.CONCURRENT_TO)

    def get_target_uri(self):
        """Get WARC-Target-URI header value, stripping angle brackets if present.

        Per WARC 1.1 Section 5.13, WARC-Target-URI should be a URI per RFC 3986
        (no angle brackets). However, readers should accept and strip angle brackets
        if present (community recommendation).

        Returns:
            bytes or None: URI value with angle brackets stripped, None if not found

        See:
            WARC 1.1 Section 5.13: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#warc-target-uri
        """
        uri = self.get_header(self.URL)
        if uri:
            # Strip angle brackets if present (community recommendation)
            uri_str = uri.decode("utf-8", errors="replace")
            if uri_str.startswith("<") and uri_str.endswith(">"):
                uri = uri_str[1:-1].encode("utf-8")
        return uri

    def get_profile(self):
        """Get WARC-Profile header value, stripping angle brackets if present.

        Per WARC 1.1 Section 5.15, WARC-Profile should be a URI (no angle brackets).
        However, readers should accept and strip angle brackets if present
        (community recommendation).

        Returns:
            bytes or None: Profile URI with angle brackets stripped, None if not found

        See:
            WARC 1.1 Section 5.15: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#warc-profile
        """
        profile = self.get_header(self.PROFILE)
        if profile:
            # Strip angle brackets if present (community recommendation)
            profile_str = profile.decode("utf-8", errors="replace")
            if profile_str.startswith("<") and profile_str.endswith(">"):
                profile = profile_str[1:-1].encode("utf-8")
        return profile

    def _write_to(self, out, nl):
        """Write WARC record in the format specified by WARC 1.1 Section 4.

        Record format per spec:
        version CRLF *named-field CRLF block CRLF CRLF

        Where:
        - version: WARC version line (e.g., "WARC/1.1")
        - *named-field: Zero or more header fields (field-name ":" field-value)
        - block: Record content block (Content-Length octets)
        - CRLF: Carriage return + line feed (\\r\\n)

        Field names are written as-is (case preserved). Field values may
        contain UTF-8 characters per spec. This implementation does not write
        multi-line headers (line folding is deprecated per community recommendation).

        Args:
            out: File-like object to write to
            nl: Newline sequence (should be b"\\r\\n" for WARC compliance)

        See:
            WARC 1.1 Section 4: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#file-and-record-model
            Community recommendation #74: https://github.com/iipc/warc-specifications/issues/74
        """
        out.write(self.version)
        out.write(nl)
        for k, v in self.headers:
            if self.content_file is not None or k not in (
                self.CONTENT_TYPE,
                self.CONTENT_LENGTH,
            ):
                out.write(k)
                out.write(b": ")
                out.write(v)
                out.write(nl)

        if self.content_file is not None:
            out.write(nl)  # end of header blank nl
            while True:
                buf = self.content_file.read(8192)
                if buf == b"":
                    break
                out.write(buf)
        else:
            # if content tuple is provided, set Content-Type and
            # Content-Length based on the values in the tuple
            content_type, content_buffer = self.content

            if content_type:
                out.write(self.CONTENT_TYPE)
                out.write(b": ")
                out.write(content_type)
                out.write(nl)
            if content_buffer is None:
                content_buffer = b""

            content_length = len(content_buffer)
            out.write(self.CONTENT_LENGTH)
            out.write(b": ")
            out.write(str(content_length).encode("ascii"))
            out.write(nl)

            out.write(nl)  # end of header blank nl
            if content_buffer:
                out.write(content_buffer)

        # end of record nl nl
        out.write(nl)
        out.write(nl)
        out.flush()

    def repair(self):
        pass

    def validate(self):
        """Validate WARC record against WARC 1.1 specification.

        Checks that all mandatory fields are present and properly formatted:
        - WARC-Record-ID (Section 5.2): Must be present, format "<" uri ">"
        - WARC-Date (Section 5.3): Must be present, W3CDTF format
        - WARC-Type (Section 5.4): Must be present, valid record type
        - Content-Length (Section 5.5): Must be present, numeric value

        Also validates record-type-specific requirements:
        - revisit records must have WARC-Profile (Section 6.7)

        See: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#named-fields

        Returns:
            list: List of error tuples, empty list if record is valid
        """
        validation_errors = list(self.errors) if self.errors else []

        # Check mandatory fields per WARC 1.1 Section 5
        # https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#named-fields

        # WARC-Record-ID (Section 5.2) - mandatory
        record_id = self.get_header(self.ID)
        if not record_id:
            validation_errors.append(("missing mandatory field", b"WARC-Record-ID"))
        else:
            # Verify format: "WARC-Record-ID" ":" "<" uri ">"
            record_id_str = record_id.decode("utf-8", errors="replace")
            if not (record_id_str.startswith("<") and record_id_str.endswith(">")):
                validation_errors.append(
                    ("invalid WARC-Record-ID format", record_id, "must be <uri>")
                )
            # Verify no internal whitespace (per spec recommendation)
            if b" " in record_id or b"\t" in record_id:
                validation_errors.append(("WARC-Record-ID contains whitespace", record_id))

        # WARC-Date (Section 5.3) - mandatory
        warc_date = self.get_header(self.DATE)
        if not warc_date:
            validation_errors.append(("missing mandatory field", b"WARC-Date"))
        else:
            # Verify W3CDTF format (basic check - should end with Z for UTC)
            date_str = warc_date.decode("utf-8", errors="replace")
            if not date_str.endswith("Z"):
                # Allow other timezone formats but warn
                if "T" not in date_str:
                    validation_errors.append(
                        ("WARC-Date format may be invalid", warc_date, "should be W3CDTF")
                    )

        # WARC-Type (Section 5.4) - mandatory
        warc_type = self.get_header(self.TYPE)
        if not warc_type:
            validation_errors.append(("missing mandatory field", b"WARC-Type"))
        else:
            # Verify it's a known record type
            valid_types = {
                self.WARCINFO,
                self.RESPONSE,
                self.RESOURCE,
                self.REQUEST,
                self.METADATA,
                self.REVISIT,
                self.CONVERSION,
                self.CONTINUATION,
            }
            if warc_type not in valid_types:
                # Unknown types are allowed per spec (should be skipped gracefully)
                # But we note it as a validation warning
                validation_errors.append(
                    ("unknown WARC-Type", warc_type, "will be skipped per spec")
                )

        # Content-Length (Section 5.5) - mandatory
        content_length = self.get_header(self.CONTENT_LENGTH)
        if not content_length:
            validation_errors.append(("missing mandatory field", b"Content-Length"))
        else:
            # Verify format: "Content-Length" ":" 1*DIGIT
            try:
                length_value = int(content_length)
                if length_value < 0:
                    validation_errors.append(
                        ("Content-Length must be non-negative", content_length)
                    )
            except ValueError:
                validation_errors.append(("Content-Length must be numeric", content_length))

        # Record-type-specific validation
        if warc_type == self.REVISIT:
            # WARC-Profile is mandatory for revisit records (Section 6.7)
            profile = self.get_header(self.PROFILE)
            if not profile:
                validation_errors.append(("WARC-Profile is mandatory for revisit records", None))

        return validation_errors

    @classmethod
    def make_parser(cls):
        return WarcParser()

    def block_digest(self, content_buffer):
        block_hash = hashlib.sha256()
        block_hash.update(content_buffer)

        digest = f"sha256:{block_hash.hexdigest()}"
        return digest

    @staticmethod
    def warc_uuid(text):
        """Generate a deterministic WARC-Record-ID from text.

        Creates a UUID-based record ID in the format required by WARC 1.1 Section 5.2:
        "WARC-Record-ID" ":" "<" uri ">"

        The ID is generated deterministically from the input text using SHA-1,
        ensuring the same text produces the same ID.

        Args:
            text: Bytes or string to generate ID from

        Returns:
            bytes: WARC-Record-ID in format <urn:uuid:...>

        See:
            WARC 1.1 Section 5.2: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#warc-record-id
        """
        if isinstance(text, str):
            text = text.encode("utf-8")
        return f"<urn:uuid:{uuid.UUID(hashlib.sha1(text).hexdigest()[0:32])}>".encode("ascii")

    @staticmethod
    def random_warc_uuid():
        """Generate a random WARC-Record-ID.

        Creates a UUID-based record ID in the format required by WARC 1.1 Section 5.2:
        "WARC-Record-ID" ":" "<" uri ">"

        The ID is globally unique for its period of intended use.

        Returns:
            bytes: WARC-Record-ID in format <urn:uuid:...>

        See:
            WARC 1.1 Section 5.2: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#warc-record-id
        """
        return f"<urn:uuid:{uuid.uuid4()}>".encode("ascii")


def rx(pat):
    """Helper to compile regexps with IGNORECASE option set."""
    return re.compile(pat, flags=re.IGNORECASE)


# Version line regex - matches WARC version declaration
# Format per WARC 1.1 Section 4: "WARC/1.1" CRLF
# https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#file-and-record-model
version_rx = rx(
    rb"^(?P<prefix>.*?)(?P<version>\s*WARC/(?P<number>.*?))"
    b"(?P<nl>\r\n|\r|\n)\\Z"
)
# Header parsing regexes per WARC 1.1 Section 4
# https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#file-and-record-model
# Header format: field-name ":" [ field-value ] CRLF
# Field names are case-insensitive, values may contain UTF-8
# Multi-line headers supported (though deprecated per community recommendation #74)
header_rx = rx(rb"^(?P<name>.*?):\s?(?P<value>.*?)" b"(?P<nl>\r\n|\r|\n)\\Z")
value_rx = rx(rb"^\s+(?P<value>.+?)" b"(?P<nl>\r\n|\r|\n)\\Z")  # Continuation lines
nl_rx = rx(b"^(?P<nl>\r\n|\r|\n\\Z)")  # Blank line (end of headers)
length_rx = rx(b"^" + WarcRecord.CONTENT_LENGTH + b"$")  # pylint: disable-msg=E1101
type_rx = rx(b"^" + WarcRecord.CONTENT_TYPE + b"$")  # pylint: disable-msg=E1101

required_headers = {
    WarcRecord.TYPE.lower(),  # pylint: disable-msg=E1101
    WarcRecord.ID.lower(),  # pylint: disable-msg=E1101
    WarcRecord.CONTENT_LENGTH.lower(),  # pylint: disable-msg=E1101
    WarcRecord.DATE.lower(),  # pylint: disable-msg=E1101
}


class WarcParser(ArchiveParser):
    """Parser for WARC format records.

    Implements WARC 1.1 record parsing per Section 4:
    https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#file-and-record-model
    """

    # Known WARC versions - per WARC 1.1 Section 4
    # https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#file-and-record-model
    KNOWN_VERSIONS = {b"1.0", b"1.1", b"0.17", b"0.18"}

    def parse(self, stream, offset, line=None):
        """Parse a WARC record from the stream.

        Reads a WARC record following the format specified in WARC 1.1 Section 4:
        version CRLF *named-field CRLF block CRLF CRLF

        The parser expects CRLF line endings and validates the record structure.
        Field names are case-insensitive per spec. UTF-8 characters are allowed
        in field values.

        Args:
            stream: File-like object to read from
            offset: Optional byte offset of record start
            line: Optional first line (if already read)

        Returns:
            tuple: (record, errors, offset) where:
                - record: WarcRecord object or None if parsing failed
                - errors: List of error tuples (empty if record is valid)
                - offset: Byte offset of record start

        See:
            WARC 1.1 Section 4: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#file-and-record-model
        """
        # pylint: disable-msg=E1101
        errors = []
        version = None
        # find WARC/.*
        if line is None:
            line = stream.readline()

        while line:
            match = version_rx.match(line)

            if match:
                version = match.group("version")
                if offset is not None:
                    offset += len(match.group("prefix"))
                break
            else:
                if offset is not None:
                    offset += len(line)
                if not nl_rx.match(line):
                    errors.append(("ignored line", line))
                    if len(errors) > bad_lines:
                        errors.append(("too many errors, giving up hope",))
                        return (None, errors, offset)
                line = stream.readline()
        if not line:
            if version:
                errors.append(("warc version but no headers", version))
            return (None, errors, offset)
        if line:
            content_length = 0

            record = WarcRecord(errors=errors, version=version)

            # Verify CRLF line endings per WARC 1.1 Section 4
            # https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#file-and-record-model
            if match.group("nl") != b"\x0d\x0a":
                record.error("incorrect newline in version", match.group("nl"))

            # Verify version is known (WARC 1.0, 1.1, or legacy versions)
            if match.group("number") not in self.KNOWN_VERSIONS:
                record.error(
                    "version field is not known ({})".format(",".join(self.KNOWN_VERSIONS)),
                    match.group("number"),
                )

            prefix = match.group("prefix")

            if prefix:
                record.error("bad prefix on WARC version header", prefix)

            # Read headers
            line = stream.readline()
            while line and not nl_rx.match(line):
                # print 'header', repr(line)
                match = header_rx.match(line)
                if match:
                    # Verify CRLF line endings in headers per WARC 1.1 Section 4
                    if match.group("nl") != b"\x0d\x0a":
                        record.error("incorrect newline in header", match.group("nl"))
                    name = match.group("name").strip()
                    value = [match.group("value").strip()]
                    # print 'match',name, value

                    line = stream.readline()
                    match = value_rx.match(line)
                    while match:
                        # print 'follow', repr(line)
                        if match.group("nl") != b"\x0d\x0a":
                            record.error(
                                "incorrect newline in follow header",
                                line,
                                match.group("nl"),
                            )
                        value.append(match.group("value").strip())
                        line = stream.readline()
                        match = value_rx.match(line)

                    value = b" ".join(value)

                    record.headers.append((name, value))

                    if type_rx.match(name):
                        if value:
                            pass
                        else:
                            record.error("invalid header", name, value)
                    elif length_rx.match(name):
                        try:
                            # print name, value
                            content_length = int(value)
                            # print content_length
                        except ValueError:
                            record.error("invalid header", name, value)

            # have read blank line following headers

            record.content_file = stream
            record.content_file.bytes_to_eoc = content_length

            # Mandatory fields are checked in validate() method, not during parsing.
            # This allows parsing to succeed even with missing fields, with errors
            # reported via validate(). Per spec, processing software should ignore
            # unrecognized fields but must handle mandatory field validation.
            #
            # Mandatory fields per WARC 1.1 Section 5:
            # - WARC-Record-ID (Section 5.2)
            # - WARC-Date (Section 5.3)
            # - WARC-Type (Section 5.4)
            # - Content-Length (Section 5.5)
            # https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#named-fields

            return (record, (), offset)


blank_rx = rx(rb"^$")
register_record_type(version_rx, WarcRecord)
register_record_type(blank_rx, WarcRecord)


def make_response(id, date, url, content, request_id):
    """Create a 'response' record.

    A 'response' record contains a complete scheme-specific response. For HTTP/HTTPS,
    the block contains the full HTTP response (headers + body) with
    Content-Type: application/http;msgtype=response. The payload is the HTTP
    entity-body per RFC 2616.

    WARC-IP-Address should be used when available. WARC-Truncated may indicate
    truncated responses. WARC-Concurrent-To links to associated request or metadata.

    Args:
        id: WARC-Record-ID (bytes)
        date: WARC-Date (bytes, W3CDTF format)
        url: WARC-Target-URI (bytes)
        content: Tuple (content_type, content_buffer) - for HTTP should be
                 (b"application/http;msgtype=response", http_response_bytes)
        request_id: Optional WARC-Record-ID of associated request (bytes or None)

    Returns:
        WarcRecord: A 'response' record

    See:
        WARC 1.1 Section 6.3: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#response
    """
    # pylint: disable-msg=E1101
    headers = [
        (WarcRecord.TYPE, WarcRecord.RESPONSE),
        (WarcRecord.ID, id),
        (WarcRecord.DATE, date),
        (WarcRecord.URL, url),
    ]
    if request_id:
        # WARC-Concurrent-To links this response to its request
        # See Section 5.7: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#warc-concurrent-to
        headers.append((WarcRecord.CONCURRENT_TO, request_id))

    record = WarcRecord(headers=headers, content=content)

    return record


def make_request(request_id, date, url, content, response_id):
    """Create a 'request' record.

    A 'request' record contains a complete scheme-specific request. For HTTP/HTTPS,
    the block contains the full HTTP request (headers + body) with
    Content-Type: application/http;msgtype=request. The payload is the HTTP
    entity-body per RFC 2616.

    WARC-IP-Address should be used when available. WARC-Concurrent-To links to
    associated response or metadata.

    Args:
        request_id: WARC-Record-ID (bytes)
        date: WARC-Date (bytes, W3CDTF format)
        url: WARC-Target-URI (bytes)
        content: Tuple (content_type, content_buffer) - for HTTP should be
                 (b"application/http;msgtype=request", http_request_bytes)
        response_id: Optional WARC-Record-ID of associated response (bytes or None)

    Returns:
        WarcRecord: A 'request' record

    See:
        WARC 1.1 Section 6.5: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#request
    """
    # pylint: disable-msg=E1101
    headers = [
        (WarcRecord.TYPE, WarcRecord.REQUEST),
        (WarcRecord.ID, request_id),
        (WarcRecord.DATE, date),
        (WarcRecord.URL, url),
    ]
    if response_id:
        # WARC-Concurrent-To links this request to its response
        # May appear multiple times (exception to no-repeat rule per Section 5.7)
        # See Section 5.7: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#warc-concurrent-to
        headers.append((WarcRecord.CONCURRENT_TO, response_id))

    record = WarcRecord(headers=headers, content=content)

    return record


def make_metadata(meta_id, date, content, concurrent_to=None, url=None):
    """Create a 'metadata' record.

    A 'metadata' record describes, explains, or accompanies a resource. It almost
    always refers to another record via WARC-Refers-To. Recommended Content-Type
    is application/warc-fields.

    Optional fields include: via, hopsFromSeed, fetchTimeMs.

    Args:
        meta_id: WARC-Record-ID (bytes)
        date: WARC-Date (bytes, W3CDTF format)
        content: Tuple (content_type, content_buffer) - recommended
                 (b"application/warc-fields", metadata_fields)
        concurrent_to: Optional WARC-Record-ID of concurrent record (bytes or None)
        url: Optional WARC-Target-URI (bytes or None)

    Returns:
        WarcRecord: A 'metadata' record

    See:
        WARC 1.1 Section 6.6: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#metadata
    """
    # pylint: disable-msg=E1101
    headers = [
        (WarcRecord.TYPE, WarcRecord.METADATA),
        (WarcRecord.ID, meta_id),
        (WarcRecord.DATE, date),
    ]
    if concurrent_to:
        headers.append((WarcRecord.CONCURRENT_TO, concurrent_to))

    if url:
        headers.append((WarcRecord.URL, url))

    record = WarcRecord(headers=headers, content=content)

    return record


def make_conversion(conv_id, date, content, refers_to=None, url=None):
    """Create a 'conversion' record.

    A 'conversion' record contains an alternative version of another record's content,
    such as a format conversion or content transformation. WARC-Refers-To should
    link to the original record.

    The payload is the record block (converted content).

    Args:
        conv_id: WARC-Record-ID (bytes)
        date: WARC-Date (bytes, W3CDTF format)
        content: Tuple (content_type, content_buffer) - converted content
        refers_to: Optional WARC-Record-ID of original record (bytes or None)
        url: Optional WARC-Target-URI (bytes or None)

    Returns:
        WarcRecord: A 'conversion' record

    See:
        WARC 1.1 Section 6.8: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#conversion
    """
    # pylint: disable-msg=E1101
    headers = [
        (WarcRecord.TYPE, WarcRecord.CONVERSION),
        (WarcRecord.ID, conv_id),
        (WarcRecord.DATE, date),
    ]
    if refers_to:
        # WARC-Refers-To links this conversion to the original record
        # See Section 5.8: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#warc-refers-to
        headers.append((WarcRecord.REFERS_TO, refers_to))

    if url:
        headers.append((WarcRecord.URL, url))

    record = WarcRecord(headers=headers, content=content)

    return record


def warc_datetime_str(d):
    """Format datetime as WARC-Date string.

    WARC-Date format follows W3CDTF (W3C profile of ISO8601).
    See WARC 1.1 Section 5.3: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/#warc-date
    Reference: https://www.w3.org/TR/NOTE-datetime
    """
    s = d.isoformat()
    if "." in s:
        s = s[: s.find(".")]
    return (s + "Z").encode("utf-8")
