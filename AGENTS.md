# AGENTS.md - Project Guide for AI Agents

This document provides essential information for AI agents working on the `warctools` project.

## Project Overview

**warctools** is a Python library and command-line tool suite for handling and manipulating WARC (Web ARChive) files. It supports the [WARC 1.0 specification](https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.0/) and is compatible with the Internet Archive's ARC File Format.

### What This Tool Does

- **Reads and writes WARC files** - Create, parse, and manipulate web archive files
- **Command-line tools** - 9 CLI utilities for common WARC operations:
  - `warcdump` - Human-readable dump of WARC files
  - `warcvalid` - Validate WARC file integrity
  - `warcfilter` - Filter records by pattern (URL, type, content, etc.)
  - `warcextract` - Extract record content to stdout
  - `warcindex` - Create index of records with offsets
  - `warclinks` - Extract links from WARC records
  - `warcunpack` - Unpack WARC records to directory structure
  - `warcpayload` - Extract HTTP payloads from records
  - `warc2warc` - Convert/copy WARC files
  - `arc2warc` - Convert ARC files to WARC format
- **Python library** - Programmatic access to WARC records and operations

## Project Layout

```
warctools/
├── src/
│   ├── hanzo/                    # Main package (legacy name, kept for compatibility)
│   │   ├── __init__.py
│   │   ├── warctools/            # Core WARC library
│   │   │   ├── __init__.py       # Main exports
│   │   │   ├── warc.py           # WarcRecord class and WARC-specific logic
│   │   │   ├── record.py         # Base ArchiveRecord and ArchiveParser
│   │   │   ├── arc.py            # ARC format support
│   │   │   ├── stream.py         # RecordStream for reading/writing
│   │   │   ├── archive_detect.py # Format detection
│   │   │   ├── s3.py             # S3 support
│   │   │   └── tests/            # Legacy unit tests
│   │   ├── httptools/            # HTTP parsing library
│   │   │   ├── messaging.py      # HTTP message parsing
│   │   │   └── semantics.py      # HTTP semantics (methods, codes, etc.)
│   │   ├── warcdump.py           # CLI: warcdump
│   │   ├── warcvalid.py          # CLI: warcvalid
│   │   ├── warcfilter.py         # CLI: warcfilter
│   │   ├── warcextract.py        # CLI: warcextract
│   │   ├── warcindex.py          # CLI: warcindex
│   │   ├── warclinks.py          # CLI: warclinks
│   │   ├── warcunpack.py         # CLI: warcunpack
│   │   ├── warcpayload.py        # CLI: warcpayload
│   │   ├── warc2warc.py          # CLI: warc2warc
│   │   └── arc2warc.py           # CLI: arc2warc
│   └── warctools/                # Compatibility re-export package
│       └── __init__.py            # Re-exports from hanzo for backward compatibility
├── tests/
│   ├── test_cli.py               # Basic CLI help tests
│   └── test_integration.py       # Comprehensive integration tests
├── pyproject.toml                # Project configuration (build, deps, linting)
├── README.md                     # User-facing documentation
├── LICENSE                       # MIT License
└── .github/workflows/ci.yml      # GitHub Actions CI/CD

```

## Tool Preferences

**CRITICAL: This project uses `uv` for all Python tooling.**

### Required Tools

- **`uv`** - Fast Python package installer and resolver
  - Virtual environment management: `uv venv`
  - Package installation: `uv sync --dev`
  - Running commands: `uv run <command>`
  - Building: `uv build`
  - Formatting: `uv format`
- **`ruff`** - Linting and formatting (via `uv`)
- **`pytest`** - Testing framework (via `uv`)
- **`mypy`** - Type checking (optional, via `uv`)

### Virtual Environment

The project uses `uv` for virtual environment management. The virtual environment is typically located at `.venv` in the project root or parent directory.

**DO NOT** use:
- ❌ `python -m venv`
- ❌ `pip` directly (use `uv pip` if needed)
- ❌ `poetry`
- ❌ `pipenv`

**DO** use:
- ✅ `uv venv` to create virtual environment
- ✅ `uv sync --dev` to install dependencies
- ✅ `uv run <command>` to run commands in the environment
- ✅ `uv build` to build the package

## Build and Test

### Initial Setup

```bash
# Create virtual environment (if not exists)
uv venv

# Activate (if needed, though uv run handles this)
source .venv/bin/activate  # or .venv/bin/activate on Unix
# On Windows: .venv\Scripts\activate

# Install dependencies (including dev dependencies)
uv sync --dev
```

### Building

```bash
# Build the package
uv build

# Output will be in dist/
# - dist/warctools-6.0.0-py3-none-any.whl
# - dist/warctools-6.0.0.tar.gz
```

### Testing

```bash
# Run all tests
uv run pytest

# Run with verbose output
uv run pytest -v

# Run specific test file
uv run pytest tests/test_integration.py

# Run specific test
uv run pytest tests/test_integration.py::test_create_and_read_warc

# With coverage (if configured)
uv run pytest --cov=src
```

### Linting and Formatting

```bash
# Check linting
uv run ruff check .

# Auto-fix linting issues
uv run ruff check --fix .

# Check formatting
uv run ruff format --check .

# Auto-format code
uv run ruff format .

# Type checking (optional)
uv run mypy .
```

### Running CLI Tools

After installation (`uv sync --dev`), CLI tools are available:

```bash
# Via uv run
uv run warcdump --help
uv run warcvalid test.warc

# Or if installed in environment
warcdump --help
warcvalid test.warc
```

## Code Style and Conventions

### Python Version

- **Minimum**: Python 3.10
- **Target versions**: 3.10, 3.11, 3.12, 3.13
- Use Python 3.10+ features (no `__future__` imports needed)

### Code Formatting

- **Line length**: 100 characters
- **Formatter**: `ruff format` (Black-compatible)
- **Linter**: `ruff` with strict rules

### Type Hints

- Use type hints for all new code
- Prefer `Optional[X]` over `X | None` for Python 3.10 compatibility
- Type checking with `mypy` (configured but not strict)

### Import Style

- Use absolute imports: `from hanzo.warctools import WarcRecord`
- Legacy code may use relative imports in `hanzo/warctools/`
- Organize imports with `ruff` (isort-compatible)

### Naming Conventions

- Follow PEP 8
- Exception: `runTest` in unittest (required by framework)
- Use descriptive names, avoid single letters except in comprehensions

### CLI Tools

- **Framework**: `click` (migrated from `optparse`)
- **Entry points**: Each CLI tool has a `run()` function in its module
- **Compatibility**: Maintain 100% argument compatibility with original `optparse` version
- **Help**: All tools support `-h` and `--help`

### Testing

- **Framework**: `pytest`
- **Test location**: `tests/` directory
- **Test types**:
  - `test_cli.py` - Basic CLI help/usage tests
  - `test_integration.py` - Comprehensive integration tests
  - Legacy tests in `src/hanzo/warctools/tests/` (unittest-based)

### Linting Rules

Key ignores in `pyproject.toml`:
- `E501` - Line too long (handled by formatter)
- `UP007` - Optional[X] vs X | None (Python 3.10 compatibility)
- `E402` - Module level import not at top (needed for re-export pattern)
- `N802` - Function name lowercase (unittest.TestCase.runTest)
- `B017` - Blind exception assertion (intentional in tests)

## Key Concepts

### WARC Records

- **WarcRecord**: Main class for WARC records
- **Record types**: WARCINFO, REQUEST, RESPONSE, REVISIT, METADATA, CONVERSION
- **Content**: Can be provided as tuple `(content_type, content_bytes)` or `content_file` handle
- **Headers**: List of `(name, value)` tuples, both bytes

### Record Streams

- **RecordStream**: Base class for reading/writing records
- **GzipRecordStream**: For per-record gzipped files
- **open_archive()**: Factory function to open WARC/ARC files

### Helper Functions

- `warctools.warc.make_response()` - Create response record
- `warctools.warc.make_request()` - Create request record
- `warctools.warc.make_metadata()` - Create metadata record
- `warctools.warc.warc_datetime_str()` - Format datetime for WARC
- `WarcRecord.random_warc_uuid()` - Generate WARC record ID

### Package Structure

- **Import path**: `from hanzo import warctools` (legacy, but standard)
- **Re-export**: `src/warctools/__init__.py` re-exports from `hanzo` for compatibility
- **Build**: `uv_build` expects packages in `src/` directory

## Common Tasks

### Adding a New CLI Tool

1. Create `src/hanzo/newtool.py`:
   ```python
   import click
   from .warctools import WarcRecord
   
   @click.command()
   def main():
       """Tool description."""
       # Implementation
   
   def run():
       main()
   ```

2. Add entry point to `pyproject.toml`:
   ```toml
   [project.scripts]
   newtool = "hanzo.newtool:run"
   ```

3. Add tests to `tests/test_integration.py`

### Modifying Core Library

- Core logic is in `src/hanzo/warctools/`
- Changes should maintain backward compatibility
- Update tests accordingly
- Run full test suite: `uv run pytest`

### Adding Dependencies

1. Add to `pyproject.toml`:
   ```toml
   dependencies = [
       "newpackage>=1.0.0",
   ]
   ```

2. Update lock file:
   ```bash
   uv sync --dev
   ```

### Running CI Locally

The CI runs:
1. `uv run ruff check .`
2. `uv run ruff format --check .`
3. `uv run pytest`

Run these commands locally before committing.

## Important Notes

### Legacy Code

- Much of the codebase was modernized from Python 2/3 compatible code
- Some legacy patterns remain in `src/hanzo/warctools/tests/` (unittest)
- CLI tools were migrated from `optparse` to `click` but maintain 100% argument compatibility

### Package Naming

- The package is named `hanzo` internally (legacy from Hanzo Archives)
- Public API uses `from hanzo import warctools`
- Build system creates `warctools` package via re-export in `src/warctools/`

### Build Backend

- Uses `uv_build` (not setuptools, not hatchling)
- Configured in `pyproject.toml`:
  ```toml
  [tool.uv_build]
  packages = ["hanzo", "warctools"]
  ```

### Testing Philosophy

- Integration tests are preferred over unit tests
- Tests should use real WARC files when possible
- CLI tools should be tested via subprocess (as users would use them)

## Troubleshooting

### Import Errors

If you see `ModuleNotFoundError: No module named 'hanzo'`:
- Ensure you're in the project root
- Run `PYTHONPATH=src:$PYTHONPATH uv run pytest` or
- Install in editable mode: `uv pip install -e .`

### Linting Errors

- Run `uv run ruff check --fix .` to auto-fix most issues
- Check `pyproject.toml` for ignored rules if error is intentional

### Test Failures

- Ensure virtual environment is activated or use `uv run`
- Check that test files create temporary WARC files correctly
- Verify CLI tools are installed: `uv sync --dev`

## Version Information

- **Current version**: 6.0.0
- **Version history**: Modernized from 5.0.1 to 6.0.0 with:
  - Python 3.10+ requirement
  - Click migration
  - Type hints
  - Modern build system
  - Comprehensive tests

## Resources

- [WARC 1.0 Specification](https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.0/)
- [ARC File Format](https://archive.org/web/researcher/ArcFileFormat.php)
- [uv Documentation](https://github.com/astral-sh/uv)
- [Click Documentation](https://click.palletsprojects.com/)
- [pytest Documentation](https://docs.pytest.org/)

---

**Last Updated**: 2024 (after modernization to version 6.0.0)

