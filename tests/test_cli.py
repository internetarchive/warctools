"""Basic tests for CLI tools."""

import subprocess
from pathlib import Path


def test_warcdump_help():
    """Test that warcdump --help works."""
    # Test via installed command
    result = subprocess.run(
        ["warcdump", "--help"],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent,
    )
    assert result.returncode == 0
    assert "Dump WARC files" in result.stdout or "Usage:" in result.stdout


def test_warcvalid_help():
    """Test that warcvalid --help works."""
    result = subprocess.run(
        ["warcvalid", "--help"],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent,
    )
    assert result.returncode == 0
    assert "Validate WARC files" in result.stdout or "Usage:" in result.stdout


def test_warcfilter_help():
    """Test that warcfilter --help works."""
    result = subprocess.run(
        ["warcfilter", "--help"],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent,
    )
    assert result.returncode == 0
    assert "Filter WARC files" in result.stdout or "Usage:" in result.stdout


def test_arc2warc_help():
    """Test that arc2warc --help works."""
    result = subprocess.run(
        ["arc2warc", "--help"],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent,
    )
    assert result.returncode == 0
    assert "Convert ARC files" in result.stdout or "Usage:" in result.stdout
