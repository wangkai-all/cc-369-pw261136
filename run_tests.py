#!/usr/bin/env python3
"""Optional convenience runner; requires the `pytest` package."""

import sys

try:
    import pytest
except ImportError:
    sys.exit("pytest is not installed: python3 -m pip install pytest")


if __name__ == "__main__":
    sys.exit(pytest.main(["tests", "-q", *sys.argv[1:]]))
