#!/usr/bin/env python3
"""fake-fill.py: a stand-in for the fill command of a workload: the contract checks that it is a file; no test runs it"""

from __future__ import annotations

import sys


def main(argv=None):
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
