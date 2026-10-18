#!/usr/bin/env python3
"""Exit successfully when an HTTP endpoint returns a 2xx response."""
from __future__ import annotations

import argparse
from urllib.request import urlopen


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url")
    parser.add_argument("--timeout-seconds", type=float, default=5.0)
    args = parser.parse_args(argv)
    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be positive")
    with urlopen(args.url, timeout=args.timeout_seconds) as response:
        return 0 if 200 <= response.status < 300 else 1


if __name__ == "__main__":
    raise SystemExit(main())
