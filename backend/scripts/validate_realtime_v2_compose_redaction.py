#!/usr/bin/env python3
"""Fail when rendered Compose output contains any configured secret value."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--secret-directory", type=Path, required=True)
    args = parser.parse_args(argv)
    rendered = sys.stdin.read()
    if not rendered.strip():
        raise RuntimeError("rendered Compose configuration is empty")
    secrets = tuple(
        path.read_text(encoding="utf-8").strip()
        for path in args.secret_directory.resolve().iterdir()
        if path.is_file()
    )
    if not secrets or any(not value for value in secrets):
        raise RuntimeError("Docker secret verification input is incomplete")
    if any(value in rendered for value in secrets):
        raise RuntimeError("rendered Compose configuration exposed a secret")
    print(
        f"COMPOSE_REDACTION_PASS services_configured=true "
        f"secret_values_checked={len(secrets)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
