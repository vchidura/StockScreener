#!/usr/bin/env python3
"""Validate redacted V2 runtime configuration and hardened DSN bindings."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from dotenv import dotenv_values
from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.database_access_v2 import (  # noqa: E402
    API_ROLE,
    APP_ROLE,
    V2AccessConfig,
    verify_pool_target_v2,
)
from realtime.equity_pipeline_config import (  # noqa: E402
    EquityPipeline,
    RealtimeV2PipelineConfig,
)
from realtime.migrations import EXPECTED_V2_DATABASE_NAME  # noqa: E402


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env-file",
        type=Path,
        default=BACKEND_DIR / ".env",
    )
    parser.add_argument("--check-connections", action="store_true")
    parser.add_argument("--create-journal-directory", action="store_true")
    return parser.parse_args(argv)


def _verify_dsn(dsn: str, role: str) -> str:
    pool = ThreadedConnectionPool(
        1,
        1,
        dsn=dsn,
        options="-c timezone=UTC",
    )
    try:
        return verify_pool_target_v2(
            pool,
            V2AccessConfig(
                security_mode="hardened",
                role_prefix=EXPECTED_V2_DATABASE_NAME,
            ),
            role,
        )
    finally:
        pool.closeall()


def _configured_secret(settings, name):
    value = str(settings.get(name) or "").strip()
    if not value or value.lower() in {
        "...",
        "replace_with_generated_value",
        "replace_with_provider_value",
        "your_polygon_api_key_here",
    }:
        raise ValueError(f"{name} is missing or still contains a placeholder")
    return value


def main(argv=None) -> int:
    args = parse_args(argv)
    if not args.env_file.is_file():
        raise FileNotFoundError(args.env_file)
    settings = dotenv_values(args.env_file)
    assignment_names = (
        "REALTIME_V2_APP_DSN",
        "REALTIME_V2_API_DSN",
        "POLYGON_API_KEY",
        "REALTIME_V2_ADVANCED_JOURNAL_DIR",
    )
    assignment_counts = {name: 0 for name in assignment_names}
    for line in args.env_file.read_text(encoding="utf-8").splitlines():
        candidate = line.strip()
        for name in assignment_names:
            if candidate.startswith(name + "="):
                assignment_counts[name] += 1
    verified = {}
    if args.check_connections:
        verified = {
            "app": _verify_dsn(
                _configured_secret(settings, "REALTIME_V2_APP_DSN"),
                APP_ROLE,
            ),
            "api": _verify_dsn(
                _configured_secret(settings, "REALTIME_V2_API_DSN"),
                API_ROLE,
            ),
        }
        if set(verified.values()) != {EXPECTED_V2_DATABASE_NAME}:
            raise RuntimeError("V2 DSN target verification failed")
    try:
        config = RealtimeV2PipelineConfig.from_mapping(settings)
    except ValueError as error:
        print(
            json.dumps(
                {
                    "status": "INVALID",
                    "configuration_error": str(error),
                    "connections_checked": args.check_connections,
                    "verified_targets": verified,
                    "assignment_counts": assignment_counts,
                    "secrets_exposed": False,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 2
    journal = config.advanced_journal_dir
    if args.create_journal_directory:
        if config.equity_pipeline is not EquityPipeline.MASSIVE_ADVANCED:
            raise ValueError(
                "journal creation requires MASSIVE_ADVANCED equity"
            )
        journal.mkdir(parents=True, exist_ok=True)
    if journal is not None and journal.exists() and not journal.is_dir():
        raise ValueError("Advanced journal path exists but is not a directory")
    print(
        json.dumps(
            {
                "status": "VALID",
                "configuration": config.redacted_identity,
                "journal_exists": bool(journal and journal.is_dir()),
                "connections_checked": args.check_connections,
                "verified_targets": verified,
                "assignment_counts": assignment_counts,
                "secrets_exposed": False,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
