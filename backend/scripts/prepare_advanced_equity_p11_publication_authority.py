#!/usr/bin/env python3
"""Prepare one exact Advanced Equity cohort-local publication authority."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5

from dotenv import dotenv_values
from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_equity_publication_authority import (  # noqa: E402
    advanced_equity_publication_authority_payload,
    build_advanced_equity_publication_authority,
)
from realtime.advanced_scheduler import (  # noqa: E402
    discover_advanced_window_recovery_cursor,
)
from realtime.contracts import canonical_sha256  # noqa: E402
from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    validate_target_database_v2,
)
from realtime.portal_projection_repository import (  # noqa: E402
    load_portal_release_pointer_cursor,
)
from realtime.preflight import ProviderTier  # noqa: E402
from scripts.realtime_script_support import write_report  # noqa: E402


UTC = timezone.utc


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--cohort-id", type=UUID, required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--cutoff", type=datetime.fromisoformat, required=True)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--expires-at", type=datetime.fromisoformat, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    if (
        args.database != EXPECTED_V2_DATABASE_NAME
        or args.cutoff.tzinfo is None
        or args.cutoff.utcoffset() is None
        or args.expires_at.tzinfo is None
        or args.expires_at.utcoffset() is None
        or not args.authorization_reference.strip()
    ):
        parser.error("P11 publication authority scope is invalid")
    args.cutoff = args.cutoff.astimezone(UTC)
    args.expires_at = args.expires_at.astimezone(UTC)
    args.output = args.output.resolve()
    if (
        args.cutoff.date() != args.session_date
        or args.expires_at <= datetime.now(UTC)
        or args.output.exists()
    ):
        parser.error("P11 publication authority clocks or output changed")
    return args


def _provider():
    values = dotenv_values(BACKEND_DIR / ".env")
    dsn = str(
        os.getenv("REALTIME_V2_APP_DSN")
        or values.get("REALTIME_V2_APP_DSN")
        or ""
    ).strip()
    if not dsn:
        raise RuntimeError("REALTIME_V2_APP_DSN is required")
    pool = ThreadedConnectionPool(
        1,
        2,
        dsn=dsn,
        options="-c timezone=UTC",
    )
    provider = V2ConnectionProvider(
        {APP_ROLE: pool},
        V2AccessConfig(
            security_mode="hardened",
            role_prefix=EXPECTED_V2_DATABASE_NAME,
            statement_timeout_ms=300_000,
        ),
    )
    provider.verify()
    return provider, pool


def execute(args):
    checked_at = datetime.now(UTC)
    provider, pool = _provider()
    try:
        with provider.transaction(
            APP_ROLE,
            read_only=True,
            isolation_level="REPEATABLE READ",
        ) as unit:
            recovery = discover_advanced_window_recovery_cursor(
                unit.cursor,
                cohort_id=args.cohort_id,
                cutoff=args.cutoff,
                checked_at=checked_at,
            )
            prior = load_portal_release_pointer_cursor(
                unit.cursor,
                cohort_id=args.cohort_id,
                provider_tier=ProviderTier.MASSIVE_ADVANCED,
            )
    finally:
        pool.closeall()
    if (
        not recovery.complete
        or recovery.input_receipt is None
        or recovery.portal_receipt is None
        or recovery.next_attempt <= 0
    ):
        raise ValueError(
            "P11 requires one completed projection with durable receipts"
        )
    suffix = f"-retry{recovery.next_attempt}"
    window_key = f"advanced-{args.cutoff:%Y%m%dT%H%M%SZ}{suffix}"
    target_window_id = uuid5(
        NAMESPACE_URL,
        f"stockscreener:window:{args.cohort_id}:{window_key}",
    )
    authority = build_advanced_equity_publication_authority(
        authorization_reference=args.authorization_reference,
        cohort_id=args.cohort_id,
        session_date=args.session_date,
        cutoff=args.cutoff,
        target_window_id=target_window_id,
        window_key_suffix=suffix,
        input_receipt=recovery.input_receipt,
        portal_receipt=recovery.portal_receipt,
        expected_prior_release_id=(
            None if prior is None else prior.release_id
        ),
        expected_prior_cutoff=(
            None if prior is None else prior.cutoff
        ),
        prepared_at=checked_at,
        expires_at=args.expires_at,
    )
    document = {
        "contract": "advanced-equity-p11-publication-authority-document-v1",
        "authority": advanced_equity_publication_authority_payload(
            authority
        ),
        "provider_requests": 0,
        "database_writes": 0,
    }
    document["semantic_sha256"] = canonical_sha256(document)
    write_report(args.output, document)
    return {
        "status": "COMPLETE",
        "authority_id": str(authority.authority_id),
        "target_window_id": str(authority.target_window_id),
        "window_key_suffix": authority.window_key_suffix,
        "semantic_sha256": document["semantic_sha256"],
        "file_sha256": hashlib.sha256(
            args.output.read_bytes()
        ).hexdigest(),
        "output": str(args.output),
        "provider_requests": 0,
        "database_writes": 0,
    }


def main(argv=None) -> int:
    result = execute(parse_args(argv))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
