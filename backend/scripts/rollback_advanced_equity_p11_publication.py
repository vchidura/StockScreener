#!/usr/bin/env python3
"""Rollback one failed-read P11 cohort-local pointer advance."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from uuid import UUID


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_equity_publication_authority import (  # noqa: E402
    advanced_equity_publication_authority_from_payload,
)
from realtime.contracts import canonical_sha256  # noqa: E402
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    validate_target_database_v2,
)
from realtime.portal_projection_repository import (  # noqa: E402
    load_portal_release_pointer_cursor,
    restore_portal_release_pointer_cursor,
)
from realtime.preflight import ProviderTier  # noqa: E402
from scripts.prepare_realtime_v2_advanced_ingress import (  # noqa: E402
    _admin_cursor,
    _settings,
)
from scripts.realtime_script_support import write_report  # noqa: E402


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--authority", type=Path, required=True)
    parser.add_argument("--authority-semantic-sha256", required=True)
    parser.add_argument("--authority-file-sha256", required=True)
    parser.add_argument("--expected-current-release-id", type=UUID, required=True)
    parser.add_argument("--report-path", type=Path, required=True)
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("P11 rollback targets only stocks_realtime_v2")
    body = args.authority.resolve().read_bytes()
    document = json.loads(body)
    unsigned = dict(document)
    semantic = unsigned.pop("semantic_sha256", None)
    if (
        hashlib.sha256(body).hexdigest()
        != args.authority_file_sha256
        or semantic != args.authority_semantic_sha256
        or canonical_sha256(unsigned) != semantic
        or args.report_path.resolve().exists()
    ):
        raise ValueError("P11 rollback authority identity changed")
    authority = advanced_equity_publication_authority_from_payload(
        document["authority"]
    )
    if authority.expected_prior_release_id is not None:
        raise ValueError("P11 rollback only restores the bound null pointer")
    settings = _settings(BACKEND_DIR / ".env")
    with _admin_cursor(settings, database=args.database) as cursor:
        restore_portal_release_pointer_cursor(
                cursor,
                cohort_id=authority.cohort_id,
                provider_tier=ProviderTier.MASSIVE_ADVANCED,
                expected_current_release_id=(
                    args.expected_current_release_id
                ),
                previous=None,
            )
    with _admin_cursor(settings, database=args.database) as cursor:
        current = load_portal_release_pointer_cursor(
            cursor,
            cohort_id=authority.cohort_id,
            provider_tier=ProviderTier.MASSIVE_ADVANCED,
        )
    if current is not None:
        raise RuntimeError("P11 rollback did not restore the null pointer")
    report = {
        "contract": "advanced-equity-p11-publication-rollback-v1",
        "status": "COMPLETE",
        "authority_id": str(authority.authority_id),
        "cohort_id": str(authority.cohort_id),
        "removed_release_id": str(args.expected_current_release_id),
        "restored_pointer": None,
        "provider_requests": 0,
    }
    report["semantic_sha256"] = canonical_sha256(report)
    write_report(args.report_path.resolve(), report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
