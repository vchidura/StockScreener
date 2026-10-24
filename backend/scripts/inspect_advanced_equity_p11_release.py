#!/usr/bin/env python3
"""Inspect exact ticker workspace identity predicates for one P11 release."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from uuid import UUID


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.database_access_v2 import APP_ROLE  # noqa: E402
from realtime.contracts import canonical_sha256  # noqa: E402
from realtime.reference_continuity import (  # noqa: E402
    source_manifest_identity_cursor,
)
from realtime.ticker_projection import TICKER_WORKSPACE_VERSION  # noqa: E402
from realtime.ticker_repository import publication_source_cutoff  # noqa: E402
from scripts.prepare_advanced_equity_p11_publication_authority import (  # noqa: E402
    _provider,
)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-id", type=UUID, required=True)
    parser.add_argument("--ticker", default="SPY")
    args = parser.parse_args(argv)
    provider, pool = _provider()
    try:
        with provider.transaction(
            APP_ROLE,
            read_only=True,
            dict_cursor=True,
            isolation_level="REPEATABLE READ",
        ) as unit:
            cursor = unit.cursor
            cursor.execute(
                "SELECT release.cohort_id::text,release.cutoff,"
                "entry.value->>'snapshot_id' AS snapshot_id "
                "FROM rt.portal_releases AS release "
                "CROSS JOIN LATERAL jsonb_array_elements("
                "release.manifest->'snapshots') AS entry(value) "
                "WHERE release.release_id=%s "
                "AND entry.value->>'snapshot_type'=%s "
                "AND entry.value->>'snapshot_key'=%s",
                (
                    str(args.release_id),
                    TICKER_WORKSPACE_VERSION,
                    args.ticker.strip().upper(),
                ),
            )
            release = cursor.fetchone()
            if release is None:
                raise ValueError("P11 release ticker entry is unavailable")
            cursor.execute(
                "SELECT catalog.snapshot_id::text,"
                "catalog.provider_tier,catalog.source_generation,"
                "catalog.generated_at,catalog.session_date,"
                "hot.source_manifest,hot.payload,"
                "manifest.manifest_id::text AS cohort_manifest_id,"
                "manifest.manifest_sha256 AS cohort_manifest_sha256,"
                "binding.binding_sha256 AS history_binding_sha256 "
                "FROM rt.snapshot_catalog AS catalog "
                "JOIN rt.portal_snapshot_hot AS hot "
                "ON hot.snapshot_id=catalog.snapshot_id "
                "AND hot.generated_at=catalog.generated_at "
                "JOIN rt.cohorts AS cohort "
                "ON cohort.cohort_id=catalog.cohort_id "
                "JOIN rt.subscription_manifests AS manifest "
                "ON manifest.manifest_id=cohort.manifest_id "
                "LEFT JOIN rt.cohort_history_bindings AS binding "
                "ON binding.target_cohort_id=cohort.cohort_id "
                "WHERE catalog.snapshot_id=%s",
                (release["snapshot_id"],),
            )
            row = cursor.fetchone()
            if row is None:
                raise ValueError("P11 release snapshot body is unavailable")
            manifest = row["source_manifest"]
            payload = row["payload"]
            source_cutoff = publication_source_cutoff(
                row["generated_at"],
                manifest.get("reconstruction"),
                session_date=row["session_date"],
                market_time=payload["quote"]["as_of"],
            )
            expected_source_sha256, _ = (
                source_manifest_identity_cursor(
                    cursor,
                    {
                        "identity_continuity_sha256": manifest.get(
                            "identity_continuity_sha256"
                        ),
                        "known_by": manifest.get("known_by"),
                    },
                    manifest_id=row["cohort_manifest_id"],
                    cohort_manifest_sha256=row[
                        "cohort_manifest_sha256"
                    ],
                    history_binding_sha256=row[
                        "history_binding_sha256"
                    ],
                    cutoff=source_cutoff,
                )
            )
    finally:
        pool.closeall()
    checks = {
        "manifest_version": (
            manifest.get("version") == TICKER_WORKSPACE_VERSION
        ),
        "reference_ticker": (
            manifest.get("reference", {}).get("ticker")
            == args.ticker.strip().upper()
        ),
        "source_manifest_sha256": (
            manifest.get("source_manifest_sha256")
            == expected_source_sha256
        ),
        "history_binding_sha256": (
            manifest.get("history_binding_sha256")
            == row["history_binding_sha256"]
        ),
        "cohort_id": (
            str(manifest.get("cohort_id"))
            == str(release["cohort_id"])
        ),
        "provider_tier": (
            manifest.get("provider_tier") == row["provider_tier"]
        ),
        "source_generation": (
            int(manifest.get("source_generation"))
            == int(row["source_generation"])
        ),
    }
    print(json.dumps({
        "release_id": str(args.release_id),
        "snapshot_id": str(row["snapshot_id"]),
        "ticker": args.ticker.strip().upper(),
        "checks": checks,
        "failed_checks": sorted(
            name for name, passed in checks.items() if not passed
        ),
        "stored": {
            "source_manifest_sha256": manifest.get(
                "source_manifest_sha256"
            ),
            "history_binding_sha256": manifest.get(
                "history_binding_sha256"
            ),
            "cohort_id": manifest.get("cohort_id"),
            "provider_tier": manifest.get("provider_tier"),
            "source_generation": manifest.get("source_generation"),
            "identity_continuity_sha256": manifest.get(
                "identity_continuity_sha256"
            ),
            "known_by": manifest.get("known_by"),
        },
        "expected": {
            "source_manifest_sha256": expected_source_sha256,
            "history_binding_sha256": row[
                "history_binding_sha256"
            ],
            "cohort_id": release["cohort_id"],
            "provider_tier": row["provider_tier"],
            "source_generation": row["source_generation"],
            "cohort_manifest_sha256": row[
                "cohort_manifest_sha256"
            ],
            "without_identity_continuity_sha256": canonical_sha256({
                "manifest_sha256": row[
                    "cohort_manifest_sha256"
                ],
                "history_binding_sha256": row[
                    "history_binding_sha256"
                ],
            }),
        },
        "provider_requests": 0,
        "database_writes": 0,
    }, indent=2, sort_keys=True, default=str))
    return 0 if all(checks.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
