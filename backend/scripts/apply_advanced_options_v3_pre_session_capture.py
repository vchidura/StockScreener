#!/usr/bin/env python3
"""Plan or apply one sealed V3 pre-session option capture."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from dotenv import dotenv_values
from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.calendar import OptionExchangeCalendar  # noqa: E402
from realtime.contracts import UniverseManifest, canonical_sha256  # noqa: E402
from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    validate_target_database_v2,
)
from realtime.reference_adapters import _option_fact, _security_id  # noqa: E402
from realtime.reference_facts import (  # noqa: E402
    ReferenceFact,
    ReferenceFactRelation,
)
from realtime.reference_repository import (  # noqa: E402
    persist_reference_facts_cursor,
)
from realtime.reference_seed import ReferenceKind  # noqa: E402
from realtime.reference_service import ReferenceSeedRequest  # noqa: E402
from scripts.apply_advanced_options_p8_catalog_hydration import (  # noqa: E402
    SCHEMA,
    _canonical_sha256,
    _current_rows,
    _diff,
    _entries,
    apply_manifest_cursor,
)
from scripts.realtime_script_support import write_report  # noqa: E402


UTC = timezone.utc
UNDERLYINGS = ("QQQ", "SPY")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--capture", type=Path, required=True)
    parser.add_argument("--expected-capture-semantic-sha256", required=True)
    parser.add_argument("--expected-capture-file-sha256", required=True)
    parser.add_argument("--reference-valid-until", type=datetime.fromisoformat, required=True)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--expected-contract-inserts", type=int)
    parser.add_argument("--expected-version-inserts", type=int)
    parser.add_argument("--expected-instrument-inserts", type=int)
    parser.add_argument("--expected-oi-inserts", type=int)
    parser.add_argument("--report-path", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("V3 capture application targets stocks_realtime_v2")
    args.capture = args.capture.resolve()
    args.report_path = args.report_path.resolve()
    if not args.capture.is_file():
        parser.error("--capture must exist")
    if args.report_path.exists():
        parser.error("--report-path must be new")
    if (
        args.reference_valid_until.tzinfo is None
        or args.reference_valid_until.utcoffset() is None
        or not args.authorization_reference.strip()
        or any(
            getattr(args, name) < 0
            for name in (
                "expected_contract_inserts",
                "expected_version_inserts",
                "expected_instrument_inserts",
                "expected_oi_inserts",
            )
        )
        or (
            args.apply
            and any(
                getattr(args, name) is None
                for name in (
                    "expected_contract_inserts",
                    "expected_version_inserts",
                    "expected_instrument_inserts",
                    "expected_oi_inserts",
                )
            )
        )
    ):
        parser.error("V3 capture application scope is invalid")
    args.reference_valid_until = args.reference_valid_until.astimezone(UTC)
    return args


def _load_capture(args):
    body = args.capture.read_bytes()
    capture = json.loads(body)
    unsigned = dict(capture)
    semantic = unsigned.pop("semantic_sha256", None)
    if (
        hashlib.sha256(body).hexdigest()
        != args.expected_capture_file_sha256
        or semantic != args.expected_capture_semantic_sha256
        or canonical_sha256(unsigned) != semantic
        or capture.get("contract")
        != "advanced-options-v3-pre-session-capture-v1"
        or capture.get("provider_requests") != 2
        or len(capture.get("selected_snapshots") or {}) != 4
    ):
        raise ValueError("sealed V3 pre-session capture identity changed")
    return capture


def _hydration_manifest(capture):
    responses = []
    snapshots = capture["selected_snapshots"]
    for underlyer in UNDERLYINGS:
        candidates = []
        for ticker, raw in sorted(snapshots.items()):
            details = dict(raw["details"])
            if str(raw["underlying_asset"]["ticker"]).upper() != underlyer:
                continue
            payload = {
                **details,
                "underlying_ticker": underlyer,
            }
            candidates.append({
                "ticker": ticker,
                "underlying_ticker": underlyer,
                "underlying_security_id": str(_security_id(underlyer)),
                "contract_type": details["contract_type"],
                "expiration_date": details["expiration_date"],
                "strike_price": details["strike_price"],
                "exercise_style": details["exercise_style"],
                "shares_per_contract": details["shares_per_contract"],
                "primary_exchange": details.get("primary_exchange"),
                "provider_payload_sha256": _canonical_sha256(payload),
                "provider_payload": payload,
            })
        if len(candidates) != 2:
            raise ValueError("sealed capture underlyer membership changed")
        responses.append({
            "underlyer": underlyer,
            "candidates": candidates,
        })
    manifest = {
        "schema": SCHEMA,
        "captured_at_utc": capture["captured_at_utc"],
        "session_date": capture["session_date"],
        "provider_requests": 2,
        "pagination_requests": 0,
        "database_writes": 0,
        "responses": responses,
    }
    manifest["manifest_sha256"] = _canonical_sha256(manifest)
    return manifest


def _oi_facts(capture, request):
    settlement = OptionExchangeCalendar().previous_session_date(
        request.session_date
    )
    facts = []
    for ticker, raw in sorted(capture["selected_snapshots"].items()):
        underlyer = str(raw["underlying_asset"]["ticker"]).upper()
        details = {
            **raw["details"],
            "underlying_ticker": underlyer,
        }
        catalog = _option_fact(underlyer, details, request)
        contract_id = dict(catalog.values)["contract_id"]
        open_interest = raw.get("open_interest")
        if not isinstance(open_interest, int) or open_interest < 0:
            raise ValueError("sealed capture open interest is invalid")
        digest = canonical_sha256(raw)
        facts.append(ReferenceFact.build(
            ReferenceFactRelation.OI_BASELINE,
            ReferenceKind.OI_COVERAGE.value,
            underlyer,
            baseline_id=uuid5(
                NAMESPACE_URL,
                "advanced-options-v3-oi:"
                f"{ticker}:{settlement}:{request.created_at.isoformat()}:"
                f"{digest}",
            ),
            contract_id=contract_id,
            settlement_session=settlement,
            open_interest=open_interest,
            source="massive",
            date_basis="PROVIDER_CHAIN_SNAPSHOT_PRIOR_SESSION",
            observed_session=request.session_date,
            observed_at=request.created_at,
            received_at=request.created_at,
            payload_sha256=digest,
            supersedes_baseline_id=None,
        ))
    return tuple(facts), settlement


def _provider():
    values = dotenv_values(BACKEND_DIR / ".env")
    dsn = str(
        os.getenv("REALTIME_V2_APP_DSN")
        or values.get("REALTIME_V2_APP_DSN")
        or ""
    ).strip()
    if not dsn:
        raise RuntimeError("REALTIME_V2_APP_DSN is required")
    pool = ThreadedConnectionPool(1, 2, dsn=dsn, options="-c timezone=UTC")
    provider = V2ConnectionProvider(
        {APP_ROLE: pool},
        V2AccessConfig(
            security_mode="hardened",
            role_prefix=EXPECTED_V2_DATABASE_NAME,
        ),
    )
    provider.verify()
    return provider, pool


def _authorized_effects(
    args,
    catalog_effects,
    oi_inserts,
    *,
    enforce=True,
):
    authorized = {
        "contract_inserts": args.expected_contract_inserts,
        "version_inserts": args.expected_version_inserts,
        "instrument_inserts": args.expected_instrument_inserts,
        "oi_inserts": args.expected_oi_inserts,
    }
    observed = {
        key: catalog_effects[key]
        for key in (
            "contract_inserts",
            "version_inserts",
            "instrument_inserts",
        )
    }
    observed = {
        **observed,
        "oi_inserts": oi_inserts,
    }
    if enforce and observed != authorized:
        raise ValueError(
            "sealed capture application effects changed: "
            f"authorized={authorized}, observed={observed}"
        )
    return observed


def main(argv=None) -> int:
    args = parse_args(argv)
    capture = _load_capture(args)
    manifest = _hydration_manifest(capture)
    captured_at = datetime.fromisoformat(capture["captured_at_utc"]).astimezone(UTC)
    session_date = datetime.fromisoformat(
        capture["session_date"] + "T00:00:00+00:00"
    ).date()
    if (
        captured_at.date() != session_date
        or args.reference_valid_until.date() != session_date
        or args.reference_valid_until <= captured_at
    ):
        raise ValueError("sealed capture application clocks changed")
    request = ReferenceSeedRequest(
        UniverseManifest(
            "advanced-options-v3-pre-session-v1",
            UNDERLYINGS,
            UNDERLYINGS,
        ),
        session_date,
        captured_at,
        captured_at,
        args.reference_valid_until,
    )
    oi_facts, settlement = _oi_facts(capture, request)
    entries = _entries(manifest, expected_candidate_count=4)
    provider, pool = _provider()
    try:
        with provider.transaction(
            APP_ROLE,
            read_only=not args.apply,
            isolation_level="SERIALIZABLE",
            dict_cursor=True,
        ) as unit:
            current = _current_rows(
                unit.cursor,
                tuple(
                    entry.catalog.contract_ticker
                    for entry, _ in entries
                ),
            )
            expected = _diff(entries, current)
            if expected["conflicts"]:
                raise ValueError("sealed capture catalog conflicts")
            oi_baseline_ids = tuple(
                str(fact.identity[1]) for fact in oi_facts
            )
            unit.cursor.execute(
                "SELECT baseline_id::text FROM ref.oi_baselines "
                "WHERE baseline_id=ANY(%s::uuid[]) ORDER BY baseline_id",
                (list(oi_baseline_ids),),
            )
            existing_oi_ids = {
                str(row["baseline_id"]) for row in unit.cursor.fetchall()
            }
            effects = _authorized_effects(
                args,
                expected,
                len(oi_baseline_ids) - len(existing_oi_ids),
                enforce=args.apply,
            )
            if args.apply:
                applied = apply_manifest_cursor(
                    unit.cursor,
                    manifest,
                    expected_candidate_count=4,
                    expected_contract_inserts=expected[
                        "contract_inserts"
                    ],
                    expected_version_inserts=expected[
                        "version_inserts"
                    ],
                    expected_instrument_inserts=expected[
                        "instrument_inserts"
                    ],
                )
                persist_reference_facts_cursor(unit.cursor, oi_facts)
                unit.cursor.execute(
                    "SELECT baseline_id::text FROM ref.oi_baselines "
                    "WHERE baseline_id=ANY(%s::uuid[])",
                    (list(oi_baseline_ids),),
                )
                if {
                    str(row["baseline_id"])
                    for row in unit.cursor.fetchall()
                } != set(oi_baseline_ids):
                    raise RuntimeError(
                        "sealed capture OI postcondition failed"
                    )
            else:
                applied = {
                    "contract_inserts": 0,
                    "version_inserts": 0,
                    "instrument_inserts": 0,
                }
    finally:
        pool.closeall()
    report = {
        "contract": "advanced-options-v3-pre-session-capture-application-v1",
        "mode": "APPLY" if args.apply else "PLAN",
        "status": "COMPLETE" if args.apply else "PLANNED",
        "capture_semantic_sha256": capture["semantic_sha256"],
        "hydration_manifest_sha256": manifest["manifest_sha256"],
        "authorization_reference": args.authorization_reference,
        "expected": effects,
        "applied": applied,
        "oi_inserts": effects["oi_inserts"] if args.apply else 0,
        "settlement_session": settlement.isoformat(),
        "provider_requests": 0,
        "database_writes": (
            sum(applied.values()) + effects["oi_inserts"]
            if args.apply
            else 0
        ),
    }
    report["semantic_sha256"] = canonical_sha256(report)
    write_report(args.report_path, report)
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
