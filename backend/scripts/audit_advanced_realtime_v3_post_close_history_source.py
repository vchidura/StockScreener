#!/usr/bin/env python3
"""Audit the exact read-only production baseline for post-close V3 replay."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import UUID


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.contracts import (  # noqa: E402
    EQUITY_VALIDATION_UNIVERSE,
    canonical_sha256,
)
from realtime.database_access_v2 import APP_ROLE  # noqa: E402
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    validate_target_database_v2,
)
from scripts.audit_realtime_v2_target9_scanner_warmup import (  # noqa: E402
    build_report as build_warmup_report,
)
from scripts.inspect_realtime_v2_advanced_equity_membership import (  # noqa: E402
    _provider,
)


UTC = timezone.utc
REQUIRED_INTERVALS = {
    "1m",
    "5m",
    "15m",
    "30m",
    "1h",
    "1d",
    "1wk",
    "1mo",
}


def _read(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON artifact must be an object: {path}")
    return value


def _rows(cursor, statement, parameters=()):
    cursor.execute(statement, parameters)
    columns = tuple(column.name for column in cursor.description)
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]


def _write(path: Path, value: object) -> None:
    encoded = (
        json.dumps(value, indent=2, sort_keys=True, default=str) + "\n"
    ).encode("utf-8")
    with path.open("xb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--plan-id", type=UUID, required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument(
        "--history-valid-through",
        type=date.fromisoformat,
        required=True,
    )
    parser.add_argument("--as-of", type=datetime.fromisoformat, required=True)
    parser.add_argument("--replay-manifest", type=Path, required=True)
    parser.add_argument(
        "--options-reference-supplement",
        type=Path,
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    args.as_of = args.as_of.astimezone(UTC)
    args.replay_manifest = args.replay_manifest.resolve()
    if args.options_reference_supplement is not None:
        args.options_reference_supplement = (
            args.options_reference_supplement.resolve()
        )
    args.output = args.output.resolve()
    if (
        args.database != EXPECTED_V2_DATABASE_NAME
        or args.history_valid_through >= args.session_date
        or args.as_of.date() != args.session_date
        or not args.replay_manifest.is_file()
        or (
            args.options_reference_supplement is not None
            and not args.options_reference_supplement.is_file()
        )
        or args.output.exists()
        or not args.output.parent.is_dir()
    ):
        parser.error("post-close history source audit scope is invalid")
    return args


def execute(args):
    manifest = _read(args.replay_manifest)
    supplement = (
        None
        if args.options_reference_supplement is None
        else _read(args.options_reference_supplement)
    )
    if (
        manifest.get("status") != "READY_FOR_READ_ONLY_HISTORY_EXPORT"
        or manifest.get("session_date") != args.session_date.isoformat()
        or manifest.get("history_valid_through")
        != args.history_valid_through.isoformat()
        or manifest.get("historical_as_of_utc") != args.as_of.isoformat()
    ):
        raise ValueError("post-close replay manifest identity changed")
    contracts = tuple(manifest["cycles"]["contracts"])
    provider, pool = _provider()
    try:
        with provider.transaction(
            APP_ROLE,
            read_only=True,
            isolation_level="REPEATABLE READ",
        ) as unit:
            cursor = unit.cursor
            cursor.execute("SET LOCAL statement_timeout = '120s'")
            plans = _rows(
                cursor,
                "SELECT plan.plan_id::text,plan.cohort_id::text,"
                "plan.manifest_id::text,plan.session_date,"
                "plan.universe_target,plan.status,plan.terminal_outcome,"
                "plan.completed_at "
                "FROM rt.advanced_ingress_plans AS plan "
                "WHERE plan.plan_id=%s",
                (str(args.plan_id),),
            )
            if len(plans) != 1:
                raise RuntimeError("target-9 source plan is unavailable")
            plan = plans[0]
            bindings = _rows(
                cursor,
                "SELECT target_cohort_id::text,source_cohort_id::text,"
                "source_provider_tier,history_before,"
                "source_manifest_sha256,authorization_reference,"
                "binding_sha256 "
                "FROM rt.cohort_history_bindings "
                "WHERE target_cohort_id=%s AND history_before<=%s",
                (plan["cohort_id"], args.as_of),
            )
            if len(bindings) > 1:
                raise RuntimeError("target-9 history binding is ambiguous")
            history_binding = None if not bindings else bindings[0]
            history_cohort_id = UUID(
                plan["cohort_id"]
                if history_binding is None
                else history_binding["source_cohort_id"]
            )
            members = _rows(
                cursor,
                "SELECT member.instrument_id,"
                "instrument.security_id::text,revision.ticker,"
                "revision.active,revision.effective_from,"
                "revision.effective_to,revision.observed_at,"
                "revision.revision_id::text "
                "FROM rt.subscription_members AS member "
                "JOIN rt.instruments AS instrument USING(instrument_id) "
                "JOIN LATERAL ("
                "SELECT candidate.* "
                "FROM ref.security_revisions AS candidate "
                "WHERE candidate.security_id=instrument.security_id "
                "AND candidate.observed_at<=%s "
                "ORDER BY candidate.observed_at DESC,"
                "candidate.revision_id DESC LIMIT 1"
                ") AS revision ON true "
                "WHERE member.manifest_id=%s AND member.channel='AM' "
                "ORDER BY revision.ticker",
                (args.as_of, plan["manifest_id"]),
            )
            warmup = build_warmup_report(
                cursor,
                {
                    "database": args.database,
                    "read_only": True,
                    "isolation": "REPEATABLE READ",
                    "checked_at": datetime.now(UTC),
                },
                cohort_id=history_cohort_id,
                through_session=args.history_valid_through,
            )
            bar_coverage = _rows(
                cursor,
                "SELECT current.interval,current.adjustment_basis,"
                "revision.source_kind,COUNT(*) AS row_count,"
                "MIN(revision.session_date) AS first_session,"
                "MAX(revision.session_date) AS latest_session,"
                "MIN(revision.bar_start) AS first_bar_start,"
                "MAX(revision.bar_end) AS latest_bar_end "
                "FROM rt.bar_current AS current "
                "JOIN rt.bar_revisions AS revision "
                "ON revision.bar_revision_id=current.bar_revision_id "
                "AND revision.observed_at=current.observed_at "
                "WHERE current.cohort_id=%s "
                "AND current.instrument_id=ANY(%s) "
                "AND revision.session_date<=%s "
                "GROUP BY current.interval,current.adjustment_basis,"
                "revision.source_kind "
                "ORDER BY current.interval,current.adjustment_basis,"
                "revision.source_kind",
                (
                    str(history_cohort_id),
                    [int(row["instrument_id"]) for row in members],
                    args.history_valid_through,
                ),
            )
            security_ids = [row["security_id"] for row in members]
            live_references = _rows(
                cursor,
                "SELECT DISTINCT ON (revision.security_id) "
                "revision.security_id::text,revision.revision_id::text,"
                "revision.ticker,revision.effective_from,"
                "revision.effective_to,revision.observed_at,"
                "revision.payload_sha256 "
                "FROM ref.security_revisions AS revision "
                "WHERE revision.security_id=ANY(%s::uuid[]) "
                "AND revision.effective_from<=%s "
                "AND (revision.effective_to IS NULL "
                "OR revision.effective_to>%s) "
                "ORDER BY revision.security_id,revision.observed_at,"
                "revision.revision_id",
                (
                    security_ids,
                    manifest["terminal_failure"][
                        "last_completed_boundary_utc"
                    ],
                    manifest["cycles"]["first_boundary_utc"],
                ),
            )
            reference_counts = {}
            for name, statement, parameters in (
                (
                    "security_revisions",
                    "SELECT COUNT(*) FROM ref.security_revisions "
                    "WHERE security_id=ANY(%s::uuid[]) AND observed_at<=%s",
                    (security_ids, args.as_of),
                ),
                (
                    "corporate_action_revisions",
                    "SELECT COUNT(*) FROM ref.corporate_action_revisions "
                    "WHERE security_id=ANY(%s::uuid[]) AND observed_at<=%s",
                    (security_ids, args.as_of),
                ),
                (
                    "corporate_action_coverage",
                    "SELECT COUNT(*) FROM ref.corporate_action_coverage "
                    "WHERE security_id=ANY(%s::uuid[]) AND observed_at<=%s",
                    (security_ids, args.as_of),
                ),
                (
                    "risk_free_rate_observations",
                    "SELECT COUNT(*) FROM ref.risk_free_rate_observations "
                    "WHERE rate_date<=%s",
                    (args.history_valid_through,),
                ),
                (
                    "exact_option_contracts",
                    "SELECT COUNT(*) FROM ref.option_contracts "
                    "WHERE contract_ticker=ANY(%s)",
                    (list(contracts),),
                ),
                (
                    "exact_option_contract_versions",
                    "SELECT COUNT(*) "
                    "FROM ref.option_contract_versions AS version "
                    "JOIN ref.option_contracts AS contract USING(contract_id) "
                    "WHERE contract.contract_ticker=ANY(%s) "
                    "AND version.observed_at<=%s",
                    (list(contracts), args.as_of),
                ),
                (
                    "exact_option_oi_baselines",
                    "SELECT COUNT(*) "
                    "FROM ref.oi_baselines AS baseline "
                    "JOIN ref.option_contracts AS contract USING(contract_id) "
                    "WHERE contract.contract_ticker=ANY(%s) "
                    "AND baseline.observed_at<=%s",
                    (list(contracts), args.as_of),
                ),
            ):
                cursor.execute(statement, parameters)
                reference_counts[name] = int(cursor.fetchone()[0])
    finally:
        pool.closeall()

    expected_symbols = set(EQUITY_VALIDATION_UNIVERSE.equity_tickers)
    observed_symbols = {str(row["ticker"]) for row in members}
    intervals = {str(row["interval"]) for row in bar_coverage}
    failures = []
    if (
        plan["status"] != "COMPLETE"
        or int(plan["universe_target"]) != 9
        or observed_symbols != expected_symbols
        or len(members) != 9
    ):
        failures.append("TARGET9_SOURCE_MEMBERSHIP")
    if warmup["verdict"] != "PASS":
        failures.append("TARGET9_WARMUP")
    if intervals != REQUIRED_INTERVALS:
        failures.append("INTERVAL_COVERAGE")
    if reference_counts["security_revisions"] < 9:
        failures.append("SECURITY_REFERENCE")
    if len(live_references) != 9:
        failures.append("LIVE_SECURITY_REFERENCE")
    if reference_counts["risk_free_rate_observations"] <= 0:
        failures.append("TREASURY_REFERENCE")
    supplement_valid = (
        supplement is not None
        and supplement.get("status") == "PASS"
        and supplement.get("classification")
        == "SEALED_ARTIFACT_DISPOSABLE_ONLY"
        and supplement.get("replay_manifest_semantic_sha256")
        == manifest["semantic_sha256"]
        and supplement.get("contract_count") == len(contracts)
        and supplement.get("oi_baseline_count") == len(contracts)
        and {
            row.get("contract", {}).get("contract_ticker")
            for row in supplement.get("rows", ())
        }
        == set(contracts)
    )
    if not supplement_valid:
        if reference_counts["exact_option_contracts"] != len(contracts):
            failures.append("OPTION_CATALOG")
        if reference_counts["exact_option_contract_versions"] < len(contracts):
            failures.append("OPTION_CATALOG_VERSIONS")
        if reference_counts["exact_option_oi_baselines"] < len(contracts):
            failures.append("OPTION_PRIOR_OI")
    report = {
        "schema": "advanced-realtime-v3-post-close-history-source-v1",
        "status": "PASS" if not failures else "BLOCKED",
        "database": args.database,
        "transaction": {
            "role": APP_ROLE,
            "read_only": True,
            "isolation": "REPEATABLE READ",
            "statement_timeout_seconds": 120,
        },
        "as_of_utc": args.as_of.isoformat(),
        "history_valid_through": args.history_valid_through.isoformat(),
        "replay_manifest": {
            "path": str(args.replay_manifest),
            "file_sha256": hashlib.sha256(
                args.replay_manifest.read_bytes()
            ).hexdigest(),
            "semantic_sha256": manifest["semantic_sha256"],
        },
        "source_plan": plan,
        "history_binding": history_binding,
        "history_cohort_id": str(history_cohort_id),
        "members": members,
        "warmup": warmup,
        "bar_coverage": bar_coverage,
        "reference_counts": reference_counts,
        "live_references": live_references,
        "live_reference_observed_after_as_of": [
            row
            for row in live_references
            if datetime.fromisoformat(str(row["observed_at"]))
            > args.as_of
        ],
        "options_reference": {
            "mode": (
                "PRODUCTION"
                if not supplement_valid
                else "SEALED_ARTIFACT_DISPOSABLE_ONLY"
            ),
            "supplement": (
                None
                if args.options_reference_supplement is None
                else {
                    "path": str(args.options_reference_supplement),
                    "file_sha256": hashlib.sha256(
                        args.options_reference_supplement.read_bytes()
                    ).hexdigest(),
                    "semantic_sha256": supplement.get("semantic_sha256"),
                }
            ),
        },
        "failures": failures,
        "effects": {
            "provider_requests": 0,
            "production_database_reads": 1,
            "production_database_writes": 0,
            "disposable_database_writes": 0,
            "service_actions": 0,
            "publication": 0,
            "dispatch": 0,
            "delivery": 0,
            "broker": 0,
            "trading": 0,
        },
    }
    report["semantic_sha256"] = canonical_sha256(report)
    _write(args.output, report)
    return report


def main(argv=None):
    args = parse_args(argv)
    report = execute(args)
    print(json.dumps({
        "status": report["status"],
        "source_plan_id": report["source_plan"]["plan_id"],
        "source_cohort_id": report["source_plan"]["cohort_id"],
        "member_count": len(report["members"]),
        "intervals": sorted({
            row["interval"] for row in report["bar_coverage"]
        }),
        "failures": report["failures"],
        "semantic_sha256": report["semantic_sha256"],
        "output": str(args.output),
    }, indent=2, sort_keys=True))
    return 0 if report["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
