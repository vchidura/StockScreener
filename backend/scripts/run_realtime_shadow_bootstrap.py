#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from contextlib import closing
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from psycopg2 import pool, sql


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.config import DataEngine, load_option_runtime_configuration
from realtime.contracts import CANARY_UNIVERSE, canonical_sha256
from realtime.database_access import verify_realtime_pool_target
from realtime.migrations import load_realtime_migrations, schema_fingerprint
from realtime.preflight import (
    EXPECTED_SCHEMA_VERSIONS,
    PassiveHostEvidence,
    PassivePreflightConfiguration,
    PassivePreflightEvidence,
    PassiveSchemaEvidence,
    ProviderTier,
    run_passive_preflight,
)
from realtime.preflight_repository import persist_preflight_profile
from realtime.reference_seed import (
    ReferenceKind,
    ReferenceSeedManifest,
    ReferenceSeedMember,
    SeedMemberStatus,
    SeedStatus,
)
from realtime.security import build_role_names, security_policy_sha256
from realtime.shadow_control_plane import (
    build_shadow_control_plane_plan,
    persist_shadow_control_plane,
    persist_shadow_instruments,
)


DEFAULT_REPORT_PATH = BACKEND_DIR / ".realtime-canary" / "shadow-bootstrap.json"
MODEL_BUNDLE_SHA256 = canonical_sha256(
    {
        "version": "advanced-realtime-shadow-model-bundle-v1",
        "status": "NOT_MATERIALIZED",
    }
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bootstrap the inert SHADOW control plane for the realtime canary."
    )
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--role-prefix", default="stocks_realtime")
    parser.add_argument("--report-path", default=str(DEFAULT_REPORT_PATH))
    operations = parser.add_mutually_exclusive_group()
    operations.add_argument("--execute", action="store_true")
    operations.add_argument("--status", action="store_true")
    args = parser.parse_args()
    if args.database != "stocks_realtime_v1" or args.confirm_database_name != args.database:
        parser.error("SHADOW bootstrap requires exact stocks_realtime_v1 confirmation")
    build_role_names(args.role_prefix)
    return args


def _environment(require_dsn: bool) -> tuple[dict[str, str], object]:
    load_dotenv(BACKEND_DIR / ".env", override=True)
    environ = dict(os.environ)
    environ.pop("OPTION_STOCK_BEHAVIOR_SHADOW_LAUNCH_FILE", None)
    environ.update(
        {
            "OPTION_DATA_ENGINE": DataEngine.POLYGON_DEVELOPER.value,
            "OPTION_EQUITY_CONTEXT_ENABLED": "false",
            "OPTION_STOCK_BEHAVIOR_SHADOW_ENABLED": "false",
            "OPTION_PACKAGE_ASSESSMENTS_ENABLED": "false",
            "OPTION_OUTCOME_UNAVAILABLE_EVIDENCE_ENABLED": "false",
            "OPTION_EXECUTION_ENGINE": "paper_proxy",
            "OPTION_UNIVERSE_MODE": "fixed",
            "OPTION_FIXED_STOCK_UNDERLYERS": "AAPL,MSFT,NVDA,AMZN,TSLA",
            "OPTION_FIXED_ETF_UNDERLYERS": "SPY,QQQ,IWM",
            "OPTION_STOCK_UNIVERSE_SIZE": "5",
            "OPTION_ETF_UNIVERSE_SIZE": "3",
            "OPTION_RAW_ARCHIVE_ENABLED": "false",
            "OPTION_START_READ_ONLY": "true",
            "OPTION_TRADE_INGESTION_ENABLED": "false",
        }
    )
    runtime = load_option_runtime_configuration(environ, BACKEND_DIR)
    if set(runtime.settings.underlyers) != set(CANARY_UNIVERSE.equity_tickers):
        raise RuntimeError("SHADOW options configuration changed the canary universe")
    if runtime.settings.data_engine is not DataEngine.POLYGON_DEVELOPER:
        raise RuntimeError("SHADOW options configuration changed the Developer engine")
    if not runtime.settings.start_read_only:
        raise RuntimeError("SHADOW options configuration must remain read-only")
    settings = {"REALTIME_RUNTIME_DSN": os.getenv("REALTIME_RUNTIME_DSN", "").strip()}
    if require_dsn and not settings["REALTIME_RUNTIME_DSN"]:
        raise RuntimeError("SHADOW bootstrap requires REALTIME_RUNTIME_DSN")
    return settings, runtime


def _write_report(path: Path, report: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    payload = (json.dumps(report, indent=2, sort_keys=True) + "\n").encode("utf-8")
    with temporary.open("wb") as output:
        output.write(payload)
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary, path)


def _role_connection(dsn: str, role_name: str):
    connection = psycopg2.connect(dsn=dsn)
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(role_name)))
        return connection
    except Exception:
        connection.close()
        raise


def _read_seed_and_identities(connection):
    with connection.cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout = '10s'")
        cursor.execute(
            "SELECT manifest.seed_id, manifest.seed_sha256, manifest.universe_sha256, "
            "manifest.session_date, manifest.status, manifest.reason_codes, "
            "manifest.created_at, manifest.effective_at, manifest.valid_until, "
            "manifest.source_generations FROM rt.seed_current AS current "
            "JOIN rt.seed_manifests AS manifest ON manifest.seed_id=current.seed_id "
            "WHERE current.scope='MARKET_REFERENCES'"
        )
        header = cursor.fetchone()
        if header is None:
            raise RuntimeError("SHADOW bootstrap requires a current reference seed")
        cursor.execute(
            "SELECT reference_kind, reference_key, source_id, payload_sha256, status, "
            "reason_codes, observed_at, received_at FROM rt.seed_manifest_members "
            "WHERE seed_id=%s ORDER BY reference_kind, reference_key",
            (str(header[0]),),
        )
        members = tuple(
            ReferenceSeedMember(
                ReferenceKind(kind), key, source_id, payload_sha256,
                SeedMemberStatus(status), observed_at, received_at, tuple(reasons),
            )
            for kind, key, source_id, payload_sha256, status, reasons, observed_at, received_at
            in cursor.fetchall()
        )
        cursor.execute(
            "SELECT DISTINCT ON (revision.ticker) revision.ticker, revision.security_id "
            "FROM ref.security_revisions AS revision "
            "WHERE revision.ticker=ANY(%s) AND revision.effective_from<=%s "
            "AND (revision.effective_to IS NULL OR revision.effective_to>%s) "
            "ORDER BY revision.ticker, revision.effective_from DESC",
            (
                list(CANARY_UNIVERSE.equity_tickers),
                header[7],
                header[7],
            ),
        )
        security_ids = {ticker: security_id for ticker, security_id in cursor.fetchall()}
        cursor.execute(
            "SELECT DISTINCT ON (revision.ticker) revision.ticker, contract.contract_id, "
            "contract.contract_ticker, version.version_id "
            "FROM ref.option_contracts AS contract "
            "JOIN ref.option_contract_versions AS version ON version.contract_id=contract.contract_id "
            "AND version.effective_from<=%s "
            "AND (version.effective_to IS NULL OR version.effective_to>%s) "
            "AND version.eligibility='VALIDATED_ACTIVE' "
            "JOIN ref.security_revisions AS revision "
            "ON revision.security_id=contract.underlying_security_id "
            "AND revision.effective_from<=%s "
            "AND (revision.effective_to IS NULL OR revision.effective_to>%s) "
            "WHERE revision.ticker=ANY(%s) "
            "ORDER BY revision.ticker, version.effective_from DESC, contract.contract_ticker",
            (
                header[7],
                header[7],
                header[7],
                header[7],
                list(CANARY_UNIVERSE.option_underlyings),
            ),
        )
        option_rows = cursor.fetchall()
    connection.rollback()
    option_contracts = {}
    for ticker, contract_id, contract_ticker, version_id in option_rows:
        if ticker in option_contracts:
            raise RuntimeError("SHADOW seed contains multiple admitted contracts per underlying")
        option_contracts[ticker] = (int(contract_id), contract_ticker, int(version_id))
    source_generations = header[9]
    seed = ReferenceSeedManifest(
        seed_id=header[0],
        seed_sha256=header[1],
        universe_sha256=header[2],
        session_date=header[3],
        status=SeedStatus(header[4]),
        reason_codes=tuple(header[5]),
        members=members,
        source_generations=tuple(sorted(dict(source_generations).items())),
        created_at=header[6],
        effective_at=header[7],
        valid_until=header[8],
    )
    return seed, security_ids, option_contracts


def _blocked_profile(seed: ReferenceSeedManifest):
    schema_sha256 = schema_fingerprint(load_realtime_migrations())
    policy_sha256 = security_policy_sha256()
    configuration = PassivePreflightConfiguration(
        provider_tier=ProviderTier.MASSIVE_DEVELOPER,
        account_alias="polygon-developer-shadow",
        universe=CANARY_UNIVERSE,
        expected_schema_sha256=schema_sha256,
        expected_security_policy_sha256=policy_sha256,
    )
    evidence = PassivePreflightEvidence(
        schema=PassiveSchemaEvidence(
            "stocks_realtime_v1",
            EXPECTED_SCHEMA_VERSIONS,
            schema_sha256,
            policy_sha256,
            True,
        ),
        host=PassiveHostEvidence(
            local_filesystem=False,
            journal_mode="UNVERIFIED",
            journal_synchronous=0,
            planned_journal_bytes=1,
            usable_disk_bytes=1,
            clock_verified=False,
        ),
    )
    return run_passive_preflight(configuration, evidence, checked_at=seed.effective_at)


def _plan(dsn: str, roles, runtime):
    with closing(_role_connection(dsn, roles.control)) as connection:
        seed, security_ids, option_contracts = _read_seed_and_identities(connection)
    profile = _blocked_profile(seed)
    return build_shadow_control_plane_plan(
        seed,
        profile,
        security_ids=security_ids,
        option_contracts=option_contracts,
        configuration_sha256=runtime.configuration_sha256,
        model_bundle_sha256=MODEL_BUNDLE_SHA256,
        valuation_sha256=runtime.valuation_policy_sha256,
        effective_at=seed.effective_at,
    ), profile


def _status(dsn: str, roles) -> dict[str, object]:
    with closing(_role_connection(dsn, roles.control)) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '10s'")
            cursor.execute("SELECT asset_class, COUNT(*) FROM rt.instruments GROUP BY asset_class")
            instruments = {name: count for name, count in cursor.fetchall()}
            cursor.execute(
                "SELECT cohort_id, cohort_sha256, manifest_id, mode FROM rt.cohorts "
                "WHERE mode='SHADOW' ORDER BY created_at DESC LIMIT 1"
            )
            cohort = cursor.fetchone()
            cursor.execute("SELECT COUNT(*) FROM rt.portal_cohort_current")
            portal_pointer_count = int(cursor.fetchone()[0])
        connection.rollback()
    return {
        "database": "stocks_realtime_v1",
        "instrument_counts": instruments,
        "shadow_cohort": None if cohort is None else {
            "cohort_id": str(cohort[0]),
            "cohort_sha256": cohort[1],
            "manifest_id": str(cohort[2]),
            "mode": cohort[3],
        },
        "portal_pointer_count": portal_pointer_count,
    }


def _execute(dsn: str, roles, runtime) -> dict[str, object]:
    principal_pool = pool.SimpleConnectionPool(1, 1, dsn=dsn)
    try:
        verify_realtime_pool_target(principal_pool, role_name=roles.control)
    finally:
        principal_pool.closeall()
    plan, profile = _plan(dsn, roles, runtime)
    with closing(_role_connection(dsn, roles.control)) as connection:
        persist_preflight_profile(connection, profile)
    with closing(_role_connection(dsn, roles.references)) as connection:
        instrument_ids = persist_shadow_instruments(connection, plan)
    with closing(_role_connection(dsn, roles.control)) as connection:
        persist_shadow_control_plane(connection, plan, instrument_ids)
    return {
        "state": "COMPLETE",
        "profile_status": profile.verdict.value,
        "readiness_status": profile.readiness.status.value,
        "instrument_count": len(instrument_ids),
        "member_count": len(plan.manifest.members),
        "manifest_id": str(plan.manifest_id),
        "cohort_id": str(plan.cohort_id),
        "cohort_mode": "SHADOW",
        "portal_activation": False,
        "provider_requests": 0,
    }


def main() -> int:
    args = parse_args()
    roles = build_role_names(args.role_prefix)
    settings, runtime = _environment(require_dsn=args.execute or args.status)
    if not (args.execute or args.status):
        print(json.dumps({
            "mode": "PLAN",
            "database": args.database,
            "configuration_sha256": runtime.configuration_sha256,
            "valuation_sha256": runtime.valuation_policy_sha256,
            "model_bundle_sha256": MODEL_BUNDLE_SHA256,
            "instrument_count": 16,
            "member_count": 40,
            "cohort_mode": "SHADOW",
            "profile_status": "BLOCKED_NON_PASS",
            "portal_activation": False,
            "provider_requests": 0,
        }, indent=2))
        print("No database writes or provider requests performed.")
        return 0
    if args.status:
        report = _status(settings["REALTIME_RUNTIME_DSN"], roles)
    else:
        try:
            report = _execute(settings["REALTIME_RUNTIME_DSN"], roles, runtime)
        except Exception as error:
            report = {
                "state": "FAILED",
                "error_type": type(error).__name__,
                "error_message": str(error),
                "values_exposed": False,
            }
            _write_report(Path(args.report_path).resolve(), report)
            raise
        _write_report(Path(args.report_path).resolve(), report)
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())