#!/usr/bin/env python3
"""Read-only production and stored-data preflight for Advanced Options V3."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import psycopg2
from dotenv import dotenv_values
from psycopg2.extensions import make_dsn
from psycopg2.extras import RealDictCursor


BACKEND_DIR = Path(__file__).resolve().parent.parent
REPOSITORY_ROOT = BACKEND_DIR.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.calendar import OptionExchangeCalendar  # noqa: E402
from realtime.advanced_options_equity_input import (  # noqa: E402
    advanced_options_equity_context_requirements_payload,
    initial_advanced_options_equity_context_requirements,
)
from realtime.advanced_options_flow_policy import (  # noqa: E402
    load_advanced_options_flow_policy,
)
from realtime.advanced_options_p7_selection import (  # noqa: E402
    P7TransportSelectionPolicy,
)
from realtime.advanced_options_p8_production import (  # noqa: E402
    _p8_manifest_policy,
)
from realtime.advanced_options_provider_contract import (  # noqa: E402
    MASSIVE_OPTIONS_PROVIDER_CONTRACT,
    load_default_options_exchange_reference,
)
from realtime.advanced_options_reference_producer import (  # noqa: E402
    _coalesce_terminal_open_interest,
)
from realtime.advanced_options_v3_deployment import (  # noqa: E402
    ADVANCED_OPTIONS_V3_PROFILE_SHA256,
    advanced_options_v3_profile_payload,
)
from realtime.contracts import canonical_sha256  # noqa: E402
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    V2_MIGRATIONS_DIR,
    load_realtime_migrations,
    schema_fingerprint,
)
from realtime.option_flow.conditions import (  # noqa: E402
    load_default_live_semantics,
)
from realtime.relation_registry import RELATIONS  # noqa: E402
from realtime.security_v2 import (  # noqa: E402
    audit_database_security_v2,
    build_role_names_v2,
)


UTC = timezone.utc
TARGET_SYMBOLS = ("SPY", "QQQ", "IWM", "AAPL", "MSFT", "NVDA", "AMZN", "TSLA", "NFLX")
OPTION_UNDERLYINGS = ("QQQ", "SPY")
V3_MIGRATIONS = (
    "033_advanced_options_equity_input_bindings",
    "034_advanced_options_v3_authorities",
    "035_advanced_options_v3_stage_barrier",
)
SOURCE_ANCHORS = (
    "backend/realtime/advanced_realtime_session_scheduler.py",
    "backend/realtime/advanced_options_equity_input.py",
    "backend/realtime/advanced_options_native_loader.py",
    "backend/realtime/advanced_options_reference_producer.py",
    "backend/realtime/advanced_options_p8_production.py",
    "backend/realtime/advanced_options_v3_authority.py",
    "backend/realtime/advanced_options_v3_deployment.py",
    "backend/realtime/stage_graph.py",
    "backend/realtime/normalizer.py",
    "docker-compose.realtime-v2.yml",
)


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("timestamp must include an offset")
    return parsed.astimezone(UTC)


def _json_default(value):
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Path):
        return value.as_posix()
    raise TypeError(f"unsupported JSON value: {type(value).__name__}")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--equity-cutoff-utc", type=_timestamp, required=True)
    parser.add_argument("--options-cutoff-utc", type=_timestamp, required=True)
    parser.add_argument("--observed-at-utc", type=_timestamp, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def _dsn() -> str:
    values = dotenv_values(BACKEND_DIR / ".env")
    explicit = str(
        os.getenv("REALTIME_V2_ADMIN_DSN")
        or values.get("REALTIME_V2_ADMIN_DSN")
        or ""
    ).strip()
    if explicit:
        return explicit
    settings = {
        name: str(os.getenv(name) or values.get(name) or "").strip()
        for name in (
            "DB_HOST",
            "DB_PORT",
            "POSTGRES_ADMIN_USER",
            "POSTGRES_ADMIN_PASSWORD",
        )
    }
    missing = [name for name, value in settings.items() if not value]
    if missing:
        raise RuntimeError(
            "V3 operations audit requires: " + ", ".join(missing)
        )
    return make_dsn(
        dbname=EXPECTED_V2_DATABASE_NAME,
        user=settings["POSTGRES_ADMIN_USER"],
        password=settings["POSTGRES_ADMIN_PASSWORD"],
        host=settings["DB_HOST"],
        port=settings["DB_PORT"],
    )


def _rows(cursor, statement: str, parameters=()):
    cursor.execute(statement, parameters)
    return tuple(dict(row) for row in cursor.fetchall())


def _row(cursor, statement: str, parameters=()):
    rows = _rows(cursor, statement, parameters)
    return None if not rows else rows[0]


def _relation_exists(cursor, qualified_name: str) -> bool:
    cursor.execute("SELECT to_regclass(%s) IS NOT NULL AS present", (qualified_name,))
    return bool(cursor.fetchone()["present"])


def _source_evidence() -> dict[str, object]:
    files = []
    for relative in SOURCE_ANCHORS:
        path = REPOSITORY_ROOT / relative
        body = path.read_bytes()
        files.append({
            "path": relative,
            "bytes": len(body),
            "sha256": hashlib.sha256(body).hexdigest(),
        })
    p8_policy = _p8_manifest_policy()
    flow_policy = load_advanced_options_flow_policy()
    semantics = load_default_live_semantics()
    exchange = load_default_options_exchange_reference()
    equity_context = advanced_options_equity_context_requirements_payload(
        initial_advanced_options_equity_context_requirements()
    )
    return {
        "files": files,
        "v3_profile_sha256": ADVANCED_OPTIONS_V3_PROFILE_SHA256,
        "v3_profile": advanced_options_v3_profile_payload(),
        "policies": {
            "p7_transport_selection_sha256": P7TransportSelectionPolicy().sha256,
            "p8_manifest_policy_sha256": p8_policy.sha256,
            "advanced_flow_policy_sha256": flow_policy.sha256,
            "condition_semantics_sha256": semantics.semantics_sha256,
            "exchange_reference_sha256": exchange.sha256,
            "provider_contract_sha256": MASSIVE_OPTIONS_PROVIDER_CONTRACT.sha256,
            "equity_context_policy_sha256": canonical_sha256(equity_context),
            "equity_context_policy": equity_context,
        },
    }


def _freeze_facts(gaps, source) -> dict[str, object]:
    available = {
        row["fact"]: row["evidence"]
        for row in gaps["buckets"]["AVAILABLE"]
    }
    profiles = available.get("active_target9_stocks_am_profile", ())
    cohorts = available.get("target9_equity_cohort", ())
    files = {
        row["path"]: row["sha256"]
        for row in source["files"]
    }
    if (
        not isinstance(profiles, (list, tuple))
        or len(profiles) != 1
        or not isinstance(cohorts, (list, tuple))
        or len(cohorts) != 1
    ):
        return {}
    profile = profiles[0]
    cohort = cohorts[0]
    required = {
        "stored_active_profile_sha256": profile.get(
            "runtime_profile_sha256"
        ),
        "cohort_sha256": cohort.get("cohort_sha256"),
        "manifest_sha256": cohort.get("manifest_sha256"),
        "normalizer_source_sha256": files.get(
            "backend/realtime/normalizer.py"
        ),
        "policy_command_sha256": files.get(
            "backend/realtime/advanced_realtime_session_scheduler.py"
        ),
    }
    if any(
        not isinstance(value, str) or len(value) != 64
        for value in required.values()
    ):
        return {}
    return {
        **required,
        "rendered_source_profile_sha256": required[
            "stored_active_profile_sha256"
        ],
        "manifest_cohort_sha256": canonical_sha256({
            "cohort_sha256": required["cohort_sha256"],
            "manifest_sha256": required["manifest_sha256"],
        }),
        "all_gap_counts_zero": all(
            count == 0
            for name, count in gaps["counts"].items()
            if name != "AVAILABLE"
        ),
    }


def _classify(status: str, fact: str, *, cause: str, evidence: object):
    return {
        "status": status,
        "fact": fact,
        "causal_clock": cause,
        "evidence": evidence,
    }


def _exactly_one_status(rows) -> str:
    if len(rows) == 1:
        return "AVAILABLE"
    return "MISSING_STORED" if not rows else "AMBIGUOUS"


def _runtime_record_classification(
    scope,
    observed_at: datetime,
    session_date: date,
):
    nonterminal_windows = tuple(
        row for row in scope["conflicting_windows"]
        if row["state"] in {"PENDING", "RUNNING"}
    )
    stale_windows = tuple(
        row for row in nonterminal_windows
        if row["deadline"] < observed_at
        and (
            row["lease_until"] is None
            or row["lease_until"] < observed_at
        )
    )
    active_windows = tuple(
        row for row in nonterminal_windows
        if row not in stale_windows
    )
    stale_streams = tuple(
        row for row in scope["nonterminal_stream_sessions"]
        if row["ended_at"] is not None
        and row["session_date"] >= session_date
    )
    active_streams = tuple(
        row for row in scope["nonterminal_stream_sessions"]
        if row["ended_at"] is None
    )
    retained_streams = tuple(
        row for row in scope["nonterminal_stream_sessions"]
        if row["ended_at"] is not None
        and row["session_date"] < session_date
    )
    return (
        {
            "leases": scope["live_worker_leases"],
            "windows": active_windows,
            "streams": active_streams,
        },
        {
            "windows": stale_windows,
            "streams": stale_streams,
        },
        {
            "streams": retained_streams,
        },
    )


def _production_identity(cursor, expected_migrations):
    metadata = _row(
        cursor,
        "SELECT current_database() AS database,current_user AS current_user,"
        "session_user AS session_user,current_setting('transaction_read_only') "
        "AS transaction_read_only,current_setting('transaction_isolation') "
        "AS transaction_isolation,clock_timestamp() AS database_observed_at,"
        "current_setting('server_version') AS server_version",
    )
    identity = _rows(
        cursor,
        "SELECT lineage_name,database_name::text,created_at "
        "FROM rt.database_identity ORDER BY lineage_name",
    )
    applied = _rows(
        cursor,
        "SELECT version,checksum_sha256,applied_at "
        "FROM rt.schema_migrations ORDER BY version",
    )
    expected_by_version = {
        migration.version: migration.checksum_sha256
        for migration in expected_migrations
    }
    applied_by_version = {
        row["version"]: row["checksum_sha256"] for row in applied
    }
    unknown = tuple(sorted(set(applied_by_version) - set(expected_by_version)))
    drift = tuple(sorted(
        version
        for version in set(applied_by_version) & set(expected_by_version)
        if applied_by_version[version] != expected_by_version[version]
    ))
    pending = tuple(
        migration.version
        for migration in expected_migrations
        if migration.version not in applied_by_version
    )
    security_error = None
    security = ()
    try:
        security = tuple(
            {"check": check, "verdict": verdict}
            for check, verdict in audit_database_security_v2(
                cursor.connection,
                build_role_names_v2(EXPECTED_V2_DATABASE_NAME),
            )
        )
    except psycopg2.Error as error:
        security_error = {
            "sqlstate": error.pgcode,
            "detail": str(error).splitlines()[0],
        }
        cursor.connection.rollback()
        cursor.connection.set_session(
            readonly=True,
            autocommit=False,
            isolation_level="REPEATABLE READ",
        )
        cursor.execute("SET LOCAL statement_timeout = '20s'")
    return {
        "metadata": metadata,
        "database_identity": identity,
        "expected_schema_sha256": schema_fingerprint(expected_migrations),
        "migration_count": {
            "expected": len(expected_migrations),
            "applied": len(applied),
        },
        "pending_migrations": pending,
        "unknown_migrations": unknown,
        "checksum_drift": drift,
        "v3_migrations": {
            version: (
                "APPLIED" if version in applied_by_version else "PENDING"
            )
            for version in V3_MIGRATIONS
        },
        "security_audit": security,
        "security_audit_error": security_error,
    }


def _production_scope(cursor, session_date: date):
    profiles = _rows(
        cursor,
        "SELECT runtime_profile_sha256,runtime_profile_version,"
        "universe_target,ladder_position,previous_universe_target,"
        "provider_tier,feed,channels,status,configuration_sha256,"
        "runtime_policy_sha256,activated_at,superseded_at "
        "FROM rt.advanced_ingress_runtime_profiles "
        "WHERE universe_target=9 OR status='ACTIVE' "
        "ORDER BY feed,channels,universe_target,status,runtime_profile_sha256",
    )
    plans = _rows(
        cursor,
        "SELECT plan.plan_id::text,plan.plan_sha256,"
        "plan.authorization_id::text,plan.service_plan_id::text,"
        "plan.cohort_id::text,plan.manifest_id::text,plan.session_date,"
        "plan.universe_target,plan.mode,plan.runtime_profile_version,"
        "plan.runtime_profile_sha256,plan.authorized_start_at,"
        "plan.authorized_end_at,plan.status,plan.terminal_outcome,"
        "plan.activated_at,plan.completed_at,auth.feed,auth.channel,"
        "auth.status AS authorization_status "
        "FROM rt.advanced_ingress_plans AS plan "
        "JOIN rt.advanced_stream_authorizations AS auth "
        "ON auth.authorization_id=plan.authorization_id "
        "WHERE plan.universe_target=9 OR plan.session_date>=%s "
        "ORDER BY plan.session_date DESC,plan.created_at DESC LIMIT 50",
        (session_date,),
    )
    target_cohort_ids = tuple(dict.fromkeys(
        row["cohort_id"]
        for row in plans
        if row["universe_target"] == 9 and row["feed"] == "STOCKS"
    ))
    cohort_members = ()
    if target_cohort_ids:
        cohort_members = _rows(
            cursor,
            "SELECT cohort.cohort_id::text,cohort.cohort_sha256,"
            "manifest.session_date,manifest.manifest_id::text,"
            "manifest.manifest_sha256,manifest.manifest_kind,"
            "manifest.stock_count,manifest.option_underlying_count,"
            "array_agg(revision.ticker ORDER BY member.priority) AS symbols,"
            "array_agg(member.channel ORDER BY member.priority) AS channels "
            "FROM rt.cohorts AS cohort "
            "JOIN rt.subscription_manifests AS manifest "
            "ON manifest.manifest_id=cohort.manifest_id "
            "JOIN rt.subscription_members AS member "
            "ON member.manifest_id=manifest.manifest_id "
            "JOIN rt.instruments AS instrument "
            "ON instrument.instrument_id=member.instrument_id "
            "JOIN LATERAL ("
            "SELECT ticker FROM ref.security_revisions "
            "WHERE security_id=instrument.security_id "
            "ORDER BY effective_from DESC,observed_at DESC LIMIT 1"
            ") AS revision ON true "
            "WHERE cohort.cohort_id=ANY(%s::uuid[]) "
            "GROUP BY cohort.cohort_id,cohort.cohort_sha256,"
            "manifest.session_date,manifest.manifest_id,"
            "manifest.manifest_sha256,manifest.manifest_kind,"
            "manifest.stock_count,manifest.option_underlying_count "
            "ORDER BY manifest.session_date DESC,cohort.cohort_id",
            (list(target_cohort_ids),),
        )
    live_leases = _rows(
        cursor,
        "SELECT resource_key,owner_id::text,owner_epoch,lease_until,updated_at "
        "FROM rt.worker_leases WHERE lease_until>clock_timestamp() "
        "ORDER BY resource_key",
    )
    windows = _rows(
        cursor,
        "SELECT run.window_id::text,run.plan_id::text,"
        "run.cohort_id::text,service.session_date,"
        "service.stage_catalog_version,service.enabled_stages,"
        "run.provider_tier,run.window_key,run.release_at,"
        "run.cutoff,run.deadline,run.state,run.owner_id::text,"
        "run.owner_epoch,run.lease_until "
        "FROM rt.window_runs AS run "
        "JOIN rt.service_plans AS service USING(plan_id) "
        "WHERE run.state IN ('PENDING','RUNNING') "
        "ORDER BY service.session_date,run.release_at,run.window_id",
    )
    streams = _rows(
        cursor,
        "SELECT stream.session_id::text,stream.provider,stream.feed,"
        "stream.manifest_id::text,manifest.session_date,"
        "owner_id::text,owner_epoch,started_at,subscribed_at,ended_at,"
        "status,journal_generation::text,authorization_reference,"
        "connection_attempt,connection_ceiling,decoded_byte_ceiling,"
        "received_frame_count,received_byte_count,heartbeat_at "
        "FROM rt.stream_sessions AS stream "
        "JOIN rt.subscription_manifests AS manifest "
        "ON manifest.manifest_id=stream.manifest_id "
        "WHERE ended_at IS NULL OR status NOT IN ('STOPPED','FAILED') "
        "ORDER BY started_at,session_id",
    )
    return {
        "runtime_profiles": profiles,
        "advanced_plans": plans,
        "target9_cohorts": cohort_members,
        "live_worker_leases": live_leases,
        "conflicting_windows": windows,
        "nonterminal_stream_sessions": streams,
    }


def _publication_and_retention(cursor):
    portal = _rows(
        cursor,
        "SELECT cohort_id::text,provider_tier,release_id::text,cutoff,"
        "session_date,updated_at FROM rt.portal_release_current "
        "ORDER BY cohort_id,provider_tier",
    )
    release = _rows(
        cursor,
        "SELECT cohort_id::text,provider_tier,receipt_id::text,cutoff,"
        "updated_at FROM rt.release_current ORDER BY cohort_id,provider_tier",
    )
    pins = (
        _rows(
            cursor,
            "SELECT cohort_id::text,release_id::text,release_cutoff,"
            "package_sha256,enabled,enabled_at,disabled_at,updated_at "
            "FROM rt.advanced_options_read_pins ORDER BY cohort_id",
        )
        if _relation_exists(cursor, "rt.advanced_options_read_pins")
        else ()
    )
    policies = _rows(
        cursor,
        "SELECT relation_name,scheme,key_column,retention_class,"
        "window_kind,window_value,requires_archive,archive_kinds,"
        "catalog_relation FROM rt.partition_policies "
        "WHERE relation_name=ANY(%s) ORDER BY relation_name",
        ([
            "rt.bar_revisions",
            "rt.option_flow_events",
            "rt.option_flow_clusters",
            "rt.portal_releases",
            "rt.portal_snapshot_hot",
            "rt.portal_snapshot_kept",
        ],),
    )
    registry = {
        name: {
            "retention": RELATIONS[name].retention,
            "partitioned": RELATIONS[name].partitioned,
            "writes": RELATIONS[name].writes,
            "delete": RELATIONS[name].delete,
        }
        for name in (
            "rt.advanced_options_read_pins",
            "rt.advanced_options_equity_input_bindings",
            "rt.advanced_options_session_acquisition_authorizations",
            "rt.advanced_options_window_cycle_authorizations",
            "rt.advanced_options_authority_receipts",
            "rt.advanced_options_resident_completions",
        )
        if name in RELATIONS
    }
    return {
        "portal_release_current": portal,
        "release_current": release,
        "advanced_options_read_pins": pins,
        "partition_policies": policies,
        "v3_relation_registry": registry,
        "retention_independence": {
            "v3_authority_and_barrier_relations_partitioned": any(
                row["partitioned"] for row in registry.values()
            ),
            "v3_authority_and_barrier_relations": tuple(registry),
            "compose_profile_is_separate": True,
        },
    }


def _security_identities(cursor, known_at: datetime):
    return _rows(
        cursor,
        "SELECT DISTINCT ON (revision.ticker) "
        "revision.security_id::text,revision.revision_id::text,"
        "revision.ticker,revision.exchange,revision.calendar_id,"
        "revision.effective_from,revision.effective_to,revision.observed_at,"
        "revision.payload_sha256,revision.active,revision.free_float "
        "FROM ref.security_revisions AS revision "
        "WHERE revision.ticker=ANY(%s) "
        "AND revision.observed_at<=%s "
        "ORDER BY revision.ticker,revision.observed_at DESC,"
        "revision.effective_from DESC,revision.revision_id DESC",
        (list(OPTION_UNDERLYINGS), known_at),
    )


def _security_identity_groups(identity_rows):
    grouped = defaultdict(set)
    for row in identity_rows:
        ticker = str(row["ticker"])
        if ticker in OPTION_UNDERLYINGS:
            grouped[ticker].add(str(row["security_id"]))
    return grouped


def _history_coverage(cursor, sessions, known_at: datetime):
    rows = _rows(
        cursor,
        "SELECT current.cohort_id::text,revision.ticker,"
        "revision.security_id::text,revision.session_date,"
        "revision.bar_revision_id::text,revision.bar_end,"
        "revision.observed_at,revision.payload_sha256 "
        "FROM rt.bar_current AS current "
        "JOIN rt.bar_revisions AS revision "
        "ON revision.bar_revision_id=current.bar_revision_id "
        "AND revision.observed_at=current.observed_at "
        "WHERE current.interval='1d' "
        "AND current.adjustment_basis='ADJUSTED' "
        "AND revision.is_final "
        "AND revision.ticker=ANY(%s) "
        "AND revision.session_date=ANY(%s) "
        "AND revision.observed_at<=%s "
        "ORDER BY current.cohort_id,revision.ticker,revision.session_date",
        (list(OPTION_UNDERLYINGS), list(sessions), known_at),
    )
    grouped = defaultdict(list)
    for row in rows:
        grouped[(row["cohort_id"], row["ticker"])].append(row)
    cohorts = []
    for cohort_id in sorted({key[0] for key in grouped}):
        symbols = {}
        for ticker in OPTION_UNDERLYINGS:
            members = grouped.get((cohort_id, ticker), [])
            found = tuple(row["session_date"] for row in members)
            symbols[ticker] = {
                "session_count": len(found),
                "sessions": found,
                "missing_sessions": tuple(
                    session for session in sessions if session not in found
                ),
                "security_ids": tuple(sorted({
                    row["security_id"] for row in members
                })),
                "latest_observed_at": (
                    None
                    if not members
                    else max(row["observed_at"] for row in members)
                ),
                "member_sha256": canonical_sha256(tuple(
                    (
                        row["bar_revision_id"],
                        row["session_date"],
                        row["payload_sha256"],
                    )
                    for row in members
                )),
            }
        cohorts.append({
            "cohort_id": cohort_id,
            "complete": all(
                symbols[ticker]["sessions"] == sessions
                for ticker in OPTION_UNDERLYINGS
            ),
            "symbols": symbols,
        })
    return tuple(cohorts)


_CATALOG_SEMANTIC_FIELDS = (
    "contract_id",
    "contract_ticker",
    "underlying_security_id",
    "call_put",
    "expiration",
    "strike",
    "exercise_style",
    "multiplier",
    "eligibility",
    "payload_sha256",
)


def _coalesce_option_catalog_rows(rows):
    by_contract = defaultdict(list)
    for row in rows:
        by_contract[int(row["contract_id"])].append(row)
    selected = []
    for contract_id in sorted(by_contract):
        candidates = by_contract[contract_id]
        identities = {
            tuple(row[name] for name in _CATALOG_SEMANTIC_FIELDS)
            for row in candidates
        }
        if len(identities) == 1:
            selected.append(max(
                candidates,
                key=lambda row: (
                    row["effective_from"],
                    row["observed_at"],
                    int(row["version_id"]),
                ),
            ))
        else:
            selected.extend(candidates)
    return tuple(selected)


def _option_catalog(cursor, session_date: date, next_session: date, known_at: datetime):
    raw_rows = _rows(
        cursor,
        "SELECT contract.contract_id,contract.contract_ticker,"
        "contract.underlying_security_id::text,version.version_id,"
        "version.call_put,version.expiration,version.strike,"
        "version.exercise_style,version.multiplier,version.eligibility,"
        "version.effective_from,version.effective_to,version.observed_at,"
        "version.payload_sha256 "
        "FROM ref.option_contracts AS contract "
        "JOIN ref.option_contract_versions AS version "
        "ON version.contract_id=contract.contract_id "
        "WHERE contract.provider='massive' "
        "AND (contract.contract_ticker LIKE 'O:QQQ%%' "
        "OR contract.contract_ticker LIKE 'O:SPY%%') "
        "AND version.expiration=ANY(%s) "
        "AND version.effective_from<=%s "
        "AND (version.effective_to IS NULL OR version.effective_to>%s) "
        "AND version.observed_at<=%s "
        "ORDER BY version.expiration,contract.contract_ticker,version.version_id",
        ([session_date, next_session], known_at, known_at, known_at),
    )
    rows = _coalesce_option_catalog_rows(raw_rows)
    grouped = {}
    for expiration in (session_date, next_session):
        for ticker in OPTION_UNDERLYINGS:
            members = tuple(
                row for row in rows
                if row["expiration"] == expiration
                and row["contract_ticker"].startswith(f"O:{ticker}")
                and row["eligibility"] == "VALIDATED_ACTIVE"
            )
            grouped[f"{ticker}:{expiration.isoformat()}"] = {
                "contract_count": len(members),
                "call_count": sum(row["call_put"] == "C" for row in members),
                "put_count": sum(row["call_put"] == "P" for row in members),
                "underlying_security_ids": tuple(sorted({
                    row["underlying_security_id"] for row in members
                })),
                "catalog_sha256": canonical_sha256(tuple(
                    (
                        row["contract_id"],
                        row["contract_ticker"],
                        row["version_id"],
                        row["payload_sha256"],
                    )
                    for row in members
                )),
                "latest_observed_at": (
                    None
                    if not members
                    else max(row["observed_at"] for row in members)
                ),
            }
    contract_ids = [row["contract_id"] for row in rows]
    oi = ()
    if contract_ids:
        oi = _rows(
            cursor,
            "SELECT baseline.contract_id,baseline.baseline_id::text,"
            "baseline.settlement_session,baseline.open_interest,"
            "baseline.source,baseline.date_basis,baseline.observed_session,"
            "baseline.observed_at,baseline.received_at,"
            "baseline.payload_sha256,"
            "baseline.supersedes_baseline_id::text "
            "FROM ref.oi_baselines AS baseline "
            "WHERE baseline.contract_id=ANY(%s) "
            "AND baseline.observed_at<=%s AND baseline.received_at<=%s "
            "ORDER BY baseline.contract_id,baseline.received_at DESC,"
            "baseline.observed_at DESC,baseline.baseline_id",
            (contract_ids, known_at, known_at),
        )
    return grouped, rows, oi


def _reference_inputs(
    cursor,
    *,
    session_date: date,
    next_session: date,
    prior_session: date,
    known_at: datetime,
    identities,
):
    rates = _rows(
        cursor,
        "WITH eligible AS ("
        "SELECT * FROM ref.risk_free_rate_observations "
        "WHERE source='us_treasury_par_yield_v1' "
        "AND rate_date<=%s AND source_observed_at IS NOT NULL "
        "AND received_at<=%s"
        "), latest AS (SELECT max(rate_date) AS rate_date FROM eligible) "
        "SELECT observation_id::text,rate_date,tenor_days,annual_rate,"
        "source,source_key,source_observed_at,received_at,payload_sha256 "
        "FROM eligible JOIN latest USING(rate_date) "
        "ORDER BY tenor_days,received_at DESC,observation_id LIMIT 65",
        (session_date, known_at),
    )
    identity_by_ticker = defaultdict(list)
    for row in identities:
        identity_by_ticker[row["ticker"]].append(row)
    dividends = []
    splits = []
    floats = []
    short_interest = []
    for ticker in OPTION_UNDERLYINGS:
        security_ids = tuple({
            row["security_id"] for row in identity_by_ticker[ticker]
        })
        if len(security_ids) != 1:
            continue
        security_id = security_ids[0]
        dividends.extend(_rows(
            cursor,
            "SELECT coverage_id::text,security_id::text,action_type,"
            "window_start,window_end,source,source_key,"
            "response_action_count,response_sha256,observed_at,received_at,"
            "payload_sha256 FROM ref.corporate_action_coverage "
            "WHERE security_id=%s AND action_type='DIVIDEND' "
            "AND window_start<=%s AND window_end>=%s "
            "AND received_at<=%s ORDER BY received_at DESC,coverage_id",
            (security_id, session_date, next_session, known_at),
        ))
        splits.extend(_rows(
            cursor,
            "SELECT coverage_id::text,security_id::text,action_type,"
            "window_start,window_end,source,source_key,"
            "response_action_count,response_sha256,observed_at,received_at,"
            "payload_sha256 FROM ref.corporate_action_coverage "
            "WHERE security_id=%s AND action_type='SPLIT' "
            "AND window_start<=%s AND window_end>=%s "
            "AND received_at<=%s ORDER BY received_at DESC,coverage_id",
            (security_id, session_date, next_session, known_at),
        ))
        floats.extend(_rows(
            cursor,
            "SELECT revision_id::text,security_id::text,ticker,"
            "effective_from,effective_to,observed_at,payload_sha256,"
            "free_float FROM ref.security_revisions "
            "WHERE security_id=%s AND ticker=%s AND free_float IS NOT NULL "
            "AND effective_from<=%s AND observed_at<=%s "
            "AND (effective_to IS NULL OR effective_to>%s) "
            "ORDER BY effective_from DESC,observed_at DESC,revision_id LIMIT 2",
            (security_id, ticker, known_at, known_at, known_at),
        ))
        short_interest.extend(_rows(
            cursor,
            "SELECT report_id::text,security_id::text,availability_time,"
            "observed_at,payload_sha256,"
            "metrics->>'short_interest_percent' AS value "
            "FROM ref.fundamental_reports WHERE security_id=%s "
            "AND availability_time<=%s AND observed_at<=%s "
            "AND metrics ? 'short_interest_percent' "
            "ORDER BY availability_time DESC,observed_at DESC,report_id LIMIT 2",
            (security_id, known_at, known_at),
        ))
    return {
        "prior_oi_session": prior_session,
        "treasury_curve": rates,
        "dividend_coverage": tuple(dividends),
        "split_coverage": tuple(splits),
        "free_float": tuple(floats),
        "short_interest_percent": tuple(short_interest),
    }


def _gap_report(
    *,
    session_date,
    required_history_sessions,
    next_session,
    prior_session,
    observed_at,
    identity,
    security_identities,
    scope,
    history,
    catalog,
    catalog_rows,
    oi_rows,
    reference_inputs,
):
    buckets = {name: [] for name in (
        "AVAILABLE",
        "MISSING_STORED",
        "STALE",
        "AMBIGUOUS",
        "REQUIRES_PROVIDER",
    )}

    def add(status, fact, cause, evidence):
        buckets[status].append(_classify(
            status,
            fact,
            cause=cause,
            evidence=evidence,
        ))

    if (
        identity["metadata"]["database"] == EXPECTED_V2_DATABASE_NAME
        and identity["metadata"]["transaction_read_only"] == "on"
        and not identity["unknown_migrations"]
        and not identity["checksum_drift"]
    ):
        add("AVAILABLE", "production_database_identity", "database snapshot", {
            "database": identity["metadata"]["database"],
            "lineage": identity["database_identity"],
            "schema_sha256": identity["expected_schema_sha256"],
        })
    else:
        add("AMBIGUOUS", "production_database_identity", "database snapshot", identity)
    pending_v3 = tuple(
        version for version, state in identity["v3_migrations"].items()
        if state != "APPLIED"
    )
    add(
        "AVAILABLE" if not pending_v3 else "MISSING_STORED",
        "migrations_033_035",
        "migration ledger",
        {"states": identity["v3_migrations"], "pending": pending_v3},
    )
    security_failures = tuple(
        row for row in identity["security_audit"]
        if not row["verdict"].startswith("PASS")
    )
    add(
        "AVAILABLE"
        if identity["security_audit"] and not security_failures
        else "AMBIGUOUS",
        "database_security_identity",
        "read-only exact privilege audit",
        {
            "failures": security_failures,
            "audit_error": identity["security_audit_error"],
        },
    )
    active_target9 = tuple(
        row for row in scope["runtime_profiles"]
        if row["status"] == "ACTIVE"
        and row["universe_target"] == 9
        and row["feed"] == "STOCKS"
        and tuple(row["channels"]) == ("AM",)
    )
    add(
        _exactly_one_status(active_target9),
        "active_target9_stocks_am_profile",
        "runtime profile activation",
        active_target9,
    )
    exact_cohorts = tuple(
        row for row in scope["target9_cohorts"]
        if (
            len(row["symbols"]) == len(TARGET_SYMBOLS)
            and set(row["symbols"]) == set(TARGET_SYMBOLS)
        )
        and set(row["channels"]) == {"AM"}
    )
    add(
        "AVAILABLE" if exact_cohorts else "MISSING_STORED",
        "target9_equity_cohort",
        f"latest stored target-9 plan at {observed_at.isoformat()}",
        exact_cohorts,
    )
    conflicts, stale_records, retained_records = (
        _runtime_record_classification(
        scope,
        observed_at,
        session_date,
    ))
    add(
        "AVAILABLE"
        if not any(conflicts.values())
        else "AMBIGUOUS",
        "conflicting_owners_windows_services",
        "database clock",
        conflicts,
    )
    add(
        "STALE" if any(stale_records.values()) else "AVAILABLE",
        "expired_windows_ended_nonterminal_streams",
        "database clock",
        stale_records,
    )
    add(
        "AVAILABLE",
        "retained_historical_runtime_records",
        f"before session {session_date.isoformat()}",
        retained_records,
    )
    complete_history = tuple(row for row in history if row["complete"])
    add(
        "AVAILABLE" if complete_history else "REQUIRES_PROVIDER",
        "qqq_spy_adjusted_daily_h",
        f"known by {observed_at.isoformat()} before {session_date.isoformat()}",
        {
            "required_session_count": 31,
            "required_sessions": required_history_sessions,
            "complete_cohorts": complete_history,
            "candidate_cohorts": history,
        },
    )
    add(
        "AVAILABLE",
        "raw_h_not_required",
        "current V3 source identity",
        {
            "historical_raw_daily_sessions_required": 0,
            "s1_s2_required_by_options": False,
            "live_e_requirement": (
                "QQQ/SPY three finalized current-session 5m "
                "RAW_UNADJUSTED bars ending at E"
            ),
            "live_e_availability": "NOT_YET_CAUSALLY_OBSERVABLE_PRE_SESSION",
        },
    )
    identity_groups = _security_identity_groups(security_identities)
    for key, value in catalog.items():
        required = key.endswith(session_date.isoformat())
        count = value["contract_count"]
        status = (
            "AVAILABLE"
            if count or not required
            else "REQUIRES_PROVIDER"
        )
        add(
            status,
            f"option_catalog:{key}",
            f"catalog observed by {observed_at.isoformat()}",
            {
                **value,
                "required_by_current_canary": required,
            },
        )
    prior_oi = tuple(
        row for row in oi_rows
        if row["settlement_session"] == prior_session
        and row["observed_session"] == session_date
    )
    prior_oi_by_contract = defaultdict(list)
    for row in prior_oi:
        prior_oi_by_contract[int(row["contract_id"])].append(row)
    coalesced_prior_oi = tuple(
        terminal[0]
        for contract_id in sorted(prior_oi_by_contract)
        if len(terminal := _coalesce_terminal_open_interest(
            prior_oi_by_contract[contract_id],
            session_date,
        )) == 1
    )
    same_day_catalog = tuple(
        row for row in catalog_rows
        if row["expiration"] == session_date
        and row["eligibility"] == "VALIDATED_ACTIVE"
    )
    same_day_contract_ids = {
        row["contract_id"] for row in same_day_catalog
    }
    oi_contract_ids = {
        row["contract_id"] for row in coalesced_prior_oi
    }
    selected_catalog = tuple(
        row for row in same_day_catalog
        if row["contract_id"] in oi_contract_ids
    )
    selected_membership = {
        (
            "QQQ"
            if row["contract_ticker"].startswith("O:QQQ")
            else "SPY",
            row["call_put"],
        )
        for row in selected_catalog
    }
    expected_membership = {
        (ticker, call_put)
        for ticker in OPTION_UNDERLYINGS
        for call_put in ("C", "P")
    }
    add(
        "AVAILABLE"
        if (
            len(selected_catalog) == 4
            and selected_membership == expected_membership
        )
        else "REQUIRES_PROVIDER",
        "same_day_prior_open_interest",
        f"settlement={prior_session.isoformat()} observed_session={session_date.isoformat()}",
        {
            "catalog_contract_count": len(same_day_contract_ids),
            "prior_oi_row_count": len(prior_oi),
            "coalesced_prior_oi_count": len(coalesced_prior_oi),
            "selected_contract_count": len(selected_catalog),
            "selected_membership": tuple(sorted(selected_membership)),
            "required_selected_membership": tuple(
                sorted(expected_membership)
            ),
        },
    )
    rates = reference_inputs["treasury_curve"]
    latest_rate_received = (
        None if not rates else max(row["received_at"] for row in rates)
    )
    rate_age = (
        None
        if latest_rate_received is None
        else (observed_at - latest_rate_received).total_seconds()
    )
    rate_date = None if not rates else rates[0]["rate_date"]
    publication_causal = bool(rates) and all(
        row["rate_date"] == rate_date
        and row["source_observed_at"] <= row["received_at"] <= observed_at
        for row in rates
    )
    add(
        "AVAILABLE"
        if publication_causal and rate_date == prior_session
        else "STALE" if rates else "REQUIRES_PROVIDER",
        "treasury_curve",
        (
            "policy=treasury-prior-xnys-session-publication-v1 "
            f"expected_rate_date={prior_session.isoformat()} "
            f"at {observed_at.isoformat()}"
        ),
        {
            "member_count": len(rates),
            "rate_date": rate_date,
            "latest_received_at": latest_rate_received,
            "age_seconds": rate_age,
            "publication_causal": publication_causal,
        },
    )
    for ticker in OPTION_UNDERLYINGS:
        ids = identity_groups[ticker]
        add(
            "AVAILABLE" if len(ids) == 1 else "AMBIGUOUS",
            f"{ticker}_catalog_security_identity",
            f"effective and observed by {observed_at.isoformat()}",
            tuple(sorted(ids)),
        )
        dividend = tuple(
            row for row in reference_inputs["dividend_coverage"]
            if row["security_id"] in ids
        )
        if not dividend:
            dividend_status = "REQUIRES_PROVIDER"
        else:
            latest = max(row["received_at"] for row in dividend)
            dividend_status = (
                "AVAILABLE"
                if (observed_at - latest).total_seconds() < 86400
                else "STALE"
            )
        add(
            dividend_status,
            f"{ticker}_dividend_coverage",
            (
                f"window={session_date.isoformat()}.."
                f"{next_session.isoformat()}, refresh<24h"
            ),
            dividend,
        )
        split = tuple(
            row for row in reference_inputs["split_coverage"]
            if row["security_id"] in ids
        )
        add(
            "AVAILABLE" if split else "MISSING_STORED",
            f"{ticker}_split_corporate_action_coverage",
            f"window={session_date.isoformat()}..{next_session.isoformat()}",
            split,
        )
        floats = tuple(
            row for row in reference_inputs["free_float"]
            if row["security_id"] in ids
        )
        add(
            "AVAILABLE",
            f"{ticker}_free_float",
            "current accepted V3 O1/O3 detector policy",
            {
                "required_by_accepted_lanes": False,
                "stored_members": floats,
            },
        )
        reports = tuple(
            row for row in reference_inputs["short_interest_percent"]
            if row["security_id"] in ids
        )
        add(
            "AVAILABLE",
            f"{ticker}_short_interest_percent",
            "current accepted V3 O1/O3 detector policy",
            {
                "required_by_accepted_lanes": False,
                "stored_members": reports,
            },
        )
    add(
        "AVAILABLE",
        "slot_capacity_policy",
        "current source",
        {
            "selected_contracts_per_underlying": 2,
            "total_primary_contracts": 4,
            "channels_per_contract": ["T", "Q"],
            "provider_contract_cap": 1000,
            "reserved_contracts": 0,
        },
    )
    return {
        "contract": "advanced-options-v3-gap-report-v1",
        "session_date": session_date,
        "prior_session": prior_session,
        "next_session": next_session,
        "buckets": buckets,
        "counts": {
            name: len(rows) for name, rows in buckets.items()
        },
    }


def main(argv=None) -> int:
    args = parse_args(argv)
    calendar = OptionExchangeCalendar()
    if calendar.session_on_or_after(args.session_date) != args.session_date:
        raise ValueError("V3 preflight session must be an XNYS session")
    if (
        args.equity_cutoff_utc.date() != args.session_date
        or args.options_cutoff_utc.date() != args.session_date
        or args.equity_cutoff_utc > args.options_cutoff_utc
        or (
            args.options_cutoff_utc - args.equity_cutoff_utc
        ).total_seconds() > 300
    ):
        raise ValueError("V3 E/O cutoffs are invalid")
    output = args.output.resolve()
    if output.exists():
        raise FileExistsError("V3 operations report must be new")
    if not output.parent.is_dir():
        raise FileNotFoundError("V3 operations report parent is absent")
    sessions = calendar.trailing_sessions_before(args.session_date, 31)
    prior_session = calendar.previous_session_date(args.session_date)
    next_session = calendar.next_session_date(args.session_date)
    expected_migrations = load_realtime_migrations(V2_MIGRATIONS_DIR)
    source = _source_evidence()
    with psycopg2.connect(_dsn()) as connection:
        connection.set_session(
            readonly=True,
            autocommit=False,
            isolation_level="REPEATABLE READ",
        )
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SET LOCAL statement_timeout = '20s'")
            identity = _production_identity(cursor, expected_migrations)
            if (
                identity["metadata"]["database"]
                != EXPECTED_V2_DATABASE_NAME
                or identity["metadata"]["transaction_read_only"] != "on"
            ):
                raise RuntimeError("V3 operations audit database is not exact")
            scope = _production_scope(cursor, args.session_date)
            publication = _publication_and_retention(cursor)
            identities = _security_identities(cursor, args.observed_at_utc)
            history = _history_coverage(
                cursor,
                sessions,
                args.observed_at_utc,
            )
            catalog, catalog_rows, oi_rows = _option_catalog(
                cursor,
                args.session_date,
                next_session,
                args.observed_at_utc,
            )
            reference_identities = list(identities)
            known_identity_pairs = {
                (row["ticker"], row["security_id"])
                for row in reference_identities
            }
            for row in catalog_rows:
                ticker = (
                    "QQQ"
                    if row["contract_ticker"].startswith("O:QQQ")
                    else "SPY"
                )
                pair = (ticker, row["underlying_security_id"])
                if pair not in known_identity_pairs:
                    reference_identities.append({
                        "ticker": ticker,
                        "security_id": row["underlying_security_id"],
                    })
                    known_identity_pairs.add(pair)
            reference_inputs = _reference_inputs(
                cursor,
                session_date=args.session_date,
                next_session=next_session,
                prior_session=prior_session,
                known_at=args.observed_at_utc,
                identities=reference_identities,
            )
        connection.rollback()
    gaps = _gap_report(
        session_date=args.session_date,
        required_history_sessions=sessions,
        next_session=next_session,
        prior_session=prior_session,
        observed_at=args.observed_at_utc,
        identity=identity,
        security_identities=identities,
        scope=scope,
        history=history,
        catalog=catalog,
        catalog_rows=catalog_rows,
        oi_rows=oi_rows,
        reference_inputs=reference_inputs,
    )
    report = {
        "contract": "advanced-options-v3-operations-preflight-v1",
        "verdict": "BLOCKED" if any(
            gaps["buckets"][name]
            for name in (
                "MISSING_STORED",
                "STALE",
                "AMBIGUOUS",
                "REQUIRES_PROVIDER",
            )
        ) else "GO_PROVIDER_AUTHORITY_REQUIRED",
        "scope": {
            "session_date": args.session_date,
            "equity_cutoff_utc": args.equity_cutoff_utc,
            "options_cutoff_utc": args.options_cutoff_utc,
            "observed_at_utc": args.observed_at_utc,
            "database": EXPECTED_V2_DATABASE_NAME,
            "effects": {
                "provider_rest": 0,
                "provider_websocket": 0,
                "production_database_writes": 0,
                "production_migrations": 0,
                "production_service_actions": 0,
                "publication": 0,
                "stored_read_changes": 0,
                "retention_actions": 0,
                "delivery": 0,
                "trading": 0,
            },
        },
        "requirements": {
            "h": {
                "underlyings": OPTION_UNDERLYINGS,
                "adjusted_daily_session_count": 31,
                "adjusted_daily_sessions": sessions,
                "raw_daily_session_count": 0,
            },
            "e": {
                "underlyings": OPTION_UNDERLYINGS,
                "current_session_finalized_5m_raw_bars": 3,
                "equity_detectors_required": (),
                "equity_target": 9,
                "equity_symbols": TARGET_SYMBOLS,
            },
            "o": {
                "underlyings": OPTION_UNDERLYINGS,
                "required_expiration": args.session_date,
                "optional_next_session_inventory": next_session,
                "selected_contracts_per_underlying": 2,
                "channels": ("T", "Q"),
                "prior_oi_session": prior_session,
                "maximum_equity_age_seconds": 300,
                "treasury_freshness_policy": (
                    "treasury-prior-xnys-session-publication-v1"
                ),
                "treasury_expected_rate_date": prior_session,
            },
        },
        "source": source,
        "production_identity": identity,
        "production_scope": scope,
        "publication_and_retention": publication,
        "stored_data": {
            "security_identities": identities,
            "history_coverage": history,
            "option_catalog": catalog,
            "option_catalog_member_count": len(catalog_rows),
            "option_catalog_member_sha256": canonical_sha256(tuple(
                (
                    row["contract_id"],
                    row["contract_ticker"],
                    row["version_id"],
                    row["payload_sha256"],
                )
                for row in catalog_rows
            )),
            "open_interest_row_count": len(oi_rows),
            "open_interest_sha256": canonical_sha256(tuple(
                (
                    row["baseline_id"],
                    row["contract_id"],
                    row["settlement_session"],
                    row["observed_session"],
                    row["payload_sha256"],
                )
                for row in oi_rows
            )),
            "reference_inputs": reference_inputs,
        },
        "gap_report": gaps,
        "freeze_facts": _freeze_facts(gaps, source),
        "zero_effect_proof": {
            "transaction_read_only": (
                identity["metadata"]["transaction_read_only"]
            ),
            "transaction_isolation": (
                identity["metadata"]["transaction_isolation"]
            ),
            "transaction_disposition": "ROLLBACK",
            "provider_clients_constructed": 0,
            "provider_endpoints_opened": 0,
        },
    }
    report["semantic_sha256"] = canonical_sha256(report)
    encoded = (
        json.dumps(
            report,
            indent=2,
            sort_keys=True,
            default=_json_default,
        )
        + "\n"
    ).encode("utf-8")
    with output.open("xb") as stream:
        stream.write(encoded)
    print(json.dumps({
        "verdict": report["verdict"],
        "semantic_sha256": report["semantic_sha256"],
        "file_sha256": hashlib.sha256(encoded).hexdigest(),
        "gap_counts": gaps["counts"],
        "freeze_facts": report["freeze_facts"],
        "output": str(output),
        "effects": report["scope"]["effects"],
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
