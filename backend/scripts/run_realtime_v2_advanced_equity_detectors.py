#!/usr/bin/env python3
"""Run the provider-free Advanced Equity S1/S2 detector shadow."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import signal
import sys
import threading
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.calendar import OptionExchangeCalendar  # noqa: E402
from realtime.advanced_equity_detector_consumer import (  # noqa: E402
    claim_advanced_equity_detector_lease_cursor,
    persist_advanced_equity_detector_slice_cursor,
    plan_advanced_equity_detector_slice_cursor,
)
from realtime.advanced_equity_s1_s2_shadow import (  # noqa: E402
    DETECTORS,
    build_s1_s2_consumer_config,
    s1_s2_evaluator_cursor,
)
from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.equity_pipeline_config import (  # noqa: E402
    EquityPipeline,
    OptionsPipeline,
    RealtimeV2PipelineConfig,
    RealtimeV2Process,
)
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    validate_target_database_v2,
)


UTC = timezone.utc
CODE_PATHS = (
    BACKEND_DIR / "realtime" / "advanced_equity_detector_consumer.py",
    BACKEND_DIR / "realtime" / "advanced_equity_detector_shadow.py",
    BACKEND_DIR / "realtime" / "advanced_equity_s1_s2_shadow.py",
)


@dataclass(frozen=True, slots=True)
class AdvancedEquityDetectorScope:
    journal_generation: UUID
    shard_map_sha256: str
    instrument_id: int

    def __post_init__(self) -> None:
        if (
            len(self.shard_map_sha256) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.shard_map_sha256
            )
            or type(self.instrument_id) is not int
            or self.instrument_id <= 0
        ):
            raise ValueError("Advanced Equity detector scope is invalid")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--cohort-id", type=UUID, required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--maximum-records", type=int, default=100)
    parser.add_argument("--lease-seconds", type=int, default=60)
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    parser.add_argument("--continuous", action="store_true")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("Advanced Equity detectors target stocks_realtime_v2")
    if not 1 <= args.maximum_records <= 1_000:
        parser.error("--maximum-records must be in [1, 1000]")
    if not 30 <= args.lease_seconds <= 3_600:
        parser.error("--lease-seconds must be in [30, 3600]")
    if args.poll_seconds <= 0:
        parser.error("--poll-seconds must be positive")
    if not args.continuous or not args.execute:
        parser.error(
            "Advanced Equity detectors require --continuous --execute"
        )
    return args


def detector_code_sha256(paths=CODE_PATHS) -> str:
    digest = hashlib.sha256()
    for path in paths:
        resolved = Path(path).resolve()
        if not resolved.is_file():
            raise FileNotFoundError(
                "Advanced Equity detector source binding is unavailable"
            )
        digest.update(resolved.name.encode("ascii"))
        digest.update(b"\0")
        digest.update(resolved.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def load_advanced_equity_detector_scopes_cursor(
    cursor,
    *,
    cohort_id: UUID,
    session_date: date,
) -> tuple[AdvancedEquityDetectorScope, ...]:
    cursor.execute(
        "SELECT registry.journal_generation::text,"
        "registry.shard_map_sha256,instrument.instrument_id "
        "FROM rt.cohorts AS cohort "
        "JOIN rt.subscription_manifests AS manifest USING(manifest_id) "
        "JOIN rt.capability_profiles AS profile "
        "ON profile.profile_id=manifest.profile_id "
        "JOIN rt.subscription_members AS member "
        "ON member.manifest_id=manifest.manifest_id "
        "JOIN rt.instruments AS instrument "
        "ON instrument.instrument_id=member.instrument_id "
        "JOIN rt.advanced_ingress_plans AS plan "
        "ON plan.cohort_id=cohort.cohort_id "
        "AND plan.manifest_id=manifest.manifest_id "
        "JOIN rt.advanced_ingress_runtime_profiles AS runtime "
        "ON runtime.runtime_profile_sha256=plan.runtime_profile_sha256 "
        "JOIN rt.advanced_journal_registry AS registry "
        "ON registry.plan_id=plan.plan_id "
        "LEFT JOIN rt.cohort_history_bindings AS binding "
        "ON binding.target_cohort_id=cohort.cohort_id "
        "WHERE cohort.cohort_id=%s AND plan.session_date=%s "
        "AND profile.provider_tier='MASSIVE_ADVANCED' "
        "AND runtime.feed='STOCKS' "
        "AND runtime.channels=ARRAY['AM']::text[] "
        "AND member.channel='AM' "
        "AND instrument.asset_class='EQUITY' "
        "AND plan.status IN ('ACTIVE','COMPLETE') "
        "AND registry.status IN ('WRITING','DRAINING','DRAINED') "
        "AND (binding.target_cohort_id IS NULL "
        "OR registry.created_at>=binding.created_at) "
        "AND (binding.target_cohort_id IS NULL OR ("
        "SELECT count(DISTINCT history.session_date) "
        "FROM rt.bar_current AS history_current "
        "JOIN rt.bar_revisions AS history "
        "ON history.bar_revision_id=history_current.bar_revision_id "
        "AND history.observed_at=history_current.observed_at "
        "AND history.cohort_id=history_current.cohort_id "
        "AND history.provider_tier=history_current.provider_tier "
        "AND history.instrument_id=history_current.instrument_id "
        "AND history.interval=history_current.interval "
        "AND history.bar_start=history_current.bar_start "
        "AND history.session_scope=history_current.session_scope "
        "AND history.adjustment_basis=history_current.adjustment_basis "
        "WHERE history.cohort_id=binding.source_cohort_id "
        "AND history.provider_tier=binding.source_provider_tier "
        "AND history.security_id=instrument.security_id "
        "AND history.interval='1d' "
        "AND history.adjustment_basis='RAW_UNADJUSTED' "
        "AND history.availability_mode='HISTORICAL_RECONSTRUCTED' "
        "AND history.session_scope='RTH' AND history.is_final "
        "AND history.session_date<plan.session_date "
        "AND history.bar_end<binding.history_before "
        "AND history.observed_at<=registry.created_at "
        "AND history.created_at<=registry.created_at "
        "AND (history.replay_available_at IS NULL "
        "OR history.replay_available_at<=registry.created_at) "
        "AND (history.provider_published_at IS NULL "
        "OR history.provider_published_at<=registry.created_at)"
        ")>=15) "
        "ORDER BY registry.journal_generation,instrument.instrument_id",
        (str(cohort_id), session_date),
    )
    scopes = tuple(
        AdvancedEquityDetectorScope(
            journal_generation=UUID(str(row[0])),
            shard_map_sha256=str(row[1]),
            instrument_id=int(row[2]),
        )
        for row in cursor.fetchall()
    )
    if len(scopes) != len(set(scopes)):
        raise RuntimeError("Advanced Equity detector scope is ambiguous")
    return scopes


def detector_lease_resource_key(
    *,
    cohort_id: UUID,
    session_date: date,
    scope: AdvancedEquityDetectorScope,
    detector_id: str,
) -> str:
    if detector_id not in DETECTORS:
        raise ValueError("Advanced Equity detector must be S1 or S2")
    return (
        f"advanced-equity-detector:{cohort_id}:{session_date}:"
        f"{scope.journal_generation}:{scope.instrument_id}:{detector_id}"
    )


def run_detector_cycle(
    provider: V2ConnectionProvider,
    *,
    cohort_id: UUID,
    session_date: date,
    session_close: datetime,
    owner_id: UUID,
    code_sha256: str,
    maximum_records: int,
    lease_seconds: int,
    clock=lambda: datetime.now(UTC),
) -> dict[str, object]:
    checked_at = clock().astimezone(UTC)
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        scopes = load_advanced_equity_detector_scopes_cursor(
            unit.cursor,
            cohort_id=cohort_id,
            session_date=session_date,
        )
    records = 0
    active_consumers = 0
    for scope in scopes:
        for detector_id in DETECTORS:
            resource_key = detector_lease_resource_key(
                cohort_id=cohort_id,
                session_date=session_date,
                scope=scope,
                detector_id=detector_id,
            )
            with provider.transaction(
                APP_ROLE,
                isolation_level="SERIALIZABLE",
            ) as unit:
                owner_epoch = claim_advanced_equity_detector_lease_cursor(
                    unit.cursor,
                    resource_key=resource_key,
                    owner_id=owner_id,
                    checked_at=checked_at,
                    lease_seconds=lease_seconds,
                )
                config = build_s1_s2_consumer_config(
                    detector_id=detector_id,
                    cohort_id=cohort_id,
                    journal_generation=scope.journal_generation,
                    instrument_id=scope.instrument_id,
                    shard_map_sha256=scope.shard_map_sha256,
                    owner_id=owner_id,
                    owner_epoch=owner_epoch,
                    lease_resource_key=resource_key,
                    code_sha256=code_sha256,
                    readiness_epoch=1,
                )
                evaluator = s1_s2_evaluator_cursor(
                    unit.cursor,
                    detector_id=detector_id,
                    session_close=session_close,
                    decision_at=clock,
                )
                planned = plan_advanced_equity_detector_slice_cursor(
                    unit.cursor,
                    config,
                    evaluator=evaluator,
                    maximum_records=maximum_records,
                    clock=clock,
                )
                persist_advanced_equity_detector_slice_cursor(
                    unit.cursor,
                    planned,
                )
            active_consumers += 1
            records += len(planned.records)
    return {
        "status": "PROCESSED" if records else "IDLE",
        "checked_at_utc": checked_at.isoformat(),
        "cohort_id": str(cohort_id),
        "session_date": session_date.isoformat(),
        "journal_scopes": len(scopes),
        "active_consumers": active_consumers,
        "records": records,
        "provider_requests": 0,
        "alert_writes": 0,
        "outbox_writes": 0,
        "publication_writes": 0,
    }


def release_detector_leases(
    provider: V2ConnectionProvider,
    *,
    owner_id: UUID,
) -> int:
    with provider.transaction(APP_ROLE) as unit:
        unit.cursor.execute(
            "DELETE FROM rt.worker_leases WHERE owner_id=%s "
            "AND resource_key LIKE %s "
            "RETURNING resource_key",
            (str(owner_id), "advanced-equity-detector:%"),
        )
        return len(unit.cursor.fetchall())


def _provider(config):
    pool = ThreadedConnectionPool(
        1,
        4,
        dsn=config.app_dsn,
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


def main(argv=None) -> int:
    args = parse_args(argv)
    config = RealtimeV2PipelineConfig.from_mapping(
        os.environ,
        process=RealtimeV2Process.APP_WORKER,
    )
    if (
        config.equity_pipeline is not EquityPipeline.MASSIVE_ADVANCED
        or config.options_pipeline is not OptionsPipeline.DISABLED
    ):
        raise RuntimeError(
            "Advanced Equity detectors require Advanced equity "
            "and disabled options"
        )
    provider, pool = _provider(config)
    owner_id = uuid4()
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    session_close = OptionExchangeCalendar("XNYS").session_close(
        args.session_date
    )
    code_sha256 = detector_code_sha256()
    try:
        while not stop.is_set():
            result = run_detector_cycle(
                provider,
                cohort_id=args.cohort_id,
                session_date=args.session_date,
                session_close=session_close,
                owner_id=owner_id,
                code_sha256=code_sha256,
                maximum_records=args.maximum_records,
                lease_seconds=args.lease_seconds,
            )
            print(json.dumps(result, separators=(",", ":"), sort_keys=True))
            stop.wait(args.poll_seconds)
        return 0
    finally:
        release_detector_leases(provider, owner_id=owner_id)
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
