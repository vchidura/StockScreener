#!/usr/bin/env python3
"""Reconcile durable desired-state Options preparation and session rollover."""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from dotenv import dotenv_values
from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_equity_lifecycle import (  # noqa: E402
    AdvancedEquityLifecyclePlanner,
)
from realtime.advanced_realtime_v3_runtime import (  # noqa: E402
    ADVANCED_REALTIME_V3_COMPLETION_BUDGET_SECONDS,
    ADVANCED_REALTIME_V3_DRAIN_SECONDS,
    ADVANCED_REALTIME_V3_FINALITY_DELAY_SECONDS,
)
from realtime.advanced_options_lifecycle import (  # noqa: E402
    reconcile_advanced_options_lifecycle_cursor,
    run_advanced_options_preparation_cycle,
)
from realtime.advanced_options_automatic_preparation import (  # noqa: E402
    PostgresAutomaticOptionsPreparationExecutor,
)
from realtime.advanced_options_activity_momentum_c6 import (  # noqa: E402
    ACTIVITY_MOMENTUM_PROFILE_VERSION,
    LEGACY_ACTIVITY_MOMENTUM_PROFILE_VERSION,
)
from realtime.contracts import canonical_sha256  # noqa: E402
from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.equity_pipeline_config import (  # noqa: E402
    RealtimeV2PipelineConfig,
    RealtimeV2Process,
)
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    validate_target_database_v2,
)
from realtime.treasury_refresh import TreasuryRefreshOwner  # noqa: E402


UTC = timezone.utc
DEFAULT_CONNECTION_ATTEMPT_CEILING = 12
DEFAULT_DECODED_MESSAGE_CEILING = 1_000_000
DEFAULT_DECODED_BYTE_CEILING = 1_073_741_824
GENERALIZED_PROVIDER_REQUESTS_PER_UNDERLYING = 32
EXACT_FOUR_PROVIDER_REQUESTS_PER_UNDERLYING = 4


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    parser.add_argument(
        "--drain-seconds",
        type=int,
        default=ADVANCED_REALTIME_V3_DRAIN_SECONDS,
    )
    parser.add_argument(
        "--finality-delay-seconds",
        type=int,
        default=ADVANCED_REALTIME_V3_FINALITY_DELAY_SECONDS,
    )
    parser.add_argument(
        "--completion-budget-seconds",
        type=int,
        default=ADVANCED_REALTIME_V3_COMPLETION_BUDGET_SECONDS,
    )
    parser.add_argument("--continuous", action="store_true")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    if (
        args.database != EXPECTED_V2_DATABASE_NAME
        or args.poll_seconds <= 0
        or args.drain_seconds < 0
        or not 1 <= args.finality_delay_seconds <= 60
        or not 1 <= args.completion_budget_seconds <= 300
        or not args.continuous
        or not args.execute
    ):
        parser.error("continuous Options lifecycle scope is invalid")
    return args


def _provider():
    config = RealtimeV2PipelineConfig.from_mapping({
        **{
            key: value
            for key, value in dotenv_values(
                BACKEND_DIR / ".env"
            ).items()
            if value is not None
        },
        **os.environ,
    },
        process=RealtimeV2Process.APP_WORKER,
    )
    pool = ThreadedConnectionPool(
        1,
        2,
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


def _status_payload(status, lifecycle):
    return {
        "contract": "advanced-options-v3-lifecycle-status-v1",
        "status": (
            "READY_WAITING_FOR_DESIRED"
            if status is None
            else status.status
        ),
        "phase": lifecycle.phase.value,
        "current_processing_session": (
            lifecycle.processing_session_date.isoformat()
        ),
        "future_preparation_session": (
            lifecycle.preparation_session_date.isoformat()
        ),
        "revision_id": (
            None if status is None else str(status.revision_id)
        ),
        "desired_cohort_id": (
            None
            if status is None or status.desired_cohort_id is None
            else str(status.desired_cohort_id)
        ),
        "desired_snapshot_sha256": (
            None
            if status is None
            else status.desired_snapshot_sha256
        ),
        "option_underlyings": (
            [] if status is None else list(status.option_underlyings)
        ),
        "current_stage": (
            None if status is None else status.current_stage
        ),
        "session_binding_id": (
            None
            if status is None or status.session_binding_id is None
            else str(status.session_binding_id)
        ),
        "reason_codes": (
            [] if status is None else list(status.reason_codes)
        ),
        "duplicate_equity_preparation_jobs": 0,
        "provider_requests_this_cycle": 0,
        "publication_effects": 0,
        "delivery_effects": 0,
        "broker_effects": 0,
        "trading_effects": 0,
    }


def _positive_environment_int(
    environment,
    name: str,
    default: int,
) -> int:
    raw = str(environment.get(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError as error:
        raise RuntimeError(f"{name} must be a positive integer") from error
    if value <= 0:
        raise RuntimeError(f"{name} must be a positive integer")
    return value


def _optional_positive_environment_int(
    environment,
    name: str,
) -> int | None:
    raw = str(environment.get(name) or "").strip()
    if not raw:
        return None
    return _positive_environment_int(environment, name, 1)


def _exact_four_compatibility(environment) -> bool:
    return (
        str(environment.get(
            "REALTIME_V3_OPTIONS_EXACT_FOUR_COMPATIBILITY",
            "",
        )).strip().upper()
        == "ENABLED"
    )


def _maximum_provider_requests_per_underlying(environment) -> int:
    return _positive_environment_int(
        environment,
        "REALTIME_V3_OPTIONS_MAXIMUM_PROVIDER_REQUESTS_PER_UNDERLYING",
        (
            EXACT_FOUR_PROVIDER_REQUESTS_PER_UNDERLYING
            if _exact_four_compatibility(environment)
            else GENERALIZED_PROVIDER_REQUESTS_PER_UNDERLYING
        ),
    )


def main(argv=None) -> int:
    args = parse_args(argv)
    provider, pool = _provider()
    file_environment = {
        key: value
        for key, value in dotenv_values(BACKEND_DIR / ".env").items()
        if value is not None
    }
    environment = {**file_environment, **os.environ}
    api_key = str(environment.get("POLYGON_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError(
            "continuous Options preparation requires POLYGON_API_KEY"
        )
    configured_image = str(
        environment.get("REALTIME_V2_BACKEND_IMAGE") or ""
    ).strip()
    if "@sha256:" in configured_image:
        backend_image_id = "sha256:" + configured_image.rsplit(
            "@sha256:",
            1,
        )[1]
    elif configured_image.startswith("sha256:"):
        backend_image_id = configured_image
    else:
        backend_image_id = "sha256:" + canonical_sha256({
            "script": str(Path(__file__).resolve()),
            "runtime": "advanced-realtime-v3",
        })
    executor = PostgresAutomaticOptionsPreparationExecutor(
        provider,
        api_key=api_key,
        backend_image_id=backend_image_id,
        maximum_provider_requests_per_underlying=(
            _maximum_provider_requests_per_underlying(environment)
        ),
        admission_profile_version=(
            LEGACY_ACTIVITY_MOMENTUM_PROFILE_VERSION
            if _exact_four_compatibility(environment)
            else str(environment.get(
                "REALTIME_V3_OPTIONS_ADMISSION_PROFILE",
                ACTIVITY_MOMENTUM_PROFILE_VERSION,
            )).strip()
        ),
        exact_four_compatibility=_exact_four_compatibility(environment),
        active_quote_contract_budget=(
            _optional_positive_environment_int(
                environment,
                "REALTIME_V3_OPTIONS_ACTIVE_QUOTE_CONTRACT_BUDGET",
            )
        ),
        per_underlying_contract_limit=(
            _optional_positive_environment_int(
                environment,
                "REALTIME_V3_OPTIONS_PER_UNDERLYING_CONTRACT_LIMIT",
            )
        ),
        measured_capacity_limit=(
            _optional_positive_environment_int(
                environment,
                "REALTIME_V3_OPTIONS_ESTABLISHED_MEASURED_CAPACITY",
            )
        ),
        effective_provider_project_ceiling=(
            _optional_positive_environment_int(
                environment,
                "REALTIME_V3_OPTIONS_EFFECTIVE_PROVIDER_PROJECT_CEILING",
            )
        ),
        connection_attempt_ceiling=_positive_environment_int(
            environment,
            "REALTIME_V3_OPTIONS_CONNECTION_ATTEMPT_CEILING",
            DEFAULT_CONNECTION_ATTEMPT_CEILING,
        ),
        decoded_message_ceiling=_positive_environment_int(
            environment,
            "REALTIME_V3_OPTIONS_DECODED_MESSAGE_CEILING",
            DEFAULT_DECODED_MESSAGE_CEILING,
        ),
        decoded_byte_ceiling=_positive_environment_int(
            environment,
            "REALTIME_V3_OPTIONS_DECODED_BYTE_CEILING",
            DEFAULT_DECODED_BYTE_CEILING,
        ),
    )
    owner_id = uuid4()
    treasury = TreasuryRefreshOwner(provider, owner_id=owner_id)
    planner = AdvancedEquityLifecyclePlanner()
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    try:
        while not stop.is_set():
            lifecycle = planner.plan(
                drain_seconds=args.drain_seconds,
                finality_delay_seconds=args.finality_delay_seconds,
                completion_budget_seconds=(
                    args.completion_budget_seconds
                ),
            )
            treasury_status = treasury.tick(lifecycle.preparation_session_date)
            with provider.transaction(
                APP_ROLE,
                isolation_level="SERIALIZABLE",
            ) as unit:
                status = reconcile_advanced_options_lifecycle_cursor(
                    unit.cursor,
                    target_session_date=(
                        lifecycle.preparation_session_date
                    ),
                    checked_at=datetime.now(UTC),
                )
            cycle = run_advanced_options_preparation_cycle(
                provider,
                executor,
                owner_id=owner_id,
                checked_at=datetime.now(UTC),
            )
            with provider.transaction(
                APP_ROLE,
                isolation_level="SERIALIZABLE",
            ) as unit:
                status = reconcile_advanced_options_lifecycle_cursor(
                    unit.cursor,
                    target_session_date=(
                        lifecycle.preparation_session_date
                    ),
                    checked_at=datetime.now(UTC),
                )
            payload = _status_payload(status, lifecycle)
            payload["stage_cycle"] = cycle
            payload["treasury_refresh"] = treasury_status
            payload["provider_requests_this_cycle"] = (
                cycle.get("provider_requests", 0)
                + treasury_status["provider_requests_this_cycle"]
            )
            print(
                json.dumps(
                    payload,
                    sort_keys=True,
                ),
                flush=True,
            )
            stop.wait(args.poll_seconds)
        return 0
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
