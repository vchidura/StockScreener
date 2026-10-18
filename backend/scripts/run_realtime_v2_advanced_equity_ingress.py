#!/usr/bin/env python3
"""Run one fenced Massive Advanced stocks AM session into a durable journal."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
import threading
import time
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID, uuid4

from psycopg2.pool import ThreadedConnectionPool
from websockets.exceptions import ConnectionClosed


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_equity_ingress import (  # noqa: E402
    AdvancedEquityJournalPump,
    AdvancedJournalDecodeError,
    BoundedAsyncProgressReporter,
)
from realtime.advanced_gateway import (  # noqa: E402
    AdvancedGateway,
    GatewayConfiguration,
    GatewayControlError,
    GatewayFeedState,
    GatewayOverload,
)
from realtime.advanced_stream_control import (  # noqa: E402
    AdvancedStreamScope,
    record_advanced_stream_gap_cursor,
    register_advanced_stream_session_cursor,
    update_advanced_stream_session_cursor,
)
from realtime.advanced_ingress_repository import (  # noqa: E402
    load_advanced_ingress_runtime_plan_cursor,
)
from realtime.advanced_ingress_controller import (  # noqa: E402
    classify_advanced_ingress_failure,
)
from realtime.advanced_gap_repair import (  # noqa: E402
    record_first_post_reconnect_event_cursor,
)
from realtime.advanced_ingress_runtime_profile import (  # noqa: E402
    load_advanced_ingress_runtime_catalog,
)
from realtime.advanced_equity_journal_bridge import (  # noqa: E402
    AdvancedEquityJournalBridgeConfig,
)
from realtime.advanced_journal_registry import (  # noqa: E402
    JournalCapacityError,
    build_advanced_journal_registration,
    mark_advanced_journal_writing_cursor,
    measure_journal_capacity,
    register_advanced_journal_cursor,
    seal_advanced_journal_cursor,
    update_advanced_journal_offsets_cursor,
)
from realtime.buffer import BufferLimits, RawMessageBuffer  # noqa: E402
from realtime.contracts import (  # noqa: E402
    AssetClass,
    MarketChannel,
    MarketFeed,
    SubscriptionManifest,
    SubscriptionMember,
    UniverseManifest,
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
from realtime.normalizer import (  # noqa: E402
    InstrumentAdmission,
    NORMALIZER_VERSION,
)
from realtime.journal import journal_files_sha256  # noqa: E402
from scripts.realtime_script_support import write_report  # noqa: E402


UTC = timezone.utc


def _now() -> datetime:
    return datetime.now(UTC)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--ingress-plan-id", type=UUID, required=True)
    parser.add_argument("--runtime-path-attempt", type=int, default=1)
    parser.add_argument("--debug-once", action="store_true")
    parser.add_argument("--maximum-market-messages", type=int)
    parser.add_argument("--duration-seconds", type=int)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("Advanced ingress targets only stocks_realtime_v2")
    if not 1 <= args.runtime_path_attempt <= 1_000:
        parser.error("--runtime-path-attempt must be in [1, 1000]")
    if args.debug_once:
        if (
            args.maximum_market_messages is None
            or not 1 <= args.maximum_market_messages <= 1_000
            or args.duration_seconds is None
            or not 1 <= args.duration_seconds <= 600
        ):
            parser.error(
                "--debug-once requires bounded market messages and duration"
            )
    elif (
        args.maximum_market_messages is not None
        or args.duration_seconds is not None
    ):
        parser.error(
            "routine mode does not accept debug-only message/duration bounds"
        )
    if not args.execute:
        parser.error("Advanced ingress requires --execute")
    return args


def _provider(config):
    if config.app_dsn is None:
        raise ValueError("Advanced ingress app DSN is unavailable")
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
            statement_timeout_ms=30_000,
        ),
    )
    provider.verify()
    return provider, pool


def _load_runtime_scope(
    provider,
    plan_id,
    runtime_catalog,
):
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        plan = load_advanced_ingress_runtime_plan_cursor(
            unit.cursor,
            plan_id,
        )
    runtime_profile = runtime_catalog.profile_for_sha(
        plan.runtime_profile_sha256
    )
    if (
        runtime_profile.version != plan.runtime_profile_version
        or runtime_profile.universe_target != plan.universe_target
        or runtime_profile.configuration_sha256(
            plan.runtime_policy_sha256
        )
        != plan.configuration_sha256
    ):
        raise ValueError("ingress plan runtime profile changed")
    with provider.transaction(APP_ROLE, read_only=True, dict_cursor=True) as unit:
        cursor = unit.cursor
        cursor.execute(
            "SELECT manifest.manifest_id,manifest.session_date,"
            "manifest.allocation_policy_sha256,manifest.stock_count,"
            "profile.profile_sha256,profile.provider_tier,"
            "cohort.configuration_sha256 "
            "FROM rt.cohorts AS cohort "
            "JOIN rt.subscription_manifests AS manifest USING(manifest_id) "
            "JOIN rt.capability_profiles AS profile USING(profile_id) "
            "WHERE cohort.cohort_id=%s AND manifest.manifest_id=%s",
            (str(plan.cohort_id), str(plan.manifest_id)),
        )
        scope = cursor.fetchone()
        if (
            scope is None
            or scope["provider_tier"] != "MASSIVE_ADVANCED"
            or scope["session_date"] != plan.session_date
            or int(scope["stock_count"]) != plan.universe_target
            or scope["configuration_sha256"]
            != plan.configuration_sha256
        ):
            raise ValueError("Advanced ingress cohort/profile is unavailable")
        cursor.execute(
            "SELECT DISTINCT ON (member.instrument_id) "
            "member.instrument_id,reference.security_id,"
            "reference.ticker "
            "FROM rt.subscription_members AS member "
            "JOIN rt.instruments AS instrument USING(instrument_id) "
            "JOIN ref.security_revisions AS reference USING(security_id) "
            "WHERE member.manifest_id=%s AND member.channel='AM' "
            "AND instrument.asset_class='EQUITY' AND reference.active "
            "AND reference.effective_from<=clock_timestamp() "
            "AND (reference.effective_to IS NULL "
            "OR reference.effective_to>clock_timestamp()) "
            "AND reference.observed_at<=clock_timestamp() "
            "ORDER BY member.instrument_id,reference.observed_at DESC,"
            "reference.revision_id DESC",
            (str(scope["manifest_id"]),),
        )
        retained = {}
        for row in cursor.fetchall():
            retained.setdefault(row["ticker"], row)
    actual_symbols = tuple(sorted(retained))
    if len(retained) != plan.universe_target:
        raise ValueError("Advanced ingress AM member count is incomplete")
    universe = UniverseManifest(
        version=f"advanced-equity-runtime:{scope['manifest_id']}",
        equity_tickers=actual_symbols,
        option_underlyings=(),
    )
    members = tuple(
        SubscriptionMember(
            AssetClass.EQUITY,
            ticker,
            ticker,
            MarketChannel.MINUTE_AGGREGATE,
        )
        for ticker in actual_symbols
    )
    manifest = SubscriptionManifest(
        version=f"advanced-equity-runtime:{scope['manifest_id']}",
        session_date=scope["session_date"],
        universe=universe,
        capability_profile_sha256=scope["profile_sha256"],
        allocation_policy_sha256=scope["allocation_policy_sha256"],
        members=members,
        maximum_option_quote_contracts=0,
    )
    admissions = {
        ticker: InstrumentAdmission(
            int(retained[ticker]["instrument_id"]),
            ticker,
            ticker,
            AssetClass.EQUITY,
            0,
        )
        for ticker in actual_symbols
    }
    return (
        plan,
        runtime_profile,
        manifest,
        admissions,
        actual_symbols,
    )


async def _run_once(gateway, messages, duration):
    return await asyncio.wait_for(
        gateway.run_session(maximum_market_messages=messages),
        timeout=duration,
    )


async def _run_continuous(
    gateway,
    stop,
    poll_interval_seconds,
    drain_deadline,
    utc_clock=_now,
):
    task = asyncio.create_task(gateway.run_session())
    try:
        while (
            not stop.is_set()
            and not task.done()
            and utc_clock() < drain_deadline
        ):
            await asyncio.sleep(poll_interval_seconds)
        if (
            (stop.is_set() or utc_clock() >= drain_deadline)
            and not task.done()
        ):
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
            return None
        return task.result()
    finally:
        if not task.done():
            task.cancel()


def _derived_runtime_paths(
    journal_root: Path,
    *,
    plan_id: UUID,
    session_date,
    connection_attempt: int,
    runtime_path_attempt: int = 1,
) -> tuple[Path, Path]:
    if not 1 <= runtime_path_attempt <= 1_000:
        raise ValueError("runtime path attempt must be in [1, 1000]")
    identity = (
        f"{session_date.isoformat()}-{plan_id}-"
        f"attempt-{connection_attempt:03d}"
    )
    if runtime_path_attempt > 1:
        identity += f"-path-{runtime_path_attempt:03d}"
    journal_path = journal_root / f"stocks-{identity}.sqlite"
    report_path = journal_root / f"stocks-{identity}.json"
    if journal_path.exists() or report_path.exists():
        raise RuntimeError(
            "derived ingress journal/report path already exists"
        )
    return journal_path, report_path


def main(argv=None) -> int:
    args = parse_args(argv)
    config = RealtimeV2PipelineConfig.from_mapping(
        os.environ,
        process=RealtimeV2Process.INGRESS,
    )
    if (
        config.equity_pipeline is not EquityPipeline.MASSIVE_ADVANCED
        or config.options_pipeline is not OptionsPipeline.DISABLED
    ):
        raise RuntimeError(
            "Advanced equity ingress requires Advanced equity and disabled options"
        )
    journal_root = config.advanced_journal_dir
    if journal_root is None or not journal_root.is_dir():
        raise RuntimeError(
            "Advanced journal directory is missing; validate/create it first"
        )
    runtime_catalog = load_advanced_ingress_runtime_catalog(
        config.advanced_ingress_policy_path,
        expected_version=config.advanced_ingress_policy_version,
    )
    provider, pool = _provider(config)
    stop = threading.Event()
    pump = None
    gateway = None
    session = None
    registration = None
    report_path = None
    started_at = None
    live_started_at = None
    status_reporter = None
    try:
        (
            runtime_plan,
            runtime_profile,
            manifest,
            admissions,
            runtime_symbols,
        ) = _load_runtime_scope(
            provider,
            args.ingress_plan_id,
            runtime_catalog,
        )
        attempt = runtime_plan.next_connection_attempt
        journal_path, report_path = _derived_runtime_paths(
            journal_root,
            plan_id=runtime_plan.plan_id,
            session_date=runtime_plan.session_date,
            connection_attempt=attempt,
            runtime_path_attempt=args.runtime_path_attempt,
        )
        generation = uuid4()
        owner_id = uuid4()
        started_at = _now()
        settings = runtime_profile.settings
        capacity = measure_journal_capacity(
            journal_root,
            minimum_free_bytes=settings.minimum_journal_free_bytes,
            minimum_free_percent=(
                settings.minimum_journal_free_percent
            ),
        )
        if not capacity.admitted:
            raise JournalCapacityError("JOURNAL_DISK_HEADROOM_LOW")
        scope = AdvancedStreamScope(
            ingress_plan_id=args.ingress_plan_id,
            cohort_id=runtime_plan.cohort_id,
            manifest_id=runtime_plan.manifest_id,
            journal_generation=generation,
            owner_id=owner_id,
            feed=MarketFeed.STOCKS,
            normalizer_version=NORMALIZER_VERSION,
            authorization_reference=(
                runtime_plan.authorization_reference
            ),
            connection_attempt=attempt,
            connection_ceiling=(
                runtime_plan.budgets.connection_attempts
            ),
            decoded_byte_ceiling=runtime_plan.budgets.decoded_bytes,
            stale_after_seconds=settings.stale_after_seconds,
            started_at=started_at,
        )
        with provider.transaction(APP_ROLE) as unit:
            session = register_advanced_stream_session_cursor(
                unit.cursor,
                scope,
            )
        bridge_config = AdvancedEquityJournalBridgeConfig(
            cohort_id=runtime_plan.cohort_id,
            journal_generation=generation,
            stream_session_id=session.session_id,
            owner_epoch=session.owner_epoch,
            normalizer_version=NORMALIZER_VERSION,
            shard_ids=(0,),
            batch_limit=settings.bridge_batch_limit,
        )
        registration = build_advanced_journal_registration(
            root=journal_root,
            path=journal_path,
            journal_generation=generation,
            plan_id=runtime_plan.plan_id,
            stream_session_id=session.session_id,
            cohort_id=runtime_plan.cohort_id,
            manifest_id=runtime_plan.manifest_id,
            session_date=runtime_plan.session_date,
            owner_epoch=session.owner_epoch,
            normalizer_version=NORMALIZER_VERSION,
            shard_ids=bridge_config.shard_ids,
            shard_map_sha256=bridge_config.shard_map_sha256,
        )
        with provider.transaction(APP_ROLE) as unit:
            register_advanced_journal_cursor(
                unit.cursor,
                registration,
            )
        buffer = RawMessageBuffer(
            BufferLimits(
                settings.buffer_event_limit,
                settings.buffer_byte_limit,
                settings.buffer_max_age_nanoseconds,
            ),
            monotonic_ns=time.monotonic_ns,
        )

        def usage():
            return pump.usage(
                received_frame_count=(
                    gateway.received_messages if gateway is not None else 0
                ),
                received_byte_count=(
                    gateway.received_bytes if gateway is not None else 0
                ),
            )

        def persist_state(state):
            nonlocal live_started_at
            checked_at = _now()
            if state is GatewayFeedState.LIVE and live_started_at is None:
                live_started_at = checked_at
            if status_reporter is not None and pump is not None:
                status_reporter.publish(pump.progress)

        configuration = GatewayConfiguration(
            MarketFeed.STOCKS,
            GatewayConfiguration.stocks().endpoint,
            open_timeout_seconds=(
                settings.websocket_open_timeout_seconds
            ),
            ping_interval_seconds=(
                settings.websocket_ping_interval_seconds
            ),
            ping_timeout_seconds=(
                settings.websocket_ping_timeout_seconds
            ),
            close_timeout_seconds=(
                settings.websocket_close_timeout_seconds
            ),
            maximum_message_bytes=settings.maximum_message_bytes,
            library_max_queue=settings.websocket_library_max_queue,
            maximum_control_messages=(
                settings.websocket_maximum_control_messages
            ),
        )
        gateway = AdvancedGateway(
            configuration,
            manifest,
            buffer,
            api_key=config.massive_api_key,
            monotonic_ns=time.monotonic_ns,
            session_id_factory=lambda: session.session_id,
            state_observer=persist_state,
        )

        def persist_progress(_progress):
            checked_at = _now()
            with provider.transaction(APP_ROLE) as unit:
                if gateway.state in {
                    GatewayFeedState.CONNECTING,
                    GatewayFeedState.AUTHENTICATING,
                    GatewayFeedState.SUBSCRIBING,
                    GatewayFeedState.WARMING,
                    GatewayFeedState.LIVE,
                }:
                    update_advanced_stream_session_cursor(
                        unit.cursor,
                        session,
                        usage(),
                        status=gateway.state.value,
                        checked_at=checked_at,
                        terminal=False,
                    )
                update_advanced_journal_offsets_cursor(
                    unit.cursor,
                    generation,
                    raw_committed_offset=_progress.last_raw_offset,
                    normalized_committed_offset=(
                        _progress.last_event_offset
                    ),
                    bridge_committed_offset=None,
                    checked_at=checked_at,
                )
                if (
                    _progress.last_event_offset > 0
                    and _progress.event_time_watermark is not None
                ):
                    record_first_post_reconnect_event_cursor(
                        unit.cursor,
                        plan_id=runtime_plan.plan_id,
                        reconnect_journal_generation=generation,
                        first_event_at=(
                            _progress.event_time_watermark
                        ),
                        first_offset=_progress.last_event_offset,
                    )

        pump = AdvancedEquityJournalPump(
            buffer,
            journal_path,
            generation,
            admissions,
            maximum_batch_messages=settings.journal_batch_messages,
            maximum_batch_bytes=settings.journal_batch_bytes,
            on_progress=None,
        )
        status_reporter = BoundedAsyncProgressReporter(persist_progress)
        pump.on_progress = status_reporter.publish
        with provider.transaction(APP_ROLE) as unit:
            mark_advanced_journal_writing_cursor(
                unit.cursor,
                generation,
                checked_at=_now(),
            )
        status_reporter.start()
        pump.start()
        signal.signal(signal.SIGINT, lambda *_: stop.set())
        signal.signal(signal.SIGTERM, lambda *_: stop.set())
        if args.debug_once:
            result = asyncio.run(
                _run_once(
                    gateway,
                    args.maximum_market_messages,
                    args.duration_seconds,
                )
            )
        else:
            result = asyncio.run(
                _run_continuous(
                    gateway,
                    stop,
                    settings.ingress_poll_interval_seconds,
                    runtime_plan.authorized_end_at
                    + timedelta(seconds=settings.drain_deadline_seconds),
                )
            )
        pump.request_stop()
        progress = pump.join(settings.drain_deadline_seconds)
        status_reporter.publish(progress)
        status_reporter.stop(
            max(
                settings.shutdown_deadline_seconds,
                settings.database_statement_timeout_ms / 1_000 + 1,
            )
        )
        with provider.transaction(APP_ROLE) as unit:
            seal_advanced_journal_cursor(
                unit.cursor,
                generation,
                raw_committed_offset=progress.last_raw_offset,
                normalized_committed_offset=progress.last_event_offset,
                sealed_files_sha256=journal_files_sha256(journal_path),
                checked_at=_now(),
            )
        final_usage = usage()
        with provider.transaction(APP_ROLE) as unit:
            update_advanced_stream_session_cursor(
                unit.cursor,
                session,
                final_usage,
                status="STOPPED",
                checked_at=_now(),
                terminal=True,
            )
        report = {
            "status": "STOPPED",
            "plan_id": str(runtime_plan.plan_id),
            "cohort_id": str(runtime_plan.cohort_id),
            "manifest_id": str(runtime_plan.manifest_id),
            "session_date": runtime_plan.session_date.isoformat(),
            "runtime_profile_sha256": (
                runtime_plan.runtime_profile_sha256
            ),
            "connection_attempt": attempt,
            "session_id": str(session.session_id),
            "journal_generation": str(generation),
            "owner_epoch": session.owner_epoch,
            "symbols": runtime_symbols,
            "channels": ("AM",),
            "received_frames": final_usage.received_frame_count,
            "received_bytes": final_usage.received_byte_count,
            "admitted_market_messages": progress.admitted_market_messages,
            "admitted_market_bytes": progress.admitted_market_bytes,
            "last_raw_offset": progress.last_raw_offset,
            "last_event_offset": progress.last_event_offset,
            "journal_status": "DRAINING",
            "status_update_failures": status_reporter.failure_count,
            "last_status_update_failure": (
                status_reporter.last_failure_name
            ),
            "event_time_watermark": (
                progress.event_time_watermark.isoformat()
                if progress.event_time_watermark is not None
                else None
            ),
            "connection_attempts": (
                result.connection_attempts if result is not None else 1
            ),
            "live_seconds": (
                max(0.0, (_now() - live_started_at).total_seconds())
                if live_started_at is not None
                else 0.0
            ),
            "provider_requests": 0,
            "credentials_exposed": False,
            "publication_advanced": False,
        }
        write_report(report_path, report)
        print(json.dumps(report, indent=2))
        return 0
    except (
        AdvancedJournalDecodeError,
        ConnectionClosed,
        GatewayControlError,
        GatewayOverload,
        OSError,
        TimeoutError,
        ValueError,
        JournalCapacityError,
    ) as error:
        if pump is not None:
            pump.request_stop()
            with suppress(Exception):
                pump.join(
                    runtime_profile.settings.shutdown_deadline_seconds
                )
            if registration is not None and journal_path.is_file():
                progress = pump.progress
                with suppress(Exception):
                    with provider.transaction(APP_ROLE) as unit:
                        seal_advanced_journal_cursor(
                            unit.cursor,
                            registration.journal_generation,
                            raw_committed_offset=(
                                progress.last_raw_offset
                            ),
                            normalized_committed_offset=(
                                progress.last_event_offset
                            ),
                            sealed_files_sha256=journal_files_sha256(
                                journal_path
                            ),
                            checked_at=_now(),
                        )
        if status_reporter is not None:
            with suppress(TimeoutError):
                status_reporter.stop(
                    max(
                        runtime_profile.settings.shutdown_deadline_seconds,
                        runtime_profile.settings.database_statement_timeout_ms
                        / 1_000
                        + 1,
                    )
                )
        if session is not None and gateway is not None and pump is not None:
            failure_code = (
                str(error)
                if isinstance(
                    error,
                    (
                        AdvancedJournalDecodeError,
                        GatewayControlError,
                        GatewayOverload,
                    ),
                )
                else type(error).__name__.upper()
            )[:64]
            final_usage = usage()
            with provider.transaction(APP_ROLE) as unit:
                record_advanced_stream_gap_cursor(
                    unit.cursor,
                    session,
                    gap_start=(
                        final_usage.event_time_watermark or started_at
                    ),
                    gap_end=None,
                    last_durable_offset=final_usage.last_raw_offset,
                    reason=failure_code,
                    last_durable_watermark=(
                        final_usage.event_time_watermark
                    ),
                )
                update_advanced_stream_session_cursor(
                    unit.cursor,
                    session,
                    final_usage,
                    status=(
                        "BLOCKED"
                        if isinstance(
                            error,
                            (
                                AdvancedJournalDecodeError,
                                GatewayControlError,
                                GatewayOverload,
                            ),
                        )
                        else "FAILED"
                    ),
                    checked_at=_now(),
                    failure_code=failure_code,
                    terminal=True,
                )
        failure = {
            "status": "FAILED",
            "failure_code": type(error).__name__,
            "failure_detail": str(error)[:160],
            "live_seconds": (
                max(0.0, (_now() - live_started_at).total_seconds())
                if live_started_at is not None
                else 0.0
            ),
            "credentials_exposed": False,
            "publication_advanced": False,
            "status_update_failures": (
                status_reporter.failure_count
                if status_reporter is not None
                else 0
            ),
            "last_status_update_failure": (
                status_reporter.last_failure_name
                if status_reporter is not None
                else None
            ),
        }
        classification = classify_advanced_ingress_failure(
            failure["failure_code"],
            detail=failure["failure_detail"],
            live_seconds=failure["live_seconds"],
        )
        failure["failure_disposition"] = classification.disposition.value
        failure["terminal_outcome"] = classification.terminal_outcome
        if report_path is not None:
            write_report(report_path, failure)
        print(json.dumps(failure, indent=2))
        return 2
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
