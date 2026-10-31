#!/usr/bin/env python3
"""Capture and journal the remaining local V3 market segment without production effects."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from dotenv import dotenv_values


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.data.polygon_http import RequestsPolygonHttpTransport  # noqa: E402
from realtime.advanced_equity_ingress import (  # noqa: E402
    AdvancedEquityJournalPump,
)
from realtime.advanced_gateway import (  # noqa: E402
    AdvancedGateway,
    GatewayConfiguration,
)
from realtime.advanced_options_ingress import (  # noqa: E402
    AdvancedOptionsJournalPump,
)
from realtime.advanced_options_p8_selection_source import (  # noqa: E402
    MassiveAdvancedOptionsExactContractSource,
)
from realtime.advanced_realtime_session_scheduler import (  # noqa: E402
    ADVANCED_SESSION_EQUITY_SYMBOLS,
)
from realtime.buffer import BufferLimits, RawMessageBuffer  # noqa: E402
from realtime.contracts import (  # noqa: E402
    AssetClass,
    MarketChannel,
    MarketFeed,
    SubscriptionManifest,
    SubscriptionMember,
    UniverseManifest,
    canonical_sha256,
)
from realtime.normalizer import InstrumentAdmission  # noqa: E402


UTC = timezone.utc
INTERVAL = timedelta(minutes=5)
EQUITY_BYTES = 64 * 1024 * 1024
OPTIONS_BYTES = 32 * 1024 * 1024
REST_BYTES = 1_000_000


def _write(path, value, *, replace=False):
    encoded = (
        json.dumps(value, indent=2, sort_keys=True, default=str) + "\n"
    ).encode("utf-8")
    if replace:
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_bytes(encoded)
        os.replace(temporary, path)
    else:
        with path.open("xb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())


def _next_boundary(now):
    minute = now.astimezone(UTC).replace(second=0, microsecond=0)
    remainder = minute.minute % 5
    return minute + timedelta(
        minutes=5 if remainder == 0 else 5 - remainder
    )


def _boundaries(now, capture_stop):
    first = _next_boundary(now)
    result = []
    current = first
    while current <= capture_stop:
        result.append(current)
        current += INTERVAL
    return tuple(result)


def _manifest(session_date, contracts, *, options):
    universe = UniverseManifest(
        "advanced-realtime-v3-host-segment-v1",
        tuple(ADVANCED_SESSION_EQUITY_SYMBOLS),
        ("QQQ", "SPY") if options else (),
    )
    members = (
        tuple(
            SubscriptionMember(
                AssetClass.OPTION,
                ticker,
                ticker[2:5],
                channel,
                catalog_version_id=index,
            )
            for index, ticker in enumerate(contracts, start=1)
            for channel in (MarketChannel.TRADE, MarketChannel.QUOTE)
        )
        if options
        else tuple(
            SubscriptionMember(
                AssetClass.EQUITY,
                ticker,
                ticker,
                MarketChannel.MINUTE_AGGREGATE,
            )
            for ticker in ADVANCED_SESSION_EQUITY_SYMBOLS
        )
    )
    return SubscriptionManifest(
        version="advanced-realtime-v3-host-segment-v1",
        session_date=session_date,
        universe=universe,
        capability_profile_sha256="a" * 64,
        allocation_policy_sha256="b" * 64,
        members=members,
        maximum_option_quote_contracts=4 if options else 0,
    )


def _admissions(contracts, *, options):
    symbols = contracts if options else tuple(ADVANCED_SESSION_EQUITY_SYMBOLS)
    asset = AssetClass.OPTION if options else AssetClass.EQUITY
    return {
        symbol: InstrumentAdmission(
            instrument_id=index,
            provider_symbol=symbol,
            underlying=symbol[2:5] if options else symbol,
            asset_class=asset,
            shard_id=0,
        )
        for index, symbol in enumerate(symbols, start=1)
    }


def _gateway_evidence(gateway, pump):
    progress = pump.progress
    return {
        "subscriptions": list(gateway.subscriptions),
        "state": gateway.state.value,
        "received_messages": gateway.received_messages,
        "received_bytes": gateway.received_bytes,
        "transitions": [row.value for row in gateway.transitions],
        "last_raw_offset": progress.last_raw_offset,
        "last_event_offset": progress.last_event_offset,
        "admitted_market_messages": progress.admitted_market_messages,
        "admitted_market_bytes": progress.admitted_market_bytes,
    }


def _validate_options_byte_budgets(
    buffer_maximum_bytes,
    session_maximum_received_bytes,
):
    if (
        type(buffer_maximum_bytes) is not int
        or buffer_maximum_bytes != OPTIONS_BYTES
        or type(session_maximum_received_bytes) is not int
        or session_maximum_received_bytes <= buffer_maximum_bytes
    ):
        raise ValueError(
            "Options byte budgets require the bounded buffer and a larger "
            "explicit session ceiling"
        )


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--contract-ticker", action="append", required=True)
    parser.add_argument("--capture-stop", type=datetime.fromisoformat, required=True)
    parser.add_argument("--drain-stop", type=datetime.fromisoformat, required=True)
    parser.add_argument("--equity-maximum-bytes", type=int, required=True)
    parser.add_argument("--options-maximum-bytes", type=int, required=True)
    parser.add_argument(
        "--options-session-maximum-received-bytes",
        type=int,
        required=True,
    )
    parser.add_argument("--rest-maximum-response-bytes", type=int, required=True)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    args.output_directory = args.output_directory.resolve()
    args.contracts = tuple(sorted(set(args.contract_ticker)))
    args.capture_stop = args.capture_stop.astimezone(UTC)
    args.drain_stop = args.drain_stop.astimezone(UTC)
    now = datetime.now(UTC)
    args.boundaries = _boundaries(now, args.capture_stop)
    try:
        _validate_options_byte_budgets(
            args.options_maximum_bytes,
            args.options_session_maximum_received_bytes,
        )
    except ValueError as error:
        parser.error(str(error))
    if (
        len(args.contracts) != 4
        or not args.boundaries
        or args.capture_stop.date() != args.session_date
        or args.drain_stop != args.capture_stop + timedelta(minutes=5)
        or args.equity_maximum_bytes != EQUITY_BYTES
        or args.options_maximum_bytes != OPTIONS_BYTES
        or args.rest_maximum_response_bytes != REST_BYTES
        or args.output_directory.exists()
        or not args.output_directory.parent.is_dir()
        or not args.authorization_reference.strip()
        or not args.execute
    ):
        parser.error("host segment scope differs from its exact grant")
    return args


async def _run_gateway(gateway, byte_limit):
    try:
        await gateway.run_session(
            maximum_received_bytes=byte_limit,
        )
    except asyncio.CancelledError:
        return


async def execute_async(args):
    values = dotenv_values(BACKEND_DIR / ".env")
    api_key = str(
        os.getenv("POLYGON_API_KEY")
        or values.get("POLYGON_API_KEY")
        or ""
    ).strip()
    if not api_key:
        raise RuntimeError("host market segment requires POLYGON_API_KEY")
    output = args.output_directory
    output.mkdir()
    cycles_dir = output / "cycles"
    cycles_dir.mkdir()
    equity_buffer = RawMessageBuffer(
        BufferLimits(20_000, EQUITY_BYTES, 60_000_000_000),
        monotonic_ns=time.monotonic_ns,
    )
    options_buffer = RawMessageBuffer(
        BufferLimits(20_000, OPTIONS_BYTES, 60_000_000_000),
        monotonic_ns=time.monotonic_ns,
    )
    equity_generation = uuid5(
        NAMESPACE_URL,
        f"{args.authorization_reference}:equity",
    )
    options_generation = uuid5(
        NAMESPACE_URL,
        f"{args.authorization_reference}:options",
    )
    equity_pump = AdvancedEquityJournalPump(
        equity_buffer,
        output / "equity.sqlite",
        equity_generation,
        _admissions(args.contracts, options=False),
        maximum_batch_messages=500,
        maximum_batch_bytes=EQUITY_BYTES,
    )
    options_pump = AdvancedOptionsJournalPump(
        options_buffer,
        output / "options.sqlite",
        options_generation,
        _admissions(args.contracts, options=True),
        maximum_batch_messages=500,
        maximum_batch_bytes=OPTIONS_BYTES,
    )
    equity_gateway = AdvancedGateway(
        GatewayConfiguration.stocks(),
        _manifest(args.session_date, args.contracts, options=False),
        equity_buffer,
        api_key=api_key,
        monotonic_ns=time.monotonic_ns,
    )
    options_gateway = AdvancedGateway(
        GatewayConfiguration.options(),
        _manifest(args.session_date, args.contracts, options=True),
        options_buffer,
        api_key=api_key,
        monotonic_ns=time.monotonic_ns,
    )
    rest_source = MassiveAdvancedOptionsExactContractSource(
        RequestsPolygonHttpTransport(
            api_key,
            timeout_seconds=30,
            maximum_response_bytes=REST_BYTES,
        ),
        clock=lambda: datetime.now(UTC),
        maximum_decoded_bytes=REST_BYTES,
    )
    started = datetime.now(UTC)
    state_path = output / "state.json"
    cycles = []
    equity_pump.start()
    options_pump.start()
    gateway_tasks = [
        asyncio.create_task(_run_gateway(equity_gateway, EQUITY_BYTES)),
        asyncio.create_task(_run_gateway(
            options_gateway,
            args.options_session_maximum_received_bytes,
        )),
    ]
    try:
        for index, boundary in enumerate(args.boundaries, start=1):
            wait_seconds = (boundary - datetime.now(UTC)).total_seconds()
            if wait_seconds > 0:
                await asyncio.sleep(wait_seconds)
            capture = await asyncio.to_thread(
                rest_source.capture,
                contract_tickers=args.contracts,
                reviewed_at=boundary,
                session_date=args.session_date,
            )
            cycle = {
                "schema": "advanced-realtime-v3-host-segment-cycle-v1",
                "index": index,
                "boundary_utc": boundary.isoformat(),
                "completed_at_utc": datetime.now(UTC).isoformat(),
                "request_count": capture.request_count,
                "decoded_bytes": capture.decoded_bytes,
                "evidence": dict(capture.evidence),
                "contracts": list(capture.selection.contracts),
                "selection_sha256": capture.selection.selection_sha256,
                "selected_snapshots": dict(capture.selected_snapshots),
            }
            cycle["semantic_sha256"] = canonical_sha256(cycle)
            path = cycles_dir / f"cycle-{index:03d}.json"
            _write(path, cycle)
            cycles.append({
                "index": index,
                "boundary_utc": boundary.isoformat(),
                "file": str(path),
                "file_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "semantic_sha256": cycle["semantic_sha256"],
                "decoded_bytes": capture.decoded_bytes,
            })
            _write(
                state_path,
                {
                    "schema": "advanced-realtime-v3-host-segment-state-v1",
                    "status": "RUNNING",
                    "updated_at_utc": datetime.now(UTC).isoformat(),
                    "expected_cycles": len(args.boundaries),
                    "completed_cycles": len(cycles),
                    "last_boundary_utc": boundary.isoformat(),
                    "equity": _gateway_evidence(
                        equity_gateway,
                        equity_pump,
                    ),
                    "options": _gateway_evidence(
                        options_gateway,
                        options_pump,
                    ) | {
                        "buffer_maximum_bytes": args.options_maximum_bytes,
                        "session_maximum_received_bytes": (
                            args.options_session_maximum_received_bytes
                        ),
                    },
                },
                replace=state_path.exists(),
            )
        remaining = (args.capture_stop - datetime.now(UTC)).total_seconds()
        if remaining > 0:
            await asyncio.sleep(remaining)
    finally:
        for task in gateway_tasks:
            task.cancel()
        await asyncio.gather(*gateway_tasks, return_exceptions=True)
        drain_deadline = time.monotonic() + max(
            1,
            (args.drain_stop - datetime.now(UTC)).total_seconds(),
        )
        while (
            equity_buffer.snapshot().queued_events
            or options_buffer.snapshot().queued_events
        ) and time.monotonic() < drain_deadline:
            await asyncio.sleep(0.05)
        equity_pump.request_stop()
        options_pump.request_stop()
        equity_progress = equity_pump.join(30)
        options_progress = options_pump.join(30)
    report = {
        "schema": "advanced-realtime-v3-host-market-segment-v1",
        "status": "PASS",
        "classification": "LATE_START_LOCAL_DIAGNOSTIC_SEGMENT",
        "authorization_reference": args.authorization_reference,
        "session_date": args.session_date.isoformat(),
        "started_at_utc": started.isoformat(),
        "capture_stop_utc": args.capture_stop.isoformat(),
        "drain_stop_utc": args.drain_stop.isoformat(),
        "expected_cycles": len(args.boundaries),
        "completed_cycles": len(cycles),
        "cycles": cycles,
        "equity": {
            **_gateway_evidence(equity_gateway, equity_pump),
            "journal_generation": str(equity_generation),
            "journal_file_sha256": hashlib.sha256(
                (output / "equity.sqlite").read_bytes()
            ).hexdigest(),
        },
        "options": {
            **_gateway_evidence(options_gateway, options_pump),
            "buffer_maximum_bytes": args.options_maximum_bytes,
            "session_maximum_received_bytes": (
                args.options_session_maximum_received_bytes
            ),
            "journal_generation": str(options_generation),
            "journal_file_sha256": hashlib.sha256(
                (output / "options.sqlite").read_bytes()
            ).hexdigest(),
        },
        "effects": {
            "provider_rest_requests": len(cycles),
            "equity_websocket_sessions": 1,
            "options_websocket_sessions": 1,
            "database_writes": 0,
            "docker_actions": 0,
            "production_service_actions": 0,
            "publication": 0,
            "pointer_changes": 0,
            "read_pin_changes": 0,
            "dispatch": 0,
            "delivery": 0,
            "broker": 0,
            "trading": 0,
        },
    }
    report["semantic_sha256"] = canonical_sha256(report)
    _write(output / "summary.json", report)
    _write(
        state_path,
        {
            "schema": "advanced-realtime-v3-host-segment-state-v1",
            "status": "COMPLETE",
            "updated_at_utc": datetime.now(UTC).isoformat(),
            "expected_cycles": len(args.boundaries),
            "completed_cycles": len(cycles),
            "summary": str(output / "summary.json"),
        },
        replace=True,
    )
    return report


def main(argv=None):
    args = parse_args(argv)
    report = asyncio.run(execute_async(args))
    print(json.dumps({
        "status": report["status"],
        "classification": report["classification"],
        "completed_cycles": report["completed_cycles"],
        "semantic_sha256": report["semantic_sha256"],
        "output": str(args.output_directory / "summary.json"),
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
