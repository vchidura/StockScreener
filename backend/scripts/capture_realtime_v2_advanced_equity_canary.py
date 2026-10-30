#!/usr/bin/env python3
"""Capture bounded SPY/AM bytes into a local normalizer-v2 journal."""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

from dotenv import dotenv_values


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_equity_ingress import (  # noqa: E402
    AdvancedEquityJournalPump,
)
from realtime.advanced_gateway import (  # noqa: E402
    AdvancedGateway,
    GatewayConfiguration,
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
from realtime.equity_pipeline_config import (  # noqa: E402
    EquityPipeline,
)
from realtime.journal import RawJournalReader  # noqa: E402
from realtime.normalizer import (  # noqa: E402
    InstrumentAdmission,
    NORMALIZER_VERSION,
)
from realtime.contracts import EQUITY_VALIDATION_UNIVERSE  # noqa: E402
from scripts.realtime_script_support import write_report  # noqa: E402


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--symbol", action="append")
    parser.add_argument("--decoded-byte-ceiling", type=int, required=True)
    parser.add_argument("--maximum-market-messages", type=int, default=2)
    parser.add_argument("--subscription-only", action="store_true")
    parser.add_argument("--duration-seconds", type=int, default=300)
    parser.add_argument("--journal-path", type=Path, required=True)
    parser.add_argument("--report-path", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, default=BACKEND_DIR / ".env")
    args = parser.parse_args(argv)
    args.symbols = tuple(sorted({
        value.strip().upper()
        for value in (args.symbol or ("SPY",))
        if value.strip()
    }))
    target9 = tuple(sorted(EQUITY_VALIDATION_UNIVERSE.equity_tickers))
    if args.symbols not in {("SPY",), target9}:
        parser.error("the canary requires exact SPY or target-9 membership")
    expected_messages = (
        0
        if args.subscription_only
        else 2 if args.symbols == ("SPY",) else 90
    )
    if args.maximum_market_messages != expected_messages:
        parser.error(
            f"the approved canary requires {expected_messages} market messages"
        )
    allowed_durations = (
        {300, 660}
        if args.symbols == target9
        else set(range(1, 301))
    )
    if args.duration_seconds not in allowed_durations:
        parser.error(
            "--duration-seconds must be in [1, 300] for SPY "
            "or exactly 300/660 for target-9"
        )
    if args.decoded_byte_ceiling != 10 * 1024 * 1024:
        parser.error("the approved canary decoded-byte ceiling is 10 MiB")
    if not args.authorization_reference.strip():
        parser.error("--authorization-reference cannot be blank")
    args.journal_path = args.journal_path.resolve()
    args.report_path = args.report_path.resolve()
    if args.journal_path.exists() or args.report_path.exists():
        parser.error("journal and report paths must be new")
    return args


async def _capture(gateway, messages, duration):
    try:
        return await asyncio.wait_for(
            gateway.run_session(maximum_market_messages=messages),
            timeout=duration,
        ), False
    except TimeoutError:
        return None, True


def _entitlement_evidence(checked_at, symbols=("SPY",)):
    checked = checked_at.astimezone(timezone.utc)
    return {
        "checked_at": checked.isoformat(),
        "credentials_printed": False,
        "scope": {
            "authorized_universe": list(symbols),
            "authorized_channels": ["AM"],
        },
    }


def _canary_api_key(settings):
    if (
        str(settings.get("REALTIME_V2_EQUITY_PIPELINE") or "").strip()
        != EquityPipeline.MASSIVE_ADVANCED.value
    ):
        raise RuntimeError("canary requires MASSIVE_ADVANCED equity")
    api_key = str(settings.get("POLYGON_API_KEY") or "").strip()
    if not api_key or api_key.lower() in {
        "...",
        "replace_with_provider_value",
        "your_polygon_api_key_here",
    }:
        raise RuntimeError("canary requires a configured Massive API key")
    return api_key


def _normalized_scope_matches(normalized, symbols, expected_messages):
    return (
        len(normalized) == expected_messages
        and {row.provider_symbol for row in normalized} == set(symbols)
        and all(
            row.normalizer_version == NORMALIZER_VERSION
            and row.event_kind is MarketChannel.MINUTE_AGGREGATE
            and row.quality == "GOOD"
            for row in normalized
        )
    )


def main(argv=None):
    args = parse_args(argv)
    api_key = _canary_api_key(dotenv_values(args.env_file))
    generation = uuid4()
    session_id = uuid4()
    version = (
        "advanced-spy-am-live-canary-v2"
        if args.symbols == ("SPY",)
        else "advanced-target9-am-live-canary-v1"
    )
    universe = UniverseManifest(version, args.symbols, ())
    manifest = SubscriptionManifest(
        version=version,
        session_date=args.session_date,
        universe=universe,
        capability_profile_sha256="a" * 64,
        allocation_policy_sha256="b" * 64,
        members=tuple(
            SubscriptionMember(
                AssetClass.EQUITY,
                symbol,
                symbol,
                MarketChannel.MINUTE_AGGREGATE,
            )
            for symbol in args.symbols
        ),
        maximum_option_quote_contracts=0,
    )
    buffer = RawMessageBuffer(
        BufferLimits(
            max(1, args.maximum_market_messages),
            4 * 1024 * 1024,
            330_000_000_000,
        ),
        monotonic_ns=time.monotonic_ns,
    )
    gateway = AdvancedGateway(
        GatewayConfiguration(
            MarketFeed.STOCKS,
            GatewayConfiguration.stocks().endpoint,
            maximum_message_bytes=2 * 1024 * 1024,
            library_max_queue=2,
        ),
        manifest,
        buffer,
        api_key=api_key,
        monotonic_ns=time.monotonic_ns,
        session_id_factory=lambda: session_id,
    )
    pump = AdvancedEquityJournalPump(
        buffer,
        args.journal_path,
        generation,
        {
            symbol: InstrumentAdmission(
                priority,
                symbol,
                symbol,
                AssetClass.EQUITY,
                0,
            )
            for priority, symbol in enumerate(args.symbols, start=1)
        },
        maximum_batch_messages=max(1, args.maximum_market_messages),
        maximum_batch_bytes=4 * 1024 * 1024,
    )
    pump.start()
    try:
        result, timed_out = asyncio.run(
            _capture(
                gateway,
                args.maximum_market_messages,
                args.duration_seconds,
            )
        )
    finally:
        pump.request_stop()
    progress = pump.join(30)
    reader = RawJournalReader(args.journal_path)
    raw = reader.read_after(
        0,
        limit=max(1, args.maximum_market_messages),
    )
    normalized = reader.read_normalized_after(0)
    status = "PASS" if (
        not timed_out
        and result is not None
        and
        (
            args.subscription_only
            and result.accepted_messages == 0
            and not normalized
        )
        or (
            not args.subscription_only
            and _normalized_scope_matches(
                normalized,
                args.symbols,
                args.maximum_market_messages,
            )
        )
    ) and result.received_bytes <= args.decoded_byte_ceiling else "FAIL"
    report = {
        "status": status,
        **_entitlement_evidence(
            datetime.now(timezone.utc),
            args.symbols,
        ),
        "session_date": args.session_date.isoformat(),
        "authorization_reference": args.authorization_reference,
        "symbols": args.symbols,
        "channels": ("AM",),
        "subscription_only": args.subscription_only,
        "journal_generation": str(generation),
        "stream_session_id": str(session_id),
        "normalizer_version": NORMALIZER_VERSION,
        "timed_out": timed_out,
        "received_frames": (
            result.received_messages
            if result is not None
            else gateway.received_messages
        ),
        "received_bytes": (
            result.received_bytes
            if result is not None
            else gateway.received_bytes
        ),
        "market_messages": (
            result.accepted_messages
            if result is not None
            else len(raw)
        ),
        "market_bytes": (
            result.accepted_bytes
            if result is not None
            else sum(len(row.payload) for row in raw)
        ),
        "last_raw_offset": progress.last_raw_offset,
        "last_event_offset": progress.last_event_offset,
        "event_time_watermark": (
            progress.event_time_watermark.isoformat()
            if progress.event_time_watermark is not None
            else None
        ),
        "connection_attempts": 1,
        "http_requests": 0,
        "credentials_exposed": False,
        "publication_advanced": False,
        "production_writes": 0,
    }
    write_report(args.report_path, report)
    print(json.dumps(report, indent=2))
    return 0 if status == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
