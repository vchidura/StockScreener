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
    RealtimeV2PipelineConfig,
)
from realtime.journal import RawJournalReader  # noqa: E402
from realtime.normalizer import (  # noqa: E402
    InstrumentAdmission,
    NORMALIZER_VERSION,
)
from scripts.realtime_script_support import write_report  # noqa: E402


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--decoded-byte-ceiling", type=int, required=True)
    parser.add_argument("--maximum-market-messages", type=int, default=2)
    parser.add_argument("--duration-seconds", type=int, default=300)
    parser.add_argument("--journal-path", type=Path, required=True)
    parser.add_argument("--report-path", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, default=BACKEND_DIR / ".env")
    args = parser.parse_args(argv)
    if args.maximum_market_messages != 2:
        parser.error("the approved canary requires exactly two market messages")
    if not 1 <= args.duration_seconds <= 300:
        parser.error("--duration-seconds must be in [1, 300]")
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
    return await asyncio.wait_for(
        gateway.run_session(maximum_market_messages=messages),
        timeout=duration,
    )


def main(argv=None):
    args = parse_args(argv)
    config = RealtimeV2PipelineConfig.from_mapping(
        dotenv_values(args.env_file)
    )
    if config.equity_pipeline is not EquityPipeline.MASSIVE_ADVANCED:
        raise RuntimeError("canary requires MASSIVE_ADVANCED equity")
    generation = uuid4()
    session_id = uuid4()
    universe = UniverseManifest(
        "advanced-spy-am-live-canary-v2",
        ("SPY",),
        (),
    )
    manifest = SubscriptionManifest(
        version="advanced-spy-am-live-canary-v2",
        session_date=args.session_date,
        universe=universe,
        capability_profile_sha256="a" * 64,
        allocation_policy_sha256="b" * 64,
        members=(
            SubscriptionMember(
                AssetClass.EQUITY,
                "SPY",
                "SPY",
                MarketChannel.MINUTE_AGGREGATE,
            ),
        ),
        maximum_option_quote_contracts=0,
    )
    buffer = RawMessageBuffer(
        BufferLimits(
            2,
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
        api_key=config.massive_api_key,
        monotonic_ns=time.monotonic_ns,
        session_id_factory=lambda: session_id,
    )
    pump = AdvancedEquityJournalPump(
        buffer,
        args.journal_path,
        generation,
        {
            "SPY": InstrumentAdmission(
                1,
                "SPY",
                "SPY",
                AssetClass.EQUITY,
                0,
            )
        },
        maximum_batch_messages=2,
        maximum_batch_bytes=4 * 1024 * 1024,
    )
    pump.start()
    try:
        result = asyncio.run(
            _capture(
                gateway,
                args.maximum_market_messages,
                args.duration_seconds,
            )
        )
    finally:
        pump.request_stop()
    progress = pump.join(30)
    normalized = RawJournalReader(
        args.journal_path
    ).read_normalized_after(0)
    status = (
        "PASS"
        if len(normalized) == 2
        and all(
            row.normalizer_version == NORMALIZER_VERSION
            and row.event_kind is MarketChannel.MINUTE_AGGREGATE
            and row.quality == "GOOD"
            for row in normalized
        )
        and result.received_bytes <= args.decoded_byte_ceiling
        else "FAIL"
    )
    report = {
        "status": status,
        "session_date": args.session_date.isoformat(),
        "authorization_reference": args.authorization_reference,
        "symbols": ("SPY",),
        "channels": ("AM",),
        "journal_generation": str(generation),
        "stream_session_id": str(session_id),
        "normalizer_version": NORMALIZER_VERSION,
        "received_frames": result.received_messages,
        "received_bytes": result.received_bytes,
        "market_messages": result.accepted_messages,
        "market_bytes": result.accepted_bytes,
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
