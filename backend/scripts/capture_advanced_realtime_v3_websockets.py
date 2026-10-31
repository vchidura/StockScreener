#!/usr/bin/env python3
"""Capture one bounded target-9 Equity and exact-four Options WebSocket session."""
from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import os
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

from dotenv import dotenv_values


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_gateway import (  # noqa: E402
    AdvancedGateway,
    GatewayConfiguration,
)
from realtime.advanced_realtime_session_scheduler import (  # noqa: E402
    ADVANCED_SESSION_EQUITY_SYMBOLS,
)
from realtime.buffer import BufferLimits, RawMessageBuffer  # noqa: E402
from realtime.contracts import (  # noqa: E402
    AssetClass,
    MarketChannel,
    SubscriptionManifest,
    SubscriptionMember,
    UniverseManifest,
    canonical_sha256,
)


UTC = timezone.utc
EQUITY_BYTES = 64 * 1024 * 1024
OPTIONS_BYTES = 32 * 1024 * 1024
MAXIMUM_SECONDS = 20 * 60


def _write(path, value):
    encoded = (
        json.dumps(value, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    with path.open("xb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())


def _drain(buffer):
    claim = buffer.claim_nowait(
        maximum_events=10_000,
        maximum_bytes=buffer.limits.maximum_bytes,
    )
    if claim is None:
        return []
    rows = [
        {
            "stream_session_id": str(row.stream_session_id),
            "message_number": row.message_number,
            "feed": row.feed.value,
            "received_at_utc": row.received_at.isoformat(),
            "received_monotonic_ns": row.received_monotonic_ns,
            "payload_bytes": row.byte_length,
            "payload_sha256": hashlib.sha256(row.payload).hexdigest(),
            "payload_base64": base64.b64encode(row.payload).decode("ascii"),
        }
        for row in claim.messages
    ]
    buffer.acknowledge(claim.claim_id)
    return rows


def _manifest(session_date, contracts, *, options):
    universe = UniverseManifest(
        "advanced-realtime-v3-host-provider-test-v1",
        tuple(ADVANCED_SESSION_EQUITY_SYMBOLS),
        ("QQQ", "SPY") if options else (),
    )
    if options:
        members = tuple(
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
    else:
        members = tuple(
            SubscriptionMember(
                AssetClass.EQUITY,
                ticker,
                ticker,
                MarketChannel.MINUTE_AGGREGATE,
            )
            for ticker in ADVANCED_SESSION_EQUITY_SYMBOLS
        )
    return SubscriptionManifest(
        version="advanced-realtime-v3-host-provider-test-v1",
        session_date=session_date,
        universe=universe,
        capability_profile_sha256="a" * 64,
        allocation_policy_sha256="b" * 64,
        members=members,
        maximum_option_quote_contracts=4 if options else 0,
    )


async def _capture(configuration, manifest, byte_limit, message_limit, api_key):
    buffer = RawMessageBuffer(
        BufferLimits(10_000, byte_limit, MAXIMUM_SECONDS * 1_000_000_000),
        monotonic_ns=time.monotonic_ns,
    )
    gateway = AdvancedGateway(
        configuration,
        manifest,
        buffer,
        api_key=api_key,
        monotonic_ns=time.monotonic_ns,
    )
    result = await asyncio.wait_for(
        gateway.run_session(
            maximum_market_messages=message_limit,
            maximum_received_bytes=byte_limit,
        ),
        timeout=MAXIMUM_SECONDS,
    )
    return result, _drain(buffer)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--contract-ticker", action="append", required=True)
    parser.add_argument("--maximum-seconds", type=int, required=True)
    parser.add_argument("--equity-maximum-bytes", type=int, required=True)
    parser.add_argument("--options-maximum-bytes", type=int, required=True)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    args.output = args.output.resolve()
    args.contracts = tuple(sorted(set(args.contract_ticker)))
    if (
        len(args.contracts) != 4
        or args.maximum_seconds != MAXIMUM_SECONDS
        or args.equity_maximum_bytes != EQUITY_BYTES
        or args.options_maximum_bytes != OPTIONS_BYTES
        or args.output.exists()
        or not args.output.parent.is_dir()
        or not args.authorization_reference.strip()
        or not args.execute
    ):
        parser.error("WebSocket capture scope differs from its exact grant")
    return args


def execute(args):
    values = dotenv_values(BACKEND_DIR / ".env")
    api_key = str(
        os.getenv("POLYGON_API_KEY")
        or values.get("POLYGON_API_KEY")
        or ""
    ).strip()
    if not api_key:
        raise RuntimeError("WebSocket capture requires POLYGON_API_KEY")
    started = datetime.now(UTC)
    equity_manifest = _manifest(
        args.session_date,
        args.contracts,
        options=False,
    )
    options_manifest = _manifest(
        args.session_date,
        args.contracts,
        options=True,
    )

    async def run():
        return await asyncio.gather(
            _capture(
                GatewayConfiguration.stocks(),
                equity_manifest,
                args.equity_maximum_bytes,
                len(ADVANCED_SESSION_EQUITY_SYMBOLS),
                api_key,
            ),
            _capture(
                GatewayConfiguration.options(),
                options_manifest,
                args.options_maximum_bytes,
                len(args.contracts) * 2,
                api_key,
            ),
        )

    (equity_result, equity_rows), (
        options_result,
        options_rows,
    ) = asyncio.run(run())
    report = {
        "schema": "advanced-realtime-v3-host-websocket-capture-v1",
        "status": "PASS",
        "authorization_reference": args.authorization_reference,
        "session_date": args.session_date.isoformat(),
        "started_at_utc": started.isoformat(),
        "completed_at_utc": datetime.now(UTC).isoformat(),
        "equity": {
            "subscriptions": list(equity_result.subscriptions),
            "connection_attempts": equity_result.connection_attempts,
            "received_messages": equity_result.received_messages,
            "received_bytes": equity_result.received_bytes,
            "accepted_messages": equity_result.accepted_messages,
            "accepted_bytes": equity_result.accepted_bytes,
            "final_state": equity_result.final_state.value,
            "frames": equity_rows,
        },
        "options": {
            "subscriptions": list(options_result.subscriptions),
            "connection_attempts": options_result.connection_attempts,
            "received_messages": options_result.received_messages,
            "received_bytes": options_result.received_bytes,
            "accepted_messages": options_result.accepted_messages,
            "accepted_bytes": options_result.accepted_bytes,
            "final_state": options_result.final_state.value,
            "frames": options_rows,
        },
        "effects": {
            "equity_websocket_sessions": 1,
            "options_websocket_sessions": 1,
            "database_reads": 0,
            "database_writes": 0,
            "docker_actions": 0,
            "production_service_actions": 0,
            "publication": 0,
            "pointer_changes": 0,
            "read_pin_changes": 0,
        },
    }
    report["semantic_sha256"] = canonical_sha256(report)
    _write(args.output, report)
    return report


def main(argv=None):
    report = execute(parse_args(argv))
    print(json.dumps({
        "status": report["status"],
        "semantic_sha256": report["semantic_sha256"],
        "equity_messages": report["equity"]["accepted_messages"],
        "equity_bytes": report["equity"]["accepted_bytes"],
        "options_messages": report["options"]["accepted_messages"],
        "options_bytes": report["options"]["accepted_bytes"],
        "output": str(parse_args(argv).output) if False else None,
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
