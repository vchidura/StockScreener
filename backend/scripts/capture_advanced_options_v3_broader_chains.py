#!/usr/bin/env python3
"""Wait for a live checkpoint, then capture bounded broader QQQ/SPY chains."""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from dotenv import dotenv_values


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.data.polygon_http import RequestsPolygonHttpTransport  # noqa: E402
from realtime.advanced_options_p7_selection import (  # noqa: E402
    P7TransportSelectionPolicy,
)
from realtime.advanced_options_p8_selection_source import (  # noqa: E402
    MassiveAdvancedOptionsSelectionSource,
)
from realtime.contracts import canonical_sha256  # noqa: E402


UTC = timezone.utc
UNDERLYINGS = ("QQQ", "SPY")


class StaticSpots:
    def __init__(self, values):
        self._values = values

    def load_spots(self, *, underlyings, available_before):
        del available_before
        if tuple(underlyings) != UNDERLYINGS:
            raise ValueError("broader-chain underlying scope changed")
        return self._values


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


def _read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def _spots(cycles_directory):
    cycles = sorted(cycles_directory.glob("cycle-*.json"))
    if not cycles:
        raise RuntimeError("live segment has no completed cycle artifact")
    cycle = _read(cycles[-1])
    values = {}
    for row in cycle["selected_snapshots"].values():
        underlying = row.get("underlying_asset") or {}
        ticker = str(underlying.get("ticker", "")).upper()
        price = underlying.get("price", underlying.get("value"))
        if ticker in UNDERLYINGS and price is not None:
            values[ticker] = Decimal(str(price))
    if set(values) != set(UNDERLYINGS):
        raise RuntimeError("live segment spot evidence is incomplete")
    return values, cycles[-1]


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--live-state", type=Path, required=True)
    parser.add_argument("--live-cycles-directory", type=Path, required=True)
    parser.add_argument("--required-completed-cycles", type=int, required=True)
    parser.add_argument("--maximum-dte", type=int, required=True)
    parser.add_argument("--maximum-moneyness-fraction", type=Decimal, required=True)
    parser.add_argument("--maximum-response-bytes", type=int, required=True)
    parser.add_argument("--maximum-total-bytes", type=int, required=True)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--wait-until", type=datetime.fromisoformat, required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    for name in ("live_state", "live_cycles_directory", "output_directory"):
        setattr(args, name, getattr(args, name).resolve())
    args.wait_until = args.wait_until.astimezone(UTC)
    if (
        args.required_completed_cycles != 6
        or args.maximum_dte != 2
        or args.maximum_moneyness_fraction != Decimal("0.05")
        or args.maximum_response_bytes != 50_000_000
        or args.maximum_total_bytes != 100_000_000
        or args.output_directory.exists()
        or not args.output_directory.parent.is_dir()
        or not args.authorization_reference.strip()
        or not args.execute
    ):
        parser.error("broader-chain scope differs from its exact grant")
    return args


def execute(args):
    output = args.output_directory
    output.mkdir()
    state_path = output / "state.json"
    _write(state_path, {
        "schema": "advanced-options-v3-broader-chain-state-v1",
        "status": "WAITING_FOR_CYCLE_6",
        "updated_at_utc": datetime.now(UTC).isoformat(),
        "provider_requests": 0,
    })
    live_state = None
    while datetime.now(UTC) < args.wait_until:
        if args.live_state.is_file():
            live_state = _read(args.live_state)
            if live_state.get("status") not in {"RUNNING", "COMPLETE"}:
                raise RuntimeError("live segment is not healthy")
            if (
                int(live_state.get("completed_cycles", 0))
                >= args.required_completed_cycles
            ):
                break
        time.sleep(5)
    else:
        raise TimeoutError("cycle-6 chain-test checkpoint was not reached")
    spots, spot_cycle = _spots(args.live_cycles_directory)
    values = dotenv_values(BACKEND_DIR / ".env")
    api_key = str(
        os.getenv("POLYGON_API_KEY")
        or values.get("POLYGON_API_KEY")
        or ""
    ).strip()
    if not api_key:
        raise RuntimeError("broader-chain capture requires POLYGON_API_KEY")
    policy = P7TransportSelectionPolicy(
        version="advanced-options-v3-broader-chain-test-v1",
        maximum_dte=args.maximum_dte,
        maximum_moneyness_fraction=args.maximum_moneyness_fraction,
    )
    source = MassiveAdvancedOptionsSelectionSource(
        RequestsPolygonHttpTransport(
            api_key,
            timeout_seconds=30,
            maximum_response_bytes=args.maximum_response_bytes,
        ),
        StaticSpots(spots),
        policy=policy,
        maximum_response_decoded_bytes=args.maximum_response_bytes,
        maximum_total_decoded_bytes=args.maximum_total_bytes,
    )
    captures = []
    total_bytes = 0
    for underlyer in UNDERLYINGS:
        rows, response_bytes, snapshot_at, payload, raw_snapshots = (
            source._request(
                underlyer,
                spot=spots[underlyer],
                session_date=args.session_date,
            )
        )
        total_bytes += response_bytes
        if total_bytes > args.maximum_total_bytes:
            raise RuntimeError("broader-chain total byte ceiling exceeded")
        expirations = sorted({
            row.expiration_date.isoformat() for row in rows
        })
        captures.append({
            "underlyer": underlyer,
            "spot": str(spots[underlyer]),
            "request_id": payload.get("request_id"),
            "response_bytes": response_bytes,
            "response_sha256": canonical_sha256(payload),
            "response_row_count": len(payload["results"]),
            "eligible_row_count": len(rows),
            "skipped_row_count": len(payload["results"]) - len(rows),
            "call_count": sum(row.option_type == "C" for row in rows),
            "put_count": sum(row.option_type == "P" for row in rows),
            "expirations": expirations,
            "snapshot_at_utc": snapshot_at.isoformat(),
            "payload": payload,
            "eligible_snapshots": raw_snapshots,
        })
        _write(
            output / f"{underlyer.casefold()}-chain.json",
            captures[-1],
        )
    report = {
        "schema": "advanced-options-v3-broader-chain-capture-v1",
        "status": "PASS",
        "classification": "MARKET_HOURS_TEST_ONLY",
        "authorization_reference": args.authorization_reference,
        "session_date": args.session_date.isoformat(),
        "checkpoint": {
            "required_completed_cycles": args.required_completed_cycles,
            "observed_completed_cycles": live_state["completed_cycles"],
            "spot_cycle": str(spot_cycle),
        },
        "scope": {
            "underlyings": list(UNDERLYINGS),
            "minimum_dte": policy.minimum_dte,
            "maximum_dte": policy.maximum_dte,
            "maximum_moneyness_fraction": str(
                policy.maximum_moneyness_fraction
            ),
            "maximum_results_per_underlying": 250,
            "pagination_requests": 0,
        },
        "provider_requests": 2,
        "decoded_bytes": total_bytes,
        "captures": [
            {
                key: value
                for key, value in capture.items()
                if key not in {"payload", "eligible_snapshots"}
            }
            for capture in captures
        ],
        "effects": {
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
    _write(output / "summary.json", report)
    _write(
        state_path,
        {
            "schema": "advanced-options-v3-broader-chain-state-v1",
            "status": "COMPLETE",
            "updated_at_utc": datetime.now(UTC).isoformat(),
            "provider_requests": 2,
            "summary": str(output / "summary.json"),
        },
        replace=True,
    )
    return report


def main(argv=None):
    args = parse_args(argv)
    report = execute(args)
    print(json.dumps({
        "status": report["status"],
        "provider_requests": report["provider_requests"],
        "decoded_bytes": report["decoded_bytes"],
        "semantic_sha256": report["semantic_sha256"],
        "output": str(args.output_directory / "summary.json"),
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
