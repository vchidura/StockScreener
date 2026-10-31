#!/usr/bin/env python3
"""Capture bounded exact-four V3 snapshots at consecutive five-minute boundaries."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Mapping

from dotenv import dotenv_values


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.data.polygon_http import (  # noqa: E402
    PolygonHttpResponse,
    RequestsPolygonHttpTransport,
)
from realtime.advanced_options_p8_selection_source import (  # noqa: E402
    MassiveAdvancedOptionsExactContractSource,
)
from realtime.contracts import canonical_sha256  # noqa: E402


UTC = timezone.utc
EXPECTED_CYCLES = 3
INTERVAL_MINUTES = 5
EXPECTED_CONTRACTS = 4
MAXIMUM_RESPONSE_BYTES = 1_000_000
MAXIMUM_TOTAL_BYTES = EXPECTED_CYCLES * MAXIMUM_RESPONSE_BYTES


class CountingTransport:
    def __init__(self, inner) -> None:
        self._inner = inner
        self.request_count = 0

    def get(
        self,
        url: str,
        params: Mapping[str, str],
    ) -> PolygonHttpResponse:
        self.request_count += 1
        return self._inner.get(url, params)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write(path: Path, value: object) -> None:
    encoded = (
        json.dumps(value, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    with path.open("xb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())


def next_boundary(now: datetime) -> datetime:
    observed = now.astimezone(UTC)
    minute = observed.replace(second=0, microsecond=0)
    remainder = minute.minute % INTERVAL_MINUTES
    if remainder == 0 and observed == minute:
        return minute
    return minute + timedelta(
        minutes=(
            INTERVAL_MINUTES
            if remainder == 0
            else INTERVAL_MINUTES - remainder
        )
    )


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--session-date",
        type=date.fromisoformat,
        required=True,
    )
    parser.add_argument(
        "--contract-ticker",
        action="append",
        required=True,
    )
    parser.add_argument("--cycles", type=int, required=True)
    parser.add_argument("--interval-minutes", type=int, required=True)
    parser.add_argument("--maximum-response-bytes", type=int, required=True)
    parser.add_argument("--maximum-total-bytes", type=int, required=True)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--authorized-until", type=datetime.fromisoformat, required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    args.output_directory = args.output_directory.resolve()
    args.contract_tickers = tuple(sorted({
        value.strip().upper()
        for value in args.contract_ticker
        if value.strip()
    }))
    if (
        len(args.contract_tickers) != EXPECTED_CONTRACTS
        or args.cycles != EXPECTED_CYCLES
        or args.interval_minutes != INTERVAL_MINUTES
        or args.maximum_response_bytes != MAXIMUM_RESPONSE_BYTES
        or args.maximum_total_bytes != MAXIMUM_TOTAL_BYTES
        or args.authorized_until.tzinfo is None
        or args.authorized_until.utcoffset() is None
        or not args.authorization_reference.strip()
        or args.output_directory.exists()
        or not args.output_directory.parent.is_dir()
        or not args.execute
    ):
        parser.error(
            "exact-cycle capture requires exact contracts, three five-minute "
            "cycles, 1 MB response bounds, a new output and --execute"
        )
    args.authorized_until = args.authorized_until.astimezone(UTC)
    first = next_boundary(datetime.now(UTC))
    last = first + timedelta(
        minutes=(EXPECTED_CYCLES - 1) * INTERVAL_MINUTES
    )
    if (
        first.date() != args.session_date
        or last >= args.authorized_until
    ):
        parser.error("exact-cycle capture clocks exceed authorization")
    return args


def _cycle_document(index, boundary, completed_at, capture):
    report = {
        "schema": "advanced-options-v3-exact-cycle-capture-v1",
        "cycle_index": index,
        "boundary_utc": boundary.isoformat(),
        "completed_at_utc": completed_at.isoformat(),
        "request_count": capture.request_count,
        "decoded_bytes": capture.decoded_bytes,
        "evidence": dict(capture.evidence),
        "selection": {
            "status": capture.selection.status.value,
            "reviewed_at_utc": capture.selection.reviewed_at.isoformat(),
            "valid_until_utc": capture.selection.valid_until.isoformat(),
            "source_sha256": capture.selection.source_sha256,
            "selection_sha256": capture.selection.selection_sha256,
            "contracts": list(capture.selection.contracts),
        },
        "selected_snapshots": dict(capture.selected_snapshots),
        "effects": {
            "provider_requests": 1,
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
    return report


def execute(args):
    values = dotenv_values(BACKEND_DIR / ".env")
    api_key = str(
        os.getenv("POLYGON_API_KEY")
        or values.get("POLYGON_API_KEY")
        or ""
    ).strip()
    if not api_key:
        raise RuntimeError("exact-cycle capture requires POLYGON_API_KEY")
    args.output_directory.mkdir()
    transport = CountingTransport(RequestsPolygonHttpTransport(
        api_key,
        timeout_seconds=30,
        maximum_response_bytes=args.maximum_response_bytes,
    ))
    source = MassiveAdvancedOptionsExactContractSource(
        transport,
        clock=lambda: datetime.now(UTC),
        maximum_decoded_bytes=args.maximum_response_bytes,
    )
    first = next_boundary(datetime.now(UTC))
    cycles = []
    total_bytes = 0
    try:
        for index in range(1, args.cycles + 1):
            boundary = first + timedelta(
                minutes=(index - 1) * args.interval_minutes
            )
            remaining = (boundary - datetime.now(UTC)).total_seconds()
            if remaining > 0:
                time.sleep(remaining)
            if datetime.now(UTC) >= args.authorized_until:
                raise TimeoutError(
                    "exact-cycle capture authorization expired"
                )
            capture = source.capture(
                contract_tickers=args.contract_tickers,
                reviewed_at=boundary,
                session_date=args.session_date,
            )
            completed_at = datetime.now(UTC)
            total_bytes += capture.decoded_bytes
            if (
                capture.request_count != 1
                or total_bytes > args.maximum_total_bytes
            ):
                raise RuntimeError(
                    "exact-cycle provider budget changed"
                )
            path = args.output_directory / f"cycle-{index:02d}.json"
            _write(
                path,
                _cycle_document(
                    index,
                    boundary,
                    completed_at,
                    capture,
                ),
            )
            cycles.append({
                "cycle_index": index,
                "boundary_utc": boundary.isoformat(),
                "file": str(path),
                "file_sha256": _sha256(path),
                "decoded_bytes": capture.decoded_bytes,
                "contracts": list(capture.selection.contracts),
            })
    except BaseException as error:
        _write(
            args.output_directory / "failure.json",
            {
                "schema": "advanced-options-v3-exact-cycle-failure-v1",
                "status": "FAILED",
                "failed_at_utc": datetime.now(UTC).isoformat(),
                "error_type": type(error).__name__,
                "message": str(error),
                "provider_requests_started": transport.request_count,
                "completed_cycles": len(cycles),
                "decoded_bytes": total_bytes,
                "retry_authorized": False,
            },
        )
        raise
    summary = {
        "schema": "advanced-options-v3-exact-cycle-session-v1",
        "status": "PASS",
        "classification": "MARKET_HOURS_TEST_ONLY",
        "authorization_reference": args.authorization_reference,
        "session_date": args.session_date.isoformat(),
        "interval_minutes": args.interval_minutes,
        "provider_requests": transport.request_count,
        "decoded_bytes": total_bytes,
        "cycles": cycles,
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
    summary["semantic_sha256"] = canonical_sha256(summary)
    summary_path = args.output_directory / "summary.json"
    _write(summary_path, summary)
    return {
        "status": summary["status"],
        "provider_requests": summary["provider_requests"],
        "decoded_bytes": summary["decoded_bytes"],
        "completed_cycles": len(cycles),
        "semantic_sha256": summary["semantic_sha256"],
        "file_sha256": _sha256(summary_path),
        "output": str(summary_path),
        "effects": summary["effects"],
    }


def main(argv=None):
    result = execute(parse_args(argv))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
