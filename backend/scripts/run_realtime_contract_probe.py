#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from urllib.parse import quote, urlsplit

from dotenv import load_dotenv


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.calendar import OptionExchangeCalendar
from options.data.polygon_http import RequestsPolygonHttpTransport
from realtime.contracts import CANARY_UNIVERSE, canonical_sha256
from scripts.realtime_script_support import PacedPolygonTransport


DEFAULT_REPORT_PATH = BACKEND_DIR / ".realtime-canary" / "contract-candidates.json"
MAXIMUM_PROVIDER_REQUESTS = 16
MAXIMUM_RESPONSE_BYTES = 2 * 1024 * 1024
MAXIMUM_TOTAL_BYTES = 32 * 1024 * 1024
MAXIMUM_CHAIN_ROWS = 250
STRIKE_CORRIDOR = Decimal("0.15")
MINIMUM_DAY_VOLUME = 20
MINIMUM_OPEN_INTEREST = 100


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Qualify one Developer option contract per canary underlying."
    )
    parser.add_argument("--report-path", default=str(DEFAULT_REPORT_PATH))
    parser.add_argument("--execute", action="store_true")
    return parser.parse_args()


def _write_report(path: Path, report: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    payload = (json.dumps(report, indent=2, sort_keys=True) + "\n").encode("utf-8")
    with temporary.open("wb") as output:
        output.write(payload)
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary, path)


class ContractQualificationProbe:
    def __init__(self, transport, *, calendar=None) -> None:
        self.transport = transport
        self.calendar = calendar or OptionExchangeCalendar()
        self.requests = 0
        self.response_bytes = 0

    def _request(self, url: str, params: dict[str, str]) -> dict[str, object]:
        parsed = urlsplit(url)
        if parsed.scheme != "https" or parsed.hostname not in {"api.massive.com", "api.polygon.io"}:
            raise ValueError("contract probe requires the approved Polygon HTTPS host")
        if self.requests >= MAXIMUM_PROVIDER_REQUESTS:
            raise ValueError("contract probe exceeded its request bound")
        response = self.transport.get(url, params)
        self.requests += 1
        body_length = len(response.body)
        if body_length > MAXIMUM_RESPONSE_BYTES:
            raise ValueError("contract probe response exceeded its byte bound")
        self.response_bytes += body_length
        if self.response_bytes > MAXIMUM_TOTAL_BYTES:
            raise ValueError("contract probe exceeded its total byte bound")
        if response.status_code != 200:
            raise ValueError(f"contract probe failed with HTTP {response.status_code}")
        payload = json.loads(response.body.decode("utf-8"))
        if not isinstance(payload, dict) or str(payload.get("status") or "").upper() not in {
            "OK",
            "DELAYED",
        }:
            raise ValueError("contract probe response status is invalid")
        if payload.get("next_url"):
            raise ValueError("contract probe response requires pagination")
        return payload

    def _spot(self, ticker: str, requested_at: datetime) -> tuple[Decimal, datetime, dict]:
        bar_start = self.calendar.latest_completed_minute_start(requested_at)
        milliseconds = int(bar_start.timestamp() * 1000)
        payload = self._request(
            f"https://api.massive.com/v2/aggs/ticker/{quote(ticker, safe='')}/"
            f"range/1/minute/{milliseconds}/{milliseconds}",
            {"adjusted": "false", "sort": "asc", "limit": "1"},
        )
        rows = payload.get("results")
        if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict):
            raise ValueError(f"contract probe requires one completed minute for {ticker}")
        row = dict(rows[0])
        if int(row.get("t", -1)) != milliseconds:
            raise ValueError(f"contract probe minute identity drifted for {ticker}")
        spot = Decimal(str(row.get("c")))
        if not spot.is_finite() or spot <= 0:
            raise ValueError(f"contract probe spot is invalid for {ticker}")
        return spot, bar_start + timedelta(minutes=1), row

    @staticmethod
    def _candidate(
        row: dict,
        ticker: str,
        spot: Decimal,
        market_date: date,
        target_expiration: date,
    ):
        details = row.get("details")
        if not isinstance(details, dict):
            return None
        try:
            contract_ticker = str(details["ticker"])
            contract_type = str(details["contract_type"]).lower()
            expiration = date.fromisoformat(str(details["expiration_date"]))
            strike = Decimal(str(details["strike_price"]))
            exercise_style = str(details.get("exercise_style") or "").lower()
            multiplier = int(details.get("shares_per_contract") or 0)
            volume = int((row.get("day") or {}).get("volume") or 0)
            open_interest = int(row.get("open_interest") or 0)
        except (KeyError, TypeError, ValueError, ArithmeticError):
            return None
        lower = spot * (Decimal("1") - STRIKE_CORRIDOR)
        upper = spot * (Decimal("1") + STRIKE_CORRIDOR)
        dte = (expiration - market_date).days
        if (
            not contract_ticker.startswith(f"O:{ticker}")
            or contract_type != "call"
            or exercise_style != "american"
            or multiplier != 100
            or expiration != target_expiration
            or not 0 <= dte <= 60
            or not lower <= strike <= upper
            or (volume < MINIMUM_DAY_VOLUME and open_interest < MINIMUM_OPEN_INTEREST)
        ):
            return None
        return {
            "underlying": ticker,
            "contract_ticker": contract_ticker,
            "expiration_date": expiration.isoformat(),
            "strike": str(strike),
            "day_volume": volume,
            "open_interest": open_interest,
            "distance_from_spot": str(abs(strike - spot)),
            "snapshot_sha256": canonical_sha256(row),
            "snapshot": row,
        }

    def run(self, requested_at: datetime) -> dict[str, object]:
        requested = requested_at.astimezone(timezone.utc)
        selections = []
        for ticker in CANARY_UNIVERSE.option_underlyings:
            spot, market_time, bar = self._spot(ticker, requested)
            market_date = market_time.date()
            days_to_friday = (4 - market_date.weekday()) % 7
            target_expiration = market_date + timedelta(
                days=days_to_friday if days_to_friday else 7
            )
            lower = spot * (Decimal("1") - STRIKE_CORRIDOR)
            upper = spot * (Decimal("1") + STRIKE_CORRIDOR)
            payload = self._request(
                f"https://api.massive.com/v3/snapshot/options/{quote(ticker, safe='')}",
                {
                    "contract_type": "call",
                    "expiration_date.gte": target_expiration.isoformat(),
                    "expiration_date.lte": target_expiration.isoformat(),
                    "strike_price.gte": str(lower),
                    "strike_price.lte": str(upper),
                    "limit": str(MAXIMUM_CHAIN_ROWS),
                    "sort": "ticker",
                    "order": "asc",
                },
            )
            rows = payload.get("results")
            if not isinstance(rows, list) or len(rows) > MAXIMUM_CHAIN_ROWS:
                raise ValueError(f"contract probe chain shape is invalid for {ticker}")
            candidates = [
                candidate
                for row in rows
                if isinstance(row, dict)
                for candidate in [
                    self._candidate(
                        row,
                        ticker,
                        spot,
                        market_date,
                        target_expiration,
                    )
                ]
                if candidate is not None
            ]
            if not candidates:
                raise ValueError(f"contract probe found no eligible call for {ticker}")
            selected = min(
                candidates,
                key=lambda row: (
                    Decimal(row["distance_from_spot"]),
                    row["expiration_date"],
                    row["contract_ticker"],
                ),
            )
            selected.update(
                {
                    "spot": str(spot),
                    "spot_market_time": market_time.isoformat(),
                    "spot_bar_sha256": canonical_sha256(bar),
                }
            )
            selections.append(selected)
        if self.requests != MAXIMUM_PROVIDER_REQUESTS:
            raise ValueError("contract probe request count changed")
        manifest_sha256 = canonical_sha256(
            {
                "version": "developer-contract-candidates-v1",
                "requested_at": requested,
                "selections": tuple(selections),
            }
        )
        return {
            "state": "COMPLETE",
            "schema": "developer-contract-candidates-v1",
            "requested_at": requested.isoformat(),
            "provider_requests": self.requests,
            "response_bytes": self.response_bytes,
            "selection_count": len(selections),
            "manifest_sha256": manifest_sha256,
            "selections": selections,
            "database_writes": 0,
            "values_exposed": False,
        }


def main() -> int:
    args = parse_args()
    if not args.execute:
        print(json.dumps({
            "mode": "PLAN",
            "symbols": CANARY_UNIVERSE.option_underlyings,
            "spot_requests": 8,
            "option_snapshot_requests": 8,
            "maximum_provider_requests": 16,
            "maximum_requests_per_minute": 5,
            "automatic_retries": 0,
            "pagination": False,
            "database_writes": 0,
        }, indent=2))
        print("No database writes or provider requests performed.")
        return 0
    load_dotenv(BACKEND_DIR / ".env", override=True)
    api_key = os.getenv("POLYGON_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("contract probe requires POLYGON_API_KEY")
    transport = PacedPolygonTransport(RequestsPolygonHttpTransport(api_key))
    try:
        report = ContractQualificationProbe(transport).run(datetime.now(timezone.utc))
    except Exception as error:
        message = str(error).replace(api_key, "[REDACTED]")
        report = {
            "state": "FAILED",
            "error_type": type(error).__name__,
            "error_message": message,
            "database_writes": 0,
            "values_exposed": False,
        }
        _write_report(Path(args.report_path).resolve(), report)
        raise
    _write_report(Path(args.report_path).resolve(), report)
    print(json.dumps({key: value for key, value in report.items() if key != "selections"}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())