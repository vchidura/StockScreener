#!/usr/bin/env python3
"""Run the exact approved two-request P7 contract reference lookup."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

import psycopg2
from dotenv import dotenv_values


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.data.polygon_http import (  # noqa: E402
    RequestsPolygonHttpTransport,
)
from provider_endpoints import REST_BASE_URL  # noqa: E402
from scripts.audit_advanced_options_p7_inputs import _dsn  # noqa: E402


UTC = timezone.utc
PLAN_VERSION = "advanced-options-p7-reference-lookup-plan-v1"
APPROVAL_REFERENCE = "ADVANCED-OPTIONS-P7-REFERENCE-LOOKUP-20261020-01"
_OPTION = re.compile(
    r"^O:(?P<underlyer>[A-Z][A-Z0-9.-]{0,14})"
    r"(?P<expiration>\d{6})C(?P<strike>\d{8})$"
)


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--plan", type=Path, required=True)
    value.add_argument("--approval-reference", required=True)
    value.add_argument("--env-file", type=Path, default=BACKEND_DIR / ".env")
    value.add_argument("--output", type=Path, required=True)
    value.add_argument("--execute", action="store_true")
    value.add_argument("--allow-finalized-bar-fallback", action="store_true")
    value.add_argument("--local-input-approval-reference")
    return value


def _load_plan(path: Path) -> dict[str, object]:
    plan = json.loads(path.read_text(encoding="utf-8"))
    if (
        not isinstance(plan, dict)
        or plan.get("version") != PLAN_VERSION
        or plan.get("state") != "AWAITING_EXACT_APPROVAL"
        or plan.get("provider") != "massive"
        or plan.get("endpoint") != "/v3/reference/options/contracts"
        or plan.get("underlyings") != ["SPY", "QQQ"]
        or plan.get("request_scope", {}).get("maximum_requests") != 2
        or plan.get("request_scope", {}).get("pagination_authorized") is not False
        or plan.get("prohibited_effects", {}).get("websocket_connections") != 0
        or plan.get("prohibited_effects", {}).get("production_database_writes") != 0
    ):
        raise ValueError("P7 reference lookup plan changed")
    return plan


def _api_key(path: Path) -> str:
    values = dotenv_values(path)
    key = str(
        values.get("POLYGON_API_KEY")
        or values.get("MASSIVE_API_KEY")
        or ""
    ).strip()
    if not key or key.lower() in {
        "your_polygon_api_key_here",
        "replace_me",
    }:
        raise RuntimeError("P7 reference lookup credential is unavailable")
    return key


def _spots(
    *,
    allow_finalized_bar_fallback: bool,
) -> tuple[dict[str, Decimal], dict[str, dict[str, object]]]:
    dsn, _ = _dsn(allow_admin_read_only=True)
    with psycopg2.connect(dsn) as connection:
        connection.set_session(
            readonly=True,
            autocommit=False,
            isolation_level="REPEATABLE READ",
        )
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL statement_timeout = '15s'")
            cursor.execute(
                """
                WITH latest AS (
                    SELECT DISTINCT ON (revision.security_id)
                        revision.security_id,
                        revision.ticker
                    FROM ref.security_revisions AS revision
                    ORDER BY revision.security_id,
                             revision.effective_from DESC,
                             revision.observed_at DESC
                )
                SELECT
                    latest.ticker,
                    trade.price,
                    quote.bid,
                    quote.ask
                FROM latest
                JOIN rt.instruments AS instrument
                  ON instrument.security_id = latest.security_id
                 AND instrument.asset_class = 'EQUITY'
                LEFT JOIN rt.trade_current AS trade
                  ON trade.provider = 'massive'
                 AND trade.instrument_id = instrument.instrument_id
                LEFT JOIN rt.quote_current AS quote
                  ON quote.provider = 'massive'
                 AND quote.instrument_id = instrument.instrument_id
                WHERE latest.ticker = ANY(%s)
                ORDER BY latest.ticker
                """,
                (["SPY", "QQQ"],),
            )
            rows = tuple(cursor.fetchall())
        connection.rollback()
    result = {}
    evidence = {}
    for ticker, price, bid, ask in rows:
        values = [
            Decimal(str(value))
            for value in (price, bid, ask)
            if value is not None and Decimal(str(value)) > 0
        ]
        if price is not None and Decimal(str(price)) > 0:
            result[str(ticker)] = Decimal(str(price))
            evidence[str(ticker)] = {"source": "TRADE_CURRENT"}
        elif bid is not None and ask is not None and len(values) == 2:
            result[str(ticker)] = sum(values) / Decimal(2)
            evidence[str(ticker)] = {"source": "QUOTE_CURRENT_MIDPOINT"}
    missing = sorted(set(("SPY", "QQQ")) - set(result))
    if missing and allow_finalized_bar_fallback:
        dsn, _ = _dsn(allow_admin_read_only=True)
        with psycopg2.connect(dsn) as connection:
            connection.set_session(
                readonly=True,
                autocommit=False,
                isolation_level="REPEATABLE READ",
            )
            with connection.cursor() as cursor:
                cursor.execute("SET LOCAL statement_timeout = '15s'")
                cursor.execute(
                    """
                    SELECT DISTINCT ON (ticker)
                        ticker,
                        close_price,
                        interval,
                        bar_end,
                        bar_revision_id::text,
                        payload_sha256
                    FROM rt.bar_revisions
                    WHERE ticker = ANY(%s)
                      AND is_final
                      AND close_price > 0
                      AND bar_end <= %s
                    ORDER BY ticker,bar_end DESC,observed_at DESC
                    """,
                    (missing, datetime.now(UTC)),
                )
                bar_rows = tuple(cursor.fetchall())
            connection.rollback()
        for (
            ticker,
            close_price,
            interval,
            bar_end,
            revision_id,
            payload_sha256,
        ) in bar_rows:
            result[str(ticker)] = Decimal(str(close_price))
            evidence[str(ticker)] = {
                "source": "FINALIZED_BAR",
                "interval": str(interval),
                "bar_end": bar_end,
                "bar_revision_id": str(revision_id),
                "payload_sha256": str(payload_sha256),
            }
    if set(result) != {"SPY", "QQQ"}:
        raise RuntimeError("retained SPY/QQQ spot evidence is unavailable")
    return result, evidence


def _params(
    plan: dict[str, object],
    underlyer: str,
    spot: Decimal,
) -> dict[str, str]:
    values = dict(plan["request_scope"]["parameters"])
    values["underlying_ticker"] = underlyer
    values["strike_price.gte"] = str(
        (spot * Decimal("0.95")).quantize(Decimal("0.01"))
    )
    values["strike_price.lte"] = str(
        (spot * Decimal("1.05")).quantize(Decimal("0.01"))
    )
    return values


def _select(
    underlyer: str,
    spot: Decimal,
    rows: object,
) -> dict[str, object] | None:
    if not isinstance(rows, list):
        raise ValueError("provider contract results are not a list")
    candidates = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            ticker = str(row["ticker"])
            expiration = date.fromisoformat(str(row["expiration_date"]))
            strike = Decimal(str(row["strike_price"]))
            shares = int(row.get("shares_per_contract") or 100)
            contract_type = str(row["contract_type"]).lower()
        except (
            KeyError,
            InvalidOperation,
            TypeError,
            ValueError,
        ):
            continue
        match = _OPTION.fullmatch(ticker)
        if (
            match is None
            or match.group("underlyer") != underlyer
            or contract_type != "call"
            or shares != 100
            or expiration < date(2026, 10, 27)
            or expiration > date(2026, 11, 19)
            or strike <= 0
        ):
            continue
        candidates.append((
            expiration,
            abs(strike - spot),
            ticker,
            {
                "ticker": ticker,
                "underlyer": underlyer,
                "expiration_date": expiration.isoformat(),
                "strike_price": str(strike),
                "contract_type": contract_type,
                "shares_per_contract": shares,
                "primary_exchange": row.get("primary_exchange"),
                "cfi": row.get("cfi"),
            },
        ))
    return min(candidates, default=(None, None, None, None))[3]


def main(argv=None) -> int:
    args = parser().parse_args(argv)
    plan_path = args.plan.resolve()
    output = args.output.resolve()
    plan = _load_plan(plan_path)
    if (
        args.approval_reference != APPROVAL_REFERENCE
        or not args.execute
    ):
        raise RuntimeError("P7 reference lookup lacks exact execution approval")
    if args.allow_finalized_bar_fallback and (
        args.local_input_approval_reference
        != "ADVANCED-OPTIONS-P7-REFERENCE-SPOT-FALLBACK-20261020-01"
    ):
        raise RuntimeError(
            "P7 finalized-bar fallback lacks exact approval"
        )
    if output.exists():
        raise FileExistsError("P7 reference lookup output must be new")
    if not output.parent.is_dir():
        raise FileNotFoundError("P7 reference lookup output parent is absent")
    spots, spot_evidence = _spots(
        allow_finalized_bar_fallback=args.allow_finalized_bar_fallback,
    )
    maximum_per_request = int(
        plan["request_scope"]["maximum_response_bytes_per_request"]
    )
    maximum_total = int(
        plan["request_scope"]["maximum_total_response_bytes"]
    )
    transport = RequestsPolygonHttpTransport(
        _api_key(args.env_file.resolve()),
        timeout_seconds=20,
        maximum_response_bytes=maximum_per_request,
    )
    responses = []
    selected = {}
    total_bytes = 0
    for underlyer in plan["underlyings"]:
        params = _params(plan, underlyer, spots[underlyer])
        response = transport.get(
            REST_BASE_URL + plan["endpoint"],
            params,
        )
        total_bytes += len(response.body)
        if total_bytes > maximum_total:
            raise RuntimeError(
                "P7 reference lookup exceeded its total response byte ceiling"
            )
        try:
            payload = json.loads(response.body)
        except json.JSONDecodeError as error:
            raise RuntimeError(
                "P7 reference lookup returned invalid JSON"
            ) from error
        if response.status_code != 200 or not isinstance(payload, dict):
            raise RuntimeError(
                f"P7 reference lookup failed with status {response.status_code}"
            )
        if payload.get("next_url"):
            pagination_ignored = True
        else:
            pagination_ignored = False
        choice = _select(
            underlyer,
            spots[underlyer],
            payload.get("results"),
        )
        if choice is not None:
            selected[underlyer] = choice
        responses.append({
            "underlyer": underlyer,
            "parameters": params,
            "status_code": response.status_code,
            "response_bytes": len(response.body),
            "payload_sha256": hashlib.sha256(response.body).hexdigest(),
            "pagination_ignored": pagination_ignored,
            "payload": payload,
        })
    status = "READY" if set(selected) == {"SPY", "QQQ"} else "BLOCKED"
    evidence = {
        "version": "advanced-options-p7-reference-lookup-evidence-v1",
        "status": status,
        "plan_sha256": hashlib.sha256(plan_path.read_bytes()).hexdigest(),
        "approval_reference": args.approval_reference,
        "endpoint": plan["endpoint"],
        "request_count": len(responses),
        "response_bytes": total_bytes,
        "pagination_requests": 0,
        "websocket_connections": 0,
        "database_writes": 0,
        "retained_spots": {
            key: {
                "value": str(value),
                **spot_evidence[key],
            }
            for key, value in spots.items()
        },
        "selected": selected,
        "responses": responses,
        "blockers": (
            []
            if status == "READY"
            else ["EXACT_REFERENCE_CONTRACT_UNAVAILABLE"]
        ),
    }
    body = (
        json.dumps(evidence, indent=2, sort_keys=True, default=str) + "\n"
    ).encode("utf-8")
    with output.open("xb") as handle:
        handle.write(body)
    print(json.dumps({
        "status": status,
        "output": str(output),
        "sha256": hashlib.sha256(body).hexdigest(),
        "request_count": len(responses),
        "response_bytes": total_bytes,
        "selected": {
            key: value["ticker"] for key, value in selected.items()
        },
        "websocket_connections": 0,
        "database_writes": 0,
    }, indent=2, sort_keys=True))
    return 0 if status == "READY" else 2


if __name__ == "__main__":
    raise SystemExit(main())
