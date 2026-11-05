"""P4C stored Advanced Options projections and inert publisher handoff."""
from __future__ import annotations

from collections import Counter
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
import re
from typing import Mapping
from uuid import NAMESPACE_URL, UUID, uuid5

from .advanced_options_control_plane import (
    CANONICAL_PUBLISHER_OWNER,
    AdvancedOptionsControlPlanePlan,
)
from .advanced_options_cycle import AdvancedOptionsCycleResult
from .contracts import canonical_sha256
from .orchestration_repository import (
    OPTIONS_CYCLE,
    OPTIONS_PORTAL_READY,
    CycleReceiptSpec,
    build_options_receipt_spec,
)
from .portal_projection import (
    RealtimePortalSnapshot,
    build_portal_snapshot,
)
from .portal_projection_repository import (
    PortalReleaseReceipt,
    persist_portal_snapshot_cursor,
)
from .preflight import DetectorFamily, ProviderTier


OPTIONS_PROJECTION_TYPES = (
    "OPTIONS_FLOW",
    "OPTIONS_SCREENER",
    "OPTIONS_ALERTS",
    "OPTIONS_MARKS",
)
OPTIONS_PAGE_SNAPSHOT_TYPES = {
    "FLOW": "OPTIONS_FLOW_PAGE_V2",
    "SCREENER": "OPTIONS_SCREENER_PAGE_V2",
    "ALERTS": "OPTIONS_ALERTS_PAGE_V2",
}
OPTIONS_FULL_PAGE_ARTIFACT_KINDS = tuple(OPTIONS_PAGE_SNAPSHOT_TYPES)
OPTIONS_SNAPSHOT_TYPES = (
    *OPTIONS_PROJECTION_TYPES,
    *OPTIONS_PAGE_SNAPSHOT_TYPES.values(),
)
OPTIONS_PAGE_CONTRACTS = {
    "FLOW": "advanced-options-flow-page-v2",
    "SCREENER": "advanced-options-screener-page-v2",
    "ALERTS": "advanced-options-alerts-page-v2",
}
P8_DISPOSABLE_DATABASE_PREFIX = "test_realtime_v2_p8_"
P8_DISPOSABLE_SCOPE_REFERENCE = "OWNED_DISPOSABLE_P8_CANARY"
ADVANCED_OPTIONS_PAGE_FLOW_EVENT_LIMIT = 5_000
_OPTION_TICKER = re.compile(
    r"^O:(?P<underlyer>[A-Z0-9.-]+)"
    r"(?P<expiration>\d{6})(?P<call_put>[CP])"
    r"(?P<strike>\d{8})$"
)


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _json_value(value):
    if isinstance(value, Mapping):
        return {
            str(key): _json_value(item)
            for key, item in value.items()
        }
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    raise TypeError(
        "Advanced Options page payload contains an unsupported "
        f"{type(value).__name__}"
    )


def _number(value, default=0):
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _integer(value, default=0):
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _asset_type(underlyer: str) -> str:
    return (
        "ETF"
        if underlyer in {"DIA", "IWM", "QQQ", "SPY"}
        else "STOCK"
    )


def _evaluation_legs(evaluation) -> tuple[dict[str, object], ...]:
    payload = evaluation.payload
    values = payload.get("legs") if isinstance(payload, Mapping) else None
    if not isinstance(values, (tuple, list)):
        return ()
    rows = []
    for value in values:
        if not isinstance(value, Mapping):
            continue
        row = _json_value(value)
        if (
            row.get("snapshot_id") is None
            or row.get("contract_id") is None
            or row.get("contract_ticker") is None
        ):
            continue
        rows.append(row)
    return tuple(rows)


def _flow_contract_row(leg: Mapping[str, object], cutoff: datetime):
    expiration = str(leg.get("expiration_date") or cutoff.date())
    try:
        calendar_dte = max(
            0,
            (date.fromisoformat(expiration) - cutoff.date()).days,
        )
    except ValueError:
        calendar_dte = 0
    volume = _integer(leg.get("day_volume"))
    open_interest = leg.get("open_interest")
    model_mark = leg.get("model_mark")
    multiplier = _integer(leg.get("multiplier"), 100)
    premium = (
        None
        if model_mark is None
        else str(Decimal(str(model_mark)) * volume * multiplier)
    )
    return {
        "contract_id": int(leg["contract_id"]),
        "contract_ticker": str(leg["contract_ticker"]),
        "contract_type": str(leg.get("contract_type") or "CALL"),
        "expiration_date": expiration,
        "calendar_dte": calendar_dte,
        "strike": str(leg.get("strike") or "0"),
        "spot": str(leg.get("spot") or "0"),
        "day_volume": volume,
        "open_interest": (
            None if open_interest is None else _integer(open_interest)
        ),
        "model_mark": (
            None if model_mark is None else str(model_mark)
        ),
        "display_mark": (
            None if model_mark is None else str(model_mark)
        ),
        "local_iv": (
            None
            if leg.get("local_iv") is None
            else _number(leg.get("local_iv"))
        ),
        "local_delta": (
            None
            if leg.get("local_delta") is None
            else _number(leg.get("local_delta"))
        ),
        "volume_open_interest_ratio": (
            None
            if not open_interest
            else volume / _integer(open_interest)
        ),
        "premium_activity": premium,
        "moneyness_fraction": str(
            leg.get("moneyness_fraction") or "0"
        ),
    }


def _flow_event_contract_rows(
    events: tuple[Mapping[str, object], ...],
    cutoff: datetime,
) -> tuple[
    dict[str, tuple[dict[str, object], ...]],
    dict[str, tuple[datetime, ...]],
    dict[str, int],
]:
    grouped: dict[tuple[str, int], dict[str, object]] = {}
    observed: dict[str, list[datetime]] = {}
    event_counts: Counter = Counter()
    for event in events:
        underlyer = str(event["underlyer"])
        ticker = str(event["contract_ticker"])
        match = _OPTION_TICKER.fullmatch(ticker)
        if match is None or match.group("underlyer") != underlyer:
            raise ValueError(
                "Advanced Options page flow contract identity changed"
            )
        event_time = event["event_time"]
        if not isinstance(event_time, datetime):
            event_time = datetime.fromisoformat(str(event_time))
        event_time = _utc(event_time, "flow event time")
        contract_id = int(event["contract_id"])
        key = underlyer, contract_id
        expiration = datetime.strptime(
            match.group("expiration"),
            "%y%m%d",
        ).date()
        strike = (
            Decimal(match.group("strike")) / Decimal("1000")
        )
        row = grouped.setdefault(key, {
            "contract_id": contract_id,
            "contract_ticker": ticker,
            "contract_type": (
                "CALL" if match.group("call_put") == "C" else "PUT"
            ),
            "expiration_date": expiration.isoformat(),
            "calendar_dte": max(0, (expiration - cutoff.date()).days),
            "strike": str(strike),
            "spot": "0",
            "day_volume": 0,
            "open_interest": (
                None
                if event["prior_open_interest"] is None
                else int(event["prior_open_interest"])
            ),
            "model_mark": str(event["price"]),
            "display_mark": str(event["price"]),
            "local_iv": None,
            "local_delta": None,
            "volume_open_interest_ratio": None,
            "premium_activity": "0",
            "moneyness_fraction": "0",
            "market_data_time": event_time.isoformat(),
        })
        row["day_volume"] += int(event["size"])
        row["premium_activity"] = str(
            Decimal(str(row["premium_activity"]))
            + Decimal(str(event["premium"]))
        )
        if event_time.isoformat() > row["market_data_time"]:
            row["market_data_time"] = event_time.isoformat()
            row["model_mark"] = str(event["price"])
            row["display_mark"] = str(event["price"])
        observed.setdefault(underlyer, []).append(event_time)
        event_counts[underlyer] += 1
    by_underlyer: dict[str, list[dict[str, object]]] = {}
    for (underlyer, _contract_id), row in grouped.items():
        open_interest = row["open_interest"]
        row["volume_open_interest_ratio"] = (
            None
            if not open_interest
            else row["day_volume"] / open_interest
        )
        by_underlyer.setdefault(underlyer, []).append(row)
    return (
        {
            underlyer: tuple(
                sorted(rows, key=lambda row: row["contract_id"])
            )
            for underlyer, rows in by_underlyer.items()
        },
        {
            underlyer: tuple(rows)
            for underlyer, rows in observed.items()
        },
        dict(event_counts),
    )


def _eligible_chain_row(
    evaluation,
    leg: Mapping[str, object],
    cutoff: datetime,
) -> dict[str, object]:
    flow = _flow_contract_row(leg, cutoff)
    market_time = str(
        leg.get("source_market_time")
        or evaluation.observed_at.isoformat()
    )
    model_mark = flow["model_mark"] or "0"
    local_delta = _number(leg.get("local_delta"))
    spot = str(leg.get("spot") or "0")
    strike = flow["strike"]
    spot_value = Decimal(spot)
    strike_value = Decimal(strike)
    return {
        "snapshot_id": str(leg["snapshot_id"]),
        "contract_id": flow["contract_id"],
        "contract_ticker": flow["contract_ticker"],
        "underlying": evaluation.underlyer,
        "contract_type": flow["contract_type"],
        "expiration_date": flow["expiration_date"],
        "strike": strike,
        "calendar_dte": flow["calendar_dte"],
        "spot": spot,
        "model_mark": model_mark,
        "mark": model_mark,
        "absolute_delta": abs(local_delta),
        "local_delta": local_delta,
        "local_iv": _number(leg.get("local_iv")),
        "local_gamma": _number(leg.get("local_gamma")),
        "local_theta_per_day": _number(
            leg.get("local_theta_per_day")
        ),
        "local_vega_per_vol_point": _number(
            leg.get("local_vega_per_vol_point")
        ),
        "local_rho_per_rate_point": _number(
            leg.get("local_rho_per_rate_point")
        ),
        "bid": (
            None if leg.get("bid") is None else str(leg.get("bid"))
        ),
        "ask": (
            None if leg.get("ask") is None else str(leg.get("ask"))
        ),
        "mark_market_data_time": market_time,
        "day_volume": (
            None
            if leg.get("day_volume") is None
            else _integer(leg.get("day_volume"))
        ),
        "open_interest": (
            None
            if leg.get("open_interest") is None
            else _integer(leg.get("open_interest"))
        ),
        "volume_open_interest_ratio": (
            flow["volume_open_interest_ratio"]
        ),
        "otm_fraction": (
            "0"
            if spot_value == 0
            else str(abs(strike_value - spot_value) / spot_value)
        ),
        "market_data_time": market_time,
        "first_observed_at": evaluation.observed_at.isoformat(),
        "mark_source": str(
            leg.get("mark_source") or "ADVANCED_CANONICAL_EVIDENCE"
        ),
        "quality_flags": list(leg.get("quality_flags") or ()),
        "asset_type": _asset_type(evaluation.underlyer),
        "batch_id": str(evaluation.dataset_id),
        "matrix_id": (
            str(evaluation.package.matrix_id)
            if evaluation.package is not None
            else str(evaluation.dataset_id)
        ),
        "display_mark": model_mark,
        "time_to_expiration_years": leg.get(
            "time_to_expiration_years"
        ),
        "risk_free_rate": leg.get("risk_free_rate"),
        "dividend_yield": leg.get("dividend_yield"),
        "model_version": leg.get("model_version"),
        "valuation_policy_version": leg.get(
            "valuation_policy_version"
        ),
        "valuation_policy_sha256": leg.get(
            "valuation_policy_sha256"
        ),
    }


def _flow_response(
    *,
    underlyer: str,
    underlyers: tuple[str, ...],
    evaluations,
    dataset_id: UUID,
    flow_events: tuple[Mapping[str, object], ...],
    cutoff: datetime,
    policy_sha256: str,
    configuration_sha256: str,
    session_date: date,
) -> dict[str, object]:
    event_rows, event_times, event_counts = _flow_event_contract_rows(
        flow_events,
        cutoff,
    )
    rows_by_underlyer: dict[str, list[dict[str, object]]] = {
        symbol: list(event_rows.get(symbol, ()))
        for symbol in underlyers
    }
    observed: dict[str, list[datetime]] = {
        symbol: list(event_times.get(symbol, ()))
        for symbol in underlyers
    }
    matrix_ids: dict[str, set[str]] = {
        symbol: set() for symbol in underlyers
    }
    for evaluation in evaluations:
        observed[evaluation.underlyer].append(evaluation.observed_at)
        if evaluation.package is not None:
            matrix_ids[evaluation.underlyer].add(
                str(evaluation.package.matrix_id)
            )
        for leg in _evaluation_legs(evaluation):
            projected = _flow_contract_row(leg, cutoff)
            existing = next((
                row
                for row in rows_by_underlyer[evaluation.underlyer]
                if row["contract_id"] == projected["contract_id"]
            ), None)
            if existing is None:
                rows_by_underlyer[evaluation.underlyer].append(projected)
            else:
                existing.update({
                    key: value
                    for key, value in projected.items()
                    if value is not None
                    and not (
                        key in {"day_volume", "premium_activity"}
                        and not value
                    )
                })
    summaries = []
    for symbol in underlyers:
        contracts = {
            row["contract_id"]: row
            for row in rows_by_underlyer[symbol]
        }
        rows = tuple(contracts.values())
        call_rows = tuple(
            row for row in rows if row["contract_type"] == "CALL"
        )
        put_rows = tuple(
            row for row in rows if row["contract_type"] == "PUT"
        )
        call_volume = sum(row["day_volume"] for row in call_rows)
        put_volume = sum(row["day_volume"] for row in put_rows)
        call_oi = sum(row["open_interest"] or 0 for row in call_rows)
        put_oi = sum(row["open_interest"] or 0 for row in put_rows)
        call_premium = sum(
            Decimal(row["premium_activity"] or "0")
            for row in call_rows
        )
        put_premium = sum(
            Decimal(row["premium_activity"] or "0")
            for row in put_rows
        )
        source_times = observed[symbol]
        summaries.append({
            "underlying": symbol,
            "asset_type": _asset_type(symbol),
            "batch_id": str(dataset_id),
            "matrix_id": (
                sorted(matrix_ids[symbol])[0]
                if matrix_ids[symbol]
                else str(dataset_id)
            ),
            "scheduled_cycle": cutoff.isoformat(),
            "market_data_time": (
                max(source_times).isoformat()
                if source_times
                else cutoff.isoformat()
            ),
            "first_observed_at": (
                min(source_times).isoformat()
                if source_times
                else cutoff.isoformat()
            ),
            "received_row_count": (
                event_counts.get(symbol, len(rows))
            ),
            "retained_row_count": len(rows),
            "retention_fraction": 1,
            "model_version": "advanced-options-v2",
            "spot": rows[0]["spot"] if rows else "0",
            "contract_count": len(rows),
            "expiration_count": len({
                row["expiration_date"] for row in rows
            }),
            "strike_count": len({row["strike"] for row in rows}),
            "call_contract_count": len(call_rows),
            "put_contract_count": len(put_rows),
            "call_volume": call_volume,
            "put_volume": put_volume,
            "total_volume": call_volume + put_volume,
            "call_open_interest": call_oi,
            "put_open_interest": put_oi,
            "total_open_interest": call_oi + put_oi,
            "call_premium_activity": str(call_premium),
            "put_premium_activity": str(put_premium),
            "total_premium_activity": str(call_premium + put_premium),
            "premium_activity_contract_count": sum(
                row["premium_activity"] is not None for row in rows
            ),
            "put_call_volume_ratio": (
                None if call_volume == 0 else put_volume / call_volume
            ),
            "put_call_open_interest_ratio": (
                None if call_oi == 0 else put_oi / call_oi
            ),
            "open_interest_change": None,
        })
    selected_rows = tuple({
        row["contract_id"]: row
        for row in rows_by_underlyer[underlyer]
    }.values())
    expiration_groups: dict[str, list[dict[str, object]]] = {}
    strike_groups: dict[tuple[str, str], list[dict[str, object]]] = {}
    for row in selected_rows:
        expiration_groups.setdefault(
            row["expiration_date"], []
        ).append(row)
        strike_groups.setdefault(
            (row["expiration_date"], row["strike"]), []
        ).append(row)
    expirations = []
    for expiration, rows in sorted(expiration_groups.items()):
        calls = tuple(
            row for row in rows if row["contract_type"] == "CALL"
        )
        puts = tuple(
            row for row in rows if row["contract_type"] == "PUT"
        )
        expirations.append({
            "expiration_date": expiration,
            "calendar_dte": rows[0]["calendar_dte"],
            "contract_count": len(rows),
            "call_volume": sum(row["day_volume"] for row in calls),
            "put_volume": sum(row["day_volume"] for row in puts),
            "call_open_interest": sum(
                row["open_interest"] or 0 for row in calls
            ),
            "put_open_interest": sum(
                row["open_interest"] or 0 for row in puts
            ),
            "call_premium_activity": str(sum(
                Decimal(row["premium_activity"] or "0")
                for row in calls
            )),
            "put_premium_activity": str(sum(
                Decimal(row["premium_activity"] or "0")
                for row in puts
            )),
        })
    strikes = []
    for (expiration, strike), rows in sorted(strike_groups.items()):
        calls = tuple(
            row for row in rows if row["contract_type"] == "CALL"
        )
        puts = tuple(
            row for row in rows if row["contract_type"] == "PUT"
        )
        strikes.append({
            "expiration_date": expiration,
            "strike": strike,
            "contract_count": len(rows),
            "call_volume": sum(row["day_volume"] for row in calls),
            "put_volume": sum(row["day_volume"] for row in puts),
            "call_open_interest": sum(
                row["open_interest"] or 0 for row in calls
            ),
            "put_open_interest": sum(
                row["open_interest"] or 0 for row in puts
            ),
        })
    selected_summary = next(
        row for row in summaries if row["underlying"] == underlyer
    )
    return {
        "available": True,
        "reason": None,
        "data_tier": "REAL-TIME ADVANCED RESEARCH DATA",
        "generated_at": cutoff.isoformat(),
        "as_of": cutoff.isoformat(),
        "observed_at": selected_summary["first_observed_at"],
        "policy_sha256": policy_sha256,
        "model_version": "advanced-options-v2",
        "data": {
            "serving_mode": "CURRENT_POLICY",
            "active_policy_sha256": policy_sha256,
            "active_configuration_sha256": configuration_sha256,
            "underlyers": summaries,
            "selected": underlyer,
            "session_date": session_date.isoformat(),
            "selected_summary": selected_summary,
            "expirations": expirations,
            "strikes": strikes,
            "top_contracts": sorted(
                selected_rows,
                key=lambda row: (
                    -Decimal(row["premium_activity"] or "0"),
                    row["contract_id"],
                ),
            )[:25],
            "directional_flow_available": False,
            "quote_liquidity": "NOT_AVAILABLE",
            "trade_tape_scope": (
                "EXCLUDED_FROM_TOTALS_WATCHLIST_BIASED"
            ),
            "definitions": {
                "source": (
                    "Canonical Advanced detector/package evidence sealed "
                    "during OPTIONS_PROJECTION."
                )
            },
        },
    }


def _alert_evaluation_row(evaluation, cutoff: datetime):
    direction = evaluation.payload.get("direction")
    if direction not in (-1, 1):
        decision = evaluation.payload.get("decision")
        if isinstance(decision, Mapping):
            direction = decision.get("direction")
    if direction not in (-1, 1):
        direction = None
    detector = evaluation.detector.value
    return {
        "evaluation_id": str(evaluation.evaluation_id),
        "candidate_id": (
            str(evaluation.package.candidate_id)
            if evaluation.package is not None
            else None
        ),
        "run_id": str(evaluation.dataset_id),
        "scheduled_cycle": cutoff.isoformat(),
        "detector_id": detector,
        "origin": (
            "OPTIONS_FIRST"
            if evaluation.detector in {
                DetectorFamily.O1,
                DetectorFamily.O2,
                DetectorFamily.O3,
            }
            else "STOCK_FIRST"
        ),
        "underlyer": evaluation.underlyer,
        "direction": direction,
        "category": detector,
        "strategy_name": detector,
        "candidate_rank": None,
        "selection_status": evaluation.selection_status,
        "selection_reason": evaluation.selection_status,
        "plan_sha256": None,
        "selected_at": evaluation.observed_at.isoformat(),
        "entry_deadline": (
            evaluation.package.exit_deadline.isoformat()
            if evaluation.package is not None
            and evaluation.package.exit_deadline is not None
            else None
        ),
        "exit_deadline": (
            evaluation.package.exit_deadline.isoformat()
            if evaluation.package is not None
            and evaluation.package.exit_deadline is not None
            else None
        ),
        "entry_limit": (
            str(evaluation.package.entry_net_premium)
            if evaluation.package is not None
            else None
        ),
        "outcome_status": "NOT_BOUND_TO_PROSPECTIVE_OUTCOMES",
        "net_return": None,
        "event_horizon_status": None,
        "shared_model_exposure": False,
        "observation": (
            None
            if evaluation.package is not None
            and evaluation.detector in {
                DetectorFamily.OS1,
                DetectorFamily.OS2,
            }
            else _json_value(evaluation.payload)
        ),
    }


def _sealed_current_mark(
    mark: Mapping[str, object] | None,
    *,
    package,
    cutoff: datetime,
):
    if mark is None:
        return {
            "status": "UNAVAILABLE",
            "reason": "CURRENT_MARK_NOT_SEALED_IN_PAGE_ARTIFACT",
            "market_time": None,
            "observed_time": None,
            "age_seconds": None,
            "signed_package_mark": None,
            "package_price": None,
            "gross_pnl": None,
            "price_return": None,
            "estimated_cost": None,
            "net_pnl": None,
            "net_return": None,
            "basis": "PERSISTED_EVIDENCE_ONLY",
            "price_return_basis": "UNAVAILABLE",
            "net_return_basis": "UNAVAILABLE",
            "slippage": "UNAVAILABLE",
            "execution_permission": False,
        }
    market_value = mark["market_time"]
    observed_value = mark["observed_time"]
    market_time = _utc(
        (
            datetime.fromisoformat(market_value)
            if isinstance(market_value, str)
            else market_value
        ),
        "mark market_time",
    )
    observed_time = _utc(
        (
            datetime.fromisoformat(observed_value)
            if isinstance(observed_value, str)
            else observed_value
        ),
        "mark observed_time",
    )
    gross_pnl = Decimal(str(mark["gross_pnl"]))
    capital_at_risk = Decimal(str(mark["capital_at_risk"]))
    return {
        "status": "FRESH",
        "reason": None,
        "market_time": market_time.isoformat(),
        "observed_time": observed_time.isoformat(),
        "oldest_leg_market_time": market_time.isoformat(),
        "age_seconds": max(
            0,
            int((cutoff - observed_time).total_seconds()),
        ),
        "signed_package_mark": str(mark["exit_net_premium"]),
        "package_price": str(abs(Decimal(str(
            mark["exit_net_premium"]
        )))),
        "gross_pnl": str(gross_pnl),
        "price_return": str(gross_pnl / capital_at_risk),
        "estimated_cost": str(mark["estimated_cost"]),
        "net_pnl": str(mark["net_pnl"]),
        "net_return": str(mark["net_return"]),
        "basis": str(mark["availability_flag"]),
        "price_return_basis": "GROSS_PNL_OVER_CAPITAL_AT_RISK",
        "net_return_basis": "PERSISTED_PACKAGE_MARK",
        "valuation_policy_sha256": str(
            mark["valuation_policy_sha256"]
        ),
        "source_snapshot_ids": list(
            mark.get("source_snapshot_ids") or ()
        ),
        "after_exit_deadline": (
            package.exit_deadline is not None
            and market_time > package.exit_deadline
        ),
        "slippage": "UNAVAILABLE",
        "execution_permission": False,
    }


def _alert_run_row(
    evaluation,
    cutoff: datetime,
    current_marks: Mapping[str, Mapping[str, object]],
):
    row = _alert_evaluation_row(evaluation, cutoff)
    package = evaluation.package
    if package is None:
        return None
    direction = row["direction"]
    if direction not in (-1, 1):
        direction = 1
    return {
        "evaluation_id": row["evaluation_id"],
        "candidate_id": row["candidate_id"],
        "matrix_id": str(package.matrix_id),
        "run_id": row["run_id"],
        "scheduled_cycle": row["scheduled_cycle"],
        "triggered_at": evaluation.observed_at.isoformat(),
        "detector_id": row["detector_id"],
        "origin": row["origin"],
        "underlyer": row["underlyer"],
        "direction": direction,
        "category": row["category"],
        "strategy_name": row["strategy_name"],
        "candidate_rank": row["candidate_rank"],
        "first_selected_at": evaluation.observed_at.isoformat(),
        "hit_count": 1,
        "repeat_count": 0,
        "confirmation_timeframes": [],
        "last_seen_at": evaluation.observed_at.isoformat(),
        "plan_sha256": None,
        "management_status": (
            str(current_marks[str(package.candidate_id)][
                "management_status"
            ])
            if str(package.candidate_id) in current_marks
            and current_marks[str(package.candidate_id)][
                "management_status"
            ] is not None
            else "UNAVAILABLE"
        ),
        "entry_limit": str(package.entry_net_premium),
        "entry_deadline": row["entry_deadline"],
        "exit_deadline": row["exit_deadline"],
        "management_policy": {},
        "event_horizon_status": None,
        "original_package": {
            "status": "AVAILABLE",
            "basis": "ORIGINAL_CANDIDATE_SNAPSHOTS",
            "structure_type": None,
            "expiration_date": (
                max(leg.expiration_date for leg in package.legs).isoformat()
            ),
            "calendar_dte": None,
            "net_premium": str(package.entry_net_premium),
            "capital_at_risk": str(package.capital_at_risk),
            "maximum_loss": str(package.capital_at_risk),
            "maximum_profit": None,
            "breakevens": [],
            "market_data_time": package.entry_market_time.isoformat(),
            "observed_time": evaluation.observed_at.isoformat(),
            "legs": [
                {
                    "leg_index": leg.leg_index,
                    "contract_id": leg.contract_id,
                    "contract_ticker": leg.contract_ticker,
                    "side": leg.side.value,
                    "ratio": leg.ratio,
                    "multiplier": leg.multiplier,
                    "expiration_date": leg.expiration_date.isoformat(),
                    "strike": str(leg.strike),
                    "contract_type": leg.contract_type.value,
                    "model_mark": str(leg.entry_mark),
                    "local_iv": None,
                    "local_delta": None,
                    "local_gamma": None,
                    "source_market_time": (
                        leg.entry_market_time.isoformat()
                    ),
                    "mark_source": "ADVANCED_CANONICAL_PACKAGE",
                    "quality_flags": [],
                    "quote_bid": None,
                    "quote_ask": None,
                    "quote_midpoint": None,
                    "quote_spread_midpoint": None,
                }
                for leg in package.legs
            ],
        },
        "current_mark": _sealed_current_mark(
            current_marks.get(str(package.candidate_id)),
            package=package,
            cutoff=cutoff,
        ),
        "observation": _json_value(evaluation.payload),
        "outcome_status": "NOT_BOUND_TO_PROSPECTIVE_OUTCOMES",
        "net_return": None,
        "fill": None,
    }


def _candidate_detail(evaluation, policy_sha256: str):
    package = evaluation.package
    if package is None:
        return None
    legs = [
        {
            "leg_index": leg.leg_index,
            "contract_id": leg.contract_id,
            "contract_ticker": leg.contract_ticker,
            "side": leg.side.value,
            "ratio": leg.ratio,
            "multiplier": leg.multiplier,
            "expiration_date": leg.expiration_date.isoformat(),
            "strike": str(leg.strike),
            "contract_type": leg.contract_type.value,
            "model_mark": str(leg.entry_mark),
            "local_iv": None,
            "local_delta": None,
            "local_gamma": None,
            "source_market_time": leg.entry_market_time.isoformat(),
            "mark_source": "ADVANCED_CANONICAL_PACKAGE",
            "quality_flags": [],
            "quote_bid": None,
            "quote_ask": None,
            "quote_midpoint": None,
            "quote_spread_midpoint": None,
            "valuation_policy_sha256": (
                leg.valuation_policy_sha256
            ),
        }
        for leg in package.legs
    ]
    expiration = max(leg.expiration_date for leg in package.legs)
    candidate_kind = (
        "SINGLE_CONTRACT"
        if len(package.legs) == 1
        else "MULTI_LEG"
    )
    candidate_id = str(package.candidate_id)
    return {
        "candidate": {
            "candidate_id": candidate_id,
            "candidate_identity": package.candidate_identity_sha256,
            "candidate_identity_sha256": (
                package.candidate_identity_sha256
            ),
            "matrix_id": str(package.matrix_id),
            "strategy_name": evaluation.detector.value,
            "strategy_version": "advanced-options-v2",
            "display_name": (
                f"{evaluation.detector.value} {evaluation.underlyer}"
            ),
            "underlying": evaluation.underlyer,
            "candidate_kind": candidate_kind,
            "strategy_archetype": evaluation.detector.value,
            "persona_tags": [],
            "structure_type": "ADVANCED_CANONICAL_PACKAGE",
            "structure_risk_class": "DEFINED_RISK",
            "expiration_date": expiration.isoformat(),
            "candidate_rank": 1,
            "status": "SELECTED",
            "primary_metric_name": None,
            "primary_metric_value": None,
            "rank_components": {},
            "primary_evidence": _json_value(evaluation.payload),
            "net_premium": str(package.entry_net_premium),
            "collateral_required": str(package.capital_at_risk),
            "capital_at_risk": str(package.capital_at_risk),
            "maximum_profit": None,
            "maximum_loss": str(package.capital_at_risk),
            "return_on_collateral": None,
            "return_on_risk": None,
            "breakevens": [],
            "execution_eligibility": None,
            "reason_codes": [],
            "management_policy_version": None,
            "management_policy": {},
            "policy_sha256": policy_sha256,
            "model_version": "advanced-options-v2",
            "market_data_time": package.entry_market_time.isoformat(),
            "observed_time": evaluation.observed_at.isoformat(),
            "valid_until": (
                package.exit_deadline.isoformat()
                if package.exit_deadline is not None
                else None
            ),
            "presentation_metadata": {},
            "source_contract_id": package.legs[0].contract_id,
            "source_contract_ticker": (
                package.legs[0].contract_ticker
            ),
            "context_status": "PERSISTED_EVIDENCE_ONLY",
            "trend_state": None,
            "earnings_blackout_state": None,
            "fed_blackout_state": None,
            "quote_spread_state": None,
            "context_reason_codes": [],
            "normalized_legs": deepcopy(legs),
            "decision_context": _json_value(evaluation.payload),
            "evidence_rank_components": {},
            "trigger_values": {},
            "evidence_quality_flags": [],
            "signal_id": str(evaluation.event_id),
            "signal_status": evaluation.selection_status,
            "signal_blocked_reasons": [],
        },
        "legs": legs,
        "scenarios": [],
        "execution_gates": [],
        "market_event_evidence": [],
        "event_coverage_evidence": [],
        "research_evidence": None,
        "quote_liquidity": "NOT_AVAILABLE",
        "execution_mode": "READ_ONLY_RESEARCH",
    }


def _run_summary(cycle, cutoff: datetime):
    evaluations = cycle.evaluations
    selected = sum(
        row.selection_status == "SELECTED" for row in evaluations
    )
    statuses = Counter(row.selection_status for row in evaluations)
    return {
        "run_id": str(cycle.dataset_id),
        "scheduled_cycle": cutoff.isoformat(),
        "selected_at": cutoff.isoformat(),
        "published_at": cutoff.isoformat(),
        "market_time": cutoff.isoformat(),
        "observed_time": max(
            (row.observed_at for row in evaluations),
            default=cutoff,
        ).isoformat(),
        "expected_underlyings": len({
            row.underlyer for row in evaluations
        }),
        "covered_underlyings": len({
            row.underlyer for row in evaluations
        }),
        "coverage_status": "COMPLETE",
        "unavailable_underlyers": {},
        "selection_counts": dict(statuses),
        "rejections": {},
        "selected": selected,
    }


def _build_advanced_options_page_payloads(
    *,
    control_plane: AdvancedOptionsControlPlanePlan,
    cycle: AdvancedOptionsCycleResult,
    generated_at: datetime,
    page_source: AdvancedOptionsPageSource,
) -> tuple[dict[str, object], ...]:
    if not isinstance(page_source, AdvancedOptionsPageSource):
        raise TypeError("Advanced Options page source is invalid")
    underlyers = control_plane.universe.option_underlyings
    if (
        not underlyers
        or {
            row.underlyer for row in cycle.evaluations
        } - set(underlyers)
    ):
        raise ValueError(
            "Advanced Options full-page evidence is incomplete"
        )
    closure = cycle.input_gate.closure
    configuration_sha256 = (
        closure.cycle_input.configuration_sha256
    )
    policy_sha256 = control_plane.manifest_policy.sha256
    published = generated_at.isoformat()
    valid_until = closure.reference_valid_until.isoformat()
    common_identity = {
        "cohort_id": str(control_plane.cohort_id),
        "provider_tier": control_plane.binding.provider_tier.value,
        "cycle_sha256": cycle.cycle_sha256,
        "dataset_id": str(cycle.dataset_id),
        "published_at": published,
        "valid_until": valid_until,
    }
    scope_sha256 = canonical_sha256({
        "contract": "advanced-options-page-scope-v2",
        **common_identity,
        "manifest_sha256": control_plane.manifest_sha256,
        "configuration_sha256": configuration_sha256,
        "policy_sha256": policy_sha256,
        "page_source_sha256": page_source.sha256,
        "evaluations": tuple(
            (row.evaluation_id, row.payload_sha256)
            for row in cycle.evaluations
        ),
    })
    flow = {
        "contract": OPTIONS_PAGE_CONTRACTS["FLOW"],
        "artifact_kind": "FLOW",
        **common_identity,
        "scope_sha256": scope_sha256,
        "responses": {
            underlyer: _flow_response(
                underlyer=underlyer,
                underlyers=underlyers,
                evaluations=cycle.evaluations,
                dataset_id=cycle.dataset_id,
                flow_events=page_source.flow_events,
                cutoff=generated_at,
                policy_sha256=policy_sha256,
                configuration_sha256=configuration_sha256,
                session_date=control_plane.session_date,
            )
            for underlyer in underlyers
        },
    }
    screener_rows = []
    seen_snapshots = set()
    coverage = []
    for underlyer in underlyers:
        members = []
        source_evaluations = tuple(
            row for row in cycle.evaluations
            if row.underlyer == underlyer
        )
        for evaluation in source_evaluations:
            for leg in _evaluation_legs(evaluation):
                snapshot_id = str(leg["snapshot_id"])
                if snapshot_id in seen_snapshots:
                    continue
                seen_snapshots.add(snapshot_id)
                row = _eligible_chain_row(
                    evaluation,
                    leg,
                    generated_at,
                )
                screener_rows.append(row)
                members.append(row)
        coverage.append({
            "underlying": underlyer,
            "batch_id": str(cycle.dataset_id),
            "matrix_id": (
                members[0]["matrix_id"]
                if members
                else str(cycle.dataset_id)
            ),
            "market_time": (
                max(
                    row.observed_at for row in source_evaluations
                ).isoformat()
                if source_evaluations
                else published
            ),
            "observed_time": (
                max(
                    row.observed_at for row in source_evaluations
                ).isoformat()
                if source_evaluations
                else published
            ),
            "received": len(members),
            "retained": len(members),
            "eligible": len(members),
        })
    screener = {
        "contract": OPTIONS_PAGE_CONTRACTS["SCREENER"],
        "artifact_kind": "SCREENER",
        **common_identity,
        "scope_sha256": scope_sha256,
        "policy_sha256": policy_sha256,
        "underlyers": list(underlyers),
        "rows": screener_rows,
        "coverage": coverage,
        "filter_values": {},
        "blocked": [],
        "session_date": control_plane.session_date.isoformat(),
    }
    evaluation_rows = [
        _alert_evaluation_row(row, generated_at)
        for row in cycle.evaluations
    ]
    current_marks = {
        str(row["candidate_id"]): row
        for row in page_source.current_marks
    }
    run_rows = [
        stored
        for row in cycle.evaluations
        if (
            stored := _alert_run_row(
                row,
                generated_at,
                current_marks,
            )
        ) is not None
    ]
    run = _run_summary(cycle, generated_at)
    alerts = {
        "contract": OPTIONS_PAGE_CONTRACTS["ALERTS"],
        "artifact_kind": "ALERTS",
        **common_identity,
        "scope_sha256": scope_sha256,
        "policy_sha256": policy_sha256,
        "underlyers": list(underlyers),
        "session_date": control_plane.session_date.isoformat(),
        "run": run,
        "evaluation_rows": evaluation_rows,
        "alert_rows": run_rows,
        "lanes": [
            {
                "detector": row.detector.value,
                "state": row.state.value,
                "reason_codes": list(row.reason_codes),
                "outcome_sha256": row.outcome_sha256,
            }
            for row in cycle.outcomes
        ],
        "candidate_details": {
            str(row.package.candidate_id): detail
            for row in cycle.evaluations
            if row.package is not None
            if (
                detail := _candidate_detail(
                    row,
                    policy_sha256,
                )
            ) is not None
        },
        "production_authorized": False,
        "delivery_authorized": False,
        "execution_authorized": False,
    }
    return flow, screener, alerts


class AdvancedOptionsFlowPagePublication:
    def __init__(self, payload: Mapping[str, object]):
        self._payload = payload
        self.provider_tier = ProviderTier(
            str(payload["provider_tier"])
        )
        self.scope_sha256 = str(payload["scope_sha256"])

    def read(
        self,
        *,
        underlyer: str | None,
        cohort_id: UUID,
        provider_tier: ProviderTier,
        scope_sha256: str,
    ) -> dict[str, object]:
        _validate_page_identity(
            self._payload,
            cohort_id=cohort_id,
            provider_tier=provider_tier,
            scope_sha256=scope_sha256,
        )
        responses = self._payload["responses"]
        result = (
            next(iter(responses.values()), None)
            if underlyer is None
            else responses.get(underlyer)
        )
        if result is None:
            raise AdvancedOptionsPageUnavailable(
                "Requested Flow scope is outside the Advanced release"
            )
        return deepcopy(result)


class AdvancedOptionsScreenerPagePublication:
    def __init__(self, payload: Mapping[str, object]):
        self._payload = payload
        self.provider_tier = ProviderTier(
            str(payload["provider_tier"])
        )
        self.scope_sha256 = str(payload["scope_sha256"])

    def query(
        self,
        request,
        *,
        cohort_id: UUID,
        provider_tier: ProviderTier,
        scope_sha256: str,
    ) -> dict[str, object]:
        from options.analytics.eligible_chain_projection import (
            query_stored_eligible_chain,
        )

        _validate_page_identity(
            self._payload,
            cohort_id=cohort_id,
            provider_tier=provider_tier,
            scope_sha256=scope_sha256,
        )
        result = query_stored_eligible_chain(
            request,
            rows=tuple(self._payload["rows"]),
            coverage=tuple(self._payload["coverage"]),
            underlyers=tuple(self._payload["underlyers"]),
            filter_values=self._payload["filter_values"],
        )
        return {
            **result,
            "data_tier": "REAL-TIME ADVANCED RESEARCH DATA",
            "generated_at": self._payload["published_at"],
            "policy_sha256": self._payload["policy_sha256"],
            "model_version": "advanced-options-v2",
        }


class AdvancedOptionsAlertsPagePublication:
    def __init__(self, payload: Mapping[str, object]):
        self._payload = payload
        self.provider_tier = ProviderTier(
            str(payload["provider_tier"])
        )
        self.scope_sha256 = str(payload["scope_sha256"])
        self.dataset_id = str(payload["dataset_id"])

    def _stored(self, identity):
        _validate_page_identity(self._payload, **identity)
        return self._payload

    @staticmethod
    def _envelope(stored, *, available, reason=None, data=None):
        return {
            "available": available,
            "reason": reason,
            "data_tier": "REAL-TIME ADVANCED RESEARCH DATA",
            "generated_at": stored["published_at"],
            "as_of": stored["published_at"] if available else None,
            "observed_at": stored["run"]["observed_time"],
            "policy_sha256": stored["policy_sha256"],
            "model_version": "advanced-options-v2",
            "data": data or {},
        }

    def datasets(self, **identity):
        stored = self._stored(identity)
        session = stored["session_date"]
        data = {
            "storage_ready": True,
            "datasets": [self.dataset_id],
            "dataset_sessions": {self.dataset_id: [session]},
            "sessions": [session],
            "session_datasets": {session: self.dataset_id},
            "current_session_date": session,
            "current_dataset_id": self.dataset_id,
            "default_dataset_id": self.dataset_id,
            "configured_dataset_id": self.dataset_id,
            "configured_effective_from": stored["published_at"],
        }
        return self._envelope(stored, available=True, data=data)

    def schedule(self, *, checked_at: datetime, **identity):
        stored = self._stored(identity)
        result = self._envelope(
            stored,
            available=False,
            reason="DETECTOR_LAUNCH_NOT_CONFIGURED",
            data={
                "dataset_id": self.dataset_id,
                "retained_cycle_cutoff": stored["published_at"],
                "completed_run_count": 1,
                "operational_schedule_state": "NOT_CONFIGURED",
                "worker_health": "NOT_CHECKED",
            },
        )
        result["generated_at"] = checked_at
        return result

    @staticmethod
    def _matches(row, request):
        if request.dataset_id not in (None, row["run_id"]):
            return False
        if request.detector is not None and (
            row["detector_id"] != request.detector
        ):
            return False
        if request.underlyer is not None and (
            row["underlyer"] != request.underlyer.strip().upper()
        ):
            return False
        return (
            request.selection_status is None
            or row["selection_status"] == request.selection_status
        )

    def evaluations(self, request, **identity):
        stored = self._stored(identity)
        lane_by_detector = {
            row["detector"]: row for row in stored["lanes"]
        }
        if (
            len(lane_by_detector) != len(stored["lanes"])
            or request.detector is not None
            and request.detector not in lane_by_detector
        ):
            return self._envelope(
                stored,
                available=False,
                reason="DETECTOR_LANE_UNAVAILABLE",
                data={
                    "available_detectors": sorted(lane_by_detector),
                },
            )
        rows = [
            deepcopy(row)
            for row in stored["evaluation_rows"]
            if self._matches(row, request)
        ]
        reverse = request.sort_order == "desc"
        sort_key = {
            "triggered_at": "selected_at",
            "run": "scheduled_cycle",
            "underlyer": "underlyer",
            "detector": "detector_id",
            "category": "category",
            "strategy": "strategy_name",
            "rank": "candidate_rank",
            "entry_limit": "entry_limit",
        }[request.sort_by]
        rows.sort(
            key=lambda row: (
                row.get(sort_key) is None,
                str(row.get(sort_key)),
                row["evaluation_id"],
            ),
            reverse=reverse,
        )
        page = rows[request.offset:request.offset + request.limit]
        models = []
        for detector, lane in lane_by_detector.items():
            detector_rows = [
                row for row in stored["evaluation_rows"]
                if row["detector_id"] == detector
            ]
            detector_statuses = Counter(
                row["selection_status"] for row in detector_rows
            )
            models.append({
                "detector_id": detector,
                "accepted": not any(
                    reason.endswith("MODEL_NOT_ACCEPTED")
                    for reason in lane["reason_codes"]
                ),
                "lane_state": lane["state"],
                "reason_codes": list(lane["reason_codes"]),
                "evaluated": len(detector_rows),
                "detected": detector_statuses["SELECTED"],
                "selected": detector_statuses["SELECTED"],
                "not_selected": detector_statuses["NOT_SELECTED"],
                "repeats": detector_statuses["REPEAT"],
                "observations": detector_statuses["OBSERVATION"],
                "measured": None,
                "positive_rate": None,
                "mean_net_return": None,
                "outcome_status": (
                    "NOT_BOUND_TO_PROSPECTIVE_OUTCOMES"
                ),
            })
        data = {
            "storage_ready": True,
            "dataset_id": self.dataset_id,
            "datasets": [self.dataset_id],
            "sessions": [stored["session_date"]],
            "session_date": stored["session_date"],
            "as_of": stored["published_at"],
            "maximum_new_alerts": 20,
            "completed_runs": [deepcopy(stored["run"])],
            "outcome_status": "NOT_BOUND_TO_PROSPECTIVE_OUTCOMES",
            "total": len(rows),
            "models": models,
            "rows": page,
            "cells": [
                {
                    "detector_id": detector,
                    "selection_status": status,
                    "selection_reason": status,
                    "occurrences": count,
                    "distinct_packages": count,
                    "repeated_package_occurrences": 0,
                    "measured": None,
                    "positive_rate": None,
                    "mean_net_return": None,
                    "outcome_status": (
                        "NOT_BOUND_TO_PROSPECTIVE_OUTCOMES"
                    ),
                }
                for detector in lane_by_detector
                for status, count in sorted(Counter(
                    row["selection_status"]
                    for row in stored["evaluation_rows"]
                    if row["detector_id"] == detector
                ).items())
            ],
        }
        return self._envelope(stored, available=True, data=data)

    def detector_runs(self, request, **identity):
        stored = self._stored(identity)
        if request.dataset_id != self.dataset_id:
            return self._envelope(
                stored,
                available=False,
                reason="DETECTOR_DATASET_UNAVAILABLE",
            )
        rows = [
            deepcopy(row) for row in stored["alert_rows"]
            if (
                request.detector is None
                or row["detector_id"] == request.detector
            )
            and (
                request.underlyer is None
                or row["underlyer"]
                == request.underlyer.strip().upper()
            )
        ]
        reverse = request.sort_order == "desc"
        sort_key = {
            "triggered_at": "triggered_at",
            "run": "scheduled_cycle",
            "underlyer": "underlyer",
            "detector": "detector_id",
            "category": "category",
            "strategy": "strategy_name",
            "rank": "candidate_rank",
            "entry_limit": "entry_limit",
        }[request.sort_by]
        rows.sort(
            key=lambda row: (
                row.get(sort_key) is None,
                str(row.get(sort_key)),
                row["evaluation_id"],
            ),
            reverse=reverse,
        )
        page = rows[request.offset:request.offset + request.limit]
        data = {
            "version": "option_detector_alert_review_v2",
            "dataset_id": self.dataset_id,
            "scope": request.scope,
            "status": "COMPLETE",
            "as_of": stored["published_at"],
            "latest_run_id": stored["run"]["run_id"],
            "run": deepcopy(stored["run"]),
            "latest_run": deepcopy(stored["run"]),
            "runs": [deepcopy(stored["run"])],
            "session_date": stored["session_date"],
            "sessions": [stored["session_date"]],
            "total": len(rows),
            "new_alerts": len(rows),
            "detected_observations": len(
                stored["evaluation_rows"]
            ),
            "repeat_hits": 0,
            "maximum_new_alerts": 20,
            "outcome_status": "NOT_BOUND_TO_PROSPECTIVE_OUTCOMES",
            "execution_permission": False,
            "rows": page,
        }
        return self._envelope(stored, available=True, data=data)

    def candidate(self, candidate_id: UUID, **identity):
        stored = self._stored(identity)
        detail = stored["candidate_details"].get(str(candidate_id))
        return self._envelope(
            stored,
            available=detail is not None,
            reason=None if detail is not None else "CANDIDATE_NOT_FOUND",
            data=deepcopy(detail) if detail is not None else {},
        )

    def preview(
        self,
        candidate_id: UUID,
        *,
        assessed_at: datetime,
        request: dict,
        **identity,
    ):
        stored = self._stored(identity)
        return self._envelope(
            stored,
            available=False,
            reason="ASSESSMENT_UNAVAILABLE",
            data={},
        )

    def unavailable(
        self,
        contract,
        *,
        requested_contract_ids=(),
        **identity,
    ):
        stored = self._stored(identity)
        reasons = {
            "schedule": "DETECTOR_LAUNCH_NOT_CONFIGURED",
            "candidate": "CANDIDATE_UNAVAILABLE",
            "quotes": "ADVANCED_QUOTES_UNAVAILABLE",
            "preview": "ASSESSMENT_UNAVAILABLE",
        }
        data = {}
        if contract == "quotes":
            values = list(requested_contract_ids)
            data = {
                "quotes": [],
                "requested_contract_ids": values,
                "missing_contract_ids": values,
                "source": "PERSISTED_ADVANCED_QUOTE_CURRENT",
                "provider_fetch_performed": False,
                "execution_permission": False,
            }
        return self._envelope(
            stored,
            available=False,
            reason=reasons[contract],
            data=data,
        )


class AdvancedOptionsPageUnavailable(LookupError):
    pass


def _validate_page_identity(
    payload: Mapping[str, object],
    *,
    cohort_id: UUID,
    provider_tier: ProviderTier,
    scope_sha256: str,
) -> None:
    if (
        str(payload["cohort_id"]) != str(cohort_id)
        or payload["provider_tier"] != provider_tier.value
        or payload["scope_sha256"] != scope_sha256
    ):
        raise AdvancedOptionsPageUnavailable(
            "Advanced Options stored page scope mismatch; fallback prohibited"
        )


def _stored_page_row(row):
    if isinstance(row, Mapping):
        return dict(row)
    names = (
        "release_id",
        "cutoff",
        "session_date",
        "provider_tier",
        "manifest",
        "manifest_sha256",
        "snapshot_type",
        "snapshot_id",
        "generated_at",
        "payload_sha256",
        "payload",
    )
    return dict(zip(names, row, strict=True))


def load_advanced_options_page_release_cursor(
    cursor,
    *,
    cohort_id: UUID,
    available_by: datetime,
    artifact_kind: str,
    release_id: UUID,
    release_cutoff: datetime,
):
    if artifact_kind not in OPTIONS_FULL_PAGE_ARTIFACT_KINDS:
        raise ValueError("Advanced Options page artifact kind is invalid")
    checked = _utc(available_by, "available_by")
    exact_cutoff = _utc(release_cutoff, "release_cutoff")
    cursor.execute(
        "SELECT release.release_id,release.cutoff,release.session_date,"
        "release.provider_tier,release.manifest,"
        "release.manifest_sha256,"
        "entry.value->>'snapshot_type' AS snapshot_type,"
        "catalog.snapshot_id,catalog.generated_at,"
        "catalog.payload_sha256,hot.payload "
        "FROM rt.portal_releases AS release "
        "JOIN LATERAL jsonb_array_elements("
        "release.manifest->'snapshots') AS entry(value) "
        "ON entry.value->>'snapshot_type'=ANY(%s) "
        "AND entry.value->>'snapshot_key'='CURRENT' "
        "JOIN rt.snapshot_catalog AS catalog "
        "ON catalog.snapshot_id=(entry.value->>'snapshot_id')::uuid "
        "AND catalog.cohort_id=release.cohort_id "
        "AND catalog.provider_tier=release.provider_tier "
        "JOIN rt.portal_snapshot_hot AS hot "
        "ON hot.snapshot_id=catalog.snapshot_id "
        "AND hot.generated_at=catalog.generated_at "
        "WHERE release.release_id=%s AND release.cutoff=%s "
        "AND release.cohort_id=%s "
        "AND release.provider_tier='MASSIVE_ADVANCED' "
        "AND release.cutoff<=%s "
        "ORDER BY entry.value->>'snapshot_type'",
        (
            list(OPTIONS_PAGE_SNAPSHOT_TYPES.values()),
            str(release_id),
            exact_cutoff,
            str(cohort_id),
            checked,
        ),
    )
    rows = tuple(_stored_page_row(row) for row in cursor.fetchall())
    if not rows:
        return None
    by_type = {row["snapshot_type"]: row for row in rows}
    if set(by_type) != set(OPTIONS_PAGE_SNAPSHOT_TYPES.values()):
        raise AdvancedOptionsPageUnavailable(
            "Advanced Options release does not contain every full page artifact"
        )
    first = rows[0]
    manifest = first["manifest"]
    if (
        canonical_sha256(manifest) != first["manifest_sha256"]
        or first["provider_tier"] != ProviderTier.MASSIVE_ADVANCED.value
        or any(
            row["release_id"] != first["release_id"]
            or row["cutoff"] != first["cutoff"]
            or row["manifest_sha256"] != first["manifest_sha256"]
            for row in rows
        )
    ):
        raise AdvancedOptionsPageUnavailable(
            "Advanced Options full page release identity drift"
        )
    manifest_types = {
        entry["snapshot_type"] for entry in manifest["snapshots"]
    }
    if not set(OPTIONS_SNAPSHOT_TYPES).issubset(manifest_types):
        raise AdvancedOptionsPageUnavailable(
            "Advanced Options release is missing page or diagnostic evidence"
        )
    payloads = {}
    valid_until_by_kind = {}
    for kind, snapshot_type in OPTIONS_PAGE_SNAPSHOT_TYPES.items():
        row = by_type[snapshot_type]
        payload = row["payload"]
        try:
            valid_until = _utc(
                datetime.fromisoformat(str(payload["valid_until"])),
                "Advanced Options page valid_until",
            )
        except (KeyError, TypeError, ValueError) as error:
            raise AdvancedOptionsPageUnavailable(
                "Advanced Options full page artifact identity drift"
            ) from error
        if (
            canonical_sha256(payload) != row["payload_sha256"]
            or payload.get("contract") != OPTIONS_PAGE_CONTRACTS[kind]
            or payload.get("artifact_kind") != kind
            or payload.get("cohort_id") != str(cohort_id)
            or payload.get("session_date") != first["session_date"].isoformat()
            or payload.get("provider_tier")
            != ProviderTier.MASSIVE_ADVANCED.value
            or payload.get("published_at")
            != row["generated_at"].astimezone(timezone.utc).isoformat()
        ):
            raise AdvancedOptionsPageUnavailable(
                "Advanced Options full page artifact identity drift"
            )
        payloads[kind] = payload
        valid_until_by_kind[kind] = valid_until
    scope_sha256s = {
        payload["scope_sha256"] for payload in payloads.values()
    }
    cycle_sha256s = {
        payload["cycle_sha256"] for payload in payloads.values()
    }
    dataset_ids = {
        payload["dataset_id"] for payload in payloads.values()
    }
    if (
        len(scope_sha256s) != 1
        or len(cycle_sha256s) != 1
        or len(dataset_ids) != 1
    ):
        raise AdvancedOptionsPageUnavailable(
            "Advanced Options full page artifacts changed release scope"
        )
    selected = by_type[OPTIONS_PAGE_SNAPSHOT_TYPES[artifact_kind]]
    valid_until = valid_until_by_kind[artifact_kind]
    staleness_seconds = max(
        0,
        int((checked - valid_until).total_seconds()),
    )
    publication_types = {
        "FLOW": AdvancedOptionsFlowPagePublication,
        "SCREENER": AdvancedOptionsScreenerPagePublication,
        "ALERTS": AdvancedOptionsAlertsPagePublication,
    }
    return (
        publication_types[artifact_kind](payloads[artifact_kind]),
        {
            "release_id": str(first["release_id"]),
            "cutoff": first["cutoff"],
            "manifest_sha256": first["manifest_sha256"],
            "scope_sha256": next(iter(scope_sha256s)),
            "snapshot_id": str(selected["snapshot_id"]),
            "retained_session_date": first["session_date"].isoformat(),
            "valid_until": valid_until.isoformat(),
            "checked_at": checked.isoformat(),
            "stale": staleness_seconds > 0,
            "staleness_seconds": staleness_seconds,
            "serving_latest_available": staleness_seconds > 0,
        },
    )


@dataclass(frozen=True, slots=True)
class AdvancedOptionsProjectionBatch:
    cycle: AdvancedOptionsCycleResult
    snapshots: tuple[RealtimePortalSnapshot, ...]
    batch_sha256: str
    portal_ready_receipt: CycleReceiptSpec
    publication_authorized: bool = False


@dataclass(frozen=True, slots=True)
class AdvancedOptionsPageSource:
    flow_events: tuple[Mapping[str, object], ...] = ()
    current_marks: tuple[Mapping[str, object], ...] = ()

    @property
    def sha256(self) -> str:
        return canonical_sha256({
            "contract": "advanced-options-page-source-v1",
            "flow_events": self.flow_events,
            "current_marks": self.current_marks,
        })


def _flow_event_row(row):
    if isinstance(row, Mapping):
        return dict(row)
    names = (
        "event_id",
        "event_time",
        "contract_ticker",
        "contract_id",
        "underlyer",
        "price",
        "size",
        "multiplier",
        "premium",
        "prior_open_interest",
    )
    return dict(zip(names, row, strict=True))


def _current_mark_row(row):
    if isinstance(row, Mapping):
        candidate_id = row["candidate_id"]
        package_mark = row["package_mark"]
    else:
        candidate_id, package_mark = row
    if (
        not isinstance(package_mark, Mapping)
        or not isinstance(package_mark.get("outcome"), Mapping)
    ):
        raise ValueError(
            "Advanced Options package mark evidence is invalid"
        )
    return {
        "candidate_id": str(candidate_id),
        "computed_at": package_mark["computed_at"],
        "management_status": None,
        **dict(package_mark["outcome"]),
    }


def load_advanced_options_page_source_cursor(
    cursor,
    *,
    control_plane: AdvancedOptionsControlPlanePlan,
    cycle: AdvancedOptionsCycleResult,
    generated_at: datetime,
) -> AdvancedOptionsPageSource:
    generated = _utc(generated_at, "generated_at")
    cursor.execute(
        "SELECT event.event_id,event.event_time,"
        "event.contract_ticker,"
        "COALESCE(event.contract_id,contract.contract_id) AS contract_id,"
        "event.underlyer,event.price,event.size,event.multiplier,"
        "event.premium,event.prior_open_interest "
        "FROM rt.option_flow_events AS event "
        "LEFT JOIN ref.option_contracts AS contract "
        "ON contract.contract_ticker=event.contract_ticker "
        "WHERE event.cohort_id=%s "
        "AND event.provider_tier='MASSIVE_ADVANCED' "
        "AND event.session_date=%s AND event.event_time<=%s "
        "ORDER BY event.event_time,event.event_id LIMIT %s",
        (
            str(control_plane.cohort_id),
            control_plane.session_date,
            generated,
            ADVANCED_OPTIONS_PAGE_FLOW_EVENT_LIMIT + 1,
        ),
    )
    events = tuple(_flow_event_row(row) for row in cursor.fetchall())
    if len(events) > ADVANCED_OPTIONS_PAGE_FLOW_EVENT_LIMIT:
        raise ValueError(
            "Advanced Options page flow evidence exceeds its sealed limit"
        )
    if any(
        str(row["underlyer"]) not in control_plane.universe.option_underlyings
        or row["event_time"] > generated
        or row["contract_id"] is None
        for row in events
    ):
        raise ValueError(
            "Advanced Options page flow evidence changed cycle scope"
        )
    if cycle.input_gate.closure.cycle_input.cohort_id != (
        control_plane.cohort_id
    ):
        raise ValueError(
            "Advanced Options page source changed cycle identity"
        )
    candidate_ids = sorted({
        str(row.package.candidate_id)
        for row in cycle.evaluations
        if row.package is not None
    })
    marks = ()
    if candidate_ids:
        cursor.execute(
            "SELECT candidate_id,"
            "evidence_document->'package_mark' AS package_mark "
            "FROM rt.advanced_options_flow_package_evidence "
            "WHERE dataset_id=%s "
            "AND candidate_id=ANY(%s::uuid[]) "
            "AND selection_status='SELECTED' "
            "ORDER BY candidate_id",
            (
                str(cycle.dataset_id),
                candidate_ids,
            ),
        )
        marks = tuple(
            _current_mark_row(row) for row in cursor.fetchall()
        )
        if {
            str(row["candidate_id"]) for row in marks
        } != set(candidate_ids):
            raise ValueError(
                "Advanced Options page mark evidence is incomplete"
            )
    return AdvancedOptionsPageSource(events, marks)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsPublisherHandoff:
    owner_name: str
    receipt: PortalReleaseReceipt
    snapshots: tuple[RealtimePortalSnapshot, ...]
    batch_sha256: str
    dispatch_authorized: bool = False
    release_authorized: bool = False
    pointer_authorized: bool = False


@dataclass(frozen=True, slots=True)
class AdvancedOptionsPublicationAuthorization:
    authorization_id: UUID
    cohort_id: UUID
    provider_tier: ProviderTier
    batch_sha256: str
    receipt_id: UUID
    scope_reference: str
    release_authorized: bool = True
    pointer_authorized: bool = True
    dispatch_authorized: bool = False
    delivery_authorized: bool = False
    broker_authorized: bool = False
    trading_authorized: bool = False

    def __post_init__(self) -> None:
        if (
            not isinstance(self.authorization_id, UUID)
            or self.authorization_id.int == 0
            or not isinstance(self.cohort_id, UUID)
            or self.cohort_id.int == 0
            or not isinstance(self.provider_tier, ProviderTier)
            or not isinstance(self.receipt_id, UUID)
            or self.receipt_id.int == 0
            or len(self.batch_sha256) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.batch_sha256
            )
            or not self.scope_reference.strip()
            or any((
                not self.release_authorized,
                not self.pointer_authorized,
                self.dispatch_authorized,
                self.delivery_authorized,
                self.broker_authorized,
                self.trading_authorized,
            ))
        ):
            raise ValueError(
                "Advanced Options publication authority is invalid"
            )

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsCanaryPublicationAuthorization:
    database_name: str
    cohort_id: UUID
    batch_sha256: str
    receipt_id: UUID
    scope_reference: str = P8_DISPOSABLE_SCOPE_REFERENCE
    provider_authorized: bool = False
    production_authorized: bool = False
    release_authorized: bool = True
    pointer_authorized: bool = True
    dispatch_authorized: bool = False
    delivery_authorized: bool = False
    execution_authorized: bool = False

    def __post_init__(self) -> None:
        if (
            not self.database_name.startswith(
                P8_DISPOSABLE_DATABASE_PREFIX
            )
            or self.scope_reference != P8_DISPOSABLE_SCOPE_REFERENCE
            or not isinstance(self.receipt_id, UUID)
            or len(self.batch_sha256) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.batch_sha256
            )
            or any((
                self.provider_authorized,
                self.production_authorized,
                self.dispatch_authorized,
                self.delivery_authorized,
                self.execution_authorized,
                not self.release_authorized,
                not self.pointer_authorized,
            ))
        ):
            raise ValueError(
                "Advanced Options canary publication authority is invalid"
            )

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


def build_advanced_options_projection_batch(
    *,
    control_plane: AdvancedOptionsControlPlanePlan,
    cycle: AdvancedOptionsCycleResult,
    generated_at: datetime,
    build_id: str,
    page_source: AdvancedOptionsPageSource | None = None,
) -> AdvancedOptionsProjectionBatch:
    generated = _utc(generated_at, "generated_at")
    source = (
        AdvancedOptionsPageSource()
        if page_source is None
        else page_source
    )
    if (
        cycle.input_gate.closure.control_plane_plan_id
        != control_plane.plan_id
        or cycle.receipt.receipt_kind != OPTIONS_CYCLE
        or cycle.receipt.cutoff != generated
    ):
        raise ValueError("Advanced Options projection cycle scope changed")
    lanes = [
        {
            "detector": row.detector.value,
            "state": row.state.value,
            "reason_codes": list(row.reason_codes),
            "outcome_sha256": row.outcome_sha256,
        }
        for row in cycle.outcomes
    ]
    evaluations = [
        {
            "evaluation_id": str(row.evaluation_id),
            "event_id": str(row.event_id),
            "detector": row.detector.value,
            "underlyer": row.underlyer,
            "selection_status": row.selection_status,
            "observed_at": row.observed_at,
            "payload_sha256": row.payload_sha256,
            "payload": row.payload,
            "candidate_id": (
                str(row.package.candidate_id)
                if row.package is not None
                else None
            ),
        }
        for row in cycle.evaluations
    ]
    manifest = {
        "contract": "advanced-options-projection-manifest-v1",
        "cohort_id": str(control_plane.cohort_id),
        "manifest_sha256": control_plane.manifest_sha256,
        "cycle_sha256": cycle.cycle_sha256,
        "input_sha256": cycle.input_gate.closure_sha256,
        "valuation_policy_sha256": (
            control_plane.pins.valuation_policy_sha256
        ),
        "model_bundle_sha256": control_plane.pins.model_bundle_sha256,
        "build_id": build_id,
    }
    payloads: Mapping[str, Mapping[str, object]] = {
        "OPTIONS_FLOW": {
            "status": cycle.dataset_status,
            "lanes": lanes,
            "evaluations": [
                row for row in evaluations
                if row["detector"] in {
                    "AGGRESSIVE_PREMIUM_BURST",
                    "PERSISTENT_REALTIME_FLOW",
                    "SWEEP_LIKE_CLUSTER",
                }
            ],
        },
        "OPTIONS_SCREENER": {
            "status": cycle.dataset_status,
            "lanes": lanes,
            "evaluations": evaluations,
        },
        "OPTIONS_ALERTS": {
            "status": cycle.dataset_status,
            "lanes": lanes,
            "evaluations": [
                row for row in evaluations
                if row["selection_status"] == "SELECTED"
            ],
            "production_authorized": False,
            "delivery_authorized": False,
        },
        "OPTIONS_MARKS": {
            "status": cycle.dataset_status,
            "candidate_ids": sorted({
                row["candidate_id"]
                for row in evaluations
                if row["candidate_id"] is not None
            }),
            "valuation_policy_sha256": (
                control_plane.pins.valuation_policy_sha256
            ),
        },
    }
    diagnostic_snapshots = tuple(
        build_portal_snapshot(
            cohort_id=control_plane.cohort_id,
            provider_tier=control_plane.binding.provider_tier,
            snapshot_type=snapshot_type,
            snapshot_key="CURRENT",
            source_generation=cycle.receipt.reference_generation,
            source_manifest={**manifest, "snapshot_type": snapshot_type},
            payload=payloads[snapshot_type],
            generated_at=generated,
        )
        for snapshot_type in OPTIONS_PROJECTION_TYPES
    )
    page_payloads = _build_advanced_options_page_payloads(
        control_plane=control_plane,
        cycle=cycle,
        generated_at=generated,
        page_source=source,
    )
    page_snapshots = tuple(
        build_portal_snapshot(
            cohort_id=control_plane.cohort_id,
            provider_tier=control_plane.binding.provider_tier,
            snapshot_type=OPTIONS_PAGE_SNAPSHOT_TYPES[
                payload["artifact_kind"]
            ],
            snapshot_key="CURRENT",
            source_generation=cycle.receipt.reference_generation,
            source_manifest={
                **manifest,
                "snapshot_type": OPTIONS_PAGE_SNAPSHOT_TYPES[
                    payload["artifact_kind"]
                ],
                "artifact_kind": payload["artifact_kind"],
                "scope_sha256": payload["scope_sha256"],
                "contract": payload["contract"],
            },
            payload=payload,
            generated_at=generated,
            valid_until=datetime.fromisoformat(payload["valid_until"]),
        )
        for payload in page_payloads
    )
    snapshots = (*diagnostic_snapshots, *page_snapshots)
    batch_sha256 = canonical_sha256({
        "contract": "advanced-options-projection-batch-v1",
        "cycle_sha256": cycle.cycle_sha256,
        "snapshot_ids": tuple(row.snapshot_id for row in snapshots),
        "publication_authorized": False,
    })
    portal_ready = build_options_receipt_spec(
        receipt_kind=OPTIONS_PORTAL_READY,
        cutoff=generated,
        reference_generation=cycle.receipt.reference_generation,
        universe_sha256=control_plane.universe.sha256,
        input_sha256=batch_sha256,
        build_id=build_id,
    )
    return AdvancedOptionsProjectionBatch(
        cycle,
        snapshots,
        batch_sha256,
        portal_ready,
    )


def persist_advanced_options_projection_cursor(
    cursor,
    batch: AdvancedOptionsProjectionBatch,
    *,
    snapshot_repository=persist_portal_snapshot_cursor,
) -> None:
    if batch.publication_authorized:
        raise ValueError("P4C projection cannot authorize publication")
    for snapshot in batch.snapshots:
        snapshot_repository(cursor, snapshot)


def load_advanced_options_projection_batch_cursor(
    cursor,
    *,
    control_plane: AdvancedOptionsControlPlanePlan,
    cycle: AdvancedOptionsCycleResult,
    portal_ready_receipt: CycleReceiptSpec,
) -> AdvancedOptionsProjectionBatch:
    if (
        portal_ready_receipt.receipt_kind != OPTIONS_PORTAL_READY
        or portal_ready_receipt.cutoff != cycle.receipt.cutoff
        or portal_ready_receipt.reference_generation
        != cycle.receipt.reference_generation
        or portal_ready_receipt.universe_sha256
        != control_plane.universe.sha256
    ):
        raise ValueError(
            "durable OPTIONS_PORTAL_READY receipt scope changed"
        )
    cursor.execute(
        "SELECT catalog.snapshot_id,catalog.cohort_id,"
        "catalog.provider_tier,catalog.snapshot_type,"
        "catalog.snapshot_key,catalog.source_generation,"
        "catalog.manifest_sha256,hot.source_manifest,"
        "catalog.payload_sha256,hot.payload,catalog.generated_at,"
        "catalog.valid_until FROM rt.snapshot_catalog AS catalog "
        "JOIN rt.portal_snapshot_hot AS hot "
        "ON hot.snapshot_id=catalog.snapshot_id "
        "AND hot.generated_at=catalog.generated_at "
        "WHERE catalog.cohort_id=%s "
        "AND catalog.provider_tier=%s "
        "AND catalog.snapshot_class='EPHEMERAL_PORTAL' "
        "AND catalog.snapshot_type=ANY(%s) "
        "AND catalog.snapshot_key='CURRENT' "
        "AND catalog.generated_at=%s "
        "ORDER BY catalog.snapshot_type",
        (
            str(control_plane.cohort_id),
            control_plane.binding.provider_tier.value,
            list(OPTIONS_SNAPSHOT_TYPES),
            portal_ready_receipt.cutoff,
        ),
    )
    names = (
        "snapshot_id",
        "cohort_id",
        "provider_tier",
        "snapshot_type",
        "snapshot_key",
        "source_generation",
        "manifest_sha256",
        "source_manifest",
        "payload_sha256",
        "payload",
        "generated_at",
        "valid_until",
    )
    stored = {}
    for row in cursor.fetchall():
        value = (
            dict(row)
            if isinstance(row, Mapping)
            else dict(zip(names, row, strict=True))
        )
        snapshot = RealtimePortalSnapshot(
            snapshot_id=UUID(str(value["snapshot_id"])),
            cohort_id=UUID(str(value["cohort_id"])),
            provider_tier=ProviderTier(str(value["provider_tier"])),
            snapshot_type=str(value["snapshot_type"]),
            snapshot_key=str(value["snapshot_key"]),
            source_generation=int(value["source_generation"]),
            source_manifest_sha256=str(value["manifest_sha256"]),
            source_manifest=value["source_manifest"],
            payload_sha256=str(value["payload_sha256"]),
            payload=value["payload"],
            generated_at=value["generated_at"],
            valid_until=value["valid_until"],
        )
        rebuilt = build_portal_snapshot(
            cohort_id=snapshot.cohort_id,
            provider_tier=snapshot.provider_tier,
            snapshot_type=snapshot.snapshot_type,
            snapshot_key=snapshot.snapshot_key,
            source_generation=snapshot.source_generation,
            source_manifest=snapshot.source_manifest,
            payload=snapshot.payload,
            generated_at=snapshot.generated_at,
            valid_until=snapshot.valid_until,
        )
        if (
            rebuilt != snapshot
            or snapshot.cohort_id != control_plane.cohort_id
            or snapshot.provider_tier
            is not control_plane.binding.provider_tier
            or snapshot.source_generation
            != portal_ready_receipt.reference_generation
            or snapshot.generated_at != portal_ready_receipt.cutoff
            or snapshot.source_manifest.get("cycle_sha256")
            != cycle.cycle_sha256
        ):
            raise ValueError(
                "durable Advanced Options projection identity changed"
            )
        if snapshot.snapshot_type in stored:
            raise ValueError(
                "durable Advanced Options projection is ambiguous"
            )
        stored[snapshot.snapshot_type] = snapshot
    if set(stored) != set(OPTIONS_SNAPSHOT_TYPES):
        raise ValueError(
            "durable Advanced Options projection is incomplete"
        )
    snapshots = tuple(stored[name] for name in OPTIONS_SNAPSHOT_TYPES)
    batch_sha256 = canonical_sha256({
        "contract": "advanced-options-projection-batch-v1",
        "cycle_sha256": cycle.cycle_sha256,
        "snapshot_ids": tuple(row.snapshot_id for row in snapshots),
        "publication_authorized": False,
    })
    if portal_ready_receipt.input_sha256 != batch_sha256:
        raise ValueError(
            "durable OPTIONS_PORTAL_READY projection hash changed"
        )
    return AdvancedOptionsProjectionBatch(
        cycle,
        snapshots,
        batch_sha256,
        portal_ready_receipt,
    )


def build_advanced_options_publisher_handoff(
    *,
    control_plane: AdvancedOptionsControlPlanePlan,
    batch: AdvancedOptionsProjectionBatch,
) -> AdvancedOptionsPublisherHandoff:
    receipt_id = uuid5(
        NAMESPACE_URL,
        "stockscreener:advanced-options-portal-ready:"
        f"{control_plane.cohort_id}:{batch.batch_sha256}",
    )
    return AdvancedOptionsPublisherHandoff(
        owner_name=CANONICAL_PUBLISHER_OWNER,
        receipt=PortalReleaseReceipt(
            receipt_id,
            control_plane.cohort_id,
            control_plane.binding.provider_tier,
            batch.portal_ready_receipt.cutoff,
            control_plane.universe.sha256,
        ),
        snapshots=batch.snapshots,
        batch_sha256=batch.batch_sha256,
    )


def authorize_advanced_options_publication(
    handoff: AdvancedOptionsPublisherHandoff,
    authorization: AdvancedOptionsPublicationAuthorization,
) -> AdvancedOptionsPublisherHandoff:
    if (
        not isinstance(handoff, AdvancedOptionsPublisherHandoff)
        or not isinstance(
            authorization,
            AdvancedOptionsPublicationAuthorization,
        )
        or handoff.owner_name != CANONICAL_PUBLISHER_OWNER
        or handoff.receipt.cohort_id != authorization.cohort_id
        or handoff.receipt.provider_tier is not authorization.provider_tier
        or handoff.batch_sha256 != authorization.batch_sha256
        or any((
            handoff.dispatch_authorized,
            handoff.release_authorized,
            handoff.pointer_authorized,
            authorization.dispatch_authorized,
            authorization.delivery_authorized,
            authorization.broker_authorized,
            authorization.trading_authorized,
            not authorization.release_authorized,
            not authorization.pointer_authorized,
        ))
    ):
        raise ValueError(
            "Advanced Options publisher authority scope changed"
        )
    return AdvancedOptionsPublisherHandoff(
        owner_name=handoff.owner_name,
        receipt=PortalReleaseReceipt(
            authorization.receipt_id,
            handoff.receipt.cohort_id,
            handoff.receipt.provider_tier,
            handoff.receipt.cutoff,
            handoff.receipt.universe_sha256,
        ),
        snapshots=handoff.snapshots,
        batch_sha256=handoff.batch_sha256,
        dispatch_authorized=False,
        release_authorized=True,
        pointer_authorized=True,
    )


def authorize_advanced_options_canary_publication(
    handoff: AdvancedOptionsPublisherHandoff,
    authorization: AdvancedOptionsCanaryPublicationAuthorization,
) -> AdvancedOptionsPublisherHandoff:
    if (
        not isinstance(handoff, AdvancedOptionsPublisherHandoff)
        or not isinstance(
            authorization,
            AdvancedOptionsCanaryPublicationAuthorization,
        )
        or handoff.owner_name != CANONICAL_PUBLISHER_OWNER
        or handoff.receipt.cohort_id != authorization.cohort_id
        or handoff.batch_sha256 != authorization.batch_sha256
        or any((
            handoff.dispatch_authorized,
            handoff.release_authorized,
            handoff.pointer_authorized,
        ))
    ):
        raise ValueError(
            "Advanced Options canary publisher handoff scope changed"
        )
    return AdvancedOptionsPublisherHandoff(
        owner_name=handoff.owner_name,
        receipt=PortalReleaseReceipt(
            authorization.receipt_id,
            handoff.receipt.cohort_id,
            handoff.receipt.provider_tier,
            handoff.receipt.cutoff,
            handoff.receipt.universe_sha256,
        ),
        snapshots=handoff.snapshots,
        batch_sha256=handoff.batch_sha256,
        dispatch_authorized=False,
        release_authorized=True,
        pointer_authorized=True,
    )


def read_advanced_options_projection_cursor(
    cursor,
    *,
    cohort_id: UUID,
    snapshot_type: str,
    available_by: datetime,
    release_id: UUID | None = None,
    release_cutoff: datetime | None = None,
) -> dict[str, object]:
    if snapshot_type not in OPTIONS_PROJECTION_TYPES:
        raise ValueError("Advanced Options projection type is invalid")
    checked = _utc(available_by, "available_by")
    if (release_id is None) != (release_cutoff is None):
        raise ValueError(
            "Advanced Options release pin identity is incomplete"
        )
    pinned_cutoff = (
        None
        if release_cutoff is None
        else _utc(release_cutoff, "release_cutoff")
    )
    release_source = (
        "FROM rt.portal_releases AS release "
        if release_id is not None
        else
        "FROM rt.portal_release_current AS pointer "
        "JOIN rt.portal_releases AS release "
        "ON release.release_id=pointer.release_id "
        "AND release.cutoff=pointer.cutoff "
    )
    where = (
        "WHERE release.release_id=%s AND release.cutoff=%s "
        "AND release.cohort_id=%s "
        "AND release.provider_tier='MASSIVE_ADVANCED' "
        "AND release.cutoff<=%s "
        if release_id is not None
        else
        "WHERE pointer.cohort_id=%s AND pointer.cutoff<=%s "
    )
    parameters = (
        (
            snapshot_type,
            str(release_id),
            pinned_cutoff,
            str(cohort_id),
            checked,
        )
        if release_id is not None
        else (snapshot_type, str(cohort_id), checked)
    )
    cursor.execute(
        "SELECT catalog.snapshot_id,catalog.generated_at,catalog.payload_sha256,"
        "hot.payload,release.release_id,release.cutoff,"
        "release.manifest_sha256 "
        + release_source +
        "JOIN LATERAL jsonb_array_elements("
        "release.manifest->'snapshots') AS entry(value) "
        "ON entry.value->>'snapshot_type'=%s "
        "AND entry.value->>'snapshot_key'='CURRENT' "
        "JOIN rt.snapshot_catalog AS catalog "
        "ON catalog.snapshot_id=(entry.value->>'snapshot_id')::uuid "
        "AND catalog.cohort_id=release.cohort_id "
        "AND catalog.provider_tier=release.provider_tier "
        "JOIN rt.portal_snapshot_hot AS hot "
        "ON hot.snapshot_id=catalog.snapshot_id "
        "AND hot.generated_at=catalog.generated_at "
        + where +
        "ORDER BY release.cutoff DESC LIMIT 1",
        parameters,
    )
    row = cursor.fetchone()
    if row is None:
        return {
            "available": False,
            "reason": "STORED_OPTIONS_PROJECTION_UNAVAILABLE",
            "snapshot_type": snapshot_type,
            "provider_fetch_performed": False,
            "detector_work_performed": False,
            "publication_performed": False,
            "data": None,
        }
    if isinstance(row, Mapping):
        values = (
            row["snapshot_id"],
            row["generated_at"],
            row["payload_sha256"],
            row["payload"],
            row["release_id"],
            row["cutoff"],
            row["manifest_sha256"],
        )
    else:
        values = row
    payload = values[3]
    if canonical_sha256(payload) != str(values[2]):
        raise ValueError("stored Advanced Options projection hash mismatch")
    return {
        "available": True,
        "reason": None,
        "snapshot_type": snapshot_type,
        "snapshot_id": str(values[0]),
        "generated_at": values[1],
        "release_id": str(values[4]),
        "release_cutoff": values[5],
        "release_manifest_sha256": str(values[6]),
        "provider_fetch_performed": False,
        "detector_work_performed": False,
        "publication_performed": False,
        "data": payload,
    }
