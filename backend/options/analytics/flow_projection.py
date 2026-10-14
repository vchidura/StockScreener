from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
from typing import Mapping

from options.calendar import OptionExchangeCalendar
from options.domain import ContractType, OptionContractSnapshot


FLOW_DEFINITIONS = {
    "volume": "Cumulative provider day volume summed across retained standard contracts in the latest complete matrix.",
    "open_interest": "Latest provider open interest, representing the prior completed settlement.",
    "premium_activity": "Estimated activity = day volume x latest aligned mark x contract multiplier. It is not transacted premium or net flow.",
    "open_interest_change": "Change across contracts present in both latest captured settlements. It identifies positioning change, not trade direction.",
    "coverage": "Totals cover the configured DTE, strike corridor, standard-contract and liquidity-retained matrix rather than the provider's full chain.",
}
FLOW_COUNT_FIELDS = ("call_volume", "put_volume", "call_open_interest", "put_open_interest")


@dataclass(frozen=True, slots=True)
class OptionFlowProjection:
    available: bool
    reason: str | None
    as_of: datetime | None
    observed_at: datetime | None
    model_version: str | None
    data: dict


def assemble_option_flow(
    *, summaries: tuple[Mapping, ...], open_interest_changes: tuple[Mapping, ...] = (),
    expirations: tuple[Mapping, ...] = (), strikes: tuple[Mapping, ...] = (),
    top_contracts: tuple[Mapping, ...] = (), underlyer: str | None = None,
    session_date: date | None = None, policy_sha256: str, configuration_sha256: str,
) -> OptionFlowProjection:
    rows = [deepcopy(dict(row)) for row in summaries]
    requested = underlyer.strip().upper() if underlyer else "SPY"
    if rows and requested not in {row["underlying"] for row in rows}:
        requested = rows[0]["underlying"]
    selected = next((row for row in rows if row["underlying"] == requested), None)
    oi_by_underlying = {}
    for row in open_interest_changes:
        item = deepcopy(dict(row))
        for field in ("matched_contract_count", "current_contract_count",
                "call_open_interest_change", "put_open_interest_change"):
            item[field] = int(item[field] or 0)
        count = item["current_contract_count"]
        item["matched_coverage_fraction"] = item["matched_contract_count"] / count if count else 0.0
        item["total_open_interest_change"] = item["call_open_interest_change"] + item["put_open_interest_change"]
        oi_by_underlying[item["underlying"]] = item
    for summary in rows:
        for field in FLOW_COUNT_FIELDS:
            summary[field] = int(summary[field] or 0)
        summary["total_volume"] = summary["call_volume"] + summary["put_volume"]
        summary["total_open_interest"] = summary["call_open_interest"] + summary["put_open_interest"]
        summary["total_premium_activity"] = summary["call_premium_activity"] + summary["put_premium_activity"]
        summary["put_call_volume_ratio"] = summary["put_volume"] / summary["call_volume"] if summary["call_volume"] else None
        summary["put_call_open_interest_ratio"] = summary["put_open_interest"] / summary["call_open_interest"] if summary["call_open_interest"] else None
        summary["retention_fraction"] = summary["retained_row_count"] / summary["received_row_count"] if summary["received_row_count"] else 0.0
        summary["open_interest_change"] = oi_by_underlying.get(summary["underlying"])
    profiles = []
    for values in (expirations, strikes):
        normalized = [deepcopy(dict(row)) for row in values] if selected is not None else []
        for row in normalized:
            for field in FLOW_COUNT_FIELDS:
                row[field] = int(row[field] or 0)
        profiles.append(normalized)
    newest_market = max((row["market_data_time"] for row in rows if row["market_data_time"]), default=None)
    newest_observed = max((row["first_observed_at"] for row in rows if row["first_observed_at"]), default=None)
    data = {
        "serving_mode": "CURRENT_POLICY" if selected and selected["current_policy"] else "HISTORICAL_PREVIOUS_POLICY",
        "active_policy_sha256": policy_sha256, "active_configuration_sha256": configuration_sha256,
        "underlyers": rows, "selected": requested,
        "session_date": session_date.isoformat() if session_date is not None else
            OptionExchangeCalendar().session_for_market_time(newest_market).isoformat() if newest_market is not None else None,
        "selected_summary": selected, "expirations": profiles[0], "strikes": profiles[1],
        "top_contracts": [deepcopy(dict(row)) for row in top_contracts] if selected is not None else [],
        "directional_flow_available": False, "quote_liquidity": "NOT_AVAILABLE",
        "trade_tape_scope": "EXCLUDED_FROM_TOTALS_WATCHLIST_BIASED", "definitions": deepcopy(FLOW_DEFINITIONS),
    }
    return OptionFlowProjection(bool(rows), None if rows else "NO_COMPLETE_MATRIX", newest_market,
        newest_observed, (selected or {}).get("model_version"), data)


def option_flow_snapshot_rows(snapshots: tuple[OptionContractSnapshot, ...]) -> dict:
    if not snapshots or len({row.batch_id for row in snapshots}) != 1 or len({row.underlyer for row in snapshots}) != 1:
        raise ValueError("Flow aggregates require one nonempty exact matrix")

    def activity(row):
        mark = row.model_mark if row.model_mark is not None else row.display_mark
        return None if row.day_volume is None or mark is None else row.day_volume * mark * row.shares_per_contract

    def totals(rows):
        values = {"contract_count": len(rows)}
        for side, name in ((ContractType.CALL, "call"), (ContractType.PUT, "put")):
            members = tuple(row for row in rows if row.contract_type is side)
            values[f"{name}_volume"] = sum(row.day_volume or 0 for row in members)
            values[f"{name}_open_interest"] = sum(row.open_interest or 0 for row in members)
            values[f"{name}_premium_activity"] = sum((activity(row) or Decimal("0") for row in members), Decimal("0"))
        return values

    expiration_groups, strike_groups = defaultdict(list), defaultdict(list)
    for row in snapshots:
        expiration_groups[row.expiration_date].append(row)
        strike_groups[(row.expiration_date, row.strike)].append(row)
    expirations = tuple(dict(expiration_date=day, calendar_dte=min(row.calendar_dte for row in rows), **totals(rows))
        for day, rows in sorted(expiration_groups.items()))
    strikes = tuple(dict(expiration_date=day, strike=strike,
        **{key: value for key, value in totals(rows).items() if not key.endswith("premium_activity")})
        for (day, strike), rows in sorted(strike_groups.items()))
    ranked = sorted((row for row in snapshots if row.day_volume is not None),
        key=lambda row: (activity(row) is None, -(activity(row) or Decimal("0")), -row.day_volume, row.contract_id))[:24]
    top_contracts = tuple({**{field: getattr(row, field) for field in (
        "contract_id", "contract_ticker", "contract_type", "expiration_date", "calendar_dte", "strike", "spot",
        "day_volume", "open_interest", "model_mark", "display_mark", "local_iv", "local_delta")},
        "volume_open_interest_ratio": row.day_volume / max(row.open_interest, 1) if row.open_interest is not None else None,
        "premium_activity": activity(row), "moneyness_fraction": abs(row.strike / row.spot - 1)} for row in ranked)
    summary = {**totals(snapshots), "spot": max(row.spot for row in snapshots),
        "expiration_count": len(expiration_groups), "strike_count": len({row.strike for row in snapshots}),
        "call_contract_count": sum(row.contract_type is ContractType.CALL for row in snapshots),
        "put_contract_count": sum(row.contract_type is ContractType.PUT for row in snapshots),
        "premium_activity_contract_count": sum(activity(row) is not None for row in snapshots)}
    return dict(summary=summary, expirations=expirations, strikes=strikes, top_contracts=top_contracts)