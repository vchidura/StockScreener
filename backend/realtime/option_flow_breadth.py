"""Options Flow and Activity from the full listed chain (user, Nov 9).

The published Flow release covers only the streamed contracts (8 per underlying), so the pages showed eight
contracts per name. Breadth (G75) captures the listed chain each Options window (0-60 DTE, strikes within 20% of
spot) with the provider's day volume and open interest. At read time this rebuilds the page's per-underlying summary,
expiry and strike concentration and top contracts from those rows, in the page's existing shapes, and labels the
source. Flow events and alerts still come from the streamed contracts; nothing here is a detector input.
"""
from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from decimal import Decimal, InvalidOperation

BREADTH_FLOW_SOURCE = "BREADTH"
TOP_CONTRACT_LIMIT = 25
CONTRACT_MULTIPLIER = Decimal(100)
BREADTH_FLOW_DEFINITION = (
    "Volume and open interest are the provider's day volume and prior-settlement open interest for the full listed "
    "chain (0-60 DTE, strikes within 20% of spot), refreshed each Options window. Estimated premium activity is day "
    "volume times the current quote midpoint. Flow events and alerts come from the streamed contracts only."
)


def _decimal(value) -> Decimal | None:
    if value is None:
        return None
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return number if number.is_finite() else None


def _count(value) -> int:
    return int(value) if isinstance(value, int) and value > 0 else 0


def _money(value: Decimal) -> str:
    return format(value.quantize(Decimal("0.01")), "f")


def _premium(row) -> Decimal | None:
    mark = _decimal(row.get("mark"))
    return None if mark is None else Decimal(_count(row.get("day_volume"))) * mark * CONTRACT_MULTIPLIER


def _ratio(numerator: int, denominator: int) -> float | None:
    return round(numerator / denominator, 4) if denominator > 0 else None


def _sides(rows):
    calls = [row for row in rows if row["contract_type"] == "CALL"]
    puts = [row for row in rows if row["contract_type"] == "PUT"]
    return calls, puts


def _activity(rows):
    calls, puts = _sides(rows)
    call_premium = sum((_premium(row) or Decimal(0) for row in calls), Decimal(0))
    put_premium = sum((_premium(row) or Decimal(0) for row in puts), Decimal(0))
    return {
        "contract_count": len(rows),
        "call_volume": sum(_count(row.get("day_volume")) for row in calls),
        "put_volume": sum(_count(row.get("day_volume")) for row in puts),
        "call_open_interest": sum(_count(row.get("open_interest")) for row in calls),
        "put_open_interest": sum(_count(row.get("open_interest")) for row in puts),
        "call_premium_activity": _money(call_premium),
        "put_premium_activity": _money(put_premium),
    }


def _summary(underlying: str, rows, streamed: Mapping | None, capture: Mapping | None):
    calls, puts = _sides(rows)
    activity = _activity(rows)
    total_volume = activity["call_volume"] + activity["put_volume"]
    total_open_interest = activity["call_open_interest"] + activity["put_open_interest"]
    observed = (capture or {}).get("breadth_observed_at") or max(str(row["market_data_time"]) for row in rows)
    summary = dict(streamed or {})
    summary.update(
        underlying=underlying,
        asset_type=rows[0].get("asset_type") or summary.get("asset_type") or "STOCK",
        market_data_time=observed,
        received_row_count=len(rows),
        retained_row_count=len(rows),
        retention_fraction=1.0,
        spot=next((str(row["spot"]) for row in rows if row.get("spot")), summary.get("spot")),
        contract_count=len(rows),
        expiration_count=len({row["expiration_date"] for row in rows}),
        strike_count=len({row["strike"] for row in rows}),
        call_contract_count=len(calls),
        put_contract_count=len(puts),
        call_volume=activity["call_volume"],
        put_volume=activity["put_volume"],
        total_volume=total_volume,
        call_open_interest=activity["call_open_interest"],
        put_open_interest=activity["put_open_interest"],
        total_open_interest=total_open_interest,
        call_premium_activity=activity["call_premium_activity"],
        put_premium_activity=activity["put_premium_activity"],
        total_premium_activity=_money(
            Decimal(activity["call_premium_activity"]) + Decimal(activity["put_premium_activity"])
        ),
        premium_activity_contract_count=sum(_premium(row) is not None for row in rows),
        put_call_volume_ratio=_ratio(activity["put_volume"], activity["call_volume"]),
        put_call_open_interest_ratio=_ratio(activity["put_open_interest"], activity["call_open_interest"]),
        # Matched OI change needs two settlement captures of the same chain; not derived from breadth yet.
        open_interest_change=None,
        coverage_source=BREADTH_FLOW_SOURCE,
        streamed_contract_count=(streamed or {}).get("contract_count"),
    )
    return summary


def _top_contract(row):
    spot, strike = _decimal(row.get("spot")), _decimal(row.get("strike"))
    premium = _premium(row)
    ratio = _decimal(row.get("volume_open_interest_ratio"))
    return {
        "contract_id": int(row.get("contract_id") or 0),
        "contract_ticker": row["contract_ticker"],
        "contract_type": row["contract_type"],
        "expiration_date": row["expiration_date"],
        "calendar_dte": row["calendar_dte"],
        "strike": row["strike"],
        "spot": row.get("spot"),
        "day_volume": _count(row.get("day_volume")),
        "open_interest": row.get("open_interest"),
        "model_mark": row.get("mark"),
        "display_mark": row.get("mark"),
        "local_iv": row.get("local_iv"),
        "local_delta": row.get("local_delta"),
        "volume_open_interest_ratio": None if ratio is None else float(ratio),
        "premium_activity": None if premium is None else _money(premium),
        "moneyness_fraction": (
            "0" if not spot or strike is None else format(abs(strike - spot) / spot, "f")
        ),
        "coverage_source": BREADTH_FLOW_SOURCE,
    }


def breadth_flow_data(
    data: Mapping[str, object],
    rows: Sequence[Mapping[str, object]],
    captures: Mapping[str, Mapping[str, object]],
) -> dict[str, object]:
    """The Flow data with breadth in place of the streamed contracts for every underlying with a COMPLETE capture (the
    last good one when a later capture failed, R48: the capture label then names the failed one); underlyings without
    one keep their streamed rows. When the selected one has no breadth, the streamed data is served and
    `breadth_fallback` says so (R48: the page warns instead of switching silently)."""
    by_underlying = defaultdict(list)
    for row in rows:
        by_underlying[str(row["underlying"])].append(row)
    usable = {
        underlying for underlying, items in by_underlying.items()
        if items and (captures.get(underlying) or {}).get("breadth_state") == "COMPLETE"
    }
    selected = str(data.get("selected") or "")
    streamed = {str(item["underlying"]): item for item in data.get("underlyers") or ()}
    streamed_only = sorted(underlying for underlying in streamed if underlying not in usable)
    if selected not in usable:
        result = dict(data)
        result.update(
            breadth=dict(captures.get(selected) or {"breadth_state": "NOT_CAPTURED"}),
            breadth_fallback={"selected": True, "streamed_only": streamed_only},
        )
        return result
    summaries = [
        _summary(underlying, by_underlying[underlying], streamed.get(underlying), captures.get(underlying))
        if underlying in usable else dict(item)
        for underlying, item in streamed.items()
    ]
    chain = by_underlying[selected]
    expirations = []
    for expiration, items in sorted(_group(chain, lambda row: row["expiration_date"]).items()):
        expirations.append({"expiration_date": expiration, "calendar_dte": items[0]["calendar_dte"], **_activity(items)})
    strikes = []
    for (expiration, strike), items in sorted(
        _group(chain, lambda row: (row["expiration_date"], row["strike"])).items(),
        key=lambda item: (item[0][0], Decimal(item[0][1])),
    ):
        activity = _activity(items)
        strikes.append({
            "expiration_date": expiration, "strike": strike, "contract_count": activity["contract_count"],
            **{key: activity[key] for key in ("call_volume", "put_volume", "call_open_interest", "put_open_interest")},
        })
    ranked = sorted(
        chain,
        key=lambda row: (_premium(row) or Decimal(-1), _count(row.get("day_volume")), row["contract_ticker"]),
        reverse=True,
    )[:TOP_CONTRACT_LIMIT]
    result = dict(data)
    result.update(
        underlyers=summaries,
        selected_summary=next(item for item in summaries if item["underlying"] == selected),
        expirations=expirations,
        strikes=strikes,
        top_contracts=[_top_contract(row) for row in ranked],
        coverage_source=BREADTH_FLOW_SOURCE,
        breadth=dict(captures.get(selected) or {}),
        breadth_fallback={"selected": False, "streamed_only": streamed_only} if streamed_only else None,
        definitions={**dict(data.get("definitions") or {}), "coverage_source": BREADTH_FLOW_DEFINITION},
    )
    return result


def _group(rows, key):
    groups = defaultdict(list)
    for row in rows:
        groups[key(row)].append(row)
    return groups
