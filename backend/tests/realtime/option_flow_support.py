"""Shared builders for the option flow tests (no provider or database I/O)."""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID

from options.config import FlowStrategyPolicy
from realtime.advanced_options_flow_policy import (
    AdvancedOptionsContractScope,
    AdvancedOptionsContractScopeEntry,
    load_advanced_options_flow_policy,
)
from realtime.option_flow.messages import underlyer_of
from realtime.option_flow.conditions import ConditionSemantics, load_semantics
from realtime.option_flow.engine import EngineConfig, OptionFlowEngine

FIXTURES = Path(__file__).resolve().parent / "fixtures"
PAYLOAD = FIXTURES / "option_trade_conditions_synthetic_v1.json"
REVIEW = FIXTURES / "option_trade_conditions_synthetic_v1.review.json"
COHORT = UUID("11111111-2222-3333-4444-555555555555")
JOURNAL_GENERATION = UUID("66666666-7777-8888-9999-000000000000")

CALL = "O:SPY261019C00650000"
PUT = "O:SPY261019P00600000"
OTHER = "O:QQQ261019C00500000"

# 2026-10-12 10:30:00 in New York.
T0 = int(datetime(2026, 10, 12, 14, 30, tzinfo=timezone.utc).timestamp() * 1000)

REGULAR = 9001
SWEEP = 9003
FLOOR = 9004
CROSS = 9005
MULTI_LEG = 9006
LATE = 9008
NON_VOLUME = 9010
CANCEL = 9011
UNREVIEWED = 9013
UNKNOWN_CODE = 9999


def semantics() -> ConditionSemantics:
    return load_semantics(PAYLOAD, REVIEW)


def policy(**overrides) -> FlowStrategyPolicy:
    values = dict(
        minimum_print_notional=Decimal("50000"),
        minimum_sweep_prints=3,
        minimum_distinct_exchanges=2,
        sweep_window_seconds=180,
        minimum_volume_oi_ratio=3.0,
        maximum_candidates=20,
    )
    values.update(overrides)
    return FlowStrategyPolicy(**values)


def advanced_policy():
    return load_advanced_options_flow_policy()


def advanced_contract_scope(
    contracts,
    *,
    session_date=date(2026, 10, 12),
    flow_policy=None,
):
    flow_policy = flow_policy or advanced_policy()
    entries = []
    for contract in contracts:
        underlyer = underlyer_of(contract)
        expiration = datetime.strptime(
            contract.split(":")[1][len(underlyer):len(underlyer) + 6],
            "%y%m%d",
        ).date()
        lane = flow_policy.lane_for((expiration - session_date).days)
        if lane is None:
            raise ValueError("test contract is outside reviewed DTE lanes")
        entries.append(AdvancedOptionsContractScopeEntry(
            contract,
            underlyer,
            expiration,
            flow_policy.tier_for(underlyer),
            lane,
        ))
    return AdvancedOptionsContractScope(
        version="advanced-options-contract-scope-v1",
        session_date=session_date,
        manifest_sha256="1" * 64,
        slot_sha256="2" * 64,
        flow_policy_sha256=flow_policy.sha256,
        contracts=tuple(entries),
    )


def config(**overrides) -> EngineConfig:
    values = dict(
        cohort_id=COHORT,
        provider_tier="MASSIVE_ADVANCED",
        policy=policy(),
        semantics=semantics(),
        journal_generation=JOURNAL_GENERATION,
        shard_count=2,
        allow_synthetic_semantics=True,
    )
    values.update(overrides)
    return EngineConfig(**values)


def engine(prior_open_interest=None, **overrides) -> OptionFlowEngine:
    return OptionFlowEngine(config(**overrides), prior_open_interest)


def trade(symbol=CALL, seq=1, time_ms=T0, price=5.0, size=100, conditions=(REGULAR,), exchange=302):
    message = {"ev": "T", "sym": symbol, "p": price, "s": size, "c": list(conditions), "t": time_ms, "q": seq}
    if exchange is not None:
        message["x"] = exchange
    return message


def quote(symbol=CALL, seq=1, time_ms=T0 - 100, bid=4.9, ask=5.0, bid_size=10, ask_size=12):
    return {"ev": "Q", "sym": symbol, "bx": 301, "ax": 302, "bp": bid, "ap": ask, "bs": bid_size,
            "as": ask_size, "t": time_ms, "q": seq}


def run(eng: OptionFlowEngine, messages, *, first_offset: int = 1, final: bool = True):
    """Feed messages in order and return one merged output (flushed at the end by default)."""
    from realtime.option_flow.records import EngineOutput

    merged = EngineOutput()
    for index, message in enumerate(messages):
        merged.extend(eng.feed(message, first_offset + index))
    if final:
        merged.extend(eng.flush(final=True))
    return merged


def check_event_rules(event) -> None:
    """The rt.option_flow_events CHECKs, restated so offline tests catch a violating row."""
    assert event.contract_ticker == event.contract_ticker.upper() and event.underlyer == event.underlyer.upper()
    assert event.source_seq >= 0 and event.price > 0 and event.size > 0 and event.multiplier > 0
    assert event.premium >= 0
    assert event.trade_treatment in ("INCLUDED", "EXCLUDED", "CANCELED", "UNKNOWN")
    assert event.side_class in ("AT_OR_ABOVE_ASK", "AT_OR_BELOW_BID", "INSIDE_SPREAD", "QUOTE_CROSSED_OR_LOCKED",
                                "QUOTE_STALE", "NO_QUOTE", "AGGRESSION_UNAVAILABLE_NO_QUOTE", "EXCLUDED_COMPLEX",
                                "NOT_CLASSIFIED")
    assert event.side_confidence in ("HIGH", "MEDIUM", "LOW", "NONE")
    if event.side_class in ("AT_OR_ABOVE_ASK", "AT_OR_BELOW_BID", "INSIDE_SPREAD"):
        assert None not in (event.quote_bid, event.quote_ask, event.quote_time)
        assert event.side_confidence != "NONE"
    if event.quote_time is not None:
        assert event.quote_time <= event.event_time
    if event.cumulative_volume_before is not None and event.cumulative_volume_after is not None:
        assert event.cumulative_volume_after >= event.cumulative_volume_before
    assert event.volume_baseline_kind in ("SESSION_COMPLETE", "SNAPSHOT_RECONCILED", "WARMING", "INCOMPLETE")
    if event.volume_exceeds_prior_oi:
        assert event.prior_open_interest is not None and event.open_interest_as_of is not None
        assert event.cumulative_volume_after > event.prior_open_interest
        assert event.volume_baseline_kind in ("SESSION_COMPLETE", "SNAPSHOT_RECONCILED")
    assert event.opening_inference in ("NOT_ASSESSED", "LIKELY_OPENING", "UNLIKELY_OPENING", "INDETERMINATE")
    assert event.opening_inference != "LIKELY_OPENING" or event.volume_exceeds_prior_oi
    assert event.spread_position is None or (0 <= event.spread_position <= 1 and event.side_class == "INSIDE_SPREAD")
    assert (event.side_class == "INSIDE_SPREAD") == (event.spread_position is not None)
    assert event.opening_score is None or (0 <= event.opening_score <= 1 and event.opening_inference != "NOT_ASSESSED")
    assert event.opening_inference == "NOT_ASSESSED" or len(event.opening_reasons) > 0
    if event.opening_inference == "NOT_ASSESSED":
        assert event.opening_score is None and event.opening_reasons == ()
    else:
        assert event.opening_score is not None and event.opening_reasons[-1] == "HEURISTIC_UNCALIBRATED_V1"
    assert event.revision >= 1 and (event.revision == 1) == (event.supersedes_event_id is None)
    assert event.source_offset is None or event.source_offset > 0


def check_cluster_rules(row) -> None:
    """The rt.option_flow_clusters CHECKs."""
    from datetime import timedelta

    assert row.revision >= 1 and row.window_seconds > 0
    assert row.window_end - row.window_start == timedelta(seconds=row.window_seconds)
    assert row.side_class in ("AT_OR_ABOVE_ASK", "AT_OR_BELOW_BID", "INSIDE_SPREAD", "MIXED", "NO_QUOTE",
                              "AGGRESSION_UNAVAILABLE_NO_QUOTE", "NOT_CLASSIFIED")
    assert row.status in ("PROVISIONAL", "FINAL", "RETRACTED") and len(row.policy_sha256) == 64
    if row.status == "RETRACTED":
        assert row.revision > 1
        assert (row.print_count, row.total_size, row.total_premium, row.distinct_exchanges) == (0, 0, 0, 0)
        assert row.member_event_ids == ()
    else:
        assert row.print_count >= 1 and row.total_size >= row.print_count and row.total_premium >= 0
        assert 1 <= row.distinct_exchanges <= row.print_count
        assert len(row.member_event_ids) == row.print_count
        assert len(set(row.member_event_ids)) == row.print_count
    assert row.first_event_time >= row.window_start and row.last_event_time <= row.window_end
    assert row.last_event_time >= row.first_event_time
    assert row.contract_ticker == row.contract_ticker.upper()
