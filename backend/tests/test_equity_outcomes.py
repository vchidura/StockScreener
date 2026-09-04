import json
import sys
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from equity.domain import (
    BarAvailabilityMode,
    BarSourceKind,
    EquityBarRevision,
    EquityEvidence,
    EvidenceRole,
    EvidenceType,
    LifecycleStatus,
    QualityState,
)
from equity.outcomes import (
    default_directional_policy,
    evaluate_directional_outcome,
    recommendation_plan_policy,
)
from equity.polygon import canonical_json, sha256_json


UTC = timezone.utc
SIGNAL_MARKET_TIME = datetime(2026, 8, 28, 14, 0, tzinfo=UTC)
SIGNAL_OBSERVED_TIME = SIGNAL_MARKET_TIME + timedelta(seconds=2)


def subject(stop="99", target="103"):
    payload = {"stop_price": stop, "target_price": target}
    return EquityEvidence(
        evidence_id=uuid4(), evidence_key=f"event:{uuid4()}", lifecycle_key=None,
        evidence_type=EvidenceType.SCANNER_RESULT,
        evidence_role=EvidenceRole.DIRECTION, security_id=uuid4(), ticker="AAPL",
        interval="30m", direction=1, lifecycle_status=LifecycleStatus.MATCH,
        strength=None, market_time=SIGNAL_MARKET_TIME,
        observed_at=SIGNAL_OBSERVED_TIME,
        valid_until=SIGNAL_MARKET_TIME + timedelta(hours=2),
        source_name="breakout_expansion", source_version="1.0",
        payload_schema_version="1.0", analysis_run_id=uuid4(),
        latest_bar_revision_id=uuid4(), security_revision_id=uuid4(),
        fundamental_report_ids=(), source_revision_ids=(),
        quality_state=QualityState.RESEARCH_ONLY, quality_codes=(),
        qualification_revision_id=None, payload_json=canonical_json(payload),
        payload_sha256=sha256_json(payload),
    )


def bar(start, open_price, high, low, close, *, ticker="AAPL"):
    return EquityBarRevision(
        bar_revision_id=uuid4(), security_id=uuid4(), ticker=ticker,
        interval="30m", session_date=start.date(), bar_start=start,
        bar_end=start + timedelta(minutes=30), open_price=Decimal(str(open_price)),
        high_price=Decimal(str(high)), low_price=Decimal(str(low)),
        close_price=Decimal(str(close)), volume=Decimal("1000"), vwap=None,
        transaction_count=100, source_kind=BarSourceKind.NATIVE_REST,
        availability_mode=BarAvailabilityMode.LIVE_OBSERVED, is_final=True,
        system_observed_at=start + timedelta(minutes=30), replay_available_at=None,
        adjusted=False, payload_sha256="a" * 64,
    )


def policy():
    return default_directional_policy(
        source_name="breakout_expansion", source_version="1.0", interval="30m",
        horizons={"60m": 2},
        effective_from=datetime(2026, 8, 1, tzinfo=UTC),
    )


def plan_policy(horizons=None):
    return recommendation_plan_policy(
        source_name="breakout_expansion", source_version="1.0", interval="30m",
        horizons=horizons or {"60m": 2},
        effective_from=datetime(2026, 8, 1, tzinfo=UTC),
    )


def test_outcome_enters_first_bar_strictly_after_observation_and_computes_alpha():
    bars = (
        bar(datetime(2026, 8, 28, 14, 0, tzinfo=UTC), 90, 91, 89, 90),
        bar(datetime(2026, 8, 28, 14, 30, tzinfo=UTC), 100, 102, 99.5, 101),
        bar(datetime(2026, 8, 28, 15, 0, tzinfo=UTC), 101, 103, 100, 102),
    )
    benchmark = (
        bar(datetime(2026, 8, 28, 14, 30, tzinfo=UTC), 200, 201, 199, 200, ticker="SPY"),
        bar(datetime(2026, 8, 28, 15, 0, tzinfo=UTC), 200, 202, 199, 201, ticker="SPY"),
    )

    result = evaluate_directional_outcome(
        subject(stop="95", target="110"), policy(), "60m", bars,
        market_bars=benchmark,
    )

    assert result.entry_time == datetime(2026, 8, 28, 14, 30, tzinfo=UTC)
    assert result.entry_price == Decimal("100")
    assert result.exit_price == Decimal("102")
    assert round(result.net_return, 6) == 0.0196
    assert round(result.market_return, 6) == 0.005
    assert round(result.net_alpha, 6) == 0.0146


def test_intraday_entry_skips_the_bar_that_opens_at_the_signal_instant():
    """observed_at equals the signal bar end; that bar's successor is not tradable yet."""
    signal_end = datetime(2026, 8, 28, 14, 30, tzinfo=UTC)
    intraday_subject = replace(
        subject(stop="95", target="110"),
        market_time=signal_end, observed_at=signal_end,
    )
    bars = (
        bar(signal_end, 100, 102, 99.5, 101),
        bar(datetime(2026, 8, 28, 15, 0, tzinfo=UTC), 101, 103, 100, 102),
        bar(datetime(2026, 8, 28, 15, 30, tzinfo=UTC), 102, 104, 101, 103),
    )

    result = evaluate_directional_outcome(
        intraday_subject, policy(), "60m", bars,
    )

    assert result.entry_status == "ENTERED"
    assert result.entry_time > signal_end
    assert result.entry_price == Decimal("101")


def test_same_bar_stop_and_target_is_loss_under_primary_policy():
    bars = (
        bar(datetime(2026, 8, 28, 14, 30, tzinfo=UTC), 100, 104, 98, 101),
        bar(datetime(2026, 8, 28, 15, 0, tzinfo=UTC), 101, 102, 100, 101),
    )

    result = evaluate_directional_outcome(subject(), policy(), "60m", bars)

    assert result.stop_hit is True
    assert result.target_hit is True
    assert result.first_hit == "SAME_BAR"
    assert result.outcome_category == "LOSS"
    assert "SAME_BAR_PATH_AMBIGUOUS" in result.quality_codes


def test_incomplete_horizon_is_persisted_as_unavailable_coverage():
    bars = (
        bar(datetime(2026, 8, 28, 14, 30, tzinfo=UTC), 100, 101, 99, 100),
    )

    result = evaluate_directional_outcome(
        subject(), policy(), "60m", bars,
        market_benchmark_ticker="SPY", sector_benchmark_ticker="SMH",
    )

    assert result.entry_status == "UNAVAILABLE"
    assert result.outcome_category == "UNAVAILABLE"
    assert result.net_return is None
    assert result.path_bar_ids == (bars[0].bar_revision_id,)
    assert result.quality_codes == ("OUTCOME_HORIZON_INCOMPLETE",)
    assert result.market_benchmark_ticker == "SPY"
    assert result.sector_benchmark_ticker == "SMH"


def test_daily_outcome_rejects_first_bar_after_missing_entry_session():
    signal_time = datetime(2026, 8, 28, 20, tzinfo=UTC)
    daily_subject = replace(
        subject(stop="95", target="110"),
        interval="1d",
        market_time=signal_time,
        observed_at=signal_time,
        valid_until=signal_time + timedelta(days=30),
    )
    late_start = datetime(2026, 9, 1, 13, 30, tzinfo=UTC)
    late_bars = tuple(
        replace(
            bar(late_start + timedelta(days=index), 100, 102, 99, 101),
            interval="1d",
            session_date=(late_start + timedelta(days=index)).date(),
            bar_end=(late_start + timedelta(days=index)).replace(
                hour=20, minute=0
            ),
            system_observed_at=(late_start + timedelta(days=index)).replace(
                hour=20, minute=0
            ),
        )
        for index in range(2)
    )
    daily_policy = default_directional_policy(
        source_name="breakout_expansion", source_version="1.0", interval="1d",
        horizons={"2d": 2}, effective_from=datetime(2026, 8, 1, tzinfo=UTC),
    )

    result = evaluate_directional_outcome(
        daily_subject, daily_policy, "2d", late_bars,
        market_benchmark_ticker="SPY", sector_benchmark_ticker="SMH",
    )

    assert result.entry_status == "UNAVAILABLE"
    assert result.quality_codes == ("NEXT_DAILY_ENTRY_SESSION_MISSING",)
    assert result.entry_time is None


def test_entry_outside_required_bracket_is_not_triggered():
    bars = (
        bar(datetime(2026, 8, 28, 14, 30, tzinfo=UTC), 104, 105, 103, 104),
        bar(datetime(2026, 8, 28, 15, 0, tzinfo=UTC), 104, 105, 103, 104),
    )
    payload = {
        "stop_price": "99", "target_price": "103",
        "require_entry_between_stop_target": True,
    }
    bracketed = replace(
        subject(), payload_json=canonical_json(payload),
        payload_sha256=sha256_json(payload),
    )

    result = evaluate_directional_outcome(bracketed, policy(), "60m", bars)

    assert result.entry_status == "NOT_TRIGGERED"
    assert result.outcome_category == "NOT_ENTERED"
    assert result.quality_codes == ("ENTRY_OUTSIDE_BRACKET",)


def test_outcome_policy_requires_named_positive_horizon_mapping():
    with pytest.raises(ValueError, match="non-empty JSON object"):
        replace(policy(), horizons_json="[]")
    with pytest.raises(ValueError, match="positive bar counts"):
        replace(policy(), horizons_json=json.dumps({"5d": 0}))


def test_recommendation_plan_exits_at_first_target_before_horizon():
    bars = (
        bar(datetime(2026, 8, 28, 14, 30, tzinfo=UTC), 100, 103.5, 99.5, 102),
        bar(datetime(2026, 8, 28, 15, 0, tzinfo=UTC), 98, 99, 97, 98),
    )

    result = evaluate_directional_outcome(
        subject(stop="99", target="103"),
        plan_policy(horizons={"90m": 3}),
        "90m",
        bars,
    )

    assert result.entry_status == "ENTERED"
    assert result.first_hit == "TARGET"
    assert result.stop_hit is False
    assert result.target_hit is True
    assert result.exit_bar_id == bars[0].bar_revision_id
    assert result.exit_price == Decimal("103")
    assert result.path_bar_ids == (bars[0].bar_revision_id,)
    assert result.net_return == pytest.approx(0.0296)


def test_recommendation_plan_same_bar_uses_conservative_stop_exit():
    bars = (
        bar(datetime(2026, 8, 28, 14, 30, tzinfo=UTC), 100, 104, 98, 101),
        bar(datetime(2026, 8, 28, 15, 0, tzinfo=UTC), 101, 102, 100, 101),
    )

    result = evaluate_directional_outcome(subject(), plan_policy(), "60m", bars)

    assert result.first_hit == "SAME_BAR"
    assert result.exit_price == Decimal("99")
    assert result.outcome_category == "LOSS"
    assert result.net_return == pytest.approx(-0.0104)
    assert "SAME_BAR_PATH_AMBIGUOUS" in result.quality_codes


def test_recommendation_plan_stop_gap_uses_observable_open():
    bars = (
        bar(datetime(2026, 8, 28, 14, 30, tzinfo=UTC), 100, 102, 99.5, 101),
        bar(datetime(2026, 8, 28, 15, 0, tzinfo=UTC), 97, 98, 96, 97),
    )

    result = evaluate_directional_outcome(
        subject(stop="99", target="110"),
        plan_policy(horizons={"90m": 3}),
        "90m",
        bars,
    )

    assert result.first_hit == "STOP"
    assert result.exit_price == Decimal("97")
    assert result.path_bar_ids == tuple(row.bar_revision_id for row in bars)
    assert result.net_return == pytest.approx(-0.0304)


def test_recommendation_plan_rejects_missing_or_wrong_sided_bracket():
    bars = (
        bar(datetime(2026, 8, 28, 14, 30, tzinfo=UTC), 100, 102, 99, 101),
        bar(datetime(2026, 8, 28, 15, 0, tzinfo=UTC), 101, 102, 100, 101),
    )

    missing = evaluate_directional_outcome(
        subject(stop=None, target=None), plan_policy(), "60m", bars
    )
    wrong_sided = evaluate_directional_outcome(
        subject(stop="101", target="99"), plan_policy(), "60m", bars
    )

    assert missing.entry_status == "NOT_TRIGGERED"
    assert wrong_sided.entry_status == "NOT_TRIGGERED"
    assert missing.quality_codes == ("PLAN_BRACKET_INVALID",)
    assert wrong_sided.quality_codes == ("PLAN_BRACKET_INVALID",)


def test_recommendation_plan_falls_back_to_horizon_close_without_hit():
    bars = (
        bar(datetime(2026, 8, 28, 14, 30, tzinfo=UTC), 100, 101, 99.5, 100.5),
        bar(datetime(2026, 8, 28, 15, 0, tzinfo=UTC), 100.5, 102, 100, 101),
    )

    result = evaluate_directional_outcome(
        subject(stop="95", target="110"), plan_policy(), "60m", bars
    )

    assert result.first_hit == "NONE"
    assert result.exit_bar_id == bars[-1].bar_revision_id
    assert result.exit_price == Decimal("101")
    assert result.net_return == pytest.approx(0.0096)


def test_shared_market_sector_bars_are_not_duplicated_in_lineage():
    bars = (
        bar(datetime(2026, 8, 28, 14, 30, tzinfo=UTC), 100, 101, 99.5, 100.5),
        bar(datetime(2026, 8, 28, 15, 0, tzinfo=UTC), 100.5, 102, 100, 101),
    )

    result = evaluate_directional_outcome(
        subject(stop="95", target="110"), policy(), "60m", bars,
        market_bars=bars, sector_bars=bars,
        market_benchmark_ticker="SPY", sector_benchmark_ticker="SPY",
    )

    assert result.benchmark_bar_ids == tuple(row.bar_revision_id for row in bars)