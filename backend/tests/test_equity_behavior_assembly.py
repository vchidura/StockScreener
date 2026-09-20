from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import numpy as np
import pytest
from pydantic import ValidationError

from equity.behavior import BehaviorSource, StockBehaviorSnapshot
from equity.behavior_assembly import (
    FinalizedBarSeries,
    assemble_interval_components,
    assemble_momentum_component,
    assemble_participation_component,
    assemble_relative_strength_component,
    assemble_stock_behavior_snapshot,
    assemble_trend_component,
    finalized_series_from_revisions,
)
from equity.domain import (
    BarAvailabilityMode, BarSessionScope, BarSourceKind, DecisionWatermark, EquityBarRevision,
)
from test_equity_behavior import NOW


def bar_series(length=200, *, received_at=NOW, valid_until=None, adjusted=False,
               interval="30m", security_id=None):
    identity = security_id or SECURITY
    bar_ends = tuple(NOW - timedelta(minutes=30 * (length - index)) for index in range(length))
    close = np.arange(100.0, 100.0 + length)
    source = BehaviorSource(
        evidence_id=uuid4(), security_id=identity, interval=interval,
        payload_sha256="a" * 64, policy_sha256="b" * 64,
        market_time=bar_ends[-1], observed_at=received_at, recorded_at=received_at,
        received_at=received_at, valid_until=valid_until or NOW + timedelta(hours=1),
        price_basis="REVIEWED_SPLIT_ADJUSTED" if adjusted else "RAW_ACTION_GATED",
        action_review_ids=("split-review",) if adjusted else (),
        availability_mode="PROSPECTIVE_RECEIPT",
    )
    return FinalizedBarSeries(
        security_id=identity, ticker="SPY", interval=interval,
        bar_revision_ids=tuple(uuid4() for _ in range(length)),
        bar_payload_sha256s=tuple(f"{index:064x}" for index in range(length)),
        bar_ends=bar_ends, bar_available_ats=bar_ends,
        high=tuple(close + 1), low=tuple(close - 1), close=tuple(close),
        volume=tuple(np.arange(1.0, length + 1)),
        source=source, adjusted=adjusted,
    )


SECURITY = uuid4()
DECISION = DecisionWatermark(NOW - timedelta(minutes=30), NOW)


def test_causal_finalized_series_builds_measurement_ready_trend_without_state():
    component = assemble_trend_component(bar_series(), DECISION)

    assert component.status == "READY"
    assert component.state is None and component.classification_policy_sha256 is None
    assert [metric.definition.metric_id for metric in component.metrics] == ["ema50_slope10_atr", "adx14"]
    assert all(metric.status == "READY" for metric in component.metrics)
    assert len(component.sources) == 1


def test_short_causal_history_retains_provenance_and_explicit_metrics():
    component = assemble_trend_component(bar_series(199), DECISION)

    assert component.status == "INSUFFICIENT_HISTORY"
    assert component.reason_codes == ("INSUFFICIENT_FINALIZED_BARS",)
    assert all(metric.status == "INSUFFICIENT_HISTORY" for metric in component.metrics)
    assert len(component.sources) == 1 and component.state is None


def test_source_received_after_decision_is_unavailable_before_calculation(monkeypatch):
    series = bar_series(received_at=NOW + timedelta(seconds=1))
    monkeypatch.setattr("equity.behavior_assembly.calculate_trend_metrics", lambda *args: pytest.fail("calculator must not run"))

    component = assemble_trend_component(series, DECISION)

    assert component.status == "UNAVAILABLE"
    assert component.reason_codes == ("SOURCE_RECEIPT_AFTER_CUTOFF",)
    assert component.metrics == () and component.sources == () and component.state is None


def test_source_market_time_after_decision_is_unavailable():
    component = assemble_trend_component(bar_series(), DecisionWatermark(NOW - timedelta(minutes=31), NOW))

    assert component.status == "UNAVAILABLE"
    assert component.reason_codes == ("SOURCE_MARKET_TIME_AFTER_CUTOFF",)
    assert component.metrics == () and component.sources == ()


def test_expired_source_exposes_no_metrics_or_state():
    component = assemble_trend_component(bar_series(valid_until=NOW), DECISION)

    assert component.status == "STALE" and component.reason_codes == ("SOURCE_EXPIRED_AT_CUTOFF",)
    assert component.metrics == () and component.state is None and len(component.sources) == 1


@pytest.mark.parametrize("mutation", ["length", "duplicate", "order", "availability", "identity", "market", "basis", "volume"])
def test_series_rejects_ambiguous_or_mismatched_lineage(mutation):
    payload = bar_series().model_dump(mode="python")
    if mutation == "length":
        payload["high"] = payload["high"][:-1]
    if mutation == "duplicate":
        payload["bar_revision_ids"] = (payload["bar_revision_ids"][0],) * len(payload["bar_revision_ids"])
    if mutation == "order":
        payload["bar_ends"] = tuple(reversed(payload["bar_ends"]))
    if mutation == "availability":
        payload["bar_available_ats"] = tuple(value - timedelta(seconds=1) for value in payload["bar_ends"])
    if mutation == "identity":
        payload["security_id"] = uuid4()
    if mutation == "market":
        payload["source"]["market_time"] = NOW - timedelta(hours=2)
    if mutation == "basis":
        payload["adjusted"] = True
    if mutation == "volume":
        payload["volume"] = (*payload["volume"][:-1], -1)
    with pytest.raises(ValidationError):
        FinalizedBarSeries.model_validate(payload)


def test_adjusted_series_requires_reviewed_basis_and_action_identity():
    assert bar_series(adjusted=True).source.action_review_ids == ("split-review",)


def test_interval_assembly_builds_each_applicable_factor_without_states():
    components = assemble_interval_components(bar_series(), DECISION)

    assert [component.factor for component in components] == ["TREND", "MOMENTUM", "VOLATILITY", "LOCATION"]
    assert all(component.status == "READY" and component.state is None for component in components)
    assert {metric.definition.metric_id for component in components for metric in component.metrics} == {
        "ema50_slope10_atr", "adx14", "return5", "momentum_change5",
        "atr14_fraction", "compression_tr5_20", "extension_ema21_atr", "prior_range20_position",
    }


def test_daily_assembly_includes_participation_and_daily_volatility_metrics():
    daily = bar_series(273, interval="1d")
    components = assemble_interval_components(daily, DECISION)

    assert [component.factor for component in components] == ["TREND", "MOMENTUM", "VOLATILITY", "PARTICIPATION", "LOCATION"]
    volatility = next(component for component in components if component.factor == "VOLATILITY")
    assert {metric.definition.metric_id for metric in volatility.metrics} == {
        "atr14_fraction", "rv20_cc_annual", "rv20_percentile252", "compression_tr5_20",
    }


def test_partial_metric_history_keeps_ready_metric_visible_without_factor_state():
    payload = __import__("test_equity_behavior").snapshot_payload()
    series = bar_series(6, security_id=UUID(payload["security_id"]))
    component = assemble_momentum_component(series, DECISION)
    assert component.status == "INSUFFICIENT_HISTORY"
    assert [metric.status for metric in component.metrics] == ["READY", "INSUFFICIENT_HISTORY"]

    payload["components"].append(component.model_dump(mode="json"))
    snapshot = StockBehaviorSnapshot.model_validate(payload)
    assessed = next(item for item in snapshot.assess_at(NOW).components if item.key == "MOMENTUM.30m")
    assert assessed.status == "INSUFFICIENT_HISTORY" and assessed.state is None
    assert [metric.definition.metric_id for metric in assessed.metrics] == ["return5"]


def test_relative_strength_requires_distinct_paired_window_and_basis():
    own = bar_series(21, interval="1d")
    benchmark_payload = own.model_dump(mode="python")
    benchmark_payload["security_id"] = uuid4()
    benchmark_payload["source"]["security_id"] = benchmark_payload["security_id"]
    benchmark_payload["source"]["evidence_id"] = uuid4()
    benchmark = FinalizedBarSeries.model_validate(benchmark_payload)
    component = assemble_relative_strength_component(own, benchmark, DECISION)
    assert component.status == "READY" and len(component.sources) == 2
    assert component.metrics[0].benchmark_security_id == benchmark.security_id

    mismatched = benchmark.model_copy(update={"bar_ends": tuple(
        value - timedelta(days=1) for value in benchmark.bar_ends
    )})
    component = assemble_relative_strength_component(own, mismatched, DECISION)
    assert component.status == "UNAVAILABLE"
    assert component.reason_codes == ("BENCHMARK_WINDOW_OR_BASIS_MISMATCH",)


def test_participation_is_not_applicable_outside_daily_interval():
    component = assemble_participation_component(bar_series(), DECISION)
    assert component.status == "NOT_APPLICABLE" and component.metrics == () and component.sources == ()


def profile_series(**overrides):
    values = {
        "1d": bar_series(273, interval="1d"),
        "1h": bar_series(interval="1h"),
        "30m": bar_series(),
    }
    values.update(overrides)
    return values


def assemble_profile(series_by_interval=None, **overrides):
    values = dict(
        security_id=SECURITY, security_revision_id=uuid4(), ticker="SPY",
        computed_at=NOW, available_at=NOW, valid_until=NOW + timedelta(minutes=30),
    )
    values.update(overrides)
    return assemble_stock_behavior_snapshot(series_by_interval or profile_series(), DECISION, **values)


def test_profile_assembly_is_deterministic_ready_and_unclassified():
    revision = uuid4()
    series = profile_series()
    first = assemble_profile(series, security_revision_id=revision)
    second = assemble_profile(series, security_revision_id=revision)

    assert first == second and first.snapshot_id == second.snapshot_id
    assert first.assess_at(NOW).data_status == "READY"
    assert first.assess_at(NOW).alignment_state == "PARTIAL"
    assert all(component.state is None for component in first.components)
    assert set(first.required_components) == {"TREND.1d", "TREND.1h", "TREND.30m"}
    assert len(first.components) == 13


def test_missing_required_interval_is_explicit_partial_not_omitted():
    snapshot = assemble_profile(profile_series(**{"1h": None}))
    missing = next(component for component in snapshot.components if component.key == "TREND.1h")

    assert missing.status == "UNAVAILABLE" and missing.reason_codes == ("SOURCE_INTERVAL_MISSING",)
    assert snapshot.assess_at(NOW).data_status == "PARTIAL"


@pytest.mark.parametrize("mutation", ["keys", "identity", "interval", "clock", "validity"])
def test_profile_rejects_ambiguous_identity_clock_or_basis(mutation):
    series = profile_series()
    arguments = {}
    if mutation == "keys":
        series.pop("1h")
    if mutation == "identity":
        series["1h"] = bar_series(interval="1h", security_id=uuid4())
    if mutation == "interval":
        series["1h"] = bar_series(interval="30m")
    if mutation == "clock":
        arguments["computed_at"] = NOW - timedelta(seconds=1)
    if mutation == "validity":
        arguments["valid_until"] = NOW + timedelta(hours=2)
    with pytest.raises((ValueError, ValidationError)):
        assemble_profile(series, **arguments)


def test_profile_allows_component_local_adjusted_daily_and_raw_intraday_bases():
    series = profile_series()
    adjusted = bar_series(273, interval="1d", adjusted=True)
    adjusted_source = adjusted.source.model_copy(update={
        "price_basis": "PROVIDER_SPLIT_ADJUSTED",
        "action_review_ids": (),
        "source_manifest_sha256": "f" * 64,
        "history_mode": "RECONSTRUCTED_HISTORY",
        "history_available_at": NOW,
    })
    series["1d"] = adjusted.model_copy(update={"source": adjusted_source})
    snapshot = assemble_profile(series)

    assert snapshot.availability_mode == "PROSPECTIVE_RECEIPT"
    daily = next(component for component in snapshot.components if component.key == "TREND.1d")
    assert daily.sources[0].price_basis == "PROVIDER_SPLIT_ADJUSTED"
    assert daily.sources[0].history_mode == "RECONSTRUCTED_HISTORY"
    assert snapshot.assess_at(NOW).data_status == "READY"


def test_reconstructed_source_cannot_be_promoted_to_prospective_profile():
    series = profile_series()
    payload = series["1d"].model_dump(mode="python")
    payload["source"]["availability_mode"] = "RECONSTRUCTED"
    payload["source"]["history_mode"] = "RECONSTRUCTED_HISTORY"
    payload["source"]["history_available_at"] = NOW
    series["1d"] = FinalizedBarSeries.model_validate(payload)
    snapshot = assemble_profile(series)

    assert snapshot.availability_mode == "RECONSTRUCTED"


def canonical_bar(index, *, interval="30m", mode=BarAvailabilityMode.LIVE_OBSERVED):
    bar_end = NOW - timedelta(minutes=30 * (3 - index))
    close = Decimal(100 + index)
    return EquityBarRevision(
        bar_revision_id=uuid4(), security_id=SECURITY, ticker="SPY", interval=interval,
        session_date=date(2026, 9, 17), bar_start=bar_end - timedelta(minutes=30), bar_end=bar_end,
        open_price=close, high_price=close + 1, low_price=close - 1, close_price=close,
        volume=Decimal(1000 + index), vwap=None, transaction_count=None,
        source_kind=BarSourceKind.NATIVE_REST, availability_mode=mode, is_final=True,
        system_observed_at=bar_end + timedelta(seconds=2),
        replay_available_at=bar_end + timedelta(seconds=1) if mode is BarAvailabilityMode.HISTORICAL_RECONSTRUCTED else None,
        adjusted=False, payload_sha256=f"{index + 1:064x}", session_scope=BarSessionScope.RTH,
        provider_published_at=bar_end + timedelta(seconds=1),
    )


def source_for_bars(bars, *, mode="PROSPECTIVE_RECEIPT"):
    return BehaviorSource(
        evidence_id=uuid4(), security_id=SECURITY, interval=bars[0].interval,
        payload_sha256="a" * 64, policy_sha256="b" * 64, market_time=bars[-1].bar_end,
        observed_at=NOW, recorded_at=NOW, received_at=NOW,
        valid_until=NOW + timedelta(hours=1), price_basis="RAW_ACTION_GATED",
        availability_mode=mode,
        history_mode="RECONSTRUCTED_HISTORY" if mode == "RECONSTRUCTED" else "LIVE_OBSERVED_HISTORY",
        history_available_at=NOW if mode == "RECONSTRUCTED" else None,
    )


def test_canonical_revision_adapter_preserves_identity_prices_and_conservative_availability():
    bars = tuple(canonical_bar(index) for index in range(3))
    series = finalized_series_from_revisions(
        bars, source_for_bars(bars),
        bar_recorded_ats=tuple(bar.system_observed_at for bar in bars),
    )

    assert series.bar_revision_ids == tuple(bar.bar_revision_id for bar in bars)
    assert series.bar_payload_sha256s == tuple(bar.payload_sha256 for bar in bars)
    assert series.bar_available_ats == tuple(bar.system_observed_at for bar in bars)
    assert series.ticker == "SPY" and series.volume[-1] == 1002


@pytest.mark.parametrize("mutation", ["forming", "scope", "identity", "basis", "mode", "source_mode"])
def test_canonical_revision_adapter_rejects_mixed_or_unverified_facts(mutation):
    bars = [canonical_bar(index) for index in range(3)]
    if mutation == "forming":
        bars[1] = replace(bars[1], is_final=False)
    if mutation == "scope":
        bars[1] = replace(bars[1], session_scope=BarSessionScope.EXTENDED)
    if mutation == "identity":
        bars[1] = replace(bars[1], security_id=uuid4())
    if mutation == "basis":
        bars[1] = replace(bars[1], adjusted=True)
    if mutation == "mode":
        bars[1] = replace(bars[1], availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
                          replay_available_at=bars[1].bar_end + timedelta(seconds=1))
    source = source_for_bars(bars)
    if mutation == "source_mode":
        source = source.model_copy(update={"availability_mode": "RECONSTRUCTED"})
    with pytest.raises(ValueError):
        finalized_series_from_revisions(
            tuple(bars), source,
            bar_recorded_ats=tuple(bar.system_observed_at for bar in bars),
        )


def test_reconstructed_revision_adapter_preserves_reconstructed_mode():
    bars = tuple(canonical_bar(index, mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED) for index in range(3))
    series = finalized_series_from_revisions(
        bars, source_for_bars(bars, mode="RECONSTRUCTED"),
        bar_recorded_ats=tuple(bar.system_observed_at for bar in bars),
    )
    assert series.source.availability_mode == "RECONSTRUCTED"