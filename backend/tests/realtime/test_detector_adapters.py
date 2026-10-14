from datetime import datetime, timezone
from uuid import UUID

import pandas as pd
import pytest

from options.alert_qualification import qualify_option_alert
from options.dual_origin import detect_option_participation
from options.strategies.domain import StructureType
from realtime import detector_adapters
from realtime.detector_adapters import (
    run_o1_adapter,
    run_o2_adapter,
    run_canonical_stock_signal_adapter,
    run_option_qualification_adapter,
    run_stock_intraday_adapter,
)
from realtime.preflight import ProviderTier
from research.strategy_v2 import StrategyConfig, detect
from tests.options.test_dual_origin import activity_source
from tests.options.test_stock_bar_detection import OPEN, SECURITY, bar, daily_history


UTC = timezone.utc
COHORT = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
INPUT_SHA256 = "b" * 64


def test_stock_intraday_adapter_returns_stable_detector_output_unchanged():
    frame = pd.DataFrame()
    config = StrategyConfig("relative_trend_resumption_v1", "rs70", 21)

    adapted = run_stock_intraday_adapter(
        cohort_id=COHORT,
        provider_tier=ProviderTier.MASSIVE_DEVELOPER,
        input_sha256=INPUT_SHA256,
        frame=frame,
        config=config,
    )

    assert adapted.output == detect(frame, config)
    assert adapted.adapter_id == "stock-intraday-quality-v2"
    assert not adapted.publication_permission
    assert not adapted.execution_permission


def test_o1_adapter_returns_stable_finding_unchanged():
    source = activity_source()
    decision_at = source.received_at

    adapted = run_o1_adapter(
        source,
        cohort_id=COHORT,
        provider_tier=ProviderTier.MASSIVE_DEVELOPER,
        input_sha256=INPUT_SHA256,
        market_cutoff=source.market_time,
        decision_at=decision_at,
    )

    assert adapted.output == detect_option_participation(
        source,
        market_cutoff=source.market_time,
        decision_at=decision_at,
    )
    assert adapted.status == "EXECUTED"


def test_developer_o2_is_unavailable_without_calling_stable_detector(monkeypatch):
    def fail_if_called(*args, **kwargs):
        raise AssertionError("Developer O2 called the stable detector")

    monkeypatch.setattr(detector_adapters, "_assess_surface_first", fail_if_called)
    adapted = run_o2_adapter(
        object(),
        None,
        cohort_id=COHORT,
        provider_tier=ProviderTier.MASSIVE_DEVELOPER,
        input_sha256=INPUT_SHA256,
        market_cutoff=datetime(2026, 10, 5, 15, tzinfo=UTC),
        decision_at=datetime(2026, 10, 5, 15, 1, tzinfo=UTC),
    )

    assert adapted.status == "UNAVAILABLE"
    assert adapted.output is None
    assert adapted.reason_codes == ("DETECTOR_DISABLED_FOR_TIER",)


def test_option_qualification_adapter_returns_stable_assessment_unchanged():
    decision_at = datetime(2026, 10, 5, 15, tzinfo=UTC)
    direct = qualify_option_alert(
        "VOLUME_OI_ANOMALY",
        StructureType.VOLUME_OI_ANOMALY,
        {},
        decision_at,
    )

    adapted = run_option_qualification_adapter(
        "VOLUME_OI_ANOMALY",
        StructureType.VOLUME_OI_ANOMALY,
        {},
        decision_at,
        cohort_id=COHORT,
        provider_tier=ProviderTier.MASSIVE_DEVELOPER,
        input_sha256=INPUT_SHA256,
    )

    assert adapted.output == direct
    assert adapted.output["execution_permission"] is False


def test_adapter_rejects_unversioned_input_identity():
    with pytest.raises(ValueError, match="input SHA-256"):
        run_stock_intraday_adapter(
            cohort_id=COHORT,
            provider_tier=ProviderTier.MASSIVE_DEVELOPER,
            input_sha256="not-a-hash",
            frame=pd.DataFrame(),
            config=StrategyConfig("relative_trend_resumption_v1", "rs70", 21),
        )


def test_canonical_stock_adapter_returns_real_no_match_without_substitution():
    intraday = (
        bar("30m", OPEN, 30, 100),
        bar("30m", OPEN + pd.Timedelta(minutes=30).to_pytimedelta(), 30, 100),
    )
    bars = tuple(
        (row, row.system_observed_at) for row in (*daily_history(), *intraday)
    )
    adapted = run_canonical_stock_signal_adapter(
        detector_id="S1",
        security_id=SECURITY,
        underlyer="AAPL",
        bars=bars,
        cohort_id=COHORT,
        provider_tier=ProviderTier.MASSIVE_DEVELOPER,
        input_sha256=INPUT_SHA256,
        market_cutoff=intraday[-1].bar_end,
        decision_at=intraday[-1].bar_end + pd.Timedelta(minutes=1).to_pytimedelta(),
        session_close=datetime(2026, 9, 28, 20, tzinfo=UTC),
    )

    assert adapted.status == "EXECUTED"
    assert adapted.output == ()