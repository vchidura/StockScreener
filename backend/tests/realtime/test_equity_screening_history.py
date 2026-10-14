from datetime import timedelta

import pytest

from realtime.contracts import EQUITY_VALIDATION_UNIVERSE
from realtime.equity_screening_history import (
    REALTIME_SCREENING_HISTORY_POLICY,
    RealtimeScreeningHistoryPolicy,
    select_realtime_screening_history,
)
from realtime.equity_validation_history import (
    SplitAction,
    SplitCoverage,
    build_equity_validation_history_package,
)
from tests.realtime.test_equity_validation_history import OBSERVED_AT, _facts


def _package():
    facts, daily_start, daily_end = _facts()
    return build_equity_validation_history_package(
        facts,
        split_coverage={
            ticker: SplitCoverage(ticker, daily_start, daily_end, OBSERVED_AT)
            for ticker in EQUITY_VALIDATION_UNIVERSE.equity_tickers
        },
        split_actions=(
            SplitAction("NFLX", daily_start + timedelta(days=30), "1", "10"),
        ),
    )


def test_screening_uses_253_adjusted_sessions_and_latest_raw_quote():
    history = select_realtime_screening_history(_package(), "NFLX")

    assert len(history.adjusted_daily) == 253
    assert all(fact.revision.adjusted for fact in history.adjusted_daily)
    assert history.current_raw_daily.revision.adjusted is False
    assert history.adjusted_daily[-1].revision.session_date == history.current_raw_daily.revision.session_date
    assert history.policy_sha256 == REALTIME_SCREENING_HISTORY_POLICY.sha256
    assert history.source_history_sha256


def test_screening_policy_cannot_weaken_depth_or_mix_price_bases():
    with pytest.raises(ValueError, match="12-1"):
        RealtimeScreeningHistoryPolicy(adjusted_daily_sessions=252)
    with pytest.raises(ValueError, match="split adjusted"):
        RealtimeScreeningHistoryPolicy(historical_price_basis="RAW_ACTION_GATED")
    with pytest.raises(ValueError, match="current quote"):
        RealtimeScreeningHistoryPolicy(current_quote_basis="PROVIDER_SPLIT_ADJUSTED")
