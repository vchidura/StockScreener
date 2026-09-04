from datetime import date
from decimal import Decimal

from equity.historical_universe import (
    HistoricalUniversePolicy,
    grouped_daily_rows,
    historical_session_plan,
    historical_universe_run_id,
    select_historical_members,
)


def test_selection_uses_prior_sessions_and_fixed_liquidity_policy() -> None:
    policy = HistoricalUniversePolicy(
        policy_version="liquid_us_common_stocks_v1",
        lookback_sessions=3,
        minimum_coverage_ratio=2 / 3,
        minimum_price=Decimal("5"),
        minimum_median_dollar_volume=Decimal("20000000"),
    )
    references = [
        {"active": True, "ticker": "PASS", "type": "CS"},
        {"active": True, "ticker": "LOWPX", "type": "CS"},
        {"active": True, "ticker": "ILLIQ", "type": "CS"},
        {"active": True, "ticker": "ETF", "type": "ETF"},
    ]
    history = {
        date(2024, 1, 2): {
            "PASS": {"close": 10, "volume": 3_000_000},
            "LOWPX": {"close": 4, "volume": 10_000_000},
            "ILLIQ": {"close": 10, "volume": 1_000_000},
        },
        date(2024, 1, 3): {
            "PASS": {"close": 11, "volume": 3_000_000},
            "LOWPX": {"close": 4, "volume": 10_000_000},
            "ILLIQ": {"close": 10, "volume": 1_000_000},
        },
        date(2024, 1, 4): {
            "PASS": {"close": 12, "volume": 3_000_000},
            "LOWPX": {"close": 4, "volume": 10_000_000},
            "ILLIQ": {"close": 10, "volume": 1_000_000},
        },
        date(2024, 1, 5): {
            "ILLIQ": {"close": 500, "volume": 1_000_000},
        },
    }

    selection = select_historical_members(
        references,
        history,
        signal_date=date(2024, 1, 5),
        prior_sessions=(date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)),
        policy=policy,
    )

    assert [row.ticker for row in selection.members] == ["PASS"]
    assert selection.members[0].latest_price == Decimal("12")
    assert selection.members[0].median_dollar_volume == Decimal("33000000")
    assert selection.exclusion_counts == {
        "INSUFFICIENT_HISTORY": 0,
        "LOW_DOLLAR_VOLUME": 1,
        "LOW_PRICE": 1,
        "UNSUPPORTED_SECURITY_TYPE": 1,
    }


def test_policy_hash_changes_with_eligibility_thresholds() -> None:
    baseline = HistoricalUniversePolicy()
    revised = HistoricalUniversePolicy(minimum_price=Decimal("10"))

    assert baseline.policy_sha256 != revised.policy_sha256


def test_session_plan_separates_warmup_from_research_sessions() -> None:
    plan = historical_session_plan(
        end_date=date(2024, 1, 8), research_sessions=3, warmup_sessions=2
    )

    assert plan.warmup_sessions == (date(2024, 1, 2), date(2024, 1, 3))
    assert plan.research_sessions == (
        date(2024, 1, 4), date(2024, 1, 5), date(2024, 1, 8)
    )


def test_grouped_rows_and_run_identity_are_order_independent() -> None:
    rows = grouped_daily_rows([
        {"T": "MSFT", "c": 400, "v": 10},
        {"T": "AAPL", "c": 200, "v": 20},
    ])

    assert rows["AAPL"] == {"close": 200, "volume": 20}
    values = {
        "signal_date": date(2024, 1, 8),
        "policy_sha256": "a" * 64,
        "source_request_sha256": "b" * 64,
    }
    assert historical_universe_run_id(
        **values, member_tickers=["MSFT", "AAPL"]
    ) == historical_universe_run_id(
        **values, member_tickers=["AAPL", "MSFT"]
    )


def test_grouped_rows_preserve_case_distinct_provider_symbols() -> None:
    rows = grouped_daily_rows([
        {"T": "BCPC", "c": 170, "v": 100_000},
        {"T": "BCpC", "c": 24, "v": 10_000},
    ])

    assert rows["BCPC"]["close"] == 170
    assert rows["BCpC"]["close"] == 24