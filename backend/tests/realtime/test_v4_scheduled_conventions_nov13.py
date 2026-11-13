"""V4.1 rehearsal finding (Nov 13): the scheduled preparation must pass a last-trading convention for every underlying,
or the v3-family profiles drop all 0-5 DTE candidates (no same-day, near or this-week contracts)."""
from realtime.advanced_options_automatic_preparation import (
    DEFAULT_LAST_TRADING_CONVENTION,
    scheduled_last_trading_conventions,
)


def test_every_underlying_gets_the_standard_equity_convention_by_default():
    assert DEFAULT_LAST_TRADING_CONVENTION == "US_EQUITY_STANDARD_PM"
    assert scheduled_last_trading_conventions({"spy", "TSLA"}, environment={}) == {
        "SPY": "US_EQUITY_STANDARD_PM", "TSLA": "US_EQUITY_STANDARD_PM"}


def test_the_setting_overrides_for_all_or_per_name():
    assert scheduled_last_trading_conventions(["SPY"], environment={
        "REALTIME_V3_OPTIONS_LAST_TRADING_CONVENTION": "other"}) == {"SPY": "OTHER"}
    assert scheduled_last_trading_conventions(["SPY", "XSP"], environment={
        "REALTIME_V3_OPTIONS_LAST_TRADING_CONVENTION": "XSP=INDEX_AM,*=US_EQUITY_STANDARD_PM"}) == {
        "SPY": "US_EQUITY_STANDARD_PM", "XSP": "INDEX_AM"}


def test_the_profile_supports_the_default():
    from realtime.advanced_options_activity_momentum_c6 import (
        V4_ACTIVITY_MOMENTUM_PROFILE_VERSION,
        load_activity_momentum_profile,
    )

    profile = load_activity_momentum_profile(version=V4_ACTIVITY_MOMENTUM_PROFILE_VERSION)
    assert DEFAULT_LAST_TRADING_CONVENTION in profile.supported_last_trading_conventions
