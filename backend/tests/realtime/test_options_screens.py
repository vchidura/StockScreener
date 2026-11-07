"""Options screen definitions (options-screen-v1) and the built-in Unusual-activity screens."""
from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

import pytest
from pydantic import ValidationError

from options.discovery import CONTRACT_FILTERS, EligibleChainQuery, contract_filter_sql
from options.screens import (
    ScreenDefinition, builtin_screen_catalog, builtin_screens, derive_screen_fields, evaluate_screen, rule_labels,
)

NOW = datetime(2026, 11, 9, 15, 0, tzinfo=timezone.utc)


def _row(ticker, underlying="SPY", kind="CALL", *, dte=10, volume=8000, oi=1000, mark="2.00"):
    return {"contract_ticker": ticker, "underlying": underlying, "contract_type": kind, "calendar_dte": dte,
            "day_volume": volume, "open_interest": oi, "mark": mark, "model_mark": mark,
            "volume_open_interest_ratio": None if not oi else str(volume / oi), "expiration_date": "2026-11-19",
            "strike": "740", "contract_id": 0}


def test_the_builtin_screens_load_and_follow_the_flow_policy():
    screens = builtin_screens()
    assert set(screens) == {"unusual-activity-calls", "unusual-activity-puts"}
    calls = screens["unusual-activity-calls"]
    assert calls.contract_type == "CALL" and calls.sort == "day_premium" and calls.caps.per_underlying == 3
    assert calls.caps.total == 20 and calls.parameters.values["ETF"] == {
        "minimum_day_volume": 5000, "minimum_day_premium": 1000000}
    assert calls.parameters.underlying_tiers["NVDA"] == "HIGH_BETA_HIGH_PREMIUM"
    assert len(calls.sha256) == 64 and calls.sha256 != screens["unusual-activity-puts"].sha256
    labels = rule_labels(calls.rule, calls.parameters)
    assert labels[0] == "Open interest >= 500" and labels[2] == "not (DTE in 3, 4)"
    assert labels[3].startswith("Volume >= CORE_MEGA_CAP 1500, ETF 5000, HIGH_BETA_HIGH_PREMIUM 750")
    assert {item["id"] for item in builtin_screen_catalog()} == set(screens)


def test_a_definition_is_checked_before_it_runs():
    base = builtin_screens()["unusual-activity-calls"].model_dump(mode="json", by_alias=True)
    with pytest.raises(ValidationError):
        ScreenDefinition.model_validate({**base, "rule": {"field": "open_interest", "op": ">=", "parameter": "nope"}})
    with pytest.raises(ValidationError):
        ScreenDefinition.model_validate({**base, "rule": {"field": "not_a_field", "op": ">=", "value": 1}})
    with pytest.raises(ValidationError):
        ScreenDefinition.model_validate({**base, "rule": {"field": "open_interest", "op": "in", "value": 1}})
    with pytest.raises(ValidationError):
        ScreenDefinition.model_validate({**base, "unexpected": True})


def test_tiers_lanes_premium_and_caps_decide_what_the_screen_shows():
    screen = builtin_screens()["unusual-activity-calls"]
    rows = [
        _row("O:SPY_A", volume=20000, oi=4000, mark="3.00"),             # premium $6.0M: in
        _row("O:SPY_B", volume=9000, oi=2000, mark="2.00"),              # $1.8M: in
        _row("O:SPY_C", volume=6000, oi=1500, mark="2.00"),              # $1.2M: in
        _row("O:SPY_D", volume=5500, oi=1500, mark="2.50"),              # $1.375M: in, but over the per-underlying cap
        _row("O:SPY_E", volume=4000, oi=1000, mark="9.00"),              # under the ETF volume minimum
        _row("O:SPY_F", volume=9000, oi=400, mark="5.00"),               # prior OI under 500
        _row("O:SPY_G", volume=9000, oi=2000, mark="5.00", dte=3),       # 3-4 DTE lane excluded
        _row("O:SPY_H", volume=9000, oi=2000, mark="5.00", kind="PUT"),  # a put
        _row("O:NVDA_A", "NVDA", volume=800, oi=200, mark="8.00"),       # OI under 500
        _row("O:NVDA_B", "NVDA", volume=2000, oi=600, mark="3.00"),      # high-beta: 750 contracts, $0.6M: in
        _row("O:XYZ_A", "XYZ", volume=99999, oi=1000, mark="9.00"),      # untiered: excluded, reported
        _row("O:SPY_I", volume=9000, oi=2000, mark=None),                # no mark, no premium: out
    ]
    shown, note = evaluate_screen(screen, rows)
    assert [row["contract_ticker"] for row in shown] == ["O:SPY_A", "O:SPY_B", "O:SPY_D", "O:NVDA_B"]
    assert note["matched"] == 5 and note["shown"] == 4 and note["untiered_underlyings"] == ["XYZ"]
    assert shown[0]["day_premium"] == "6000000.00" and shown[0]["screen_tier"] == "ETF"
    assert derive_screen_fields({"day_volume": 10, "mark": "0"}) == {"day_premium": None}


def test_the_screener_runs_a_builtin_screen_beside_the_users_filters(monkeypatch):
    from options.discovery import ContractRange
    from realtime import advanced_options_projection as projection

    monkeypatch.setattr(projection, "_validate_page_identity", lambda *_a, **_k: None)
    publication = projection.AdvancedOptionsScreenerPagePublication({
        "provider_tier": "MASSIVE_ADVANCED", "scope_sha256": "s" * 64, "rows": [_row("O:SPY_A", volume=20000, oi=4000)],
        "coverage": [{"underlying": "SPY", "market_time": NOW.isoformat(), "observed_time": NOW.isoformat()},
                     {"underlying": "NVDA", "market_time": NOW.isoformat(), "observed_time": NOW.isoformat()}],
        "underlyers": ["SPY", "NVDA"], "filter_values": {}, "published_at": NOW.isoformat(), "policy_sha256": "p" * 64,
    })
    breadth = [_row("O:SPY_B", volume=9000, oi=2000), _row("O:NVDA_B", "NVDA", volume=2000, oi=600, mark="3.00")]
    result = publication.query(
        EligibleChainQuery(screen_id="unusual-activity-calls", sort="day_premium"),
        cohort_id=UUID(int=1), provider_tier="MASSIVE_ADVANCED", scope_sha256="s" * 64, breadth_rows=breadth,
    )
    data = result["data"]
    assert [row["contract_ticker"] for row in data["rows"]] == ["O:SPY_A", "O:SPY_B", "O:NVDA_B"]
    assert data["screen"]["id"] == "unusual-activity-calls" and data["screen"]["shown"] == 3
    narrowed = publication.query(
        EligibleChainQuery(screen_id="unusual-activity-calls", underlyer="NVDA",
                           filters=(ContractRange(field="day_premium", minimum=100000),)),
        cohort_id=UUID(int=1), provider_tier="MASSIVE_ADVANCED", scope_sha256="s" * 64, breadth_rows=breadth,
    )
    assert [row["contract_ticker"] for row in narrowed["data"]["rows"]] == ["O:NVDA_B"]
    with pytest.raises(ValueError):
        publication.query(EligibleChainQuery(screen_id="no-such-screen"), cohort_id=UUID(int=1),
                          provider_tier="MASSIVE_ADVANCED", scope_sha256="s" * 64)
    assert "day_premium" in CONTRACT_FILTERS


def test_the_legacy_sql_screener_refuses_screens_but_filters_day_premium():
    from options.discovery import ContractRange

    with pytest.raises(ValueError):
        contract_filter_sql(EligibleChainQuery(screen_id="unusual-activity-calls"))
    sql, params = contract_filter_sql(EligibleChainQuery(filters=(ContractRange(field="day_premium", minimum=1),)))
    assert "day_premium >= %s" in sql and params == (1.0,)
