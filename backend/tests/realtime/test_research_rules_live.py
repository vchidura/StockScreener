"""Nov 8 (docs/EQUITY_MODEL_LAB_DESIGN.md section 6): live input for the research rules and their live policy."""
from datetime import date, datetime, timedelta, timezone

import pytest

from realtime import research_rule_input as rri
from research.equity_models.catalog import CATALOG, research_policy
from research.equity_models.data import ResearchData

UTC = timezone.utc


def _row(start, minutes, close=100.0, basis="RAW_UNADJUSTED", session=date(2026, 11, 4), revision="r"):
    return dict(bar_revision_id=f"00000000-0000-0000-0000-{abs(hash((start, minutes, revision))) % 10**12:012d}",
                bar_start=start, bar_end=start + timedelta(minutes=minutes), observed_at=start + timedelta(minutes=minutes, seconds=5),
                open_price=close, high_price=close + 1, low_price=close - 1, close_price=close, volume=1000.0,
                vwap=close, adjustment_basis=basis, session_date=session, payload_sha256="a" * 64)


def _validator():
    return ResearchData({"schema": rri.DATASET_SCHEMA, "availability_mode": "OBSERVED"})


def test_live_rules_are_the_29_bar_rules_with_events_optional():
    assert len(rri.RESEARCH_RULES) == 29
    assert all(CATALOG[rule].interval in rri.TRIGGER_INTERVALS for rule in rri.RESEARCH_RULES)
    swing = rri.rule_policy("SW3_B")
    assert swing.earnings_policy == "optional" and swing.macro_policy == "none"
    # The default registration keeps its Oct 25 hash; the optional one differs.
    assert research_policy("B3_1h_combined", "none").policy_hash.startswith("7d6c3f268047")
    assert rri.rule_policy("SW3_B").policy_hash != research_policy("SW3_B", "none").policy_hash


def test_the_closing_half_hour_is_not_an_hourly_bar():
    member = rri.ResearchMember("s1", 1, "AAPL", "CS", None, True)
    opening = datetime(2026, 11, 4, 14, 30, tzinfo=UTC)
    rows = [_row(opening + timedelta(hours=i), 60) for i in range(6)] + [_row(opening + timedelta(hours=6), 30)]
    kept, skipped = rri.bar_records(member, "1h", rows, {}, _validator())
    assert len(kept) == 6 and skipped == 1
    assert all(record["interval"] == "1h" for record in kept)


def test_a_vwap_outside_the_bar_is_dropped_not_the_bar():
    member = rri.ResearchMember("s1", 1, "AAPL", "CS", None, True)
    row = _row(datetime(2026, 11, 4, 14, 30, tzinfo=UTC), 30)
    row["vwap"] = 500.0
    kept, skipped = rri.bar_records(member, "30m", [row], {}, _validator())
    assert skipped == 0 and kept[0]["vwap"] is None


def test_a_split_starts_a_new_raw_segment_but_not_an_adjusted_one():
    splits = rri.split_segments([{"security_id": "s1", "action_type": "SPLIT", "ex_date": date(2026, 7, 2)},
                                 {"security_id": "s1", "action_type": "DIVIDEND", "ex_date": date(2026, 8, 1)}])
    assert splits == {"s1": [date(2026, 7, 2)]}
    assert rri._segment("s1", "RAW_UNADJUSTED", date(2026, 7, 1), splits) != rri._segment(
        "s1", "RAW_UNADJUSTED", date(2026, 7, 2), splits)
    assert rri._segment("s1", "ADJUSTED", date(2026, 7, 1), splits) == rri._segment(
        "s1", "ADJUSTED", date(2026, 7, 2), splits)


def test_prior_day_facts_come_from_bars_and_missing_inputs_stay_none():
    spy = rri.ResearchMember("spy", 1, "SPY", "ETF", None, False)
    a = rri.ResearchMember("a", 2, "AAA", "CS", None, True)          # no sector ETF in the universe
    b = rri.ResearchMember("b", 3, "BBB", "CS", None, True)
    days = [date(2026, 1, 1) + timedelta(days=i) for i in range(80)]

    def series(step):
        return [dict(session_date=day, close_price=100 * (1 + step) ** i) for i, day in enumerate(days)]

    daily = {1: series(0.001), 2: series(0.003), 3: series(-0.001)}
    close = datetime(2026, 3, 22, 21, tzinfo=UTC)
    facts = rri.daily_facts([spy, a, b], daily, session=days[-1] + timedelta(days=1), prior_close=close,
                            valid_until=close + timedelta(days=1), covered={"a": True, "b": False}, cohort_id="c")
    by = {(f["security_id"], f["kind"]): f["values"] for f in facts}
    assert ("spy", "eligibility") not in by                       # benchmarks are not evaluated
    assert by[("a", "eligibility")]["actions_covered"] is True and by[("b", "eligibility")]["actions_covered"] is False
    assert by[("a", "relative_strength")]["rs63"] > 0 > by[("b", "relative_strength")]["rs63"]
    assert by[("a", "relative_strength")]["percentile63"] == 1.0 and by[("b", "relative_strength")]["percentile63"] == 0.5
    assert by[("a", "relative_strength")]["sector_rs20"] is None and by[("a", "benchmarks")]["sector_security_id"] is None


def test_research_entry_deadlines():
    from realtime.canonical_stock_alert_research import entry_valid_until

    at = datetime(2026, 11, 6, 19, 45, tzinfo=UTC)
    assert entry_valid_until(rri.rule_policy("B3_1h_structure"), at, "2026-11-06") == datetime(
        2026, 11, 6, 20, 45, tzinfo=UTC)
    assert entry_valid_until(rri.rule_policy("B3_30m_structure"), datetime(2026, 11, 6, 20, 45, tzinfo=UTC),
                             "2026-11-06") == datetime(2026, 11, 6, 21, 0, tzinfo=UTC)
    # Swing: the next session's close (Friday -> Monday).
    assert entry_valid_until(rri.rule_policy("SW3_R"), at, "2026-11-06") == datetime(2026, 11, 9, 21, 0, tzinfo=UTC)


def test_rules_mode_switch(monkeypatch):
    from realtime.canonical_stock_alert_research import stock_alert_rules

    monkeypatch.delenv("REALTIME_V2_STOCK_ALERT_RULES", raising=False)
    assert stock_alert_rules() == "RESEARCH"
    monkeypatch.setenv("REALTIME_V2_STOCK_ALERT_RULES", "placeholder")
    assert stock_alert_rules() == "PLACEHOLDER"
    monkeypatch.setenv("REALTIME_V2_STOCK_ALERT_RULES", "other")
    with pytest.raises(ValueError):
        stock_alert_rules()


def _page_row(alert_id, at, model, *, direction=1, style="INTRADAY", group="g1", run="r1"):
    return dict(alert_id=alert_id, strategy_instance_id="canonical-equity-stock-alert-v1", security_id="s1",
                direction=direction, trade_style=style, interval="30m", model=model, triggered_at=at,
                status="OPEN", run_id=run, episode_group=group, trigger_price=100.0, stop=99.0, target=102.0,
                warnings=[], hits=None, hit_models=[], hit_intervals=[], first_seen=None, last_seen=None)


def test_repeats_fold_into_the_first_alert_of_the_episode():
    from research.stock_alerts import collapse_alert_rows

    rows = collapse_alert_rows([
        _page_row("b", "2026-11-09T16:00:05+00:00", "R2_30m_structure", run="r2"),
        _page_row("a", "2026-11-09T15:30:05+00:00", "B3_30m_structure", run="r1"),
        _page_row("c", "2026-11-09T15:30:05+00:00", "B3_30m_structure", direction=-1, group="g2"),
    ])
    long_row = next(row for row in rows if row["direction"] == 1)
    assert long_row["alert_id"] == "a" and long_row["plan_count"] == 2       # the first alert stays the row
    assert long_row["display_models"] == ["B3_30m_structure", "R2_30m_structure"]
    assert len(rows) == 2                                                    # the other side is its own row


def test_rows_without_an_episode_keep_the_old_folding():
    from research.stock_alerts import collapse_alert_rows

    rows = collapse_alert_rows([_page_row("a", "2026-11-09T15:30:05+00:00", "m", group=None),
                                _page_row("b", "2026-11-09T16:00:05+00:00", "m", group=None)])
    assert len(rows) == 2


def test_every_matched_window_is_a_hit_with_its_rules():
    from realtime.stock_alert_projection import canonical_hit_observations

    hits = canonical_hit_observations([
        dict(disposition="MATCH", direction=-1, session="2026-11-04", strategy_instance_id="c", security_id="s1",
             run_id="r1", published_at="t1", model="B3_1h_structure", interval="1h",
             also_matched=[{"rule": "SW3_B"}]),
        dict(disposition="UNAVAILABLE", direction=0, session="2026-11-04", security_id="s1", run_id="r2",
             model="research-rule-set"),
        dict(disposition="NO_MATCH", direction=1, session="2026-11-04", security_id="s1", run_id="r3"),
    ])
    assert len(hits) == 1
    assert hits[0]["models"] == ["B3_1h_structure", "SW3_B"] and hits[0]["valid"] and hits[0]["run_id"] == "r1"
