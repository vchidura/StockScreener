from datetime import datetime, timedelta, timezone
import os
from uuid import uuid4

import pytest

from options.analytics.behavior_review import annotate_candidates, daily_scorecard, selection_funnel
from options.stock_behavior_gates import evaluate_option_stock_behavior
from options.strategies.domain import StructureType


NOW = datetime(2026, 9, 18, 15, 30, tzinfo=timezone.utc)

def test_completed_runs_roll_latest_into_history_only_after_full_cycle():
    from options.analytics.behavior_review import completed_run_selection

    first_cycle = NOW - timedelta(hours=1)
    first = [dict(matrix_id=uuid4(), underlying=symbol, scheduled_cycle=first_cycle, completed_at=first_cycle + timedelta(minutes=20))
        for symbol in ("AAPL", "MSFT")]
    next_cycle = NOW - timedelta(minutes=30)
    next_rows = [dict(matrix_id=uuid4(), underlying=symbol, scheduled_cycle=next_cycle, completed_at=NOW)
        for symbol in ("AAPL", "MSFT")]
    incomplete = completed_run_selection(first + next_rows[:1], ("AAPL", "MSFT"), as_of=NOW)
    assert incomplete["latest_run"]["matrix_ids"] == [row["matrix_id"] for row in first]
    assert incomplete["history_runs"] == []
    assert incomplete["newer_partial_run"]["covered_underlyings"] == 1
    assert incomplete["newer_partial_run"]["missing_underlyings"] == ["MSFT"]
    complete = completed_run_selection(first + next_rows, ("AAPL", "MSFT"), as_of=NOW)
    assert complete["latest_run"]["matrix_ids"] == [row["matrix_id"] for row in next_rows]
    assert complete["history_runs"][0]["matrix_ids"] == [row["matrix_id"] for row in first]
    assert complete["withheld_run"]["run_id"] == complete["active_run"]["run_id"]
    assert complete["newer_partial_run"] is None


def test_completed_runs_persist_across_days_and_do_not_mix_dates_or_future_receipts():
    from options.analytics.behavior_review import completed_run_selection

    friday = dict(matrix_id=uuid4(), underlying="AAPL", scheduled_cycle=NOW, completed_at=NOW)
    monday = dict(matrix_id=uuid4(), underlying="AAPL", scheduled_cycle=NOW + timedelta(days=3), completed_at=NOW + timedelta(days=3, minutes=20))
    weekend = completed_run_selection([friday, monday], ("AAPL",), as_of=NOW + timedelta(days=1))
    assert weekend["latest_run"]["matrix_ids"] == [friday["matrix_id"]]
    assert weekend["sessions"] == ["2026-09-18"] and weekend["history_runs"] == []
    after_next = completed_run_selection([friday, monday], ("AAPL",), as_of=NOW + timedelta(days=3, hours=1), session_date=NOW.date())
    assert after_next["sessions"] == ["2026-09-18", "2026-09-21"]
    assert after_next["history_runs"][0]["matrix_ids"] == [friday["matrix_id"]]
    assert after_next["withheld_run"] is None
    assert completed_run_selection([], ("AAPL",), as_of=NOW)["latest_run"] is None


def test_completed_matrix_reader_requires_acknowledged_policy_bound_cycle(monkeypatch):
    from contextlib import contextmanager
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    from options.repositories.stock_behavior_assessments import OptionStockBehaviorAssessmentRepository

    cursor = MagicMock()
    cursor.fetchall.return_value = []
    @contextmanager
    def read():
        yield cursor
    repository = OptionStockBehaviorAssessmentRepository()
    monkeypatch.setattr(repository, "_cursor", read)
    config = SimpleNamespace(strategy_policy=SimpleNamespace(strategy_version="phase2_v4"),
        configuration_sha256="c" * 64, policy_sha256="d" * 64, settings=SimpleNamespace(underlyers=("AAPL",)))
    assert repository.completed_matrices(configuration=config, as_of=NOW) == []
    sql, args = cursor.execute.call_args.args
    for required in ("work.status='COMPLETED'", "ingestion.status='COMPLETE'", "analysis.status='COMPLETE'",
        "work.completed_at<=%s", "work.business_key='strategy:'", "ingestion.configuration_sha256=%s", "LIMIT 25001"):
        assert required in sql
    assert sql.count("%s") == len(args)
    assert args[0] == "phase2_v4" and "READ ONLY" in cursor.execute.call_args_list[0].args[0]


def test_latest_and_history_share_selection_and_zero_selection_run_replaces_prior():
    from types import SimpleNamespace
    from options.analytics.behavior_review import build_behavior_review

    older = candidate(calendar_dte=20, strategy_name="INCOME_WHEEL", structure_type="CASH_SECURED_PUT")
    latest = candidate(calendar_dte=20, strategy_name="INCOME_WHEEL", structure_type="CASH_SECURED_PUT")
    incomplete = candidate(calendar_dte=20, matrix_id=latest["matrix_id"], strategy_name="SPREAD_RANGE_LOCATOR", structure_type="IRON_CONDOR")
    matrices = [dict(matrix_id=older["matrix_id"], underlying="AAPL", scheduled_cycle=NOW - timedelta(minutes=30), completed_at=NOW),
        dict(matrix_id=latest["matrix_id"], underlying="AAPL", scheduled_cycle=NOW - timedelta(minutes=15), completed_at=NOW)]
    rows = [older, latest, incomplete]
    def read(**kwargs):
        return dict(candidates=[row for row in rows if row["matrix_id"] in kwargs["matrix_ids"]], assessments={}, outcomes=[], unavailable=[], schema={})
    config = SimpleNamespace(strategy_policy_sha256="b" * 64, valuation_policy_sha256="e" * 64, valuation_policy=None,
        configuration_sha256="c" * 64, settings=SimpleNamespace(underlyers=("AAPL",)))
    repository = SimpleNamespace(completed_matrices=lambda **_: matrices, research_inputs=read, page_leg_activity=lambda _: {})
    mark_repository = SimpleNamespace(retained_plan_marks=lambda *args, **kwargs: {"ready": True, "rows": {}})
    latest_page = build_behavior_review(config, as_of=NOW, view="ALL", scope="CURRENT", repository=repository)
    history_page = build_behavior_review(config, as_of=NOW, view="ALL", scope="HISTORY", repository=repository, mark_repository=mark_repository)
    assert [row["candidate_id"] for row in latest_page["rows"]] == [latest["candidate_id"]]
    assert [row["candidate_id"] for row in history_page["rows"]] == [older["candidate_id"]]
    assert latest_page["default_selection"] == history_page["default_selection"]
    rows[:] = [older]
    zero = build_behavior_review(config, as_of=NOW, view="ALL", scope="CURRENT", repository=repository)
    assert zero["total"] == 0 and zero["run"]["run_id"] == latest_page["run"]["run_id"]


def test_baseline_review_uses_only_worker_members_and_no_live_candidate_fallback():
    from types import SimpleNamespace
    from options.analytics.behavior_review import build_behavior_review
    from options.analytics.alert_selection import baseline_package_identity

    row = candidate(calendar_dte=20, strategy_name="INCOME_WHEEL", structure_type="CASH_SECURED_PUT")
    row["legs"][0]["side"] = "SELL"
    run = dict(publication_id=uuid4(), scheduled_cycle=NOW - timedelta(minutes=15), as_of_session=NOW.date(),
        published_at=NOW, source_matrix_ids=[row["matrix_id"]], expected_underlying_count=1, covered_underlying_count=1,
        new_alerts=1, repeat_hits=3, rejections={}, by_model={})
    runs = [run]
    member = dict(candidate_id=row["candidate_id"], hit_count=4, last_seen=NOW, published_at=NOW,
        selection_evidence={"alert_identity": baseline_package_identity(row)})
    requested = []
    def read(**kwargs):
        requested.append(kwargs["candidate_ids"])
        return dict(candidates=[row], assessments={}, outcomes=[], unavailable=[], schema={})
    config = SimpleNamespace(strategy_policy_sha256="b" * 64, valuation_policy_sha256="e" * 64,
        configuration_sha256="c" * 64, settings=SimpleNamespace(underlyers=("AAPL",), baseline_alerts_enabled=True,
        baseline_alerts_effective_from=NOW - timedelta(hours=1)))
    legacy_matrix = dict(matrix_id=uuid4(), underlying="AAPL", scheduled_cycle=NOW - timedelta(days=1), completed_at=NOW)
    repository = SimpleNamespace(research_inputs=read, completed_matrices=lambda **_: [legacy_matrix])
    alerts = SimpleNamespace(baseline_runs=lambda **_: runs, baseline_members=lambda ids, **_: {str(row["candidate_id"]): member} if ids else {})
    result = build_behavior_review(config, as_of=NOW, view="ALL", scope="CURRENT", repository=repository, alert_repository=alerts)
    assert requested == [[str(row["candidate_id"])]]
    assert result["rows"][0]["hit_count"] == 4
    assert result["rows"][0]["display_name"] == "Cash-secured put baseline"
    assert result["rows"][0]["category_ids"] == ["INCOME"]
    assert result["run_funnel"] == {"new_alerts": 1, "repeat_hits": 3}
    assert result["sessions"] == ["2026-09-17", "2026-09-18"]
    report = build_behavior_review(config, as_of=NOW, view="DAILY", scope="HISTORY", repository=repository, alert_repository=alerts)
    assert report["cohort_count"] == 1
    assert {cell["arm"] for cell in report["cells"]} == {"PERSISTED_BASELINE_ALERTS"}
    assert all(cell["cohorts"] == 1 and cell["mean_net_return"] is None for cell in report["cells"])
    runs.clear()
    empty = build_behavior_review(config, as_of=NOW, view="ALL", scope="CURRENT", repository=repository, alert_repository=alerts)
    assert empty["rows"] == [] and empty["run"] is None
    assert len(requested) == 2
    runs.append(run)
    member["selection_evidence"]["alert_identity"] = "wrong"
    with pytest.raises(ValueError, match="identity or original deadline"):
        build_behavior_review(config, as_of=NOW, view="ALL", scope="CURRENT", repository=repository, alert_repository=alerts)


def test_baseline_reader_sql_is_read_only_bounded_and_policy_bound(monkeypatch):
    from contextlib import contextmanager
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    from options.repositories.board import OptionBoardPublicationRepository
    from options.analytics.alert_selection import BASELINE_SELECTOR_SHA256

    cursor = MagicMock()
    cursor.fetchall.return_value = []
    @contextmanager
    def read():
        yield cursor
    repository = OptionBoardPublicationRepository()
    monkeypatch.setattr(repository, "_cursor", read)
    config = SimpleNamespace(strategy_policy_sha256="a" * 64, configuration_sha256="b" * 64)
    assert repository.baseline_runs(configuration=config, as_of=NOW) == []
    assert repository.baseline_members([uuid4()], configuration=config, as_of=NOW) == {}
    for call in cursor.execute.call_args_list:
        query = call.args[0]
        assert query.strip().startswith(("SET", "SELECT", "WITH"))
        if len(call.args) > 1:
            assert query.count("%s") == len(call.args[1])
            assert BASELINE_SELECTOR_SHA256 in call.args[1]
    assert "COUNT(DISTINCT publication.publication_id)" in cursor.execute.call_args.args[0]


def history_mark_fixture(credit=False):
    from decimal import Decimal
    from options.outcomes import configured_valuation_policy

    policy = configured_valuation_policy()
    candidate_id, matrix_id, batch_id = uuid4(), uuid4(), uuid4()
    plan = dict(candidate_id=str(candidate_id), candidate_identity_sha256="a" * 64, matrix_id=str(matrix_id),
        source_market_time=(NOW - timedelta(hours=1)).isoformat(), exit_deadline=(NOW + timedelta(days=1)).isoformat(),
        legs=[], original_economics=dict(net_premium="200" if credit else "-200", capital_at_risk="300" if credit else "200"))
    marks = []
    for index, (entry, exit_mark) in enumerate(((4, 3), (2, 2)) if credit else ((4, 5), (2, 2))):
        side = ("SELL" if credit else "BUY") if index == 0 else ("BUY" if credit else "SELL")
        leg = dict(contract_id=index + 1, contract_ticker=f"O:TEST{index}", side=side, ratio=1, multiplier=100,
            strike=str(100 + index * 5), expiration_date="2026-10-16", contract_type="CALL", entry_model_mark=str(entry),
            source_market_time=plan["source_market_time"], valuation_policy_sha256=policy.policy_sha256)
        plan["legs"].append(leg)
        marks.append({**leg, "entry_mark": entry, "exit_mark": exit_mark, "snapshot_id": uuid4(), "batch_id": batch_id,
            "entry_valuation_policy_sha256": policy.policy_sha256, "snapshot_multiplier": 100,
            "source_market_time": NOW - timedelta(minutes=15), "source_observed_time": NOW, "revised_observed_at": None,
            "entry_mark_source": "DEVELOPER_ALIGNED_AGG_CLOSE", "exit_mark_source": "DEVELOPER_ALIGNED_AGG_CLOSE"})
    record = dict(candidate_id=candidate_id, candidate_identity="a" * 64, matrix_id=matrix_id, legs=marks,
        valuation_policy_sha256=policy.policy_sha256, market_time=NOW - timedelta(minutes=15), observed_time=NOW, updated_at=NOW,
        entry_net_premium=Decimal(plan["original_economics"]["net_premium"]), exit_net_premium=Decimal("100" if credit else "-300"),
        gross_pnl=Decimal("100"), estimated_cost=Decimal("2.60"), net_pnl=Decimal("97.40"),
        capital_at_risk=Decimal(plan["original_economics"]["capital_at_risk"]),
        net_return=Decimal("97.40") / Decimal(plan["original_economics"]["capital_at_risk"]),
        source_snapshot_ids=[mark["snapshot_id"] for mark in marks], source_batch_id=batch_id)
    return plan, record, policy


@pytest.mark.parametrize("credit", [False, True])
def test_behavior_review_history_prices_bind_debit_credit_and_costs(credit):
    from decimal import Decimal
    from options.outcomes import review_retained_plan_mark

    plan, record, policy = history_mark_fixture(credit)
    result = review_retained_plan_mark(plan, record, checked_at=NOW, policy=policy)
    assert result["status"] == "FRESH"
    assert result["gross_pnl"] == 100 and result["price_return"] == Decimal(".5")
    assert result["net_pnl"] == Decimal("97.4") and result["slippage"] == "UNAVAILABLE"
    assert result["package_price"] == (1 if credit else -3)
    stale = review_retained_plan_mark(plan, record, checked_at=NOW + timedelta(days=2), policy=policy)
    assert stale["status"] == "STALE"
    assert stale["after_exit_deadline"] is False
    plan["exit_deadline"] = (NOW - timedelta(minutes=30)).isoformat()
    assert review_retained_plan_mark(plan, record, checked_at=NOW, policy=policy)["after_exit_deadline"] is True


@pytest.mark.parametrize("change", ["identity", "missing_leg", "side", "multiplier", "future", "batch", "policy", "pnl", "revision"])
def test_behavior_review_history_rejects_unbound_marks(change):
    from options.outcomes import review_retained_plan_mark

    plan, record, policy = history_mark_fixture()
    if change == "identity": record["candidate_identity"] = "b" * 64
    if change == "missing_leg": record["legs"].pop()
    if change == "side": record["legs"][0]["side"] = "SELL"
    if change == "multiplier": record["legs"][0]["multiplier"] = 10
    if change == "future": record["legs"][0]["source_observed_time"] = NOW + timedelta(seconds=1)
    if change == "batch": record["legs"][0]["batch_id"] = uuid4()
    if change == "policy": record["legs"][0]["valuation_policy_sha256"] = "b" * 64
    if change == "pnl": record["gross_pnl"] = 101
    if change == "revision": record["legs"][0]["revised_observed_at"] = NOW + timedelta(seconds=1)
    result = review_retained_plan_mark(plan, record, checked_at=NOW, policy=policy)
    assert result["status"] == "UNAVAILABLE" and result["gross_pnl"] is None


def candidate(**changes):
    row = dict(candidate_id=uuid4(), candidate_identity="a" * 64, matrix_id=uuid4(), underlying="AAPL",
        strategy_name="DIRECTIONAL_LONG_PREMIUM", strategy_version="v1", structure_type="LONG_CALL", status="SELECTED",
        policy_sha256="b" * 64, configuration_sha256="c" * 64, market_policy_sha256="d" * 64, analysis_policy_sha256="d" * 64,
        market_data_time=NOW - timedelta(minutes=15), scheduled_cycle=NOW - timedelta(minutes=15), observed_time=NOW,
        valid_until=NOW + timedelta(minutes=2), candidate_rank=1,
        legs=[dict(contract_id=1, model_mark=5, ratio=1, multiplier=100, source_market_time=NOW - timedelta(minutes=15))])
    row.update(changes)
    return row


def test_review_keeps_missing_and_unregistered_distinct():
    rows = annotate_candidates([candidate(), candidate(strategy_name="INCOME_WHEEL", structure_type="CASH_SECURED_PUT")], {}, NOW)
    assert [row["behavior"]["disposition"] for row in rows] == ["UNAVAILABLE", "NOT_APPLICABLE"]
    assert selection_funnel(rows)["shortlist"] == 0


def test_review_rejects_tampered_persisted_assessment():
    row = candidate()
    assessment = evaluate_option_stock_behavior(None, candidate_id=row["candidate_id"], matrix_id=row["matrix_id"], underlyer="AAPL",
        strategy_name=row["strategy_name"], structure_type=StructureType.LONG_CALL, directional_thesis="BULLISH", decision_at=NOW)
    result = annotate_candidates([row], {str(row["candidate_id"]): dict(payload_text=assessment.canonical_json(), payload_sha256="0" * 64, recorded_at=NOW)}, NOW)
    assert result[0]["behavior"]["reasons"] == ["STOCK_ASSESSMENT_INVALID"]


def test_eod_selects_cohort_before_outcomes_and_never_zero_fills_missing():
    first, later = candidate(), candidate(market_data_time=NOW, candidate_rank=1)
    rows = annotate_candidates([later, first], {}, NOW)
    for row in rows:
        row["behavior"].update(eligible=True, timely_at_recording=True, disposition="ELIGIBLE_RESEARCH")
    report = daily_scorecard(rows, [dict(candidate_id=later["candidate_id"], measurement_type="60MIN", net_return=99)], [], NOW + timedelta(hours=5))
    assert report["cohort_count"] == 1
    assert all(cell["mean_net_return"] is None for cell in report["cells"])
    assert all(cell["measured"] == 0 for cell in report["cells"])
    assert report["probability"] is None and report["threshold_changes"] is False


def test_review_filtering_precedes_pagination_and_keeps_funnel():
    from types import SimpleNamespace
    from options.analytics.behavior_review import build_behavior_review

    rows = [candidate(calendar_dte=20), candidate(calendar_dte=20, underlying="MSFT")]
    repository = SimpleNamespace(research_inputs=lambda **_: dict(candidates=rows, assessments={}, outcomes=[], unavailable=[], schema={}))
    config = SimpleNamespace(strategy_policy_sha256="b" * 64, valuation_policy_sha256="e" * 64)
    result = build_behavior_review(config, session_date=NOW.date(), as_of=NOW, view="ALL", limit=1, offset=1, repository=repository)
    assert result["total"] == 2 and len(result["rows"]) == 1
    assert result["funnel"]["candidates"] == 2
    empty = build_behavior_review(config, session_date=NOW.date(), as_of=NOW, view="SHORTLIST", repository=repository)
    assert empty["total"] == 0 and empty["funnel"]["candidates"] == 2


@pytest.mark.parametrize("cutoff, last_session", [
    (NOW + timedelta(days=1), "2026-09-18"),
    (datetime(2026, 9, 7, 16, tzinfo=timezone.utc), "2026-09-04"),
])
def test_latest_run_remains_on_weekends_and_holidays(cutoff, last_session):
    from types import SimpleNamespace
    from unittest.mock import Mock
    from options.analytics.behavior_review import build_behavior_review

    row = candidate(calendar_dte=20)
    cycle = datetime.fromisoformat(last_session + "T19:00:00+00:00")
    row.update(market_data_time=cycle, observed_time=cycle + timedelta(minutes=20), scheduled_cycle=cycle)
    row["legs"][0]["source_market_time"] = cycle
    read = Mock(return_value=dict(candidates=[row], assessments={}, outcomes=[], unavailable=[], schema={}))
    config = SimpleNamespace(strategy_policy_sha256="b" * 64, valuation_policy_sha256="e" * 64,
        configuration_sha256="c" * 64, settings=SimpleNamespace(underlyers=("AAPL",)))
    repository = SimpleNamespace(research_inputs=read, completed_matrices=lambda **_: [
        dict(matrix_id=row["matrix_id"], underlying="AAPL", scheduled_cycle=cycle, completed_at=cycle + timedelta(minutes=20))])
    result = build_behavior_review(config, as_of=cutoff, view="ALL", scope="CURRENT", repository=repository)
    assert result["session_date"] == last_session and result["session_state"] == "CLOSED"
    assert result["latest_completed_session"] == last_session
    assert result["total"] == 1
    assert read.call_args.kwargs["matrix_ids"] == [row["matrix_id"]]


def test_day_history_reads_all_session_detections_before_paging_without_publication():
    from types import SimpleNamespace
    from unittest.mock import Mock
    from options.analytics.behavior_review import build_behavior_review

    first = candidate(calendar_dte=20)
    later = candidate(calendar_dte=20, market_data_time=NOW, strategy_name="INCOME_WHEEL", structure_type="CASH_SECURED_PUT")
    read = Mock(return_value=dict(candidates=[first, later], assessments={}, outcomes=[], unavailable=[], schema={}))
    config = SimpleNamespace(strategy_policy_sha256="b" * 64, valuation_policy_sha256="e" * 64, valuation_policy=None,
        configuration_sha256="c" * 64, settings=SimpleNamespace(underlyers=("AAPL",)))
    matrices = [dict(matrix_id=first["matrix_id"], underlying="AAPL", scheduled_cycle=NOW - timedelta(minutes=30), completed_at=NOW),
        dict(matrix_id=later["matrix_id"], underlying="AAPL", scheduled_cycle=NOW - timedelta(minutes=15), completed_at=NOW),
        dict(matrix_id=uuid4(), underlying="AAPL", scheduled_cycle=NOW, completed_at=NOW)]
    marks = Mock(return_value={"ready": True, "rows": {}})
    activity = Mock(return_value={})
    result = build_behavior_review(config, as_of=NOW + timedelta(days=1), view="ALL", scope="HISTORY", limit=1,
        repository=SimpleNamespace(research_inputs=read, page_leg_activity=activity, completed_matrices=lambda **_: matrices), mark_repository=SimpleNamespace(retained_plan_marks=marks))
    assert result["session_date"] == "2026-09-18" and result["scope"] == "HISTORY"
    assert read.call_args.kwargs["history"] is True and read.call_args.kwargs["daily"] is False
    assert result["total"] == 2 and result["rows"][0]["candidate_id"] == later["candidate_id"]
    assert result["history_basis"] == "RETAINED_CANDIDATE_DETECTIONS_NOT_PUBLICATIONS"
    assert result["selection_basis"] == "EARLIER_COMPLETE_CYCLES_EXCLUDING_ACTIVE_RUN"
    assert read.call_args.kwargs["matrix_ids"] == [first["matrix_id"], later["matrix_id"]]
    assert marks.call_args.args[0] == [later["candidate_id"]]
    assert activity.call_args.args[0] == [later["candidate_id"]]
    assert result["rows"][0]["current_mark"]["reason"] == "CURRENT_MARK_NOT_RECORDED"


def test_day_history_candidate_mark_reuses_exact_binding_without_a_plan_or_deadline():
    from options.outcomes import review_retained_candidate_mark

    plan, mark, policy = history_mark_fixture()
    row = candidate(candidate_id=mark["candidate_id"], matrix_id=mark["matrix_id"],
        market_data_time=datetime.fromisoformat(plan["source_market_time"]),
        net_premium=plan["original_economics"]["net_premium"], capital_at_risk=plan["original_economics"]["capital_at_risk"],
        legs=[{**leg, "model_mark": leg["entry_model_mark"]} for leg in plan["legs"]])
    result = review_retained_candidate_mark(row, mark, checked_at=NOW, policy=policy)
    assert result["gross_pnl"] == 100 and result["after_exit_deadline"] is None
    assert "plan_id" not in row and "published_at" not in row
    mark["candidate_identity"] = "f" * 64
    assert review_retained_candidate_mark(row, mark, checked_at=NOW, policy=policy)["status"] == "UNAVAILABLE"


def test_no_completed_runs_returns_empty_without_mixing_latest_matrices():
    from types import SimpleNamespace
    from unittest.mock import Mock
    from options.analytics.behavior_review import build_behavior_review

    read = Mock(return_value=dict(candidates=[], assessments={}, outcomes=[], unavailable=[], schema={}))
    config = SimpleNamespace(strategy_policy_sha256="b" * 64, valuation_policy_sha256="e" * 64)
    config.configuration_sha256 = "c" * 64
    config.settings = SimpleNamespace(underlyers=("AAPL",))
    result = build_behavior_review(config, as_of=NOW, view="ALL", scope="CURRENT",
        repository=SimpleNamespace(research_inputs=read, completed_matrices=lambda **_: []))
    assert result["run"] is None and result["sessions"] == [] and result["total"] == 0
    read.assert_not_called()


@pytest.mark.parametrize("clock, expected", [("2026-11-27T17:59:00+00:00", "2026-11-25"), ("2026-11-27T18:00:00+00:00", "2026-11-27")])
def test_empty_run_lookup_uses_calendar_only_as_empty_display_date(clock, expected):
    from types import SimpleNamespace
    from options.analytics.behavior_review import build_behavior_review

    reader = SimpleNamespace(completed_matrices=lambda **_: [])
    config = SimpleNamespace(strategy_policy_sha256="b" * 64, valuation_policy_sha256="e" * 64,
        configuration_sha256="c" * 64, settings=SimpleNamespace(underlyers=("AAPL",)))
    result = build_behavior_review(config, as_of=datetime.fromisoformat(clock), view="ALL", scope="HISTORY", repository=reader)
    assert result["session_date"] == expected and result["total"] == 0


def test_day_history_enrichment_failures_do_not_hide_detections():
    from types import SimpleNamespace
    from options.analytics.behavior_review import build_behavior_review
    from psycopg2 import OperationalError

    def fail(*args, **kwargs):
        raise OperationalError("unavailable")
    row = candidate(calendar_dte=20)
    matrices = [dict(matrix_id=row["matrix_id"], underlying="AAPL", scheduled_cycle=NOW - timedelta(minutes=15), completed_at=NOW),
        dict(matrix_id=uuid4(), underlying="AAPL", scheduled_cycle=NOW, completed_at=NOW)]
    reader = SimpleNamespace(research_inputs=lambda **_: dict(candidates=[row], assessments={}, outcomes=[], unavailable=[], schema={}),
        page_leg_activity=fail, completed_matrices=lambda **_: matrices)
    config = SimpleNamespace(strategy_policy_sha256="b" * 64, valuation_policy_sha256="e" * 64, valuation_policy=None,
        configuration_sha256="c" * 64, settings=SimpleNamespace(underlyers=("AAPL",)))
    result = build_behavior_review(config, as_of=NOW + timedelta(days=1), view="ALL", scope="HISTORY", repository=reader,
        mark_repository=SimpleNamespace(retained_plan_marks=fail))
    assert result["total"] == 1 and result["rows"][0]["current_mark"]["reason"] == "CURRENT_MARK_READ_UNAVAILABLE"
    assert "current_mark" not in row


def test_day_history_activity_query_is_exact_and_page_bounded(monkeypatch):
    from contextlib import contextmanager
    from unittest.mock import MagicMock
    from options.repositories.stock_behavior_assessments import OptionStockBehaviorAssessmentRepository

    candidate_id = uuid4()
    cursor = MagicMock()
    cursor.fetchall.return_value = [dict(candidate_id=candidate_id, leg_index=0, day_volume=0, open_interest=None)]
    @contextmanager
    def read():
        yield cursor
    reader = OptionStockBehaviorAssessmentRepository()
    monkeypatch.setattr(reader, "_cursor", read)
    assert reader.page_leg_activity([candidate_id])[(str(candidate_id), 0)] == {"day_volume": 0, "open_interest": None}
    sql, args = cursor.execute.call_args.args
    assert "snapshot.snapshot_id=leg.snapshot_id AND snapshot.contract_id=leg.contract_id" in sql
    assert "snapshot.first_observed_at<=candidate.observed_time" in sql and "snapshot.market_data_time<=candidate.market_data_time" in sql
    assert sql.count("%s") == len(args)
    assert "READ ONLY" in cursor.execute.call_args_list[0].args[0]
    with pytest.raises(ValueError):
        reader.page_leg_activity([uuid4() for _ in range(201)])


def assessed_candidates():
    from test_stock_behavior_gates import current_aapl_snapshot

    stock = current_aapl_snapshot()
    clock = stock.available_at
    first = candidate(observed_time=clock, market_data_time=stock.market_time, scheduled_cycle=stock.market_time,
        valid_until=clock + timedelta(minutes=2))
    second = {**first, "candidate_id": uuid4(), "candidate_rank": 2}
    records = {}
    for row in (first, second):
        gate = evaluate_option_stock_behavior(stock, candidate_id=row["candidate_id"], candidate_identity_sha256=row["candidate_identity"],
            matrix_id=row["matrix_id"], underlyer="AAPL", strategy_name=row["strategy_name"], structure_type=StructureType.LONG_CALL,
            directional_thesis="BULLISH", decision_at=clock, option_strategy_version=row["strategy_version"],
            option_strategy_policy_sha256=row["policy_sha256"], option_configuration_sha256=row["configuration_sha256"],
            option_market_policy_sha256=row["market_policy_sha256"], option_analysis_policy_sha256=row["analysis_policy_sha256"],
            option_market_time=row["market_data_time"], option_observed_at=clock, stock_market_cutoff=row["scheduled_cycle"],
            entry_deadline=row["valid_until"])
        records[str(row["candidate_id"])] = dict(payload_text=gate.canonical_json(), payload_sha256=gate.sha256,
            recorded_at=clock + timedelta(seconds=1))
    return first, second, records, clock


def test_real_v1_gate_rank_dedup_and_original_deadline():
    first, second, records, clock = assessed_candidates()
    rows = annotate_candidates([second, first], records, clock + timedelta(days=1))
    assert selection_funnel(rows)["eligible"] == 2
    assert selection_funnel(rows)["shortlist"] == 1
    assert selection_funnel(rows)["entry_open_now"] == 0
    assert next(row for row in rows if row["behavior"]["shortlist"])["candidate_id"] == first["candidate_id"]
    records[str(first["candidate_id"])]["recorded_at"] = first["valid_until"]
    rows = annotate_candidates([first, second], records, clock + timedelta(days=1))
    assert rows[0]["behavior"]["timely_at_recording"] is False
    assert rows[1]["behavior"]["shortlist"] is True


@pytest.mark.parametrize("change", ["profile", "gate", "identity", "future", "hash"])
def test_review_rejects_policy_identity_and_receipt_changes(change):
    from options.stock_behavior_gates import StockBehaviorGateAssessment

    first, _, records, clock = assessed_candidates()
    record = records[str(first["candidate_id"])]
    gate = StockBehaviorGateAssessment.model_validate_json(record["payload_text"])
    if change == "profile":
        gate = gate.model_copy(update={"stock_policy_sha256": "0" * 64})
    elif change == "gate":
        gate = gate.model_copy(update={"gates": (gate.gates[0].model_copy(update={"gate_id": "DIFFERENT_REQUIRED_GATE"}), *gate.gates[1:])})
    elif change == "identity":
        gate = gate.model_copy(update={"matrix_id": uuid4()})
    record.update(payload_text=gate.canonical_json(), payload_sha256=gate.sha256)
    if change == "future":
        record["recorded_at"] = clock + timedelta(days=2)
    elif change == "hash":
        record["payload_sha256"] = "0" * 64
    rows = annotate_candidates([first], records, clock + timedelta(days=1))
    assert rows[0]["behavior"]["reasons"] == ["STOCK_ASSESSMENT_INVALID"]
    assert rows[0]["behavior"]["eligible"] is False


@pytest.mark.parametrize("leg", [{}, {"model_mark": float("inf")}, {"source_market_time": None}, {"ratio": 0}])
def test_incomplete_legs_are_visible_but_not_eligible(leg):
    first, _, records, clock = assessed_candidates()
    first["legs"] = [{**first["legs"][0], **leg}] if leg else [{}]
    rows = annotate_candidates([first], records, clock + timedelta(days=1))
    assert rows[0]["behavior"]["eligible"] is False
    assert "OPTION_PACKAGE_INCOMPLETE" in rows[0]["behavior"]["reasons"]


def test_daily_matched_arms_keep_missingness_and_empty_timely_arm():
    first, second, records, clock = assessed_candidates()
    second["underlying"] = "MSFT"
    rows = annotate_candidates([first, second], records, clock + timedelta(days=1))
    rows[1]["behavior"].update(disposition="BLOCKED", eligible=False)
    rows[0]["behavior"]["timely_at_recording"] = False
    report = daily_scorecard(rows, [dict(candidate_id=first["candidate_id"], measurement_type="60MIN", net_return=.1)],
        [dict(candidate_id=second["candidate_id"], measurement_type="60MIN")], clock + timedelta(days=1))
    cells = {cell["arm"]: cell for cell in report["cells"] if cell["horizon"] == "60MIN"}
    baseline = cells["ASSESSMENT_COVERED_CANDIDATE_BASELINE"]
    assert baseline["cohorts"] == 2 and baseline["outcome_coverage"] == .5
    assert baseline["states"] == {"MEASURED": 1, "UNAVAILABLE": 1}
    assert baseline["mean_net_return"] == .1
    assert cells["BEHAVIOR_V1_PASSED"]["cohorts"] == 1
    assert cells["BEHAVIOR_V1_TIMELY"]["cohorts"] == 0
    assert cells["BEHAVIOR_V1_TIMELY"]["mean_net_return"] is None
    assert cells["BEHAVIOR_V1_TIMELY"]["outcome_coverage"] is None
    assert {row["candidate_id"] for row in report["cohort_rows"]} == {first["candidate_id"], second["candidate_id"]}
    assert report["cohort_rows"][0]["outcomes"]["60MIN"]["net_return"] == .1
    assert report["cohort_rows"][1]["outcomes"]["60MIN"]["net_return"] is None


def test_behavior_review_queries_are_bounded_read_only_and_policy_pinned(monkeypatch):
    from contextlib import contextmanager
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    from options.repositories.stock_behavior_assessments import OptionStockBehaviorAssessmentRepository
    from options.stock_behavior_gates import STOCK_BEHAVIOR_GATE_POLICY

    cursor = MagicMock()
    cursor.fetchone.return_value = dict(stock_ready=True, outcomes_ready=True, unavailable_ready=True)
    cursor.fetchall.side_effect = [[candidate()], [], []]
    cursor.__iter__.return_value = iter(())
    repository = OptionStockBehaviorAssessmentRepository()

    @contextmanager
    def reader():
        yield cursor

    monkeypatch.setattr(repository, "_cursor", reader)
    configuration = SimpleNamespace(policy_sha256="d" * 64, configuration_sha256="c" * 64,
        strategy_policy_sha256="b" * 64, valuation_policy_sha256="e" * 64, settings=SimpleNamespace(underlyers=("AAPL",)))
    result = repository.research_inputs(configuration=configuration, session_date=NOW.date(), as_of=NOW, daily=True)
    assert len(result["candidates"]) == 1
    statements = cursor.execute.call_args_list
    assert statements[0].args[0] == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
    assert statements[1].args[0] == "SET LOCAL statement_timeout = '5s'"
    for call in statements:
        assert call.args[0].strip().startswith(("SET", "WITH", "SELECT"))
        if len(call.args) > 1:
            assert call.args[0].count("%s") == len(call.args[1])
    assert statements[3].args[1][-1] == 50001
    assert "analysis.completed_at<=%s" in statements[3].args[0]
    assert "analysis.created_at<=%s" in statements[3].args[0]
    assert "candidate.created_at<=%s" in statements[3].args[0]
    assert "entry_snapshot.snapshot_id=leg.snapshot_id" in statements[3].args[0]
    assert "entry_snapshot.contract_id=leg.contract_id" in statements[3].args[0]
    assert "entry_snapshot.first_observed_at<=candidate.observed_time" in statements[3].args[0]
    assert "entry_snapshot.market_data_time<=candidate.market_data_time AND NOT %s" in statements[3].args[0]
    for field in ("spot", "model_mark", "local_iv", "quote_bid", "quote_ask", "day_volume", "open_interest"):
        assert f"'{field}'" in statements[3].args[0]
    assert STOCK_BEHAVIOR_GATE_POLICY.sha256 in statements[4].args[1]
    assert "recorded_at<=%s" in statements[4].args[0]
    assert "created_at<=%s AND observed_time<=%s" in statements[5].args[0]
    assert "availability_policy_sha256=%s" in statements[6].args[0]


@pytest.mark.parametrize("session_date,cutoff", [(NOW.date(), NOW.replace(tzinfo=None)),
    (NOW.date() + timedelta(days=1), NOW), (NOW.date() - timedelta(days=61), NOW)])
def test_behavior_review_rejects_invalid_bounds_before_connection(session_date, cutoff):
    from options.repositories.stock_behavior_assessments import OptionStockBehaviorAssessmentRepository

    repository = OptionStockBehaviorAssessmentRepository(lambda: pytest.fail("must not connect"))
    with pytest.raises(ValueError):
        repository.research_inputs(configuration=None, session_date=session_date, as_of=cutoff)


def test_behavior_review_cli_explicit_cutoff_and_no_overwrite(monkeypatch, tmp_path, capsys):
    import json
    import sys
    from options.analytics import behavior_review
    from scripts import report_option_stock_behavior_assessments as report

    calls = []
    def build(configuration, **kwargs):
        calls.append(kwargs)
        return dict(version="option_behavior_review_v1", schema={"stock_ready": False}, as_of=kwargs["as_of"])
    monkeypatch.setattr(behavior_review, "build_behavior_review", build)
    output = tmp_path / "review.json"
    monkeypatch.setattr(sys, "argv", ["report", "--session-date", "2026-09-18", "--as-of", "2026-09-18T20:30:00+00:00", "--output", str(output)])
    assert report.main() == 0
    retained = output.read_bytes()
    assert calls[0]["view"] == "DAILY" and calls[0]["as_of"].hour == 20
    assert json.loads(capsys.readouterr().out)["version"] == "option_behavior_review_v1"
    with pytest.raises(FileExistsError):
        report.main()
    assert output.read_bytes() == retained
    monkeypatch.setattr(sys, "argv", ["report", "--session-date", "2026-09-18", "--require-storage-ready"])
    assert report.main() == 2


@pytest.mark.parametrize("flags", [["--as-of", "2026-09-18T20:30:00"],
    ["--as-of", "2099-09-18T20:30:00+00:00"], ["--write-launch-artifact"], ["--launch-file", "frozen.json"]])
def test_behavior_review_cli_rejects_unsafe_cutoffs_and_launch_output(monkeypatch, flags):
    import sys
    from options.analytics import behavior_review
    from scripts import report_option_stock_behavior_assessments as report

    monkeypatch.setattr(behavior_review, "build_behavior_review", lambda *args, **kwargs: pytest.fail("must not query"))
    monkeypatch.setattr(sys, "argv", ["report", "--session-date", "2026-09-18", *flags])
    with pytest.raises(ValueError):
        report.main()


def test_behavior_review_api_dispatches_without_legacy_reader(monkeypatch):
    from types import SimpleNamespace
    from options import api
    from options.analytics import behavior_review

    calls = []
    monkeypatch.setattr(api, "_configuration", lambda: SimpleNamespace(policy_sha256="d" * 64))
    monkeypatch.setattr(api, "get_db_cursor", lambda: pytest.fail("legacy read must not run"))
    def build(configuration, **kwargs):
        calls.append(kwargs)
        return {"total": 0, "rows": [], "funnel": {"candidates": 897}}
    monkeypatch.setattr(behavior_review, "build_behavior_review", build)
    response = api.option_candidates(behavior_view="ELIGIBLE", session_date=NOW.date(), limit=50, offset=0)
    assert response.available and response.data["funnel"]["candidates"] == 897
    assert calls[0]["view"] == "ELIGIBLE"
    monkeypatch.setattr(behavior_review, "build_behavior_review", lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("bounds")))
    response = api.option_candidates(behavior_view="DAILY", session_date=NOW.date(), limit=50, offset=0)
    assert not response.available and response.reason == "BEHAVIOR_REVIEW_UNAVAILABLE"


def test_behavior_review_history_api_preserves_plan_and_binds_mark(monkeypatch):
    import hashlib
    import json
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    from options import api

    plan, mark, policy = history_mark_fixture()
    plan.update(underlying="AAPL", strategy="DIRECTIONAL_DEBIT_SPREAD", structure="CALL_DEBIT_VERTICAL",
        entry_limit="190", entry_limit_kind="MAXIMUM_DEBIT", entry_deadline=NOW.isoformat(),
        management_policy={"stop_loss_fraction": .35, "take_profit_fraction": .5}, management_policy_version="test_v1")
    text = json.dumps(plan)
    record = dict(event_id=uuid4(), sequence=4, plan_id=uuid4(), event_type="OBSERVED", recorded_at=NOW,
        plan_sha256=hashlib.sha256(text.encode("ascii")).hexdigest(), payload_text=text,
        request_text='{"event":"OBSERVED"}', hit_count=2, published_at=NOW)
    cursor = MagicMock()
    cursor.fetchone.return_value = {"ready": True}
    context = MagicMock()
    context.__enter__.return_value = cursor
    monkeypatch.setattr(api, "get_db_cursor", lambda: context)
    monkeypatch.setattr(api, "_configuration", lambda: SimpleNamespace(policy_sha256="a" * 64))
    monkeypatch.setattr(api, "configured_valuation_policy", lambda: policy)
    monkeypatch.setattr(api, "OptionAlertPublicationRepository", lambda: SimpleNamespace(history=lambda **_: (record,)))
    reader = MagicMock(return_value={"ready": True, "rows": {plan["candidate_id"]: mark}})
    monkeypatch.setattr(api, "OptionOutcomeRepository", lambda: SimpleNamespace(retained_plan_marks=reader))
    result = api.option_alert_publications(limit=10, offset=0).model_dump(mode="json")
    row = result["data"]["rows"][0]
    assert row["current_mark"]["gross_pnl"] == "100"
    assert row["current_mark"]["price_return"] == "0.5"
    assert row["current_mark"]["status"] == "STALE"
    assert row["entry_limit"] == "190" and row["original_economics"]["net_premium"] == "-200"
    assert row["hit_count"] == 2 and row["event_type"] == "OBSERVED"
    assert row["management_policy_version"] == "test_v1"
    reader.assert_called_once()
    record["plan_sha256"] = "0" * 64
    row = api.option_alert_publications(limit=10, offset=0).data["rows"][0]
    assert row["current_mark"]["reason"] == "FROZEN_PLAN_HASH_MISMATCH"
    assert row["current_mark"]["gross_pnl"] is None


def test_behavior_review_history_mark_batch_is_read_only_bounded_and_optional(monkeypatch):
    from contextlib import contextmanager
    from unittest.mock import MagicMock
    from options.repositories.outcomes import OptionOutcomeRepository

    _, record, policy = history_mark_fixture()
    cursor = MagicMock()
    cursor.fetchone.return_value = {"ready": True}
    cursor.fetchall.return_value = [record]
    @contextmanager
    def read():
        yield cursor
    repo = OptionOutcomeRepository()
    monkeypatch.setattr(repo, "_cursor", read)
    result = repo.retained_plan_marks([record["candidate_id"]], available_by=NOW, valuation_policy_sha256=policy.policy_sha256)
    assert result["rows"][str(record["candidate_id"])] == record
    sql, args = cursor.execute.call_args.args
    assert "snapshot.snapshot_id=ANY(mark.source_snapshot_ids)" in sql
    assert "snapshot.batch_id=mark.source_batch_id" in sql and "mark.updated_at<=%s" in sql
    assert sql.count("%s") == len(args)
    assert all(call.args[0].strip().startswith(("SET", "SELECT")) for call in cursor.execute.call_args_list)
    assert "READ ONLY" in cursor.execute.call_args_list[0].args[0]
    cursor.fetchone.return_value = {"ready": False}
    assert repo.retained_plan_marks([record["candidate_id"]], available_by=NOW, valuation_policy_sha256=policy.policy_sha256) == {"ready": False, "rows": {}}
    with pytest.raises(ValueError):
        repo.retained_plan_marks([uuid4() for _ in range(201)], available_by=NOW, valuation_policy_sha256=policy.policy_sha256)


def test_behavior_review_history_hits_count_distinct_source_windows_only(monkeypatch):
    from contextlib import contextmanager
    from unittest.mock import MagicMock
    from options.repositories.alert_publications import OptionAlertPublicationRepository

    cursor = MagicMock()
    cursor.fetchall.return_value = []
    @contextmanager
    def read():
        yield cursor
    repository = OptionAlertPublicationRepository()
    monkeypatch.setattr(repository, "_cursor", read)
    assert repository.history(limit=2, offset=3) == ()
    sql, args = cursor.execute.call_args.args
    assert args == (2, 3)
    assert "COUNT(DISTINCT CASE" in sql
    assert "event.event_type='PUBLISHED'" in sql and "event.event_type='OBSERVED'" in sql
    assert "source_available_at" in sql and "source_ids" in sql
    assert "event.event_type='EXPIRED'" not in sql and "event.event_type='INVALIDATED'" not in sql
    assert "READ ONLY" in cursor.execute.call_args_list[0].args[0]


@pytest.mark.skipif(os.getenv("OPTION_ALERT_READONLY_TESTS") != "1", reason="explicit read-only PostgreSQL check required")
def test_behavior_review_retained_history_and_mark_queries_live_read_only():
    from database import get_db_cursor
    from options.outcomes import configured_valuation_policy
    from options.repositories.outcomes import OptionOutcomeRepository
    from options.repositories.alert_publications import OptionAlertPublicationRepository
    from options.repositories.board import OptionBoardPublicationRepository
    from options.api import _configuration

    policy = configured_valuation_policy()
    assert len(OptionAlertPublicationRepository().history(limit=2)) <= 2
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION READ ONLY")
        cursor.execute("SET LOCAL statement_timeout = '5s'")
        cursor.execute("SELECT candidate_id FROM option_signal_current_marks WHERE valuation_policy_sha256=%s LIMIT 1", (policy.policy_sha256,))
        retained = cursor.fetchone()
    candidate_id = retained["candidate_id"] if retained else uuid4()
    result = OptionOutcomeRepository().retained_plan_marks([candidate_id], available_by=datetime.now(timezone.utc), valuation_policy_sha256=policy.policy_sha256)
    assert result["ready"]
    if retained:
        assert str(candidate_id) in result["rows"]
        assert 0 < len(result["rows"][str(candidate_id)]["legs"]) <= 4
    configuration = _configuration()
    board = OptionBoardPublicationRepository()
    assert isinstance(board.baseline_runs(configuration=configuration, as_of=datetime.now(timezone.utc)), list)
    assert board.baseline_members([uuid4()], configuration=configuration, as_of=datetime.now(timezone.utc)) == {}