import inspect
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch
from types import SimpleNamespace
from uuid import uuid4

import pytest


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from equity.domain import (
    BarAvailabilityMode,
    BarSessionScope,
    BarSourceKind,
    ContextStatus,
    DecisionWatermark,
    EquityBarRevision,
    EquityContextSnapshot,
    EquityCorporateAction,
)
from equity.repositories import (
    EquityAnalysisRepository,
    EquityBarRepository,
    EquityCorporateActionRepository,
    EquityEvidenceRepository,
    EquityIngestionRepository,
    EquityOutcomeRepository,
    EquityReferenceRepository,
    EquityUniverseRepository,
)
from equity.outcomes import default_directional_policy
from equity.historical_universe import select_universe_revisions


UTC = timezone.utc
HASH = "a" * 64


def _repository(repository_type):
    cursor = MagicMock()
    cursor.closed = False
    cursor.fetchone.return_value = None
    cursor.fetchall.return_value = []
    connection = MagicMock()
    connection.closed = False
    connection.cursor.return_value = cursor

    @contextmanager
    def factory():
        yield connection

    return repository_type(factory), connection, cursor


def _watermark():
    market_time = datetime(2026, 8, 28, 15, 0, tzinfo=UTC)
    return DecisionWatermark(market_time, market_time + timedelta(seconds=2))


def _context(**overrides):
    values = {
        "equity_context_snapshot_id": uuid4(),
        "security_id": uuid4(),
        "ticker": "AAPL",
        "strategy_horizon": "INTRADAY_30M",
        "market_time": datetime(2026, 8, 28, 15, 0, tzinfo=UTC),
        "observed_at": datetime(2026, 8, 28, 15, 0, 2, tzinfo=UTC),
        "valid_until": datetime(2026, 8, 28, 16, 0, tzinfo=UTC),
        "status": ContextStatus.DEGRADED,
        "universe_run_id": uuid4(),
        "security_revision_id": uuid4(),
        "fundamental_snapshot_id": None,
        "regime_state": "CONTINUATION",
        "ema_direction": "BULLISH",
        "qualified_direction": None,
        "direction_qualification_id": None,
        "direction_evidence_id": None,
        "direction_horizon": None,
        "direction_valid_until": None,
        "trigger_state": None,
        "trigger_valid_until": None,
        "range_forecast_id": None,
        "range_lower": None,
        "range_upper": None,
        "range_valid_until": None,
        "market_cap": Decimal("3000000000000"),
        "shares_outstanding": Decimal("15000000000"),
        "free_float": Decimal("14500000000"),
        "dividend_yield": 0.004,
        "enterprise_value": None,
        "ebitda": None,
        "operating_income": None,
        "free_cash_flow": None,
        "risk_levels_json": "{}",
        "conflict_state_json": "{}",
        "stale_components_json": "[]",
        "reason_codes": ("QUALIFIED_DIRECTION_UNAVAILABLE",),
        "summary_json": "{}",
        "context_policy_version": "equity_context_v1",
        "context_policy_sha256": HASH,
    }
    values.update(overrides)
    return EquityContextSnapshot(**values)


@pytest.mark.parametrize("duplicate", ["security_id", "ticker"])
def test_universe_rejects_duplicate_members_before_writing(duplicate):
    repository, _, cursor = _repository(EquityUniverseRepository)
    first = SimpleNamespace(security_id=uuid4(), ticker="FIRST")
    second = SimpleNamespace(security_id=uuid4(), ticker="SECOND")
    setattr(second, duplicate, getattr(first, duplicate))

    with pytest.raises(ValueError, match="duplicate"):
        repository.persist_complete_run(
            universe_run_id=uuid4(), source="TEST", mode="RANKED",
            effective_from=_watermark().market_time, observed_at=_watermark().observed_time,
            policy_version="test", policy_sha256=HASH, members=(first, second), configuration={},
        )

    cursor.execute.assert_not_called()


@pytest.mark.parametrize("stored, expected_error", [(1503, True), (1504, False)])
def test_historical_resume_verifies_stored_membership_count(stored, expected_error):
    repository, _, cursor = _repository(EquityUniverseRepository)
    run_id = uuid4()
    cursor.fetchone.return_value = {
        "universe_run_id": run_id, "admitted_members": 1504, "stored_member_count": stored,
    }
    arguments = {"policy_sha256": HASH, "effective_from": _watermark().market_time}
    if expected_error:
        with pytest.raises(ValueError, match="refusing to resume"):
            repository.get_reconstructed_session(**arguments)
    else:
        assert repository.get_reconstructed_session(**arguments) == {
            "universe_run_id": run_id, "admitted_members": 1504,
        }
    assert "COUNT(*) FROM equity_universe_members" in cursor.execute.call_args.args[0]


@pytest.fixture
def universe_revision_chain():
    observed = datetime(2026, 9, 4, tzinfo=UTC)
    original = {"universe_run_id": uuid4(), "effective_from": datetime(2024, 3, 4, tzinfo=UTC),
                "policy_sha256": HASH, "observed_at": observed, "supersedes_universe_run_id": None,
                "revision_published_at": None}
    correction = original | {"universe_run_id": uuid4(), "observed_at": observed + timedelta(days=8),
                             "supersedes_universe_run_id": original["universe_run_id"],
                             "revision_published_at": observed + timedelta(days=8, hours=1)}
    return original, correction


def test_universe_revisions_default_to_original_and_pins_do_not_upgrade(universe_revision_chain):
    original, correction = universe_revision_chain
    assert select_universe_revisions(universe_revision_chain) == (original,)
    assert select_universe_revisions(universe_revision_chain, pinned_run_ids=[original["universe_run_id"]]) == (original,)
    assert select_universe_revisions(universe_revision_chain, pinned_run_ids=[correction["universe_run_id"]]) == (correction,)


@pytest.mark.parametrize("offset, corrected", [(-1, False), (0, True), (1, True)])
def test_universe_revision_visibility_uses_publication_cutoff(universe_revision_chain, offset, corrected):
    original, correction = universe_revision_chain
    cutoff = correction["revision_published_at"] + timedelta(seconds=offset)
    assert select_universe_revisions(universe_revision_chain, revision_cutoff=cutoff) == ((correction if corrected else original),)


@pytest.mark.parametrize("failure", ["root", "parent", "branch", "policy", "date", "time", "observed"])
def test_universe_revision_graph_rejects_ambiguity_and_invalid_lineage(universe_revision_chain, failure):
    original, correction = [dict(row) for row in universe_revision_chain]
    rows = [original, correction]
    if failure == "root":
        rows.append(original | {"universe_run_id": uuid4()})
    elif failure == "parent":
        correction["supersedes_universe_run_id"] = uuid4()
    elif failure == "branch":
        rows.append(correction | {"universe_run_id": uuid4()})
    elif failure == "policy":
        correction["policy_sha256"] = "b" * 64
    elif failure == "date":
        correction["effective_from"] += timedelta(days=1)
    elif failure == "time":
        correction["revision_published_at"] = original["observed_at"]
    else:
        correction["observed_at"] = correction["revision_published_at"] + timedelta(seconds=1)
    with pytest.raises(ValueError):
        select_universe_revisions(rows)


@pytest.mark.parametrize("pins", [[], ["unknown"]])
def test_universe_revision_pins_must_cover_requested_sessions(universe_revision_chain, pins):
    with pytest.raises(ValueError):
        select_universe_revisions(universe_revision_chain, pinned_run_ids=pins)


def test_universe_revision_cutoff_does_not_backdate_observation(universe_revision_chain):
    original, _ = universe_revision_chain
    with pytest.raises(ValueError, match="not observed"):
        select_universe_revisions(universe_revision_chain, revision_cutoff=original["effective_from"])
    with pytest.raises(ValueError, match="timezone-aware"):
        select_universe_revisions(universe_revision_chain, revision_cutoff=datetime(2026, 9, 12))


def test_multiple_corrections_follow_cutoff_but_exact_pins_stay_fixed(universe_revision_chain):
    original, correction = universe_revision_chain
    later = correction | {"universe_run_id": uuid4(), "supersedes_universe_run_id": correction["universe_run_id"],
                          "revision_published_at": correction["revision_published_at"] + timedelta(hours=1)}
    rows = [later, original, correction]
    assert select_universe_revisions(rows, revision_cutoff=correction["revision_published_at"]) == (correction,)
    assert select_universe_revisions(rows, revision_cutoff=later["revision_published_at"]) == (later,)
    assert select_universe_revisions(rows, pinned_run_ids=[correction["universe_run_id"]]) == (correction,)
    with pytest.raises(ValueError, match="not both"):
        select_universe_revisions(rows, pinned_run_ids=[original["universe_run_id"]], revision_cutoff=later["revision_published_at"])


def test_repository_returns_only_selected_complete_universe_revision(universe_revision_chain):
    repository, _, cursor = _repository(EquityUniverseRepository)
    rows = [row | {"status": "COMPLETE", "admitted_members": 1, "stored_member_count": 1}
        for row in universe_revision_chain]
    cursor.fetchall.return_value = rows
    assert repository.list_reconstructed_revisions(policy_version="test") == (rows[0],)
    assert repository.list_reconstructed_revisions(policy_version="test", revision_cutoff=rows[1]["revision_published_at"]) == (rows[1],)
    rows[1]["stored_member_count"] = 0
    with pytest.raises(ValueError, match="incomplete"):
        repository.list_reconstructed_revisions(
            policy_version="test", pinned_run_ids=[rows[1]["universe_run_id"]],
        )


def test_audited_count_discrepancy_is_explicit_exact_and_original_only(universe_revision_chain):
    repository, _, cursor = _repository(EquityUniverseRepository)
    original, correction = universe_revision_chain
    row = dict(original, status="COMPLETE", admitted_members=1504, stored_member_count=1503)
    audit = dict(session="2024-03-04", policy_sha256=HASH, declared_members=1504, stored_members=1503)
    allowed = {str(row["universe_run_id"]): audit}
    cursor.fetchall.return_value = [row]
    with pytest.raises(ValueError, match="declared=1504, stored=1503"):
        repository.list_reconstructed_revisions(policy_version="test")
    selected = repository.list_reconstructed_revisions(policy_version="test", audited_count_discrepancies=allowed)
    assert selected[0]["member_count_discrepancy"] == audit
    assert selected[0]["stored_member_count"] == 1503
    assert "member_count_discrepancy" not in row
    for changes in ({"stored_member_count": 1502}, {"admitted_members": 1505}, {"status": "DEGRADED"},
                    {"policy_sha256": "f" * 64}, {"effective_from": row["effective_from"] + timedelta(days=1)},
                    {"universe_run_id": uuid4()}):
        cursor.fetchall.return_value = [dict(row, **changes)]
        with pytest.raises(ValueError, match="incomplete"):
            repository.list_reconstructed_revisions(policy_version="test", audited_count_discrepancies=allowed)
    revised = dict(correction, status="COMPLETE", admitted_members=1504, stored_member_count=1503)
    cursor.fetchall.return_value = [row, revised]
    with pytest.raises(ValueError, match="incomplete"):
        repository.list_reconstructed_revisions(policy_version="test", pinned_run_ids=[revised["universe_run_id"]],
                                               audited_count_discrepancies={str(revised["universe_run_id"]): audit})


def test_reference_read_requires_both_watermarks():
    repository, connection, cursor = _repository(EquityReferenceRepository)
    context = _watermark()

    assert repository.get_security_as_of("aapl", context) is None

    sql, parameters = cursor.execute.call_args.args
    assert "effective_from <= %s" in sql
    assert "observed_at <= %s" in sql
    assert parameters == ("AAPL", context.market_time, context.observed_time)
    connection.commit.assert_called_once_with()


def test_reconstructed_universe_is_available_only_to_replay_reads():
    repository, _, cursor = _repository(EquityUniverseRepository)
    context = _watermark()

    assert repository.get_latest_as_of(context) is None
    live_sql, live_parameters = cursor.execute.call_args.args
    assert "observed_at <= %s" in live_sql
    assert "availability_mode = 'LIVE_OBSERVED'" in live_sql
    assert live_parameters == (context.market_time, context.observed_time)

    assert repository.get_latest_for_replay(context) is None
    replay_sql, replay_parameters = cursor.execute.call_args.args
    assert "COALESCE(replay_available_at, observed_at) <= %s" in replay_sql
    assert "observed_at DESC" in replay_sql
    assert replay_parameters == (context.market_time, context.observed_time)


def test_replay_members_prefer_point_in_time_classified_security_revision():
    repository, _, cursor = _repository(EquityUniverseRepository)
    universe_run_id = uuid4()

    assert repository.members_for_replay(universe_run_id, ("msft", "AAPL")) == ()

    sql, parameters = cursor.execute.call_args.args
    assert "candidate.security_id = member.security_id" in sql
    assert "candidate.effective_from <= member.effective_from" in sql
    assert "candidate.sector IS NOT NULL" in sql
    assert "member.ticker = ANY(%s::TEXT[])" in sql
    assert "reference.observed_at <=" not in sql
    assert parameters == (None, None, None, universe_run_id, ["AAPL", "MSFT"], ["AAPL", "MSFT"])
    cutoff = "2026-09-12T00:00:00+00:00"
    repository.members_for_replay(universe_run_id, ("AAPL",), observed_by=cutoff)
    query, parameters = cursor.execute.call_args.args
    assert "candidate.observed_at<=%s AND candidate.created_at<=%s" in query
    assert parameters[:3] == (cutoff, cutoff, cutoff)


def test_historical_sector_candidates_exclude_existing_point_in_time_classification():
    repository, _, cursor = _repository(EquityReferenceRepository)

    assert repository.list_historical_sector_candidates("policy-v2") == ()

    sql, parameters = cursor.execute.call_args.args
    assert "run.availability_mode = 'HISTORICAL_RECONSTRUCTED'" in sql
    assert "reference.effective_from <= member.effective_from" in sql
    assert "reference.sector IS NOT NULL" in sql
    assert parameters == ("policy-v2",)


def test_universe_snapshot_member_read_does_not_filter_against_wall_clock():
    repository, _, cursor = _repository(EquityUniverseRepository)
    universe_run_id = uuid4()

    assert repository.member_tickers(universe_run_id) == frozenset()

    sql, parameters = cursor.execute.call_args.args
    assert "NOW()" not in sql
    assert parameters == (universe_run_id,)


def test_corporate_action_persistence_is_idempotent_by_action_identity():
    repository, _, _ = _repository(EquityCorporateActionRepository)
    action = EquityCorporateAction(
        corporate_action_id=uuid4(), security_id=uuid4(), ticker="AAPL",
        action_type="SPLIT", effective_date=_watermark().market_time.date(),
        declaration_date=None, ex_date=None, record_date=None, pay_date=None,
        cash_amount=None, split_from=Decimal("1"), split_to=Decimal("2"),
        new_ticker=None, source="POLYGON_CORPORATE_ACTIONS_V1",
        source_key="split-1", first_observed_at=_watermark().observed_time,
        revised_observed_at=None, payload_sha256=HASH, raw_payload_json="{}",
        availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
        replay_available_at=_watermark().market_time,
    )

    with patch(
        "equity.repositories.execute_values",
        return_value=[{"corporate_action_id": action.corporate_action_id}],
    ) as execute_values:
        assert repository.persist((action,)) == 1

    sql = " ".join(execute_values.call_args.args[1].split())
    assert "ON CONFLICT (corporate_action_id) DO NOTHING" in sql


def test_action_and_coverage_observation_share_one_transaction():
    repository, connection, cursor = _repository(EquityCorporateActionRepository)
    with patch.object(repository, "persist", return_value=1) as actions, \
            patch.object(repository, "persist_coverage", return_value=2) as coverage:
        assert repository.persist_observation(["coverage"], ["action"]) == (1, 2)
        actions.assert_called_once_with(["action"], cursor=cursor)
        coverage.assert_called_once_with(["coverage"], ["action"], cursor=cursor)
    connection.commit.assert_called_once_with()


def test_failed_coverage_rolls_back_action_observation():
    repository, connection, _ = _repository(EquityCorporateActionRepository)
    with patch.object(repository, "persist", return_value=1), \
            patch.object(repository, "persist_coverage", side_effect=ValueError("bad response")):
        with pytest.raises(ValueError, match="bad response"):
            repository.persist_observation(["coverage"], ["action"])
    connection.commit.assert_not_called()
    connection.rollback.assert_called_once_with()


def test_corporate_action_replay_read_uses_availability_watermark():
    repository, _, cursor = _repository(EquityCorporateActionRepository)
    context = _watermark()

    assert repository.list_for_replay(
        ["aapl"], context, start_date=context.market_time.date()
    ) == ()

    sql, parameters = cursor.execute.call_args.args
    assert "COALESCE(replay_available_at, first_observed_at) <= %s" in sql
    assert parameters[0] == ["AAPL"]
    assert parameters[-1] == context.observed_time


def test_fundamental_read_uses_public_availability_and_observation_bounds():
    repository, _, cursor = _repository(EquityReferenceRepository)
    context = _watermark()
    security_id = uuid4()

    assert repository.list_fundamentals_as_of(
        security_id, context, timeframe="quarterly", limit=4
    ) == ()

    sql, parameters = cursor.execute.call_args.args
    assert "availability_time <= %s" in sql
    assert "observed_at <= %s" in sql
    assert parameters[:3] == (security_id, context.market_time, context.observed_time)


def test_bar_read_selects_latest_visible_revision_before_limiting():
    repository, _, cursor = _repository(EquityBarRepository)
    context = _watermark()

    assert repository.list_final_as_of("AAPL", "30m", context, limit=20) == ()

    sql, parameters = cursor.execute.call_args.args
    assert "DISTINCT ON (ticker, interval, bar_start)" in sql
    assert "bar_end <= %s" in sql
    assert "COALESCE(replay_available_at, system_observed_at) <= %s" in sql
    assert "session_scope = %s" in sql
    assert "source_kind = 'RECONCILED' THEN 0" in sql
    assert "availability_mode = 'LIVE_OBSERVED' THEN 0 ELSE 1" in sql
    assert "GROUPED_DAILY_AGGREGATE" in sql
    assert parameters[2:4] == (BarSessionScope.RTH.value, False)
    assert parameters[-1] == 20


def test_bulk_bar_read_limits_each_ticker_after_revision_selection():
    repository, _, cursor = _repository(EquityBarRepository)
    context = _watermark()

    result = repository.list_final_for_tickers_as_of(
        ("AAPL", "MSFT"), "30m", context, limit_per_ticker=20
    )

    sql, parameters = cursor.execute.call_args.args
    assert "DISTINCT ON (ticker, interval, bar_start)" in sql
    assert "ROW_NUMBER() OVER" in sql
    assert "PARTITION BY ticker ORDER BY bar_start DESC" in sql
    assert "recency_rank <= %s" in sql
    assert parameters[-1] == 20
    assert result == {"AAPL": (), "MSFT": ()}


def test_bulk_bar_read_can_return_full_visible_history():
    repository, _, cursor = _repository(EquityBarRepository)
    context = _watermark()

    result = repository.list_final_for_tickers_as_of(
        ("AAPL", "MSFT"), "30m", context, limit_per_ticker=None
    )

    sql, parameters = cursor.execute.call_args.args
    assert "recency_rank <= %s" not in sql
    assert parameters == (
        ["AAPL", "MSFT"], "30m", BarSessionScope.RTH.value, False,
        context.market_time, context.observed_time,
    )
    assert result == {"AAPL": (), "MSFT": ()}


def test_daily_session_read_uses_canonical_source_precedence():
    repository, _, cursor = _repository(EquityBarRepository)
    session_date = _watermark().market_time.date()

    assert repository.daily_session_bars(
        ("AAPL", "MSFT"), session_date,
        observed_by=_watermark().observed_time,
    ) == {}

    sql, parameters = cursor.execute.call_args.args
    assert "interval = '1d'" in sql
    assert "session_date = %s" in sql
    assert "source_kind = 'RECONCILED' THEN 0" in sql
    assert "source_kind = 'DERIVED' THEN 1" in sql
    assert "availability_mode = 'LIVE_OBSERVED' THEN 0 ELSE 1" in sql
    assert parameters == (
        ["AAPL", "MSFT"], session_date, "RTH", False,
        _watermark().observed_time,
    )


def test_bar_persist_is_insert_only_and_deduplicates_on_conflict():
    repository, _, _ = _repository(EquityBarRepository)
    bar = MagicMock()
    bar.source_kind.value = "NATIVE_REST"
    bar.availability_mode.value = "LIVE_OBSERVED"
    bar.session_scope.value = "RTH"

    with patch("equity.repositories.execute_values", return_value=[]) as execute_values:
        assert repository.persist([bar]) == 0

    sql = " ".join(execute_values.call_args.args[1].split()).upper()
    assert "INSERT INTO EQUITY_BAR_REVISIONS" in sql
    assert "ON CONFLICT" in sql
    assert "PAYLOAD_SHA256" in sql
    assert "SESSION_SCOPE" in sql
    assert "ADJUSTED" in sql
    assert "DO NOTHING" in sql
    assert "DO UPDATE" not in sql


def test_outcome_persist_returns_execute_values_insert_count():
    repository, _, _ = _repository(EquityOutcomeRepository)
    outcome = MagicMock()
    outcome.quality_codes = ()
    outcome.path_bar_ids = ()
    outcome.benchmark_bar_ids = ()

    with patch(
        "equity.repositories.execute_values",
        return_value=[{"outcome_id": uuid4()}],
    ) as execute_values:
        assert repository.persist_outcomes((outcome,)) == 1

    sql = " ".join(execute_values.call_args.args[1].split())
    assert "ON CONFLICT" in sql
    assert "RETURNING outcome_id" in sql


def test_pending_outcome_subjects_bind_maturity_cutoff():
    repository, _, cursor = _repository(EquityOutcomeRepository)
    policy = default_directional_policy(
        source_name="GAP_BREAKAWAY_HOLD",
        source_version="gap_formation_v1",
        interval="1d",
        horizons={"5d": 5},
        effective_from=datetime(2026, 4, 9, tzinfo=UTC),
    )
    cutoff = datetime(2026, 8, 24, 20, tzinfo=UTC)
    subject_ids = (uuid4(), uuid4())

    assert repository.list_pending_directional_subjects(
        policy, "5d", available_by=_watermark().observed_time,
        signal_observed_through=cutoff, prospective_only=True,
        subject_evidence_ids=subject_ids,
    ) == ()

    sql, parameters = cursor.execute.call_args.args
    assert "evidence.observed_at <= %s" in sql
    assert "analysis.run_purpose = 'ORIGINAL'" in sql
    assert "evidence.observed_at >= %s" in sql
    assert "UNNEST(%s::UUID[]) WITH ORDINALITY" in sql
    assert "requested.evidence_id = evidence.evidence_id" in sql
    assert "ORDER BY requested.ordinal" in sql
    assert "cardinality" not in sql
    assert parameters[5:10] == (
        policy.effective_from, None, None, _watermark().observed_time, True,
    )
    assert parameters[10:12] == (cutoff, cutoff)
    assert parameters[0] == list(subject_ids)
    assert parameters[12:14] == (policy.outcome_policy_id, "5d")


def test_stale_outcome_revision_context_and_marking_are_explicit():
    repository, _, cursor = _repository(EquityOutcomeRepository)
    policy = default_directional_policy(
        source_name="breakout_expansion", source_version="1.0", interval="1d",
        horizons={"5d": 5}, effective_from=datetime(2026, 4, 9, tzinfo=UTC),
    )
    subject_id = uuid4()
    outcome_id = uuid4()
    cursor.fetchall.return_value = [{
        "subject_evidence_id": subject_id,
        "outcome_id": outcome_id,
        "outcome_revision": 1,
    }]

    revisions = repository.outcome_revision_context(
        (subject_id,), policy, "5d"
    )

    assert revisions == {subject_id: (2, outcome_id)}
    sql, parameters = cursor.execute.call_args.args
    assert "outcome_revision DESC" in sql
    assert parameters == ([subject_id], policy.outcome_policy_id, "5d")

    cursor.rowcount = 3
    assert repository.mark_outcomes_stale(
        (subject_id,), "ENTRY_SESSION_CONTINUITY_CORRECTION"
    ) == 3
    sql, parameters = cursor.execute.call_args.args
    assert "is_stale = TRUE" in sql
    assert "array_append" in sql
    assert parameters == (
        "ENTRY_SESSION_CONTINUITY_CORRECTION",
        "ENTRY_SESSION_CONTINUITY_CORRECTION",
        [subject_id],
    )


def test_historical_outcome_bars_can_require_exact_reconstructed_lineage():
    repository, _, cursor = _repository(EquityBarRepository)

    assert repository.list_final_after(
        "AAPL", "1d", after=_watermark().market_time,
        available_by=_watermark().observed_time, limit=5,
        historical_reconstructed_only=True,
    ) == ()

    sql, parameters = cursor.execute.call_args.args
    assert "GROUPED_DAILY_EXACT_TICKER_V2" in sql
    assert "availability_mode = 'HISTORICAL_RECONSTRUCTED'" in sql
    assert parameters[4] is True


def test_outcome_entry_bar_boundary_excludes_the_signal_bar_instant():
    """Entry may not occur at the instant the signal was observed."""
    repository, _, cursor = _repository(EquityBarRepository)

    repository.list_final_after(
        "AAPL", "30m", after=datetime(2026, 9, 2, 14, 30, tzinfo=UTC),
        available_by=datetime(2026, 9, 2, 21, 0, tzinfo=UTC), limit=4,
    )

    sql, _ = cursor.execute.call_args.args
    assert "bar_start > %s" in sql


def test_qualification_observations_are_scoped_to_declared_sources():
    repository, _, cursor = _repository(EquityOutcomeRepository)
    sources = ("GAP_BREAKAWAY_HOLD", "GAP_CONTINUATION_HOLD")
    subject_ids = (uuid4(), uuid4())
    policy_keys = (
        "GAP_BREAKAWAY_HOLD:gap_formation_v2:1d:SIGNED:SECTOR_PRIMARY",
    )
    policy_ids = (uuid4(),)

    assert repository.qualification_observations(
        available_by=_watermark().observed_time,
        interval="1d",
        source_names=sources,
        subject_evidence_ids=subject_ids,
        outcome_policy_keys=policy_keys,
        outcome_policy_ids=policy_ids,
    ) == []

    sql, parameters = cursor.execute.call_args.args
    assert "DISTINCT ON" in sql
    assert "outcome.outcome_policy_id" in sql
    assert "policy.created_at DESC" in sql
    assert "outcome.outcome_revision = 1" not in sql
    assert "outcome.outcome_revision DESC" in sql
    assert "outcome.outcome_id" in sql
    assert "outcome.outcome_id, outcome.outcome_policy_id," in " ".join(sql.split())
    assert "evidence.observed_at AS signal_time" in sql
    assert "evidence.market_time AS signal_market_time" in sql
    assert "outcome.entry_time, outcome.exit_time" in sql
    assert "outcome.outcome_available_at," in sql
    assert "outcome.subject_evidence_id" in sql
    assert "outcome.sector_net_alpha" in sql
    assert "AS primary_benchmark" in sql
    assert "AS has_bracket" in sql
    assert "outcome.subject_evidence_id = ANY(%s::UUID[])" in sql
    assert "evidence.source_name = ANY(%s::TEXT[])" in sql
    assert "policy.policy_key = ANY(%s::TEXT[])" in sql
    assert "OR outcome.outcome_policy_id = ANY(%s::UUID[])" in sql
    assert parameters[3:5] == (list(subject_ids), list(subject_ids))
    assert parameters[5:7] == (list(sources), list(sources))
    assert parameters[7:9] == (list(policy_keys), list(policy_keys))
    assert parameters[9:11] == (list(policy_ids), list(policy_ids))


def test_qualification_observations_preserve_policy_identity_without_an_id_filter():
    repository, _, cursor = _repository(EquityOutcomeRepository)
    policy_id = uuid4()
    cursor.fetchall.return_value = [{"outcome_policy_id": policy_id}]

    rows = repository.qualification_observations(available_by=_watermark().observed_time)

    assert rows == [{"outcome_policy_id": policy_id}]
    _, parameters = cursor.execute.call_args.args
    assert parameters[9:11] == ([], [])


def test_canonical_publication_is_atomic_and_advances_current_pointer():
    repository, _, cursor = _repository(EquityBarRepository)
    publication_id = uuid4()
    security_id = uuid4()
    bar = EquityBarRevision(
        bar_revision_id=uuid4(), security_id=security_id, ticker="AAPL",
        interval="30m", session_date=_watermark().market_time.date(),
        bar_start=_watermark().market_time - timedelta(minutes=30),
        bar_end=_watermark().market_time, open_price=Decimal("100"),
        high_price=Decimal("102"), low_price=Decimal("99"),
        close_price=Decimal("101"), volume=Decimal("1000"), vwap=None,
        transaction_count=None, source_kind=BarSourceKind.RECONCILED,
        availability_mode=BarAvailabilityMode.LIVE_OBSERVED, is_final=True,
        system_observed_at=_watermark().observed_time, replay_available_at=None,
        adjusted=False, payload_sha256=HASH,
    )
    member = MagicMock(security_id=security_id, ticker="AAPL")
    cursor.fetchone.return_value = {
        "publication_id": publication_id,
        "status": "COMPLETE",
        "published_at": _watermark().observed_time,
    }

    with patch("equity.repositories.execute_values") as execute_values:
        result = repository.publish_canonical_cohort(
            publication_id=publication_id, business_key="30m:test", interval="30m",
            market_time=_watermark().market_time,
            observed_at=_watermark().observed_time,
            session_scope=BarSessionScope.RTH, adjusted=False,
            selection_policy_version="equity_bar_selection_v1",
            selection_policy_sha256=HASH, input_sha256=HASH, output_sha256=HASH,
            members=(member,), selected={"AAPL": bar}, minimum_coverage=0.95,
        )

    assert result["status"] == "COMPLETE"
    assert execute_values.call_count == 2
    member_sql = " ".join(execute_values.call_args_list[0].args[1].split())
    projection_sql = " ".join(execute_values.call_args_list[1].args[1].split())
    assert "equity_bar_publication_members" in member_sql
    assert "equity_current_bar_projection" in projection_sql
    assert "DO UPDATE SET" in projection_sql


def test_evidence_read_never_uses_current_projection_for_asof_context():
    repository, _, cursor = _repository(EquityEvidenceRepository)
    context = _watermark()

    assert repository.list_as_of("AAPL", context) == ()

    sql, parameters = cursor.execute.call_args.args
    assert "FROM equity_evidence" in sql
    assert "equity_current_projection" not in sql
    assert "market_time <= %s" in sql
    assert "observed_at <= %s" in sql
    assert parameters[1:3] == (context.market_time, context.observed_time)


@pytest.mark.parametrize("migrated", [False, True])
def test_context_read_enforces_market_observation_and_validity_bounds(migrated):
    repository, _, cursor = _repository(EquityEvidenceRepository)
    context = _watermark()
    cursor.fetchone.side_effect = [{"ready": migrated}, None]

    assert repository.get_context_as_of(
        "AAPL", "INTRADAY_30M", context, policy_sha256=HASH
    ) is None

    sql, parameters = cursor.execute.call_args.args
    assert "market_time <= %s" in sql
    assert "observed_at <= %s" in sql
    assert "valid_until IS NULL OR valid_until > %s" in sql
    assert parameters[2:5] == (
        context.market_time, context.observed_time, context.market_time
    )
    assert ("context_kind = 'LEGACY'" in sql) is migrated
    assert "to_jsonb" not in sql
    assert "SELECT *" not in sql

def test_behavior_feature_source_read_is_bounded_and_uses_both_asof_clocks():
    repository, _, cursor = _repository(EquityEvidenceRepository)
    context = _watermark()
    cursor.fetchall.return_value = []

    assert repository.read_behavior_feature_sources(["aapl", "AAPL"], context) == ()

    sql, parameters = cursor.execute.call_args_list[2].args
    assert "PARTITION BY ticker, interval" in sql
    assert "evidence_type = 'FEATURE_SNAPSHOT'" in sql and "source_name = 'EQUITY_FEATURES'" in sql
    assert "market_time <= %s" in sql and "observed_at <= %s" in sql and "created_at <= %s" in sql
    assert parameters == (["AAPL"], ["1d", "1h", "30m"], context.market_time, context.observed_time, context.observed_time)
    assert cursor.execute.call_count == 3

@pytest.mark.parametrize("tickers,intervals,bound", [([], ("1d",), 10), (["AAPL"], ("5m",), 10), (["AAPL"], ("1d",), 0)])
def test_behavior_feature_source_read_rejects_unbounded_or_unsupported_scope(tickers, intervals, bound):
    repository, _, _ = _repository(EquityEvidenceRepository)
    with pytest.raises(ValueError):
        repository.read_behavior_feature_sources(tickers, _watermark(), intervals=intervals, maximum_source_bars=bound)


def test_behavior_feature_source_window_selects_bounded_latest_revisions():
    from equity.repositories import _behavior_source_revision_window

    revisions = tuple(uuid4() for _ in range(300))
    assert _behavior_source_revision_window({"source_revision_ids": revisions}, 273) == revisions[-273:]


def test_behavior_feature_source_read_accepts_exact_per_interval_limits():
    repository, _, cursor = _repository(EquityEvidenceRepository)
    cursor.fetchall.return_value = []
    result = repository.read_behavior_feature_sources(
        ["AAPL"], _watermark(), maximum_source_bars=273,
        source_bars_by_interval={"1d": 273, "1h": 200, "30m": 200},
    )
    assert result == ()

    with pytest.raises(ValueError, match="per-interval"):
        repository.read_behavior_feature_sources(
            ["AAPL"], _watermark(), maximum_source_bars=273,
            source_bars_by_interval={"5m": 200},
        )


def test_behavior_adjusted_daily_read_is_exact_bounded_and_recording_safe():
    repository, _, cursor = _repository(EquityBarRepository)
    context = _watermark()
    cursor.fetchall.return_value = []

    assert repository.read_behavior_adjusted_daily(["aapl", "AAPL"], context) == ()

    sql, parameters = cursor.execute.call_args_list[2].args
    assert "adjusted = %s" in sql and "availability_mode = 'HISTORICAL_RECONSTRUCTED'" in sql
    assert "GROUPED_DAILY_EXACT_TICKER_V2" in sql
    assert "system_observed_at <= %s" in sql and "replay_available_at <= %s" in sql
    assert "created_at <= %s" in sql and "provider_published_at IS NULL" in sql
    assert parameters == (
        ["AAPL"], True, context.market_time, context.observed_time, context.observed_time,
        context.observed_time, context.observed_time, 273,
    )

    cursor.reset_mock()
    cursor.fetchall.return_value = []
    assert repository.read_behavior_grouped_daily(
        ["AAPL"], context, adjusted=False, limit_per_ticker=200,
    ) == ()
    assert cursor.execute.call_args_list[2].args[1][1] is False


@pytest.mark.parametrize("tickers,limit", [([], 273), (["AAPL"], 0), (["AAPL"], 1025)])
def test_behavior_adjusted_daily_read_rejects_invalid_scope(tickers, limit):
    repository, _, _ = _repository(EquityBarRepository)
    with pytest.raises(ValueError):
        repository.read_behavior_adjusted_daily(tickers, _watermark(), limit_per_ticker=limit)


def test_behavior_split_coverage_read_is_bounded_and_observation_safe():
    repository, _, cursor = _repository(EquityCorporateActionRepository)
    context = _watermark()
    cursor.fetchall.return_value = []
    start = context.market_time.date() - timedelta(days=365)
    end = context.market_time.date()

    assert repository.read_behavior_split_coverage(
        ["aapl", "AAPL"], context, window_start=start, window_end=end,
    ) == ((), {})

    sql, parameters = cursor.execute.call_args_list[2].args
    assert "action_type = 'SPLIT'" in sql and "response_action_count" not in sql
    assert "first_observed_at <= %s" in sql and "created_at <= %s" in sql
    assert parameters == (["AAPL"], end, start, context.observed_time, context.observed_time, 1001)
    assert cursor.execute.call_count == 3


@pytest.mark.parametrize("tickers,start_offset,end_offset,coverage_bound,action_bound", [
    ([], 0, 1, 10, 10), (["AAPL"], 1, 0, 10, 10),
    (["AAPL"], 0, 1, 0, 10), (["AAPL"], 0, 1, 10, 0),
])
def test_behavior_split_coverage_read_rejects_invalid_scope(tickers, start_offset, end_offset, coverage_bound, action_bound):
    repository, _, _ = _repository(EquityCorporateActionRepository)
    today = _watermark().market_time.date()
    with pytest.raises(ValueError):
        repository.read_behavior_split_coverage(
            tickers, _watermark(), window_start=today + timedelta(days=start_offset),
            window_end=today + timedelta(days=end_offset),
            maximum_coverage_rows=coverage_bound, maximum_action_rows=action_bound,
        )


def test_legacy_context_mapper_ignores_new_columns_and_writer_keeps_old_insert():
    from equity.repositories import _context_from_row, LEGACY_CONTEXT_COLUMNS

    original = _context()
    row = dict(original.__dict__) if hasattr(original, "__dict__") else {field: getattr(original, field) for field in original.__dataclass_fields__}
    row["status"] = original.status.value
    for key in ("risk_levels", "conflict_state", "stale_components", "summary"):
        import json
        row[key] = json.loads(row.pop(key + "_json"))
    assert _context_from_row({**row, "context_kind": "LEGACY", "behavior_payload": None}) == original
    assert len(LEGACY_CONTEXT_COLUMNS) == 40
    repository, _, cursor = _repository(EquityEvidenceRepository)
    repository.persist_context(original, ())
    sql, values = cursor.execute.call_args.args
    assert "behavior_" not in sql and "context_kind" not in sql
    assert len(values) == 39


def test_robust_qualification_lookup_is_effective_at_observation_time():
    repository, _, cursor = _repository(EquityEvidenceRepository)
    context = _watermark()

    assert repository.robust_qualification_ids_as_of(context) == frozenset()

    sql, parameters = cursor.execute.call_args.args
    assert "effective_from <= %s" in sql
    assert "effective_to IS NULL OR effective_to > %s" in sql
    assert "metrics->>'research_scope'" in sql
    assert "EQUITY_SIGNAL" in sql
    assert parameters == (context.observed_time, context.observed_time)


def test_robust_qualification_mapping_is_scoped_to_horizon_and_default_policy():
    repository, _, cursor = _repository(EquityEvidenceRepository)
    context = _watermark()

    assert repository.robust_qualifications_as_of(
        context, interval="30m", horizon_key="60m"
    ) == {}

    sql, parameters = cursor.execute.call_args.args
    assert "horizon_key = %s" in sql
    assert "outcome_policy_key =" in sql
    assert "':SIGNED:SECTOR_PRIMARY'" in sql
    assert "metrics->>'research_scope'" in sql
    assert "EQUITY_SIGNAL" in sql
    assert parameters[-2:] == ("30m", "60m")


def test_qualification_persistence_writes_shared_report_identity():
    repository, _, _ = _repository(EquityOutcomeRepository)
    revision = MagicMock(
        qualification_revision_id=uuid4(),
        source_name="breakout_expansion",
        source_version="1.0",
        interval="30m",
        direction=1,
        horizon_key="60m",
        outcome_policy_key="breakout_expansion:1.0:30m:SIGNED",
        evaluation_version="equity_qualification_v2",
        qualification_state="UNRANKED",
        effective_from=_watermark().observed_time,
        sample_size=100,
        independent_periods=40,
        mean_net_alpha=0.001,
        alpha_t_stat=1.0,
        alpha_fdr_q=0.5,
        calibrated_probability=None,
        probability_ci_low=None,
        probability_ci_high=None,
        brier_score=None,
        brier_skill_score=None,
        expected_calibration_error=None,
        report_identity=HASH,
        metrics_json='{"research_scope":"EQUITY_SIGNAL"}',
    )

    with patch(
        "equity.repositories.execute_values",
        return_value=[{"qualification_revision_id": revision.qualification_revision_id}],
    ) as execute_values:
        assert repository.persist_qualification_revisions((revision,)) == 1

    sql = " ".join(execute_values.call_args.args[1].split())
    values = execute_values.call_args.args[2][0]
    assert "report_identity, metrics" in sql
    assert values[-2] == HASH


def test_qualified_direction_requires_qualification_and_evidence_ids():
    with pytest.raises(ValueError, match="qualified direction requires"):
        _context(qualified_direction="BULLISH")

    context = _context(
        status=ContextStatus.COMPLETE,
        qualified_direction="BULLISH",
        direction_qualification_id=uuid4(),
        direction_evidence_id=uuid4(),
    )
    assert context.qualified_direction == "BULLISH"


def test_every_decision_facing_read_requires_context_parameter():
    methods = (
        EquityReferenceRepository.get_security_as_of,
        EquityReferenceRepository.list_securities_as_of,
        EquityReferenceRepository.list_fundamentals_as_of,
        EquityBarRepository.list_final_as_of,
        EquityEvidenceRepository.list_as_of,
        EquityEvidenceRepository.get_context_as_of,
    )

    assert all(
        inspect.signature(method).parameters["context"].default
        is inspect.Parameter.empty
        for method in methods
    )


def test_analysis_publication_fails_closed_on_unresolved_members():
    repository, _, cursor = _repository(EquityAnalysisRepository)
    cursor.fetchone.return_value = {"status": "FAILED"}

    result = repository.publish_run(uuid4())

    sql = cursor.execute.call_args.args[0]
    assert "status IN ('PENDING', 'CLAIMED')" in sql
    assert "counts.unresolved > 0" in sql
    assert "counts.unresolved = 0" in sql
    assert "counts.usable::double precision / counts.total < 0.90" in sql
    assert "counts.usable::double precision / counts.total < 0.95" in sql
    assert "counts.usable::double precision / counts.total >= 0.90" in sql
    assert result["status"] == "FAILED"


def test_analysis_publication_commits_projection_batch_with_run():
    repository, connection, cursor = _repository(EquityAnalysisRepository)
    run_id = uuid4()
    cursor.fetchone.return_value = {
        "analysis_run_id": run_id,
        "interval": "30m",
        "run_purpose": "ORIGINAL",
        "status": "COMPLETE",
        "published_at": _watermark().observed_time,
    }
    projection = {
        "ticker": "AAPL",
        "interval_key": "30m",
        "projection_type": "FEATURE_SNAPSHOT",
        "source_name": "FEATURES",
        "evidence_id": uuid4(),
        "equity_context_snapshot_id": None,
        "market_time": _watermark().market_time,
        "observed_at": _watermark().observed_time,
        "payload": {"ema_direction": "BULLISH"},
    }

    result = repository.publish_run(
        run_id, output_sha256=HASH, projections=(projection,)
    )

    assert result["status"] == "COMPLETE"
    publication_parameters = cursor.execute.call_args_list[0].args[1]
    assert publication_parameters == (run_id, HASH, run_id)
    delete_sql, delete_parameters = cursor.execute.call_args_list[1].args
    assert "DELETE FROM equity_current_projection" in delete_sql
    assert "member.analysis_run_id = %s" in delete_sql
    assert delete_parameters == ("30m", run_id)
    cursor.executemany.assert_called_once()
    projection_sql, values = cursor.executemany.call_args.args
    assert "INSERT INTO equity_current_projection" in projection_sql
    assert "analysis_run_id = EXCLUDED.analysis_run_id" in projection_sql
    assert values[0][0:4] == ("AAPL", "30m", "FEATURE_SNAPSHOT", "FEATURES")
    assert values[0][6] == run_id
    connection.commit.assert_called_once()


def test_original_no_match_publication_clears_stale_projection_without_insert():
    repository, _, cursor = _repository(EquityAnalysisRepository)
    run_id = uuid4()
    cursor.fetchone.return_value = {
        "analysis_run_id": run_id,
        "interval": "30m",
        "run_purpose": "ORIGINAL",
        "status": "COMPLETE",
        "published_at": _watermark().observed_time,
    }

    repository.publish_run(run_id, output_sha256=HASH)

    assert "DELETE FROM equity_current_projection" in cursor.execute.call_args_list[1].args[0]
    cursor.executemany.assert_not_called()


def test_stale_analysis_recovery_terminal_fails_unresolved_work():
    repository, _, cursor = _repository(EquityAnalysisRepository)
    run_id = uuid4()
    cursor.fetchall.side_effect = [
        [{"analysis_run_id": run_id}],
        [{"analysis_run_id": run_id, "business_key": "30m:test", "failed_members": 2}],
    ]

    result = repository.fail_stale_runs(stale_after=timedelta(minutes=30))

    select_sql, select_parameters = cursor.execute.call_args_list[0].args
    member_sql, member_parameters = cursor.execute.call_args_list[1].args
    run_sql, run_parameters = cursor.execute.call_args_list[2].args
    assert "FOR UPDATE SKIP LOCKED" in select_sql
    assert "member.status IN ('PENDING', 'CLAIMED')" in member_sql
    assert "lease_expires_at = NULL" in member_sql
    assert "run.analysis_run_id = ANY(%s)" in run_sql
    assert "status = 'FAILED'" in run_sql
    assert select_parameters == (timedelta(minutes=30),)
    assert member_parameters == (
        "ANALYSIS_RUN_LEASE_EXPIRED", [run_id],
    )
    assert run_parameters == ([run_id],)
    assert result[0]["analysis_run_id"] == run_id


def test_stale_analysis_recovery_rejects_negative_age():
    repository, _, _ = _repository(EquityAnalysisRepository)

    with pytest.raises(ValueError, match="stale_after must not be negative"):
        repository.fail_stale_runs(stale_after=timedelta(seconds=-1))


def test_lease_expired_analysis_run_can_be_reopened():
    repository, _, cursor = _repository(EquityAnalysisRepository)
    run_id = uuid4()
    cursor.fetchone.return_value = {
        "status": "FAILED",
        "expired": 97,
        "ineligible": 0,
    }
    cursor.rowcount = 1

    assert repository.restart_lease_expired_run(run_id) is True

    select_sql, select_parameters = cursor.execute.call_args_list[0].args
    member_sql, member_parameters = cursor.execute.call_args_list[1].args
    run_sql, run_parameters = cursor.execute.call_args_list[2].args
    assert "WITH locked_run AS" in select_sql
    assert "FOR UPDATE" in select_sql
    assert "failure_reason = %s" in select_sql
    assert "SET status = 'PENDING'" in member_sql
    assert "failure_reason = NULL" in member_sql
    assert "SET completed_members = 0" in run_sql
    assert "status = 'RUNNING'" in run_sql
    assert select_parameters == (
        run_id,
        "ANALYSIS_RUN_LEASE_EXPIRED",
        "ANALYSIS_RUN_LEASE_EXPIRED",
    )
    assert member_parameters == (run_id,)
    assert run_parameters == (run_id,)


@pytest.mark.parametrize(
    "row",
    [
        {"status": "COMPLETE", "expired": 0, "ineligible": 0},
        {"status": "FAILED", "expired": 0, "ineligible": 1},
        {"status": "FAILED", "expired": 1, "ineligible": 1},
    ],
)
def test_analysis_run_reopen_rejects_non_lease_failures(row):
    repository, _, cursor = _repository(EquityAnalysisRepository)
    cursor.fetchone.return_value = row

    assert repository.restart_lease_expired_run(uuid4()) is False
    assert cursor.execute.call_count == 1


def test_stale_ingestion_recovery_terminal_fails_writing_segments():
    repository, _, cursor = _repository(EquityIngestionRepository)
    segment_id = uuid4()
    cursor.fetchall.return_value = [{
        "ingestion_segment_id": segment_id,
        "dataset": "EQUITY_BARS",
        "interval": "30m",
    }]

    result = repository.fail_stale_segments(stale_after=timedelta(minutes=30))

    sql, parameters = cursor.execute.call_args.args
    assert "status = 'WRITING'" in sql
    assert "FOR UPDATE SKIP LOCKED" in sql
    assert "status = 'FAILED'" in sql
    assert "jsonb_build_object" in sql
    assert parameters == (timedelta(minutes=30), "INGESTION_SEGMENT_STALE")
    assert result[0]["ingestion_segment_id"] == segment_id


def test_stale_ingestion_recovery_rejects_negative_age():
    repository, _, _ = _repository(EquityIngestionRepository)

    with pytest.raises(ValueError, match="stale_after must not be negative"):
        repository.fail_stale_segments(stale_after=timedelta(seconds=-1))


def test_existing_analysis_run_is_returned_without_member_mutation():
    repository, _, cursor = _repository(EquityAnalysisRepository)
    run_id = uuid4()
    cursor.fetchone.side_effect = [None, {
        "analysis_run_id": run_id,
        "business_key": "existing",
        "status": "COMPLETE",
        "completed_members": 386,
    }]

    result = repository.start_run(
        analysis_run_id=run_id,
        business_key="existing",
        run_purpose="ORIGINAL",
        interval="1h",
        market_time=_watermark().market_time,
        observed_at=_watermark().observed_time,
        universe_run_id=uuid4(),
        model_bundle_version="equity_materialization_v15",
        model_bundle_sha256=HASH,
        input_sha256=HASH,
        members=(),
    )

    assert result["was_created"] is False
    assert result["status"] == "COMPLETE"
    assert cursor.execute.call_count == 2


def test_analysis_repository_loads_latest_published_market_times():
    repository, _, cursor = _repository(EquityAnalysisRepository)
    cursor.fetchall.return_value = [
        {"interval": "5m", "market_time": _watermark().market_time},
        {"interval": "1d", "market_time": _watermark().market_time},
    ]

    result = repository.latest_published_market_times(("5m", "1d", "5m"))

    sql, parameters = cursor.execute.call_args.args
    assert "DISTINCT ON (interval)" in sql
    assert "published_at IS NOT NULL" in sql
    assert "status IN ('COMPLETE', 'DEGRADED')" in sql
    assert parameters == (["5m", "1d"], "ORIGINAL")
    assert result == {
        "5m": _watermark().market_time,
        "1d": _watermark().market_time,
    }


def test_latest_common_market_time_requires_coverage_and_observation_visibility():
    repository, _, cursor = _repository(EquityBarRepository)
    cursor.fetchone.return_value = {"bar_end": _watermark().market_time}

    result = repository.latest_common_market_time(
        ["AAPL", "MSFT"], "30m",
        observed_by=_watermark().observed_time,
    )

    sql, parameters = cursor.execute.call_args.args
    assert "COALESCE(replay_available_at, system_observed_at) <= %s" in sql
    assert "COUNT(DISTINCT ticker) >= CEIL(%s * %s)" in sql
    assert parameters[-2:] == (2, 0.90)
    assert result == _watermark().market_time


def test_pending_reconciliation_excludes_stream_bars_already_linked():
    repository, _, cursor = _repository(EquityBarRepository)
    cursor.fetchall.return_value = []

    result = repository.list_pending_reconciliation(
        "30m", available_by=_watermark().observed_time, limit=100
    )

    sql, parameters = cursor.execute.call_args.args
    assert "stream.source_kind = 'REALTIME_STREAM'" in sql
    assert "DISTINCT ON (stream.ticker, stream.interval, stream.bar_start)" in sql
    assert "stream.reconciliation_status = 'PENDING'" in sql
    assert "reconciliation.source_kind = 'RECONCILED'" in sql
    assert "stream.bar_revision_id = ANY" in sql
    assert parameters == ("30m", _watermark().observed_time, 100)
    assert result == ()