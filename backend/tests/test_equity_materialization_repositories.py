import inspect
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch
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
    assert parameters == (universe_run_id, ["AAPL", "MSFT"], ["AAPL", "MSFT"])


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

    assert repository.qualification_observations(
        available_by=_watermark().observed_time,
        interval="1d",
        source_names=sources,
        subject_evidence_ids=subject_ids,
        outcome_policy_keys=policy_keys,
    ) == []

    sql, parameters = cursor.execute.call_args.args
    assert "DISTINCT ON" in sql
    assert "outcome.outcome_policy_id" in sql
    assert "policy.created_at DESC" in sql
    assert "outcome.outcome_revision = 1" not in sql
    assert "outcome.outcome_revision DESC" in sql
    assert "outcome.outcome_id" in sql
    assert "outcome.subject_evidence_id" in sql
    assert "outcome.sector_net_alpha" in sql
    assert "AS primary_benchmark" in sql
    assert "AS has_bracket" in sql
    assert "outcome.subject_evidence_id = ANY(%s::UUID[])" in sql
    assert "evidence.source_name = ANY(%s::TEXT[])" in sql
    assert "policy.policy_key = ANY(%s::TEXT[])" in sql
    assert parameters[3:5] == (list(subject_ids), list(subject_ids))
    assert parameters[5:7] == (list(sources), list(sources))
    assert parameters[7:9] == (list(policy_keys), list(policy_keys))


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


def test_context_read_enforces_market_observation_and_validity_bounds():
    repository, _, cursor = _repository(EquityEvidenceRepository)
    context = _watermark()

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


def test_stale_analysis_recovery_rejects_nonpositive_age():
    repository, _, _ = _repository(EquityAnalysisRepository)

    with pytest.raises(ValueError, match="stale_after must be positive"):
        repository.fail_stale_runs(stale_after=timedelta(0))


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


def test_stale_ingestion_recovery_rejects_nonpositive_age():
    repository, _, _ = _repository(EquityIngestionRepository)

    with pytest.raises(ValueError, match="stale_after must be positive"):
        repository.fail_stale_segments(stale_after=timedelta(0))


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