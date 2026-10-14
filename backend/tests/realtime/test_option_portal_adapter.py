from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID
from unittest.mock import MagicMock

from options.config import load_valuation_policy
from options.domain import (
    ContractType,
    ExerciseStyle,
    MarkSource,
    OptionContractSnapshot,
)
from options.outcomes import review_retained_plan_mark
from options.strategies.domain import OptionSide
from realtime.materialization import MaterializedQuote
from realtime.option_portal_adapter import (
    RealtimeOptionPackage,
    RealtimeOptionPackageLeg,
    RealtimePackageMarkStatus,
    materialize_developer_option_package_mark,
    materialize_realtime_option_package_mark,
    portal_current_quote_row,
)
from realtime.option_portal_repository import (
    RealtimeOptionCurrentQuoteRepository,
    RealtimeOptionPortalMarkRepository,
    persist_realtime_option_package_mark,
    persist_realtime_option_package_mark_cursor,
    select_developer_option_packages_cursor,
)
from realtime.preflight import (
    DetectorFamily,
    ProviderTier,
    detector_availability,
)
from realtime.contracts import CapabilityVerdict


UTC = timezone.utc
NOW = datetime(2026, 10, 5, 14, 30, tzinfo=UTC)
GENERATION = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
SESSION = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
CANDIDATE = UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")
MATRIX = UUID("dddddddd-dddd-dddd-dddd-dddddddddddd")
COHORT = UUID("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee")


def _policy():
    return load_valuation_policy(
        Path(__file__).parents[2] / "options" / "policies" / "valuation_advanced_nbbo_v1.json"
    )


def _developer_policy():
    return load_valuation_policy(
        Path(__file__).parents[2] / "options" / "policies" / "valuation_v1.json"
    )


def _leg(policy_sha256, instrument_id, contract_id, ticker, side, strike, entry_mark):
    return RealtimeOptionPackageLeg(
        leg_index=instrument_id - 1,
        instrument_id=instrument_id,
        contract_id=contract_id,
        catalog_version_id=contract_id + 100,
        contract_ticker=ticker,
        side=side,
        ratio=1,
        multiplier=100,
        strike=Decimal(str(strike)),
        expiration_date=date(2026, 11, 20),
        contract_type=ContractType.CALL,
        entry_mark=Decimal(str(entry_mark)),
        entry_market_time=NOW,
        valuation_policy_sha256=policy_sha256,
    )


def _quote(
    instrument_id,
    ticker,
    bid,
    ask,
    offset,
    milliseconds,
    *,
    generation=GENERATION,
    session=SESSION,
    quality="GOOD",
    reasons=(),
):
    event_at = NOW + timedelta(milliseconds=milliseconds)
    return MaterializedQuote(
        journal_generation=generation,
        stream_session_id=session,
        instrument_id=instrument_id,
        provider_symbol=ticker,
        source_session=date(2026, 10, 5),
        source_sequence=offset,
        source_timestamp=1_000 + milliseconds,
        source_timestamp_unit="ms",
        event_at=event_at,
        received_at=event_at + timedelta(milliseconds=1),
        bid=Decimal(str(bid)),
        ask=Decimal(str(ask)),
        bid_size=10,
        ask_size=11,
        bid_exchange=1,
        ask_exchange=2,
        quality=quality,
        reason_codes=tuple(reasons),
        source_offset=offset,
        payload_sha256=f"{offset:064x}",
    )


def _package(policy_sha256, provider_tier=ProviderTier.MASSIVE_ADVANCED):
    legs = (
        _leg(policy_sha256, 1, 101, "O:SPY261120C00450000", OptionSide.BUY, 450, 5),
        _leg(policy_sha256, 2, 102, "O:SPY261120C00455000", OptionSide.SELL, 455, 2),
    )
    return RealtimeOptionPackage(
        candidate_id=CANDIDATE,
        event_id=None,
        matrix_id=MATRIX,
        candidate_identity_sha256="f" * 64,
        cohort_id=COHORT,
        provider_tier=provider_tier,
        entry_market_time=NOW,
        entry_net_premium=Decimal("-300"),
        capital_at_risk=Decimal("300"),
        exit_deadline=NOW + timedelta(hours=2),
        legs=legs,
    )


def _developer_snapshot(leg, policy, *, batch_id, mark, milliseconds):
    market_time = NOW + timedelta(milliseconds=milliseconds)
    observed_time = market_time + timedelta(milliseconds=1)
    return OptionContractSnapshot(
        snapshot_id=UUID(int=leg.instrument_id + 1000),
        contract_id=leg.contract_id,
        contract_ticker=leg.contract_ticker,
        underlyer="SPY",
        provider="polygon_developer",
        contract_type=leg.contract_type,
        expiration_date=leg.expiration_date,
        expiration_cutoff=datetime(
            leg.expiration_date.year,
            leg.expiration_date.month,
            leg.expiration_date.day,
            20,
            tzinfo=UTC,
        ),
        calendar_dte=(leg.expiration_date - market_time.date()).days,
        time_to_expiration_years=(leg.expiration_date - market_time.date()).days / 365,
        strike=leg.strike,
        shares_per_contract=leg.multiplier,
        exercise_style=ExerciseStyle.AMERICAN,
        spot=Decimal("450"),
        spot_market_data_time=market_time,
        bid=None,
        ask=None,
        midpoint=None,
        display_mark=Decimal(str(mark)),
        model_mark=Decimal(str(mark)),
        mark_market_data_time=market_time,
        mark_source=MarkSource.DEVELOPER_ALIGNED_AGG_CLOSE,
        day_volume=100,
        open_interest=500,
        market_data_time=market_time,
        first_observed_at=observed_time,
        revised_observed_at=None,
        local_iv=0.30,
        local_gamma=0.02,
        local_delta=0.50,
        local_theta_per_day=-0.01,
        local_vega_per_vol_point=0.10,
        local_rho_per_rate_point=0.001,
        intrinsic_value=Decimal("0"),
        extrinsic_value=Decimal(str(mark)),
        single_contract_breakeven=leg.strike + Decimal(str(mark)),
        provider_iv=None,
        provider_gamma=None,
        risk_free_rate=0.04,
        dividend_yield=0.0,
        iv_converged=True,
        iv_solver="NEWTON",
        iv_iteration_count=3,
        iv_price_error=1e-9,
        iv_failure_reason=None,
        model_version="developer-aligned-v1",
        quality_flags=(),
        batch_id=batch_id,
        raw_payload_sha256=f"{leg.instrument_id + 20:064x}",
        normalized_payload_sha256=f"{leg.instrument_id + 30:064x}",
        valuation_policy_version=policy.policy_version,
        valuation_policy_sha256=policy.policy_sha256,
    )


def _plan(package):
    return {
        "candidate_id": str(package.candidate_id),
        "candidate_identity_sha256": package.candidate_identity_sha256,
        "matrix_id": str(package.matrix_id),
        "source_market_time": package.entry_market_time,
        "exit_deadline": package.exit_deadline,
        "original_economics": {
            "net_premium": package.entry_net_premium,
            "capital_at_risk": package.capital_at_risk,
        },
        "legs": [
            {
                "contract_id": leg.contract_id,
                "leg_index": leg.leg_index,
                "contract_ticker": leg.contract_ticker,
                "side": leg.side.value,
                "ratio": leg.ratio,
                "multiplier": leg.multiplier,
                "strike": leg.strike,
                "expiration_date": leg.expiration_date,
                "contract_type": leg.contract_type.value,
                "entry_model_mark": leg.entry_mark,
                "source_market_time": leg.entry_market_time,
                "valuation_policy_sha256": leg.valuation_policy_sha256,
            }
            for leg in package.legs
        ],
    }


def test_realtime_package_mark_preserves_existing_portal_serializer_contract():
    policy = _policy()
    package = _package(policy.policy_sha256)
    quotes = {
        1: _quote(1, package.legs[0].contract_ticker, "6.00", "6.20", 10, 100),
        2: _quote(2, package.legs[1].contract_ticker, "2.40", "2.60", 11, 150),
    }

    result = materialize_realtime_option_package_mark(
        package,
        quotes,
        checked_at=NOW + timedelta(milliseconds=200),
        policy=policy,
    )

    assert result.status is RealtimePackageMarkStatus.READY
    retained = result.mark.retained_row()
    assert retained["availability_flag"] == "REALTIME_ADVANCED_NBBO"
    assert retained["quality_flags"] == [
        "COMMISSION_ONLY_COST_MODEL",
        "ADVANCED_NBBO_MIDPOINT",
        "SLIPPAGE_UNAVAILABLE",
    ]
    portal = review_retained_plan_mark(
        _plan(package),
        retained,
        checked_at=NOW + timedelta(milliseconds=200),
        policy=policy,
    )
    assert portal["status"] == "FRESH"
    assert portal["reason"] is None
    assert portal["signed_package_mark"] == Decimal("-360.00")
    assert portal["package_price"] == Decimal("-3.60")
    assert portal["gross_pnl"] == Decimal("60.00")
    assert portal["estimated_cost"] == Decimal("2.60")
    assert portal["net_pnl"] == Decimal("57.40")
    assert portal["valuation_policy_sha256"] == policy.policy_sha256
    assert portal["execution_permission"] is False
    assert len(portal["source_snapshot_ids"]) == 2
    assert set(portal) == {
        "status", "reason", "market_time", "observed_time",
        "oldest_leg_market_time", "age_seconds", "maximum_age_seconds",
        "signed_package_mark", "package_price", "gross_pnl", "price_return",
        "estimated_cost", "net_pnl", "net_return", "price_return_basis",
        "net_return_basis", "basis", "slippage", "execution_permission",
        "valuation_policy_sha256", "source_snapshot_ids", "after_exit_deadline",
    }


def test_developer_package_uses_fresh_snapshot_data_and_stays_reduced_capability():
    policy = _developer_policy()
    assert policy.policy_sha256 != _policy().policy_sha256
    package = _package(
        policy.policy_sha256,
        ProviderTier.MASSIVE_DEVELOPER,
    )
    batch_id = UUID("12121212-1212-1212-1212-121212121212")
    snapshots = {
        1: _developer_snapshot(package.legs[0], policy, batch_id=batch_id, mark="6.10", milliseconds=100),
        2: _developer_snapshot(package.legs[1], policy, batch_id=batch_id, mark="2.50", milliseconds=150),
    }

    result = materialize_developer_option_package_mark(
        package,
        snapshots,
        checked_at=NOW + timedelta(milliseconds=200),
        policy=policy,
    )

    assert result.status is RealtimePackageMarkStatus.READY
    assert result.mark.outcome.availability_flag == "DEVELOPER_DELAYED_SNAPSHOT"
    assert "INDICATIVE_NO_OPTION_QUOTES" in result.mark.outcome.quality_flags
    retained = result.mark.retained_row()
    assert all(
        leg["exit_mark_source"] == "DEVELOPER_ALIGNED_AGG_CLOSE"
        for leg in retained["legs"]
    )
    portal = review_retained_plan_mark(
        _plan(package),
        retained,
        checked_at=NOW + timedelta(milliseconds=200),
        policy=policy,
    )
    assert portal["status"] == "FRESH"
    assert portal["execution_permission"] is False
    assert detector_availability(
        DetectorFamily.O1,
        ProviderTier.MASSIVE_DEVELOPER,
    ).verdict is CapabilityVerdict.PASS
    assert detector_availability(
        DetectorFamily.O2,
        ProviderTier.MASSIVE_DEVELOPER,
    ).verdict is CapabilityVerdict.UNAVAILABLE
    assert detector_availability(
        DetectorFamily.AGGRESSIVE_PREMIUM_BURST,
        ProviderTier.MASSIVE_DEVELOPER,
    ).verdict is CapabilityVerdict.UNAVAILABLE

    advanced_only = materialize_realtime_option_package_mark(
        package,
        {},
        checked_at=NOW + timedelta(milliseconds=200),
        policy=policy,
    )
    assert advanced_only.reason_codes == ("ADVANCED_PACKAGE_REQUIRES_ADVANCED_COHORT",)


def test_developer_mark_persistence_uses_batch_revision_lineage_without_quotes():
    policy = _developer_policy()
    package = _package(policy.policy_sha256, ProviderTier.MASSIVE_DEVELOPER)
    batch_id = UUID("34343434-3434-3434-3434-343434343434")
    result = materialize_developer_option_package_mark(
        package,
        {
            1: _developer_snapshot(package.legs[0], policy, batch_id=batch_id, mark="6.10", milliseconds=100),
            2: _developer_snapshot(package.legs[1], policy, batch_id=batch_id, mark="2.50", milliseconds=150),
        },
        checked_at=NOW + timedelta(milliseconds=200),
        policy=policy,
    )
    cursor = MagicMock()
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    stored = {"package": None, "legs": {}, "evidence": {}, "revision": None, "input": None}

    def execute(statement, parameters=None):
        if statement.startswith("INSERT INTO rt.option_packages"):
            stored["package"] = parameters
        elif statement.startswith("SELECT cohort_id"):
            cursor.fetchone.return_value = stored["package"][1:]
        elif statement.startswith("INSERT INTO rt.option_package_legs"):
            stored["legs"][parameters[1]] = parameters
        elif statement.startswith("SELECT instrument_id") and "option_package_legs" in statement:
            cursor.fetchone.return_value = stored["legs"][parameters[1]][2:]
        elif statement.startswith("INSERT INTO rt.option_mark_evidence"):
            stored["evidence"][parameters[0]] = parameters
        elif statement.startswith("SELECT instrument_id") and "option_mark_evidence" in statement:
            cursor.fetchone.return_value = stored["evidence"][parameters[0]][1:]
        elif statement.startswith("INSERT INTO rt.option_package_mark_revisions"):
            stored["revision"] = parameters
        elif statement.startswith("SELECT candidate_id, event_id"):
            cursor.fetchone.return_value = stored["revision"][1:]
        elif statement.startswith("INSERT INTO rt.option_package_mark_inputs"):
            stored["input"] = parameters
        elif statement.startswith("SELECT evidence_id"):
            cursor.fetchone.return_value = (stored["input"][2],)
        elif statement.startswith("SELECT mark_id, market_time"):
            cursor.fetchone.return_value = None
        elif statement.startswith("INSERT INTO rt.option_package_mark_current"):
            cursor.fetchone.return_value = (parameters[2],)

    cursor.execute.side_effect = execute

    persist_realtime_option_package_mark_cursor(cursor, result)

    evidence_inserts = [
        call.args[1]
        for call in cursor.execute.call_args_list
        if call.args[0].startswith("INSERT INTO rt.option_mark_evidence")
    ]
    assert len(evidence_inserts) == 2
    assert all(row[4] == "MASSIVE_DEVELOPER" for row in evidence_inserts)
    assert all(row[5] == "DEVELOPER_ALIGNED_AGGREGATE" for row in evidence_inserts)
    assert all(row[7] == str(batch_id) and row[8] is not None for row in evidence_inserts)
    assert all(row[9:12] == (None, None, None) for row in evidence_inserts)
    assert all(row[14:16] == (None, None) for row in evidence_inserts)
    assert connection.commit.call_count == 0
    assert connection.rollback.call_count == 0


def test_realtime_package_mark_fails_closed_for_missing_or_incoherent_quote():
    policy = _policy()
    package = _package(policy.policy_sha256)
    first = _quote(1, package.legs[0].contract_ticker, "6.00", "6.20", 10, 100)

    missing = materialize_realtime_option_package_mark(
        package,
        {1: first},
        checked_at=NOW + timedelta(seconds=1),
        policy=policy,
    )
    assert missing.status is RealtimePackageMarkStatus.UNAVAILABLE
    assert missing.reason_codes == ("PACKAGE_LEG_QUOTE_MISSING:102",)

    late = _quote(2, package.legs[1].contract_ticker, "2.40", "2.60", 11, 900)
    skewed = materialize_realtime_option_package_mark(
        package,
        {1: first, 2: late},
        checked_at=NOW + timedelta(seconds=1),
        policy=policy,
    )
    assert skewed.status is RealtimePackageMarkStatus.UNAVAILABLE
    assert skewed.reason_codes == ("PACKAGE_QUOTE_TIME_SKEW_EXCEEDED",)


def test_developer_option_selector_requires_complete_unmaterialized_packages():
    policy = _developer_policy()
    package = _package(policy.policy_sha256, ProviderTier.MASSIVE_DEVELOPER)
    cursor = MagicMock()
    cursor.fetchall.return_value = tuple(
        (
            package.candidate_id,
            package.event_id,
            package.matrix_id,
            package.candidate_identity_sha256,
            package.cohort_id,
            package.provider_tier.value,
            package.entry_market_time,
            package.entry_net_premium,
            package.capital_at_risk,
            package.exit_deadline,
            len(package.legs),
            leg.leg_index,
            leg.instrument_id,
            leg.contract_id,
            leg.catalog_version_id,
            leg.contract_ticker,
            leg.side.value,
            leg.ratio,
            leg.multiplier,
            leg.strike,
            leg.expiration_date,
            leg.contract_type.value,
            leg.entry_mark,
            leg.entry_market_time,
            leg.valuation_policy_sha256,
        )
        for leg in package.legs
    )
    source_batch_id = UUID("45454545-4545-4545-4545-454545454545")

    selected = select_developer_option_packages_cursor(
        cursor,
        cohort_id=package.cohort_id,
        valuation_policy_sha256=policy.policy_sha256,
        source_batch_id=source_batch_id,
        instrument_ids=tuple(leg.instrument_id for leg in package.legs),
        source_cutoff=NOW + timedelta(seconds=1),
        limit=25,
    )

    statement, parameters = cursor.execute.call_args.args
    assert "NOT (missing.instrument_id=ANY(%s::bigint[]))" in statement
    assert "mark.evidence_batch_id=%s" in statement
    assert "LIMIT %s" in statement
    assert parameters[-2:] == (str(source_batch_id), 25)
    assert selected == (package,)


def test_realtime_package_mark_rejects_bad_quality_crossed_and_mixed_lineage():
    policy = _policy()
    package = _package(policy.policy_sha256)
    first = _quote(1, package.legs[0].contract_ticker, "6.00", "6.20", 10, 100)
    invalid = _quote(
        2,
        package.legs[1].contract_ticker,
        "2.40",
        "2.60",
        11,
        150,
        quality="INVALID",
        reasons=("EMPTY_QUOTE",),
    )
    bad_quality = materialize_realtime_option_package_mark(
        package,
        {1: first, 2: invalid},
        checked_at=NOW + timedelta(milliseconds=200),
        policy=policy,
    )
    assert bad_quality.reason_codes == ("PACKAGE_LEG_QUOTE_INVALID:102",)

    crossed = _quote(2, package.legs[1].contract_ticker, "2.60", "2.40", 11, 150)
    unusable = materialize_realtime_option_package_mark(
        package,
        {1: first, 2: crossed},
        checked_at=NOW + timedelta(milliseconds=200),
        policy=policy,
    )
    assert unusable.reason_codes == ("PACKAGE_LEG_QUOTE_UNUSABLE:102",)

    other_generation = _quote(
        2,
        package.legs[1].contract_ticker,
        "2.40",
        "2.60",
        11,
        150,
        generation=UUID("11111111-1111-1111-1111-111111111111"),
    )
    mixed = materialize_realtime_option_package_mark(
        package,
        {1: first, 2: other_generation},
        checked_at=NOW + timedelta(milliseconds=200),
        policy=policy,
    )
    assert mixed.reason_codes == ("PACKAGE_QUOTES_CROSS_JOURNAL_GENERATION",)


def test_realtime_package_mark_rejects_policy_and_entry_premium_drift():
    policy = _policy()
    package = _package(policy.policy_sha256)
    quotes = {
        1: _quote(1, package.legs[0].contract_ticker, "6.00", "6.20", 10, 100),
        2: _quote(2, package.legs[1].contract_ticker, "2.40", "2.60", 11, 150),
    }
    mismatched_leg = _leg(
        "e" * 64,
        2,
        102,
        "O:SPY261120C00455000",
        OptionSide.SELL,
        455,
        2,
    )
    policy_drift = RealtimeOptionPackage(
        candidate_id=package.candidate_id,
        event_id=None,
        matrix_id=package.matrix_id,
        candidate_identity_sha256=package.candidate_identity_sha256,
        cohort_id=package.cohort_id,
        provider_tier=package.provider_tier,
        entry_market_time=package.entry_market_time,
        entry_net_premium=package.entry_net_premium,
        capital_at_risk=package.capital_at_risk,
        exit_deadline=package.exit_deadline,
        legs=(package.legs[0], mismatched_leg),
    )
    mismatched = materialize_realtime_option_package_mark(
        policy_drift,
        quotes,
        checked_at=NOW + timedelta(milliseconds=200),
        policy=policy,
    )
    assert mismatched.reason_codes == ("PACKAGE_ENTRY_VALUATION_POLICY_MISMATCH",)

    premium_drift = RealtimeOptionPackage(
        candidate_id=package.candidate_id,
        event_id=None,
        matrix_id=package.matrix_id,
        candidate_identity_sha256=package.candidate_identity_sha256,
        cohort_id=package.cohort_id,
        provider_tier=package.provider_tier,
        entry_market_time=package.entry_market_time,
        entry_net_premium=Decimal("-301"),
        capital_at_risk=package.capital_at_risk,
        exit_deadline=package.exit_deadline,
        legs=package.legs,
    )
    premium = materialize_realtime_option_package_mark(
        premium_drift,
        quotes,
        checked_at=NOW + timedelta(milliseconds=200),
        policy=policy,
    )
    assert premium.reason_codes == ("PACKAGE_ENTRY_PREMIUM_MISMATCH",)


def test_stored_advanced_quote_portal_shape_is_unchanged():
    policy = _policy()
    leg = _package(policy.policy_sha256).legs[0]
    quote = _quote(leg.instrument_id, leg.contract_ticker, "6.00", "6.20", 10, 100)

    row = portal_current_quote_row(leg, quote)

    assert set(row) == {
        "contract_id", "contract_ticker", "quote_time", "received_at",
        "bid", "ask", "bid_size", "ask_size", "updated_at",
    }
    assert row["contract_id"] == leg.contract_id
    assert row["quote_time"] == quote.event_at


def test_ready_realtime_mark_persists_immutable_evidence_before_current_pointer():
    policy = _policy()
    package = _package(policy.policy_sha256)
    result = materialize_realtime_option_package_mark(
        package,
        {
            1: _quote(1, package.legs[0].contract_ticker, "6.00", "6.20", 10, 100),
            2: _quote(2, package.legs[1].contract_ticker, "2.40", "2.60", 11, 150),
        },
        checked_at=NOW + timedelta(milliseconds=200),
        policy=policy,
    )
    mark = result.mark
    cursor = MagicMock()
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    packages = {
        "SELECT cohort_id": (
            package.cohort_id, package.event_id, package.matrix_id,
            package.candidate_identity_sha256, package.provider_tier.value,
            package.entry_market_time,
            package.entry_net_premium, package.capital_at_risk, package.exit_deadline,
            policy.policy_sha256, len(package.legs),
        ),
    }
    leg_rows = {
        leg.leg_index: (
            leg.instrument_id, leg.contract_id, leg.catalog_version_id,
            leg.contract_ticker, leg.side.value, leg.ratio, leg.multiplier,
            leg.strike, leg.expiration_date, leg.contract_type.value,
            leg.entry_mark, leg.entry_market_time, leg.valuation_policy_sha256,
        )
        for leg in package.legs
    }
    evidence_rows = {
        str(row.evidence_id): (
            row.instrument_id, row.contract_id, row.catalog_version_id,
            package.provider_tier.value, "ADVANCED_NBBO",
            row.mark_source.value, None, None,
            row.journal_generation, row.stream_session_id, row.source_offset,
            row.market_time, row.observed_time, row.bid, row.ask, row.mark,
            row.payload_sha256,
        )
        for row in mark.mark_evidence
    }
    state = {"current_mark_id": None, "input_evidence_id": None}
    revision_row = (
        package.candidate_id,
        package.event_id,
        package.provider_tier.value,
        mark.outcome.valuation_policy_version,
        mark.outcome.valuation_policy_sha256,
        mark.adapter_version,
        mark.computed_at,
        mark.outcome.market_time,
        mark.outcome.observed_time,
        mark.outcome.entry_net_premium,
        mark.outcome.exit_net_premium,
        mark.outcome.gross_pnl,
        mark.outcome.estimated_cost,
        mark.outcome.net_pnl,
        mark.outcome.capital_at_risk,
        mark.outcome.net_return,
        mark.outcome.availability_flag,
        list(mark.outcome.quality_flags),
        mark.evidence_batch_id,
        len(mark.mark_evidence),
        None,
    )

    def execute(statement, parameters=None):
        if statement.startswith("SELECT cohort_id"):
            cursor.fetchone.return_value = packages["SELECT cohort_id"]
        elif statement.startswith("SELECT instrument_id") and "option_package_legs" in statement:
            cursor.fetchone.return_value = leg_rows[parameters[1]]
        elif statement.startswith("SELECT instrument_id") and "option_mark_evidence" in statement:
            cursor.fetchone.return_value = evidence_rows[parameters[0]]
        elif statement.startswith("SELECT candidate_id, event_id"):
            cursor.fetchone.return_value = revision_row
        elif statement.startswith("INSERT INTO rt.option_package_mark_inputs"):
            state["input_evidence_id"] = parameters[2]
        elif statement.startswith("SELECT evidence_id"):
            cursor.fetchone.return_value = (state["input_evidence_id"],)
        elif statement.startswith("SELECT mark_id, market_time"):
            cursor.fetchone.return_value = None
        elif statement.startswith("INSERT INTO rt.option_package_mark_current"):
            state["current_mark_id"] = parameters[2]
            cursor.fetchone.return_value = (parameters[2],)

    cursor.execute.side_effect = execute

    mark_id = persist_realtime_option_package_mark(connection, result)

    statements = [call.args[0] for call in cursor.execute.call_args_list]
    evidence_index = next(index for index, row in enumerate(statements) if row.startswith("INSERT INTO rt.option_mark_evidence"))
    revision_index = next(index for index, row in enumerate(statements) if row.startswith("INSERT INTO rt.option_package_mark_revisions"))
    current_index = next(index for index, row in enumerate(statements) if row.startswith("INSERT INTO rt.option_package_mark_current"))
    assert evidence_index < revision_index < current_index
    assert state["current_mark_id"] == str(mark_id)
    assert connection.commit.call_count == 1


def test_realtime_mark_reader_filters_exact_cohort_and_tier():
    from contextlib import nullcontext
    from unittest.mock import MagicMock

    cursor = MagicMock()
    cursor.fetchone.return_value = {"ready": True}
    cursor.fetchall.return_value = []
    cohort = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    candidate = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
    repository = RealtimeOptionPortalMarkRepository(lambda: nullcontext(cursor), transaction_configured=True)
    result = repository.retained_plan_marks((candidate,), available_by=NOW, valuation_policy_sha256="a" * 64,
        cohort_id=cohort, provider_tier=ProviderTier.MASSIVE_DEVELOPER)
    assert result["ready"] is True and result["rows"] == {}
    query, arguments = cursor.execute.call_args.args
    assert "package.cohort_id=%s::uuid" in query and "package.provider_tier=%s::text" in query
    assert arguments[2:6] == (str(cohort), str(cohort), "MASSIVE_DEVELOPER", "MASSIVE_DEVELOPER")
    assert all(not call.args[0].startswith("SET") for call in cursor.execute.call_args_list)


def test_realtime_mark_reader_matches_legacy_repository_method_shape():
    candidate_id = CANDIDATE
    row = {
        "candidate_id": candidate_id,
        "valuation_policy_sha256": "a" * 64,
        "legs": [],
    }
    cursor = MagicMock()
    cursor.fetchone.return_value = {"ready": True}
    cursor.fetchall.return_value = (row,)
    context = MagicMock()
    context.__enter__.return_value = cursor
    repository = RealtimeOptionPortalMarkRepository(lambda: context)

    result = repository.retained_plan_marks(
        [candidate_id],
        available_by=NOW,
        valuation_policy_sha256="a" * 64,
    )

    assert result == {
        "ready": True,
        "management_status_ready": False,
        "rows": {str(candidate_id): row},
    }
    sql = cursor.execute.call_args_list[-1].args[0]
    assert "option_package_mark_current" in sql
    assert "evidence.mark_source" in sql
    assert "evidence.instrument_id=leg.instrument_id" in sql


def test_realtime_current_quote_reader_preserves_existing_endpoint_envelope():
    quote_time = NOW + timedelta(milliseconds=100)
    observed_time = quote_time + timedelta(milliseconds=1)
    rows = (
        {
            "contract_id": 101,
            "contract_ticker": "O:SPY261120C00450000",
            "quote_time": quote_time,
            "received_at": observed_time,
            "bid": Decimal("6.00"),
            "ask": Decimal("6.20"),
            "bid_size": 10,
            "ask_size": 11,
            "updated_at": observed_time,
        },
    )
    cursor = MagicMock()
    cursor.fetchone.return_value = ("MASSIVE_ADVANCED",)
    cursor.fetchall.return_value = rows
    context = MagicMock()
    context.__enter__.return_value = cursor
    repository = RealtimeOptionCurrentQuoteRepository(lambda: context)

    result = repository.current_quotes(
        (101, 102),
        available_by=NOW + timedelta(seconds=1),
    )

    assert result["available"] is True
    assert result["as_of"] == quote_time
    assert result["observed_at"] == observed_time
    assert result["data"]["quotes"] == list(rows)
    assert result["data"]["missing_contract_ids"] == [102]
    assert result["data"]["source"] == "PERSISTED_ADVANCED_QUOTE_CURRENT"
    assert result["data"]["provider_fetch_performed"] is False
    assert result["data"]["execution_permission"] is False
    sql = cursor.execute.call_args_list[-1].args[0]
    assert "quote.quality='GOOD'" in sql


def test_developer_active_cohort_never_reads_stale_advanced_quotes():
    cursor = MagicMock()
    cursor.fetchone.return_value = ("MASSIVE_DEVELOPER",)
    context = MagicMock()
    context.__enter__.return_value = cursor
    repository = RealtimeOptionCurrentQuoteRepository(lambda: context)

    result = repository.current_quotes(
        (101,),
        available_by=NOW + timedelta(seconds=1),
    )

    assert result["available"] is False
    assert result["reason"] == "DEVELOPER_OPTION_QUOTES_UNAVAILABLE"
    assert result["data"]["quotes"] == []
    assert not any(
        "FROM rt.quote_current" in call.args[0]
        for call in cursor.execute.call_args_list
    )