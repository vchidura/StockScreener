from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from realtime.contracts import CANARY_UNIVERSE
from realtime.reference_facts import ReferenceFact, ReferenceFactRelation
from realtime.reference_repository import persist_reference_result, persist_reference_seed
from realtime.reference_repository import persist_reference_facts_cursor, load_dividend_reference_cursor
from realtime.reference_seed import (
    ReferenceKind,
    ReferenceSeedMember,
    SeedMemberStatus,
    SeedStatus,
    build_reference_seed_manifest,
)
from realtime.reference_service import ReferenceServiceResult


UTC = timezone.utc
NOW = datetime(2026, 10, 5, 12, 0, tzinfo=UTC)
ETFS = {"SPY", "QQQ", "IWM"}


def _member(kind, key, status=SeedMemberStatus.READY):
    reasons = () if status is SeedMemberStatus.READY else (
        "VERIFIED_ETF" if status is SeedMemberStatus.NOT_APPLICABLE else "SOURCE_UNAVAILABLE",
    )
    return ReferenceSeedMember(
        kind,
        key,
        f"fixture:{kind.value}:{key}",
        "a" * 64,
        status,
        NOW,
        NOW + timedelta(seconds=1),
        reasons,
    )


def _members():
    rows = []
    for ticker in CANARY_UNIVERSE.equity_tickers:
        rows.extend(
            (
                _member(ReferenceKind.SECURITY, ticker),
                _member(ReferenceKind.FUNDAMENTALS, ticker),
                _member(ReferenceKind.CORPORATE_ACTION_COVERAGE, ticker),
                _member(
                    ReferenceKind.EARNINGS_COVERAGE,
                    ticker,
                    SeedMemberStatus.NOT_APPLICABLE if ticker in ETFS else SeedMemberStatus.READY,
                ),
                _member(ReferenceKind.OPTION_CATALOG, ticker),
                _member(ReferenceKind.OI_COVERAGE, ticker),
            )
        )
    rows.extend(
        (
            _member(ReferenceKind.FOMC_COVERAGE, "GLOBAL"),
            _member(ReferenceKind.RATE_CURVE, "USD"),
        )
    )
    return tuple(rows)


def _manifest(members=None):
    return build_reference_seed_manifest(
        CANARY_UNIVERSE,
        session_date=date(2026, 10, 5),
        members=members or _members(),
        source_generations=(
            ("federal_reserve", "e" * 64),
            ("finnhub", "b" * 64),
            ("massive", "c" * 64),
            ("treasury", "d" * 64),
        ),
        created_at=NOW,
        effective_at=NOW + timedelta(minutes=1),
        valid_until=NOW + timedelta(hours=6),
    )


def test_complete_reference_seed_is_deterministic_and_ready():
    first = _manifest()
    repeated = _manifest(tuple(reversed(_members())))

    assert first.status is SeedStatus.READY
    assert first.reason_codes == ()
    assert len(first.members) == 50
    assert first.seed_sha256 == repeated.seed_sha256
    assert first.seed_id == repeated.seed_id


def test_missing_or_unavailable_reference_evidence_fails_closed():
    missing = tuple(
        row
        for row in _members()
        if not (row.kind is ReferenceKind.EARNINGS_COVERAGE and row.key == "AAPL")
    )
    unavailable = tuple(
        _member(row.kind, row.key, SeedMemberStatus.UNAVAILABLE)
        if row.kind is ReferenceKind.OI_COVERAGE and row.key == "NVDA"
        else row
        for row in _members()
    )

    missing_manifest = _manifest(missing)
    unavailable_manifest = _manifest(unavailable)

    assert missing_manifest.status is SeedStatus.UNAVAILABLE
    assert "MISSING_EARNINGS_COVERAGE:AAPL" in missing_manifest.reason_codes
    assert unavailable_manifest.status is SeedStatus.UNAVAILABLE
    assert "UNAVAILABLE_OI_COVERAGE:NVDA" in unavailable_manifest.reason_codes


def _connection(*rows):
    cursor = MagicMock()
    cursor.fetchone.side_effect = rows
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    return connection, cursor


def test_ready_seed_persists_members_and_promotes_atomically():
    manifest = _manifest()
    connection, cursor = _connection((manifest.seed_sha256,), None)

    persist_reference_seed(connection, manifest, generation=1)

    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert statements[0].startswith("INSERT INTO rt.seed_manifests")
    assert sum("INSERT INTO rt.seed_manifest_members" in row for row in statements) == len(manifest.members)
    assert any("INSERT INTO rt.seed_current" in row for row in statements)
    assert connection.commit.call_count == 1


def test_equity_validation_seed_uses_isolated_current_scope():
    manifest = _manifest()
    connection, cursor = _connection((manifest.seed_sha256,), None)

    persist_reference_seed(
        connection,
        manifest,
        generation=1,
        scope="EQUITY_VALIDATION_REFERENCES",
    )

    current_calls = [
        call for call in cursor.execute.call_args_list
        if "seed_current" in call.args[0]
    ]
    assert current_calls[0].args[1] == ("EQUITY_VALIDATION_REFERENCES",)
    assert current_calls[1].args[1][0] == "EQUITY_VALIDATION_REFERENCES"
    assert connection.commit.call_count == 1


def test_reference_seed_rejects_unreviewed_scope():
    connection, _ = _connection()

    with pytest.raises(ValueError, match="scope is not reviewed"):
        persist_reference_seed(
            connection,
            _manifest(),
            generation=1,
            scope="OTHER",
        )

    assert connection.cursor.call_count == 0


def test_reference_facts_commit_in_same_transaction_before_promotion():
    manifest = _manifest()
    fact = ReferenceFact.build(
        ReferenceFactRelation.RATE_OBSERVATION,
        ReferenceKind.RATE_CURVE.value,
        "USD",
        observation_id=manifest.seed_id,
        rate_date=manifest.session_date,
        tenor_days=30,
        annual_rate=0.04,
        source="treasury",
        source_key="2026-10-05:30d",
        source_observed_at=NOW,
        received_at=NOW,
        payload_sha256="b" * 64,
    )
    connection, cursor = _connection((manifest.seed_sha256,), None)

    persist_reference_seed(connection, manifest, generation=1, facts=(fact,))

    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert statements[0].startswith("INSERT INTO ref.risk_free_rate_observations")
    assert statements.index(next(row for row in statements if "rt.seed_current" in row)) > 0
    assert connection.commit.call_count == 1


def test_reference_fact_failure_rolls_back_without_manifest_commit():
    manifest = _manifest()
    fact = ReferenceFact.build(
        ReferenceFactRelation.RATE_OBSERVATION,
        ReferenceKind.RATE_CURVE.value,
        "USD",
        observation_id=manifest.seed_id,
        rate_date=manifest.session_date,
        tenor_days=30,
        annual_rate=0.04,
        source="treasury",
        source_key="2026-10-05:30d",
        source_observed_at=NOW,
        received_at=NOW,
        payload_sha256="b" * 64,
    )
    connection, cursor = _connection()

    def fail_manifest(statement, _parameters):
        if statement.startswith("INSERT INTO rt.seed_manifests"):
            raise RuntimeError("forced manifest failure")

    cursor.execute.side_effect = fail_manifest

    try:
        persist_reference_seed(connection, manifest, generation=1, facts=(fact,))
    except RuntimeError as error:
        assert str(error) == "forced manifest failure"
    else:
        raise AssertionError("manifest failure was not propagated")
    assert connection.commit.call_count == 0
    assert connection.rollback.call_count == 1


def test_ready_service_result_requires_durable_fact_for_every_member():
    manifest = _manifest()
    result = ReferenceServiceResult(manifest, 0, 0, ())
    connection, _ = _connection()

    with pytest.raises(ValueError, match="missing durable facts"):
        persist_reference_result(connection, result, generation=1)

    assert connection.cursor.call_count == 0


def test_reference_security_identity_conflict_rolls_back():
    manifest = _manifest()
    security_id = uuid4()
    fact = ReferenceFact.build(
        ReferenceFactRelation.SECURITY,
        ReferenceKind.SECURITY.value,
        "AAPL",
        security_id=security_id,
        revision_id=uuid4(),
        ticker="AAPL",
        asset_type="STOCK",
        source_origin="MASSIVE_TICKER_OVERVIEW_V3",
        exchange=None,
        calendar_id="XNYS",
        effective_from=NOW,
        effective_to=NOW + timedelta(hours=6),
        observed_at=NOW,
        payload_sha256="b" * 64,
    )
    connection, _ = _connection(("ETF", "OTHER_SOURCE"))

    with pytest.raises(ValueError, match="security identity conflict"):
        persist_reference_seed(connection, manifest, generation=1, facts=(fact,))

    assert connection.commit.call_count == 0
    assert connection.rollback.call_count == 1


def test_dividend_facts_and_coverage_persist_exact_original_metadata_before_commit():
    from realtime.reference_adapters import _corporate_action_fact, _corporate_action_coverage_fact
    from tests.realtime.test_reference_adapters import _request, _dividend_payload
    payload = _dividend_payload()
    fact = _corporate_action_fact("AAPL", "DIVIDEND", payload, _request())
    coverage = _corporate_action_coverage_fact("AAPL", "DIVIDEND", (payload,), NOW.date(), NOW.date() + timedelta(days=20), _request())
    connection, cursor = _connection(fact.ordered_values(), coverage.ordered_values())
    persist_reference_facts_cursor(cursor, (fact, coverage))
    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert statements[0].startswith("INSERT INTO ref.corporate_action_revisions")
    assert statements[1].startswith("SELECT action_id")
    assert statements[2].startswith("INSERT INTO ref.corporate_action_coverage")
    assert statements[3].startswith("SELECT coverage_id")
    assert dict(fact.values)["raw_payload"] == payload
    assert connection.commit.call_count == 0


def test_dividend_persistence_rejects_conflicting_immutable_metadata():
    from realtime.reference_adapters import _corporate_action_fact
    from tests.realtime.test_reference_adapters import _request, _dividend_payload
    fact = _corporate_action_fact("AAPL", "DIVIDEND", _dividend_payload(), _request())
    stored = dict(fact.values)
    stored["raw_payload"] = dict(stored["raw_payload"], currency="EUR")
    _, cursor = _connection(stored)
    with pytest.raises(ValueError, match="identity/metadata conflict"):
        persist_reference_facts_cursor(cursor, (fact,))


def test_dividend_reader_is_stored_only_exact_receipt_pinned_and_preserves_known_by():
    from realtime.reference_adapters import _corporate_action_fact, _corporate_action_coverage_fact, _security_id
    from tests.realtime.test_reference_adapters import _request, _dividend_payload
    payload = _dividend_payload()
    fact = _corporate_action_fact("AAPL", "DIVIDEND", payload, _request())
    start, end = NOW.date(), NOW.date() + timedelta(days=20)
    coverage = dict(_corporate_action_coverage_fact("AAPL", "DIVIDEND", (payload,), start, end, _request()).values)
    cursor = MagicMock()
    cursor.fetchone.return_value = coverage
    cursor.fetchall.return_value = [dict(fact.values)]
    result = load_dividend_reference_cursor(cursor, security_id=_security_id("AAPL"), ticker="AAPL",
        window_start=start, window_end=end, known_by=NOW, coverage_id=coverage["coverage_id"])
    assert result.status == "AVAILABLE"
    assert result.coverage_id == coverage["coverage_id"]
    assert result.dividends[0].original_cash_amount == Decimal("0.26")
    calls = cursor.execute.call_args_list
    assert all(call.args[0].startswith("SELECT") for call in calls)
    assert calls[0].args[1][-2:] == (str(coverage["coverage_id"]), str(coverage["coverage_id"]))
    assert calls[1].args[1][3:5] == (NOW, NOW)
    cursor.fetchone.return_value = None
    missing = load_dividend_reference_cursor(cursor, security_id=_security_id("AAPL"), ticker="AAPL",
        window_start=start, window_end=end, known_by=NOW, coverage_id=uuid4())
    assert missing.status == "UNAVAILABLE"
    assert missing.dividends == ()


def test_unavailable_seed_is_retained_but_not_promoted():
    members = tuple(
        row
        for row in _members()
        if not (row.kind is ReferenceKind.EARNINGS_COVERAGE and row.key == "AAPL")
    )
    manifest = _manifest(members)
    connection, cursor = _connection((manifest.seed_sha256,))

    persist_reference_seed(connection, manifest, generation=1)

    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert not any("seed_current" in row for row in statements)
    assert connection.commit.call_count == 1


def test_ready_seed_rejects_nonadvancing_generation():
    manifest = _manifest()
    connection, _ = _connection((manifest.seed_sha256,), (2,))

    try:
        persist_reference_seed(connection, manifest, generation=2)
    except ValueError as error:
        assert "must advance" in str(error)
    else:
        raise AssertionError("nonadvancing seed generation was accepted")
    assert connection.rollback.call_count == 1