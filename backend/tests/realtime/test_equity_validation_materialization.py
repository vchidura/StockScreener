from datetime import timedelta
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from equity.domain import SecurityReferenceRevision
from equity.materialization import FEATURE_VERSION
from realtime.contracts import EQUITY_VALIDATION_UNIVERSE, canonical_sha256
from realtime.equity_validation_history import (
    SplitAction,
    SplitCoverage,
    build_equity_validation_history_package,
)
from realtime.equity_validation_materialization import (
    equity_validation_feature_definition,
    produce_equity_validation_values,
)
from tests.realtime.test_equity_validation_history import OBSERVED_AT, _facts


BACKEND_DIR = Path(__file__).resolve().parents[2]


def _security(ticker):
    security_id = uuid5(NAMESPACE_URL, f"equity-validation-security:{ticker}")
    return SecurityReferenceRevision(
        security_revision_id=uuid5(NAMESPACE_URL, f"equity-validation-revision:{ticker}"),
        security_id=security_id,
        ticker=ticker,
        active=True,
        company_name=ticker,
        security_type="ETF" if ticker in {"SPY", "QQQ", "IWM"} else "CS",
        cik=None,
        composite_figi=None,
        share_class_figi=None,
        primary_exchange="XNAS",
        sic_code=None,
        sic_description=None,
        sector=None,
        industry=None,
        list_date=None,
        delisted_date=None,
        weighted_shares=None,
        free_float=None,
        free_float_percent=None,
        market_cap=None,
        source="EQUITY_VALIDATION_TEST",
        effective_from=OBSERVED_AT - timedelta(days=500),
        observed_at=OBSERVED_AT,
        payload_sha256=canonical_sha256(ticker),
        raw_payload_json="{}",
    )


def test_equity_validation_materializes_nine_ticker_pages_without_option_claims():
    facts, daily_start, daily_end = _facts()
    history = build_equity_validation_history_package(
        facts,
        split_coverage={
            ticker: SplitCoverage(ticker, daily_start, daily_end, OBSERVED_AT)
            for ticker in EQUITY_VALIDATION_UNIVERSE.equity_tickers
        },
        split_actions=(SplitAction("NFLX", daily_start + timedelta(days=30), "1", "10"),),
    )
    values = produce_equity_validation_values(
        cohort_id=uuid5(NAMESPACE_URL, "equity-validation-materialization-cohort"),
        source_manifest_sha256="a" * 64,
        source_generation=1,
        checked_at=OBSERVED_AT,
        history=history,
        securities={
            ticker: _security(ticker)
            for ticker in EQUITY_VALIDATION_UNIVERSE.equity_tickers
        },
    )

    assert len(values.features) == 27
    assert {
        feature.interval for feature in values.features
    } == {"30m", "1h", "1d"}
    assert len(values.screening.rows) == 9
    assert len(values.portal_snapshots) == 10
    ticker_pages = tuple(
        row for row in values.portal_snapshots if row.snapshot_type == "TICKER_PAGE"
    )
    assert {row.snapshot_key for row in ticker_pages} == set(
        EQUITY_VALIDATION_UNIVERSE.equity_tickers
    )
    assert all(row.payload["split_safe"] is True for row in ticker_pages)
    assert all(
        set(row.payload["technical_intervals"]) == {"30m", "1h", "1d"}
        and row.payload["technicals"] == row.payload["technical_intervals"]["30m"]
        for row in ticker_pages
    )
    assert all(
        row.payload["detectors"]["O1"]["status"] == "UNAVAILABLE"
        for row in ticker_pages
    )

    thirty_definition = equity_validation_feature_definition(
        BACKEND_DIR,
        history_policy_sha256=history.policy_sha256,
        interval="30m",
    )
    daily_definition = equity_validation_feature_definition(
        BACKEND_DIR,
        history_policy_sha256=history.policy_sha256,
        interval="1d",
    )
    assert thirty_definition.dependency_spec["option_underlyings"] == ()
    assert thirty_definition.dependency_spec["primary_price_basis"] == "RAW_ACTION_GATED"
    assert daily_definition.dependency_spec["primary_price_basis"] == "PROVIDER_SPLIT_ADJUSTED"
    assert thirty_definition.bundle_version == FEATURE_VERSION
    assert {
        evidence.source_version
        for feature in values.features
        for evidence in feature.materialization.evidence
        if evidence.evidence_type.value == "FEATURE_SNAPSHOT"
    } == {FEATURE_VERSION}


def test_equity_validation_persistence_publishes_a_release_only_with_a_receipt(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    from realtime import equity_validation_materialization as module
    from realtime.contracts import canonical_sha256 as digest
    from realtime.portal_projection_repository import PortalReleaseReceipt
    from realtime.preflight import ProviderTier

    snapshots = (SimpleNamespace(snapshot_id=uuid5(NAMESPACE_URL, "a")),
                 SimpleNamespace(snapshot_id=uuid5(NAMESPACE_URL, "b")))
    values = SimpleNamespace(features=(), screening=SimpleNamespace(generation_id=uuid5(NAMESPACE_URL, "g")),
                             portal_snapshots=snapshots)
    events = []
    monkeypatch.setattr(module, "persist_screening_generation_cursor", lambda cursor, screening: None)
    monkeypatch.setattr(module, "persist_portal_snapshot_cursor", lambda cursor, snapshot: events.append("snapshot"))
    compute, publication = MagicMock(), MagicMock()
    receipt = PortalReleaseReceipt(uuid5(NAMESPACE_URL, "r"), uuid5(NAMESPACE_URL, "c"),
                                   ProviderTier.MASSIVE_DEVELOPER, OBSERVED_AT, digest("u"))

    module.persist_equity_validation_values(compute, publication, values, {}, generation=1)
    assert events == ["snapshot", "snapshot"]
    module.persist_equity_validation_values(
        compute, publication, values, {}, generation=1, receipt=receipt,
        release_repository=lambda cursor, active, rows: events.append(("release", active is receipt, len(rows))),
    )
    assert events[2:] == ["snapshot", "snapshot", ("release", True, 2)]