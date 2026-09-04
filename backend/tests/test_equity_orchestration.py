import sys
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
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
    EquityEvidence,
    EvidenceRole,
    EvidenceType,
    LifecycleStatus,
    QualityState,
    SecurityReferenceRevision,
)
from equity.materialization import MaterializationResult
from equity.outcomes import default_directional_policy
from equity.orchestration import (
    EquityMaterializationService,
    _context_evidence,
    _evidence_allowed_by_interval,
)
from equity.polygon import canonical_json, sha256_json


UTC = timezone.utc
NOW = datetime(2026, 8, 28, 15, 0, tzinfo=UTC)
HASH = "a" * 64


def overview(ticker):
    return {
        "active": True, "cik": f"{len(ticker):010d}",
        "composite_figi": f"FIGI-{ticker}", "market_cap": 10_000_000_000,
        "name": f"{ticker} Company", "primary_exchange": "XNAS",
        "sic_code": "3571", "sic_description": "ELECTRONIC COMPUTERS",
        "ticker": ticker, "type": "CS", "weighted_shares_outstanding": 1_000_000,
    }


def security(ticker):
    return SecurityReferenceRevision(
        security_revision_id=uuid4(), security_id=uuid4(), ticker=ticker,
        active=True, company_name=f"{ticker} Company", security_type="CS",
        cik="0000000001", composite_figi=f"FIGI-{ticker}",
        share_class_figi=None, primary_exchange="XNAS", sic_code="3571",
        sic_description="ELECTRONIC COMPUTERS", sector="Information Technology",
        industry="Technology", list_date=None, delisted_date=None,
        weighted_shares=Decimal("1000000"), free_float=Decimal("900000"),
        free_float_percent=90.0, market_cap=Decimal("10000000000"),
        source="POLYGON_TICKER_OVERVIEW_V3",
        effective_from=datetime(2026, 8, 28, tzinfo=UTC), observed_at=NOW,
        payload_sha256=HASH, raw_payload_json="{}",
    )


class FakeClient:
    def fetch_float(self, tickers):
        return tuple(
            {"ticker": ticker, "free_float": 900_000, "free_float_percent": 90.0}
            for ticker in tickers
        )

    def fetch_ticker_overview(self, ticker, *, as_of_date=None):
        return overview(ticker)

    def fetch_native_bars(self, ticker, interval, start, end, *, adjusted):
        return ({
            "t": int(datetime(2026, 8, 28, 13, 30, tzinfo=UTC).timestamp() * 1000),
            "o": 100, "h": 102, "l": 99, "c": 101, "v": 1000,
        },)


class ForkingFakeClient(FakeClient):
    def __init__(self, state=None, *, fail_ticker=None):
        self.state = state or {"forks": [], "fetch_clients": []}
        self.fail_ticker = fail_ticker
        self.client_id = uuid4()

    def fork(self):
        client = type(self)(self.state, fail_ticker=self.fail_ticker)
        self.state["forks"].append(client.client_id)
        return client

    def fetch_native_bars(self, ticker, interval, start, end, *, adjusted):
        self.state["fetch_clients"].append(self.client_id)
        if ticker == self.fail_ticker:
            raise RuntimeError("ticker unavailable")
        return super().fetch_native_bars(ticker, interval, start, end, adjusted=adjusted)


class FakeReferenceRepository:
    def __init__(self):
        self.persisted = ()
        self.projected = ()
        self.by_revision = {}

    def persist_security_revisions(self, rows):
        self.persisted = tuple(rows)
        return len(rows)

    def update_selected_ticker_projection(self, rows):
        self.projected = tuple(rows)

    def list_fundamentals_as_of(self, security_id, watermark, limit=8):
        return ()

    def get_security_revision(self, security_revision_id):
        return self.by_revision.get(security_revision_id)


class FakeUniverseRepository:
    def __init__(self):
        self.call = None

    def persist_complete_run(self, **kwargs):
        self.call = kwargs


class FakeIngestionRepository:
    def __init__(self):
        self.started = None
        self.completed = None

    def start_segment(self, **kwargs):
        self.started = kwargs

    def complete_segment(self, segment_id, **kwargs):
        self.completed = (segment_id, kwargs)


class FakeBarRepository:
    def __init__(self):
        self.persisted = []
        self.persist_batches = []
        self.latest_bar_end = NOW
        self.pending_reconciliation = ()
        self.publication = None
        self.bulk_bars = {}
        self.bulk_read_calls = []

    def persist(self, rows):
        batch = tuple(rows)
        self.persisted.extend(batch)
        self.persist_batches.append(batch)
        return len(batch)

    def list_final_as_of(self, ticker, interval, watermark, *, limit, **kwargs):
        return tuple(
            SimpleNamespace(
                bar_revision_id=uuid4(),
                ticker=ticker,
                interval=interval,
                bar_end=(
                    self.latest_bar_end
                    if index == 49
                    else self.latest_bar_end - timedelta(minutes=50 - index)
                ),
            )
            for index in range(50)
        )

    def list_final_after(
        self, ticker, interval, *, after, available_by, limit,
        historical_reconstructed_only=False, adjusted=False,
    ):
        return ()

    def list_final_for_tickers_as_of(self, tickers, interval, watermark, **kwargs):
        self.bulk_read_calls.append(kwargs)
        return {ticker: tuple(self.bulk_bars.get(ticker, ())) for ticker in tickers}

    def list_pending_reconciliation(self, interval, *, available_by, limit):
        return self.pending_reconciliation

    def common_market_times(
        self, tickers, interval, *, start, end, available_by, limit
    ):
        return (NOW - timedelta(minutes=30), NOW)

    def publish_canonical_cohort(self, **kwargs):
        self.publication = kwargs
        selected = len(kwargs["selected"])
        failed = len(kwargs["failure_reasons"])
        expected = len(kwargs["members"])
        status = (
            "COMPLETE" if selected == expected
            else "DEGRADED" if selected / expected >= kwargs["minimum_coverage"]
            else "FAILED"
        )
        return {
            "publication_id": kwargs["publication_id"],
            "status": status,
            "selected_members": selected,
            "missing_members": expected - selected - failed,
            "failed_members": failed,
        }


class FakeAnalysisRepository:
    def __init__(self):
        self.members = []
        self.projections = []

    def start_run(self, **kwargs):
        self.started = kwargs
        return {
            "analysis_run_id": kwargs["analysis_run_id"],
            "observed_at": kwargs["observed_at"],
            "status": "RUNNING",
            "was_created": True,
        }

    def complete_member(self, run_id, security_id, **kwargs):
        self.members.append((run_id, security_id, kwargs))

    def publish_run(self, run_id, *, output_sha256=None, projections=()):
        self.output_sha256 = output_sha256
        self.projections.extend(projections)
        return {
            "analysis_run_id": run_id,
            "status": "COMPLETE",
            "published_at": NOW + timedelta(seconds=2),
        }


class FakeEvidenceRepository:
    def __init__(self):
        self.by_ticker = {}
        self.contexts = []
        self.projections = []

    def robust_qualification_ids_as_of(self, watermark):
        return frozenset()

    def robust_qualifications_as_of(self, watermark, *, interval, horizon_key):
        return {}

    def persist(self, rows):
        for row in rows:
            self.by_ticker.setdefault(row.ticker, []).append(row)
        return len(rows)

    def list_as_of(self, ticker, watermark):
        return tuple(self.by_ticker.get(ticker, ()))

    def persist_context(self, context, links):
        self.contexts.append(context)

    def upsert_current_projection(self, **kwargs):
        self.projections.append(kwargs)


class FakeOutcomeRepository:
    def __init__(self):
        self.subjects = ()
        self.persisted = []
        self.observations = []
        self.qualifications = []
        self.revision_context = {}

    def persist_policy(self, policy):
        self.policy = policy

    def list_pending_directional_subjects(
        self, policy, horizon_key, *, available_by,
        signal_observed_through=None, prospective_only=False,
        subject_evidence_ids=(), limit
    ):
        self.prospective_only = prospective_only
        self.subject_evidence_ids = subject_evidence_ids
        return self.subjects

    def persist_outcomes(self, outcomes):
        self.persisted.extend(outcomes)
        return len(outcomes)

    def outcome_revision_context(self, subject_ids, policy, horizon_key):
        return self.revision_context

    def qualification_observations(
        self, *, available_by, interval=None, source_names=(),
        outcome_policy_keys=()
    ):
        return self.observations

    def persist_qualification_revisions(self, revisions):
        self.qualifications.extend(revisions)
        return len(revisions)


def service(client=None, **overrides):
    dependencies = {
        "reference_repository": FakeReferenceRepository(),
        "universe_repository": FakeUniverseRepository(),
        "ingestion_repository": FakeIngestionRepository(),
        "bar_repository": FakeBarRepository(),
        "analysis_repository": FakeAnalysisRepository(),
        "evidence_repository": FakeEvidenceRepository(),
        "outcome_repository": FakeOutcomeRepository(),
    }
    dependencies.update(overrides)
    return EquityMaterializationService(client or FakeClient(), **dependencies), dependencies


def test_reference_refresh_persists_company_names_and_point_in_time_universe():
    pipeline, dependencies = service()

    result = pipeline.refresh_reference(
        ["aapl", "msft"], observed_at=NOW, as_of_date=date(2026, 8, 28)
    )

    assert [row.company_name for row in result.revisions] == [
        "AAPL Company", "MSFT Company"
    ]
    assert dependencies["reference_repository"].projected == result.revisions
    assert dependencies["universe_repository"].call["members"] == result.revisions
    assert result.missing_tickers == ()


def test_native_30m_ingestion_persists_segment_and_bar_lineage():
    pipeline, dependencies = service()
    securities = (security("AAPL"),)

    result = pipeline.ingest_native_interval(
        securities, interval="30m", start=date(2026, 8, 28),
        end=date(2026, 8, 28), observed_at=datetime(2026, 8, 28, 14, 1, tzinfo=UTC),
    )

    assert result.bar_count == 1
    assert result.inserted_count == 1
    assert dependencies["ingestion_repository"].started["interval"] == "30m"
    bar = dependencies["bar_repository"].persisted[0]
    assert bar.ingestion_segment_id == result.ingestion_segment_id
    assert dependencies["ingestion_repository"].completed[1]["status"] == "COMPLETE"
    assert dependencies["ingestion_repository"].completed[1]["market_watermark"] == datetime(
        2026, 8, 28, 14, 0, tzinfo=UTC
    )


def test_native_ingestion_terminal_fails_segment_on_provider_error():
    pipeline, dependencies = service()
    pipeline.client.fetch_native_bars = lambda *args, **kwargs: (_ for _ in ()).throw(
        RuntimeError("provider unavailable")
    )

    with pytest.raises(RuntimeError, match="provider unavailable"):
        pipeline.ingest_native_interval(
            (security("AAPL"),), interval="30m", start=date(2026, 8, 28),
            end=date(2026, 8, 28), observed_at=NOW,
        )

    _, completion = dependencies["ingestion_repository"].completed
    assert completion["status"] == "FAILED"
    assert completion["record_count"] == 0
    assert completion["gap_details"] == {"failure_type": "RuntimeError"}


def test_parallel_native_ingestion_uses_forked_clients_and_degrades_at_95_percent():
    client = ForkingFakeClient(fail_ticker="T19")
    pipeline, dependencies = service(client=client, native_fetch_workers=4)
    securities = tuple(security(f"T{index:02d}") for index in range(20))

    result = pipeline.ingest_native_interval(
        securities, interval="30m", start=date(2026, 8, 28),
        end=date(2026, 8, 28), observed_at=NOW,
    )

    _, completion = dependencies["ingestion_repository"].completed
    assert result.bar_count == 19
    assert result.inserted_count == 19
    assert result.failed_tickers == ("T19",)
    assert result.missing_tickers == ("T19",)
    assert completion["status"] == "DEGRADED"
    assert completion["gap_details"]["fetch_workers"] == 4
    assert completion["gap_details"]["failed_tickers"] == {"T19": "RuntimeError"}
    assert client.state["forks"]
    assert client.client_id not in client.state["fetch_clients"]
    assert set(client.state["fetch_clients"]).issubset(set(client.state["forks"]))
    assert result.elapsed_seconds >= result.persist_seconds


def test_stream_reconciliation_persists_native_and_matched_revision():
    pipeline, dependencies = service()
    security_row = security("AAPL")
    stream_bar = EquityBarRevision(
        bar_revision_id=uuid4(), security_id=security_row.security_id,
        ticker="AAPL", interval="30m", session_date=date(2026, 8, 28),
        bar_start=datetime(2026, 8, 28, 13, 30, tzinfo=UTC),
        bar_end=datetime(2026, 8, 28, 14, 0, tzinfo=UTC),
        open_price=Decimal("100"), high_price=Decimal("102"),
        low_price=Decimal("99"), close_price=Decimal("101"),
        volume=Decimal("1000"), vwap=None, transaction_count=None,
        source_kind=BarSourceKind.REALTIME_STREAM,
        availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
        is_final=True, system_observed_at=NOW, replay_available_at=None,
        adjusted=False, payload_sha256=HASH, reconciliation_status="PENDING",
    )
    dependencies["bar_repository"].pending_reconciliation = (stream_bar,)

    result = pipeline.reconcile_stream_interval(
        interval="30m", observed_at=NOW, limit=100
    )

    assert result.pending_count == 1
    assert result.native_count == 1
    assert result.reconciled_count == 1
    assert result.inserted_count == 2
    assert result.status_counts == (("MATCHED", 1),)
    native, reconciled = dependencies["bar_repository"].persisted
    assert native.source_kind is BarSourceKind.NATIVE_REST
    assert reconciled.source_kind is BarSourceKind.RECONCILED
    assert reconciled.reconciliation_status == "MATCHED"
    assert reconciled.source_bar_revision_ids == (
        stream_bar.bar_revision_id, native.bar_revision_id,
    )
    assert dependencies["ingestion_repository"].completed[1]["status"] == "COMPLETE"


def test_stream_reconciliation_terminal_fails_segment_on_provider_error():
    pipeline, dependencies = service()
    security_row = security("AAPL")
    stream_bar = EquityBarRevision(
        bar_revision_id=uuid4(), security_id=security_row.security_id,
        ticker="AAPL", interval="30m", session_date=date(2026, 8, 28),
        bar_start=datetime(2026, 8, 28, 13, 30, tzinfo=UTC),
        bar_end=datetime(2026, 8, 28, 14, 0, tzinfo=UTC),
        open_price=Decimal("100"), high_price=Decimal("102"),
        low_price=Decimal("99"), close_price=Decimal("101"),
        volume=Decimal("1000"), vwap=None, transaction_count=None,
        source_kind=BarSourceKind.REALTIME_STREAM,
        availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
        is_final=True, system_observed_at=NOW, replay_available_at=None,
        adjusted=False, payload_sha256=HASH, reconciliation_status="PENDING",
    )
    dependencies["bar_repository"].pending_reconciliation = (stream_bar,)
    pipeline.client.fetch_native_bars = lambda *args, **kwargs: (_ for _ in ()).throw(
        RuntimeError("provider unavailable")
    )

    with pytest.raises(RuntimeError, match="provider unavailable"):
        pipeline.reconcile_stream_interval(
            interval="30m", observed_at=NOW, limit=100
        )

    assert dependencies["ingestion_repository"].completed[1]["status"] == "FAILED"


def test_canonical_publication_selects_only_bars_at_market_watermark():
    pipeline, dependencies = service()
    securities = (security("AAPL"), security("MSFT"))
    dependencies["bar_repository"].latest_bar_end = NOW

    result = pipeline.publish_canonical_interval(
        securities, interval="30m", watermark=DecisionWatermark(NOW, NOW)
    )

    assert result.status == "COMPLETE"
    assert result.selected == 2
    publication = dependencies["bar_repository"].publication
    assert set(publication["selected"]) == {"AAPL", "MSFT"}
    assert publication["selection_policy_version"] == "equity_bar_selection_v1"
    assert publication["session_scope"] is BarSessionScope.RTH


def test_daily_derivation_persists_complete_multiticker_cohort():
    pipeline, dependencies = service()
    securities = (security("AAPL"), security("MSFT"))
    session_open = datetime(2026, 8, 28, 13, 30, tzinfo=UTC)
    session_close = datetime(2026, 8, 28, 20, 0, tzinfo=UTC)
    for item in securities:
        bars = []
        start = session_open
        while start < session_close:
            end = min(start + timedelta(minutes=30), session_close)
            bars.append(EquityBarRevision(
                bar_revision_id=uuid4(), security_id=item.security_id,
                ticker=item.ticker, interval="30m", session_date=date(2026, 8, 28),
                bar_start=start, bar_end=end, open_price=Decimal("100"),
                high_price=Decimal("102"), low_price=Decimal("99"),
                close_price=Decimal("101"), volume=Decimal("1000"), vwap=None,
                transaction_count=None, source_kind=BarSourceKind.RECONCILED,
                availability_mode=BarAvailabilityMode.LIVE_OBSERVED, is_final=True,
                system_observed_at=end, replay_available_at=None, adjusted=False,
                payload_sha256=sha256_json({"ticker": item.ticker, "start": start.isoformat()}),
            ))
            start = end
        dependencies["bar_repository"].bulk_bars[item.ticker] = bars

    result = pipeline.derive_interval(
        securities,
        target_interval="1d",
        watermark=DecisionWatermark(session_close, session_close),
    )

    assert result.bar_count == 2
    assert result.inserted_count == 2
    assert result.missing_tickers == ()
    assert {bar.ticker for bar in dependencies["bar_repository"].persisted} == {
        "AAPL", "MSFT"
    }
    assert all(bar.interval == "1d" for bar in dependencies["bar_repository"].persisted)
    assert dependencies["ingestion_repository"].started["provider_mode"] == "DERIVED"
    assert dependencies["ingestion_repository"].completed[1]["status"] == "COMPLETE"


def test_existing_analysis_run_returns_without_recomputing(monkeypatch):
    pipeline, dependencies = service()
    item = security("AAPL")
    existing_id = uuid4()
    dependencies["analysis_repository"].start_run = lambda **kwargs: {
        "analysis_run_id": existing_id,
        "observed_at": kwargs["observed_at"],
        "status": "COMPLETE",
        "completed_members": 1,
        "insufficient_members": 0,
        "failed_members": 0,
        "was_created": False,
    }
    with patch("equity.orchestration.materialize_equity_evidence") as materialize:
        result = pipeline.materialize_interval(
            (item,), universe_run_id=uuid4(), interval="30m",
            watermark=DecisionWatermark(NOW, NOW),
        )

    assert result.analysis_run_id == existing_id
    assert result.status == "COMPLETE"
    assert result.completed == 1
    materialize.assert_not_called()
    assert dependencies["analysis_repository"].members == []


def test_history_derivation_persists_all_complete_periods():
    pipeline, dependencies = service()
    item = security("AAPL")
    dependencies["bar_repository"].bulk_bars[item.ticker] = []
    for day in (27, 28):
        start = datetime(2026, 8, day, 13, 30, tzinfo=UTC)
        for index in range(13):
            end = start + timedelta(minutes=30)
            dependencies["bar_repository"].bulk_bars[item.ticker].append(
                EquityBarRevision(
                    bar_revision_id=uuid4(), security_id=item.security_id,
                    ticker=item.ticker, interval="30m", session_date=date(2026, 8, day),
                    bar_start=start, bar_end=end, open_price=Decimal("100"),
                    high_price=Decimal("102"), low_price=Decimal("99"),
                    close_price=Decimal("101"), volume=Decimal("1000"), vwap=None,
                    transaction_count=None, source_kind=BarSourceKind.RECONCILED,
                    availability_mode=BarAvailabilityMode.LIVE_OBSERVED, is_final=True,
                    system_observed_at=end, replay_available_at=None, adjusted=False,
                    payload_sha256=sha256_json({"day": day, "index": index}),
                )
            )
            start = end

    result = pipeline.derive_interval(
        (item,), target_interval="1d",
        watermark=DecisionWatermark(
            datetime(2026, 8, 28, 20, 0, tzinfo=UTC),
            datetime(2026, 8, 28, 20, 0, tzinfo=UTC),
        ),
        include_history=True,
    )

    assert result.bar_count == 2
    assert [bar.session_date for bar in dependencies["bar_repository"].persisted] == [
        date(2026, 8, 27), date(2026, 8, 28)
    ]
    assert dependencies["bar_repository"].bulk_read_calls == [{
        "limit_per_ticker": None,
        "session_scope": BarSessionScope.RTH,
        "adjusted": False,
    }]


def test_history_derivation_reads_and_persists_in_bounded_ticker_batches():
    pipeline, dependencies = service()
    securities = tuple(security(f"T{index:02d}") for index in range(51))
    for item in securities:
        start = datetime(2026, 8, 28, 13, 30, tzinfo=UTC)
        dependencies["bar_repository"].bulk_bars[item.ticker] = [
            EquityBarRevision(
                bar_revision_id=uuid4(), security_id=item.security_id,
                ticker=item.ticker, interval="30m", session_date=date(2026, 8, 28),
                bar_start=start + timedelta(minutes=30 * index),
                bar_end=start + timedelta(minutes=30 * (index + 1)),
                open_price=Decimal("100"), high_price=Decimal("102"),
                low_price=Decimal("99"), close_price=Decimal("101"),
                volume=Decimal("1000"), vwap=None, transaction_count=None,
                source_kind=BarSourceKind.NATIVE_REST,
                availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
                is_final=True,
                system_observed_at=datetime(2026, 8, 28, 21, 0, tzinfo=UTC),
                replay_available_at=None,
                adjusted=False,
                payload_sha256=sha256_json({"ticker": item.ticker, "index": index}),
            )
            for index in range(13)
        ]

    result = pipeline.derive_interval(
        securities, target_interval="1d",
        watermark=DecisionWatermark(
            datetime(2026, 8, 28, 20, 0, tzinfo=UTC),
            datetime(2026, 8, 28, 21, 0, tzinfo=UTC),
        ),
        include_history=True,
    )

    assert result.bar_count == 51
    assert len(dependencies["bar_repository"].bulk_read_calls) == 2
    assert all(
        call["limit_per_ticker"] is None
        for call in dependencies["bar_repository"].bulk_read_calls
    )
    assert [len(batch) for batch in dependencies["bar_repository"].persist_batches] == [
        50, 1,
    ]


def test_analysis_projections_remain_scoped_to_owning_ticker():
    evidence_repository = FakeEvidenceRepository()
    pipeline, dependencies = service(evidence_repository=evidence_repository)
    securities = (security("AAPL"), security("MSFT"))

    def materialized(**kwargs):
        item = kwargs["security"]
        payload = {"ticker": item.ticker, "ema_direction": "NEUTRAL"}
        digest = sha256_json(payload)
        row = EquityEvidence(
            evidence_id=uuid4(), evidence_key=f"evidence:{item.ticker}",
            lifecycle_key=f"feature:{item.ticker}:30m",
            evidence_type=EvidenceType.FEATURE_SNAPSHOT,
            evidence_role=EvidenceRole.REGIME, security_id=item.security_id,
            ticker=item.ticker, interval="30m", direction=0,
            lifecycle_status=LifecycleStatus.SNAPSHOT, strength=None,
            market_time=NOW, observed_at=NOW + timedelta(seconds=1),
            valid_until=NOW + timedelta(hours=1), source_name="FEATURES",
            source_version="1.0", payload_schema_version="1.0",
            analysis_run_id=kwargs["analysis_run_id"], latest_bar_revision_id=uuid4(),
            security_revision_id=item.security_revision_id,
            fundamental_report_ids=(), source_revision_ids=(),
            quality_state=QualityState.COMPLETE, quality_codes=(),
            qualification_revision_id=None, payload_json=canonical_json(payload),
            payload_sha256=digest,
        )
        return MaterializationResult((row,), "NEUTRAL", "NEUTRAL", ())

    def resolved(**kwargs):
        item = kwargs["security"]
        context = EquityContextSnapshot(
            equity_context_snapshot_id=uuid4(), security_id=item.security_id,
            ticker=item.ticker, strategy_horizon="INTRADAY_30M",
            market_time=NOW, observed_at=NOW + timedelta(seconds=1),
            valid_until=NOW + timedelta(hours=1), status=ContextStatus.DEGRADED,
            universe_run_id=kwargs["universe_run_id"],
            security_revision_id=item.security_revision_id,
            fundamental_snapshot_id=None, regime_state=None,
            ema_direction="NEUTRAL", qualified_direction=None,
            direction_qualification_id=None, direction_evidence_id=None,
            direction_horizon=None, direction_valid_until=None,
            trigger_state=None, trigger_valid_until=None, range_forecast_id=None,
            range_lower=None, range_upper=None, range_valid_until=None,
            market_cap=item.market_cap, shares_outstanding=item.weighted_shares,
            free_float=item.free_float, dividend_yield=None, enterprise_value=None,
            ebitda=None, operating_income=None, free_cash_flow=None,
            risk_levels_json="{}", conflict_state_json="{}",
            stale_components_json="[]",
            reason_codes=("QUALIFIED_DIRECTION_UNAVAILABLE",),
            summary_json=canonical_json({"company_name": item.company_name}),
            context_policy_version="context_v1", context_policy_sha256=HASH,
        )
        return context, ()

    with (
        patch("equity.orchestration.materialize_equity_evidence", side_effect=materialized),
        patch("equity.orchestration.build_equity_context", side_effect=resolved),
    ):
        result = pipeline.materialize_interval(
            securities, universe_run_id=uuid4(), interval="30m",
            watermark=DecisionWatermark(NOW, NOW + timedelta(seconds=1)),
        )

    assert result.completed == 2
    assert result.evidence_count == 2
    assert result.inserted_evidence_count == 2
    assert len(dependencies["analysis_repository"].started["input_sha256"]) == 64
    assert len(dependencies["analysis_repository"].output_sha256) == 64
    projections = dependencies["analysis_repository"].projections
    assert {row["ticker"] for row in projections} == {"AAPL", "MSFT"}
    for projection in projections:
        assert projection["payload"].get("ticker", projection["ticker"]) == projection["ticker"]


def test_shadow_analysis_persists_results_without_current_projections():
    evidence_repository = FakeEvidenceRepository()
    pipeline, dependencies = service(evidence_repository=evidence_repository)

    with patch("equity.orchestration.materialize_equity_evidence") as materialize:
        materialize.return_value = MaterializationResult((), "NEUTRAL", "NEUTRAL", ())
        result = pipeline.materialize_interval(
            (security("AAPL"),), universe_run_id=uuid4(), interval="15m",
            watermark=DecisionWatermark(NOW, NOW + timedelta(seconds=1)),
            run_purpose="SHADOW",
        )

    assert result.status == "COMPLETE"
    assert dependencies["analysis_repository"].projections == []
    assert materialize.call_args.kwargs["fundamental_metrics"]["market_cap"] == Decimal(
        "10000000000"
    )


def test_analysis_marks_member_insufficient_when_latest_bar_misses_watermark():
    pipeline, dependencies = service()
    dependencies["bar_repository"].latest_bar_end = NOW - timedelta(minutes=30)

    result = pipeline.materialize_interval(
        (security("AAPL"),), universe_run_id=uuid4(), interval="30m",
        watermark=DecisionWatermark(NOW, NOW + timedelta(seconds=1)),
        run_purpose="SHADOW",
    )

    assert result.completed == 0
    assert result.insufficient == 1
    assert dependencies["analysis_repository"].members[0][2]["status"] == "INSUFFICIENT_DATA"
    assert dependencies["analysis_repository"].members[0][2]["failure_reason"] == (
        "latest finalized interval bar is unavailable"
    )


def test_context_evidence_replaces_same_interval_and_rejects_invalid_families():
    confirmation_id = uuid4()
    current_feature = SimpleNamespace(
        evidence_id=uuid4(),
        interval="15m", evidence_type=EvidenceType.FEATURE_SNAPSHOT,
        source_version="equity_features_v1",
    )
    old_feature = SimpleNamespace(
        evidence_id=uuid4(),
        interval="15m", evidence_type=EvidenceType.FEATURE_SNAPSHOT,
        source_version="equity_features_v1",
    )
    invalid_scanner = SimpleNamespace(
        evidence_id=uuid4(),
        interval="15m", evidence_type=EvidenceType.SCANNER_RESULT,
        source_version="1.0",
    )
    invalid_setup = SimpleNamespace(
        evidence_id=uuid4(),
        interval="15m", evidence_type=EvidenceType.TRADE_SETUP,
        source_version="equity_setup_v1",
    )
    obsolete_fundamental = SimpleNamespace(
        evidence_id=uuid4(),
        interval=None, evidence_type=EvidenceType.FUNDAMENTAL_SNAPSHOT,
        source_version="fundamental_snapshot_v1",
    )
    valid_30m_scanner = SimpleNamespace(
        evidence_id=uuid4(),
        interval="30m", evidence_type=EvidenceType.SCANNER_RESULT,
        source_version="1.0",
    )
    previous_confirmation = SimpleNamespace(
        evidence_id=confirmation_id,
        interval="1h", evidence_type=EvidenceType.REGIME_SIGNAL,
        source_version="ema_confirmation_1h_v2",
    )
    current_confirmation = SimpleNamespace(
        evidence_id=confirmation_id,
        interval="1h", evidence_type=EvidenceType.REGIME_SIGNAL,
        source_version="ema_confirmation_1h_v2",
    )

    selected = _context_evidence(
        (
            old_feature, invalid_scanner, invalid_setup, obsolete_fundamental,
            valid_30m_scanner, previous_confirmation,
        ),
        (current_feature, current_confirmation),
        interval="15m",
    )

    assert selected == (valid_30m_scanner, current_feature, current_confirmation)
    assert _evidence_allowed_by_interval(invalid_scanner) is False
    assert _evidence_allowed_by_interval(invalid_setup) is False
    assert _evidence_allowed_by_interval(obsolete_fundamental) is False


def test_immature_outcome_remains_pending_until_explicit_finalization():
    outcome_repository = FakeOutcomeRepository()
    pipeline, _ = service(outcome_repository=outcome_repository)
    payload = {"stop_price": "95", "target_price": "110"}
    digest = sha256_json(payload)
    outcome_repository.subjects = (EquityEvidence(
        evidence_id=uuid4(), evidence_key="scanner:AAPL:1", lifecycle_key=None,
        evidence_type=EvidenceType.SCANNER_RESULT,
        evidence_role=EvidenceRole.DIRECTION, security_id=uuid4(), ticker="AAPL",
        interval="30m", direction=1, lifecycle_status=LifecycleStatus.MATCH,
        strength=None, market_time=NOW, observed_at=NOW + timedelta(seconds=1),
        valid_until=NOW + timedelta(hours=2), source_name="breakout_expansion",
        source_version="1.0", payload_schema_version="1.0",
        analysis_run_id=uuid4(), latest_bar_revision_id=uuid4(),
        security_revision_id=uuid4(), fundamental_report_ids=(),
        source_revision_ids=(), quality_state=QualityState.RESEARCH_ONLY,
        quality_codes=(), qualification_revision_id=None,
        payload_json=canonical_json(payload), payload_sha256=digest,
    ),)
    policy = default_directional_policy(
        source_name="breakout_expansion", source_version="1.0", interval="30m",
        horizons={"60m": 2}, effective_from=datetime(2026, 8, 1, tzinfo=UTC),
    )

    result = pipeline.evaluate_directional_outcomes(
        policy, "60m", available_by=NOW + timedelta(minutes=30)
    )

    assert result.due == 1
    assert result.pending == 1
    assert result.persisted == 0
    assert outcome_repository.persisted == []


def test_outcome_uses_subject_security_revision_sector_etf_and_spy():
    outcome_repository = FakeOutcomeRepository()
    bar_repository = FakeBarRepository()
    reference_repository = FakeReferenceRepository()
    pipeline, _ = service(
        outcome_repository=outcome_repository,
        bar_repository=bar_repository,
        reference_repository=reference_repository,
    )
    reference = replace(
        security("AAPL"), sector="IT - Semiconductors & Hardware"
    )
    reference_repository.by_revision[reference.security_revision_id] = reference
    payload = {"stop_price": "95", "target_price": "110"}
    digest = sha256_json(payload)
    outcome_repository.subjects = (EquityEvidence(
        evidence_id=uuid4(), evidence_key="scanner:AAPL:sector", lifecycle_key=None,
        evidence_type=EvidenceType.SCANNER_RESULT,
        evidence_role=EvidenceRole.DIRECTION, security_id=reference.security_id,
        ticker="AAPL", interval="30m", direction=1,
        lifecycle_status=LifecycleStatus.MATCH, strength=None,
        market_time=NOW, observed_at=NOW + timedelta(seconds=1),
        valid_until=NOW + timedelta(hours=2), source_name="breakout_expansion",
        source_version="1.0", payload_schema_version="1.0",
        analysis_run_id=uuid4(), latest_bar_revision_id=uuid4(),
        security_revision_id=reference.security_revision_id,
        fundamental_report_ids=(), source_revision_ids=(),
        quality_state=QualityState.RESEARCH_ONLY, quality_codes=(),
        qualification_revision_id=None, payload_json=canonical_json(payload),
        payload_sha256=digest,
    ),)
    superseded_outcome_id = uuid4()
    outcome_repository.revision_context = {
        outcome_repository.subjects[0].evidence_id: (2, superseded_outcome_id)
    }
    after = NOW + timedelta(seconds=2)
    bar_repository.list_final_after = lambda ticker, interval, **kwargs: (
        EquityBarRevision(
            bar_revision_id=uuid4(), security_id=uuid4(), ticker=ticker,
            interval=interval, session_date=NOW.date(),
            bar_start=after + timedelta(minutes=index * 30),
            bar_end=after + timedelta(minutes=(index + 1) * 30),
            open_price=Decimal("100"), high_price=Decimal("102"),
            low_price=Decimal("99"), close_price=Decimal("101"),
            volume=Decimal("1000"), vwap=None, transaction_count=100,
            source_kind=BarSourceKind.NATIVE_REST,
            availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
            is_final=True, system_observed_at=after + timedelta(hours=1),
            replay_available_at=None, adjusted=False, payload_sha256=HASH,
        )
        for index in range(kwargs["limit"])
    )
    policy = default_directional_policy(
        source_name="breakout_expansion", source_version="1.0", interval="30m",
        horizons={"60m": 2}, effective_from=datetime(2026, 8, 1, tzinfo=UTC),
        primary_benchmark="SECTOR",
    )

    result = pipeline.evaluate_directional_outcomes(
        policy, "60m", available_by=NOW + timedelta(hours=2)
    )

    assert result.persisted == 1
    outcome = outcome_repository.persisted[0]
    assert outcome.market_benchmark_ticker == "SPY"
    assert outcome.sector_benchmark_ticker == "SMH"
    assert outcome.market_return is not None
    assert outcome.sector_return is not None
    assert outcome.sector_net_alpha is not None
    assert outcome.outcome_revision == 2
    assert outcome.supersedes_outcome_id == superseded_outcome_id


def test_historical_outcome_path_cache_prefetches_and_reuses_longest_path():
    outcome_repository = FakeOutcomeRepository()
    bar_repository = FakeBarRepository()
    pipeline, _ = service(
        outcome_repository=outcome_repository,
        bar_repository=bar_repository,
        outcome_path_cache_size=10,
        outcome_path_prefetch_limit=2,
    )
    payload = {"stop_price": "95", "target_price": "110"}
    subject = EquityEvidence(
        evidence_id=uuid4(), evidence_key="scanner:AAPL:cached", lifecycle_key=None,
        evidence_type=EvidenceType.SCANNER_RESULT,
        evidence_role=EvidenceRole.DIRECTION, security_id=uuid4(), ticker="AAPL",
        interval="30m", direction=1, lifecycle_status=LifecycleStatus.MATCH,
        strength=None, market_time=NOW, observed_at=NOW + timedelta(seconds=1),
        valid_until=NOW + timedelta(hours=2), source_name="breakout_expansion",
        source_version="1.0", payload_schema_version="1.0",
        analysis_run_id=uuid4(), latest_bar_revision_id=uuid4(),
        security_revision_id=None, fundamental_report_ids=(),
        source_revision_ids=(), quality_state=QualityState.RESEARCH_ONLY,
        quality_codes=(), qualification_revision_id=None,
        payload_json=canonical_json(payload), payload_sha256=sha256_json(payload),
    )
    outcome_repository.subjects = (subject,)
    calls = []
    after = NOW + timedelta(seconds=2)

    def list_final_after(ticker, interval, **kwargs):
        calls.append((ticker, kwargs["limit"]))
        return tuple(
            EquityBarRevision(
                bar_revision_id=uuid4(), security_id=uuid4(), ticker=ticker,
                interval=interval, session_date=NOW.date(),
                bar_start=after + timedelta(minutes=index * 30),
                bar_end=after + timedelta(minutes=(index + 1) * 30),
                open_price=Decimal("100"), high_price=Decimal("102"),
                low_price=Decimal("99"), close_price=Decimal("101"),
                volume=Decimal("1000"), vwap=None, transaction_count=100,
                source_kind=BarSourceKind.NATIVE_REST,
                availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
                is_final=True, system_observed_at=after + timedelta(hours=1),
                replay_available_at=after, adjusted=False, payload_sha256=HASH,
            )
            for index in range(kwargs["limit"])
        )

    bar_repository.list_final_after = list_final_after
    policy = default_directional_policy(
        source_name="breakout_expansion", source_version="1.0", interval="30m",
        horizons={"30m": 1, "60m": 2},
        effective_from=datetime(2026, 8, 1, tzinfo=UTC),
    )

    first = pipeline.evaluate_directional_outcomes(
        policy, "30m", available_by=NOW + timedelta(hours=2),
        historical_reconstructed_only=True,
    )
    second = pipeline.evaluate_directional_outcomes(
        policy, "60m", available_by=NOW + timedelta(hours=2),
        historical_reconstructed_only=True,
    )

    assert first.persisted == second.persisted == 1
    assert calls == [("AAPL", 2), ("SPY", 2)]


def test_outcome_path_reads_the_adjusted_lineage_when_requested():
    outcome_repository = FakeOutcomeRepository()
    bar_repository = FakeBarRepository()
    pipeline, _ = service(
        outcome_repository=outcome_repository,
        bar_repository=bar_repository,
        outcome_path_cache_size=10,
    )
    payload = {"stop_price": "95", "target_price": "110"}
    subject = EquityEvidence(
        evidence_id=uuid4(), evidence_key="scanner:AAPL:adjusted", lifecycle_key=None,
        evidence_type=EvidenceType.SCANNER_RESULT,
        evidence_role=EvidenceRole.DIRECTION, security_id=uuid4(), ticker="AAPL",
        interval="1d", direction=1, lifecycle_status=LifecycleStatus.MATCH,
        strength=None, market_time=NOW, observed_at=NOW + timedelta(seconds=1),
        valid_until=None, source_name="level_retest_rejection",
        source_version="1.2", payload_schema_version="1.0",
        analysis_run_id=None, latest_bar_revision_id=uuid4(),
        security_revision_id=None, fundamental_report_ids=(),
        source_revision_ids=(), quality_state=QualityState.RESEARCH_ONLY,
        quality_codes=(), qualification_revision_id=None,
        payload_json=canonical_json(payload), payload_sha256=sha256_json(payload),
    )
    outcome_repository.subjects = (subject,)
    requested = []

    def list_final_after(ticker, interval, **kwargs):
        requested.append((ticker, kwargs["adjusted"]))
        return ()

    bar_repository.list_final_after = list_final_after
    policy = default_directional_policy(
        source_name="level_retest_rejection", source_version="1.2", interval="1d",
        horizons={"5d": 5}, effective_from=datetime(2026, 8, 1, tzinfo=UTC),
    )

    pipeline.evaluate_directional_outcomes(
        policy, "5d", available_by=NOW + timedelta(days=10),
        historical_reconstructed_only=True, adjusted=True,
    )

    assert requested, "no bar path was requested"
    assert all(flag is True for _, flag in requested)

    requested.clear()
    pipeline.clear_outcome_path_cache()
    pipeline.evaluate_directional_outcomes(
        policy, "5d", available_by=NOW + timedelta(days=10),
        historical_reconstructed_only=True,
    )
    assert all(flag is False for _, flag in requested)


def test_qualification_run_is_safe_before_outcomes_mature():
    outcome_repository = FakeOutcomeRepository()
    pipeline, _ = service(outcome_repository=outcome_repository)

    result = pipeline.qualify_materialized_outcomes(
        available_by=NOW,
        interval="30m",
    )

    assert result.observations == 0
    assert result.revisions == 0
    assert result.robust_passes == 0


def test_replay_uses_each_market_watermark_and_never_requests_original_run():
    pipeline, _ = service()
    calls = []

    def materialize(securities, **kwargs):
        calls.append(kwargs)
        return SimpleNamespace(status="COMPLETE")

    pipeline.materialize_interval = materialize
    results = pipeline.replay_interval_range(
        (security("AAPL"),), universe_run_id=uuid4(), interval="30m",
        start=NOW - timedelta(hours=1), end=NOW, available_by=NOW,
    )

    assert len(results) == 2
    assert all(call["run_purpose"] == "REPLAY" for call in calls)
    assert [call["watermark"].observed_time for call in calls] == [
        NOW - timedelta(minutes=30), NOW,
    ]