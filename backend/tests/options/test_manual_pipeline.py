import json
import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.config import load_option_runtime_configuration
from options.domain import (
    AssetType,
    BatchStatus,
    CatalogEligibility,
    ContractType,
    DataQualityFlag,
    DecisionContext,
    DurableWorkItem,
    ExerciseStyle,
    OptionContractCatalogEntry,
    OptionContractReference,
    PageValidationStatus,
    RawBatchPage,
    RawOptionBatch,
    SpotPrice,
    WorkStage,
    WorkStatus,
)
from options.orchestration import (
    ManualOptionPipeline,
    _fresh_mark_window,
    _resolve_dividend_yield,
)
from options.analytics.marks import UnderlyingMinuteBar


UTC = timezone.utc
MARKET_TIME = datetime(2026, 8, 28, 20, 0, tzinfo=UTC)
OBSERVED_AT = MARKET_TIME + timedelta(minutes=15)
HASH = "a" * 64


class FakeCalendar:
    def latest_completed_session(self, as_of):
        return date(2026, 8, 28)

    def expiration_cutoff(self, value):
        if value == date(2026, 8, 28):
            return MARKET_TIME
        return datetime.combine(value, datetime.min.time(), tzinfo=UTC).replace(hour=20)

    def session_for_slot(self, value):
        return value.date()


class FakeEngine:
    def __init__(self):
        self.batch_id = uuid4()
        self.reference_bounds = None
        self.chain_bounds = None

    def list_option_references(
        self, underlyer, as_of, expiration_through, asset_type, strike_min, strike_max
    ):
        self.reference_bounds = (expiration_through, strike_min, strike_max)
        return (
            OptionContractReference(
                "O:SPY260904C00100000", underlyer, asset_type, "polygon", "1",
                "call", date(2026, 9, 4), Decimal("100"), "american", 100,
                "X", None, "[]", "{}", False, MARKET_TIME, None,
                OBSERVED_AT, None, OBSERVED_AT, HASH,
            ),
        )

    def get_spot_price(self, underlyer, as_of):
        return SpotPrice(underlyer, "polygon_stocks", Decimal("100"), MARKET_TIME, OBSERVED_AT)

    def get_option_chain(self, underlyer, as_of, expiration_through, strike_min, strike_max):
        self.chain_bounds = (expiration_through, strike_min, strike_max)
        payload = {
            "results": [{
                "details": {"ticker": "O:SPY260904C00100000"},
                "underlying_asset": {"price": 100},
                "day": {
                    "close": 2.5,
                    "volume": 20,
                    "last_updated": int(MARKET_TIME.timestamp() * 1_000_000_000),
                },
                "open_interest": 100,
            }]
        }
        body = json.dumps(payload).encode()
        page = RawBatchPage(
            self.batch_id, 1, 1, body, HASH, OBSERVED_AT, True,
            PageValidationStatus.VALID, HASH,
        )
        return RawOptionBatch(
            self.batch_id, "polygon", underlyer, as_of, HASH, HASH,
            BatchStatus.COMPLETE, (page,), OBSERVED_AT, OBSERVED_AT,
        )

    def get_underlying_minute_bars(self, underlyer, start_time, end_time):
        return (UnderlyingMinuteBar(Decimal("100"), MARKET_TIME),)


class FakeUniverseRepository:
    def __init__(self):
        self.completed = None

    def create_run(self, *args):
        return args[0]

    def activate_members(self, members):
        return len(tuple(members))

    def complete_run(self, *args):
        self.completed = args


class FakeCatalogRepository:
    def upsert_references(self, references):
        return tuple(range(1, len(tuple(references)) + 1))

    def get_by_tickers(self, tickers, context: DecisionContext):
        return {
            "O:SPY260904C00100000": OptionContractCatalogEntry(
                1, "O:SPY260904C00100000", "SPY", AssetType.ETF, "polygon",
                "1", ContractType.CALL, date(2026, 9, 4), Decimal("100"),
                ExerciseStyle.AMERICAN, 100, "X", CatalogEligibility.VALIDATED_ACTIVE,
                (), MARKET_TIME, None, OBSERVED_AT, None, HASH,
            )
        }


class FakeWorkRepository:
    def __init__(self):
        self.completed = False
        self.recovered = False

    def recover_expired_claims(self):
        self.recovered = True
        return 0

    def claim_by_business_key(self, business_key, lease_owner, lease_duration):
        return DurableWorkItem(
            uuid4(), WorkStage.NORMALIZE, "batch", business_key,
            WorkStatus.CLAIMED, 1, 5, OBSERVED_AT, OBSERVED_AT,
            lease_owner, OBSERVED_AT + timedelta(minutes=10), None, None,
        )

    def get_by_business_key(self, business_key):
        return None

    def complete(self, work_id, lease_owner):
        self.completed = True
        return True

    def retry(self, *args):
        raise AssertionError("successful cycle must not retry work")

    def terminal_fail(self, *args):
        raise AssertionError("successful cycle must not terminal-fail work")


class CompletedWorkRepository(FakeWorkRepository):
    def claim_by_business_key(self, business_key, lease_owner, lease_duration):
        return None

    def get_by_business_key(self, business_key):
        return DurableWorkItem(
            uuid4(), WorkStage.NORMALIZE, "batch", business_key,
            WorkStatus.COMPLETED, 1, 5, OBSERVED_AT, OBSERVED_AT,
            None, None, None, OBSERVED_AT,
        )


class TerminalQualityWorkRepository(FakeWorkRepository):
    def __init__(self):
        super().__init__()
        self.terminal_error = None

    def terminal_fail(self, work_id, lease_owner, error):
        self.terminal_error = error
        return True


class FakeIngestionRepository:
    def __init__(self):
        self.telemetry = None

    def record_normalization(self, *args, **kwargs):
        self.telemetry = (args, kwargs)


class FakeSnapshotRepository:
    def __init__(self):
        self.snapshots = ()

    def persist(self, snapshots, *args):
        self.snapshots = tuple(snapshots)
        return len(self.snapshots)

    def list_for_batch(self, batch_id, context):
        return self.snapshots


class FakeAnalysisRepository:
    def __init__(self, existing=None):
        self.finished = None
        self.existing = existing

    def start(self, run):
        return run.matrix_id

    def get(self, matrix_id, context):
        return self.existing

    def persist_expirations(self, analytics):
        return len(tuple(analytics))

    def finish(self, run):
        self.finished = run


def test_manual_pipeline_runs_complete_fixture_cycle():
    configuration = load_option_runtime_configuration(
        {"POLYGON_API_KEY": "test-secret"}, BACKEND_DIR
    )
    universe = FakeUniverseRepository()
    work = FakeWorkRepository()
    ingestion = FakeIngestionRepository()
    snapshots = FakeSnapshotRepository()
    analyses = FakeAnalysisRepository()
    pipeline = ManualOptionPipeline(
        configuration,
        FakeEngine(),
        calendar=FakeCalendar(),
        catalog_repository=FakeCatalogRepository(),
        universe_repository=universe,
        ingestion_repository=ingestion,
        snapshot_repository=snapshots,
        analysis_repository=analyses,
        work_repository=work,
        clock=lambda: OBSERVED_AT,
    )

    result = pipeline.run_once(("SPY",), as_of=OBSERVED_AT)

    assert result.results[0].status == "COMPLETE"
    assert result.results[0].retained_count == 1
    assert result.results[0].iv_convergence_fraction == 1.0
    assert work.completed is True
    assert work.recovered is True
    assert ingestion.telemetry is not None
    assert analyses.finished.status.value == "COMPLETE"
    assert universe.completed[1].value == "COMPLETE"
    assert DataQualityFlag.DIVIDEND_YIELD_DEFAULTED in snapshots.snapshots[0].quality_flags


def test_explicit_cycle_time_uses_intraday_slot_identity():
    configuration = load_option_runtime_configuration(
        {"POLYGON_API_KEY": "test-secret"}, BACKEND_DIR
    )
    universe = FakeUniverseRepository()
    pipeline = ManualOptionPipeline(
        configuration,
        FakeEngine(),
        calendar=FakeCalendar(),
        catalog_repository=FakeCatalogRepository(),
        universe_repository=universe,
        ingestion_repository=FakeIngestionRepository(),
        snapshot_repository=FakeSnapshotRepository(),
        analysis_repository=FakeAnalysisRepository(),
        work_repository=FakeWorkRepository(),
        clock=lambda: OBSERVED_AT,
    )

    result = pipeline.run_once(
        ("SPY",), as_of=OBSERVED_AT, cycle_time=MARKET_TIME,
    )

    assert result.as_of_session == date(2026, 8, 28)
    assert result.results[0].status == "COMPLETE"


def test_manual_pipeline_reports_progress_between_underlyers():
    configuration = load_option_runtime_configuration(
        {"POLYGON_API_KEY": "test-secret"}, BACKEND_DIR
    )
    pipeline = ManualOptionPipeline(
        configuration,
        FakeEngine(),
        calendar=FakeCalendar(),
        catalog_repository=FakeCatalogRepository(),
        universe_repository=FakeUniverseRepository(),
        ingestion_repository=FakeIngestionRepository(),
        snapshot_repository=FakeSnapshotRepository(),
        analysis_repository=FakeAnalysisRepository(),
        work_repository=FakeWorkRepository(),
        clock=lambda: OBSERVED_AT,
    )
    heartbeats = []

    pipeline.run_once(
        ("SPY",),
        as_of=OBSERVED_AT,
        cycle_time=MARKET_TIME,
        progress_callback=lambda: heartbeats.append(OBSERVED_AT),
    )

    assert heartbeats == [OBSERVED_AT, OBSERVED_AT]


def test_manual_pipeline_extends_chain_to_retained_candidate_legs():
    configuration = load_option_runtime_configuration(
        {"POLYGON_API_KEY": "test-secret"}, BACKEND_DIR
    )
    engine = FakeEngine()

    class OutcomeRepository:
        def retained_leg_bounds(self, underlyer, *, available_by):
            assert underlyer == "SPY"
            assert available_by == OBSERVED_AT
            return {
                "minimum_strike": Decimal("75"),
                "maximum_strike": Decimal("130"),
                "expiration_through": date(2026, 10, 2),
                "contract_count": 2,
            }

    pipeline = ManualOptionPipeline(
        configuration,
        engine,
        calendar=FakeCalendar(),
        catalog_repository=FakeCatalogRepository(),
        universe_repository=FakeUniverseRepository(),
        ingestion_repository=FakeIngestionRepository(),
        snapshot_repository=FakeSnapshotRepository(),
        analysis_repository=FakeAnalysisRepository(),
        work_repository=FakeWorkRepository(),
        outcome_repository=OutcomeRepository(),
        clock=lambda: OBSERVED_AT,
    )

    pipeline.run_once(("SPY",), as_of=OBSERVED_AT)

    expected = (date(2026, 10, 12), Decimal("75"), Decimal("130"))
    assert engine.reference_bounds == expected
    assert engine.chain_bounds == expected


def test_empty_normalized_matrix_is_terminal_quality_failure():
    configuration = load_option_runtime_configuration(
        {"POLYGON_API_KEY": "test-secret"}, BACKEND_DIR
    )
    work = TerminalQualityWorkRepository()

    class EmptyNormalizer:
        def normalize(self, batch_id, inputs):
            return SimpleNamespace(
                snapshots=(), matrix_snapshots=(), rejected_counts={},
                received_count=len(tuple(inputs)), retained_count=0,
                iv_convergence_fraction=None,
            )

    pipeline = ManualOptionPipeline(
        configuration,
        FakeEngine(),
        calendar=FakeCalendar(),
        catalog_repository=FakeCatalogRepository(),
        universe_repository=FakeUniverseRepository(),
        ingestion_repository=FakeIngestionRepository(),
        snapshot_repository=FakeSnapshotRepository(),
        analysis_repository=FakeAnalysisRepository(),
        work_repository=work,
        normalizer=EmptyNormalizer(),
        clock=lambda: OBSERVED_AT,
    )

    result = pipeline.run_once(("SPY",), as_of=OBSERVED_AT)

    assert result.results[0].status == "FAILED"
    assert result.results[0].retryable is False
    assert result.results[0].reasons == ("TerminalOptionQualityError",)
    assert work.terminal_error == "normalization produced no retained contracts"


def test_explicit_terminal_slot_restart_returns_already_completed():
    configuration = load_option_runtime_configuration(
        {"POLYGON_API_KEY": "test-secret"}, BACKEND_DIR
    )
    engine = FakeEngine()
    first_analyses = FakeAnalysisRepository()
    first = ManualOptionPipeline(
        configuration,
        engine,
        calendar=FakeCalendar(),
        catalog_repository=FakeCatalogRepository(),
        universe_repository=FakeUniverseRepository(),
        ingestion_repository=FakeIngestionRepository(),
        snapshot_repository=FakeSnapshotRepository(),
        analysis_repository=first_analyses,
        work_repository=FakeWorkRepository(),
        clock=lambda: OBSERVED_AT,
    ).run_once(("SPY",), as_of=OBSERVED_AT, cycle_time=MARKET_TIME)

    strategy_pipeline = SimpleNamespace(
        calls=[],
        process_persisted=lambda analysis, asset_type: (
            strategy_pipeline.calls.append((analysis.matrix_id, asset_type))
            or SimpleNamespace(status="COMPLETE", error=None)
        ),
    )
    restarted = ManualOptionPipeline(
        configuration,
        engine,
        calendar=FakeCalendar(),
        catalog_repository=FakeCatalogRepository(),
        universe_repository=FakeUniverseRepository(),
        ingestion_repository=FakeIngestionRepository(),
        snapshot_repository=FakeSnapshotRepository(),
        analysis_repository=FakeAnalysisRepository(first_analyses.finished),
        work_repository=CompletedWorkRepository(),
        strategy_pipeline=strategy_pipeline,
        clock=lambda: OBSERVED_AT,
    ).run_once(("SPY",), as_of=OBSERVED_AT, cycle_time=MARKET_TIME)

    assert first.results[0].status == "COMPLETE"
    assert restarted.universe_run_id == first.universe_run_id
    assert restarted.results[0].status == "ALREADY_COMPLETED"
    assert restarted.results[0].matrix_id == first.results[0].matrix_id
    assert strategy_pipeline.calls == [
        (first.results[0].matrix_id, AssetType.ETF),
    ]


def test_dividend_yield_uses_latest_valid_fundamental_evidence():
    evidence = SimpleNamespace(
        market_time=MARKET_TIME,
        observed_at=OBSERVED_AT,
        payload_json=json.dumps({"dividend_yield": 0.004}),
    )

    value, flags = _resolve_dividend_yield((evidence,), 0.0)

    assert value == 0.004
    assert flags == ()


def test_fresh_mark_window_excludes_stale_rows_from_bar_request():
    stale = SimpleNamespace(option_mark_time=OBSERVED_AT - timedelta(days=30))
    recent = SimpleNamespace(option_mark_time=OBSERVED_AT - timedelta(minutes=10))
    newest = SimpleNamespace(option_mark_time=OBSERVED_AT - timedelta(minutes=1))

    window = _fresh_mark_window((stale, recent, newest), OBSERVED_AT, 1800)

    assert window == (recent.option_mark_time, newest.option_mark_time)


def test_dividend_yield_invalid_evidence_keeps_explicit_default_flag():
    evidence = SimpleNamespace(
        market_time=MARKET_TIME,
        observed_at=OBSERVED_AT,
        payload_json=json.dumps({"dividend_yield": "invalid"}),
    )

    value, flags = _resolve_dividend_yield((evidence,), 0.0)

    assert value == 0.0
    assert flags == (DataQualityFlag.DIVIDEND_YIELD_DEFAULTED,)