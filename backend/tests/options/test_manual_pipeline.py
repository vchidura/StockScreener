import json
import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
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
from options.orchestration import ManualOptionPipeline
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


class FakeEngine:
    def __init__(self):
        self.batch_id = uuid4()

    def list_option_references(
        self, underlyer, as_of, expiration_through, asset_type, strike_min, strike_max
    ):
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

    def complete(self, work_id, lease_owner):
        self.completed = True
        return True

    def retry(self, *args):
        raise AssertionError("successful cycle must not retry work")


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
    def __init__(self):
        self.finished = None

    def start(self, run):
        return run.matrix_id

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