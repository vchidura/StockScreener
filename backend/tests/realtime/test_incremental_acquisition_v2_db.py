import json
import re
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import pytest

from options.data.polygon_http import PolygonHttpResponse
from realtime.contracts import UniverseManifest
from realtime.database_access import RealtimeDatabaseRole
from realtime.equity_bar_drain import drain_equity_bar_jobs
from realtime.incremental_acquisition import (
    IncrementalEquitySource,
    plan_incremental_requests_cursor,
)
from realtime.incremental_finalization import (
    finalize_incremental_five_minute_windows,
)
from realtime.provider_budget import ProviderBudgetScope
from realtime.acquisition_ledger import ProviderRequestLane
from realtime.stage_database_v2 import V2WorkerDatabase
from realtime.stage_runners_ingest import (
    EquityAcquisitionRequest,
    acquire_equity_bars,
)
from tests.realtime.test_equity_recent_repair_v2_db import _provider, _scope
from v2_database import disposable_v2_database, requires_v2_database


pytestmark = requires_v2_database
SESSION = date(2026, 10, 15)
CUTOFF = datetime(2026, 10, 15, 13, 35, tzinfo=timezone.utc)


@pytest.fixture
def pool():
    with disposable_v2_database(
        f"test_realtime_v2_incremental_{uuid4().hex[:8]}"
    ) as value:
        yield value


class Transport:
    def __init__(self):
        self.calls = []

    def get(self, url, params):
        self.calls.append((url, params))
        width, start, end = map(
            int,
            re.search(r"/range/(\d+)/minute/(\d+)/(\d+)", url).groups(),
        )
        rows = [
            {
                "t": value,
                "o": 100,
                "h": 101,
                "l": 99,
                "c": 100.5,
                "v": 1000.123456789,
            }
            for value in range(start, end, width * 60_000)
        ]
        return PolygonHttpResponse(
            200,
            json.dumps({"results": rows}).encode(),
            (),
        )


def test_incremental_cycle_reserves_persists_materializes_and_replays(pool):
    native = _scope(pool, SESSION)
    provider = _provider(pool)
    identity = native.identities[0]
    with provider.transaction(read_only=True) as unit:
        plans = plan_incremental_requests_cursor(
            unit.cursor,
            cohort_id=native.cohort_id,
            identities=(identity,),
            session_date=SESSION,
            interval="1m",
            cutoff=CUTOFF,
        )
    assert len(plans) == 1 and plans[0].expected_bars == 5
    budget = ProviderBudgetScope(
        account_alias="MASSIVE_DEVELOPER",
        session_date=SESSION,
        authorization_reference="incremental-fixture",
        provider_authorization_reference="fixture-subscription",
        expected_cost="fixture-cost",
        request_ceiling=1,
        decoded_byte_ceiling=2 * 1024 * 1024 + 1,
        lanes=frozenset({ProviderRequestLane.EQUITY_CURRENT}),
        authorized_until=datetime.now(timezone.utc) + timedelta(minutes=30),
    )
    transport = Transport()
    source = IncrementalEquitySource(
        provider,
        transport,
        budget,
        (identity,),
        plans,
        response_byte_limit=2 * 1024 * 1024,
        clock=lambda: datetime.now(timezone.utc),
    )
    request = EquityAcquisitionRequest(
        native.cohort_id,
        UniverseManifest(
            version="incremental-aapl-v1",
            equity_tickers=("AAPL",),
            option_underlyings=(),
        ),
        CUTOFF,
    )
    acquired = acquire_equity_bars(
        V2WorkerDatabase(provider, RealtimeDatabaseRole.INGRESS),
        source,
        request,
    )
    drained = drain_equity_bar_jobs(
        V2WorkerDatabase(provider, RealtimeDatabaseRole.COMPUTE),
        cohort_id=native.cohort_id,
        lease_owner=uuid4(),
        max_jobs=len(acquired.acquisition_batch_ids),
        acquisition_batch_ids=acquired.acquisition_batch_ids,
    )
    replay = source.collect(request)
    finalization = finalize_incremental_five_minute_windows(
        provider,
        cohort_id=native.cohort_id,
        session_date=SESSION,
        instrument_ids=(identity.instrument_id,),
        through=CUTOFF,
        checked_at=datetime.now(timezone.utc),
        authorization_reference="incremental-fixture",
        plan_deadline=budget.authorized_until,
    )

    assert acquired.fact_count == drained.bars == 5
    assert replay.provider_requests == 0
    assert len(transport.calls) == 1
    assert finalization == ({
        "cutoff": CUTOFF,
        "status": "COMPLETE",
        "bars": 0,
        "barrier_state": "COMPLETE",
        "reason_codes": (),
    },)
    with provider.transaction(read_only=True) as unit:
        unit.cursor.execute(
            "SELECT count(*) FROM rt.bar_current WHERE cohort_id=%s "
            "AND interval='1m' AND adjustment_basis='RAW_UNADJUSTED'",
            (str(native.cohort_id),),
        )
        assert unit.cursor.fetchone()[0] == 5
        unit.cursor.execute(
            "SELECT state,budget_session_date,reserved_response_bytes,payload_id "
            "FROM rt.provider_request_ledger "
            "WHERE endpoint_class='EQUITY_INCREMENTAL'"
        )
        state, budget_date, reserved_bytes, payload_id = unit.cursor.fetchone()
        assert (state, budget_date, reserved_bytes) == (
            "COMPLETE",
            SESSION,
            2 * 1024 * 1024 + 1,
        )
        assert payload_id is not None
