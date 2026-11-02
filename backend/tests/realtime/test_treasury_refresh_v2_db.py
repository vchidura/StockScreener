from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from threading import Lock
from uuid import uuid4

import pytest

from options.model_inputs import TREASURY_TENORS
from realtime.database_access_v2 import OWNER_POOL, V2AccessConfig, V2ConnectionProvider
from realtime.reference_transports import ReferenceHttpResponse
from realtime.treasury_refresh import TreasuryRefreshOwner, verify_treasury_curve_cursor
from v2_database import disposable_v2_database, requires_v2_database


pytestmark = requires_v2_database
UTC = timezone.utc
SESSION = date(2026, 11, 2)


@pytest.fixture
def provider():
    with disposable_v2_database(f"test_realtime_v2_treasury_{uuid4().hex[:8]}") as pool:
        yield V2ConnectionProvider(
            {OWNER_POOL: pool}, V2AccessConfig(allow_disposable=True),
        )


def _http_factory(*, fail=False, counter=None):
    class Http:
        def __init__(self, **kwargs):
            self.requests = 0
            self.response_bytes = 0
            self.http_status = None
            self.received_at = None

        def get(self, url, params, headers):
            self.requests += 1
            if counter is not None:
                with counter["lock"]:
                    counter["requests"] += 1
            if fail:
                raise TimeoutError("fixture")
            fields = "".join(f"<d:{field}>4.20</d:{field}>" for field in TREASURY_TENORS)
            body = (
                '<feed xmlns="http://www.w3.org/2005/Atom" '
                'xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices" '
                'xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata">'
                "<updated>2026-10-30T22:00:00Z</updated><entry><content><m:properties>"
                "<d:NEW_DATE>2026-10-30T00:00:00Z</d:NEW_DATE>"
                f"{fields}</m:properties></content></entry></feed>"
            ).encode()
            self.received_at = datetime.now(UTC)
            self.http_status = 200
            self.response_bytes = len(body)
            return ReferenceHttpResponse(200, body)

    return Http


def test_real_ledger_fence_and_read_only_verification_with_concurrent_owners(provider):
    counter = {"requests": 0, "lock": Lock()}

    def run(_):
        return TreasuryRefreshOwner(
            provider, owner_id=uuid4(), http_factory=_http_factory(counter=counter),
        ).tick(SESSION)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = tuple(executor.map(run, range(2)))
    assert counter["requests"] == 1
    assert sum(result["status"] == "REFRESHED" for result in results) == 1
    assert {result["status"] for result in results} <= {
        "REFRESHED", "VERIFIED", "OTHER_OWNER", "RETRY_WAIT",
    }
    with provider.transaction(read_only=True, isolation_level="REPEATABLE READ") as unit:
        report = verify_treasury_curve_cursor(
            unit.cursor, session_date=SESSION, checked_at=datetime.now(UTC),
        )
        unit.cursor.execute(
            "SELECT state,attempt,response_bytes,row_count,reserved_response_bytes,"
            "account_request_ceiling,account_decoded_byte_ceiling "
            "FROM rt.provider_request_ledger"
        )
        rows = unit.cursor.fetchall()
    assert report["status"] == "VERIFIED"
    assert report["tenor_count"] == 14
    assert len(rows) == 1
    assert rows[0][0:2] == ("COMPLETE", 1)
    assert rows[0][3:] == (14, 2_000_000, 6, 12_000_000)


def test_real_failed_attempt_is_durable_and_restart_does_not_repeat(provider):
    first = TreasuryRefreshOwner(
        provider, owner_id=uuid4(), http_factory=_http_factory(fail=True),
    ).tick(SESSION)
    assert first["status"] == "FAILED"
    restarted = TreasuryRefreshOwner(
        provider, owner_id=uuid4(), http_factory=_http_factory(fail=True),
    ).tick(SESSION)
    assert restarted["status"] == "RETRY_WAIT"
    assert restarted["provider_requests_this_cycle"] == 0
    with provider.transaction(read_only=True) as unit:
        unit.cursor.execute("SELECT state,attempt,failure_code FROM rt.provider_request_ledger")
        rows = unit.cursor.fetchall()
    assert rows == [("FAILED", 1, "TREASURY_TIMEOUTERROR")]
