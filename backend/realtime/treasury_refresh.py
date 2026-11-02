"""Exact-session Treasury refresh and bounded ownership in the resident lifecycle."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import math
import time
from uuid import NAMESPACE_URL, UUID, uuid5

import requests

from options.calendar import OptionExchangeCalendar
from options.model_inputs import (
    TREASURY_CURVE_SOURCE,
    TREASURY_TENORS,
    parse_treasury_curve,
)

from .acquisition_ledger import (
    ProviderRequestCompletion,
    ProviderRequestLane,
    ProviderRequestOutcome,
    ProviderRequestReservation,
    ProviderRequestState,
    RequestCompleteness,
    finish_provider_request_cursor,
    reserve_provider_request_cursor,
)
from .advanced_equity_lifecycle import (
    claim_lifecycle_lease_cursor,
    release_lifecycle_lease_cursor,
)
from .contracts import canonical_sha256
from .database_access_v2 import APP_ROLE, WriteFenceRejected
from .reference_facts import ReferenceFact, ReferenceFactRelation
from .reference_repository import persist_reference_facts_cursor
from .reference_transports import (
    ReferenceHttpResponse,
    ReferenceTransportLimits,
    TreasuryReferenceTransport,
)


UTC = timezone.utc
TREASURY_RESPONSE_BYTE_LIMIT = 2_000_000
TREASURY_MAXIMUM_ATTEMPTS = 6
TREASURY_ACCOUNT = "treasury_public_v1"
TREASURY_ENDPOINT = "treasury_par_yield"
_COLUMNS = (
    "observation_id", "rate_date", "tenor_days", "annual_rate", "source",
    "source_key", "source_observed_at", "received_at", "payload_sha256",
)


def _utc(value):
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Treasury evidence clock must be timezone-aware")
    return value.astimezone(UTC)


def required_treasury_rate_date(session_date, *, calendar=None):
    return (calendar or OptionExchangeCalendar()).previous_session_date(session_date)


def validate_treasury_rows(rows, *, required_rate_date, checked_at):
    checked = _utc(checked_at)
    if len(rows) != len(TREASURY_TENORS) or {
        row["tenor_days"] for row in rows
    } != set(TREASURY_TENORS.values()):
        raise ValueError("Treasury curve tenor coverage is incomplete or ambiguous")
    receipts = set()
    observations = set()
    for row in rows:
        received = _utc(row["received_at"])
        observed = _utc(row["source_observed_at"])
        digest = str(row["payload_sha256"])
        key = f"{required_rate_date.isoformat()}:{row['tenor_days']}d"
        identity = uuid5(
            NAMESPACE_URL,
            f"option-rate:{TREASURY_CURVE_SOURCE}:{key}:{received.isoformat()}:{digest}",
        )
        if (
            row["source"] != TREASURY_CURVE_SOURCE
            or row["rate_date"] != required_rate_date
            or row["source_key"] != key
            or not math.isfinite(float(row["annual_rate"]))
            or not required_rate_date <= observed.date()
            or not observed <= received <= checked
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
            or UUID(str(row["observation_id"])) != identity
        ):
            raise ValueError("Treasury curve source, hash identity, or clocks are invalid")
        receipts.add(received)
        observations.add(observed)
    if len(receipts) != 1 or len(observations) != 1:
        raise ValueError("Treasury curve combines different response generations")


def verify_treasury_curve_cursor(cursor, *, session_date, checked_at, calendar=None):
    required = required_treasury_rate_date(session_date, calendar=calendar)
    cursor.execute(
        "SELECT DISTINCT ON (tenor_days) " + ",".join(_COLUMNS)
        + " FROM ref.risk_free_rate_observations "
        "WHERE source=%s AND rate_date=%s "
        "ORDER BY tenor_days,received_at DESC,observation_id LIMIT 65",
        (TREASURY_CURVE_SOURCE, required),
    )
    rows = tuple(
        dict(row) if isinstance(row, Mapping) else dict(zip(_COLUMNS, row))
        for row in cursor.fetchall()
    )
    reason = None
    try:
        validate_treasury_rows(rows, required_rate_date=required, checked_at=checked_at)
    except (ValueError, TypeError, AttributeError) as error:
        reason = str(error)
    return {
        "status": "VERIFIED" if reason is None else "MISSING_OR_INVALID",
        "session_date": session_date.isoformat(),
        "required_rate_date": required.isoformat(),
        "checked_at": _utc(checked_at).isoformat(),
        "source": TREASURY_CURVE_SOURCE,
        "reason": reason,
        "tenor_count": len(rows),
        "required_tenors": sorted(TREASURY_TENORS.values()),
        "curve_sha256": canonical_sha256(rows),
        # Original XML is not retained in this relation: verify digest format and
        # its observation-ID binding, not a reconstruction of the raw payload.
        "payload_hash_verification": "FORMAT_AND_OBSERVATION_ID_BINDING",
        "observations": [
            {
                key: value.isoformat() if isinstance(value, (date, datetime))
                else str(value) if isinstance(value, (UUID, Decimal)) else value
                for key, value in row.items()
            }
            for row in rows
        ],
    }


class BoundedTreasuryHttp:
    """One HTTP attempt, no adapter retries/redirects, bounded decoded body."""

    def __init__(self, *, authorized_until, clock=None, session_factory=None):
        self.deadline = _utc(authorized_until)
        self.clock = clock or (lambda: datetime.now(UTC))
        self.session_factory = session_factory or requests.Session
        self.requests = 0
        self.response_bytes = 0
        self.http_status = None
        self.received_at = None

    def get(self, url, params, headers):
        if self.requests or _utc(self.clock()) > self.deadline:
            raise RuntimeError("Treasury request budget or authorization expired")
        self.requests += 1
        started = time.monotonic()
        with self.session_factory() as session:
            with session.get(
                url, params=dict(params), headers=dict(headers),
                timeout=(5, 30), allow_redirects=False, stream=True,
            ) as response:
                self.http_status = response.status_code
                body = bytearray()
                for chunk in response.iter_content(chunk_size=65_536):
                    self.response_bytes += len(chunk)
                    if (
                        self.response_bytes > TREASURY_RESPONSE_BYTE_LIMIT
                        or time.monotonic() - started > 45
                        or _utc(self.clock()) > self.deadline
                    ):
                        raise ValueError("Treasury response exceeded its byte/time budget")
                    body.extend(chunk)
                self.received_at = _utc(self.clock())
                if self.received_at > self.deadline:
                    raise ValueError("Treasury response arrived after authorization expired")
                return ReferenceHttpResponse(response.status_code, bytes(body))


def collect_treasury_facts(*, required_rate_date, http, checked_at):
    transport = TreasuryReferenceTransport(
        http,
        limits=ReferenceTransportLimits(
            maximum_requests=1,
            maximum_response_bytes=TREASURY_RESPONSE_BYTE_LIMIT,
            maximum_total_bytes=TREASURY_RESPONSE_BYTE_LIMIT,
            maximum_rows=1_000,
            maximum_pages=1,
        ),
    )
    xml = transport.fetch_xml(required_rate_date.year)
    received = _utc(http.received_at)
    rows = tuple(
        {
            "observation_id": row.rate_observation_id,
            "rate_date": row.rate_date,
            "tenor_days": row.tenor_days,
            "annual_rate": row.annual_rate,
            "source": row.source,
            "source_key": row.source_key,
            "source_observed_at": row.source_observed_at,
            "received_at": row.first_observed_at,
            "payload_sha256": row.payload_sha256,
        }
        for row in parse_treasury_curve(xml, received_at=received)
        if row.rate_date == required_rate_date
    )
    validate_treasury_rows(
        rows, required_rate_date=required_rate_date, checked_at=checked_at(),
    )
    return tuple(
        ReferenceFact.build(
            ReferenceFactRelation.RATE_OBSERVATION, "RATE_CURVE", "USD", **row,
        )
        for row in rows
    )


@dataclass(frozen=True)
class TreasuryAuthorizationFence:
    authorized_until: datetime

    def verify(self, cursor):
        cursor.execute("SELECT clock_timestamp()<=%s", (self.authorized_until,))
        if not cursor.fetchone()[0]:
            raise WriteFenceRejected("Treasury refresh authorization expired")


@dataclass(frozen=True)
class TreasuryRefreshFence:
    lease: object
    authorized_until: datetime

    def verify(self, cursor):
        cursor.execute(
            "SELECT owner_epoch FROM rt.worker_leases "
            "WHERE resource_key=%s AND owner_id=%s AND owner_epoch=%s "
            "AND lease_until>clock_timestamp() AND clock_timestamp()<=%s FOR UPDATE",
            (
                self.lease.resource_key, str(self.lease.owner_id),
                self.lease.owner_epoch, self.authorized_until,
            ),
        )
        if cursor.fetchone() is None:
            raise WriteFenceRejected("Treasury refresh lease or authorization expired")


def treasury_retry_at(reserved_at, attempt):
    return reserved_at + timedelta(minutes=min(240, 15 * 2 ** (attempt - 1)))


class TreasuryRefreshOwner:
    """Durable six-attempt budget per rate date, shared across restarts/replicas."""

    def __init__(self, provider, *, owner_id, clock=None, calendar=None, http_factory=None):
        self.provider = provider
        self.owner_id = owner_id
        self.clock = clock or (lambda: datetime.now(UTC))
        self.calendar = calendar or OptionExchangeCalendar()
        self.http_factory = http_factory or BoundedTreasuryHttp
        self._required = None
        self._next_check = None
        self._last = None
        self._cycle_http = None

    def tick(self, session_date):
        now = _utc(self.clock())
        required = required_treasury_rate_date(session_date, calendar=self.calendar)
        if self._required == required and self._next_check is not None and now < self._next_check:
            return {
                **self._last, "provider_requests_this_cycle": 0,
                "provider_response_bytes": 0, "database_writes": 0, "cached": True,
            }
        self._required = required
        self._next_check = now + timedelta(minutes=1)
        self._cycle_http = None
        try:
            result = self._refresh(session_date, required, now)
        except Exception as error:
            # Do not print provider/connection exception strings, which may contain secrets.
            result = {
                "status": "ERROR", "error_type": type(error).__name__,
                "provider_requests_this_cycle": (
                    0 if self._cycle_http is None else self._cycle_http.requests
                ),
            }
        result.setdefault("provider_requests_this_cycle", 0)
        result["required_rate_date"] = required.isoformat()
        self._last = result
        return result

    def _refresh(self, session_date, required, now):
        if now < self.calendar.session_close(required):
            return {"status": "WAITING_FOR_SESSION_CLOSE"}
        request_hash = canonical_sha256({
            "source": TREASURY_CURVE_SOURCE, "required_rate_date": required,
        })
        resource = f"treasury-curve:{required.isoformat()}"
        with self.provider.transaction(APP_ROLE) as unit:
            unit.cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (resource,),
            )
            verification = verify_treasury_curve_cursor(
                unit.cursor, session_date=session_date, checked_at=now, calendar=self.calendar,
            )
            if verification["status"] == "VERIFIED":
                return {"status": "VERIFIED", "curve_sha256": verification["curve_sha256"]}
            unit.cursor.execute(
                "SELECT attempt,reserved_at,state FROM rt.provider_request_ledger "
                "WHERE account_alias=%s AND endpoint_class=%s AND request_sha256=%s "
                "ORDER BY attempt DESC LIMIT 7",
                (TREASURY_ACCOUNT, TREASURY_ENDPOINT, request_hash),
            )
            attempts = tuple(unit.cursor.fetchall())
            attempt = max((row[0] for row in attempts), default=0) + 1
            if len(attempts) >= TREASURY_MAXIMUM_ATTEMPTS or attempt > TREASURY_MAXIMUM_ATTEMPTS:
                return {"status": "BUDGET_EXHAUSTED", "attempts": attempt - 1}
            if attempts:
                retry_at = treasury_retry_at(attempts[0][1], attempts[0][0])
                if now < retry_at:
                    return {"status": "RETRY_WAIT", "attempts": attempt - 1,
                            "next_retry_at": retry_at.isoformat()}
            lease = claim_lifecycle_lease_cursor(
                unit.cursor, owner_id=self.owner_id, checked_at=now,
                lease_seconds=120, resource_key=resource,
            )
            if lease is None:
                return {"status": "OTHER_OWNER"}
            reservation = ProviderRequestReservation(
                request_id=uuid5(NAMESPACE_URL, f"{resource}:{attempt}"),
                account_alias=TREASURY_ACCOUNT, lane=ProviderRequestLane.REFERENCE,
                endpoint_class=TREASURY_ENDPOINT, request_sha256=request_hash,
                reserved_at=now, attempt=attempt,
                budget_scope_sha256=request_hash, budget_session_date=required,
                authorization_reference=f"continuous-options-lifecycle:{required}",
                reserved_response_bytes=TREASURY_RESPONSE_BYTE_LIMIT,
                account_request_ceiling=TREASURY_MAXIMUM_ATTEMPTS,
                account_decoded_byte_ceiling=(
                    TREASURY_MAXIMUM_ATTEMPTS * TREASURY_RESPONSE_BYTE_LIMIT
                ),
            )
            reserve_provider_request_cursor(unit.cursor, reservation)
        deadline = now + timedelta(seconds=60)
        http = self.http_factory(authorized_until=deadline, clock=self.clock)
        self._cycle_http = http
        try:
            facts = collect_treasury_facts(
                required_rate_date=required, http=http, checked_at=self.clock,
            )
            with self.provider.transaction(
                APP_ROLE, fence=TreasuryRefreshFence(lease, deadline),
            ) as unit:
                persist_reference_facts_cursor(unit.cursor, facts)
                finish_provider_request_cursor(
                    unit.cursor, reservation.request_id,
                    ProviderRequestCompletion(
                        state=ProviderRequestState.COMPLETE, started_at=now,
                        completed_at=_utc(self.clock()), outcome=ProviderRequestOutcome.OK,
                        http_status=http.http_status, response_bytes=http.response_bytes,
                        row_count=len(facts), pages_fetched=1,
                        completeness=RequestCompleteness.COMPLETE,
                    ),
                )
            result = {"status": "REFRESHED", "attempts": attempt, "database_writes": len(facts)}
        except Exception as error:
            with self.provider.transaction(APP_ROLE) as unit:
                finish_provider_request_cursor(
                    unit.cursor, reservation.request_id,
                    ProviderRequestCompletion(
                        state=ProviderRequestState.FAILED, started_at=now,
                        completed_at=_utc(self.clock()), outcome=ProviderRequestOutcome.INVALID,
                        http_status=http.http_status, response_bytes=http.response_bytes,
                        failure_code=f"TREASURY_{type(error).__name__.upper()}"[:64],
                    ),
                )
            result = {"status": "FAILED", "attempts": attempt,
                      "error_type": type(error).__name__,
                      "next_retry_at": treasury_retry_at(now, attempt).isoformat()}
        finally:
            with self.provider.transaction(APP_ROLE) as unit:
                release_lifecycle_lease_cursor(unit.cursor, lease)
        return {**result, "provider_requests_this_cycle": http.requests,
                "provider_response_bytes": http.response_bytes}
