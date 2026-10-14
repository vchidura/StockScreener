from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import NAMESPACE_URL, UUID, uuid5

from .contracts import canonical_sha256
from .forward_storage import decode_forward_artifact, encode_forward_artifact
from .forward_runtime import DeveloperForwardScheduler
from .option_pre_session_producer import (
    CompletedOptionDiscoveryPackage, OptionPreSessionCollection,
    OptionPreSessionProducer, OptionPreSessionRequest,
)


STAGING_VERSION = "option_pre_session_staging_v1"
MAXIMUM_STAGED_UNDERLYINGS = 1_000


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def option_pre_session_run_id(request: OptionPreSessionRequest) -> UUID:
    if not isinstance(request, OptionPreSessionRequest):
        raise TypeError("staged pre-session run requires a typed request")
    identity = canonical_sha256({"version": STAGING_VERSION, "scope": request.scope,
        "policy_sha256": request.policy.sha256, "session_date": request.session_date,
        "scheduled_cycle": request.scheduled_cycle, "effective_at": request.effective_at,
        "maximum_provider_requests": request.maximum_provider_requests,
        "maximum_response_bytes": request.maximum_response_bytes})
    return uuid5(NAMESPACE_URL, f"option-pre-session-staging:{identity}")


@dataclass(frozen=True, slots=True)
class StagedOptionPackageReceipt:
    run_id: UUID
    underlyer: str
    security_id: UUID
    package: CompletedOptionDiscoveryPackage
    provider_requests: int
    response_bytes: int
    completed_at: datetime

    def __post_init__(self):
        completed = _utc(self.completed_at, "completed_at")
        batch = self.package.evidence.result.batch
        if (self.underlyer != self.underlyer.upper() or self.underlyer != batch.underlyer
                or self.security_id != self.package.evidence.underlying_security_id
                or type(self.provider_requests) is not int or self.provider_requests < 0
                or type(self.response_bytes) is not int or self.response_bytes < 0):
            raise ValueError("staged option package identity/counters are invalid")
        object.__setattr__(self, "completed_at", completed)

    @property
    def package_sha256(self):
        return encode_forward_artifact(self.package)[1]


@dataclass(frozen=True, slots=True)
class StagedOptionPackageClaim:
    run_id: UUID
    underlyer: str
    owner_id: UUID
    owner_epoch: int
    attempts: int
    lease_expires_at: datetime


def staged_package_set_sha256(receipts) -> str:
    rows = tuple(sorted(receipts, key=lambda row: row.underlyer))
    return canonical_sha256({"version": STAGING_VERSION,
        "packages": [(row.underlyer, row.security_id, row.package_sha256,
            row.provider_requests, row.response_bytes, row.completed_at) for row in rows]})


def assemble_staged_collection(request: OptionPreSessionRequest, receipts, *,
                               provider_requests=None, response_bytes=None) -> OptionPreSessionCollection:
    rows = tuple(sorted(receipts, key=lambda row: row.underlyer))
    if (not rows or len(rows) > MAXIMUM_STAGED_UNDERLYINGS
            or any(not isinstance(row, StagedOptionPackageReceipt) for row in rows)
            or any(row.run_id != option_pre_session_run_id(request) for row in rows)):
        raise ValueError("staged option package receipt set is invalid")
    expected = request.scope.universe.option_underlyings
    if tuple(row.underlyer for row in rows) != expected:
        raise ValueError("staged option packages do not cover the exact universe")
    successful_requests = sum(row.provider_requests for row in rows)
    successful_bytes = sum(row.response_bytes for row in rows)
    total_requests = successful_requests if provider_requests is None else provider_requests
    total_bytes = successful_bytes if response_bytes is None else response_bytes
    if (type(total_requests) is not int or total_requests < successful_requests
            or type(total_bytes) is not int or total_bytes < successful_bytes):
        raise ValueError("staged option package aggregate usage is invalid")
    collection = OptionPreSessionCollection(tuple(row.package for row in rows),
        total_requests, total_bytes)
    OptionPreSessionProducer._validate_collection(request, collection)
    return collection


def persist_staged_run_cursor(cursor, request: OptionPreSessionRequest, *, collection_deadline: datetime):
    run_id = option_pre_session_run_id(request)
    deadline = _utc(collection_deadline, "collection_deadline")
    if deadline <= request.scheduled_cycle or len(request.scope.universe.option_underlyings) > MAXIMUM_STAGED_UNDERLYINGS:
        raise ValueError("staged option run deadline/universe is invalid")
    values = (str(run_id), str(request.scope.profile_id), request.scope.profile_sha256,
        request.scope.provider_tier.value, request.scope.universe.sha256, request.policy.sha256,
        request.session_date, request.scheduled_cycle, request.effective_at, deadline,
        request.maximum_provider_requests, request.maximum_response_bytes,
        list(request.scope.universe.option_underlyings))
    cursor.execute("INSERT INTO rt.option_pre_session_runs (run_id,profile_id,profile_sha256,provider_tier,"
        "universe_sha256,policy_sha256,session_date,scheduled_cycle,effective_at,collection_deadline,"
        "maximum_provider_requests,maximum_response_bytes,expected_underlyings) VALUES ("
        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (run_id) DO NOTHING", values)
    cursor.execute("SELECT profile_id::text,profile_sha256,provider_tier,universe_sha256,policy_sha256,"
        "session_date,scheduled_cycle,effective_at,collection_deadline,maximum_provider_requests,"
        "maximum_response_bytes,expected_underlyings FROM rt.option_pre_session_runs WHERE run_id=%s", (str(run_id),))
    if cursor.fetchone() != values[1:]:
        raise ValueError("staged option run identity conflicts with retained state")
    for underlyer, security_id in request.underlying_security_ids:
        cursor.execute("INSERT INTO rt.option_pre_session_work (run_id,underlyer,security_id,status) "
            "VALUES (%s,%s,%s,'PENDING') ON CONFLICT (run_id,underlyer) DO NOTHING",
            (str(run_id), underlyer, str(security_id)))
    cursor.execute("INSERT INTO rt.option_pre_session_account_usage (run_id) VALUES (%s) "
        "ON CONFLICT (run_id) DO NOTHING", (str(run_id),))
    return run_id


def reserve_staged_request_cursor(cursor, run_id: UUID, *, requests_per_second):
    if requests_per_second <= 0 or requests_per_second > 2:
        raise ValueError("staged request rate must be in (0, 2]")
    cursor.execute("SELECT clock_timestamp()")
    now = cursor.fetchone()[0]
    cursor.execute("SELECT usage.provider_requests,usage.next_request_at,run.maximum_provider_requests,"
        "run.collection_deadline,run.scheduled_cycle FROM rt.option_pre_session_account_usage AS usage "
        "JOIN rt.option_pre_session_runs AS run USING(run_id) WHERE usage.run_id=%s FOR UPDATE OF usage", (str(run_id),))
    row = cursor.fetchone()
    if row is None or now >= row[3]:
        raise ValueError("staged provider request is past its collection deadline")
    if now < row[4]:
        return "WAIT", (row[4] - now).total_seconds()
    if row[0] >= row[2]:
        raise ValueError("staged aggregate provider request budget exhausted")
    if row[1] is not None and row[1] > now:
        return "WAIT", (row[1] - now).total_seconds()
    cursor.execute("UPDATE rt.option_pre_session_account_usage SET provider_requests=provider_requests+1,"
        "next_request_at=%s + (%s * INTERVAL '1 second'),updated_at=%s WHERE run_id=%s",
        (now, float(1 / requests_per_second), now, str(run_id)))
    return "RESERVED", 0.0


def defer_staged_request_cursor(cursor, run_id: UUID, *, until: datetime):
    deferred = _utc(until, "defer_until")
    cursor.execute("UPDATE rt.option_pre_session_account_usage SET next_request_at=GREATEST("
        "COALESCE(next_request_at,%s),%s),updated_at=clock_timestamp() WHERE run_id=%s",
        (deferred, deferred, str(run_id)))
    if cursor.rowcount != 1:
        raise ValueError("staged provider defer run is unavailable")


def record_staged_response_cursor(cursor, run_id: UUID, *, response_bytes: int):
    if type(response_bytes) is not int or response_bytes < 0:
        raise ValueError("staged response byte evidence is invalid")
    cursor.execute("SELECT usage.response_bytes,run.maximum_response_bytes FROM "
        "rt.option_pre_session_account_usage AS usage JOIN rt.option_pre_session_runs AS run USING(run_id) "
        "WHERE usage.run_id=%s FOR UPDATE OF usage", (str(run_id),))
    row = cursor.fetchone()
    if row is None:
        raise ValueError("staged provider usage run is unavailable")
    total = int(row[0]) + response_bytes
    cursor.execute("UPDATE rt.option_pre_session_account_usage SET response_bytes=%s,updated_at=clock_timestamp() "
        "WHERE run_id=%s", (total, str(run_id)))
    return total, total <= int(row[1])


def claim_staged_package_cursor(cursor, run_id: UUID, owner_id: UUID, *, lease_seconds=120,
                                underlyer=None):
    if type(lease_seconds) is not int or not 1 <= lease_seconds <= 900:
        raise ValueError("staged option package lease must be in [1, 900]")
    cursor.execute("SELECT clock_timestamp()")
    now = cursor.fetchone()[0]
    cursor.execute("WITH candidate AS (SELECT work.run_id,work.underlyer FROM rt.option_pre_session_work AS work "
        "JOIN rt.option_pre_session_runs AS run USING(run_id) WHERE work.run_id=%s "
        "AND (%s::text IS NULL OR work.underlyer=%s) AND run.collection_deadline>%s "
        "AND work.attempts<3 AND (work.status='PENDING' OR (work.status='CLAIMED' AND work.lease_expires_at<=%s)) "
        "ORDER BY work.underlyer FOR UPDATE OF work SKIP LOCKED LIMIT 1) UPDATE rt.option_pre_session_work AS work "
        "SET status='CLAIMED',owner_id=%s,owner_epoch=work.owner_epoch+1,attempts=work.attempts+1,"
        "lease_expires_at=LEAST(%s + (%s * INTERVAL '1 second'),(SELECT collection_deadline "
        "FROM rt.option_pre_session_runs WHERE run_id=work.run_id)),reason=NULL FROM candidate "
        "WHERE work.run_id=candidate.run_id AND work.underlyer=candidate.underlyer RETURNING work.run_id::text,"
        "work.underlyer,work.owner_id::text,work.owner_epoch,work.attempts,work.lease_expires_at",
        (str(run_id), underlyer, underlyer, now, now, str(owner_id), now, lease_seconds))
    row = cursor.fetchone()
    return (StagedOptionPackageClaim(UUID(row[0]), row[1], UUID(row[2]), row[3], row[4], row[5])
        if row is not None else None)


def complete_staged_package_cursor(cursor, claim: StagedOptionPackageClaim,
                                   receipt: StagedOptionPackageReceipt):
    if (not isinstance(claim, StagedOptionPackageClaim) or not isinstance(receipt, StagedOptionPackageReceipt)
            or (claim.run_id, claim.underlyer) != (receipt.run_id, receipt.underlyer)):
        raise ValueError("staged option package completion identity drift")
    cursor.execute("SELECT clock_timestamp()")
    now = cursor.fetchone()[0]
    cursor.execute("SELECT status,owner_id::text,owner_epoch,lease_expires_at FROM rt.option_pre_session_work "
        "WHERE run_id=%s AND underlyer=%s FOR UPDATE", (str(claim.run_id), claim.underlyer))
    row = cursor.fetchone()
    if (row is None or tuple(row[:3]) != ("CLAIMED", str(claim.owner_id), claim.owner_epoch)
            or row[3] <= now):
        raise ValueError("staged option package owner/epoch/lease fence failed")
    payload, digest = encode_forward_artifact(receipt.package)
    cursor.execute("INSERT INTO rt.option_pre_session_packages (run_id,underlyer,security_id,"
        "package_payload_text,package_sha256,provider_requests,response_bytes,completed_at) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (run_id,underlyer) DO NOTHING",
        (str(receipt.run_id), receipt.underlyer, str(receipt.security_id), payload, digest,
            receipt.provider_requests, receipt.response_bytes, receipt.completed_at))
    cursor.execute("SELECT security_id::text,package_sha256,provider_requests,response_bytes,completed_at "
        "FROM rt.option_pre_session_packages WHERE run_id=%s AND underlyer=%s",
        (str(receipt.run_id), receipt.underlyer))
    if cursor.fetchone() != (str(receipt.security_id), digest, receipt.provider_requests,
            receipt.response_bytes, receipt.completed_at):
        raise ValueError("staged option package conflicts with immutable receipt")
    cursor.execute("UPDATE rt.option_pre_session_work SET status='COMPLETE',completed_at=%s,"
        "provider_requests=provider_requests+%s,response_bytes=response_bytes+%s,lease_expires_at=NULL "
        "WHERE run_id=%s AND underlyer=%s AND owner_id=%s AND owner_epoch=%s",
        (now, receipt.provider_requests, receipt.response_bytes, str(claim.run_id), claim.underlyer,
            str(claim.owner_id), claim.owner_epoch))
    if cursor.rowcount != 1:
        raise ValueError("staged option package completion fence changed")


def fail_staged_package_cursor(cursor, claim: StagedOptionPackageClaim, *, reason,
                               provider_requests, response_bytes):
    if (not isinstance(claim, StagedOptionPackageClaim) or not isinstance(reason, str)
            or not reason.strip() or len(reason) > 240
            or type(provider_requests) is not int or provider_requests < 0
            or type(response_bytes) is not int or response_bytes < 0):
        raise ValueError("staged option package failure evidence is invalid")
    cursor.execute("SELECT clock_timestamp()")
    now = cursor.fetchone()[0]
    cursor.execute("SELECT work.status,work.owner_id::text,work.owner_epoch,work.attempts,"
        "work.lease_expires_at,run.collection_deadline FROM rt.option_pre_session_work AS work "
        "JOIN rt.option_pre_session_runs AS run USING(run_id) WHERE work.run_id=%s AND work.underlyer=%s FOR UPDATE OF work",
        (str(claim.run_id), claim.underlyer))
    row = cursor.fetchone()
    if (row is None or tuple(row[:3]) != ("CLAIMED", str(claim.owner_id), claim.owner_epoch)
            or row[4] <= now):
        raise ValueError("staged option package failure fence changed")
    status = "PENDING" if row[3] < 3 and now < row[5] else "BLOCKED"
    cursor.execute("UPDATE rt.option_pre_session_work SET status=%s,provider_requests=provider_requests+%s,"
        "response_bytes=response_bytes+%s,lease_expires_at=NULL,reason=%s WHERE run_id=%s AND underlyer=%s "
        "AND owner_id=%s AND owner_epoch=%s", (status, provider_requests, response_bytes, reason,
            str(claim.run_id), claim.underlyer, str(claim.owner_id), claim.owner_epoch))
    if cursor.rowcount != 1:
        raise ValueError("staged option package failure update fence changed")
    return status


def load_staged_package_receipts_cursor(cursor, request: OptionPreSessionRequest):
    run_id = option_pre_session_run_id(request)
    cursor.execute("SELECT underlyer,security_id::text,package_payload_text,package_sha256,provider_requests,"
        "response_bytes,completed_at FROM rt.option_pre_session_packages WHERE run_id=%s ORDER BY underlyer",
        (str(run_id),))
    receipts = []
    for underlyer, security_id, payload, digest, requests, byte_count, completed_at in cursor.fetchall():
        package = decode_forward_artifact(payload, digest)
        if not isinstance(package, CompletedOptionDiscoveryPackage):
            raise ValueError("staged option package payload type drift")
        receipts.append(StagedOptionPackageReceipt(run_id, underlyer, UUID(security_id),
            package, requests, byte_count, completed_at))
    return tuple(receipts)


def load_staged_usage_cursor(cursor, run_id: UUID):
    cursor.execute("SELECT provider_requests,response_bytes FROM rt.option_pre_session_account_usage WHERE run_id=%s",
        (str(run_id),))
    usage = cursor.fetchone()
    cursor.execute("SELECT COUNT(*) FILTER (WHERE status='COMPLETE'),COUNT(*) FILTER (WHERE status='BLOCKED') "
        "FROM rt.option_pre_session_work WHERE run_id=%s", (str(run_id),))
    complete, blocked = cursor.fetchone()
    if usage is None:
        raise ValueError("staged provider usage is unavailable")
    requests, byte_count = usage
    return int(requests), int(byte_count), int(complete), int(blocked)


class StagedAccountScheduler(DeveloperForwardScheduler):
    def __init__(self, database, run_id, budget, *, clock, wait):
        super().__init__(budget, clock=clock, wait=wait)
        self.database, self.run_id = database, run_id

    def request(self, operation, *, available_after, deadline):
        if not self._request_lock.acquire(blocking=True):
            raise ValueError("staged scheduler request lock unavailable")
        try:
            now = self.clock()
            if now.tzinfo is None or not available_after <= now < deadline:
                raise ValueError("staged provider request is outside its package window")
            if self.next_request_at is not None and self.next_request_at > self.clock():
                with self.database.transaction() as cursor:
                    defer_staged_request_cursor(cursor, self.run_id, until=self.next_request_at)
                self.next_request_at = None
            while True:
                with self.database.transaction() as cursor:
                    status, seconds = reserve_staged_request_cursor(cursor, self.run_id,
                        requests_per_second=self.budget.requests_per_second)
                if status == "RESERVED":
                    break
                if self.clock() + timedelta(seconds=seconds) >= deadline:
                    raise ValueError("staged provider pacing cannot meet package deadline")
                self.wait(seconds)
            self.requests += 1
            result = operation()
            size = getattr(result, "response_bytes", getattr(result, "body", None))
            size = len(size) if isinstance(size, bytes) else size
            if type(size) is not int or size < 0:
                raise ValueError("staged provider response lacks byte evidence")
            with self.database.transaction() as cursor:
                total, within = record_staged_response_cursor(cursor, self.run_id, response_bytes=size)
            self.response_bytes += size
            if not within:
                raise ValueError(f"staged aggregate response-byte budget exhausted at {total}")
            return result
        finally:
            self._request_lock.release()


def persist_staged_finalization_cursor(cursor, request: OptionPreSessionRequest,
                                       selection_id: UUID, *, finalized_at: datetime):
    run_id = option_pre_session_run_id(request)
    receipts = load_staged_package_receipts_cursor(cursor, request)
    provider_requests, response_bytes, complete, blocked = load_staged_usage_cursor(cursor, run_id)
    if complete != len(request.scope.universe.option_underlyings) or blocked:
        raise ValueError("staged option run is not dependency-complete for finalization")
    collection = assemble_staged_collection(request, receipts,
        provider_requests=provider_requests, response_bytes=response_bytes)
    package_sha = staged_package_set_sha256(receipts)
    finalized = _utc(finalized_at, "finalized_at")
    cursor.execute("INSERT INTO rt.option_pre_session_finalizations (run_id,selection_id,package_set_sha256,"
        "provider_requests,response_bytes,finalized_at) VALUES (%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (run_id) DO NOTHING", (str(run_id), str(selection_id), package_sha,
            collection.provider_requests, collection.response_bytes, finalized))
    cursor.execute("SELECT selection_id::text,package_set_sha256,provider_requests,response_bytes,finalized_at "
        "FROM rt.option_pre_session_finalizations WHERE run_id=%s", (str(run_id),))
    stored = cursor.fetchone()
    if (stored is None or tuple(stored[:4]) != (str(selection_id), package_sha,
            collection.provider_requests, collection.response_bytes)):
        raise ValueError("staged option finalization conflicts with retained evidence")
    return collection