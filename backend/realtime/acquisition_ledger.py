"""Provider request ledger, acquisition watermarks and raw payload writers (schema V2)."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import Enum
from uuid import NAMESPACE_URL, UUID, uuid5

from psycopg2.extras import execute_values


RAW_PAYLOAD_CHUNK_BYTES = 1_048_576
RAW_PAYLOAD_CHUNK_PAGE_SIZE = 20
_WATERMARK_INTERVALS = frozenset({"1m", "5m", "15m", "30m", "1h", "1d", "1wk", "1mo"})


class ProviderRequestLane(str, Enum):
    EQUITY_CURRENT = "EQUITY_CURRENT"
    EQUITY_HISTORY = "EQUITY_HISTORY"
    OPTIONS_CHAIN = "OPTIONS_CHAIN"
    OPTIONS_ALIGNMENT = "OPTIONS_ALIGNMENT"
    REFERENCE = "REFERENCE"
    PRE_SESSION_STAGING = "PRE_SESSION_STAGING"


class ProviderRequestState(str, Enum):
    RESERVED = "RESERVED"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"
    AMBIGUOUS = "AMBIGUOUS"


class ProviderRequestOutcome(str, Enum):
    OK = "OK"
    HTTP_429 = "HTTP_429"
    HTTP_ERROR = "HTTP_ERROR"
    TIMEOUT = "TIMEOUT"
    INVALID = "INVALID"


class RequestCompleteness(str, Enum):
    COMPLETE = "COMPLETE"
    INCOMPLETE = "INCOMPLETE"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class PriceBasis(str, Enum):
    NOT_APPLICABLE = "NOT_APPLICABLE"
    RAW_FINALIZED_MINUTE_CLOSE_V1 = "RAW_FINALIZED_MINUTE_CLOSE_V1"
    ADJUSTED_BAR_START_V1 = "ADJUSTED_BAR_START_V1"


class WatermarkTerminalState(str, Enum):
    COMPLETE = "COMPLETE"
    NO_TRADE = "NO_TRADE"
    GAP = "GAP"


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _optional_utc(value: datetime | None, name: str) -> datetime | None:
    return None if value is None else _utc(value, name)


def _sha256(value: str, name: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


def _text(value: str, name: str, limit: int) -> str:
    text = value.strip()
    if not 1 <= len(text) <= limit:
        raise ValueError(f"{name} must contain 1-{limit} characters")
    return text


@dataclass(frozen=True, slots=True)
class ProviderRequestReservation:
    request_id: UUID
    account_alias: str
    lane: ProviderRequestLane
    endpoint_class: str
    request_sha256: str
    reserved_at: datetime
    attempt: int = 1
    cohort_id: UUID | None = None
    window_id: UUID | None = None
    stage_run_id: UUID | None = None
    budget_scope_sha256: str | None = None
    budget_session_date: date | None = None
    authorization_reference: str | None = None
    reserved_response_bytes: int | None = None
    account_request_ceiling: int | None = None
    account_decoded_byte_ceiling: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.lane, ProviderRequestLane):
            raise TypeError("provider request lane is invalid")
        if type(self.attempt) is not int or self.attempt < 1:
            raise ValueError("provider request attempt must be positive")
        object.__setattr__(self, "account_alias", _text(self.account_alias, "account_alias", 64))
        object.__setattr__(self, "endpoint_class", _text(self.endpoint_class, "endpoint_class", 48))
        object.__setattr__(self, "request_sha256", _sha256(self.request_sha256, "request_sha256"))
        object.__setattr__(self, "reserved_at", _utc(self.reserved_at, "reserved_at"))
        budget_values = (
            self.budget_scope_sha256,
            self.budget_session_date,
            self.authorization_reference,
            self.reserved_response_bytes,
            self.account_request_ceiling,
            self.account_decoded_byte_ceiling,
        )
        if any(value is not None for value in budget_values):
            if any(value is None for value in budget_values):
                raise ValueError("coordinated request budget fields must be all present")
            object.__setattr__(
                self,
                "budget_scope_sha256",
                _sha256(str(self.budget_scope_sha256), "budget_scope_sha256"),
            )
            if not isinstance(self.budget_session_date, date):
                raise TypeError("budget_session_date must be a date")
            object.__setattr__(
                self,
                "authorization_reference",
                _text(
                    str(self.authorization_reference),
                    "authorization_reference",
                    160,
                ),
            )
            for value, name in (
                (self.reserved_response_bytes, "reserved_response_bytes"),
                (self.account_request_ceiling, "account_request_ceiling"),
                (
                    self.account_decoded_byte_ceiling,
                    "account_decoded_byte_ceiling",
                ),
            ):
                if type(value) is not int or value <= 0:
                    raise ValueError(f"{name} must be a positive integer")
            if (
                self.reserved_response_bytes
                > self.account_decoded_byte_ceiling
            ):
                raise ValueError(
                    "reserved_response_bytes exceeds the account byte ceiling"
                )


@dataclass(frozen=True, slots=True)
class ProviderRequestCompletion:
    """Terminal facts of one request; mirrors the ledger CHECK constraints."""

    state: ProviderRequestState
    completed_at: datetime | None = None
    started_at: datetime | None = None
    outcome: ProviderRequestOutcome | None = None
    http_status: int | None = None
    response_bytes: int | None = None
    row_count: int | None = None
    pages_fetched: int | None = None
    requested_from: datetime | None = None
    requested_to: datetime | None = None
    price_basis: PriceBasis = PriceBasis.NOT_APPLICABLE
    completeness: RequestCompleteness = RequestCompleteness.NOT_APPLICABLE
    failure_code: str | None = None
    payload_id: UUID | None = None

    def __post_init__(self) -> None:
        if self.state is ProviderRequestState.RESERVED:
            raise ValueError("a completion cannot leave the request RESERVED")
        completed = _optional_utc(self.completed_at, "completed_at")
        started = _optional_utc(self.started_at, "started_at")
        requested_from = _optional_utc(self.requested_from, "requested_from")
        requested_to = _optional_utc(self.requested_to, "requested_to")
        if self.state in (ProviderRequestState.COMPLETE, ProviderRequestState.FAILED) and (
            completed is None or self.outcome is None
        ):
            raise ValueError("a finished provider request requires completed_at and outcome")
        if self.state is ProviderRequestState.FAILED and not self.failure_code:
            raise ValueError("a FAILED provider request requires a failure code")
        if (
            self.completeness is RequestCompleteness.INCOMPLETE and not self.failure_code
        ):
            raise ValueError("an INCOMPLETE provider request requires a failure code")
        if self.completeness is RequestCompleteness.COMPLETE and (
            self.state is not ProviderRequestState.COMPLETE
            or self.outcome is not ProviderRequestOutcome.OK
        ):
            raise ValueError("only an OK COMPLETE request can have COMPLETE completeness")
        if completed is not None and started is not None and completed < started:
            raise ValueError("provider request completes before it starts")
        if (
            requested_from is not None
            and requested_to is not None
            and requested_to < requested_from
        ):
            raise ValueError("provider request window is inverted")
        for value, name in (
            (self.response_bytes, "response_bytes"),
            (self.pages_fetched, "pages_fetched"),
            (self.row_count, "row_count"),
        ):
            if value is not None and (type(value) is not int or value < 0):
                raise ValueError(f"{name} cannot be negative")
        object.__setattr__(self, "completed_at", completed)
        object.__setattr__(self, "started_at", started)
        object.__setattr__(self, "requested_from", requested_from)
        object.__setattr__(self, "requested_to", requested_to)


@dataclass(frozen=True, slots=True)
class RecordedProviderRequest:
    reservation: ProviderRequestReservation
    completion: ProviderRequestCompletion


def _reservation_identity(value: ProviderRequestReservation) -> tuple[object, ...]:
    return (
        str(value.request_id),
        value.account_alias,
        value.lane.value,
        str(value.cohort_id) if value.cohort_id is not None else None,
        str(value.window_id) if value.window_id is not None else None,
        str(value.stage_run_id) if value.stage_run_id is not None else None,
        value.endpoint_class,
        value.request_sha256,
        value.attempt,
        value.budget_scope_sha256,
        value.budget_session_date,
        value.authorization_reference,
        value.reserved_response_bytes,
        value.account_request_ceiling,
        value.account_decoded_byte_ceiling,
    )


def _normalized(row) -> tuple[object, ...]:
    return tuple(str(value) if isinstance(value, UUID) else value for value in row)


def reserve_provider_request_cursor(cursor, reservation: ProviderRequestReservation) -> UUID:
    identity = _reservation_identity(reservation)
    coordinated = reservation.budget_scope_sha256 is not None
    if coordinated:
        cursor.execute(
            "INSERT INTO rt.provider_request_ledger (request_id, account_alias, lane, "
            "cohort_id, window_id, stage_run_id, endpoint_class, request_sha256, attempt, "
            "budget_scope_sha256, budget_session_date, authorization_reference, "
            "reserved_response_bytes, account_request_ceiling, account_decoded_byte_ceiling, "
            "state, reserved_at) VALUES ("
            "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
            "'RESERVED', %s) "
            "ON CONFLICT (request_id) DO NOTHING",
            (*identity, reservation.reserved_at),
        )
        cursor.execute(
            "SELECT request_id, account_alias, lane, cohort_id, window_id, stage_run_id, "
            "endpoint_class, request_sha256, attempt, budget_scope_sha256, "
            "budget_session_date, authorization_reference, reserved_response_bytes, "
            "account_request_ceiling, account_decoded_byte_ceiling "
            "FROM rt.provider_request_ledger WHERE request_id=%s",
            (str(reservation.request_id),),
        )
    else:
        cursor.execute(
            "INSERT INTO rt.provider_request_ledger (request_id, account_alias, lane, "
            "cohort_id, window_id, stage_run_id, endpoint_class, request_sha256, attempt, "
            "state, reserved_at) VALUES ("
            "%s, %s, %s, %s, %s, %s, %s, %s, %s, 'RESERVED', %s) "
            "ON CONFLICT (request_id) DO NOTHING",
            (*identity[:9], reservation.reserved_at),
        )
        cursor.execute(
            "SELECT request_id, account_alias, lane, cohort_id, window_id, stage_run_id, "
            "endpoint_class, request_sha256, attempt "
            "FROM rt.provider_request_ledger WHERE request_id=%s",
            (str(reservation.request_id),),
        )
    stored = cursor.fetchone()
    expected = identity if coordinated else identity[:9]
    if stored is None or _normalized(stored) != expected:
        raise ValueError("provider request ledger identity conflict")
    return reservation.request_id


def _completion_identity(value: ProviderRequestCompletion) -> tuple[object, ...]:
    return (
        value.state.value,
        value.started_at,
        value.completed_at,
        value.http_status,
        value.outcome.value if value.outcome is not None else None,
        value.response_bytes,
        value.row_count,
        value.pages_fetched,
        value.requested_from,
        value.requested_to,
        value.price_basis.value,
        value.completeness.value,
        value.failure_code,
        str(value.payload_id) if value.payload_id is not None else None,
    )


def finish_provider_request_cursor(
    cursor,
    request_id: UUID,
    completion: ProviderRequestCompletion,
) -> None:
    identity = _completion_identity(completion)
    cursor.execute(
        "UPDATE rt.provider_request_ledger SET state=%s, started_at=%s, completed_at=%s, "
        "http_status=%s, outcome=%s, response_bytes=%s, row_count=%s, pages_fetched=%s, "
        "requested_from=%s, requested_to=%s, price_basis=%s, completeness=%s, "
        "failure_code=%s, payload_id=%s "
        "WHERE request_id=%s AND state IN ('RESERVED', 'AMBIGUOUS')",
        (*identity, str(request_id)),
    )
    if cursor.rowcount == 1:
        return
    cursor.execute(
        "SELECT state, started_at, completed_at, http_status, outcome, response_bytes, "
        "row_count, pages_fetched, requested_from, requested_to, price_basis, "
        "completeness, failure_code, payload_id FROM rt.provider_request_ledger "
        "WHERE request_id=%s",
        (str(request_id),),
    )
    stored = cursor.fetchone()
    if stored is None:
        raise ValueError("provider request is not reserved")
    if _normalized(stored) != identity:
        raise ValueError("provider request was already finished with a different result")


def record_provider_request_cursor(
    cursor,
    reservation: ProviderRequestReservation,
    completion: ProviderRequestCompletion,
) -> UUID:
    reserve_provider_request_cursor(cursor, reservation)
    finish_provider_request_cursor(cursor, reservation.request_id, completion)
    return reservation.request_id


def load_recorded_provider_request_cursor(
    cursor,
    request_id: UUID,
) -> RecordedProviderRequest | None:
    cursor.execute(
        "SELECT request_id,account_alias,lane,endpoint_class,request_sha256,"
        "reserved_at,attempt,cohort_id,window_id,stage_run_id,"
        "budget_scope_sha256,budget_session_date,authorization_reference,"
        "reserved_response_bytes,account_request_ceiling,"
        "account_decoded_byte_ceiling,state,completed_at,started_at,outcome,"
        "http_status,response_bytes,row_count,pages_fetched,requested_from,"
        "requested_to,price_basis,completeness,failure_code,payload_id "
        "FROM rt.provider_request_ledger WHERE request_id=%s",
        (str(request_id),),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    reservation = ProviderRequestReservation(
        request_id=UUID(str(row[0])),
        account_alias=row[1],
        lane=ProviderRequestLane(row[2]),
        endpoint_class=row[3],
        request_sha256=row[4],
        reserved_at=row[5],
        attempt=int(row[6]),
        cohort_id=UUID(str(row[7])) if row[7] is not None else None,
        window_id=UUID(str(row[8])) if row[8] is not None else None,
        stage_run_id=UUID(str(row[9])) if row[9] is not None else None,
        budget_scope_sha256=row[10],
        budget_session_date=row[11],
        authorization_reference=row[12],
        reserved_response_bytes=row[13],
        account_request_ceiling=row[14],
        account_decoded_byte_ceiling=row[15],
    )
    if row[16] == ProviderRequestState.RESERVED.value:
        return None
    completion = ProviderRequestCompletion(
        state=ProviderRequestState(row[16]),
        completed_at=row[17],
        started_at=row[18],
        outcome=ProviderRequestOutcome(row[19]) if row[19] is not None else None,
        http_status=row[20],
        response_bytes=row[21],
        row_count=row[22],
        pages_fetched=row[23],
        requested_from=row[24],
        requested_to=row[25],
        price_basis=PriceBasis(row[26]),
        completeness=RequestCompleteness(row[27]),
        failure_code=row[28],
        payload_id=UUID(str(row[29])) if row[29] is not None else None,
    )
    return RecordedProviderRequest(reservation, completion)


def mark_stale_provider_requests_ambiguous_cursor(cursor, *, reserved_before: datetime) -> int:
    cursor.execute(
        "UPDATE rt.provider_request_ledger SET state='AMBIGUOUS' "
        "WHERE state='RESERVED' AND reserved_at < %s",
        (_utc(reserved_before, "reserved_before"),),
    )
    return int(cursor.rowcount)


def require_complete_provider_request_cursor(cursor, request_id: UUID) -> None:
    """Reject a request whose result is partial, failed or unresolved (RT-03)."""
    cursor.execute(
        "SELECT state, outcome, completeness FROM rt.provider_request_ledger "
        "WHERE request_id=%s",
        (str(request_id),),
    )
    row = cursor.fetchone()
    if row is None:
        raise ValueError("provider request is not recorded in the ledger")
    state, outcome, completeness = row[0], row[1], row[2]
    if state != "COMPLETE" or outcome != "OK":
        raise ValueError("provider request did not complete successfully")
    if completeness == "INCOMPLETE":
        raise ValueError("provider request was incomplete and cannot be a source")


@dataclass(frozen=True, slots=True)
class AcquisitionWatermark:
    cohort_id: UUID
    instrument_id: int
    interval: str
    source_kind: str
    last_complete_bar_end: datetime | None
    overlap_policy: str
    terminal_state: WatermarkTerminalState = WatermarkTerminalState.COMPLETE
    reason_codes: tuple[str, ...] = ()
    request_id: UUID | None = None
    last_examined_bar_end: datetime | None = None

    def __post_init__(self) -> None:
        if type(self.instrument_id) is not int or self.instrument_id <= 0:
            raise ValueError("watermark instrument ID must be positive")
        if self.interval not in _WATERMARK_INTERVALS:
            raise ValueError("watermark interval is unsupported")
        if self.source_kind not in {"NATIVE_REST", "REALTIME_STREAM", "DERIVED", "RECONCILED"}:
            raise ValueError("watermark source kind is unsupported")
        if not isinstance(self.terminal_state, WatermarkTerminalState):
            raise TypeError("watermark terminal state is invalid")
        reasons = tuple(self.reason_codes)
        if self.terminal_state is not WatermarkTerminalState.COMPLETE and not reasons:
            raise ValueError("a NO_TRADE or GAP watermark requires reason codes")
        object.__setattr__(self, "reason_codes", reasons)
        object.__setattr__(
            self, "overlap_policy", _text(self.overlap_policy, "overlap_policy", 64)
        )
        completed = _optional_utc(
            self.last_complete_bar_end,
            "last_complete_bar_end",
        )
        examined = _optional_utc(
            self.last_examined_bar_end,
            "last_examined_bar_end",
        )
        if examined is None:
            if completed is None:
                raise ValueError(
                    "watermark requires a last examined or complete boundary"
                )
            examined = completed
        if completed is not None and completed > examined:
            raise ValueError(
                "last complete boundary cannot exceed last examined boundary"
            )
        if (
            self.terminal_state is WatermarkTerminalState.COMPLETE
            and completed != examined
        ):
            raise ValueError(
                "a COMPLETE watermark requires the examined boundary to be complete"
            )
        object.__setattr__(self, "last_complete_bar_end", completed)
        object.__setattr__(self, "last_examined_bar_end", examined)


def advance_acquisition_watermark_cursor(cursor, watermark: AcquisitionWatermark) -> bool:
    """Advance examined progress without treating a GAP as complete progress."""
    cursor.execute(
        "INSERT INTO rt.acquisition_watermarks (cohort_id, instrument_id, interval, "
        "source_kind, last_complete_bar_end, last_examined_bar_end, overlap_policy, "
        "terminal_state, reason_codes, request_id) VALUES ("
        "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (cohort_id, instrument_id, interval, source_kind) DO UPDATE SET "
        "last_complete_bar_end=CASE "
        "WHEN EXCLUDED.last_complete_bar_end IS NULL "
        "THEN rt.acquisition_watermarks.last_complete_bar_end "
        "WHEN rt.acquisition_watermarks.last_complete_bar_end IS NULL "
        "THEN EXCLUDED.last_complete_bar_end "
        "ELSE GREATEST(rt.acquisition_watermarks.last_complete_bar_end, "
        "EXCLUDED.last_complete_bar_end) END, "
        "last_examined_bar_end=EXCLUDED.last_examined_bar_end, "
        "overlap_policy=EXCLUDED.overlap_policy, terminal_state=EXCLUDED.terminal_state, "
        "reason_codes=EXCLUDED.reason_codes, request_id=EXCLUDED.request_id, "
        "updated_at=clock_timestamp() "
        "WHERE rt.acquisition_watermarks.last_examined_bar_end "
        "< EXCLUDED.last_examined_bar_end "
        "OR (rt.acquisition_watermarks.last_examined_bar_end "
        "= EXCLUDED.last_examined_bar_end "
        "AND rt.acquisition_watermarks.terminal_state = 'GAP' "
        "AND EXCLUDED.terminal_state = 'COMPLETE') "
        "RETURNING last_examined_bar_end",
        (
            str(watermark.cohort_id),
            watermark.instrument_id,
            watermark.interval,
            watermark.source_kind,
            watermark.last_complete_bar_end,
            watermark.last_examined_bar_end,
            watermark.overlap_policy,
            watermark.terminal_state.value,
            list(watermark.reason_codes),
            str(watermark.request_id) if watermark.request_id is not None else None,
        ),
    )
    return cursor.fetchone() is not None


def load_acquisition_watermark_cursor(
    cursor,
    *,
    cohort_id: UUID,
    instrument_id: int,
    interval: str,
    source_kind: str,
) -> AcquisitionWatermark | None:
    cursor.execute(
        "SELECT last_complete_bar_end, last_examined_bar_end, overlap_policy, "
        "terminal_state, reason_codes, request_id "
        "FROM rt.acquisition_watermarks WHERE cohort_id=%s AND "
        "instrument_id=%s AND interval=%s AND source_kind=%s",
        (str(cohort_id), instrument_id, interval, source_kind),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return AcquisitionWatermark(
        cohort_id=cohort_id,
        instrument_id=instrument_id,
        interval=interval,
        source_kind=source_kind,
        last_complete_bar_end=row[0],
        last_examined_bar_end=row[1],
        overlap_policy=row[2],
        terminal_state=WatermarkTerminalState(row[3]),
        reason_codes=tuple(row[4] or ()),
        request_id=UUID(str(row[5])) if row[5] is not None else None,
    )


def raw_payload_id(content: bytes) -> UUID:
    return uuid5(NAMESPACE_URL, f"realtime-raw-payload:{hashlib.sha256(content).hexdigest()}")


def persist_raw_payload_cursor(
    cursor,
    content: bytes,
    *,
    arrived_at: datetime,
    request_id: UUID | None = None,
    values_executor=execute_values,
) -> UUID:
    """Store identity-encoded content in daily partitions keyed by arrival time."""
    if not isinstance(content, (bytes, bytearray)) or not content:
        raise ValueError("raw payload content must be non-empty bytes")
    body = bytes(content)
    arrival = _utc(arrived_at, "arrived_at")
    payload_id = raw_payload_id(body)
    digest = hashlib.sha256(body).hexdigest()
    chunks = tuple(
        body[offset:offset + RAW_PAYLOAD_CHUNK_BYTES]
        for offset in range(0, len(body), RAW_PAYLOAD_CHUNK_BYTES)
    )
    cursor.execute(
        "INSERT INTO rt.raw_payloads (payload_id, arrived_at, content_sha256, byte_length, "
        "chunk_count, content_encoding, request_id) VALUES (%s, %s, %s, %s, %s, 'IDENTITY', %s) "
        "ON CONFLICT DO NOTHING",
        (
            str(payload_id), arrival, digest, len(body), len(chunks),
            str(request_id) if request_id is not None else None,
        ),
    )
    values_executor(
        cursor,
        "INSERT INTO rt.raw_payload_chunks (payload_id, arrived_at, chunk_no, body) "
        "VALUES %s ON CONFLICT DO NOTHING",
        [(str(payload_id), arrival, index, chunk) for index, chunk in enumerate(chunks)],
        page_size=RAW_PAYLOAD_CHUNK_PAGE_SIZE,
    )
    cursor.execute(
        "SELECT content_sha256, byte_length, chunk_count FROM rt.raw_payloads "
        "WHERE payload_id=%s AND arrived_at=%s",
        (str(payload_id), arrival),
    )
    header = cursor.fetchone()
    if header is None or tuple(header) != (digest, len(body), len(chunks)):
        raise ValueError("persisted raw payload header conflict")
    return payload_id


def load_raw_payload_cursor(cursor, payload_id: UUID) -> bytes:
    cursor.execute(
        "SELECT arrived_at, content_sha256, byte_length, chunk_count FROM rt.raw_payloads "
        "WHERE payload_id=%s ORDER BY arrived_at DESC LIMIT 1",
        (str(payload_id),),
    )
    header = cursor.fetchone()
    if header is None:
        raise ValueError("raw payload is not retained")
    arrived_at, digest, byte_length, chunk_count = header
    cursor.execute(
        "SELECT chunk_no, body FROM rt.raw_payload_chunks "
        "WHERE payload_id=%s AND arrived_at=%s ORDER BY chunk_no",
        (str(payload_id), arrived_at),
    )
    chunks = tuple(cursor.fetchall())
    if tuple(int(row[0]) for row in chunks) != tuple(range(chunk_count)):
        raise ValueError("raw payload chunks are incomplete")
    content = b"".join(bytes(row[1]) for row in chunks)
    if len(content) != byte_length or hashlib.sha256(content).hexdigest() != digest:
        raise ValueError("raw payload content does not match its header")
    return content
