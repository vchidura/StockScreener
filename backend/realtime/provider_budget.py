"""Durable V2 provider request and decoded-byte admission."""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from uuid import UUID

from .acquisition_ledger import (
    ProviderRequestLane,
    ProviderRequestReservation,
    reserve_provider_request_cursor,
)
from .contracts import canonical_sha256


class ProviderBudgetExceeded(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ProviderBudgetScope:
    account_alias: str
    session_date: date
    authorization_reference: str
    provider_authorization_reference: str
    expected_cost: str
    request_ceiling: int
    decoded_byte_ceiling: int
    lanes: frozenset[ProviderRequestLane]
    authorized_until: datetime
    policy_version: str = "v2-provider-budget-v1"

    def __post_init__(self) -> None:
        for name in (
            "account_alias",
            "authorization_reference",
            "provider_authorization_reference",
            "expected_cost",
            "policy_version",
        ):
            value = getattr(self, name).strip()
            if not value:
                raise ValueError(f"{name} cannot be blank")
            object.__setattr__(self, name, value)
        if not isinstance(self.session_date, date):
            raise TypeError("session_date must be a date")
        if (
            self.authorized_until.tzinfo is None
            or self.authorized_until.utcoffset() is None
        ):
            raise ValueError("authorized_until must be timezone-aware")
        object.__setattr__(
            self,
            "authorized_until",
            self.authorized_until.astimezone(timezone.utc),
        )
        if (
            type(self.request_ceiling) is not int
            or self.request_ceiling <= 0
            or type(self.decoded_byte_ceiling) is not int
            or self.decoded_byte_ceiling <= 0
            or not self.lanes
            or any(
                not isinstance(lane, ProviderRequestLane)
                for lane in self.lanes
            )
        ):
            raise ValueError("provider budget limits or lanes are invalid")

    @property
    def sha256(self) -> str:
        return canonical_sha256({
            "policy_version": self.policy_version,
            "account_alias": self.account_alias,
            "session_date": self.session_date,
            "authorization_reference": self.authorization_reference,
            "provider_authorization_reference": (
                self.provider_authorization_reference
            ),
            "expected_cost": self.expected_cost,
            "request_ceiling": self.request_ceiling,
            "decoded_byte_ceiling": self.decoded_byte_ceiling,
            "lanes": tuple(sorted(lane.value for lane in self.lanes)),
            "authorized_until": self.authorized_until,
        })

    def bind(
        self,
        reservation: ProviderRequestReservation,
        *,
        reserved_response_bytes: int,
    ) -> ProviderRequestReservation:
        if reservation.account_alias != self.account_alias:
            raise ValueError("request account does not match its budget scope")
        if reservation.lane not in self.lanes:
            raise ValueError("request lane is outside its budget scope")
        return replace(
            reservation,
            budget_scope_sha256=self.sha256,
            budget_session_date=self.session_date,
            authorization_reference=self.authorization_reference,
            reserved_response_bytes=reserved_response_bytes,
            account_request_ceiling=self.request_ceiling,
            account_decoded_byte_ceiling=self.decoded_byte_ceiling,
        )


@dataclass(frozen=True, slots=True)
class ProviderBudgetStatus:
    account_alias: str
    session_date: date
    request_ceiling: int
    decoded_byte_ceiling: int
    requests_reserved: int
    bytes_reserved: int
    reserved: int
    complete: int
    failed: int
    ambiguous: int

    @property
    def requests_remaining(self) -> int:
        return self.request_ceiling - self.requests_reserved

    @property
    def bytes_remaining(self) -> int:
        return self.decoded_byte_ceiling - self.bytes_reserved


@dataclass(frozen=True, slots=True)
class ProviderBudgetAdmission:
    request_id: UUID
    newly_reserved: bool
    state: str
    status: ProviderBudgetStatus


def _budget_status_cursor(
    cursor,
    *,
    account_alias: str,
    session_date: date,
    expected_request_ceiling: int,
    expected_byte_ceiling: int,
) -> ProviderBudgetStatus:
    cursor.execute(
        "SELECT COUNT(*), COALESCE(SUM(reserved_response_bytes),0), "
        "COUNT(*) FILTER (WHERE state='RESERVED'), "
        "COUNT(*) FILTER (WHERE state='COMPLETE'), "
        "COUNT(*) FILTER (WHERE state='FAILED'), "
        "COUNT(*) FILTER (WHERE state='AMBIGUOUS'), "
        "MIN(account_request_ceiling), MAX(account_request_ceiling), "
        "MIN(account_decoded_byte_ceiling), MAX(account_decoded_byte_ceiling) "
        "FROM rt.provider_request_ledger "
        "WHERE account_alias=%s AND budget_session_date=%s "
        "AND budget_scope_sha256 IS NOT NULL",
        (account_alias, session_date),
    )
    (
        requests_reserved,
        bytes_reserved,
        reserved,
        complete,
        failed,
        ambiguous,
        minimum_requests,
        maximum_requests,
        minimum_bytes,
        maximum_bytes,
    ) = cursor.fetchone()
    if requests_reserved and (
        minimum_requests != expected_request_ceiling
        or maximum_requests != expected_request_ceiling
        or minimum_bytes != expected_byte_ceiling
        or maximum_bytes != expected_byte_ceiling
    ):
        raise ValueError(
            "provider budget ceilings conflict for this account and session"
        )
    return ProviderBudgetStatus(
        account_alias,
        session_date,
        expected_request_ceiling,
        expected_byte_ceiling,
        int(requests_reserved),
        int(bytes_reserved),
        int(reserved),
        int(complete),
        int(failed),
        int(ambiguous),
    )


def provider_budget_status_cursor(
    cursor,
    scope: ProviderBudgetScope,
) -> ProviderBudgetStatus:
    return _budget_status_cursor(
        cursor,
        account_alias=scope.account_alias,
        session_date=scope.session_date,
        expected_request_ceiling=scope.request_ceiling,
        expected_byte_ceiling=scope.decoded_byte_ceiling,
    )


def reserve_coordinated_provider_request_cursor(
    cursor,
    scope: ProviderBudgetScope,
    reservation: ProviderRequestReservation,
    *,
    reserved_response_bytes: int,
) -> ProviderBudgetAdmission:
    coordinated = scope.bind(
        reservation,
        reserved_response_bytes=reserved_response_bytes,
    )
    cursor.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
        (
            "v2-provider-budget:"
            f"{scope.account_alias}:{scope.session_date.isoformat()}",
        ),
    )
    cursor.execute(
        "SELECT state FROM rt.provider_request_ledger WHERE request_id=%s",
        (str(coordinated.request_id),),
    )
    existing = cursor.fetchone()
    if existing is not None:
        reserve_provider_request_cursor(cursor, coordinated)
        return ProviderBudgetAdmission(
            coordinated.request_id,
            False,
            str(existing[0]),
            provider_budget_status_cursor(cursor, scope),
        )
    status = provider_budget_status_cursor(cursor, scope)
    if status.requests_reserved + 1 > scope.request_ceiling:
        raise ProviderBudgetExceeded(
            "provider account request ceiling is exhausted"
        )
    if (
        status.bytes_reserved + coordinated.reserved_response_bytes
        > scope.decoded_byte_ceiling
    ):
        raise ProviderBudgetExceeded(
            "provider account decoded-byte ceiling is exhausted"
        )
    reserve_provider_request_cursor(cursor, coordinated)
    return ProviderBudgetAdmission(
        coordinated.request_id,
        True,
        "RESERVED",
        ProviderBudgetStatus(
            status.account_alias,
            status.session_date,
            status.request_ceiling,
            status.decoded_byte_ceiling,
            status.requests_reserved + 1,
            status.bytes_reserved + coordinated.reserved_response_bytes,
            status.reserved + 1,
            status.complete,
            status.failed,
            status.ambiguous,
        ),
    )
