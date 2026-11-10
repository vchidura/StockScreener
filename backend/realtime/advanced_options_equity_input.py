"""Typed Equity-context requirements for Advanced Options lanes."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from enum import Enum
from uuid import UUID

from options.calendar import OptionExchangeCalendar
from psycopg2.extras import RealDictCursor

from .advanced_equity_control_plane import AdvancedEquityControlPlanePlan
from .advanced_options_control_plane import (
    AdvancedOptionsEquityInputBinding,
)
from .contracts import canonical_sha256
from .equity_bar_repository import KNOWN_BY_OBSERVED, load_known_bar_revisions_cursor
from .equity_history import equity_history_coordinates, load_equity_history_cursor
from .preflight import DetectorFamily


EQUITY_CONTEXT_REQUIREMENT_VERSION = (
    "advanced-options-equity-context-requirement-v1"
)
EQUITY_CONTEXT_POLICY_VERSION = "advanced-options-equity-context-policy-v1"


class AdvancedOptionsEquityContextSource(str, Enum):
    HISTORY = "H"
    LIVE_EQUITY = "E"


class AdvancedOptionsEquityContextMissingBehavior(str, Enum):
    BLOCK_LANE = "BLOCK_LANE"


@dataclass(frozen=True, slots=True)
class AdvancedOptionsEquityContextRequirement:
    lane: DetectorFamily
    source: AdvancedOptionsEquityContextSource
    interval: str
    adjustment_basis: str
    session_scope: str
    session_count: int
    current_session: bool
    final_required: bool
    availability_modes: tuple[str, ...]
    known_by_clock: str
    missing_behavior: AdvancedOptionsEquityContextMissingBehavior
    version: str = EQUITY_CONTEXT_REQUIREMENT_VERSION

    def __post_init__(self) -> None:
        modes = tuple(sorted(set(self.availability_modes)))
        if (
            self.version != EQUITY_CONTEXT_REQUIREMENT_VERSION
            or self.lane not in {
                DetectorFamily.O1,
                DetectorFamily.O3,
                DetectorFamily.OS1,
                DetectorFamily.OS2,
            }
            or self.adjustment_basis != "RAW_UNADJUSTED"
            or self.session_scope != "RTH"
            or type(self.session_count) is not int
            or self.session_count <= 0
            or not self.final_required
            or self.missing_behavior
            is not AdvancedOptionsEquityContextMissingBehavior.BLOCK_LANE
        ):
            raise ValueError("Advanced Options Equity-context requirement is invalid")
        live_requirement = (
            self.source is AdvancedOptionsEquityContextSource.LIVE_EQUITY
            and self.interval == "5m"
            and self.current_session
            and modes == ("LIVE_OBSERVED",)
            and self.known_by_clock == "E_CUTOFF"
            and (
                self.lane is DetectorFamily.O3
                and self.session_count == 3
                or self.lane is DetectorFamily.OS1
                and self.session_count == 2
                or self.lane is DetectorFamily.OS2
                and self.session_count == 3
            )
        )
        history_requirement = (
            self.lane in {DetectorFamily.OS1, DetectorFamily.OS2}
            and self.source is AdvancedOptionsEquityContextSource.HISTORY
            and self.interval == "1d"
            and self.session_count == 15
            and not self.current_session
            and modes == (
                "HISTORICAL_RECONSTRUCTED",
                "LIVE_OBSERVED",
            )
            and self.known_by_clock == "H_REFERENCE"
        )
        if not (live_requirement or history_requirement):
            raise ValueError("Advanced Options Equity-context requirement is invalid")
        object.__setattr__(self, "availability_modes", modes)

    @property
    def requirement_sha256(self) -> str:
        return canonical_sha256({
            "version": self.version,
            "lane": self.lane.value,
            "source": self.source.value,
            "interval": self.interval,
            "adjustment_basis": self.adjustment_basis,
            "session_scope": self.session_scope,
            "session_count": self.session_count,
            "current_session": self.current_session,
            "final_required": self.final_required,
            "availability_modes": self.availability_modes,
            "known_by_clock": self.known_by_clock,
            "missing_behavior": self.missing_behavior.value,
        })


@dataclass(frozen=True, slots=True)
class AdvancedOptionsEquityContextMember:
    ticker: str
    source_cohort_id: UUID
    bar_revision_id: UUID
    bar_end: datetime
    observed_at: datetime
    payload_sha256: str


@dataclass(frozen=True, slots=True)
class AdvancedOptionsLaneEquityContext:
    lane: DetectorFamily
    policy_version: str
    requirements_sha256: str
    membership_sha256: str
    available: bool
    reason_codes: tuple[str, ...]
    members: tuple[AdvancedOptionsEquityContextMember, ...]


def initial_advanced_options_equity_context_requirements(
) -> tuple[AdvancedOptionsEquityContextRequirement, ...]:
    return (
        AdvancedOptionsEquityContextRequirement(
            lane=DetectorFamily.O3,
            source=AdvancedOptionsEquityContextSource.LIVE_EQUITY,
            interval="5m",
            adjustment_basis="RAW_UNADJUSTED",
            session_scope="RTH",
            session_count=3,
            current_session=True,
            final_required=True,
            availability_modes=("LIVE_OBSERVED",),
            known_by_clock="E_CUTOFF",
            missing_behavior=(
                AdvancedOptionsEquityContextMissingBehavior.BLOCK_LANE
            ),
        ),
        *tuple(
            requirement
            for lane, intraday_count in (
                (DetectorFamily.OS1, 2),
                (DetectorFamily.OS2, 3),
            )
            for requirement in (
                AdvancedOptionsEquityContextRequirement(
                    lane=lane,
                    source=AdvancedOptionsEquityContextSource.HISTORY,
                    interval="1d",
                    adjustment_basis="RAW_UNADJUSTED",
                    session_scope="RTH",
                    session_count=15,
                    current_session=False,
                    final_required=True,
                    availability_modes=(
                        "HISTORICAL_RECONSTRUCTED",
                        "LIVE_OBSERVED",
                    ),
                    known_by_clock="H_REFERENCE",
                    missing_behavior=(
                        AdvancedOptionsEquityContextMissingBehavior.BLOCK_LANE
                    ),
                ),
                AdvancedOptionsEquityContextRequirement(
                    lane=lane,
                    source=AdvancedOptionsEquityContextSource.LIVE_EQUITY,
                    interval="5m",
                    adjustment_basis="RAW_UNADJUSTED",
                    session_scope="RTH",
                    session_count=intraday_count,
                    current_session=True,
                    final_required=True,
                    availability_modes=("LIVE_OBSERVED",),
                    known_by_clock="E_CUTOFF",
                    missing_behavior=(
                        AdvancedOptionsEquityContextMissingBehavior.BLOCK_LANE
                    ),
                ),
            )
        ),
    )


def advanced_options_equity_context_requirements_payload(
    requirements: tuple[AdvancedOptionsEquityContextRequirement, ...],
) -> dict[str, object]:
    if (
        not requirements
        or len({row.requirement_sha256 for row in requirements})
        != len(requirements)
    ):
        raise ValueError("Advanced Options Equity-context policy is invalid")
    return {
        "policy_version": EQUITY_CONTEXT_POLICY_VERSION,
        "requirements": tuple(
            {
                "requirement_sha256": row.requirement_sha256,
                "lane": row.lane.value,
                "source": row.source.value,
                "interval": row.interval,
                "adjustment_basis": row.adjustment_basis,
                "session_scope": row.session_scope,
                "session_count": row.session_count,
                "current_session": row.current_session,
                "final_required": row.final_required,
                "availability_modes": row.availability_modes,
                "known_by_clock": row.known_by_clock,
                "missing_behavior": row.missing_behavior.value,
                "version": row.version,
            }
            for row in sorted(
                requirements,
                key=lambda value: (
                    value.lane.value,
                    value.source.value,
                    value.interval,
                ),
            )
        ),
    }


def load_live_equity_context_rows_cursor(
    cursor,
    *,
    security_id: UUID,
    requirement: AdvancedOptionsEquityContextRequirement,
    equity_cutoff: datetime,
    known_by: datetime,
) -> tuple[dict, ...]:
    """The last `session_count` final bars of one requirement for one security, ending by the cutoff, as known by
    `known_by` (dict cursor). G102 A4: the current bars by instrument, whatever cohort wrote them. Nov 10 (review R2):
    through the central known-by read, so a bar corrected after the known-by time is read as it stood then instead of
    leaving the context incomplete."""
    cursor.execute(
        "SELECT instrument_id FROM rt.instruments "
        "WHERE security_id=%s AND contract_id IS NULL",
        (str(security_id),),
    )
    instrument_ids = tuple(int(row["instrument_id"]) for row in cursor.fetchall())
    cutoff_day = datetime.combine(equity_cutoff.date(), time(), timezone.utc)
    loaded = load_known_bar_revisions_cursor(
        cursor,
        instrument_ids=instrument_ids,
        interval=requirement.interval,
        basis=requirement.adjustment_basis,
        known_by=known_by,
        start=(
            cutoff_day
            if requirement.current_session
            else cutoff_day - timedelta(days=2 * requirement.session_count + 14)
        ),
        end=equity_cutoff,
        scopes=(requirement.session_scope,),
        clocks=KNOWN_BY_OBSERVED,
    )
    eligible = sorted(
        (
            row
            for rows in loaded.values()
            for row in rows
            if str(row["security_id"]) == str(security_id)
            and row["availability_mode"] in requirement.availability_modes
            and row["is_final"]
            and (
                row["session_date"] == equity_cutoff.date()
                if requirement.current_session
                else row["session_date"] < equity_cutoff.date()
            )
        ),
        key=lambda row: row["bar_end"],
    )
    return tuple(eligible[-requirement.session_count:])


def load_advanced_options_equity_context_cursor(
    cursor,
    *,
    equity_plan: AdvancedEquityControlPlanePlan,
    history_source_cohort_id: UUID,
    equity_cutoff,
    requirements: tuple[AdvancedOptionsEquityContextRequirement, ...],
    underlyings: tuple[str, ...],
    allow_shared_history_source: bool = False,
    equity_receipt_id: UUID | None = None,
) -> tuple[AdvancedOptionsLaneEquityContext, ...]:
    if not isinstance(equity_plan, AdvancedEquityControlPlanePlan):
        raise TypeError("Options Equity dependency plan is invalid")
    if (
        not isinstance(history_source_cohort_id, UUID)
        or history_source_cohort_id.int == 0
        or (
            history_source_cohort_id == equity_plan.cohort_id
            and not allow_shared_history_source
        )
    ):
        raise ValueError("Options H reference source is invalid")
    policy = advanced_options_equity_context_requirements_payload(requirements)
    security_ids = {
        ticker: security_id
        for ticker, security_id in equity_plan.security_ids
        if ticker in underlyings
    }
    if (
        set(security_ids) != set(underlyings)
        or set(underlyings) - set(equity_plan.universe.equity_tickers)
    ):
        raise ValueError("Options Equity dependency identities are incomplete")

    # A bar ending at the cutoff is always stored after it, so "observed by the cutoff"
    # could never hold for the bar this check requires, and O3, OS1 and OS2 could never
    # run (found Nov 5: 0 of 176 bars). The Equity window's own receipt fixes what it
    # accepted, which is also stable across retries.
    live_known_by = equity_cutoff
    if equity_receipt_id is not None:
        cursor.execute(
            "SELECT known_by FROM rt.cycle_receipts "
            "WHERE receipt_id=%s AND receipt_kind='INPUT_READY'",
            (str(equity_receipt_id),),
        )
        receipt = cursor.fetchone()
        if receipt is not None and receipt["known_by"] is not None:
            live_known_by = max(equity_cutoff, receipt["known_by"])
    contexts = []
    calendar = OptionExchangeCalendar("XNYS")
    requirements_by_lane: dict[
        DetectorFamily,
        list[AdvancedOptionsEquityContextRequirement],
    ] = {}
    for requirement in requirements:
        requirements_by_lane.setdefault(requirement.lane, []).append(
            requirement
        )
    for lane in (
        DetectorFamily.O1,
        DetectorFamily.O3,
        DetectorFamily.OS1,
        DetectorFamily.OS2,
    ):
        lane_requirements = tuple(requirements_by_lane.get(lane, ()))
        members = []
        reasons = []
        required_sources = (
            {AdvancedOptionsEquityContextSource.LIVE_EQUITY, AdvancedOptionsEquityContextSource.HISTORY}
            if lane in {DetectorFamily.OS1, DetectorFamily.OS2}
            else {AdvancedOptionsEquityContextSource.LIVE_EQUITY}
            if lane is DetectorFamily.O3 else set()
        )
        if not required_sources.issubset({row.source for row in lane_requirements}):
            reasons.append(f"{lane.value}_E_CONTEXT_REQUIREMENTS_INCOMPLETE")
        for requirement in lane_requirements:
            for ticker in underlyings:
                if requirement.source is AdvancedOptionsEquityContextSource.HISTORY:
                    sessions = calendar.trailing_sessions_before(
                        equity_cutoff.date(), requirement.session_count,
                    )
                    with cursor.connection.cursor(cursor_factory=RealDictCursor) as history_cursor:
                        history = load_equity_history_cursor(
                            history_cursor,
                            cohort_id=history_source_cohort_id,
                            security_id=security_ids[ticker],
                            ticker=ticker,
                            interval=requirement.interval,
                            adjusted=False,
                            sessions=sessions,
                            checked_at=equity_cutoff,
                        )
                    rows = history.rows
                    incomplete = (
                        tuple(row["session_date"] for row in rows) != sessions
                        or any(
                            row["availability_mode"] not in requirement.availability_modes
                            or (row["bar_start"], row["bar_end"])
                            != equity_history_coordinates(
                                calendar, requirement.interval, row["session_date"],
                            )[0]
                            for row in rows
                        )
                    )
                    if incomplete:
                        reasons.append(f"{lane.value}_{ticker}_E_CONTEXT_INCOMPLETE")
                        continue
                    members.extend(
                        AdvancedOptionsEquityContextMember(
                            ticker=ticker,
                            source_cohort_id=UUID(str(row["cohort_id"])),
                            bar_revision_id=UUID(str(row["bar_revision_id"])),
                            bar_end=row["bar_end"],
                            observed_at=row["observed_at"],
                            payload_sha256=str(row["payload_sha256"]),
                        )
                        for row in rows
                    )
                    continue
                rows = load_live_equity_context_rows_cursor(
                    cursor,
                    security_id=security_ids[ticker],
                    requirement=requirement,
                    equity_cutoff=equity_cutoff,
                    known_by=(
                        live_known_by
                        if requirement.source
                        is AdvancedOptionsEquityContextSource.LIVE_EQUITY
                        else equity_cutoff
                    ),
                )
                incomplete = len(rows) != requirement.session_count
                if requirement.current_session:
                    incomplete = incomplete or (
                        rows[-1]["bar_end"] != equity_cutoff
                        or any(
                            (later["bar_end"] - earlier["bar_end"])
                            .total_seconds()
                            != 300
                            for earlier, later in zip(rows, rows[1:])
                        )
                    )
                else:
                    incomplete = incomplete or (
                        len({row["bar_end"].date() for row in rows})
                        != requirement.session_count
                        or rows[-1]["bar_end"].date()
                        >= equity_cutoff.date()
                    )
                if incomplete:
                    reasons.append(
                        f"{lane.value}_{ticker}_E_CONTEXT_INCOMPLETE"
                    )
                    continue
                members.extend(
                    AdvancedOptionsEquityContextMember(
                        ticker=ticker,
                        source_cohort_id=(
                            equity_plan.cohort_id
                            if requirement.source
                            is AdvancedOptionsEquityContextSource.LIVE_EQUITY
                            else history_source_cohort_id
                        ),
                        bar_revision_id=UUID(str(row["bar_revision_id"])),
                        bar_end=row["bar_end"],
                        observed_at=row["observed_at"],
                        payload_sha256=str(row["payload_sha256"]),
                    )
                    for row in rows
                )
        requirement_identity = tuple(
            row.requirement_sha256 for row in lane_requirements
        )
        membership_identity = tuple(
            (
                row.ticker,
                str(row.source_cohort_id),
                str(row.bar_revision_id),
                row.bar_end,
                row.observed_at,
                row.payload_sha256,
            )
            for row in members
        )
        contexts.append(AdvancedOptionsLaneEquityContext(
            lane=lane,
            policy_version=EQUITY_CONTEXT_POLICY_VERSION,
            requirements_sha256=canonical_sha256(requirement_identity),
            membership_sha256=canonical_sha256(membership_identity),
            available=not reasons,
            reason_codes=tuple(sorted(set(reasons))),
            members=tuple(members),
        ))
    return tuple(contexts)


def validate_advanced_options_equity_dependencies_cursor(
    cursor,
    *,
    equity_plan: AdvancedEquityControlPlanePlan,
    equity_input: AdvancedOptionsEquityInputBinding,
    history_source_cohort_id,
    underlyings: tuple[str, ...],
    requirements: tuple[AdvancedOptionsEquityContextRequirement, ...] | None = None,
    allow_shared_history_source: bool = False,
) -> dict[str, object]:
    if not isinstance(equity_input, AdvancedOptionsEquityInputBinding):
        raise TypeError("Options Equity input binding is invalid")
    if equity_plan.cohort_id != equity_input.source_cohort_id:
        raise ValueError("Options Equity dependency membership changed")
    selected = (
        initial_advanced_options_equity_context_requirements()
        if requirements is None
        else requirements
    )
    contexts = load_advanced_options_equity_context_cursor(
        cursor,
        equity_plan=equity_plan,
        history_source_cohort_id=history_source_cohort_id,
        equity_cutoff=equity_input.equity_cutoff,
        requirements=selected,
        underlyings=underlyings,
        allow_shared_history_source=allow_shared_history_source,
        equity_receipt_id=equity_input.equity_receipt_id,
    )
    lane_context = {
        row.lane.value: {
            "status": "AVAILABLE" if row.available else "BLOCKED",
            "reason_codes": row.reason_codes,
            "requirements_sha256": row.requirements_sha256,
            "membership_sha256": row.membership_sha256,
            "member_count": len(row.members),
        }
        for row in contexts
    }
    return {
        "status": "READY",
        "history_source_cohort_id": str(history_source_cohort_id),
        "equity_source_cohort_id": str(equity_input.source_cohort_id),
        "equity_cutoff_utc": equity_input.equity_cutoff.isoformat(),
        "policy": advanced_options_equity_context_requirements_payload(
            selected
        ),
        "lane_context": lane_context,
        "context_sha256": canonical_sha256(lane_context),
    }
