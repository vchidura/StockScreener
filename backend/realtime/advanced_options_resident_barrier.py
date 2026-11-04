"""Durable completion barrier for the three Advanced Options resident owners."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from uuid import UUID

from psycopg2.extras import Json

from .contracts import canonical_sha256


class ResidentCompletionMember(str, Enum):
    INGRESS = "INGRESS"
    MARKET_STATE = "MARKET_STATE"
    FLOW = "FLOW"


class ResidentCompletionStatus(str, Enum):
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"


class ResidentCompletionBarrierState(str, Enum):
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"
    WAITING_INPUT = "WAITING_INPUT"


class ResidentCompletionDivergence(ValueError):
    """An immutable resident completion disagrees with the target identity."""


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _sha256(value: str, name: str) -> str:
    if len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


_ZERO_EFFECTS = {
    "dispatch": False,
    "delivery": False,
    "broker": False,
    "trading": False,
}


def _nonnegative_offset(evidence, name: str) -> int:
    value = evidence.get(name)
    if type(value) is not int or value < 0:
        raise ValueError(
            f"resident completion {name} is invalid"
        )
    return value


def _shard_ids(evidence) -> tuple[int, ...]:
    values = evidence.get("shard_ids")
    if not isinstance(values, list):
        raise ValueError("resident completion shard_ids is invalid")
    shard_ids = tuple(values)
    if (
        not shard_ids
        or any(type(value) is not int for value in shard_ids)
        or shard_ids != tuple(range(len(shard_ids)))
    ):
        raise ValueError("resident completion shard_ids is invalid")
    return shard_ids


def _validate_complete_evidence(
    completion: "ResidentCompletion",
) -> None:
    evidence = completion.evidence
    if (
        evidence.get("status") != ResidentCompletionStatus.COMPLETE.value
        or evidence.get("kind") != completion.member.value
        or evidence.get("effects") != _ZERO_EFFECTS
    ):
        raise ValueError("resident completion evidence scope changed")
    if completion.member is ResidentCompletionMember.INGRESS:
        _nonnegative_offset(evidence, "raw_end_offset")
        normalized = _nonnegative_offset(
            evidence,
            "normalized_end_offset",
        )
        if (
            normalized != completion.ending_offset
            or type(evidence.get("connection_attempts")) is not int
            or evidence["connection_attempts"] <= 0
        ):
            raise ValueError(
                "INGRESS resident completion evidence changed"
            )
    elif completion.member is ResidentCompletionMember.MARKET_STATE:
        normalized = _nonnegative_offset(
            evidence,
            "normalized_end_offset",
        )
        _shard_ids(evidence)
        if (
            normalized != completion.ending_offset
            or any(
                not isinstance(evidence.get(name), str)
                or len(evidence[name]) != 64
                or any(
                    character not in "0123456789abcdef"
                    for character in evidence[name]
                )
                for name in ("state_sha256", "shard_map_sha256")
            )
        ):
            raise ValueError(
                "MARKET_STATE resident completion evidence changed"
            )
    else:
        raw = _nonnegative_offset(evidence, "raw_end_offset")
        _nonnegative_offset(evidence, "normalized_end_offset")
        checkpoint = _nonnegative_offset(
            evidence,
            "checkpoint_end_offset",
        )
        _shard_ids(evidence)
        if (
            checkpoint != completion.ending_offset
            or checkpoint > raw
            or evidence.get("owner_epoch") != completion.owner_epoch
            or not isinstance(evidence.get("evidence_ref"), str)
            or not evidence["evidence_ref"].strip()
            or not isinstance(evidence.get("shard_map_sha256"), str)
            or len(evidence["shard_map_sha256"]) != 64
            or any(
                character not in "0123456789abcdef"
                for character in evidence["shard_map_sha256"]
            )
        ):
            raise ValueError(
                "FLOW resident completion evidence changed"
            )


@dataclass(frozen=True, slots=True)
class ResidentCompletion:
    target_window_id: UUID
    target_cohort_id: UUID
    member: ResidentCompletionMember
    journal_generation: UUID
    owner_epoch: int
    ending_offset: int
    evidence_sha256: str
    evidence: Mapping[str, object]
    status: ResidentCompletionStatus
    reason_codes: tuple[str, ...]
    completed_at: datetime

    def __post_init__(self) -> None:
        if (
            not isinstance(self.target_window_id, UUID)
            or not isinstance(self.target_cohort_id, UUID)
            or not isinstance(self.journal_generation, UUID)
            or min(
                self.target_window_id.int,
                self.target_cohort_id.int,
                self.journal_generation.int,
            )
            == 0
            or not isinstance(self.member, ResidentCompletionMember)
            or not isinstance(self.status, ResidentCompletionStatus)
            or type(self.owner_epoch) is not int
            or self.owner_epoch <= 0
            or type(self.ending_offset) is not int
            or self.ending_offset < 0
        ):
            raise ValueError("resident completion identity is invalid")
        reasons = tuple(reason.strip() for reason in self.reason_codes)
        if (
            any(not reason or len(reason) > 160 for reason in reasons)
            or (self.status is ResidentCompletionStatus.COMPLETE and reasons)
            or (self.status is ResidentCompletionStatus.FAILED and not reasons)
        ):
            raise ValueError("resident completion terminal reasons are invalid")
        object.__setattr__(
            self,
            "evidence_sha256",
            _sha256(self.evidence_sha256, "evidence_sha256"),
        )
        if (
            not isinstance(self.evidence, Mapping)
            or canonical_sha256(dict(self.evidence))
            != self.evidence_sha256
        ):
            raise ValueError("resident completion evidence identity changed")
        object.__setattr__(self, "evidence", dict(self.evidence))
        object.__setattr__(self, "reason_codes", reasons)
        if self.status is ResidentCompletionStatus.COMPLETE:
            _validate_complete_evidence(self)
        elif (
            self.evidence.get("status")
            != ResidentCompletionStatus.FAILED.value
            or self.evidence.get("kind") != self.member.value
            or tuple(self.evidence.get("reason_codes", ())) != reasons
            or self.evidence.get("effects") != _ZERO_EFFECTS
        ):
            raise ValueError(
                "failed resident completion evidence changed"
            )
        object.__setattr__(
            self,
            "completed_at",
            _utc(self.completed_at, "completed_at"),
        )


@dataclass(frozen=True, slots=True)
class ResidentCompletionBarrierVerdict:
    target_window_id: UUID
    target_cohort_id: UUID
    journal_generation: UUID
    state: ResidentCompletionBarrierState
    members: tuple[ResidentCompletion, ...]
    reason_codes: tuple[str, ...]
    evidence_sha256: str | None


@dataclass(frozen=True, slots=True)
class ResidentWindowTarget:
    target_window_id: UUID
    target_cohort_id: UUID
    journal_generation: UUID
    options_cutoff: datetime

    def __post_init__(self) -> None:
        if (
            not isinstance(self.target_window_id, UUID)
            or not isinstance(self.target_cohort_id, UUID)
            or not isinstance(self.journal_generation, UUID)
            or min(
                self.target_window_id.int,
                self.target_cohort_id.int,
                self.journal_generation.int,
            )
            == 0
        ):
            raise ValueError("resident window target identity is invalid")
        object.__setattr__(
            self,
            "options_cutoff",
            _utc(self.options_cutoff, "options_cutoff"),
        )


def load_ready_resident_window_targets_cursor(
    cursor,
    *,
    session_authorization_id: UUID,
    member: ResidentCompletionMember,
) -> tuple[ResidentWindowTarget, ...]:
    if (
        not isinstance(session_authorization_id, UUID)
        or session_authorization_id.int == 0
        or not isinstance(member, ResidentCompletionMember)
    ):
        raise ValueError("resident target lookup identity is invalid")
    cursor.execute(
        "SELECT cycle.target_window_id::text,"
        "cycle.target_cohort_id::text,session.journal_generation::text,"
        "cycle.options_cutoff "
        "FROM rt.advanced_options_window_cycle_authorizations AS cycle "
        "JOIN rt.advanced_options_session_acquisition_authorizations "
        "AS session ON session.authorization_id="
        "cycle.session_authorization_id "
        "JOIN rt.window_runs AS window_run "
        "ON window_run.window_id=cycle.target_window_id "
        "LEFT JOIN rt.advanced_options_resident_completions AS done "
        "ON done.target_window_id=cycle.target_window_id "
        "AND done.member=%s "
        "WHERE cycle.session_authorization_id=%s "
        "AND done.target_window_id IS NULL "
        "AND EXISTS ("
        "SELECT 1 FROM rt.stream_sessions AS stream "
        "WHERE stream.journal_generation=session.journal_generation "
        # A stopped stream cannot add anything, so its journal already holds
        # all it will ever hold at any later cutoff.
        "AND (stream.event_time_watermark>=cycle.options_cutoff "
        "OR (stream.status='STOPPED' "
        "AND stream.ended_at<=cycle.options_cutoff))"
        ") ORDER BY cycle.options_cutoff,cycle.target_window_id",
        (member.value, str(session_authorization_id)),
    )
    return tuple(
        ResidentWindowTarget(
            target_window_id=UUID(str(row[0])),
            target_cohort_id=UUID(str(row[1])),
            journal_generation=UUID(str(row[2])),
            options_cutoff=row[3],
        )
        for row in cursor.fetchall()
    )


def _row_completion(row) -> ResidentCompletion:
    return ResidentCompletion(
        target_window_id=UUID(str(row[0])),
        target_cohort_id=UUID(str(row[1])),
        member=ResidentCompletionMember(str(row[2])),
        journal_generation=UUID(str(row[3])),
        owner_epoch=int(row[4]),
        ending_offset=int(row[5]),
        evidence_sha256=str(row[6]),
        evidence=row[7],
        status=ResidentCompletionStatus(str(row[8])),
        reason_codes=tuple(row[9] or ()),
        completed_at=row[10],
    )


def record_resident_completion_cursor(
    cursor,
    completion: ResidentCompletion,
) -> bool:
    if not isinstance(completion, ResidentCompletion):
        raise TypeError("resident completion must be typed")
    cursor.execute(
        "INSERT INTO rt.advanced_options_resident_completions ("
        "target_window_id,target_cohort_id,member,journal_generation,"
        "owner_epoch,ending_offset,evidence_sha256,evidence_document,"
        "status,reason_codes,completed_at) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT DO NOTHING",
        (
            str(completion.target_window_id),
            str(completion.target_cohort_id),
            completion.member.value,
            str(completion.journal_generation),
            completion.owner_epoch,
            completion.ending_offset,
            completion.evidence_sha256,
            Json(dict(completion.evidence)),
            completion.status.value,
            list(completion.reason_codes),
            completion.completed_at,
        ),
    )
    inserted = cursor.rowcount == 1
    cursor.execute(
        "SELECT target_window_id,target_cohort_id,member,journal_generation,"
        "owner_epoch,ending_offset,evidence_sha256,evidence_document,"
        "status,reason_codes,"
        "completed_at FROM rt.advanced_options_resident_completions "
        "WHERE target_window_id=%s AND member=%s",
        (str(completion.target_window_id), completion.member.value),
    )
    row = cursor.fetchone()
    if row is None or _row_completion(row) != completion:
        raise ResidentCompletionDivergence(
            f"{completion.member.value} resident completion replay diverged"
        )
    return inserted


def evaluate_resident_completion_barrier_cursor(
    cursor,
    *,
    target_window_id: UUID,
    target_cohort_id: UUID,
    journal_generation: UUID,
) -> ResidentCompletionBarrierVerdict:
    if (
        not isinstance(target_window_id, UUID)
        or not isinstance(target_cohort_id, UUID)
        or not isinstance(journal_generation, UUID)
        or min(
            target_window_id.int,
            target_cohort_id.int,
            journal_generation.int,
        )
        == 0
    ):
        raise ValueError("resident completion barrier identity is invalid")
    cursor.execute(
        "SELECT target_window_id,target_cohort_id,member,journal_generation,"
        "owner_epoch,ending_offset,evidence_sha256,evidence_document,"
        "status,reason_codes,"
        "completed_at FROM rt.advanced_options_resident_completions "
        "WHERE target_window_id=%s ORDER BY member",
        (str(target_window_id),),
    )
    members = tuple(_row_completion(row) for row in cursor.fetchall())
    if any(
        member.target_cohort_id != target_cohort_id
        or member.journal_generation != journal_generation
        for member in members
    ):
        raise ResidentCompletionDivergence(
            "resident completion barrier target identity diverged"
        )
    failed = tuple(
        member
        for member in members
        if member.status is ResidentCompletionStatus.FAILED
    )
    if failed:
        reasons = tuple(
            f"RESIDENT_{member.member.value}_FAILED:{reason}"
            for member in failed
            for reason in member.reason_codes
        )
        return ResidentCompletionBarrierVerdict(
            target_window_id,
            target_cohort_id,
            journal_generation,
            ResidentCompletionBarrierState.FAILED,
            members,
            reasons,
            None,
        )
    present = {member.member for member in members}
    missing = tuple(
        member
        for member in ResidentCompletionMember
        if member not in present
    )
    if missing:
        return ResidentCompletionBarrierVerdict(
            target_window_id,
            target_cohort_id,
            journal_generation,
            ResidentCompletionBarrierState.WAITING_INPUT,
            members,
            tuple(
                f"RESIDENT_ROLE_NOT_COMPLETE:{member.value}"
                for member in missing
            ),
            None,
        )
    evidence_by_member = {
        member.member: member.evidence for member in members
    }
    ingress = evidence_by_member[ResidentCompletionMember.INGRESS]
    market_state = evidence_by_member[
        ResidentCompletionMember.MARKET_STATE
    ]
    flow = evidence_by_member[ResidentCompletionMember.FLOW]
    if (
        ingress["raw_end_offset"] != flow["raw_end_offset"]
        or ingress["raw_end_offset"] != flow["checkpoint_end_offset"]
        or ingress["normalized_end_offset"]
        != market_state["normalized_end_offset"]
        or flow["normalized_end_offset"]
        != market_state["normalized_end_offset"]
    ):
        raise ResidentCompletionDivergence(
            "resident completion offsets did not converge"
        )
    evidence_sha256 = canonical_sha256(
        tuple(
            (
                member.member.value,
                str(member.journal_generation),
                member.owner_epoch,
                member.ending_offset,
                member.evidence_sha256,
                member.completed_at.isoformat(),
            )
            for member in members
        )
    )
    return ResidentCompletionBarrierVerdict(
        target_window_id,
        target_cohort_id,
        journal_generation,
        ResidentCompletionBarrierState.COMPLETE,
        members,
        (),
        evidence_sha256,
    )


def resident_completion_stage_outcome(
    verdict: ResidentCompletionBarrierVerdict,
):
    from .stage_orchestrator import StageOutcome

    if not isinstance(verdict, ResidentCompletionBarrierVerdict):
        raise TypeError("resident completion verdict must be typed")
    if verdict.state is ResidentCompletionBarrierState.WAITING_INPUT:
        return StageOutcome.deferred(*verdict.reason_codes)
    if verdict.state is ResidentCompletionBarrierState.FAILED:
        return StageOutcome.failed(*verdict.reason_codes)
    if verdict.evidence_sha256 is None:
        raise ResidentCompletionDivergence(
            "complete resident barrier lacks evidence identity"
        )
    return StageOutcome.complete(evidence_ref=verdict.evidence_sha256)


def advanced_options_v3_session_status_cursor(
    cursor,
    *,
    session_authorization_id: UUID,
) -> dict[str, object]:
    if (
        not isinstance(session_authorization_id, UUID)
        or session_authorization_id.int == 0
    ):
        raise ValueError("V3 session status identity is invalid")
    cursor.execute(
        "SELECT cohort_id::text,session_date,journal_generation::text "
        "FROM rt.advanced_options_session_acquisition_authorizations "
        "WHERE authorization_id=%s",
        (str(session_authorization_id),),
    )
    session = cursor.fetchone()
    if session is None:
        raise ValueError("V3 session authority is unavailable")
    cohort_id = UUID(str(session[0]))
    journal_generation = UUID(str(session[2]))
    cursor.execute(
        "SELECT status,raw_committed_offset,normalized_committed_offset "
        "FROM rt.advanced_journal_registry "
        "WHERE journal_generation=%s",
        (str(journal_generation),),
    )
    registration = cursor.fetchone()
    cursor.execute(
        "SELECT consumer_name,max(committed_offset) "
        "FROM rt.consumer_checkpoints "
        "WHERE cohort_id=%s AND journal_generation=%s "
        "GROUP BY consumer_name",
        (str(cohort_id), str(journal_generation)),
    )
    checkpoints = {
        str(name): int(offset)
        for name, offset in cursor.fetchall()
    }
    cursor.execute(
        "SELECT cycle.target_window_id::text,window_run.window_key,"
        "cycle.equity_cutoff,cycle.options_cutoff,window_run.state "
        "FROM rt.advanced_options_window_cycle_authorizations AS cycle "
        "JOIN rt.window_runs AS window_run "
        "ON window_run.window_id=cycle.target_window_id "
        "WHERE cycle.session_authorization_id=%s "
        "ORDER BY cycle.options_cutoff,cycle.target_window_id",
        (str(session_authorization_id),),
    )
    windows = []
    for window_id, window_key, equity_cutoff, options_cutoff, state in (
        cursor.fetchall()
    ):
        cursor.execute(
            "SELECT member,status,ending_offset "
            "FROM rt.advanced_options_resident_completions "
            "WHERE target_window_id=%s ORDER BY member",
            (str(window_id),),
        )
        residents = [
            {
                "member": str(member),
                "status": str(status),
                "ending_offset": int(offset),
            }
            for member, status, offset in cursor.fetchall()
        ]
        cursor.execute(
            "SELECT stage_id,state,attempt FROM ("
            "SELECT DISTINCT ON (stage_id) stage_id,state,attempt "
            "FROM rt.stage_runs WHERE window_id=%s "
            "ORDER BY stage_id,attempt DESC"
            ") AS latest ORDER BY stage_id",
            (str(window_id),),
        )
        stages = [
            {
                "stage_id": str(stage_id),
                "state": str(stage_state),
                "attempt": int(attempt),
            }
            for stage_id, stage_state, attempt in cursor.fetchall()
        ]
        cursor.execute(
            "SELECT receipt_kind FROM rt.cycle_receipts "
            "WHERE window_id=%s ORDER BY receipt_kind",
            (str(window_id),),
        )
        receipts = [str(row[0]) for row in cursor.fetchall()]
        cursor.execute(
            "SELECT count(*) FROM rt.portal_releases AS release "
            "JOIN rt.cycle_receipts AS receipt "
            "ON receipt.receipt_id=release.receipt_id "
            "WHERE receipt.window_id=%s",
            (str(window_id),),
        )
        publication_count = int(cursor.fetchone()[0])
        windows.append({
            "target_window_id": str(window_id),
            "window_key": str(window_key),
            "equity_cutoff": equity_cutoff.isoformat(),
            "options_cutoff": options_cutoff.isoformat(),
            "state": str(state),
            "residents": residents,
            "stages": stages,
            "receipts": receipts,
            "publication_count": publication_count,
        })
    cursor.execute(
        "SELECT release_id::text,cutoff FROM rt.portal_release_current "
        "WHERE cohort_id=%s AND provider_tier='MASSIVE_ADVANCED'",
        (str(cohort_id),),
    )
    pointer = cursor.fetchone()
    raw_offset = 0 if registration is None else int(registration[1])
    normalized_offset = (
        0 if registration is None else int(registration[2])
    )
    market_offset = checkpoints.get("market-state-materializer-v1", 0)
    flow_offset = checkpoints.get("option_flow_stream_v1", 0)
    windows_complete = all(
        row["state"] in {
            "COMPLETE",
            "FAILED",
            "MISSED",
            "RETAINED_GAP",
            "UNAVAILABLE",
        }
        and len(row["residents"]) == len(ResidentCompletionMember)
        for row in windows
    )
    final_barrier = (
        registration is not None
        and str(registration[0]) == "DRAINED"
        and market_offset >= normalized_offset
        and flow_offset >= raw_offset
        and windows_complete
    )
    return {
        "schema": "advanced-options-v3-session-status-v1",
        "session_authorization_id": str(session_authorization_id),
        "cohort_id": str(cohort_id),
        "session_date": session[1].isoformat(),
        "journal_generation": str(journal_generation),
        "journal": {
            "status": None if registration is None else str(registration[0]),
            "raw_committed_offset": raw_offset,
            "normalized_committed_offset": normalized_offset,
        },
        "consumer_checkpoints": checkpoints,
        "windows": windows,
        "publication_pointer": (
            None
            if pointer is None
            else {
                "release_id": str(pointer[0]),
                "cutoff": pointer[1].isoformat(),
            }
        ),
        "final_barrier": {
            "state": "COMPLETE" if final_barrier else "WAITING_INPUT",
            "journal_drained": (
                registration is not None
                and str(registration[0]) == "DRAINED"
            ),
            "market_state_caught_up": (
                market_offset >= normalized_offset
            ),
            "flow_caught_up": flow_offset >= raw_offset,
            "windows_terminal": windows_complete,
        },
    }
