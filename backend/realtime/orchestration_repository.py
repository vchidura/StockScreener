"""Orchestration state for the V2 stage orchestrator.

The repository port (OrchestrationRepository) is what the orchestrator depends on; the Postgres
implementation writes rt.window_runs, rt.stage_runs, rt.cycle_receipts, rt.stage_readiness_current
and rt.release_current as the runtime role. Every write after a claim is fenced by the
window owner epoch, so a stale owner cannot change a window another owner has taken over.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Protocol
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from .database_access_v2 import V2ConnectionProvider
from .stage_graph import StageDefinition

TERMINAL_WINDOW_STATES = frozenset({"COMPLETE", "FAILED", "MISSED", "RETAINED_GAP"})
OWNER_LOST = "OWNER_LOST"
EQUITY_WINDOW_BUILD_ID = "v2-10-developer-window-v1"
OPTIONS_INPUT_READY = "OPTIONS_INPUT_READY"
OPTIONS_CYCLE = "OPTIONS_CYCLE"
OPTIONS_PORTAL_READY = "OPTIONS_PORTAL_READY"
OPTIONS_RECEIPT_KINDS = frozenset({
    OPTIONS_INPUT_READY,
    OPTIONS_CYCLE,
    OPTIONS_PORTAL_READY,
})


def equity_window_recovery_cursor(cursor, cohort_id: UUID, cutoff: datetime) -> dict:
    """Reuse only receipts sealed for the same window, cohort, universe and build."""
    cursor.execute(
        "SELECT window_run.window_key, window_run.state, window_run.provider_tier, window_run.updated_at, "
        "receipt.receipt_id, receipt.cohort_id, receipt.cutoff, receipt.universe_sha256, "
        "receipt.input_sha256, receipt.build_id, receipt.reference_generation, "
        "manifest.universe_sha256, plan.stage_catalog_version, plan.cohort_id, "
        "window_run.deadline "
        "FROM rt.window_runs AS window_run "
        "JOIN rt.cohorts AS cohort ON cohort.cohort_id=window_run.cohort_id "
        "JOIN rt.subscription_manifests AS manifest USING (manifest_id) "
        "JOIN rt.service_plans AS plan ON plan.plan_id=window_run.plan_id "
        "LEFT JOIN rt.cycle_receipts AS receipt "
        "ON receipt.window_id=window_run.window_id AND receipt.receipt_kind='INPUT_READY' "
        "WHERE window_run.cohort_id=%s AND window_run.cutoff=%s "
        "ORDER BY window_run.updated_at DESC, window_run.window_key",
        (str(cohort_id), cutoff),
    )
    rows = tuple(cursor.fetchall())
    complete = False
    receipt_specs = set()
    input_known_bys = set()
    for row in rows:
        if row[4] is None:
            continue
        if (
            row[2] != "MASSIVE_DEVELOPER"
            or str(row[5]) != str(cohort_id)
            or row[6] != cutoff
            or row[7] != row[11]
            or row[9] != EQUITY_WINDOW_BUILD_ID
            or row[10] < 1
            or row[12] != "stages_v2"
            or str(row[13]) != str(cohort_id)
            or len(row[8]) != 64
            or any(character not in "0123456789abcdef" for character in row[8])
        ):
            raise ValueError("durable INPUT_READY receipt does not match the recovery scope")
        receipt_specs.add(CycleReceiptSpec(
            "INPUT_READY",
            row[6],
            int(row[10]),
            row[7],
            row[8],
            row[9],
        ))
        if "-receipt-retry" not in row[0]:
            input_known_bys.add(row[14])
        complete = complete or row[1] == "COMPLETE"
    if len(receipt_specs) > 1 or len(input_known_bys) > 1:
        raise ValueError("durable INPUT_READY receipts disagree on the sealed recovery input")
    has_receipt = bool(receipt_specs)
    if has_receipt and not input_known_bys:
        raise ValueError("durable INPUT_READY receipt lacks its original visibility clock")
    prefix = "-receipt-retry" if has_receipt else "-acquisition-retry"
    keys = {row[0] for row in rows}
    attempt = 1
    while cutoff.isoformat() + prefix + str(attempt) in keys:
        attempt += 1
    failed_at = max(
        (row[3] for row in rows if row[1] in {"FAILED", "MISSED", "RETAINED_GAP"}),
        default=None,
    )
    return {
        "complete": complete,
        "has_input_receipt": has_receipt,
        "input_receipt": next(iter(receipt_specs), None),
        "input_known_by": next(iter(input_known_bys), None),
        "retry_suffix": prefix + str(attempt) if rows else "",
        "retry_not_before": failed_at + timedelta(minutes=5) if failed_at else None,
    }


class LeaseLost(RuntimeError):
    """The window was claimed by another owner (or finished) since this owner claimed it."""


@dataclass(frozen=True, slots=True)
class ServicePlan:
    plan_id: UUID
    cohort_id: UUID
    session_date: date
    stage_catalog_version: str
    enabled_stages: frozenset[str]
    config_sha256: str
    deadline: datetime


@dataclass(frozen=True, slots=True)
class WindowSpec:
    cohort_id: UUID
    provider_tier: str
    window_key: str
    release_at: datetime
    cutoff: datetime
    deadline: datetime

    @property
    def window_id(self) -> UUID:
        return uuid5(NAMESPACE_URL, f"stockscreener:window:{self.cohort_id}:{self.window_key}")


@dataclass(frozen=True, slots=True)
class WindowRecord:
    window_id: UUID
    plan_id: UUID
    cohort_id: UUID
    provider_tier: str
    window_key: str
    release_at: datetime
    cutoff: datetime
    deadline: datetime
    state: str


@dataclass(frozen=True, slots=True)
class WindowClaim:
    window: WindowRecord
    owner_id: UUID
    epoch: int
    lease_until: datetime


@dataclass(frozen=True, slots=True)
class StageRunRecord:
    stage_run_id: UUID
    window_id: UUID
    stage_id: str
    stage_version: str
    attempt: int
    state: str
    blocked_by_stage: str | None
    reason_codes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class CycleReceiptSpec:
    receipt_kind: str
    cutoff: datetime
    reference_generation: int
    universe_sha256: str
    input_sha256: str
    build_id: str
    advance_release: bool = False


def build_options_receipt_spec(
    *,
    receipt_kind: str,
    cutoff: datetime,
    reference_generation: int,
    universe_sha256: str,
    input_sha256: str,
    build_id: str,
) -> CycleReceiptSpec:
    if receipt_kind not in OPTIONS_RECEIPT_KINDS:
        raise ValueError("Options receipt kind is not product-scoped")
    if cutoff.tzinfo is None or cutoff.utcoffset() is None:
        raise ValueError("Options receipt cutoff must be timezone-aware")
    if type(reference_generation) is not int or reference_generation <= 0:
        raise ValueError("Options receipt reference generation must be positive")
    for name, value in (
        ("universe_sha256", universe_sha256),
        ("input_sha256", input_sha256),
    ):
        if len(value) != 64 or any(
            character not in "0123456789abcdef" for character in value
        ):
            raise ValueError(f"Options receipt {name} must be a SHA-256 digest")
    if not build_id.strip():
        raise ValueError("Options receipt build_id cannot be blank")
    return CycleReceiptSpec(
        receipt_kind=receipt_kind,
        cutoff=cutoff,
        reference_generation=reference_generation,
        universe_sha256=universe_sha256,
        input_sha256=input_sha256,
        build_id=build_id.strip(),
        advance_release=False,
    )


class OrchestrationRepository(Protocol):
    def load_catalog(self, version: str) -> Mapping[str, StageDefinition]: ...

    def load_plan(self, plan_id: UUID) -> ServicePlan: ...

    def ensure_window(self, plan_id: UUID, spec: WindowSpec) -> WindowRecord: ...

    def open_windows(self, cohort_id: UUID) -> Sequence[WindowRecord]: ...

    def claim_window(self, window_id: UUID, owner_id: UUID, now: datetime,
                     lease_seconds: int) -> WindowClaim | None: ...

    def renew_lease(self, claim: WindowClaim, now: datetime, lease_seconds: int) -> bool: ...

    def stage_states(self, window_id: UUID) -> Mapping[str, StageRunRecord]: ...

    def recover_orphans(self, claim: WindowClaim, now: datetime) -> Sequence[StageRunRecord]: ...

    def timeout_running_stages(self, claim: WindowClaim, stage_ids: Sequence[str],
                               now: datetime) -> Sequence[StageRunRecord]: ...

    def start_stage(self, claim: WindowClaim, stage: StageDefinition, version: str, attempt: int,
                    now: datetime) -> StageRunRecord: ...

    def finish_stage(self, claim: WindowClaim, run: StageRunRecord, state: str, reason_codes: Sequence[str],
                     receipt_id: UUID | None, now: datetime) -> None: ...

    def record_blocked(self, claim: WindowClaim, stage_id: str, version: str, blocked_by: str,
                       blocker_state: str, now: datetime) -> StageRunRecord: ...

    def record_missed(self, claim: WindowClaim, stage_id: str, version: str, reason: str,
                      now: datetime) -> StageRunRecord: ...

    def record_receipt(self, claim: WindowClaim, spec: CycleReceiptSpec, run: StageRunRecord) -> UUID: ...

    def advance_release(self, claim: WindowClaim, receipt_id: UUID, cutoff: datetime, now: datetime) -> bool: ...

    def readiness(self, cohort_id: UUID) -> Mapping[str, str]: ...

    def upsert_readiness(self, cohort_id: UUID, stage_id: str, version: str, status: str,
                         reason_codes: Sequence[str], evidence_ref: str | None, now: datetime) -> None: ...

    def finish_window(self, claim: WindowClaim, state: str, now: datetime) -> None: ...

    def release_window(self, claim: WindowClaim, now: datetime) -> None: ...


_WINDOW_COLUMNS = "window_id, plan_id, cohort_id, provider_tier, window_key, release_at, cutoff, deadline, state"
_RUN_COLUMNS = ("stage_run_id, window_id, stage_id, stage_version, attempt, state, blocked_by_stage, "
                "reason_codes")


def _window(row) -> WindowRecord:
    return WindowRecord(UUID(str(row[0])), UUID(str(row[1])), UUID(str(row[2])), row[3], row[4], row[5], row[6],
                        row[7], row[8])


def _run(row) -> StageRunRecord:
    return StageRunRecord(UUID(str(row[0])), UUID(str(row[1])), row[2], row[3], row[4], row[5], row[6],
                          tuple(row[7] or ()))


class PostgresOrchestrationRepository:
    def __init__(self, provider: V2ConnectionProvider) -> None:
        self._provider = provider

    def _transaction(self, *, read_only: bool = False):
        return self._provider.transaction(read_only=read_only)

    @staticmethod
    def _fence(cursor, claim: WindowClaim) -> None:
        cursor.execute(
            "SELECT 1 FROM rt.window_runs WHERE window_id = %s AND owner_id = %s AND owner_epoch = %s "
            "AND state = 'RUNNING' FOR SHARE", (str(claim.window.window_id), str(claim.owner_id), claim.epoch))
        if cursor.fetchone() is None:
            raise LeaseLost(f"window {claim.window.window_key} is no longer owned at epoch {claim.epoch}")

    def create_plan(self, cohort_id: UUID, session_date: date, enabled_stages: Sequence[str], *,
                    config_sha256: str, deadline: datetime, scope_reference: str,
                    catalog_version: str = "stages_v1") -> UUID:
        """Insert (idempotently) the approved scope of one resident run and return its plan id."""
        with self._transaction() as unit:
            unit.cursor.execute(
                "INSERT INTO rt.service_plans (plan_id, cohort_id, session_date, stage_catalog_version, "
                "enabled_stages, config_sha256, deadline, scope_reference) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (cohort_id, session_date, config_sha256) DO NOTHING",
                (str(uuid4()), str(cohort_id), session_date, catalog_version, sorted(set(enabled_stages)),
                 config_sha256, deadline, scope_reference))
            unit.cursor.execute(
                "SELECT plan_id, enabled_stages, stage_catalog_version FROM rt.service_plans "
                "WHERE cohort_id = %s AND session_date = %s AND config_sha256 = %s",
                (str(cohort_id), session_date, config_sha256))
            plan_id, stored, version = unit.cursor.fetchone()
        if sorted(stored) != sorted(set(enabled_stages)) or version != catalog_version:
            raise ValueError("a service plan with this configuration hash already exists with a different scope")
        return UUID(str(plan_id))

    def load_catalog(self, version: str) -> Mapping[str, StageDefinition]:
        with self._transaction(read_only=True) as unit:
            unit.cursor.execute(
                "SELECT stage_id, domain, prerequisites, execution_lane, schedule_kind, gates_strategy, "
                "deadline_offset_seconds "
                "FROM rt.stage_catalog WHERE version = %s ORDER BY stage_id", (version,))
            rows = unit.cursor.fetchall()
        return {
            row[0]: StageDefinition(
                row[0], row[1], tuple(row[2]), row[3], row[4], bool(row[5]), row[6],
            )
            for row in rows
        }

    def load_plan(self, plan_id: UUID) -> ServicePlan:
        with self._transaction(read_only=True) as unit:
            unit.cursor.execute(
                "SELECT plan_id, cohort_id, session_date, stage_catalog_version, enabled_stages, config_sha256, "
                "deadline FROM rt.service_plans WHERE plan_id = %s", (str(plan_id),))
            row = unit.cursor.fetchone()
        if row is None:
            raise LookupError(f"service plan {plan_id} does not exist")
        return ServicePlan(UUID(str(row[0])), UUID(str(row[1])), row[2], row[3], frozenset(row[4]), row[5], row[6])

    def ensure_window(self, plan_id: UUID, spec: WindowSpec) -> WindowRecord:
        with self._transaction() as unit:
            unit.cursor.execute(
                "INSERT INTO rt.window_runs (window_id, plan_id, cohort_id, provider_tier, window_key, release_at, "
                "cutoff, deadline) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (cohort_id, window_key) "
                "DO NOTHING", (str(spec.window_id), str(plan_id), str(spec.cohort_id), spec.provider_tier,
                               spec.window_key, spec.release_at, spec.cutoff, spec.deadline))
            unit.cursor.execute(f"SELECT {_WINDOW_COLUMNS} FROM rt.window_runs WHERE cohort_id = %s "
                                "AND window_key = %s", (str(spec.cohort_id), spec.window_key))
            record = _window(unit.cursor.fetchone())
        if (record.plan_id, record.provider_tier, record.release_at, record.cutoff, record.deadline) != (
                plan_id, spec.provider_tier, spec.release_at, spec.cutoff, spec.deadline):
            raise ValueError(f"window {spec.window_key} already exists with a different identity")
        return record

    def open_windows(self, cohort_id: UUID) -> Sequence[WindowRecord]:
        with self._transaction(read_only=True) as unit:
            unit.cursor.execute(
                f"SELECT {_WINDOW_COLUMNS} FROM rt.window_runs WHERE cohort_id = %s "
                "AND state IN ('PENDING', 'RUNNING') ORDER BY release_at", (str(cohort_id),))
            return [_window(row) for row in unit.cursor.fetchall()]

    def claim_window(self, window_id: UUID, owner_id: UUID, now: datetime,
                     lease_seconds: int) -> WindowClaim | None:
        with self._transaction() as unit:
            unit.cursor.execute(
                f"SELECT {_WINDOW_COLUMNS}, owner_id, owner_epoch, lease_until FROM rt.window_runs "
                "WHERE window_id = %s FOR UPDATE", (str(window_id),))
            row = unit.cursor.fetchone()
            if row is None:
                raise LookupError(f"window {window_id} does not exist")
            window, holder, epoch, lease_until = _window(row[:9]), row[9], row[10], row[11]
            if window.state in TERMINAL_WINDOW_STATES:
                return None
            if (window.state == "RUNNING" and lease_until is not None and lease_until > now
                    and str(holder) != str(owner_id)):
                return None
            new_lease = now + timedelta(seconds=lease_seconds)
            unit.cursor.execute(
                "UPDATE rt.window_runs SET state = 'RUNNING', owner_id = %s, owner_epoch = owner_epoch + 1, "
                "lease_until = %s, updated_at = %s WHERE window_id = %s RETURNING owner_epoch",
                (str(owner_id), new_lease, now, str(window_id)))
            new_epoch = unit.cursor.fetchone()[0]
        return WindowClaim(window, owner_id, new_epoch, new_lease)

    def renew_lease(self, claim: WindowClaim, now: datetime, lease_seconds: int) -> bool:
        with self._transaction() as unit:
            unit.cursor.execute(
                "UPDATE rt.window_runs SET lease_until = %s, updated_at = %s WHERE window_id = %s "
                "AND owner_id = %s AND owner_epoch = %s AND state = 'RUNNING'",
                (now + timedelta(seconds=lease_seconds), now, str(claim.window.window_id), str(claim.owner_id),
                 claim.epoch))
            return unit.cursor.rowcount == 1

    def stage_states(self, window_id: UUID) -> Mapping[str, StageRunRecord]:
        with self._transaction(read_only=True) as unit:
            unit.cursor.execute(
                f"SELECT DISTINCT ON (stage_id) {_RUN_COLUMNS} FROM rt.stage_runs WHERE window_id = %s "
                "ORDER BY stage_id, attempt DESC", (str(window_id),))
            return {row[2]: _run(row) for row in unit.cursor.fetchall()}

    def recover_orphans(self, claim: WindowClaim, now: datetime) -> Sequence[StageRunRecord]:
        with self._transaction() as unit:
            self._fence(unit.cursor, claim)
            unit.cursor.execute(
                f"UPDATE rt.stage_runs SET state = 'FAILED', reason_codes = ARRAY['{OWNER_LOST}'], "
                "finished_at = GREATEST(%s, started_at), updated_at = %s WHERE window_id = %s AND state = 'RUNNING' "
                f"RETURNING {_RUN_COLUMNS}", (now, now, str(claim.window.window_id)))
            return [_run(row) for row in unit.cursor.fetchall()]

    def timeout_running_stages(self, claim: WindowClaim, stage_ids: Sequence[str],
                               now: datetime) -> Sequence[StageRunRecord]:
        """Fail the still-running stages of an overrun lane; their late writes are then rejected."""
        with self._transaction() as unit:
            self._fence(unit.cursor, claim)
            unit.cursor.execute(
                "UPDATE rt.stage_runs SET state = 'FAILED', reason_codes = ARRAY['STAGE_TIMEOUT'], "
                "finished_at = GREATEST(%s, started_at), updated_at = %s WHERE window_id = %s "
                f"AND state = 'RUNNING' AND stage_id = ANY(%s) RETURNING {_RUN_COLUMNS}",
                (now, now, str(claim.window.window_id), list(stage_ids)))
            return [_run(row) for row in unit.cursor.fetchall()]

    def start_stage(self, claim: WindowClaim, stage: StageDefinition, version: str, attempt: int,
                    now: datetime) -> StageRunRecord:
        with self._transaction() as unit:
            self._fence(unit.cursor, claim)
            unit.cursor.execute(
                "INSERT INTO rt.stage_runs (stage_run_id, window_id, stage_id, stage_version, attempt, state, "
                f"started_at, updated_at) VALUES (%s, %s, %s, %s, %s, 'RUNNING', %s, %s) RETURNING {_RUN_COLUMNS}",
                (str(uuid4()), str(claim.window.window_id), stage.stage_id, version, attempt, now, now))
            return _run(unit.cursor.fetchone())

    def finish_stage(self, claim: WindowClaim, run: StageRunRecord, state: str, reason_codes: Sequence[str],
                     receipt_id: UUID | None, now: datetime) -> None:
        with self._transaction() as unit:
            self._fence(unit.cursor, claim)
            unit.cursor.execute(
                "UPDATE rt.stage_runs SET state = %s, reason_codes = %s, receipt_id = %s, "
                "finished_at = GREATEST(%s, started_at), updated_at = %s WHERE stage_run_id = %s "
                "AND state = 'RUNNING'",
                (state, list(reason_codes), str(receipt_id) if receipt_id else None, now, now,
                 str(run.stage_run_id)))
            if unit.cursor.rowcount != 1:
                raise LeaseLost(f"stage run {run.stage_run_id} is not running")

    def _next_attempt(self, cursor, window_id: UUID, stage_id: str) -> int:
        cursor.execute("SELECT COALESCE(max(attempt), 0) + 1 FROM rt.stage_runs WHERE window_id = %s "
                       "AND stage_id = %s", (str(window_id), stage_id))
        return cursor.fetchone()[0]

    def record_blocked(self, claim: WindowClaim, stage_id: str, version: str, blocked_by: str,
                       blocker_state: str, now: datetime) -> StageRunRecord:
        with self._transaction() as unit:
            self._fence(unit.cursor, claim)
            attempt = self._next_attempt(unit.cursor, claim.window.window_id, stage_id)
            unit.cursor.execute(
                "INSERT INTO rt.stage_runs (stage_run_id, window_id, stage_id, stage_version, attempt, state, "
                "blocked_by_stage, reason_codes, finished_at, updated_at) VALUES (%s, %s, %s, %s, %s, "
                f"'BLOCKED_PREREQ', %s, %s, %s, %s) RETURNING {_RUN_COLUMNS}",
                (str(uuid4()), str(claim.window.window_id), stage_id, version, attempt, blocked_by,
                 [f"PREREQUISITE_{blocker_state}:{blocked_by}"], now, now))
            return _run(unit.cursor.fetchone())

    def record_missed(self, claim: WindowClaim, stage_id: str, version: str, reason: str,
                      now: datetime) -> StageRunRecord:
        with self._transaction() as unit:
            self._fence(unit.cursor, claim)
            attempt = self._next_attempt(unit.cursor, claim.window.window_id, stage_id)
            unit.cursor.execute(
                "INSERT INTO rt.stage_runs (stage_run_id, window_id, stage_id, stage_version, attempt, state, "
                f"reason_codes, finished_at, updated_at) VALUES (%s, %s, %s, %s, %s, 'MISSED', %s, %s, %s) "
                f"RETURNING {_RUN_COLUMNS}",
                (str(uuid4()), str(claim.window.window_id), stage_id, version, attempt, [reason], now, now))
            return _run(unit.cursor.fetchone())

    def record_receipt(self, claim: WindowClaim, spec: CycleReceiptSpec, run: StageRunRecord) -> UUID:
        with self._transaction() as unit:
            self._fence(unit.cursor, claim)
            unit.cursor.execute("SELECT 1 FROM rt.stage_runs WHERE stage_run_id = %s AND state = 'RUNNING' "
                                "FOR SHARE", (str(run.stage_run_id),))
            if unit.cursor.fetchone() is None:
                raise LeaseLost(f"stage run {run.stage_run_id} is no longer running; its receipt is rejected")
            unit.cursor.execute(
                "INSERT INTO rt.cycle_receipts (receipt_id, window_id, cohort_id, receipt_kind, cutoff, "
                "reference_generation, universe_sha256, input_sha256, build_id) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (window_id, receipt_kind) DO NOTHING",
                (str(uuid4()), str(claim.window.window_id), str(claim.window.cohort_id), spec.receipt_kind,
                 spec.cutoff, spec.reference_generation, spec.universe_sha256, spec.input_sha256, spec.build_id))
            unit.cursor.execute(
                "SELECT receipt_id, input_sha256 FROM rt.cycle_receipts WHERE window_id = %s AND receipt_kind = %s",
                (str(claim.window.window_id), spec.receipt_kind))
            receipt_id, input_sha256 = unit.cursor.fetchone()
        if input_sha256 != spec.input_sha256:
            raise ValueError(f"receipt {spec.receipt_kind} already recorded with different inputs")
        return UUID(str(receipt_id))

    def advance_release(self, claim: WindowClaim, receipt_id: UUID, cutoff: datetime, now: datetime) -> bool:
        with self._transaction() as unit:
            self._fence(unit.cursor, claim)
            unit.cursor.execute(
                "INSERT INTO rt.release_current (cohort_id, provider_tier, receipt_id, cutoff, updated_at) "
                "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (cohort_id, provider_tier) DO UPDATE SET "
                "receipt_id = EXCLUDED.receipt_id, cutoff = EXCLUDED.cutoff, updated_at = EXCLUDED.updated_at "
                "WHERE rt.release_current.cutoff < EXCLUDED.cutoff",
                (str(claim.window.cohort_id), claim.window.provider_tier, str(receipt_id), cutoff, now))
            return unit.cursor.rowcount == 1

    def readiness(self, cohort_id: UUID) -> Mapping[str, str]:
        with self._transaction(read_only=True) as unit:
            unit.cursor.execute("SELECT stage_id, status FROM rt.stage_readiness_current WHERE cohort_id = %s",
                                (str(cohort_id),))
            return dict(unit.cursor.fetchall())

    def upsert_readiness(self, cohort_id: UUID, stage_id: str, version: str, status: str,
                         reason_codes: Sequence[str], evidence_ref: str | None, now: datetime) -> None:
        with self._transaction() as unit:
            unit.cursor.execute(
                "INSERT INTO rt.stage_readiness_current (cohort_id, stage_id, stage_version, status, reason_codes, "
                "evidence_ref, checked_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (cohort_id, stage_id) DO UPDATE SET stage_version = EXCLUDED.stage_version, "
                "status = EXCLUDED.status, reason_codes = EXCLUDED.reason_codes, evidence_ref = EXCLUDED.evidence_ref, "
                "checked_at = EXCLUDED.checked_at, updated_at = EXCLUDED.updated_at",
                (str(cohort_id), stage_id, version, status, list(reason_codes), evidence_ref, now, now))

    def finish_window(self, claim: WindowClaim, state: str, now: datetime) -> None:
        self._set_window_state(claim, state, now)

    def release_window(self, claim: WindowClaim, now: datetime) -> None:
        self._set_window_state(claim, "PENDING", now)

    def _set_window_state(self, claim: WindowClaim, state: str, now: datetime) -> None:
        with self._transaction() as unit:
            self._fence(unit.cursor, claim)
            unit.cursor.execute(
                "UPDATE rt.window_runs SET state = %s, lease_until = NULL, updated_at = %s WHERE window_id = %s",
                (state, now, str(claim.window.window_id)))
