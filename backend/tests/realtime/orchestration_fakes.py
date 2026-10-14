"""In-memory OrchestrationRepository that enforces the same rules as the V2 tables and triggers."""
from __future__ import annotations

import re
import threading
from datetime import datetime, timedelta
from uuid import UUID, uuid4

from realtime.orchestration_repository import (
    OWNER_LOST,
    TERMINAL_WINDOW_STATES,
    CycleReceiptSpec,
    LeaseLost,
    ServicePlan,
    StageRunRecord,
    WindowClaim,
    WindowRecord,
    WindowSpec,
)
from realtime.stage_graph import STAGES_V1, StageDefinition

FAILING = ("FAILED", "BLOCKED_PREREQ", "BLOCKED_POLICY", "MISSED")


class InMemoryOrchestrationRepository:
    def __init__(self, plan: ServicePlan, catalog=STAGES_V1) -> None:
        self.plan = plan
        self.catalog = dict(catalog)
        self.windows: dict[UUID, dict] = {}
        self.runs: list[dict] = []
        self.receipts: dict[tuple[UUID, str], tuple[UUID, str]] = {}
        self.release: dict[tuple[UUID, str], tuple[UUID, datetime]] = {}
        self.ready: dict[tuple[UUID, str], tuple[str, tuple[str, ...]]] = {}
        self.lock = threading.RLock()

    def load_catalog(self, version):
        return dict(self.catalog)

    def load_plan(self, plan_id):
        if plan_id != self.plan.plan_id:
            raise LookupError("unknown plan")
        return self.plan

    def _record(self, row) -> WindowRecord:
        return WindowRecord(row["id"], row["plan"], row["cohort"], row["tier"], row["key"], row["release"],
                            row["cutoff"], row["deadline"], row["state"])

    def ensure_window(self, plan_id, spec: WindowSpec):
        with self.lock:
            row = next((r for r in self.windows.values()
                        if r["cohort"] == spec.cohort_id and r["key"] == spec.window_key), None)
            if row is None:
                row = {"id": spec.window_id, "plan": plan_id, "cohort": spec.cohort_id, "tier": spec.provider_tier,
                       "key": spec.window_key, "release": spec.release_at, "cutoff": spec.cutoff,
                       "deadline": spec.deadline, "state": "PENDING", "owner": None, "epoch": 0, "lease": None}
                self.windows[spec.window_id] = row
            elif (row["plan"], row["tier"], row["release"], row["cutoff"], row["deadline"]) != (
                    plan_id, spec.provider_tier, spec.release_at, spec.cutoff, spec.deadline):
                raise ValueError("window already exists with a different identity")
            return self._record(row)

    def open_windows(self, cohort_id):
        with self.lock:
            rows = [r for r in self.windows.values() if r["cohort"] == cohort_id
                    and r["state"] in ("PENDING", "RUNNING")]
            return [self._record(r) for r in sorted(rows, key=lambda r: r["release"])]

    def claim_window(self, window_id, owner_id, now, lease_seconds):
        with self.lock:
            row = self.windows[window_id]
            if row["state"] in TERMINAL_WINDOW_STATES:
                return None
            if (row["state"] == "RUNNING" and row["lease"] is not None and row["lease"] > now
                    and row["owner"] != owner_id):
                return None
            row.update(state="RUNNING", owner=owner_id, epoch=row["epoch"] + 1,
                       lease=now + timedelta(seconds=lease_seconds))
            return WindowClaim(self._record(row), owner_id, row["epoch"], row["lease"])

    def _fence(self, claim):
        row = self.windows[claim.window.window_id]
        if (row["owner"], row["epoch"], row["state"]) != (claim.owner_id, claim.epoch, "RUNNING"):
            raise LeaseLost("window is no longer owned")
        return row

    def renew_lease(self, claim, now, lease_seconds):
        with self.lock:
            try:
                self._fence(claim)["lease"] = now + timedelta(seconds=lease_seconds)
            except LeaseLost:
                return False
            return True

    def _latest(self, window_id):
        latest: dict[str, dict] = {}
        for run in self.runs:
            if run["window"] == window_id and (run["stage"] not in latest
                                               or run["attempt"] > latest[run["stage"]]["attempt"]):
                latest[run["stage"]] = run
        return latest

    @staticmethod
    def _run_record(run) -> StageRunRecord:
        return StageRunRecord(run["id"], run["window"], run["stage"], run["version"], run["attempt"], run["state"],
                              run["blocked_by"], tuple(run["reasons"]))

    def stage_states(self, window_id):
        with self.lock:
            return {stage: self._run_record(run) for stage, run in self._latest(window_id).items()}

    def recover_orphans(self, claim, now):
        with self.lock:
            self._fence(claim)
            recovered = []
            for run in self.runs:
                if run["window"] == claim.window.window_id and run["state"] == "RUNNING":
                    run.update(state="FAILED", reasons=[OWNER_LOST], finished=now)
                    recovered.append(self._run_record(run))
            return recovered

    def timeout_running_stages(self, claim, stage_ids, now):
        with self.lock:
            self._fence(claim)
            timed_out = []
            for run in self.runs:
                if (run["window"] == claim.window.window_id and run["state"] == "RUNNING"
                        and run["stage"] in stage_ids):
                    run.update(state="FAILED", reasons=["STAGE_TIMEOUT"], finished=now)
                    timed_out.append(self._run_record(run))
            return timed_out

    def _insert(self, claim, stage_id, version, attempt, state, *, blocked_by=None, reasons=(), now=None):
        if any(r["window"] == claim.window.window_id and r["stage"] == stage_id and r["attempt"] == attempt
               for r in self.runs):
            raise ValueError("duplicate stage attempt")
        if state in FAILING and not reasons:
            raise ValueError("reason codes are required")
        if (state == "BLOCKED_PREREQ") != (blocked_by is not None):
            raise ValueError("blocked_by_stage is required exactly for BLOCKED_PREREQ")
        if state == "BLOCKED_PREREQ":
            if blocked_by not in self.catalog[stage_id].prerequisites:
                raise ValueError("not a declared prerequisite")
            prior = self._latest(claim.window.window_id).get(blocked_by)
            if prior is None or prior["state"] not in FAILING:
                raise ValueError("prerequisite has not failed or been blocked in this window")
        run = {"id": uuid4(), "window": claim.window.window_id, "stage": stage_id, "version": version,
               "attempt": attempt, "state": state, "blocked_by": blocked_by, "reasons": list(reasons),
               "receipt": None, "started": now if state == "RUNNING" else None, "finished": None}
        self.runs.append(run)
        return self._run_record(run)

    def start_stage(self, claim, stage: StageDefinition, version, attempt, now):
        with self.lock:
            self._fence(claim)
            return self._insert(claim, stage.stage_id, version, attempt, "RUNNING", now=now)

    def finish_stage(self, claim, run, state, reason_codes, receipt_id, now):
        with self.lock:
            self._fence(claim)
            row = next(r for r in self.runs if r["id"] == run.stage_run_id)
            if row["state"] != "RUNNING":
                raise LeaseLost("stage run is not running")
            if state in FAILING and not reason_codes:
                raise ValueError("reason codes are required")
            row.update(state=state, reasons=list(reason_codes), receipt=receipt_id, finished=now)

    def _next_attempt(self, window_id, stage_id):
        return max((r["attempt"] for r in self.runs if r["window"] == window_id and r["stage"] == stage_id),
                   default=0) + 1

    def record_blocked(self, claim, stage_id, version, blocked_by, blocker_state, now):
        with self.lock:
            self._fence(claim)
            return self._insert(claim, stage_id, version, self._next_attempt(claim.window.window_id, stage_id),
                                "BLOCKED_PREREQ", blocked_by=blocked_by,
                                reasons=[f"PREREQUISITE_{blocker_state}:{blocked_by}"], now=now)

    def record_missed(self, claim, stage_id, version, reason, now):
        with self.lock:
            self._fence(claim)
            return self._insert(claim, stage_id, version, self._next_attempt(claim.window.window_id, stage_id),
                                "MISSED", reasons=[reason], now=now)

    def record_receipt(self, claim, spec: CycleReceiptSpec, run):
        with self.lock:
            self._fence(claim)
            current = next((r for r in self.runs if r["id"] == run.stage_run_id), None)
            if current is None or current["state"] != "RUNNING":
                raise LeaseLost("stage run is no longer running; its receipt is rejected")
            if (spec.reference_generation <= 0 or not spec.build_id.strip()
                    or not all(re.fullmatch("[0-9a-f]{64}", value) for value in (spec.universe_sha256,
                                                                                  spec.input_sha256))):
                raise ValueError("receipt violates the table checks")
            key = (claim.window.window_id, spec.receipt_kind)
            if key not in self.receipts:
                self.receipts[key] = (uuid4(), spec.input_sha256)
            receipt_id, input_sha256 = self.receipts[key]
            if input_sha256 != spec.input_sha256:
                raise ValueError("receipt already recorded with different inputs")
            return receipt_id

    def advance_release(self, claim, receipt_id, cutoff, now):
        with self.lock:
            self._fence(claim)
            key = (claim.window.cohort_id, claim.window.provider_tier)
            if key in self.release and self.release[key][1] >= cutoff:
                return False
            self.release[key] = (receipt_id, cutoff)
            return True

    def readiness(self, cohort_id):
        with self.lock:
            return {stage: status for (cohort, stage), (status, _) in self.ready.items() if cohort == cohort_id}

    def upsert_readiness(self, cohort_id, stage_id, version, status, reason_codes, evidence_ref, now):
        if (status == "READY") != (not reason_codes):
            raise ValueError("readiness reason codes must be empty exactly for READY")
        with self.lock:
            self.ready[(cohort_id, stage_id)] = (status, tuple(reason_codes))

    def finish_window(self, claim, state, now):
        with self.lock:
            row = self._fence(claim)
            row.update(state=state, lease=None)

    def release_window(self, claim, now):
        with self.lock:
            row = self._fence(claim)
            row.update(state="PENDING", lease=None)
