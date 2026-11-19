"""G102 R99 (Nov 18 2026): close-hour sweep of windows left PENDING / RUNNING past their deadline, once per session.

Why: a window whose owner died (VM stall, a stop during a window, a crash at the deadline: R90, R98) or that was never
claimed (catch-up, stream outages: R28, R100) stays PENDING / RUNNING for ever. The R32 self-heal check reports them
(`WINDOW_PAST_DEADLINE`, record-only, 7-day look-back) and nothing closes them unless someone runs a one-off pass (Nov 12
R38 MARK_MISSED; Nov 18 03:46 cade62a6 by hand: claim -> recover_orphans -> finish_window MISSED). Nov 18: 145 such rows
(136 Options PENDING Nov 3-17, 2 Options RUNNING with dead owners, 7 Equity RUNNING Nov 13) plus the R100 windows.

Run: `scripts/run_realtime_v2_self_heal.py --window-sweep [--mode OFF|RECORD|ACT] [--act]`, once per session after the
close (after R77 completes the Equity close window and R78 publishes the daily products, ~22:45 UTC); the host runner
(`runner/window_sweep_close.sh`, or the fault collector's `--window-sweep-at`) keeps it to one run per session. Re-running
is harmless: a closed window is no longer PENDING / RUNNING.

Modes (`REALTIME_V2_WINDOW_SWEEP`; `--mode` wins; unset or blank = RECORD):
| OFF    | nothing read, nothing written; one `WINDOW_SWEEP_SUMMARY` line |
| RECORD | read-only (one REPEATABLE READ snapshot): one `WINDOW_SWEEP_RECORD {json}` line per window it would close, one
           `WINDOW_SWEEP_SUMMARY {json}` line (counts per session and kind, skips per reason) and one FAULT record per
           kind (`WINDOW_PAST_DEADLINE`, WOULD_ACT, action CLOSE_MISSED) |
| ACT    | the same lines, then each window is closed in its own transaction (`WINDOW_SWEEP_ACTED {json}` per window, FAULT
           ACTED). ONLY with the explicit `--act` flag on the command line as well: ACT without it runs as RECORD and says
           so (`WINDOW_SWEEP_ACT_REFUSED`, summary `act_refused` true) |

A window is closed only if ALL hold (read in the snapshot, and every row condition re-checked under the row lock):
- state PENDING or RUNNING (RETAINED_GAP, UNAVAILABLE, COMPLETE, FAILED and MISSED are terminal and never touched);
- deadline more than 2 minutes ago;
- no live lease: lease_until NULL or more than 2 minutes ago (a role renewing its lease keeps it in the future);
- its session is settled: now >= the session's close + 105 minutes (17:45 New York, 22:45 UTC in winter; 14:45 New
  York on a half day), so nothing of a running session is ever touched;
- a known kind: 'E:' keys (Options V3) or 'advanced-' keys (Equity); other keys are reported and left alone;
- Equity only: a window at its session's close cutoff with no COMPLETE attempt belongs to R77 (completed-session
  recovery) until the next session's open, and is left alone until then.

Close (per window, one transaction, as the app role): `SELECT ... FOR UPDATE` (a role claiming it at the same moment
waits, then finds it terminal), the conditions re-checked, its RUNNING stage runs -> FAILED with reason OWNER_LOST (as
`orchestration_repository.recover_orphans`), then the window -> MISSED with its lease cleared (as `finish_window`). No
stage row is inserted (R98: a stages_v5 gated stage may hold a row only after its cycle authority is consumed), no cohort
readiness is written (the live deadline path's readiness upsert would land on today's cohort), no other table changes.
"""
from __future__ import annotations

import json
import os
from collections import Counter
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from enum import Enum
from typing import Callable, Iterable, Mapping
from zoneinfo import ZoneInfo

from .fault_journal import emit_fault

UTC = timezone.utc
NEW_YORK = ZoneInfo("America/New_York")
SERVICE = "self-heal"
WINDOW_SWEEP_SETTING = "REALTIME_V2_WINDOW_SWEEP"
WINDOW_SWEEP_RECORD_EVENT = "WINDOW_SWEEP_RECORD"
WINDOW_SWEEP_ACTED_EVENT = "WINDOW_SWEEP_ACTED"
WINDOW_SWEEP_SUMMARY_EVENT = "WINDOW_SWEEP_SUMMARY"
WINDOW_SWEEP_ACT_REFUSED_EVENT = "WINDOW_SWEEP_ACT_REFUSED"
FAULT_KIND = "WINDOW_PAST_DEADLINE"
ACTION = "CLOSE_MISSED"
OWNER_LOST = "OWNER_LOST"
PAST_DEADLINE_GRACE = timedelta(minutes=2)
LEASE_DEAD_GRACE = timedelta(minutes=2)
SETTLE_AFTER_CLOSE = timedelta(minutes=105)
DEFAULT_LIMIT = 1000
OPEN_STATES = ("PENDING", "RUNNING")
KIND_OPTIONS, KIND_EQUITY, KIND_OTHER = "OPTIONS", "EQUITY", "OTHER"
# Skip reasons (a window seen past its deadline but not closed).
SKIP_LIVE_LEASE = "LIVE_LEASE"
SKIP_CURRENT_SESSION = "CURRENT_SESSION"
SKIP_UNKNOWN_KIND = "UNKNOWN_KIND"
SKIP_R77_CLOSE_RECOVERY = "R77_CLOSE_RECOVERY"
# Outcome of one close under the row lock.
CLOSED = "CLOSED"
_LIST_LIMIT = 30


class WindowSweepMode(str, Enum):
    OFF = "OFF"
    RECORD = "RECORD"
    ACT = "ACT"


def parse_window_sweep_mode(raw: object) -> WindowSweepMode:
    """OFF / RECORD / ACT in any case; unset or blank is RECORD; anything else raises."""
    if isinstance(raw, WindowSweepMode):
        return raw
    if raw is not None and not isinstance(raw, str):
        raise ValueError(f"{WINDOW_SWEEP_SETTING} must be OFF, RECORD or ACT, not {raw!r}")
    value = (raw or "").strip().upper() or WindowSweepMode.RECORD.value
    try:
        return WindowSweepMode(value)
    except ValueError:
        raise ValueError(f"{WINDOW_SWEEP_SETTING} must be OFF, RECORD or ACT, not {raw!r}") from None


def window_sweep_mode(cli_value: str | None = None, environ: Mapping[str, str] | None = None) -> WindowSweepMode:
    if cli_value is not None:
        return parse_window_sweep_mode(cli_value)
    return parse_window_sweep_mode((os.environ if environ is None else environ).get(WINDOW_SWEEP_SETTING))


def effective_mode(mode: WindowSweepMode, act_flag: bool) -> tuple[WindowSweepMode, bool]:
    """(the mode that runs, ACT refused). ACT needs the explicit `--act` flag; without it the sweep only records."""
    if mode is WindowSweepMode.ACT and act_flag is not True:
        return WindowSweepMode.RECORD, True
    return mode, False


def window_kind(window_key: str) -> str:
    key = str(window_key)
    if key.startswith("E:"):
        return KIND_OPTIONS
    if key.startswith("advanced-"):
        return KIND_EQUITY
    return KIND_OTHER


@dataclass(frozen=True)
class StaleWindow:
    window_id: str
    window_key: str
    state: str
    session_date: date
    cutoff: datetime
    deadline: datetime
    lease_until: datetime | None
    owner_epoch: int
    running_stages: tuple[str, ...] = ()
    sibling_complete: bool = False

    @property
    def kind(self) -> str:
        return window_kind(self.window_key)

    def document(self) -> dict[str, object]:
        return {
            "window_id": self.window_id,
            "window_key": self.window_key,
            "kind": self.kind,
            "session_date": self.session_date.isoformat(),
            "state": self.state,
            "cutoff": self.cutoff.astimezone(UTC).isoformat(),
            "deadline": self.deadline.astimezone(UTC).isoformat(),
            "lease_until": None if self.lease_until is None else self.lease_until.astimezone(UTC).isoformat(),
            "owner_epoch": self.owner_epoch,
            "running_stage_runs": list(self.running_stages),
        }


@dataclass
class SweepPlan:
    eligible: list[StaleWindow] = field(default_factory=list)
    skipped: list[tuple[StaleWindow, str]] = field(default_factory=list)


_CANDIDATES_SQL = (
    "SELECT w.window_id::text, w.window_key, w.state, p.session_date, w.cutoff, w.deadline, w.lease_until, "
    "w.owner_epoch, "
    "coalesce((SELECT array_agg(s.stage_id::text ORDER BY s.stage_id, s.attempt) FROM rt.stage_runs s "
    "WHERE s.window_id = w.window_id AND s.state = 'RUNNING'), ARRAY[]::text[]), "
    "EXISTS (SELECT 1 FROM rt.window_runs c WHERE c.cohort_id = w.cohort_id AND c.cutoff = w.cutoff "
    "AND c.state = 'COMPLETE') "
    "FROM rt.window_runs w JOIN rt.service_plans p ON p.plan_id = w.plan_id "
    "WHERE w.state IN ('PENDING', 'RUNNING') AND w.deadline < %s "
    "ORDER BY w.cutoff, w.window_key LIMIT %s"
)


def load_stale_windows(cursor, *, now: datetime, limit: int = DEFAULT_LIMIT) -> list[StaleWindow]:
    """Every PENDING / RUNNING window whose deadline passed more than the grace ago, oldest cutoff first."""
    cursor.execute(_CANDIDATES_SQL, (now - PAST_DEADLINE_GRACE, int(limit)))
    return [
        StaleWindow(
            window_id=str(row[0]), window_key=str(row[1]), state=str(row[2]), session_date=row[3], cutoff=row[4],
            deadline=row[5], lease_until=row[6], owner_epoch=int(row[7]), running_stages=tuple(row[8] or ()),
            sibling_complete=bool(row[9]),
        )
        for row in cursor.fetchall()
    ]


def _default_calendar():
    from options.calendar import OptionExchangeCalendar

    return OptionExchangeCalendar("XNYS")


def _session_close(calendar, session: date) -> datetime:
    try:
        return calendar.session_close(session)
    except Exception:  # noqa: BLE001 - not an exchange session (should not happen): the regular close time
        return datetime.combine(session, time(16, 0), NEW_YORK).astimezone(UTC)


def _next_session_open(calendar, session: date) -> datetime | None:
    try:
        return calendar.next_session_open(session)
    except Exception:  # noqa: BLE001 - unknown: treat R77's window as still open
        return None


def plan_sweep(windows: Iterable[StaleWindow], *, now: datetime, calendar=None,
               settle: timedelta = SETTLE_AFTER_CLOSE) -> SweepPlan:
    """Split the windows read in the snapshot into the ones to close and the ones left alone (with the reason)."""
    calendar = calendar or _default_calendar()
    plan = SweepPlan()
    closes: dict[date, datetime] = {}
    for window in windows:
        if window.state not in OPEN_STATES or window.deadline >= now - PAST_DEADLINE_GRACE:
            continue  # not a candidate at all (the query already excludes these)
        if window.lease_until is not None and window.lease_until >= now - LEASE_DEAD_GRACE:
            plan.skipped.append((window, SKIP_LIVE_LEASE))
            continue
        if window.kind == KIND_OTHER:
            plan.skipped.append((window, SKIP_UNKNOWN_KIND))
            continue
        close = closes.get(window.session_date)
        if close is None:
            close = closes[window.session_date] = _session_close(calendar, window.session_date)
        if now < close + settle:
            plan.skipped.append((window, SKIP_CURRENT_SESSION))
            continue
        if window.kind == KIND_EQUITY and window.cutoff == close and not window.sibling_complete:
            next_open = _next_session_open(calendar, window.session_date)
            if next_open is None or now < next_open:
                plan.skipped.append((window, SKIP_R77_CLOSE_RECOVERY))
                continue
        plan.eligible.append(window)
    return plan


def close_window_missed_cursor(cursor, window_id: str, now: datetime) -> tuple[str, tuple[str, ...]]:
    """ACT for one window inside the caller's transaction: lock, re-check, orphans FAILED OWNER_LOST, window MISSED.
    Returns (outcome, stage ids failed); outcome CLOSED or the reason it was left alone (state moved on, lease alive)."""
    cursor.execute(
        "SELECT state, deadline, lease_until FROM rt.window_runs WHERE window_id = %s FOR UPDATE", (str(window_id),))
    row = cursor.fetchone()
    if row is None:
        return "GONE", ()
    state, deadline, lease_until = row
    if state not in OPEN_STATES:
        return f"NOW_{state}", ()
    if deadline >= now - PAST_DEADLINE_GRACE:
        return "NOT_PAST_DEADLINE", ()
    if lease_until is not None and lease_until >= now - LEASE_DEAD_GRACE:
        return SKIP_LIVE_LEASE, ()
    cursor.execute(
        f"UPDATE rt.stage_runs SET state = 'FAILED', reason_codes = ARRAY['{OWNER_LOST}'], "
        "finished_at = GREATEST(%s, started_at), updated_at = %s WHERE window_id = %s AND state = 'RUNNING' "
        "RETURNING stage_id", (now, now, str(window_id)))
    failed = tuple(sorted(str(item[0]) for item in cursor.fetchall()))
    cursor.execute(
        "UPDATE rt.window_runs SET state = 'MISSED', lease_until = NULL, updated_at = %s "
        "WHERE window_id = %s AND state IN ('PENDING', 'RUNNING')", (now, str(window_id)))
    if cursor.rowcount != 1:
        raise RuntimeError(f"window {window_id} did not close (rowcount {cursor.rowcount})")
    return CLOSED, failed


def _print_line(event: str, document: Mapping[str, object]) -> None:
    print(f"{event} {json.dumps(document, sort_keys=True, default=str, separators=(',', ':'))}", flush=True)


def _per_session(windows: Iterable[StaleWindow]) -> dict[str, dict[str, int]]:
    table: dict[str, Counter] = {}
    for window in windows:
        table.setdefault(window.session_date.isoformat(), Counter())[window.kind] += 1
    return {day: dict(sorted(counts.items())) for day, counts in sorted(table.items())}


def run_window_sweep(
    read_cursor,
    *,
    now: datetime,
    mode: WindowSweepMode | str = WindowSweepMode.RECORD,
    act_flag: bool = False,
    write_transaction: Callable[[], AbstractContextManager] | None = None,
    calendar=None,
    limit: int = DEFAULT_LIMIT,
    emit_line: Callable[[str, Mapping[str, object]], None] = _print_line,
    emit: Callable[..., dict | None] = emit_fault,
) -> dict[str, object]:
    """One sweep. RECORD reads only `read_cursor` (the caller's read-only snapshot). ACT also needs
    `write_transaction` (a context manager yielding a cursor in its own transaction) and `act_flag`; each window is
    closed in its own transaction, so one failure leaves the others. Returns the summary document."""
    requested = parse_window_sweep_mode(mode)
    running, refused = effective_mode(requested, act_flag)
    summary: dict[str, object] = {
        "contract": "realtime-window-sweep-v1",
        "checked_at": now.astimezone(UTC).isoformat(),
        "mode": requested.value,
        "effective_mode": running.value,
        "act_refused": refused,
    }
    if refused:
        emit_line(WINDOW_SWEEP_ACT_REFUSED_EVENT, {
            "mode": requested.value, "effective_mode": running.value,
            "reason": "ACT needs the explicit --act flag; running as RECORD",
        })
    if running is WindowSweepMode.OFF:
        emit_line(WINDOW_SWEEP_SUMMARY_EVENT, summary)
        return summary
    if running is WindowSweepMode.ACT and write_transaction is None:
        raise ValueError("ACT needs a write transaction")
    windows = load_stale_windows(read_cursor, now=now, limit=limit)
    plan = plan_sweep(windows, now=now, calendar=calendar)
    for window in plan.eligible:
        emit_line(WINDOW_SWEEP_RECORD_EVENT, {
            **window.document(), "mode": running.value, "would": "MISSED",
            "would_fail_stage_runs": len(window.running_stages),
        })
    skipped = Counter(reason for _window, reason in plan.skipped)
    summary.update({
        "candidates": len(windows),
        "limit_reached": len(windows) >= int(limit),
        "eligible": len(plan.eligible),
        "eligible_by_kind": dict(sorted(Counter(window.kind for window in plan.eligible).items())),
        "eligible_by_state": dict(sorted(Counter(window.state for window in plan.eligible).items())),
        "eligible_per_session": _per_session(plan.eligible),
        "stage_runs_running": sum(len(window.running_stages) for window in plan.eligible),
        "skipped": dict(sorted(skipped.items())),
        "skipped_windows": [f"{window.window_key} {window.state} {reason}"
                            for window, reason in plan.skipped[:_LIST_LIMIT]],
    })
    outcomes: dict[str, tuple[str, tuple[str, ...]]] = {}
    if running is WindowSweepMode.ACT:
        for window in plan.eligible:
            try:
                with write_transaction() as cursor:
                    outcome, failed = close_window_missed_cursor(cursor, window.window_id, now)
            except Exception as error:  # noqa: BLE001 - one window never stops the others
                outcome, failed = f"ERROR {type(error).__name__}: {error}"[:300], ()
            outcomes[window.window_id] = (outcome, failed)
            emit_line(WINDOW_SWEEP_ACTED_EVENT, {
                "window_id": window.window_id, "window_key": window.window_key, "kind": window.kind,
                "session_date": window.session_date.isoformat(), "outcome": outcome,
                "stage_runs_failed": list(failed),
            })
        closed = [window for window in plan.eligible if outcomes[window.window_id][0] == CLOSED]
        summary.update({
            "closed": len(closed),
            "closed_by_kind": dict(sorted(Counter(window.kind for window in closed).items())),
            "closed_per_session": _per_session(closed),
            "stage_runs_failed": sum(len(outcomes[window.window_id][1]) for window in closed),
            "not_closed": dict(sorted(Counter(
                outcome.split(" ", 1)[0] for outcome, _failed in outcomes.values() if outcome != CLOSED).items())),
        })
    emit_line(WINDOW_SWEEP_SUMMARY_EVENT, summary)
    for kind in sorted({window.kind for window in plan.eligible}):
        mine = [window for window in plan.eligible if window.kind == kind]
        context: dict[str, object] = {
            "mode": running.value, "per_session": {day: counts.get(kind, 0)
                                                   for day, counts in _per_session(mine).items()},
            "windows": [f"{window.window_key} {window.state}" for window in mine[-_LIST_LIMIT:]],
            "oldest_deadline": mine[0].deadline.astimezone(UTC).isoformat(),
        }
        severity = "WOULD_ACT"
        if running is WindowSweepMode.ACT:
            done = [window for window in mine if outcomes[window.window_id][0] == CLOSED]
            errors = [window for window in mine if outcomes[window.window_id][0].startswith("ERROR")]
            context["closed"] = len(done)
            context["errors"] = len(errors)
            severity = "ERROR" if errors else "ACTED"
        emit(kind=FAULT_KIND, severity=severity, recoverable=True, action=ACTION, service=SERVICE,
             key=f"SWEEP|{kind}",
             message=f"close-hour sweep: {len(mine)} {kind.title()} windows PENDING/RUNNING past their deadline "
                     f"({'closed MISSED' if running is WindowSweepMode.ACT else 'would close MISSED'})",
             context=context)
    return summary
