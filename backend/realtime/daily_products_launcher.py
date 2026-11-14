"""R78 (Nov 14): the Equity scheduler's side of the daily products job - when it is due, and running it as a separate,
limited process without blocking the windows.

Due work, read from durable state on each check (every `check_seconds`, and right after every window):

- ``PRE_OPEN`` (before the target session T's open): T's cohort exists (activation, ~15 minutes before the open) and has
  no release yet. The job computes and stores the products for session S without a release (``--prepare``), so the
  first window is not followed by any computing.
- ``SESSION_COHORT`` (any phase): T's cohort for session S, once it has a current release (its first window published).
  A release needs a cycle receipt (rt.portal_releases.receipt_id references rt.cycle_receipts), which only exists after
  a window. With the products prepared this is a publish-only run (a narrow load to prove the input identity, then one
  release); without them (late activation, failed preparation) it computes, as a fallback.
- ``AFTER_CLOSE`` (lifecycle IDLE or BEFORE_OPEN): the last completed session S, its cohort, once S's closing window is
  COMPLETE (the daily bars and the close screening are stored, and the close release exists to attach to).

The job itself is idempotent (ALREADY_STORED by input identity) and never replaces a newer publication, so a
scheduler restart that re-runs a key costs one narrow load. Attempts per key are bounded (`max_attempts`, spaced by
`retry_spacing`); a run past its deadline is killed. A failed or abandoned key raises a fault record; the previous
publications stay served and the windows are never touched.

The child applies its own limits (address space, CPU seconds, nice) at start, so the launch is a plain spawn: the
scheduler holds the bars of its warm windows, and a fork with a `preexec_fn` would copy that address space.
"""
from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable

from .daily_products import MODE_OFF, daily_products_mode
from .fault_journal import emit_fault

AFTER_CLOSE = "AFTER_CLOSE"
SESSION_COHORT = "SESSION_COHORT"
PRE_OPEN = "PRE_OPEN"
SERVICE = "realtime-v2-advanced-scheduler"
JOB_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "run_realtime_v2_daily_products.py"
EXIT_NO_RELEASE = 3


@dataclass(frozen=True)
class DueProducts:
    cohort_id: str
    session: object  # date
    trigger: str

    @property
    def key(self) -> tuple[str, str, str]:
        # Preparing and publishing are separate keys: a prepared key done must not mark the publication done.
        return (self.cohort_id, str(self.session), "PREPARE" if self.trigger == PRE_OPEN else "PUBLISH")


def _int_env(environ, name, default):
    try:
        return int(environ.get(name, "") or default)
    except ValueError:
        return default


@dataclass
class DailyProductsLauncher:
    report_directory: Path
    environ: dict = field(default_factory=lambda: os.environ)
    popen: Callable = subprocess.Popen
    python: str = sys.executable
    check_seconds: int = 60
    max_attempts: int = 3
    retry_spacing: timedelta = timedelta(minutes=5)
    deadline: timedelta | None = None  # REALTIME_V2_DAILY_PRODUCTS_DEADLINE_SECONDS, default 600
    running: object = None
    running_due: DueProducts | None = None
    running_deadline: datetime | None = None
    done: set = field(default_factory=set)
    attempts: dict = field(default_factory=dict)
    next_attempt_at: dict = field(default_factory=dict)
    next_check_at: datetime | None = None
    last_outcomes: list = field(default_factory=list)

    def __post_init__(self):
        if self.deadline is None:
            self.deadline = timedelta(seconds=max(60, _int_env(self.environ, "REALTIME_V2_DAILY_PRODUCTS_DEADLINE_SECONDS", 600)))

    def mode(self) -> str:
        return daily_products_mode(self.environ)

    def check_soon(self) -> None:
        """Called by the scheduler after every window: the next tick checks for due work at once (the publish-only run
        follows the new cohort's first window without waiting for the periodic check)."""
        self.next_check_at = None

    def tick(self, now: datetime, discover: Callable[[datetime], list]) -> DueProducts | None:
        """Polls a running job; when none runs and a check is due, starts the first due key. `discover(now)` returns
        the due work in priority order. Returns the work started, if any. Never raises: the windows come first."""
        try:
            self._poll(now)
            if self.running is not None or self.mode() == MODE_OFF:
                return None
            if self.next_check_at is not None and now < self.next_check_at:
                return None
            self.next_check_at = now + timedelta(seconds=self.check_seconds)
            for due in discover(now):
                if (due.key in self.done or self.attempts.get(due.key, 0) >= self.max_attempts
                        or now < self.next_attempt_at.get(due.key, now)):
                    continue
                self._start(due, now)
                return due
        except Exception as error:  # noqa: BLE001 - a launcher fault must never stop the windows
            emit_fault(kind="DAILY_PRODUCTS_LAUNCHER_FAILED", severity="ERROR",
                       message=f"{type(error).__name__}: {error}", recoverable=True,
                       action="WINDOWS_CONTINUE", service=SERVICE, key=type(error).__name__)
        return None

    def _start(self, due: DueProducts, now: datetime) -> None:
        mode = self.mode()
        stamp = now.strftime("%Y%m%dT%H%M%SZ")
        report = self.report_directory / "daily-products" / f"{due.session}-{due.cohort_id[:8]}-{due.trigger}-{stamp}.json"
        command = [
            self.python, "-B", str(JOB_SCRIPT), "--cohort-id", due.cohort_id, "--session-date", str(due.session),
            "--mode", mode, "--trigger", due.trigger, "--report-path", str(report),
            "--memory-limit-mb", str(_int_env(self.environ, "REALTIME_V2_DAILY_PRODUCTS_MEMORY_MB", 3072)),
            "--cpu-seconds", str(int(self.deadline.total_seconds()) * 2),
            "--nice", str(_int_env(self.environ, "REALTIME_V2_DAILY_PRODUCTS_NICE", 10)),
            "--workers", str(max(1, _int_env(self.environ, "REALTIME_V2_DAILY_PRODUCTS_WORKERS", 1))),
        ]
        if due.trigger == PRE_OPEN:
            command.append("--prepare")
        environment = dict(self.environ)
        for name in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
            environment.setdefault(name, "1")
        self.attempts[due.key] = self.attempts.get(due.key, 0) + 1
        self.running = self.popen(command, env=environment, close_fds=True)
        self.running_due = due
        self.running_deadline = now + self.deadline
        print(f"Daily products started: {due.trigger} cohort {due.cohort_id} session {due.session} "
              f"mode {mode} attempt {self.attempts[due.key]}", file=sys.stderr, flush=True)

    def _poll(self, now: datetime) -> None:
        if self.running is None:
            return
        due = self.running_due
        code = self.running.poll()
        if code is None:
            if now < self.running_deadline:
                return
            self.running.kill()
            try:
                self.running.wait(timeout=10)
            except Exception:  # noqa: BLE001
                pass
            code = "DEADLINE"
        self.running = self.running_due = self.running_deadline = None
        self.last_outcomes.append((due.key, due.trigger, code))
        del self.last_outcomes[:-20]
        if code == 0:
            self.done.add(due.key)
            return
        if code == EXIT_NO_RELEASE:
            # Not a failure: the cohort has no release to attach to yet. Try again at the next check.
            self.attempts[due.key] -= 1
            return
        self.next_attempt_at[due.key] = now + self.retry_spacing
        exhausted = self.attempts.get(due.key, 0) >= self.max_attempts
        emit_fault(
            kind="DAILY_PRODUCTS_GAVE_UP" if exhausted else "DAILY_PRODUCTS_ATTEMPT_FAILED",
            severity="ERROR" if exhausted else "DEGRADED",
            message=f"daily products {due.trigger} exited {code} (attempt {self.attempts.get(due.key)})",
            recoverable=not exhausted,
            action="PREVIOUS_PUBLICATIONS_STAY_SERVED" if exhausted else f"RETRY_AFTER_{int(self.retry_spacing.total_seconds())}S",
            service=SERVICE, key=f"{due.trigger}:{code}",
            context=dict(cohort_id=due.cohort_id, session=str(due.session), attempts=self.attempts.get(due.key)),
        )


def discover_due_daily_products(provider, *, lifecycle, calendar, now, load_scope, recovery_complete, app_role):
    """The due work for the scheduler's lifecycle (see the module docstring), most urgent first: the target session's
    cohort (it is what the portal reads during the session: publish, else prepare before the open), then the closed
    session's."""
    from .advanced_equity_lifecycle import AdvancedEquityLifecyclePhase

    target = lifecycle.target_session_date
    session = calendar.previous_session_date(target)
    due = []
    with provider.transaction(app_role, read_only=True) as unit:
        cursor = unit.cursor
        scope = load_scope(cursor, session_date=target)
        if scope is not None:
            cursor.execute("SELECT 1 FROM rt.portal_release_current WHERE cohort_id=%s LIMIT 1",
                           (str(scope["cohort_id"]),))
            if cursor.fetchone() is not None:
                due.append(DueProducts(str(scope["cohort_id"]), session, SESSION_COHORT))
            elif now < calendar.session_open(target):
                due.append(DueProducts(str(scope["cohort_id"]), session, PRE_OPEN))
        if lifecycle.phase in {AdvancedEquityLifecyclePhase.IDLE, AdvancedEquityLifecyclePhase.BEFORE_OPEN}:
            closed = load_scope(cursor, session_date=session)
            if closed is not None and recovery_complete(
                    cursor, cohort_id=closed["cohort_id"], cutoff=calendar.session_close(session), checked_at=now):
                due.append(DueProducts(str(closed["cohort_id"]), session, AFTER_CLOSE))
    return due
