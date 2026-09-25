"""Independent continuous equity stages over retained canonical publications."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import logging
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

from database import get_db_cursor
from equity.domain import DecisionWatermark
from equity.leadership import try_advisory_leadership
from equity.orchestration import EquityMaterializationService
from equity.polygon import PolygonEquityClient
from equity.repositories import EquityAnalysisRepository, EquityIngestionRepository
from scripts import run_equity_worker as worker
from scripts.run_stock_alert_service import ManagedProcess, RedactedFormatter, StockAlertSupervisor, write_status


LOGGER = logging.getLogger("equity-stages")
ROOT = worker.BACKEND_DIR / "backups/equity-worker"
STAGES = {
    "native": ("30m", "5m", "15m"),
    "derived": ("1h", "1d", "1wk", "1mo"),
    "30m-analysis": ("30m",),
    "hourly-analysis": ("1h",),
    "short-analysis": ("5m", "15m"),
    "daily-analysis": ("1d", "1wk", "1mo"),
    "maintenance": ("30m", "1h", "1d", "1wk", "1mo", "5m", "15m"),
}


def publication_watermarks(intervals):
    from equity.orchestration import BAR_SELECTION_POLICY_SHA256

    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION READ ONLY")
        cursor.execute("SET LOCAL statement_timeout = '5s'")
        cursor.execute("""
            SELECT DISTINCT ON (interval) interval, market_time, observed_at, published_at, status,
                selected_members, expected_members
            FROM equity_bar_publications
            WHERE interval = ANY(%s) AND selection_policy_sha256 = %s
              AND session_scope = 'RTH' AND adjusted = FALSE
            ORDER BY interval, market_time DESC, observed_at DESC
            """, (list(intervals), BAR_SELECTION_POLICY_SHA256))
        return {row["interval"]: dict(row) for row in cursor.fetchall()}


class StageSchedule:
    def __init__(self, intervals, completed=None):
        self.intervals = tuple(intervals)
        self.completed = dict(completed or {})
        self.retries = {}
        self.position = 0

    def next(self, targets, now):
        for offset in range(len(self.intervals)):
            index = (self.position + offset) % len(self.intervals)
            interval = self.intervals[index]
            slot = targets.get(interval)
            if slot is None or self.completed.get(interval, slot - timedelta(seconds=1)) >= slot:
                continue
            retry = self.retries.get(interval)
            if retry and retry[0] == slot and now < retry[1]:
                continue
            self.position = (index + 1) % len(self.intervals)
            return interval, slot
        return None

    def finish(self, interval, slot, *, success, now):
        if success:
            self.completed[interval] = slot
            self.retries.pop(interval, None)
        else:
            self.retries[interval] = (slot, now + timedelta(seconds=worker.INTERVAL_RETRY_SECONDS))


def stage_targets(stage, now, publications):
    delay = timedelta(minutes=worker.PROVIDER_DELAY_MINUTES)
    targets = {}
    for interval in STAGES[stage]:
        if interval not in worker.INTERVALS:
            continue
        due = worker.latest_due_slot(now, interval, provider_delay=delay)
        if due is None:
            continue
        if stage == "native":
            targets[interval] = due
        elif stage == "derived":
            source = publications.get(worker.DERIVATION_SOURCES[interval])
            if source and source["status"] in ("COMPLETE", "DEGRADED") and source["market_time"] >= due:
                targets[interval] = due
        else:
            source = publications.get(interval)
            if source and source["status"] in ("COMPLETE", "DEGRADED") and source["market_time"] <= due:
                targets[interval] = source["market_time"]
    return targets


def analyze_published_interval(service, reference, *, interval, slot, observed_at):
    result = service.materialize_interval(reference.revisions, universe_run_id=reference.universe_run_id,
        interval=interval, watermark=DecisionWatermark(slot, observed_at))
    return result.status in ("COMPLETE", "DEGRADED")


def maintenance_jobs(intervals):
    jobs = []
    for interval in intervals:
        try:
            policies = worker.composite_scanner_outcome_policies(interval=interval,
                effective_from=worker.SCANNER_POLICY_EFFECTIVE_FROM)
        except ValueError:
            continue
        for policy in policies:
            for horizon in json.loads(policy.horizons_json):
                jobs.append((interval, policy, horizon))
    return tuple(jobs)


def run_stage(stage, *, behavior_shadow=False):
    intervals = tuple(interval for interval in STAGES[stage] if interval in worker.INTERVALS)
    if not intervals:
        raise ValueError("stage has no configured intervals")
    check_stage_parent()
    service = EquityMaterializationService(PolygonEquityClient(), native_fetch_workers=worker.NATIVE_FETCH_WORKERS,
        behavior_shadow_enabled=behavior_shadow,
        behavior_shadow_tickers=worker.behavior_shadow_tickers() if behavior_shadow else ())
    reference = None
    reference_checked = None
    completed = {}
    if stage.endswith("analysis"):
        service.analysis_repository.fail_stale_runs(stale_after=timedelta(0), intervals=intervals, run_purpose="ORIGINAL")
        completed = service.analysis_repository.latest_published_market_times(intervals)
    elif stage in ("native", "derived"):
        service.ingestion_repository.fail_stale_segments(stale_after=timedelta(0), intervals=intervals,
            dataset="EQUITY_BARS" if stage == "native" else "EQUITY_DERIVED_BARS")
        completed = {interval: row["market_time"] for interval, row in publication_watermarks(intervals).items()
            if row["status"] in ("COMPLETE", "DEGRADED")}
    schedule = StageSchedule(intervals, completed)
    maintenance = maintenance_jobs(intervals) if stage == "maintenance" else ()
    maintenance_index, adjusted_session = 0, None
    maintenance_retries = {}
    delay = timedelta(minutes=worker.PROVIDER_DELAY_MINUTES)
    while True:
        check_stage_parent()
        now = datetime.now(timezone.utc)
        state = dict(stage=stage, pid=os.getpid(), checked_at=now.isoformat(), state="WAITING",
            provider_delay_seconds=delay.total_seconds(), completed=schedule.completed)
        try:
            publications = publication_watermarks(worker.INTERVALS)
            targets = stage_targets(stage, now, publications)
            state["canonical"] = {interval: dict(market_time=row["market_time"], status=row["status"],
                excess_lag_seconds=max(0, (worker.latest_due_slot(now, interval, provider_delay=delay)
                    - row["market_time"]).total_seconds())) for interval, row in publications.items()}
            job = schedule.next(targets, now) if stage != "maintenance" else None
            if stage == "maintenance":
                daily = worker.latest_due_slot(now, "1d", provider_delay=delay)
                if behavior_shadow and adjusted_session != daily and now >= maintenance_retries.get("adjusted-daily", now):
                    job = ("adjusted-daily", daily)
                elif maintenance:
                    interval, policy, horizon = maintenance[maintenance_index]
                    maintenance_index = (maintenance_index + 1) % len(maintenance)
                    if interval in targets:
                        job = ("outcomes", targets[interval])
            if job:
                interval, slot = job
                state.update(state="BUSY", interval=interval, slot=slot, started_at=now.isoformat())
                write_stage_status(stage, state)
                started = time.monotonic()
                if interval == "outcomes":
                    result = service.evaluate_directional_outcomes(policy, horizon, available_by=now,
                        prospective_only=True, limit=100)
                    LOGGER.info("outcome batch horizon=%s due=%s persisted=%s pending=%s", horizon,
                        result.due, result.persisted, result.pending)
                else:
                    if reference is None or now - reference_checked >= timedelta(days=1):
                        tickers = tuple(worker.get_selected_tickers(active_only=True))
                        reference = (worker.load_or_refresh_reference(service, tickers, now, slot.date())
                            if stage == "native" else worker.load_retained_reference(tickers, now))
                        if reference is None or not reference.revisions:
                            raise RuntimeError("retained security/universe reference unavailable")
                        reference_checked = now
                    if interval == "adjusted-daily":
                        refreshed = worker.refresh_behavior_adjusted_daily(service, reference, observed_at=now,
                            session_count=worker.BEHAVIOR_ADJUSTED_CATCHUP_SESSIONS if adjusted_session is None else 1)
                        if refreshed and all(row.status in ("COMPLETE", "ALREADY_PRESENT") for row in refreshed):
                            adjusted_session = daily
                        else:
                            maintenance_retries[interval] = now + timedelta(seconds=worker.INTERVAL_RETRY_SECONDS)
                    elif stage in ("native", "derived"):
                        result = worker.publish_due_interval(service, reference, interval=interval,
                            slot=slot, observed_at=datetime.now(timezone.utc), once=False)
                        schedule.finish(interval, slot, success=result is not None and result.status in ("COMPLETE", "DEGRADED"),
                            now=datetime.now(timezone.utc))
                    else:
                        success = analyze_published_interval(service, reference, interval=interval, slot=slot,
                            observed_at=datetime.now(timezone.utc))
                        schedule.finish(interval, slot, success=success, now=datetime.now(timezone.utc))
                state.update(state="WAITING", duration_seconds=round(time.monotonic() - started, 3),
                    finished_at=datetime.now(timezone.utc).isoformat())
                LOGGER.info("stage=%s interval=%s slot=%s duration_seconds=%s", stage, interval, slot, state["duration_seconds"])
        except Exception as error:
            LOGGER.exception("stage=%s work failed; other stages continue", stage)
            state.update(state="RETRY", error=type(error).__name__)
            if state.get("interval") in intervals:
                schedule.finish(state["interval"], state["slot"], success=False, now=datetime.now(timezone.utc))
            elif state.get("interval"):
                maintenance_retries[state["interval"]] = datetime.now(timezone.utc) + timedelta(seconds=worker.INTERVAL_RETRY_SECONDS)
        state["checked_at"] = datetime.now(timezone.utc).isoformat()
        write_stage_status(stage, state)
        time.sleep(worker.POLL_SECONDS)


def write_stage_status(stage, state):
    write_status(ROOT / f"{stage}.json", json.loads(json.dumps(state, default=str)))


def stage_commands(*, behavior_shadow=False):
    return {stage: (sys.executable, "-X", "utf8", "-u", str(Path(worker.__file__).resolve()), "--stage", stage,
        *(('--behavior-shadow',) if behavior_shadow else ())) for stage, intervals in STAGES.items()
        if set(intervals) & set(worker.INTERVALS)}


class PosixStageProcess:
    def __init__(self, name, command, *, environment):
        self.process = subprocess.Popen(command, cwd=worker.BACKEND_DIR.parent, env=environment, start_new_session=True)
        self.pid = self.process.pid

    def poll(self):
        return self.process.poll()

    def close(self):
        if self.process.poll() is None:
            os.killpg(self.pid, signal.SIGTERM)
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(self.pid, signal.SIGKILL)
        self.process.wait(timeout=5)


def check_stage_parent():
    import psutil

    identity = os.environ.get("EQUITY_STAGE_PARENT_PID")
    created = os.environ.get("EQUITY_STAGE_PARENT_CREATED")
    if identity is None or created is None:
        raise RuntimeError("isolated equity stages require the managed supervisor")
    parent = psutil.Process(int(identity))
    if (parent.create_time() != float(created) or parent.pid not in {process.pid for process in psutil.Process().parents()}
            or not any(Path(argument).name == "run_equity_worker.py" for argument in parent.cmdline())):
        raise RuntimeError("equity stage supervisor identity changed")


def run_supervisor(*, behavior_shadow=False):
    import psutil
    from logging.handlers import RotatingFileHandler

    commands = stage_commands(behavior_shadow=behavior_shadow)
    if not commands or worker.POLL_SECONDS <= 0 or worker.NATIVE_FETCH_WORKERS <= 0 or worker.PROVIDER_DELAY_MINUTES < 0:
        raise ValueError("invalid continuous equity worker configuration")
    if set(worker.INTERVALS) - worker.SUPPORTED_INTERVALS:
        raise ValueError("unsupported equity intervals")
    ROOT.mkdir(parents=True, exist_ok=True)
    handler = RotatingFileHandler(ROOT / "service.log", maxBytes=2_000_000, backupCount=3, encoding="utf-8")
    handler.setFormatter(RedactedFormatter(os.environ))
    LOGGER.addHandler(handler)
    created = psutil.Process().create_time()
    environment = dict(os.environ, EQUITY_STAGE_PARENT_PID=str(os.getpid()), EQUITY_STAGE_PARENT_CREATED=str(created),
        OPENBLAS_NUM_THREADS="1", OMP_NUM_THREADS="1", MKL_NUM_THREADS="1")
    spawn = (lambda name, command: ManagedProcess(name, command, cwd=worker.BACKEND_DIR.parent,
        environment=environment, logger=LOGGER)) if os.name == "nt" else (
            lambda name, command: PosixStageProcess(name, command, environment=environment))
    supervisor = StockAlertSupervisor(commands, spawn)
    def stop_service(_signal, _frame):
        raise KeyboardInterrupt
    previous_signal = signal.signal(signal.SIGTERM, stop_service)
    try:
        while True:
            supervisor.tick()
            write_status(ROOT / "service.json", dict(supervisor.snapshot(), pid=os.getpid(),
                process_created_at=created, checked_at=datetime.now(timezone.utc).isoformat()))
            time.sleep(1)
    finally:
        supervisor.close()
        signal.signal(signal.SIGTERM, previous_signal)
        write_status(ROOT / "service.json", dict(supervisor.snapshot(), pid=os.getpid(),
            process_created_at=created, checked_at=datetime.now(timezone.utc).isoformat()))
        LOGGER.removeHandler(handler)
        handler.close()


def status():
    import psutil

    result = dict(checked_at=datetime.now(timezone.utc).isoformat(), stages={})
    for path in [ROOT / "service.json", *(ROOT / f"{name}.json" for name in STAGES)]:
        if not path.exists():
            continue
        if path.stat().st_size > 262144:
            raise ValueError("equity stage status exceeds bound")
        value = json.loads(path.read_text(encoding="utf-8"))
        if path.name == "service.json":
            try:
                value["process_alive"] = psutil.Process(value["pid"]).create_time() == value["process_created_at"]
            except psutil.NoSuchProcess:
                value["process_alive"] = False
            result["service"] = value
        else:
            result["stages"][path.stem] = value
    service = result.get("service", {})
    service["heartbeat_age_seconds"] = (datetime.now(timezone.utc)
        - datetime.fromisoformat(service["checked_at"])).total_seconds() if service.get("checked_at") else None
    if not service.get("process_alive") or service["heartbeat_age_seconds"] > 30:
        service["state"] = "UNAVAILABLE"
    for name, stage in result["stages"].items():
        component = service.get("components", {}).get(name, {})
        try:
            process = psutil.Process(stage["pid"])
            owned = component.get("pid") in {process.pid, *(parent.pid for parent in process.parents())}
            arguments = process.cmdline()
            owned = owned and "--stage" in arguments and arguments[arguments.index("--stage") + 1] == name
            owned = owned and stage["checked_at"] >= component["started_at"]
        except (psutil.NoSuchProcess, psutil.AccessDenied, KeyError, IndexError):
            owned = False
        if service.get("state") == "UNAVAILABLE" or not owned or component.get("state") != "RUNNING":
            stage["state"] = "UNVERIFIED"
    return result