"""Manage independent UI, reference-refresh and Options worker lifecycles."""
from __future__ import annotations

from datetime import datetime, timezone
import argparse
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import signal
import sys
import threading
import time
from uuid import uuid4

import psutil


BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from scripts.run_stock_alert_service import (
    ManagedProcess, RedactedFormatter, StockAlertSupervisor, WindowsJob, process_script, write_status,
)


STATE_ROOT = BACKEND / "backups/worker-groups"
GROUPS = ("projections", "references", "options")


def group_commands(group, *, scope="all", backend=BACKEND, executable=sys.executable):
    prefix = (str(executable), "-X", "utf8", "-u")
    scripts = backend / "scripts"
    if group == "projections":
        return dict(portal=(*prefix, str(scripts / "refresh_equity_portal_snapshots.py"), "--continuous"),
            screening=(*prefix, str(scripts / "run_screening_worker.py")))
    if group == "options":
        return dict(options=(*prefix, str(scripts / "run_option_worker.py")))
    if group != "references" or scope not in ("all", "equity", "options"):
        raise ValueError("unsupported worker group or reference scope")
    commands = {}
    if scope in ("all", "equity"):
        commands["corporate-actions"] = (*prefix, str(scripts / "run_corporate_action_worker.py"), "--once")
    if scope in ("all", "options"):
        commands["market-events"] = (*prefix, str(scripts / "run_market_event_worker.py"), "--once")
        commands["option-inputs"] = (*prefix, str(scripts / "run_option_model_input_worker.py"), "--once")
    return commands


def reference_schedules(names, environment):
    keys = {"corporate-actions": ("EQUITY_CORPORATE_ACTION_POLL_SECONDS", "EQUITY_CORPORATE_ACTION_RETRY_SECONDS"),
        "market-events": ("MARKET_EVENT_POLL_SECONDS", "MARKET_EVENT_RETRY_SECONDS"),
        "option-inputs": ("OPTION_MODEL_INPUT_POLL_SECONDS", "OPTION_MODEL_INPUT_RETRY_SECONDS")}
    schedules = {}
    for name in names:
        period_key, retry_key = keys[name]
        period, retry = int(environment.get(period_key, "21600")), int(environment.get(retry_key, "300"))
        if period <= 0 or retry <= 0:
            raise ValueError("reference refresh and retry intervals must be positive")
        schedules[name] = (period, retry)
    return schedules


class WorkerGroupSupervisor(StockAlertSupervisor):
    def __init__(self, commands, spawn, *, schedules=None, clock=time.monotonic, utc_clock=None):
        super().__init__(commands, spawn, clock=clock)
        self.schedules = dict(schedules or {})
        if set(self.schedules) - set(commands) or any(period <= 0 or retry <= 0 for period, retry in self.schedules.values()):
            raise ValueError("invalid scheduled worker configuration")
        self.utc_clock = utc_clock or (lambda: datetime.now(timezone.utc))
        self.last_finished = {}
        self.next_due = {}

    def failed(self, component, now, reason):
        if component.name not in self.schedules:
            return super().failed(component, now, reason)
        component.failures += 1
        component.reason, component.state = reason, "BACKOFF"
        delay = self.schedules[component.name][1]
        component.retry_at = now + delay
        self.next_due[component.name] = self.utc_clock().timestamp() + delay

    def tick(self):
        if self.stopped:
            return False
        changed = False
        for component in self.components:
            if component.name not in self.schedules or component.process is None:
                continue
            if component.process.poll() == 0:
                component.process.close()
                component.process = None
                component.exit_code, component.failures = 0, 0
                component.reason, component.state = None, "WAITING"
                delay = self.schedules[component.name][0]
                component.retry_at = self.clock() + delay
                self.last_finished[component.name] = self.utc_clock().isoformat()
                self.next_due[component.name] = self.utc_clock().timestamp() + delay
                changed = True
        changed = super().tick() or changed
        for component in self.components:
            if component.name in self.schedules and component.process is not None:
                self.next_due.pop(component.name, None)
        return changed

    def close(self):
        for component in self.components:
            if component.name in self.schedules and component.process is not None and component.process.poll() is None:
                self.failed(component, self.clock(), "ATTEMPT_INTERRUPTED_BY_STOP")
        super().close()

    def snapshot(self):
        result = super().snapshot()
        for component in self.components:
            details = result["components"][component.name]
            scheduled = component.name in self.schedules
            details.update(mode="SCHEDULED" if scheduled else "CONTINUOUS",
                refresh_seconds=self.schedules[component.name][0] if scheduled else None,
                retry_seconds=self.schedules[component.name][1] if scheduled else None,
                last_successful_exit_at=self.last_finished.get(component.name),
                next_due_at=datetime.fromtimestamp(self.next_due[component.name], timezone.utc).isoformat()
                    if component.name in self.next_due and component.process is None else None)
        if not self.stopped and all(component.state in ("RUNNING", "WAITING") for component in self.components):
            result["state"] = "RUNNING"
        return result

    def restore_schedule(self, previous):
        for component in self.components:
            if component.name not in self.schedules:
                continue
            saved = previous.get("components", {}).get(component.name, {})
            if not saved:
                continue
            period, retry = self.schedules[component.name]
            if (saved.get("refresh_seconds"), saved.get("retry_seconds")) != (period, retry):
                raise ValueError("saved reference cadence differs; review schedule before starting")
            last = saved.get("last_successful_exit_at")
            if last:
                stamp = datetime.fromisoformat(last)
                if stamp.utcoffset() is None:
                    raise ValueError("invalid saved refresh clock")
                self.last_finished[component.name] = last
            due = saved.get("next_due_at")
            if due:
                stamp = datetime.fromisoformat(due)
                if stamp.utcoffset() is None:
                    raise ValueError("invalid saved schedule clock")
                remaining = max(0, stamp.timestamp() - self.utc_clock().timestamp())
                if remaining > max(period, retry) + 30:
                    raise ValueError("saved refresh clock exceeds cadence")
                self.next_due[component.name] = stamp.timestamp()
                component.retry_at = self.clock() + remaining
                component.state = "BACKOFF" if saved.get("consecutive_failures", 0) else "WAITING"
                component.failures = saved.get("consecutive_failures", 0)
                component.reason = saved.get("reason")
                component.exit_code = saved.get("last_exit_code")
            elif saved.get("state") in ("RUNNING", "UNVERIFIED", "STOP_FAILED"):
                self.failed(component, self.clock(), "PREVIOUS_ATTEMPT_INTERRUPTED")


def argument_value(arguments, name):
    for position, value in enumerate(arguments):
        if value.startswith(name + "="):
            return value.split("=", 1)[1]
        if value == name:
            return arguments[position + 1] if position + 1 < len(arguments) else None
    return None


def group_conflicts(group, *, scope="all", backend=BACKEND, processes=None):
    paths = {str(Path(command[4]).resolve()).casefold() for command in group_commands(group, scope=scope, backend=backend).values()}
    group_path = (backend / "scripts/run_worker_group.py").resolve()
    names = {Path(command[4]).name.casefold() for command in group_commands(group, scope=scope, backend=backend).values()} | {group_path.name.casefold()}
    conflicts = []
    own = {os.getpid(), *(process.pid for process in psutil.Process().parents())}
    for process in processes if processes is not None else psutil.process_iter():
        if process.pid in own:
            continue
        try:
            path = process_script(process, names)
            if path is None:
                continue
            arguments = process.cmdline()
            flags = {value.split("=", 1)[0] for value in arguments}
            if flags & {"--plan", "--status", "--check", "--stop", "--check-technical-launch"}:
                continue
            if path == group_path and argument_value(arguments, "--group") == group:
                conflicts.append(dict(pid=process.pid, kind="GROUP_ALREADY_RUNNING"))
            elif str(path).casefold() in paths:
                conflicts.append(dict(pid=process.pid, kind="COMPONENT_ALREADY_RUNNING", script=path.name))
        except (psutil.NoSuchProcess, psutil.ZombieProcess):
            continue
        except psutil.AccessDenied as error:
            if process.name().lower() in ("python.exe", "pythonw.exe", "python", "python3"):
                raise RuntimeError("unable to verify Python process ownership") from error
    return conflicts


def read_state(group, root=None):
    root = root or STATE_ROOT
    path = root / group / "status.json"
    if not path.exists():
        return {}
    if path.stat().st_size > 262144:
        raise ValueError("worker group status exceeds bound")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("version") != "worker_group_v1" or payload.get("group") != group:
        raise ValueError("worker group status identity mismatch")
    return payload


def group_status(group, *, root=None):
    root = root or STATE_ROOT
    payload = read_state(group, root)
    if not payload:
        return dict(group=group, state="NOT_RUNNING", service_process_alive=False)
    try:
        process = psutil.Process(payload["pid"])
        alive = (process.create_time() == payload["process_created_at"]
            and process_script(process, {Path(__file__).name}) == Path(__file__).resolve()
            and argument_value(process.cmdline(), "--group") == group)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        alive = False
    age = (datetime.now(timezone.utc) - datetime.fromisoformat(payload["checked_at"])).total_seconds()
    payload.update(service_process_alive=alive, heartbeat_age_seconds=round(age, 1))
    if not alive or not 0 <= age <= 30:
        payload["state"] = "STOPPED" if payload["state"] == "STOPPED" else "STALE"
        for component in payload.get("components", {}).values():
            if component["state"] == "RUNNING":
                component["state"] = "UNVERIFIED"
    return payload


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--group", required=True, choices=GROUPS)
    parser.add_argument("--scope", default="all", choices=("all", "equity", "options"))
    operations = parser.add_mutually_exclusive_group()
    for operation in ("plan", "check", "status", "stop"):
        operations.add_argument("--" + operation, action="store_true")
    args = parser.parse_args(argv)
    if args.group != "references" and args.scope != "all":
        parser.error("scope is only supported for the shared reference group")
    commands = group_commands(args.group, scope=args.scope)
    if args.plan:
        schedules = reference_schedules(commands, os.environ) if args.group == "references" else {}
        print(json.dumps(dict(mode="PLAN_ONLY", group=args.group, scope=args.scope, commands=commands,
            schedules=schedules, provider_requests=False, data_writes=False), indent=2))
        return 0
    if args.status or args.stop:
        result = group_status(args.group)
        if args.stop and result.get("service_process_alive"):
            write_status(STATE_ROOT / args.group / "stop.json", dict(instance_id=result["instance_id"]))
            process = psutil.Process(result["pid"])
            if process.create_time() != result["process_created_at"]:
                raise RuntimeError("service identity changed; stop not confirmed")
            try:
                process.wait(timeout=30)
            except psutil.TimeoutExpired as error:
                raise RuntimeError("group shutdown not confirmed; no unrelated process stopped") from error
            result = group_status(args.group)
            if result.get("state") != "STOPPED":
                raise RuntimeError("group exited without confirming owned-child shutdown")
        print(json.dumps(result, indent=2))
        return 0
    from dotenv import load_dotenv
    load_dotenv(BACKEND / ".env")
    schedules = reference_schedules(commands, os.environ) if args.group == "references" else {}
    for command in commands.values():
        if not Path(command[0]).is_file() or not Path(command[4]).is_file():
            raise ValueError("worker group entry point or interpreter unavailable")
    conflicts = group_conflicts(args.group, scope=args.scope)
    if args.check:
        print(json.dumps(dict(status="CONFLICT" if conflicts else "PROCESS_SCOPE_READY", conflicts=conflicts,
            group=args.group, scope=args.scope, schedules=schedules, provider_requests=False, data_writes=False,
            source_readiness="NOT_CHECKED"), indent=2))
        return 1 if conflicts else 0
    if conflicts:
        raise RuntimeError("group or standalone component already running; reviewed cutover required")
    if os.name != "nt":
        raise RuntimeError("worker groups currently support native Windows only")
    probe = WindowsJob()
    probe.close()
    from database import get_db_connection

    root = STATE_ROOT / args.group
    root.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("worker-group-" + args.group)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    handlers = (logging.StreamHandler(), RotatingFileHandler(root / "service.log", maxBytes=2_000_000, backupCount=3, encoding="utf-8"))
    for handler in handlers:
        handler.setFormatter(RedactedFormatter(os.environ))
        logger.addHandler(handler)
    stopped = threading.Event()
    previous_signals = {}
    for event in (signal.SIGINT, signal.SIGTERM, signal.SIGBREAK):
        previous_signals[event] = signal.signal(event, lambda *_: stopped.set())
    identity = dict(version="worker_group_v1", group=args.group, scope=args.scope, instance_id=str(uuid4()),
        pid=os.getpid(), process_created_at=psutil.Process().create_time())
    supervisor = WorkerGroupSupervisor(commands,
        lambda name, command: ManagedProcess(name, command, cwd=BACKEND.parent, environment=dict(os.environ), logger=logger),
        schedules=schedules)
    def save():
        write_status(root / "status.json", dict(identity, checked_at=datetime.now(timezone.utc).isoformat(), **supervisor.snapshot()))
    try:
        with get_db_connection() as connection:
            lock = "worker-group-v1:" + args.group
            with connection.cursor() as cursor:
                cursor.execute("SET statement_timeout='5s'")
                cursor.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (lock,))
                acquired = cursor.fetchone()[0]
            connection.commit()
            if not acquired:
                raise RuntimeError("another group owns leadership")
            registered = False
            try:
                if group_conflicts(args.group, scope=args.scope):
                    raise RuntimeError("worker appeared during startup; no child started")
                supervisor.restore_schedule(read_state(args.group))
                registered = True
                logger.info("GROUP_STARTED group=%s scope=%s", args.group, args.scope)
                while not stopped.is_set():
                    with connection.cursor() as cursor:
                        cursor.execute("SELECT 1")
                    connection.commit()
                    if supervisor.tick():
                        logger.info("COMPONENT_STATE %s", json.dumps(supervisor.snapshot()))
                    save()
                    request = root / "stop.json"
                    if request.is_file() and request.stat().st_size <= 1024:
                        if json.loads(request.read_text(encoding="utf-8")).get("instance_id") == identity["instance_id"]:
                            stopped.set()
                            request.unlink()
                    stopped.wait(2)
            except BaseException:
                logger.exception("GROUP_STOPPING_ON_ERROR group=%s", args.group)
                raise
            finally:
                try:
                    supervisor.close()
                    if registered:
                        save()
                finally:
                    with connection.cursor() as cursor:
                        cursor.execute("SELECT pg_advisory_unlock(hashtext(%s))", (lock,))
                    connection.commit()
    finally:
        for event, handler in previous_signals.items():
            signal.signal(event, handler)
        for handler in handlers:
            logger.removeHandler(handler)
            handler.close()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(json.dumps(dict(state="GROUP_START_OR_RUNTIME_FAILED", reason=type(error).__name__)), file=sys.stderr)
        raise SystemExit(1)