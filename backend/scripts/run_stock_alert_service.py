"""Supervise enrolled stock alert strategies and result projection independently."""
from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
import argparse
import ctypes
from ctypes import wintypes
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import re
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from uuid import uuid4
import zlib

import psutil


BACKEND = Path(__file__).resolve().parents[1]
STATE_ROOT = BACKEND / "backups/stock-alert-service"


def component_commands(backend=BACKEND, executable=sys.executable):
    worker = str(backend / "scripts/run_stock_idea_worker.py")
    prefix = (str(executable), "-X", "utf8", "-u")
    return {
        "intraday": (*prefix, worker, "--quality-version", "2", "--state-dir",
            str(backend / "backups/equity-shadow/stock-ideas-forward-v2")),
        "swing": (*prefix, worker, "--quality-version", "2", "--swing", "--activate-swing-shadow", "--state-dir",
            str(backend / "backups/equity-shadow/stock-ideas-swing-v1")),
        "results": (*prefix, str(backend / "scripts/project_stock_alert_results.py"), "--continuous"),
    }


def check_enrollments(backend=BACKEND, *, policies=None):
    if str(BACKEND) not in sys.path:
        sys.path.insert(0, str(BACKEND))
    from research.stock_idea_engine import digest
    from research.stock_idea_forward import forward_config

    if policies is None:
        policies = {name: forward_config(quality_version=2, swing=name == "swing") for name in ("intraday", "swing")}
    configured_view = os.getenv("STOCK_ALERT_SHADOW_VIEW")
    expected_view = backend / "backups/equity-shadow/stock-ideas-forward-v2/alerts-view.json"
    if configured_view and Path(configured_view).resolve() != expected_view.resolve():
        raise ValueError("service requires the existing default intraday view; custom source paths need separate review")
    states = {
        "intraday": ("stock-ideas-forward-v2", "stock_ideas_forward_quality_v2"),
        "swing": ("stock-ideas-swing-v1", "stock_ideas_forward_swing_v1"),
    }
    result = {}
    for name, (directory, version) in states.items():
        path = backend / "backups/equity-shadow" / directory / "forward.sqlite"
        if not path.is_file():
            raise ValueError(f"{name} is not enrolled; service never creates an enrollment")
        with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=5)) as connection:
            connection.execute("PRAGMA query_only=ON")
            connection.execute("BEGIN")
            row = connection.execute("SELECT policy_hash,CASE WHEN length(payload)<=262144 THEN payload END FROM forward_manifest WHERE singleton=1").fetchone()
            checkpoint = connection.execute("SELECT CASE WHEN length(payload)<=4194304 THEN payload END FROM forward_checkpoint WHERE singleton=1").fetchone()
            if not row or not row[1] or not checkpoint or not checkpoint[0] or json.loads(row[1]).get("policy_version") != version:
                raise ValueError(f"{name} enrollment or policy is unavailable")
            if digest(json.loads(row[1])) != row[0] or digest(policies[name]) != row[0]:
                raise ValueError(f"{name} stored policy does not match current worker policy")
            decoder = zlib.decompressobj()
            payload = decoder.decompress(checkpoint[0], 16777217)
            if len(payload) > 16777216 or not decoder.eof or decoder.unused_data:
                raise ValueError(f"{name} enrollment checkpoint exceeds bound or is invalid")
            state = json.loads(payload)
            if not isinstance(state.get("members"), list) or not 1 <= len(state["members"]) <= 1000 or not state.get("enrolled_at"):
                raise ValueError(f"{name} has no retained enrollment identity")
            result[name] = dict(policy_version=version, policy_sha256=row[0], enrolled_at=state["enrolled_at"],
                enrollment_sha256=digest(state["members"]))
    return result


@dataclass
class Component:
    name: str
    command: tuple[str, ...]
    process: object = None
    state: str = "STARTING"
    failures: int = 0
    attempts: int = 0
    retry_at: float = 0
    started: float = 0
    started_at: str | None = None
    exit_code: int | None = None
    reason: str | None = None


class StockAlertSupervisor:
    def __init__(self, commands, spawn, *, clock=time.monotonic, max_failures=5, stable_seconds=300):
        self.components = [Component(name, tuple(command)) for name, command in commands.items()]
        self.spawn = spawn
        self.clock = clock
        self.max_failures = max_failures
        self.stable_seconds = stable_seconds
        self.stopped = False

    def failed(self, component, now, reason):
        component.failures += 1
        component.reason = reason
        component.state = "FAILED" if component.failures >= self.max_failures else "BACKOFF"
        component.retry_at = now + min(300, 5 * 2 ** (component.failures - 1))

    def tick(self):
        if self.stopped:
            return False
        changed = False
        for component in self.components:
            now = self.clock()
            if component.process is not None:
                code = component.process.poll()
                if code is None:
                    continue
                component.process.close()
                component.process = None
                component.exit_code = code
                if now - component.started >= self.stable_seconds:
                    component.failures = 0
                self.failed(component, now, "UNEXPECTED_EXIT")
                changed = True
            elif component.state != "FAILED" and now >= component.retry_at:
                component.attempts += 1
                try:
                    component.process = self.spawn(component.name, component.command)
                except (ValueError, sqlite3.Error, zlib.error):
                    component.state, component.reason = "FAILED", "LAUNCH_CONTRACT_UNAVAILABLE"
                except OSError:
                    self.failed(component, now, "PROCESS_START_FAILED")
                else:
                    component.state, component.started = "RUNNING", now
                    component.started_at = datetime.now(timezone.utc).isoformat()
                    component.reason = None
                changed = True
        return changed

    def snapshot(self):
        now = self.clock()
        states = {component.state for component in self.components}
        return dict(state=("STOPPED" if states == {"STOPPED"} else "STOP_FAILED") if self.stopped
            else "RUNNING" if states == {"RUNNING"} else "DEGRADED",
            health_basis="PROCESS_LIFECYCLE_NOT_SIGNAL_OR_DATA_READINESS",
            components={component.name: dict(state=component.state,
                pid=component.process.pid if component.process is not None else None,
                attempts=component.attempts, consecutive_failures=component.failures,
                retry_in_seconds=max(0, round(component.retry_at - now, 1)) if component.state == "BACKOFF" else None,
                started_at=component.started_at, last_exit_code=component.exit_code, reason=component.reason)
                for component in self.components})

    def close(self):
        self.stopped = True
        errors = []
        for component in reversed(self.components):
            if component.process is not None:
                try:
                    component.process.close()
                except (OSError, subprocess.TimeoutExpired):
                    errors.append(component.name)
                    component.state = "STOP_FAILED"
                    continue
            component.process, component.state = None, "STOPPED"
        if errors:
            raise RuntimeError("Failed to stop owned children: " + ", ".join(errors))


class WindowsJob:
    def __init__(self):
        if os.name != "nt":
            raise OSError("The stock alert service currently requires Windows job objects")
        class Limits(ctypes.Structure):
            _fields_ = [("process_time", ctypes.c_longlong), ("job_time", ctypes.c_longlong),
                ("flags", wintypes.DWORD), ("minimum_working_set", ctypes.c_size_t), ("maximum_working_set", ctypes.c_size_t),
                ("active_process_limit", wintypes.DWORD), ("affinity", ctypes.c_size_t),
                ("priority", wintypes.DWORD), ("scheduling", wintypes.DWORD)]
        class ExtendedLimits(ctypes.Structure):
            _fields_ = [("basic", Limits), ("io_counters", ctypes.c_ulonglong * 6),
                ("process_memory", ctypes.c_size_t), ("job_memory", ctypes.c_size_t),
                ("peak_process_memory", ctypes.c_size_t), ("peak_job_memory", ctypes.c_size_t)]
        self.kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        self.kernel.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
        self.kernel.CreateJobObjectW.restype = wintypes.HANDLE
        self.kernel.SetInformationJobObject.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD]
        self.kernel.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
        self.kernel.CloseHandle.argtypes = [wintypes.HANDLE]
        self.handle = self.kernel.CreateJobObjectW(None, None)
        if not self.handle:
            raise ctypes.WinError(ctypes.get_last_error())
        limits = ExtendedLimits()
        limits.basic.flags = 0x2000
        if not self.kernel.SetInformationJobObject(self.handle, 9, ctypes.byref(limits), ctypes.sizeof(limits)):
            error = ctypes.WinError(ctypes.get_last_error())
            self.close()
            raise error

    def assign(self, process):
        if not self.kernel.AssignProcessToJobObject(self.handle, int(process._handle)):
            raise ctypes.WinError(ctypes.get_last_error())

    def close(self):
        if self.handle:
            handle, self.handle = self.handle, None
            if not self.kernel.CloseHandle(handle):
                raise ctypes.WinError(ctypes.get_last_error())


class ManagedProcess:
    def __init__(self, name, command, *, cwd=None, environment=None, logger=None):
        self.job = WindowsJob()
        self.process = None
        self.reader = None
        self.logger = logger or logging.getLogger("stock-alert-service")
        try:
            self.process = subprocess.Popen(command, cwd=cwd, env=environment, stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace",
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | 0x00000004)
            self.job.assign(self.process)
            psutil.Process(self.process.pid).resume()
            self.reader = threading.Thread(target=self.drain, args=(name,), daemon=True)
            self.reader.start()
        except BaseException:
            self.job.close()
            if self.process is not None:
                if self.process.poll() is None:
                    self.process.kill()
                self.process.wait(timeout=5)
            raise

    @property
    def pid(self):
        return self.process.pid

    def poll(self):
        return self.process.poll()

    def drain(self, name):
        for line in self.process.stdout:
            self.logger.info("%s | %s", name, line.rstrip())

    def close(self):
        if self.process.poll() is None:
            try:
                self.process.send_signal(signal.CTRL_BREAK_EVENT)
                self.process.wait(timeout=5)
            except (OSError, subprocess.TimeoutExpired):
                pass
        self.job.close()
        self.process.wait(timeout=5)
        if self.reader is not None:
            self.reader.join(timeout=2)
        self.process.stdout.close()


class RedactedFormatter(logging.Formatter):
    def __init__(self, environment):
        super().__init__("%(asctime)s | %(levelname)s | %(message)s")
        self.secrets = sorted({value for key, value in environment.items()
            if re.search(r"key|token|secret|password|credential", key, re.I) and value and len(value) >= 4}, key=len, reverse=True)

    def format(self, record):
        text = super().format(record)
        for secret in self.secrets:
            text = text.replace(secret, "[REDACTED]")
        text = re.sub(r"(?i)((?:api[_-]?key|token|password|authorization)\s*[=:]\s*)[^\s&]+", r"\1[REDACTED]", text)
        return text[:16000] + (" [TRUNCATED]" if len(text) > 16000 else "")


def write_status(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, suffix=".tmp", delete=False) as output:
            temporary = output.name
            json.dump(payload, output, allow_nan=False, sort_keys=True)
            output.flush()
            os.fsync(output.fileno())
        for attempt in range(3):
            try:
                os.replace(temporary, path)
                break
            except PermissionError:
                if attempt == 2:
                    raise
                time.sleep(0.05)
    finally:
        if temporary and Path(temporary).exists():
            Path(temporary).unlink()


def process_script(process, names):
    arguments = process.cmdline()
    options = iter(arguments[1:])
    for argument in options:
        if argument in ("-m", "-c"):
            return None
        if argument in ("-X", "-W"):
            next(options, None)
            continue
        if argument.startswith("-"):
            continue
        if Path(argument).name.casefold() not in names:
            return None
        path = Path(argument)
        return (path if path.is_absolute() else Path(process.cwd()) / path).resolve()
    return None


def standalone_processes(backend=BACKEND):
    scripts = {str((backend / "scripts/run_stock_idea_worker.py").resolve()).casefold(): "intraday",
        str((backend / "scripts/project_stock_alert_results.py").resolve()).casefold(): "results"}
    result = {name: [] for name in ("intraday", "swing", "results")}
    for process in psutil.process_iter(["name", "cmdline", "ppid", "create_time"]):
        if process.info["name"] not in ("python.exe", "pythonw.exe"):
            continue
        arguments = process.info["cmdline"] or []
        if not any(Path(argument).name.casefold() in {"run_stock_idea_worker.py", "project_stock_alert_results.py"} for argument in arguments[1:]):
            continue
        try:
            path = process_script(process, {"run_stock_idea_worker.py", "project_stock_alert_results.py"})
        except psutil.NoSuchProcess:
            continue
        matched = scripts.get(str(path).casefold())
        if matched:
            prohibited = {"--once", "--plan", "--status", "--check-readiness", "--check-storage", "--verify",
                "--publish-reader", "--reconcile-history", "--recover-corrections", "--quarantine-security-id",
                "--retry-window", "--enable-source-readiness"}
            if any(argument.split("=", 1)[0] in prohibited for argument in arguments):
                raise ValueError("alert maintenance or inspection is running; retry service start after it exits")
            flags = arguments[arguments.index(next(argument for argument in arguments if Path(argument).name.casefold() == path.name.casefold())) + 1:]
            allowed = {"--quality-version", "--state-dir", "--swing", "--activate-swing-shadow"} if matched == "intraday" else {"--continuous"}
            if any(flag.split("=", 1)[0] not in allowed for flag in flags if flag.startswith("--")):
                raise ValueError("unrecognized alert writer arguments; refusing takeover")
            def option_value(name):
                for position, flag in enumerate(flags):
                    if flag.startswith(name + "="):
                        return flag.split("=", 1)[1]
                    if flag == name:
                        return flags[position + 1] if position + 1 < len(flags) else ""
                return None
            if option_value("--quality-version") not in (None, "2"):
                raise ValueError("non-current stock policy process is running; refusing takeover")
            state_dir = option_value("--state-dir")
            if state_dir is not None:
                root = backend / "backups/equity-shadow" / ("stock-ideas-swing-v1" if "--swing" in arguments else "stock-ideas-forward-v2")
                supplied = Path(state_dir) if state_dir else None
                if supplied is None or (supplied if supplied.is_absolute() else Path(process.cwd()) / supplied).resolve() != root.resolve():
                    raise ValueError("custom stock alert store is running; refusing service takeover")
            result["swing" if matched == "intraday" and "--swing" in arguments else matched].append(process)
    for name, processes in result.items():
        identities = {process.pid for process in processes}
        roots = [process for process in processes if process.info["ppid"] not in identities]
        if len(roots) > 1:
            raise ValueError(f"multiple {name} process roots; refusing ambiguous takeover")
    return result


def stop_standalone(processes):
    for members in reversed(list(processes.values())):
        for process in reversed(members):
            try:
                if process.create_time() != process.info["create_time"]:
                    raise RuntimeError("Standalone process identity changed; takeover refused")
                process.kill()
            except psutil.NoSuchProcess:
                pass
        _, alive = psutil.wait_procs(members, timeout=10)
        if alive:
            raise RuntimeError("Standalone alert processes remain; replacement not started")


def retained_progress(backend=BACKEND):
    result = {}
    for name, relative in (("intraday", "equity-shadow/stock-ideas-forward-v2/alerts-view.json"),
            ("swing", "equity-shadow/stock-ideas-swing-v1/alerts-view.json"), ("results", "stock-alert-results/projector-status.json")):
        try:
            path = backend / "backups" / relative
            if path.stat().st_size > 20_000_000:
                raise ValueError("source status exceeds read bound")
            payload = json.loads(path.read_text(encoding="utf-8"))
            if name == "results":
                result[name] = dict(checked_at=payload.get("checked_at"), streams=[{key: row.get(key)
                    for key in ("stream", "status", "source_as_of", "error")} for row in payload.get("streams", [])])
            else:
                latest = max(payload.get("publications", []), key=lambda row: row["published_at"], default={})
                result[name] = dict(source_as_of=payload.get("as_of"), next_boundary=payload.get("worker", {}).get("next_boundary"),
                    latest_publication={key: latest.get(key) for key in ("run_id", "published_at", "status", "selected")})
        except (OSError, ValueError, KeyError, TypeError):
            result[name] = dict(status="UNAVAILABLE")
    return result


def service_status(root=STATE_ROOT):
    path = root / "status.json"
    if not path.is_file():
        return dict(state="NOT_RUNNING", service_process_alive=False)
    if path.stat().st_size > 262144:
        raise ValueError("service status exceeds bound")
    payload = json.loads(path.read_text(encoding="utf-8"))
    try:
        process = psutil.Process(payload["pid"])
        alive = process.create_time() == payload["process_created_at"] and process_script(process, {Path(__file__).name}) == Path(__file__).resolve()
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        alive = False
    payload["service_process_alive"] = alive
    payload["heartbeat_age_seconds"] = round((datetime.now(timezone.utc) - datetime.fromisoformat(payload["checked_at"])).total_seconds(), 1)
    if not alive or payload["heartbeat_age_seconds"] > 30:
        payload["state"] = "STALE" if payload["state"] != "STOPPED" else "STOPPED"
        for component in payload.get("components", {}).values():
            if component.get("state") == "RUNNING":
                component["state"] = "UNVERIFIED"
    return payload


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    operation = parser.add_mutually_exclusive_group()
    operation.add_argument("--plan", action="store_true")
    operation.add_argument("--status", action="store_true")
    operation.add_argument("--check", action="store_true")
    operation.add_argument("--stop", action="store_true")
    parser.add_argument("--replace-standalone", action="store_true", help="Explicitly stop only same-workspace standalone alert writers/projector before takeover.")
    args = parser.parse_args(argv)
    if args.replace_standalone and (args.plan or args.status or args.check or args.stop):
        parser.error("takeover is only available when starting the service")
    commands = component_commands()
    if args.plan:
        print(json.dumps(dict(mode="PLAN_ONLY", components=commands, requires_existing_enrollments=True, source_writes=False), indent=2))
        return 0
    if args.status or args.stop:
        status = service_status()
        if args.stop and status.get("service_process_alive"):
            write_status(STATE_ROOT / "stop.json", dict(instance_id=status["instance_id"]))
            process = psutil.Process(status["pid"])
            if process.create_time() == status["process_created_at"]:
                try:
                    process.wait(timeout=30)
                except psutil.TimeoutExpired as error:
                    raise RuntimeError("Service shutdown not confirmed; no unrelated process was stopped") from error
            status = service_status()
        print(json.dumps(dict(status, retained_progress=retained_progress()), indent=2))
        return 0
    from dotenv import load_dotenv
    load_dotenv(BACKEND / ".env")
    enrollment = check_enrollments()
    for command in commands.values():
        if not Path(command[0]).is_file() or not Path(command[4]).is_file():
            raise ValueError("required interpreter or child entry point unavailable")
    if args.check:
        existing = standalone_processes()
        print(json.dumps(dict(status="ENROLLMENTS_READY", enrollment=enrollment,
            existing_processes={name: [process.pid for process in processes] for name, processes in existing.items()},
            process_readiness_is_not_signal_readiness=True), indent=2))
        return 0
    if os.name != "nt":
        raise RuntimeError("Supervised stock alerts currently support native Windows only")
    job_probe = WindowsJob()
    job_probe.close()
    if str(BACKEND) not in sys.path:
        sys.path.insert(0, str(BACKEND))
    from database import get_db_connection

    STATE_ROOT.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("stock-alert-service")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    formatter = RedactedFormatter(dict(os.environ))
    for handler in (logging.StreamHandler(), RotatingFileHandler(STATE_ROOT / "service.log", maxBytes=2_000_000, backupCount=3, encoding="utf-8")):
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    stopped = threading.Event()
    for event in (signal.SIGINT, signal.SIGTERM, signal.SIGBREAK):
        signal.signal(event, lambda *_: stopped.set())
    instance = str(uuid4())
    identity = dict(version="stock_alert_service_v1", instance_id=instance, pid=os.getpid(),
        process_created_at=psutil.Process().create_time(), enrollment=enrollment)
    def spawn(name, command):
        if check_enrollments() != enrollment:
            raise ValueError("retained enrollment changed; automatic restart refused")
        return ManagedProcess(name, command, cwd=BACKEND.parent, environment=dict(os.environ), logger=logger)
    supervisor = StockAlertSupervisor(commands, spawn)
    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET statement_timeout='5s'")
            cursor.execute("SELECT pg_try_advisory_lock(hashtext('stock-alert-service-v1'))")
            if not cursor.fetchone()[0]:
                logger.warning("ALREADY_RUNNING; no child processes changed")
                return 2
        connection.commit()
        try:
            existing = standalone_processes()
            if any(existing.values()):
                if not args.replace_standalone:
                    raise ValueError("Standalone alert components already running; explicit --replace-standalone required")
                stop_standalone(existing)
            logger.info("SERVICE_STARTED instance=%s; existing enrollments retained", instance)
            while not stopped.is_set():
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                connection.commit()
                if supervisor.tick():
                    logger.info("COMPONENT_STATE %s", json.dumps(supervisor.snapshot()))
                write_status(STATE_ROOT / "status.json", dict(identity, checked_at=datetime.now(timezone.utc).isoformat(), **supervisor.snapshot()))
                request = STATE_ROOT / "stop.json"
                if request.is_file() and request.stat().st_size <= 1024:
                    if json.loads(request.read_text(encoding="utf-8")).get("instance_id") == instance:
                        stopped.set()
                        request.unlink()
                stopped.wait(2)
        finally:
            try:
                supervisor.close()
            finally:
                write_status(STATE_ROOT / "status.json", dict(identity, checked_at=datetime.now(timezone.utc).isoformat(), **supervisor.snapshot()))
                with connection.cursor() as cursor:
                    cursor.execute("SELECT pg_advisory_unlock(hashtext('stock-alert-service-v1'))")
                connection.commit()
                logger.info("SERVICE_STOPPED instance=%s", instance)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        logger = logging.getLogger("stock-alert-service")
        if logger.handlers:
            logger.exception("Service stopped: %s", type(error).__name__)
        else:
            print(json.dumps(dict(state="START_FAILED", reason=type(error).__name__)), file=sys.stderr)
        raise SystemExit(1)