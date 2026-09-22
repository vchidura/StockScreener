import json
import os
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[2]
LAUNCHER = ROOT / "backend" / "scripts" / "start_workers.ps1"
POWERSHELL = shutil.which("powershell.exe")
pytestmark = pytest.mark.skipif(os.name != "nt" or not POWERSHELL, reason="Windows PowerShell launcher")
EQUITY = ["run_equity_worker.py", "run_corporate_action_worker.py", "run_stock_alert_service.py", "refresh_equity_portal_snapshots.py", "run_screening_worker.py"]
ONESHOT_EQUITY = [*EQUITY[:2], "run_stock_idea_worker.py", *EQUITY[3:]]
OPTIONS = ["run_market_event_worker.py", "run_option_model_input_worker.py", "run_option_worker.py"]
WORKERS = dict(zip(["Equity", "CorporateActions", "StockAlerts", "Portal", "Screening", "MarketEvents", "OptionInputs", "Options"], EQUITY + OPTIONS))
WORKERS["AlertContext"] = "run_stock_alert_context_worker.py"
WORKERS["MarketContext"] = "prepare_stock_alert_context.py"
WORKERS["SwingAlerts"] = "run_stock_idea_worker.py"
WORKERS["AlertResults"] = "project_stock_alert_results.py"
WORKERS["UIProjections"] = "run_worker_group.py"
WORKERS["ReferenceRefresh"] = "run_worker_group.py"
GROUPED_EQUITY = [EQUITY[0], EQUITY[2], "run_worker_group.py", "run_worker_group.py"]
GROUPED_OPTIONS = ["run_worker_group.py", "run_worker_group.py"]


def run_test_command(command, cwd):
    captured = "try { & {\n" + command + "\n} 3>&1 | ForEach-Object { [Console]::Out.WriteLine($_.ToString()) } } catch { [Console]::Error.WriteLine($_.Exception.Message); exit 1 }"
    return subprocess.run([POWERSHELL, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", captured],
                          cwd=cwd, capture_output=True, text=True, timeout=30)


def preview(*arguments):
    script_path = str(LAUNCHER).replace("'", "''")
    quoted = " ".join(argument if argument.startswith("-") else "'" + argument.replace("'", "''") + "'" for argument in arguments)
    command = rf"""
$ErrorActionPreference = 'Stop'
function Start-Process {{ throw 'Preview attempted a process launch' }}
function Get-CimInstance {{ throw 'Preview attempted process inspection' }}
function Test-Path {{ param($Path, $PathType) if ($Path -like '*python.exe') {{ throw 'Preview checked interpreter' }} Microsoft.PowerShell.Management\Test-Path -Path $Path -PathType $PathType }}
& '{script_path}' -Plan {quoted}
"""
    return run_test_command(command, LAUNCHER.parent)


def worker_lines(result):
    assert result.returncode == 0, result.stdout + result.stderr
    return [line for line in result.stdout.splitlines() if ': "' in line and ' -X utf8 -u ' in line]


@pytest.mark.parametrize("only,names", [("All", GROUPED_EQUITY + ["run_worker_group.py"]), ("Equity", GROUPED_EQUITY), ("Options", GROUPED_OPTIONS)])
def test_plan_lists_exact_worker_sets_without_process_or_database_work(only, names):
    result = preview("-Only", only)
    lines = worker_lines(result)
    assert len(lines) == len(names)
    assert all(name in line for name, line in zip(names, lines))
    if only != "Options":
        assert "--group projections" in lines[2]
        assert "--behavior-shadow" in lines[0]
    references = next(line for line in lines if "--group references" in line)
    assert "--scope " + only.lower() in references
    assert any("--group options" in line for line in lines) == (only != "Equity")
    assert all("run_equity_stream_worker.py" not in line and "replay" not in line for line in lines)
    assert "Plan only: no workers started" in result.stdout


def test_reference_jobs_are_independent_and_success_waits_for_next_refresh():
    from datetime import datetime, timezone
    from scripts.run_worker_group import WorkerGroupSupervisor

    class Process:
        def __init__(self, pid):
            self.pid, self.code, self.closed = pid, None, False
        def poll(self):
            return self.code
        def close(self):
            self.closed = True
    now, launched = [0], {}
    def spawn(name, command):
        process = Process(len(launched) + 1)
        launched.setdefault(name, []).append(process)
        return process
    service = WorkerGroupSupervisor(dict(actions=("a",), calendar=("c",), inputs=("i",)), spawn,
        schedules={name: (21600, 300) for name in ("actions", "calendar", "inputs")}, clock=lambda: now[0],
        utc_clock=lambda: datetime.fromtimestamp(now[0], timezone.utc))
    service.tick()
    assert set(launched) == {"actions", "calendar", "inputs"}
    launched["actions"][0].code = 0
    launched["calendar"][0].code = 1
    service.tick()
    state = service.snapshot()["components"]
    assert state["actions"]["state"] == "WAITING" and state["actions"]["consecutive_failures"] == 0
    assert state["calendar"]["state"] == "BACKOFF"
    assert state["inputs"]["state"] == "RUNNING"
    now[0] = 299
    service.tick()
    assert all(len(rows) == 1 for rows in launched.values())
    now[0] = 300
    service.tick()
    assert len(launched["calendar"]) == 2 and len(launched["actions"]) == 1
    now[0] = 21600
    service.tick()
    assert len(launched["actions"]) == 2 and len(launched["inputs"]) == 1
    service.close()
    assert all(process.closed for processes in launched.values() for process in processes)


def test_worker_group_commands_preserve_worker_behavior_and_scope():
    from scripts.run_worker_group import group_commands, reference_schedules

    assert set(group_commands("references")) == {"corporate-actions", "market-events", "option-inputs"}
    assert set(group_commands("references", scope="equity")) == {"corporate-actions"}
    assert set(group_commands("references", scope="options")) == {"market-events", "option-inputs"}
    assert all(command[-1] == "--once" for command in group_commands("references").values())
    assert set(group_commands("projections")) == {"portal", "screening"}
    assert group_commands("projections")["portal"][-1] == "--continuous"
    assert Path(group_commands("options")["options"][-1]).name == "run_option_worker.py"
    assert reference_schedules(("market-events",), {}) == {"market-events": (21600, 300)}
    with pytest.raises(ValueError):
        reference_schedules(("market-events",), {"MARKET_EVENT_POLL_SECONDS": "0"})


def test_reference_schedule_survives_restart_without_duplicate_refresh():
    from datetime import datetime, timezone
    from scripts.run_worker_group import WorkerGroupSupervisor

    service = WorkerGroupSupervisor(dict(events=("event",)), lambda *args: None,
        schedules={"events": (21600, 300)}, clock=lambda: 10,
        utc_clock=lambda: datetime.fromtimestamp(1000, timezone.utc))
    previous = dict(components=dict(events=dict(refresh_seconds=21600, retry_seconds=300,
        last_successful_exit_at=datetime.fromtimestamp(500, timezone.utc).isoformat(),
        next_due_at=datetime.fromtimestamp(22100, timezone.utc).isoformat(), consecutive_failures=0)))
    service.restore_schedule(previous)
    assert service.components[0].retry_at == 21110
    assert service.snapshot()["components"]["events"]["state"] == "WAITING"
    previous["components"]["events"]["refresh_seconds"] = 1
    with pytest.raises(ValueError, match="cadence differs"):
        service.restore_schedule(previous)


def test_worker_group_conflict_check_is_scoped_and_readonly(tmp_path):
    from types import SimpleNamespace
    from scripts.run_worker_group import group_conflicts

    def process(identity, script, *arguments):
        return SimpleNamespace(pid=identity, cmdline=lambda: ["python.exe", str(script), *arguments], cwd=lambda: str(ROOT))
    group = ROOT / "backend/scripts/run_worker_group.py"
    existing = [process(21, group, "--group", "references", "--scope", "equity"),
        process(22, ROOT / "backend/scripts/run_market_event_worker.py", "--once"),
        process(23, tmp_path / "run_market_event_worker.py"),
        process(24, group, "--group", "references", "--status"),
        process(25, group, "--group", "projections")]
    conflicts = group_conflicts("references", processes=existing)
    assert [row["pid"] for row in conflicts] == [21, 22]
    assert group_conflicts("options", processes=existing) == []


def test_worker_group_plan_never_inspects_processes_or_imports_database(monkeypatch, capsys):
    from scripts import run_worker_group as group

    monkeypatch.setattr(group, "group_conflicts", lambda *args, **kwargs: pytest.fail("plan inspected processes"))
    assert group.main(["--group", "references", "--plan"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["mode"] == "PLAN_ONLY" and payload["data_writes"] is False
    assert len(payload["commands"]) == 3


def test_reference_interrupted_stop_restores_retry_not_an_immediate_fetch():
    from datetime import datetime, timezone
    from types import SimpleNamespace
    from scripts.run_worker_group import WorkerGroupSupervisor

    now = [1000]
    commands, schedules = dict(events=("event",)), {"events": (21600, 300)}
    def make():
        return WorkerGroupSupervisor(commands, lambda *args: SimpleNamespace(pid=1, poll=lambda: None, close=lambda: None),
            schedules=schedules, clock=lambda: now[0], utc_clock=lambda: datetime.fromtimestamp(now[0], timezone.utc))
    service = make()
    service.tick()
    assert service.snapshot()["components"]["events"]["next_due_at"] is None
    service.close()
    saved = service.snapshot()
    now[0] += 10
    replacement = make()
    replacement.restore_schedule(saved)
    replacement.tick()
    assert replacement.components[0].process is None
    assert replacement.snapshot()["components"]["events"]["state"] == "BACKOFF"
    now[0] = 1300
    replacement.tick()
    assert replacement.components[0].process is not None
    replacement.close()


@pytest.mark.parametrize("selection,children", [("projections", 2), ("references", 3), ("options", 1)])
def test_worker_group_lifecycle_uses_only_fake_children_and_bound_stop(monkeypatch, tmp_path, selection, children):
    from contextlib import contextmanager
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    import sys
    from scripts import run_worker_group as group

    cursor, connection = MagicMock(), MagicMock()
    cursor.fetchone.return_value = (True,)
    connection.cursor.return_value.__enter__.return_value = cursor
    @contextmanager
    def database():
        yield connection
    monkeypatch.setitem(sys.modules, "database", SimpleNamespace(get_db_connection=database))
    monkeypatch.setattr("dotenv.load_dotenv", lambda *args: None)
    monkeypatch.setattr(group, "STATE_ROOT", tmp_path)
    monkeypatch.setattr(group, "group_conflicts", lambda *args, **kwargs: [])
    monkeypatch.setattr(group, "WindowsJob", lambda: SimpleNamespace(close=lambda: None))
    spawned, closed = [], []
    def spawn(name, command, **kwargs):
        spawned.append((name, command))
        return SimpleNamespace(pid=90000 + len(spawned), poll=lambda: None, close=lambda: closed.append(name))
    monkeypatch.setattr(group, "ManagedProcess", spawn)
    class StopEvent:
        def __init__(self):
            self.stopped, self.waits = False, 0
        def is_set(self):
            return self.stopped
        def set(self):
            self.stopped = True
        def wait(self, delay):
            if self.stopped:
                return
            self.waits += 1
            assert self.waits <= 3
            state = json.loads((tmp_path / selection / "status.json").read_text())
            assert state["state"] == "RUNNING"
            group.write_status(tmp_path / selection / "stop.json", dict(instance_id="wrong-instance" if self.waits == 1 else state["instance_id"]))
    monkeypatch.setattr(group.threading, "Event", StopEvent)
    assert group.main(["--group", selection]) == 0
    assert len(spawned) == len(closed) == children
    state = json.loads((tmp_path / selection / "status.json").read_text())
    assert state["state"] == "STOPPED" and not (tmp_path / selection / "stop.json").exists()
    queries = [call.args[0] for call in cursor.execute.call_args_list]
    assert queries[1] == "SELECT pg_try_advisory_lock(hashtext(%s))"
    assert queries.count("SELECT 1") == 3
    assert queries[-1] == "SELECT pg_advisory_unlock(hashtext(%s))"
    assert all("INSERT" not in query and "UPDATE" not in query for query in queries)


def test_worker_group_status_is_readonly_and_rejects_reused_pid(monkeypatch, tmp_path):
    from datetime import datetime, timezone
    from types import SimpleNamespace
    from scripts import run_worker_group as group

    assert group.group_status("references", root=tmp_path)["state"] == "NOT_RUNNING"
    assert not list(tmp_path.iterdir())
    group.write_status(tmp_path / "references/status.json", dict(version="worker_group_v1", group="references",
        pid=123, process_created_at=1, state="RUNNING", checked_at=datetime.now(timezone.utc).isoformat(),
        components=dict(events=dict(state="RUNNING"))))
    monkeypatch.setattr(group.psutil, "Process", lambda identity: SimpleNamespace(create_time=lambda: 2))
    value = group.group_status("references", root=tmp_path)
    assert value["state"] == "STALE" and value["components"]["events"]["state"] == "UNVERIFIED"


@pytest.mark.parametrize("final_state", ["STOPPED", "STALE", "STOP_FAILED"])
def test_worker_group_stop_requires_confirmed_owned_shutdown(monkeypatch, tmp_path, capsys, final_state):
    from types import SimpleNamespace
    from scripts import run_worker_group as group

    states = iter([dict(service_process_alive=True, instance_id="current", pid=123, process_created_at=1),
        dict(service_process_alive=False, state=final_state)])
    monkeypatch.setattr(group, "STATE_ROOT", tmp_path)
    monkeypatch.setattr(group, "group_status", lambda name: next(states))
    monkeypatch.setattr(group.psutil, "Process", lambda identity: SimpleNamespace(create_time=lambda: 1, wait=lambda timeout: None))
    if final_state == "STOPPED":
        assert group.main(["--group", "projections", "--stop"]) == 0
        assert json.loads(capsys.readouterr().out)["state"] == "STOPPED"
    else:
        with pytest.raises(RuntimeError, match="without confirming"):
            group.main(["--group", "projections", "--stop"])
    assert json.loads((tmp_path / "projections/stop.json").read_text())["instance_id"] == "current"


def test_managed_worker_tasks_resolve_group_picker_and_use_process_execution():
    tasks = json.loads((ROOT / ".vscode/tasks.json").read_text(encoding="utf-8"))
    picker = next(row for row in tasks["inputs"] if row["id"] == "managedWorkerGroup")
    assert picker["type"] == "pickString" and set(picker["options"]) == {"projections", "references", "options"}
    selected = [row for row in tasks["tasks"] if "${input:managedWorkerGroup}" in row.get("args", [])]
    assert len(selected) == 3 and all(row["type"] == "process" for row in selected)
    assert {row["args"][-1] for row in selected} == {"--status", "--check", "--stop"}


def test_one_shot_plan_orders_screening_after_equity_and_uses_explicit_flags():
    lines = worker_lines(preview("-Once", "-PublishScreening", "-IncludePaperStudy", "-RepairSessions", "3"))
    names = ONESHOT_EQUITY[:4] + ["prepare_stock_screening.py", EQUITY[4], "run_equity_paper_tracker.py"] + OPTIONS
    assert len(lines) == len(names)
    assert all(name in line for name, line in zip(names, lines))
    assert "--once --repair-sessions 3" in lines[0]
    assert "--continuous" not in " ".join(lines)
    assert "--publish" in lines[4] and "--once" in lines[5] and "--once" in lines[6]
    assert all("--once" in line for line in lines[6:])


def test_research_watcher_is_opt_in_and_never_enrolls():
    lines = worker_lines(preview("-Only", "Equity", "-IncludePaperStudy"))
    assert len(lines) == 5 and any("--watch" in line for line in lines)
    assert "--enroll" not in " ".join(lines)
    assert all("paper_tracker" not in line for line in worker_lines(preview()))


@pytest.mark.parametrize("name,script", WORKERS.items())
@pytest.mark.parametrize("once", [False, True])
def test_single_worker_plan_starts_only_selected_entry_point(name, script, once):
    if name == "StockAlerts" and once:
        script = "run_stock_idea_worker.py"
    if name == "Options" and not once:
        script = "run_worker_group.py"
    arguments = ["-Worker", name, "-NoNewWindow"] + (["-Once"] if once else [])
    result = preview(*arguments)
    if name in ("MarketContext", "UIProjections", "ReferenceRefresh") and once:
        assert result.returncode != 0 and "continuous only" in result.stderr
        assert ' -X utf8 -u ' not in result.stdout
        return
    lines = worker_lines(result)
    assert len(lines) == 1 and script in lines[0]
    if name in ("Options", "UIProjections", "ReferenceRefresh") and not once:
        assert "--group " + {"Options": "options", "UIProjections": "projections", "ReferenceRefresh": "references"}[name] in lines[0]
    assert ("--once" in lines[0]) == (once and name not in ("Portal", "AlertResults"))
    assert ("--continuous" in lines[0]) == (not once and name in ("Portal", "MarketContext", "AlertResults"))
    assert "--retry-window" not in lines[0] and "--enable-source-readiness" not in lines[0]
    assert ("--quality-version 2" in lines[0]) == (name == "SwingAlerts" or name == "StockAlerts" and once)
    assert ("--behavior-shadow" in lines[0]) == (name == "Equity")
    assert ("--swing" in lines[0]) == (name == "SwingAlerts")
    assert ("--activate-swing-shadow" in lines[0]) == (name == "SwingAlerts")
    assert "Dependencies are not started automatically" in result.stdout


def test_shadow_event_diagnostics_are_explicit_and_cannot_target_alert_selection():
    lines = worker_lines(preview("-Worker", "AlertContext", "-ShadowEvents"))
    assert len(lines) == 1 and "--shadow-events" in lines[0] and "run_stock_alert_context_worker.py" in lines[0]
    assert "--shadow-events" not in " ".join(worker_lines(preview("-Worker", "AlertContext")))
    assert preview("-Worker", "StockAlerts", "-ShadowEvents").returncode != 0
    launched = mocked_start("-Worker", "AlertContext", "-ShadowEvents")
    assert launched.returncode == 0 and "--shadow-events" in launched.stdout


def test_single_ingestion_worker_can_explicitly_request_bounded_repair():
    lines = worker_lines(preview("-Worker", "Equity", "-Once", "-RepairSessions", "3"))
    assert len(lines) == 1 and "--once --repair-sessions 3" in lines[0]


def mocked_start(*arguments, running=(), enrolled=True, launcher=LAUNCHER):
    script_path = str(launcher).replace("'", "''")
    quoted = " ".join(argument if argument.startswith("-") else "'" + argument.replace("'", "''") + "'" for argument in arguments)
    processes = ", ".join("'" + name + "'" for name in running)
    command = rf"""
$ErrorActionPreference = 'Stop'
$runningNames = @({processes})
function Get-CimInstance {{ foreach ($name in $runningNames) {{ [pscustomobject]@{{ CommandLine = $name; ProcessId = 123 }} }} }}
function Test-Path {{
    param($Path, $PathType)
    if ($Path -like '*python.exe') {{ return $true }}
    if ($Path -like '*manifest.json') {{ return ${str(enrolled).lower()} }}
    Microsoft.PowerShell.Management\Test-Path -Path $Path -PathType $PathType
}}
function Start-Process {{
    param($FilePath, $ArgumentList, $WorkingDirectory)
    Write-Host ('MOCK_START ' + (@{{ executable = $FilePath; arguments = $ArgumentList; directory = $WorkingDirectory }} | ConvertTo-Json -Compress))
}}
& '{script_path}' {quoted}
"""
    return run_test_command(command, launcher.parent)


def test_continuous_launch_skips_duplicate_and_quotes_paths_with_spaces(tmp_path):
    scripts = tmp_path / "repo with spaces" / "backend" / "scripts"
    scripts.mkdir(parents=True)
    launcher = scripts / LAUNCHER.name
    shutil.copyfile(LAUNCHER, launcher)
    for name in GROUPED_EQUITY:
        (scripts / name).touch()
    result = mocked_start("-Only", "Equity", running=[EQUITY[0]], launcher=launcher)
    assert result.returncode == 0, result.stdout + result.stderr
    launches = [json.loads(line.removeprefix("MOCK_START ")) for line in result.stdout.splitlines() if line.startswith("MOCK_START ")]
    assert len(launches) == 3 and "already running" in result.stdout
    for name, launch in zip(GROUPED_EQUITY[1:], launches):
        assert launch["executable"] == "powershell.exe"
        assert launch["directory"] == str(scripts.parent.parent)
        assert f"'{scripts / name}'" in launch["arguments"][-1]
    assert "'--group' 'projections'" in launches[-2]["arguments"][-1]


def test_one_shot_duplicates_fail_and_do_not_publish_screening():
    result = mocked_start("-Only", "Equity", "-Once", "-PublishScreening", running=EQUITY)
    assert result.returncode != 0 and "One-shot worker failures" in " ".join(result.stderr.split())
    assert "Skipping screening publication" in result.stdout
    assert "MOCK_START " not in result.stdout


@pytest.mark.parametrize("name", ["MarketEvents", "Screening", "StockAlerts", "AlertContext", "MarketContext", "AlertResults"])
def test_single_worker_launch_and_duplicate_handling(name):
    result = mocked_start("-Worker", name)
    assert result.returncode == 0, result.stdout + result.stderr
    launches = [json.loads(line.removeprefix("MOCK_START ")) for line in result.stdout.splitlines() if line.startswith("MOCK_START ")]
    assert len(launches) == 1 and WORKERS[name] in launches[0]["arguments"][-1]
    duplicate = mocked_start("-Worker", name, running=[WORKERS[name]])
    assert duplicate.returncode == 0 and "already running" in duplicate.stdout
    assert "MOCK_START " not in duplicate.stdout
    once = mocked_start("-Worker", name, "-Once", running=[WORKERS[name]])
    assert once.returncode != 0 and "MOCK_START " not in once.stdout


@pytest.mark.parametrize("standalone", ["run_screening_worker.py", "refresh_equity_portal_snapshots.py --continuous",
    "run_corporate_action_worker.py", "run_market_event_worker.py", "run_option_model_input_worker.py", "run_option_worker.py"])
def test_group_default_refuses_standalone_cutover_before_launching_anything(standalone):
    result = mocked_start(running=[standalone])
    assert result.returncode != 0 and "reviewed group cutover required" in result.stderr
    assert "MOCK_START " not in result.stdout


@pytest.mark.parametrize("worker,owner", [("Portal", "projections"), ("Screening", "projections"),
    ("CorporateActions", "references"), ("MarketEvents", "references"), ("OptionInputs", "references")])
def test_worker_group_blocks_independent_component_launch(worker, owner):
    running = [f"run_worker_group.py --group {owner}"]
    result = mocked_start("-Worker", worker, running=running)
    assert result.returncode == 0 and "already running" in result.stdout and "MOCK_START " not in result.stdout
    assert mocked_start("-Worker", worker, "-Once", running=running).returncode != 0


def test_reference_group_scope_cannot_be_silently_expanded():
    result = mocked_start("-Only", "Options", running=["run_worker_group.py --group references --scope equity"])
    assert result.returncode != 0 and "scope differs" in result.stderr and "MOCK_START " not in result.stdout
    result = mocked_start("-Only", "Options", running=["run_worker_group.py --group references --scope all"])
    assert result.returncode == 0 and result.stdout.count("MOCK_START ") == 1
    launch = next(json.loads(line.removeprefix("MOCK_START ")) for line in result.stdout.splitlines() if line.startswith("MOCK_START "))
    assert "'--group' 'options'" in launch["arguments"][-1]


def test_different_groups_share_entrypoint_without_false_duplicate():
    result = mocked_start("-Worker", "UIProjections", running=["run_worker_group.py --group references"])
    assert result.returncode == 0 and result.stdout.count("MOCK_START ") == 1
    result = mocked_start("-Worker", "Options", running=["run_worker_group.py --group options"])
    assert result.returncode == 0 and "MOCK_START " not in result.stdout
    result = mocked_start("-Worker", "UIProjections", running=["run_worker_group.py --group projections --status"])
    assert result.returncode == 0 and result.stdout.count("MOCK_START ") == 1


def test_legacy_options_stop_refuses_to_kill_a_managed_child():
    path = str(ROOT / "backend/scripts/stop_option_worker.ps1").replace("'", "''")
    group = str(ROOT / "backend/scripts/run_worker_group.py").replace("'", "''")
    result = run_test_command(f"""
function Get-CimInstance {{ [pscustomobject]@{{Name='python.exe'; CommandLine='python.exe {group} --group options'; ProcessId=123}} }}
function Stop-Process {{ throw 'TEST_FAILED_UNEXPECTED_STOP' }}
& '{path}'
""", ROOT)
    assert result.returncode != 0 and "Options is managed" in result.stderr
    assert "No child was stopped" in result.stderr and "TEST_FAILED_UNEXPECTED_STOP" not in result.stderr


def test_market_context_keeps_reviewed_configuration_and_uses_a_separate_window(tmp_path):
    scripts = tmp_path / "repo with spaces" / "backend" / "scripts"
    scripts.mkdir(parents=True)
    launcher = scripts / LAUNCHER.name
    shutil.copyfile(LAUNCHER, launcher)
    (scripts / WORKERS["MarketContext"]).touch()
    result = mocked_start("-Worker", "MarketContext", running=EQUITY + [WORKERS["AlertContext"]], launcher=launcher)
    assert result.returncode == 0, result.stdout + result.stderr
    launches = [json.loads(line.removeprefix("MOCK_START ")) for line in result.stdout.splitlines() if line.startswith("MOCK_START ")]
    assert len(launches) == 1
    launch = launches[0]
    assert launch["executable"] == "powershell.exe" and "-NoExit" in launch["arguments"]
    assert launch["directory"] == str(scripts.parent.parent)
    command = launch["arguments"][-1]
    assert "WindowTitle = 'market-context-worker'" in command
    for flag in ("--rotation-only", "--state-dir", "--split-review", "--publish-rotation-view", "--fetch-macro", "--continuous"):
        assert f"'{flag}'" in command
    for path in ("backups/equity-shadow/stock-ideas-forward-v2", "research/stock_sector_split_reviews.json"):
        assert f"'{scripts.parent / path}'" in command
    assert "--once" not in command and "--shadow-events" not in command


def test_standalone_swing_and_intraday_modes_remain_separate():
    selection = "SwingAlerts"
    other = "run_stock_idea_worker.py --quality-version 2"
    same = "run_stock_idea_worker.py --swing --activate-swing-shadow"
    result = mocked_start("-Worker", selection, running=[other])
    assert result.returncode == 0, result.stdout + result.stderr
    launches = [json.loads(line.removeprefix("MOCK_START ")) for line in result.stdout.splitlines() if line.startswith("MOCK_START ")]
    assert len(launches) == 1
    assert ("'--swing'" in launches[0]["arguments"][-1]) == (selection == "SwingAlerts")
    duplicate = mocked_start("-Worker", selection, running=[other, same])
    assert duplicate.returncode == 0 and "already running" in duplicate.stdout and "MOCK_START " not in duplicate.stdout
    once = mocked_start("-Worker", selection, "-Once", running=[same])
    assert once.returncode != 0 and "MOCK_START " not in once.stdout
    assert all("--swing" not in line for line in worker_lines(preview("-Only", "All")))


@pytest.mark.parametrize("standalone", ["run_stock_idea_worker.py --quality-version 2",
    "run_stock_idea_worker.py --swing --activate-swing-shadow", "project_stock_alert_results.py --continuous"])
def test_service_default_refuses_implicit_takeover_of_standalone_components(standalone):
    result = mocked_start("-Worker", "StockAlerts", running=[standalone])
    assert result.returncode != 0 and "reviewed service takeover" in result.stderr
    assert "MOCK_START " not in result.stdout


@pytest.mark.parametrize("worker", ["StockAlerts", "SwingAlerts", "AlertResults"])
def test_service_prevents_independent_component_launches(worker):
    result = mocked_start("-Worker", worker, running=["run_stock_alert_service.py"])
    assert result.returncode == 0 and "already running" in result.stdout and "MOCK_START " not in result.stdout


def test_service_status_probe_does_not_block_daily_startup():
    result = mocked_start("-Worker", "StockAlerts", running=["run_stock_alert_service.py --status"])
    assert result.returncode == 0 and "MOCK_START " in result.stdout


def test_service_takeover_requires_explicit_scoped_continuous_selection():
    assert preview('-ReplaceAlertWorkers').returncode != 0
    assert preview('-Worker', 'Options', '-ReplaceAlertWorkers').returncode != 0
    assert preview('-Worker', 'StockAlerts', '-Once', '-ReplaceAlertWorkers').returncode != 0
    lines = worker_lines(preview('-Worker', 'StockAlerts', '-ReplaceAlertWorkers'))
    assert len(lines) == 1 and '--replace-standalone' in lines[0]
    result = mocked_start('-Worker', 'StockAlerts', '-ReplaceAlertWorkers', running=['run_stock_idea_worker.py --swing --activate-swing-shadow'])
    assert result.returncode == 0, result.stderr
    launches = [json.loads(line.removeprefix('MOCK_START ')) for line in result.stdout.splitlines() if line.startswith('MOCK_START ')]
    assert len(launches) == 1 and "'--replace-standalone'" in launches[0]['arguments'][-1]
    assert all('--replace-standalone' not in line for line in worker_lines(preview()))


def test_unenrolled_study_fails_before_starting_any_worker():
    result = mocked_start("-IncludePaperStudy", enrolled=False)
    assert result.returncode != 0 and "Paper study is not enrolled" in result.stderr
    assert "MOCK_START " not in result.stdout


def test_no_new_window_is_valid_for_a_sequential_one_shot_plan():
    assert len(worker_lines(preview("-Once", "-NoNewWindow"))) == 8


@pytest.mark.parametrize("arguments,message", [
    (("-NoNewWindow", "-Only", "Equity"), "-NoNewWindow requires -Once"),
    (("-PublishScreening",), "-PublishScreening requires -Once"),
    (("-Only", "Options", "-PublishScreening", "-Once"), "require -Only Equity"),
    (("-Only", "Options", "-IncludePaperStudy"), "require -Only Equity"),
    (("-Only", "Options", "-RepairSessions", "3"), "applies only to the equity worker"),
    (("-Worker", "Screening", "-Only", "All"), "not both"),
    (("-Worker", "StockAlerts", "-IncludePaperStudy"), "cannot be combined"),
    (("-Worker", "Screening", "-PublishScreening", "-Once"), "cannot be combined"),
    (("-Worker", "MarketEvents", "-RepairSessions", "3"), "applies only to the equity worker"),
    (("-Worker", "Unknown"), "ValidateSet"),
])
def test_invalid_launch_combinations_fail_before_any_worker(arguments, message):
    result = preview(*arguments)
    assert result.returncode != 0 and message in result.stderr
    assert ' -X utf8 -u ' not in result.stdout


def test_service_supervisor_restarts_only_failed_component_with_bounded_backoff():
    from scripts.run_stock_alert_service import StockAlertSupervisor
    from types import SimpleNamespace

    now, started, closed = [0.], [], []
    def spawn(name, command):
        process = SimpleNamespace(pid=len(started) + 1, code=None)
        process.poll = lambda: process.code
        process.close = lambda: closed.append(process.pid)
        started.append((name, process))
        return process
    supervisor = StockAlertSupervisor(dict(intraday=("i",), swing=("s",), results=("r",)), spawn,
        clock=lambda: now[0], max_failures=2)
    assert supervisor.tick() and supervisor.snapshot()["state"] == "RUNNING"
    started[1][1].code = 1
    supervisor.tick()
    assert supervisor.snapshot()["components"]["swing"]["state"] == "BACKOFF"
    assert supervisor.snapshot()["components"]["intraday"]["pid"] == 1
    assert supervisor.snapshot()["components"]["results"]["pid"] == 3
    assert not supervisor.tick() and len(started) == 3
    now[0] = 5
    supervisor.tick()
    assert [name for name, _ in started] == ["intraday", "swing", "results", "swing"]
    started[-1][1].code = 0
    supervisor.tick()
    assert supervisor.snapshot()["components"]["swing"]["state"] == "FAILED"
    now[0] = 1000
    supervisor.tick()
    assert len(started) == 4
    supervisor.close()
    assert set(closed) == {1, 2, 3, 4} and supervisor.snapshot()["state"] == "STOPPED"
    assert not supervisor.tick()


def test_service_supervisor_start_failure_does_not_block_other_components():
    from scripts.run_stock_alert_service import StockAlertSupervisor
    from unittest.mock import Mock

    def spawn(name, command):
        if name == "swing":
            raise OSError("fixture")
        return Mock(pid=123, poll=lambda: None)
    supervisor = StockAlertSupervisor(dict(intraday=("i",), swing=("s",), results=("r",)), spawn)
    supervisor.tick()
    states = supervisor.snapshot()["components"]
    assert states["intraday"]["state"] == states["results"]["state"] == "RUNNING"
    assert states["swing"]["reason"] == "PROCESS_START_FAILED"
    supervisor.close()


def test_service_supervisor_requires_existing_separate_enrollments_without_writes(tmp_path, monkeypatch):
    import hashlib
    import sqlite3
    import zlib
    from scripts.run_stock_alert_service import check_enrollments, component_commands
    from research.stock_idea_engine import digest

    monkeypatch.delenv("STOCK_ALERT_SHADOW_VIEW", raising=False)
    policies = dict(intraday=dict(policy_version="stock_ideas_forward_quality_v2"), swing=dict(policy_version="stock_ideas_forward_swing_v1"))
    with pytest.raises(ValueError, match="not enrolled"):
        check_enrollments(tmp_path, policies=policies)
    assert not (tmp_path / "backups").exists()
    hashes = {}
    for directory, version in (("stock-ideas-forward-v2", "stock_ideas_forward_quality_v2"),
            ("stock-ideas-swing-v1", "stock_ideas_forward_swing_v1")):
        path = tmp_path / "backups/equity-shadow" / directory / "forward.sqlite"
        path.parent.mkdir(parents=True)
        with sqlite3.connect(path) as connection:
            connection.executescript("CREATE TABLE forward_manifest(singleton INTEGER,policy_hash TEXT,payload TEXT); CREATE TABLE forward_checkpoint(singleton INTEGER,payload BLOB);")
            connection.execute("INSERT INTO forward_manifest VALUES(1,?,?)", (digest(dict(policy_version=version)), json.dumps(dict(policy_version=version))))
            connection.execute("INSERT INTO forward_checkpoint VALUES(1,?)", (zlib.compress(json.dumps(dict(enrolled_at="2026-09-17T13:00:00Z", members=[dict(security_id="fixture", ticker="TEST")])).encode()),))
        hashes[path] = hashlib.sha256(path.read_bytes()).hexdigest()
    assert set(check_enrollments(tmp_path, policies=policies)) == {"intraday", "swing"}
    assert hashes == {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in hashes}
    with pytest.raises(ValueError, match="stored policy"):
        check_enrollments(tmp_path, policies=dict(policies, swing=dict(policy_version="changed")))
    monkeypatch.setenv("STOCK_ALERT_SHADOW_VIEW", str(tmp_path / "other.json"))
    with pytest.raises(ValueError, match="custom source"):
        check_enrollments(tmp_path, policies=policies)
    commands = component_commands(tmp_path, "python")
    assert "--swing" not in commands["intraday"] and "--activate-swing-shadow" in commands["swing"]
    assert commands["intraday"][-1] != commands["swing"][-1]
    assert commands["results"][-1] == "--continuous"


def test_service_supervisor_refuses_changed_enrollment_on_restart():
    from types import SimpleNamespace
    from scripts.run_stock_alert_service import StockAlertSupervisor

    now, calls = [0.], []
    process = SimpleNamespace(pid=1, poll=lambda: 1, close=lambda: None)
    def spawn(name, command):
        calls.append(name)
        if len(calls) > 1:
            raise ValueError("enrollment changed")
        return process
    service = StockAlertSupervisor(dict(intraday=("fixture",)), spawn, clock=lambda: now[0])
    service.tick()
    service.tick()
    now[0] = 5
    service.tick()
    assert service.snapshot()["components"]["intraday"]["reason"] == "LAUNCH_CONTRACT_UNAVAILABLE"
    now[0] = 1000
    service.tick()
    assert len(calls) == 2


def test_service_supervisor_shutdown_failure_does_not_skip_remaining_components():
    from types import SimpleNamespace
    from scripts.run_stock_alert_service import StockAlertSupervisor

    closed = []
    def spawn(name, command):
        def close():
            closed.append(name)
            if name == "swing":
                raise OSError("fixture")
        return SimpleNamespace(pid=1, poll=lambda: None, close=close)
    service = StockAlertSupervisor(dict(intraday=("i",), swing=("s",), results=("r",)), spawn)
    service.tick()
    with pytest.raises(RuntimeError, match="swing"):
        service.close()
    assert closed == ["results", "swing", "intraday"]
    assert service.snapshot()["state"] == "STOP_FAILED"


def test_service_supervisor_redacts_secrets_before_log_truncation():
    import logging
    from scripts.run_stock_alert_service import RedactedFormatter

    formatter = RedactedFormatter({"DB_PASSWORD": "fixture-password", "POLYGON_API_KEY": "fixture-api-key"})
    record = logging.LogRecord("test", logging.INFO, "", 0,
        "failure fixture-password api_key=unconfigured-secret fixture-api-key", (), None)
    rendered = formatter.format(record)
    assert all(secret not in rendered for secret in ("fixture-password", "fixture-api-key", "unconfigured-secret"))


def test_service_supervisor_windows_job_owns_descendants_and_stops_only_its_child(tmp_path):
    import sys
    import threading
    import psutil
    from scripts.run_stock_alert_service import ManagedProcess

    child = tmp_path / "child.py"
    child.write_text("import threading\nthreading.Event().wait()\n", encoding="utf-8")
    parent = tmp_path / "parent.py"
    parent.write_text("import subprocess,sys,threading\nchild=subprocess.Popen([sys.executable,sys.argv[1]])\nprint(child.pid,flush=True)\nthreading.Event().wait()\n", encoding="utf-8")
    ready, descendants = threading.Event(), []
    class Sink:
        def info(self, template, name, line):
            if line.isdigit():
                descendants.append(psutil.Process(int(line)))
                ready.set()
    process = ManagedProcess("fixture", (sys.executable, "-u", str(parent), str(child)), logger=Sink())
    try:
        assert ready.wait(10), "child process did not start"
        assert process.poll() is None and descendants[0].is_running()
    finally:
        process.close()
    assert process.poll() is not None
    assert not psutil.wait_procs(descendants, timeout=5)[1]


@pytest.mark.parametrize("arguments,error", [
    (["--status"], "inspection"), (["--once"], "inspection"), (["--recover-corrections"], "maintenance"),
    (["--state-dir=other"], "custom"), (["--quality-version", "1"], "non-current"),
])
def test_service_supervisor_refuses_to_take_over_nonresident_or_different_stores(monkeypatch, arguments, error):
    from types import SimpleNamespace
    from scripts.run_stock_alert_service import standalone_processes

    command = ["python.exe", str(ROOT / "backend/scripts/run_stock_idea_worker.py"), *arguments]
    process = SimpleNamespace(pid=5, info=dict(name="python.exe", cmdline=command, ppid=1, create_time=1),
        cmdline=lambda: command, cwd=lambda: str(ROOT))
    monkeypatch.setattr("psutil.process_iter", lambda *_: [process])
    with pytest.raises(ValueError, match=error):
        standalone_processes()


def test_service_supervisor_process_scope_excludes_other_workspaces_and_module_arguments(monkeypatch, tmp_path):
    from types import SimpleNamespace
    from scripts.run_stock_alert_service import standalone_processes

    commands = [["python.exe", str(tmp_path / "run_stock_idea_worker.py")],
        ["python.exe", "-m", "unrelated", str(ROOT / "backend/scripts/run_stock_idea_worker.py")]]
    processes = [SimpleNamespace(pid=position + 1, info=dict(name="python.exe", cmdline=command, ppid=0),
        cmdline=lambda command=command: command, cwd=lambda: str(ROOT)) for position, command in enumerate(commands)]
    monkeypatch.setattr("psutil.process_iter", lambda *_: processes)
    assert not any(standalone_processes().values())
    command = ["python.exe", "-X", "utf8", "-u", "backend/scripts/run_stock_idea_worker.py"]
    processes[:] = [SimpleNamespace(pid=5, info=dict(name="python.exe", cmdline=command, ppid=0),
        cmdline=lambda: command, cwd=lambda: str(ROOT))]
    assert len(standalone_processes()["intraday"]) == 1
    processes.append(SimpleNamespace(pid=6, info=dict(name="python.exe", cmdline=command, ppid=0),
        cmdline=lambda: command, cwd=lambda: str(ROOT)))
    with pytest.raises(ValueError, match="multiple intraday"):
        standalone_processes()


def test_service_supervisor_crash_closes_job_and_cannot_orphan_children(tmp_path):
    import sys
    import psutil

    child = tmp_path / "idle.py"
    child.write_text("import threading\nthreading.Event().wait()\n", encoding="utf-8")
    host = tmp_path / "host.py"
    host.write_text("import sys,threading\nsys.path.insert(0,sys.argv[1])\nfrom scripts.run_stock_alert_service import ManagedProcess\n"
        "child=ManagedProcess('test',(sys.executable,'-u',sys.argv[2]))\nprint(child.pid,flush=True)\nthreading.Event().wait()\n", encoding="utf-8")
    process = subprocess.Popen([sys.executable, str(host), str(ROOT / "backend"), str(child)], stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, text=True)
    owned = []
    try:
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor() as executor:
            read = executor.submit(process.stdout.readline)
            try:
                pid = int(read.result(timeout=10).strip())
            except BaseException:
                process.kill()
                raise
        owned = [psutil.Process(pid)]
        process.kill()
        process.wait(timeout=5)
        assert not psutil.wait_procs(owned, timeout=5)[1]
    finally:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=5)
        for remaining in owned:
            if remaining.is_running():
                remaining.kill()
        process.stdout.close()
        process.stderr.close()