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
EQUITY = ["run_equity_worker.py", "run_corporate_action_worker.py", "run_stock_idea_worker.py", "refresh_equity_portal_snapshots.py", "run_screening_worker.py"]
OPTIONS = ["run_market_event_worker.py", "run_option_model_input_worker.py", "run_option_worker.py"]
WORKERS = dict(zip(["Equity", "CorporateActions", "StockAlerts", "Portal", "Screening", "MarketEvents", "OptionInputs", "Options"], EQUITY + OPTIONS))
WORKERS["AlertContext"] = "run_stock_alert_context_worker.py"
WORKERS["MarketContext"] = "prepare_stock_alert_context.py"
WORKERS["SwingAlerts"] = "run_stock_idea_worker.py"
WORKERS["AlertResults"] = "project_stock_alert_results.py"


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


@pytest.mark.parametrize("only,names", [("All", EQUITY + OPTIONS), ("Equity", EQUITY), ("Options", OPTIONS)])
def test_plan_lists_exact_worker_sets_without_process_or_database_work(only, names):
    result = preview("-Only", only)
    lines = worker_lines(result)
    assert len(lines) == len(names)
    assert all(name in line for name, line in zip(names, lines))
    if only != "Options":
        assert "--continuous" in lines[3]
    assert all("run_equity_stream_worker.py" not in line and "replay" not in line for line in lines)
    assert "Plan only: no workers started" in result.stdout


def test_one_shot_plan_orders_screening_after_equity_and_uses_explicit_flags():
    lines = worker_lines(preview("-Once", "-PublishScreening", "-IncludePaperStudy", "-RepairSessions", "3"))
    names = EQUITY[:4] + ["prepare_stock_screening.py", EQUITY[4], "run_equity_paper_tracker.py"] + OPTIONS
    assert len(lines) == len(names)
    assert all(name in line for name, line in zip(names, lines))
    assert "--once --repair-sessions 3" in lines[0]
    assert "--continuous" not in " ".join(lines)
    assert "--publish" in lines[4] and "--once" in lines[5] and "--once" in lines[6]
    assert all("--once" in line for line in lines[6:])


def test_research_watcher_is_opt_in_and_never_enrolls():
    lines = worker_lines(preview("-Only", "Equity", "-IncludePaperStudy"))
    assert len(lines) == 6 and "--watch" in lines[-1]
    assert "--enroll" not in " ".join(lines)
    assert all("paper_tracker" not in line for line in worker_lines(preview()))


@pytest.mark.parametrize("name,script", WORKERS.items())
@pytest.mark.parametrize("once", [False, True])
def test_single_worker_plan_starts_only_selected_entry_point(name, script, once):
    arguments = ["-Worker", name, "-NoNewWindow"] + (["-Once"] if once else [])
    result = preview(*arguments)
    if name == "MarketContext" and once:
        assert result.returncode != 0 and "continuous only" in result.stderr
        assert ' -X utf8 -u ' not in result.stdout
        return
    lines = worker_lines(result)
    assert len(lines) == 1 and script in lines[0]
    assert ("--once" in lines[0]) == (once and name not in ("Portal", "AlertResults"))
    assert ("--continuous" in lines[0]) == (not once and name in ("Portal", "MarketContext", "AlertResults"))
    assert "--retry-window" not in lines[0] and "--enable-source-readiness" not in lines[0]
    assert ("--quality-version 2" in lines[0]) == (name in ("StockAlerts", "SwingAlerts"))
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
    for name in EQUITY:
        (scripts / name).touch()
    result = mocked_start("-Only", "Equity", running=[EQUITY[0]], launcher=launcher)
    assert result.returncode == 0, result.stdout + result.stderr
    launches = [json.loads(line.removeprefix("MOCK_START ")) for line in result.stdout.splitlines() if line.startswith("MOCK_START ")]
    assert len(launches) == 4 and "already running" in result.stdout
    for name, launch in zip(EQUITY[1:], launches):
        assert launch["executable"] == "powershell.exe"
        assert launch["directory"] == str(scripts.parent.parent)
        assert f"'{scripts / name}'" in launch["arguments"][-1]
    assert "'--continuous'" in launches[-2]["arguments"][-1]


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


@pytest.mark.parametrize("selection,other,same", [
    ("SwingAlerts", "run_stock_idea_worker.py --quality-version 2", "run_stock_idea_worker.py --swing --activate-swing-shadow"),
    ("StockAlerts", "run_stock_idea_worker.py --swing --activate-swing-shadow", "run_stock_idea_worker.py --quality-version 2"),
])
def test_swing_and_intraday_workers_are_independent_but_duplicate_modes_are_skipped(selection, other, same):
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