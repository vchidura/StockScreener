<#
.SYNOPSIS
    Start the resident equity and option workers in separate windows.

.DESCRIPTION
    Launches the canonical continuous workers:
      - equity materialization   (ingest, publish, analyze every due interval)
      - equity portal snapshots  (publishes the 20 generation-aware snapshots)
      - delayed option pipeline  (ingest, analyze, strategies, recommendations)

    Mutating equity phases take a PostgreSQL advisory lock, so a second
    materialization worker would fail rather than corrupt data. This script
    refuses to start a duplicate instead of relying on that.

    The Polygon Advanced stream worker is intentionally not started.
#>
param(
    [ValidateSet("All", "Equity", "Options")]
    [string]$Only = "All",

    [switch]$Once,

    # Run in this window instead of spawning separate ones. Only valid with -Only.
    [switch]$NoNewWindow
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$python = Join-Path $repoRoot "backend\.venv\Scripts\python.exe"
$scripts = Join-Path $repoRoot "backend\scripts"

if (-not (Test-Path $python -PathType Leaf)) {
    throw "Virtualenv interpreter not found: $python"
}
if ($NoNewWindow -and $Only -eq "All") {
    throw "-NoNewWindow requires -Only Equity or -Only Options"
}

$running = Get-CimInstance Win32_Process -Filter "Name like '%python%'" -ErrorAction SilentlyContinue

function Test-AlreadyRunning {
    param([string]$ScriptName)
    $match = $running | Where-Object { $_.CommandLine -and $_.CommandLine -like "*$ScriptName*" }
    if ($match) {
        Write-Warning ("{0} already running (PID {1}); skipping." -f $ScriptName, $match.ProcessId)
        return $true
    }
    return $false
}

function Start-Worker {
    param(
        [string]$Title,
        [string]$ScriptName,
        [string[]]$Arguments = @(),
        [switch]$Wait
    )

    $scriptPath = Join-Path $scripts $ScriptName
    if (-not (Test-Path $scriptPath -PathType Leaf)) {
        throw "Worker script not found: $scriptPath"
    }
    if (Test-AlreadyRunning -ScriptName $ScriptName) { return }

    # -u keeps progress unbuffered so a stalled run is distinguishable from a healthy one.
    $argList = @("-X", "utf8", "-u", $scriptPath) + $Arguments

    if ($NoNewWindow -or $Wait) {
        Write-Output "==> $Title"
        & $python @argList
        if ($LASTEXITCODE -ne 0) {
            throw "$Title failed with exit code $LASTEXITCODE"
        }
        return
    }

    $inner = "`$host.UI.RawUI.WindowTitle = '$Title'; & '$python' $($argList -join ' ')"
    Start-Process -FilePath "powershell.exe" `
        -ArgumentList @("-NoExit", "-NoProfile", "-Command", $inner) `
        -WorkingDirectory $repoRoot | Out-Null
    Write-Output ("Started {0}" -f $Title)
}

$oneShotFailures = [System.Collections.Generic.List[string]]::new()

function Invoke-OneShotWorker {
    param(
        [string]$Title,
        [string]$ScriptName,
        [string[]]$Arguments = @()
    )
    try {
        Start-Worker -Title $Title -ScriptName $ScriptName `
            -Arguments $Arguments -Wait
    }
    catch {
        $oneShotFailures.Add(("{0}: {1}" -f $Title, $_.Exception.Message))
        Write-Warning $oneShotFailures[$oneShotFailures.Count - 1]
    }
}

$env:PYTHONIOENCODING = "utf-8"
$modeArgs = if ($Once) { @("--once") } else { @() }

if ($Only -in @("All", "Equity")) {
    if ($Once) {
        Invoke-OneShotWorker -Title "equity-worker" `
            -ScriptName "run_equity_worker.py" -Arguments $modeArgs
    }
    else {
        Start-Worker -Title "equity-worker" -ScriptName "run_equity_worker.py"
    }

    # The snapshot publisher has no --once; it is either a single pass or continuous.
    $snapshotArgs = if ($Once) { @() } else { @("--continuous") }
    if ($Once) {
        Invoke-OneShotWorker -Title "equity-portal-snapshots" `
            -ScriptName "refresh_equity_portal_snapshots.py"
    }
    else {
        Start-Worker -Title "equity-portal-snapshots" `
            -ScriptName "refresh_equity_portal_snapshots.py" `
            -Arguments $snapshotArgs
    }
}

if ($Only -in @("All", "Options")) {
    if ($Once) {
        Invoke-OneShotWorker -Title "option-worker" `
            -ScriptName "run_option_worker.py" -Arguments $modeArgs
    }
    else {
        Start-Worker -Title "option-worker" -ScriptName "run_option_worker.py"
    }
}

if ($oneShotFailures.Count -gt 0) {
    throw "One-shot worker failures: $($oneShotFailures -join '; ')"
}

Write-Output ""
Write-Output "Verify with:"
Write-Output "  backend\.venv\Scripts\python.exe backend\scripts\run_equity_materialization.py --coverage-report"
Write-Output "  backend\.venv\Scripts\python.exe backend\scripts\run_option_pipeline.py --status"
