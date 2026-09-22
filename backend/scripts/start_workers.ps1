<#
.SYNOPSIS
    Preview or start the market-day equity and option workers.

.DESCRIPTION
    Launches the canonical continuous workers:
      - equity materialization   (ingest, publish, analyze every due interval)
            - corporate actions        (refresh upcoming splits and dividends)
            - stock alert service      (supervised intraday, swing and results projection)
      - equity portal snapshots  (publishes the 20 generation-aware snapshots)
            - screening publications   (completed daily anchor plus advancing hourly context)
            - market events            (refresh earnings and FOMC coverage)
            - option model inputs      (refresh official Treasury curves)
      - delayed option pipeline  (ingest, analyze, strategies, recommendations)

        Continuous All starts five managed groups. Equity selects equity, stock
        alerts, UI projections and corporate-action refresh; Options selects the
        Options group and calendar/model-input refresh. One-shot lineup is unchanged.
        -Worker selects one resident worker without starting its dependencies.
        -Worker MarketContext starts the reviewed Market Conditions refresher in
        its own window; context workers are excluded from default worker sets.
        -Plan prints the selected commands without starting processes or contacting
        providers/the database. -Once executes sequentially and aggregates failures.
        -NoNewWindow requires -Once or a single -Worker selection.

        This script skips an already-running script instead of relying solely on
        worker leadership locks. API and frontend startup remain separate.
        -IncludePaperStudy opts into the already-enrolled ridge research observer.
        -Once -PublishScreening forces a daily rebuild before the normal screening
        refresh. Those switches cannot be combined with -Worker.
        The Advanced stream and replay jobs are never started. The stock-idea
        worker publishes unqualified forward shadow observations, never orders.
        StockAlerts supervises the two already-enrolled strategies and results sync.
        One-shot recovery retains the existing intraday-only stock alert pass.
        The legacy daily discovery worker is no longer started. Replay remains frozen.

.EXAMPLE
        .\start_workers.ps1 -Worker Screening -NoNewWindow

.EXAMPLE
        .\start_workers.ps1 -Worker StockAlerts -Plan
#>
param(
    [ValidateSet("All", "Equity", "Options")]
    [string]$Only = "All",

    [ValidateSet("Equity", "CorporateActions", "StockAlerts", "SwingAlerts", "AlertResults", "AlertContext", "MarketContext", "Portal", "Screening", "MarketEvents", "OptionInputs", "Options", "UIProjections", "ReferenceRefresh")]
    [string]$Worker,

    [switch]$Once,

    [switch]$Plan,

    [switch]$ShadowEvents,

    [switch]$ReplaceAlertWorkers,

    [switch]$IncludePaperStudy,

    [switch]$PublishScreening,

    [ValidateRange(0, 30)]
    [int]$RepairSessions = 0,

    [switch]$NoNewWindow
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$python = Join-Path $repoRoot "backend\.venv\Scripts\python.exe"
$scripts = Join-Path $repoRoot "backend\scripts"
$workerScripts = @{
    Equity = "run_equity_worker.py"
    CorporateActions = "run_corporate_action_worker.py"
    StockAlerts = "run_stock_idea_worker.py"
    SwingAlerts = "run_stock_idea_worker.py"
    AlertResults = "project_stock_alert_results.py"
    AlertContext = "run_stock_alert_context_worker.py"
    MarketContext = "prepare_stock_alert_context.py"
    Portal = "refresh_equity_portal_snapshots.py"
    Screening = "run_screening_worker.py"
    MarketEvents = "run_market_event_worker.py"
    OptionInputs = "run_option_model_input_worker.py"
    Options = "run_option_worker.py"
    UIProjections = "run_worker_group.py"
    ReferenceRefresh = "run_worker_group.py"
}
$workerGroups = @{ UIProjections = 'projections'; ReferenceRefresh = 'references'; Options = 'options' }
if (-not $Once) {
    $workerScripts.StockAlerts = "run_stock_alert_service.py"
    $workerScripts.Options = "run_worker_group.py"
}
$groupedDefault = -not $Once -and -not $Worker

if (-not $Plan -and -not (Test-Path $python -PathType Leaf)) {
    throw "Virtualenv interpreter not found: $python"
}
if ($Worker -and $PSBoundParameters.ContainsKey("Only")) {
    throw "Choose -Worker for a single worker or -Only for a worker set, not both"
}
if ($ShadowEvents -and $Worker -ne "AlertContext") {
    throw "-ShadowEvents requires -Worker AlertContext and never enables a live gate"
}
if ($ReplaceAlertWorkers -and ($Worker -ne 'StockAlerts' -or $Once)) {
    throw '-ReplaceAlertWorkers requires a continuous -Worker StockAlerts launch'
}
if ($Once -and $Worker -eq "MarketContext") {
    throw "-Worker MarketContext is continuous only; use the bounded context capture CLI with a new --output for one-shot work"
}
if ($Once -and $Worker -in @('UIProjections', 'ReferenceRefresh')) {
    throw 'Managed groups are continuous only; use a named component with -Once for bounded maintenance'
}
if ($Worker -and ($IncludePaperStudy -or $PublishScreening)) {
    throw "-Worker cannot be combined with -IncludePaperStudy or -PublishScreening"
}
if ($NoNewWindow -and -not $Once -and -not $Worker) {
    throw "-NoNewWindow requires -Once or a single -Worker; a continuous group would block the remaining workers"
}
if ($RepairSessions -gt 0 -and ($Only -eq "Options" -or ($Worker -and $Worker -ne "Equity"))) {
    throw "-RepairSessions applies only to the equity worker"
}
if (($IncludePaperStudy -or $PublishScreening) -and $Only -eq "Options") {
    throw "-IncludePaperStudy and -PublishScreening require -Only Equity or -Only All"
}
if ($PublishScreening -and -not $Once) {
    throw "-PublishScreening requires -Once; it forces a daily rebuild, not continuous refresh"
}
if ($IncludePaperStudy -and -not $Plan) {
    $manifest = Join-Path $repoRoot "backend\backups\equity-paper\ridge_forward_v1\manifest.json"
    if (-not (Test-Path $manifest -PathType Leaf)) {
        throw "Paper study is not enrolled at $manifest; review enrollment separately before using -IncludePaperStudy"
    }
}

$running = if ($Plan) { @() } else { Get-CimInstance Win32_Process -Filter "Name like '%python%'" -ErrorAction Stop }

function Get-GroupOption {
    param([string]$CommandLine, [string]$Name, [string]$Default = '')
    if ($CommandLine -match ("--" + $Name + "(?:=|\s+)" + '["'']?([a-z]+)')) { return $matches[1] }
    return $Default
}

function Test-AlreadyRunning {
    param([string]$ScriptName, [string[]]$Arguments = @())
    $groups = @($running | Where-Object { $_.CommandLine -like '*run_worker_group.py*' -and $_.CommandLine -notmatch '--(status|plan|check|stop)(\s|$)' })
    $owners = @{
        'refresh_equity_portal_snapshots.py' = 'projections'; 'run_screening_worker.py' = 'projections'
        'run_corporate_action_worker.py' = 'references'; 'run_market_event_worker.py' = 'references'
        'run_option_model_input_worker.py' = 'references'; 'run_option_worker.py' = 'options'
    }
    if ($ScriptName -eq 'run_worker_group.py') {
        $requested = $Arguments[[Array]::IndexOf($Arguments, '--group') + 1]
        $scope = 'all'
        if ($Arguments -contains '--scope') { $scope = $Arguments[[Array]::IndexOf($Arguments, '--scope') + 1] }
        $existing = @($groups | Where-Object { (Get-GroupOption $_.CommandLine 'group') -eq $requested })
        if ($existing) {
            if ($requested -eq 'references' -and @($existing | Where-Object {
                (Get-GroupOption $_.CommandLine 'scope' 'all') -notin @('all', $scope)
            }).Count) { throw 'Reference group scope differs; explicit reviewed group restart required' }
            Write-Warning ("Worker group {0} already running; skipping." -f $requested)
            return $true
        }
        $components = @($owners.Keys | Where-Object { $owners[$_] -eq $requested })
        if ($requested -eq 'references' -and $scope -ne 'all') {
            $components = if ($scope -eq 'equity') { @('run_corporate_action_worker.py') } else { @('run_market_event_worker.py', 'run_option_model_input_worker.py') }
        }
        foreach ($component in $components) {
            if (@($running | Where-Object { $_.CommandLine -like "*$component*" -and $_.CommandLine -notmatch '--(status|plan|check|check-technical-launch)(\s|$)' }).Count) {
                throw ("Standalone {0} is running; reviewed group cutover required" -f $component)
            }
        }
        return $false
    }
    if ($owners.ContainsKey($ScriptName) -and @($groups | Where-Object { (Get-GroupOption $_.CommandLine 'group') -eq $owners[$ScriptName] }).Count) {
        Write-Warning ("Worker group already running; skipping independent {0}." -f $ScriptName)
        return $true
    }
    $service = $running | Where-Object { $_.CommandLine -like '*run_stock_alert_service.py*' -and $_.CommandLine -notmatch '--(status|plan|check|stop)(\s|$)' }
    if ($service -and $ScriptName -in @('run_stock_alert_service.py', 'run_stock_idea_worker.py', 'project_stock_alert_results.py')) {
        Write-Warning ("Stock alert service already running (PID {0}); skipping independent component launch." -f ($service.ProcessId -join ', '))
        return $true
    }
    if ($ScriptName -eq 'run_stock_alert_service.py') {
        $standalone = $running | Where-Object { $_.CommandLine -match 'run_stock_idea_worker\.py|project_stock_alert_results\.py' -and $_.CommandLine -notmatch '--(status|plan|check-readiness|check-storage)(\s|$)' }
        if ($standalone -and -not $ReplaceAlertWorkers) { throw 'Standalone alert components are running; use a reviewed service takeover instead of starting duplicate workers.' }
    }
    $match = $running | Where-Object { $_.CommandLine -and $_.CommandLine -like "*$ScriptName*" }
    if ($ScriptName -eq 'run_stock_alert_service.py') {
        $match = $match | Where-Object { $_.CommandLine -notmatch '--(status|plan|check|stop)(\s|$)' }
    }
    if ($ScriptName -eq "run_stock_idea_worker.py") {
        $swing = $Arguments -contains "--swing"
        $match = $match | Where-Object { ($_.CommandLine -match '(^|\s)"?--swing"?(?=\s|$)') -eq $swing }
    }
    if ($match) {
        Write-Warning ("{0} already running (PID {1}); skipping." -f $ScriptName, ($match.ProcessId -join ', '))
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

    if ($Worker -and $ScriptName -ne $workerScripts[$Worker]) { return }
    if ($Worker -and $ScriptName -eq 'run_worker_group.py') {
        if ($Arguments[[Array]::IndexOf($Arguments, '--group') + 1] -ne $workerGroups[$Worker]) { return }
    }
    $scriptPath = Join-Path $scripts $ScriptName
    if (-not (Test-Path $scriptPath -PathType Leaf)) {
        throw "Worker script not found: $scriptPath"
    }
    if ($Plan) {
        Write-Output ("{0}: `"{1}`" -X utf8 -u `"{2}`" {3}" -f $Title, $python, $scriptPath, ($Arguments -join ' '))
        return
    }
    if (Test-AlreadyRunning -ScriptName $ScriptName -Arguments $Arguments) {
        if ($Once) { throw "$Title was not run; stop the existing worker before a one-shot recovery" }
        return
    }

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

    $quotedArgs = @($argList | ForEach-Object { "'" + $_.Replace("'", "''") + "'" })
    $inner = "`$host.UI.RawUI.WindowTitle = '$Title'; & '$($python.Replace("'", "''"))' $($quotedArgs -join ' ')"
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

if (-not $Plan) { $env:PYTHONIOENCODING = "utf-8" }
$modeArgs = if ($Once) { @("--once") } else { @() }
$stockAlertArgs = @("--quality-version", "2") + $modeArgs
$equityArgs = @("--behavior-shadow") + @($modeArgs)
if ($RepairSessions -gt 0) {
    $equityArgs += @("--repair-sessions", $RepairSessions.ToString())
}

$referenceScope = if ($groupedDefault -and $Only -ne 'All') { $Only.ToLowerInvariant() } else { 'all' }
if (-not $Plan -and -not $Once) {
    $requestedGroups = @()
    if ($groupedDefault) {
        if ($Only -ne 'Options') { $requestedGroups += 'projections' }
        $requestedGroups += 'references'
        if ($Only -ne 'Equity') { $requestedGroups += 'options' }
    }
    elseif ($workerGroups.ContainsKey($Worker)) { $requestedGroups += $workerGroups[$Worker] }
    foreach ($group in $requestedGroups) {
        $groupArgs = @('--group', $group)
        if ($group -eq 'references') { $groupArgs += @('--scope', $referenceScope) }
        Test-AlreadyRunning -ScriptName 'run_worker_group.py' -Arguments $groupArgs | Out-Null
    }
}

if ($Worker -eq "MarketContext") {
    $marketContextArgs = @(
        "--rotation-only",
        "--state-dir", (Join-Path $repoRoot "backend\backups\equity-shadow\stock-ideas-forward-v2"),
        "--split-review", (Join-Path $repoRoot "backend\research\stock_sector_split_reviews.json"),
        "--publish-rotation-view", "--fetch-macro", "--continuous"
    )
    Start-Worker -Title "market-context-worker" `
        -ScriptName "prepare_stock_alert_context.py" -Arguments $marketContextArgs
}

if ($Worker -eq "AlertResults") {
    $resultArgs = if ($Once) { @("--verify") } else { @("--continuous") }
    Start-Worker -Title "stock-alert-results-projector" `
        -ScriptName "project_stock_alert_results.py" -Arguments $resultArgs -Wait:$Once
}

if ($Worker -eq "AlertContext") {
    $contextArgs = @()
    if ($ShadowEvents) { $contextArgs += "--shadow-events" }
    if ($Once) {
        $contextArgs += "--once"
        Start-Worker -Title "stock-alert-context-worker" `
            -ScriptName "run_stock_alert_context_worker.py" -Arguments $contextArgs -Wait
    }
    elseif ($ShadowEvents) {
        Start-Worker -Title "stock-alert-context-worker" `
            -ScriptName "run_stock_alert_context_worker.py" -Arguments $contextArgs
    }
    else {
        Start-Worker -Title "stock-alert-context-worker" `
            -ScriptName "run_stock_alert_context_worker.py"
    }
}

if ($Worker -eq "SwingAlerts") {
    $swingArgs = @("--quality-version", "2", "--swing", "--activate-swing-shadow") + $modeArgs
    Start-Worker -Title "stock-swing-shadow-worker" `
        -ScriptName "run_stock_idea_worker.py" -Arguments $swingArgs -Wait:$Once
}

if ($Worker -ne "SwingAlerts" -and $Only -in @("All", "Equity")) {
    if ($Once) {
        Invoke-OneShotWorker -Title "equity-worker" `
            -ScriptName "run_equity_worker.py" -Arguments $equityArgs
    }
    else {
        Start-Worker -Title "equity-worker" -ScriptName "run_equity_worker.py" `
            -Arguments $equityArgs
    }

    if ($Once) {
        Invoke-OneShotWorker -Title "corporate-action-worker" `
            -ScriptName "run_corporate_action_worker.py" -Arguments $modeArgs
    }
    elseif (-not $groupedDefault) {
        Start-Worker -Title "corporate-action-worker" `
            -ScriptName "run_corporate_action_worker.py"
    }

    if ($Once) {
        Invoke-OneShotWorker -Title "stock-idea-shadow-worker" `
            -ScriptName "run_stock_idea_worker.py" -Arguments $stockAlertArgs
    }
    else {
        $serviceArgs = @()
        if ($ReplaceAlertWorkers) { $serviceArgs += '--replace-standalone' }
        Start-Worker -Title "stock-alert-service" `
            -ScriptName "run_stock_alert_service.py" -Arguments $serviceArgs
    }

    # The snapshot publisher has no --once; it is either a single pass or continuous.
    $snapshotArgs = if ($Once) { @() } else { @("--continuous") }
    if ($Once) {
        Invoke-OneShotWorker -Title "equity-portal-snapshots" `
            -ScriptName "refresh_equity_portal_snapshots.py"
    }
    elseif ($groupedDefault -or $Worker -eq 'UIProjections') {
        Start-Worker -Title "ui-projections" -ScriptName "run_worker_group.py" -Arguments @('--group', 'projections')
    }
    else {
        Start-Worker -Title "equity-portal-snapshots" `
            -ScriptName "refresh_equity_portal_snapshots.py" `
            -Arguments $snapshotArgs
    }

    if ($PublishScreening) {
        if ($oneShotFailures.Count -gt 0) {
            Write-Warning "Skipping screening publication because an equity preparation step failed or was already running."
        }
        else {
            Invoke-OneShotWorker -Title "daily-screening-publication" `
                -ScriptName "prepare_stock_screening.py" -Arguments @("--publish")
        }
    }

    if ($Once) {
        if ($oneShotFailures.Count -gt 0) {
            Write-Warning "Skipping screening refresh because an earlier equity step failed or was already running."
        }
        else {
            Invoke-OneShotWorker -Title "screening-worker" `
                -ScriptName "run_screening_worker.py" -Arguments @("--once")
        }
    }
    elseif (-not $groupedDefault) {
        Start-Worker -Title "screening-worker" -ScriptName "run_screening_worker.py"
    }

    if ($IncludePaperStudy) {
        if ($Once) {
            Invoke-OneShotWorker -Title "equity-paper-study" `
                -ScriptName "run_equity_paper_tracker.py" -Arguments @("--once")
        }
        else {
            Start-Worker -Title "equity-paper-study" `
                -ScriptName "run_equity_paper_tracker.py" -Arguments @("--watch")
        }
    }
}

if ($groupedDefault -or $Worker -eq 'ReferenceRefresh') {
    Start-Worker -Title "reference-refresh" -ScriptName "run_worker_group.py" -Arguments @('--group', 'references', '--scope', $referenceScope)
}

if ($Only -in @("All", "Options")) {
    if ($Once) {
        Invoke-OneShotWorker -Title "market-event-worker" `
            -ScriptName "run_market_event_worker.py" -Arguments $modeArgs
    }
    elseif (-not $groupedDefault) {
        Start-Worker -Title "market-event-worker" `
            -ScriptName "run_market_event_worker.py"
    }

    if ($Once) {
        Invoke-OneShotWorker -Title "option-model-input-worker" `
            -ScriptName "run_option_model_input_worker.py" -Arguments $modeArgs
    }
    elseif (-not $groupedDefault) {
        Start-Worker -Title "option-model-input-worker" `
            -ScriptName "run_option_model_input_worker.py"
    }

    if ($Once) {
        Invoke-OneShotWorker -Title "option-worker" `
            -ScriptName "run_option_worker.py" -Arguments $modeArgs
    }
    else {
        Start-Worker -Title "options-service" -ScriptName "run_worker_group.py" -Arguments @('--group', 'options')
    }
}

if ($oneShotFailures.Count -gt 0) {
    throw "One-shot worker failures: $($oneShotFailures -join '; ')"
}

Write-Output ""
if ($Plan) { Write-Output "Plan only: no workers started; current process/DB/provider health was not checked." }
if ($Worker) { Write-Output "Selected worker: $Worker. Dependencies are not started automatically." }
Write-Output "Screening refreshes completed hourly context; Stock Alerts supervises enrolled intraday, swing and results sync. One-shot recovery remains intraday-only."
Write-Output "Research observer is opt-in (-IncludePaperStudy); Advanced streaming, replay and backtest jobs stay excluded."
Write-Output "Verify with:"
if ($Worker) {
    switch ($Worker) {
        "Equity" {
            Write-Output "  backend\.venv\Scripts\python.exe backend\scripts\report_equity_analysis_status.py"
            Write-Output "  backend\.venv\Scripts\python.exe backend\scripts\report_stock_behavior_production_readiness.py"
        }
        "StockAlerts" { Write-Output "  backend\.venv\Scripts\python.exe backend\scripts\run_stock_alert_service.py --status" }
        "SwingAlerts" { Write-Output "  backend\.venv\Scripts\python.exe backend\scripts\run_stock_idea_worker.py --swing --status" }
        "AlertResults" { Write-Output "  Inspect stock-alert-results-projector and GET /api/stocks/alert-view?combined=true for independent stream freshness." }
        "AlertContext" { Write-Output "  backend\.venv\Scripts\python.exe backend\scripts\run_stock_alert_context_worker.py --status" }
        "MarketContext" { Write-Output "  Inspect market-context-worker for WAITING_FOR_NEXT_SOURCE_WINDOW or a verified capture; GET /api/stocks/market-conditions for source dates and coverage." }
        "Screening" { Write-Output "  backend\.venv\Scripts\python.exe backend\scripts\run_screening_worker.py --status" }
        "Options" { Write-Output "  backend\.venv\Scripts\python.exe backend\scripts\run_option_pipeline.py --status" }
        "UIProjections" { Write-Output "  backend\.venv\Scripts\python.exe backend\scripts\run_worker_group.py --group projections --status" }
        "ReferenceRefresh" { Write-Output "  backend\.venv\Scripts\python.exe backend\scripts\run_worker_group.py --group references --status" }
        default { Write-Output "  Inspect the selected worker's console for a successful refresh or an error." }
    }
}
elseif ($Only -in @("All", "Equity")) {
    Write-Output "  backend\.venv\Scripts\python.exe backend\scripts\run_equity_materialization.py --coverage-report"
    Write-Output "  backend\.venv\Scripts\python.exe backend\scripts\report_equity_analysis_status.py"
    Write-Output "  backend\.venv\Scripts\python.exe backend\scripts\report_stock_behavior_production_readiness.py"
    Write-Output "  backend\.venv\Scripts\python.exe backend\scripts\run_screening_worker.py --status"
    if ($IncludePaperStudy) { Write-Output "  backend\.venv\Scripts\python.exe backend\scripts\run_equity_paper_tracker.py --status" }
    if ($PublishScreening) { Write-Output "  GET /api/stocks/screening/catalog: inspect latest session/generation and field coverage" }
}
if (-not $Worker -and $Only -in @("All", "Options")) {
    Write-Output "  backend\.venv\Scripts\python.exe backend\scripts\run_option_pipeline.py --status"
}
