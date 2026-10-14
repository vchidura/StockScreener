<#
.SYNOPSIS
    Plan or start the dedicated continuous Advanced Realtime service.

.DESCRIPTION
    Runs an inert plan first, then starts the target-local reference -> ingestion ->
    materialization -> detectors -> immutable publication service only when -Start
    is explicit. This script never enrolls, migrates, unlocks roles, starts canonical
    workers, or creates monitoring acceptance.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$SessionDate,

    [Parameter(Mandatory = $true)]
    [string]$EnrollmentFile,

    [string]$MonitoringAcceptance,

    [ValidateSet("zero-yield-research-v1")]
    [string]$ResearchVariant,

    [string]$DeveloperPolicy = "options/policies/developer_forward_liquidity_v2.json",

    [string]$ValuationPolicy = "options/policies/valuation_raw_spot_v2.json",

    [string]$StrategyPolicy = "options/policies/strategy_technical_forward_v2.json",

    [ValidateRange(1, 86400)]
    [int]$EntryDelaySeconds = 1,

    [ValidateRange(1, 86400)]
    [int]$EntryWindowSeconds = 300,

    [ValidateRange(1, 604800)]
    [int]$ExitWindowSeconds = 86400,

    [ValidateRange(1, 30)]
    [int]$PollSeconds = 5,

    [ValidateRange(300, 86400)]
    [int]$ReferenceRefreshIntervalSeconds = 21600,

    [ValidateRange(4, 1000)]
    [int]$ServerMaxConnections = 20,

    [ValidateRange(1, 999)]
    [int]$ReservedConnections = 5,

    [string]$StateDir,

    [string]$ReferenceReportDir,

    [string]$EnvFile,

    [switch]$DisableStrategyReleases,

    [switch]$Start
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$python = Join-Path $repoRoot "backend\.venv\Scripts\python.exe"
$service = Join-Path $PSScriptRoot "run_realtime_developer_forward_service.py"

function Resolve-WorkspaceFile {
    param([string]$Value, [string]$Label)
    $candidate = if ([IO.Path]::IsPathRooted($Value)) { $Value } else { Join-Path $repoRoot $Value }
    $resolved = [IO.Path]::GetFullPath($candidate)
    $workspacePrefix = [IO.Path]::GetFullPath($repoRoot).TrimEnd('\') + '\'
    if (-not $resolved.StartsWith($workspacePrefix, [StringComparison]::OrdinalIgnoreCase)) {
        throw "$Label must be workspace-local"
    }
    if (-not (Test-Path $resolved -PathType Leaf)) {
        if ($Label -eq "EnrollmentFile") {
            throw "EnrollmentFile is missing: $resolved. Supply the immutable output of a successful READY staged finalization; do not create or rename a placeholder receipt."
        }
        throw "$Label is missing: $resolved"
    }
    return $resolved
}

if (-not (Test-Path $python -PathType Leaf)) {
    throw "Virtualenv interpreter not found: $python"
}
if ($ReservedConnections -ge $ServerMaxConnections) {
    throw "ReservedConnections must be below ServerMaxConnections"
}

$enrollment = Resolve-WorkspaceFile $EnrollmentFile "EnrollmentFile"
$monitoring = if ($MonitoringAcceptance) {
    Resolve-WorkspaceFile $MonitoringAcceptance "MonitoringAcceptance"
} else {
    $null
}
if ($Start -and -not $monitoring) {
    throw "MonitoringAcceptance is required with -Start"
}
if ($Start -and -not $ResearchVariant) {
    throw "ResearchVariant is required with -Start"
}
$environment = if ($EnvFile) {
    Resolve-WorkspaceFile $EnvFile "EnvFile"
} else {
    Resolve-WorkspaceFile (Join-Path $repoRoot "backend\.env") "EnvFile"
}
$state = if ($StateDir) { [IO.Path]::GetFullPath($StateDir) } else {
    Join-Path $repoRoot "backend\backups\realtime-developer-forward-service"
}
$referenceReports = if ($ReferenceReportDir) { [IO.Path]::GetFullPath($ReferenceReportDir) } else {
    Join-Path $state "reference-reports"
}

$common = @(
    "--session-date", $SessionDate,
    "--enrollment-file", $enrollment,
    "--entry-delay-seconds", $EntryDelaySeconds.ToString(),
    "--entry-window-seconds", $EntryWindowSeconds.ToString(),
    "--exit-window-seconds", $ExitWindowSeconds.ToString(),
    "--poll-seconds", $PollSeconds.ToString(),
    "--reference-refresh-interval-seconds", $ReferenceRefreshIntervalSeconds.ToString(),
    "--reference-report-dir", $referenceReports,
    "--server-max-connections", $ServerMaxConnections.ToString(),
    "--reserved-connections", $ReservedConnections.ToString(),
    "--state-dir", $state,
    "--env-file", $environment,
    "--developer-policy", $DeveloperPolicy,
    "--valuation-policy", $ValuationPolicy,
    "--strategy-policy", $StrategyPolicy
)
if ($ResearchVariant) {
    $common += @("--research-variant", $ResearchVariant)
}
if (-not $DisableStrategyReleases) {
    $common += "--strategy-releases"
}

& $python -B $service --plan @common
if ($LASTEXITCODE -ne 0) {
    throw "Advanced Realtime plan failed; continuous service was not started"
}
if (-not $Start) {
    Write-Output "Plan only: add -Start after reviewing the plan and principal state."
    exit 0
}

& $python -B $service --continuous @common --monitoring-acceptance $monitoring
exit $LASTEXITCODE