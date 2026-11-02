param(
    [Parameter(Mandatory = $true)]
    [string]$BackupPath,
    [string]$Database,
    [Parameter(Mandatory = $true)]
    [ValidateSet("Legacy", "RealtimeV3", "Both")]
    [string]$ValidationScope,
    [switch]$PlanOnly
)

$ErrorActionPreference = "Stop"
$legacyCriticalTables = @(
    "public.selected_tickers",
    "public.equity_bar_revisions",
    "public.equity_bar_publications",
    "public.equity_bar_publication_members",
    "public.equity_current_bar_projection",
    "public.equity_analysis_runs",
    "public.equity_analysis_members",
    "public.equity_evidence",
    "public.equity_context_snapshots",
    "public.equity_context_evidence",
    "public.equity_current_projection",
    "public.equity_portal_source_state",
    "public.equity_portal_snapshots",
    "public.equity_portal_current_projections",
    "public.equity_outcome_policies",
    "public.equity_research_outcomes",
    "public.equity_qualification_revisions",
    "public.option_snapshot_fact_keys"
)
$realtimeV3CriticalTables = @(
    "rt.database_identity",
    "rt.schema_migrations",
    "rt.database_security_policies",
    "rt.security_phase_events",
    "rt.partition_policies",
    "rt.session_archives",
    "rt.archive_segments",
    "rt.realtime_desired_revisions",
    "rt.realtime_desired_current",
    "rt.realtime_desired_member_intents",
    "rt.realtime_desired_intent_readiness",
    "rt.realtime_desired_member_bindings",
    "rt.realtime_desired_session_snapshots",
    "rt.advanced_options_preparation_sessions",
    "rt.advanced_options_preparation_work",
    "rt.advanced_options_preparation_evidence",
    "rt.advanced_options_preparation_evidence_history",
    "rt.advanced_options_preparation_refresh_requests",
    "rt.advanced_options_continuous_sessions",
    "rt.advanced_options_downstream_bindings",
    "rt.advanced_options_downstream_heads",
    "rt.advanced_journal_registry",
    "rt.consumer_checkpoints",
    "rt.advanced_options_rolling_checkpoints",
    "rt.advanced_options_selection_generations",
    "rt.advanced_options_selection_members",
    "rt.advanced_options_physical_subscriptions",
    "rt.advanced_options_physical_subscription_states",
    "rt.advanced_options_subscription_shards",
    "rt.advanced_options_shard_states",
    "rt.advanced_options_consumer_states",
    "rt.advanced_options_detector_routing_states",
    "rt.portal_publication_work",
    "rt.portal_publication_receipts",
    "rt.portal_releases",
    "rt.portal_release_current",
    "rt.advanced_options_read_pins",
    "rt.advanced_realtime_service_states"
)
$criticalTables = @(
    if ($ValidationScope -in @("Legacy", "Both")) {
        $legacyCriticalTables
    }
    if ($ValidationScope -in @("RealtimeV3", "Both")) {
        $realtimeV3CriticalTables
    }
)
$validators = @(
    if ($ValidationScope -in @("Legacy", "Both")) {
        "validate_equity_storage.py"
    }
    if ($ValidationScope -in @("RealtimeV3", "Both")) {
        "validate_advanced_realtime_v3_restore.py"
    }
)
if ($PlanOnly) {
    [pscustomobject]@{
        ValidationScope = $ValidationScope
        CriticalTables = $criticalTables
        Validators = $validators
    } | ConvertTo-Json -Depth 4
    return
}

$backendDir = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $backendDir ".env"
$resolvedBackup = Resolve-Path $BackupPath

if (-not (Test-Path $envFile)) {
    throw "Missing database configuration: $envFile"
}

$settings = @{}
Get-Content $envFile | ForEach-Object {
    if ($_ -match '^\s*([^#][^=]*)=(.*)$') {
        $settings[$matches[1].Trim()] = $matches[2].Trim().Trim('"').Trim("'")
    }
}
foreach ($name in @("DB_HOST", "DB_PORT")) {
    if (-not $settings.ContainsKey($name) -or -not $settings[$name]) {
        throw "Missing $name in $envFile"
    }
}
$databaseName = if ($Database) {
    $Database
} elseif ($settings.ContainsKey("DB_NAME") -and $settings["DB_NAME"]) {
    $settings["DB_NAME"]
} else {
    throw "Missing -Database or DB_NAME in $envFile"
}
$userKey = if ($settings.ContainsKey("DB_USER") -and $settings["DB_USER"]) {
    "DB_USER"
} else {
    "POSTGRES_ADMIN_USER"
}
$passwordKey = if (
    $settings.ContainsKey("DB_PASSWORD") -and $settings["DB_PASSWORD"]
) {
    "DB_PASSWORD"
} else {
    "POSTGRES_ADMIN_PASSWORD"
}
foreach ($name in @($userKey, $passwordKey)) {
    if (-not $settings.ContainsKey($name) -or -not $settings[$name]) {
        throw "Missing $name in $envFile"
    }
}

$postgresBin = "C:\Program Files\PostgreSQL\17\bin"
$createdb = Join-Path $postgresBin "createdb.exe"
$dropdb = Join-Path $postgresBin "dropdb.exe"
$pgRestore = Join-Path $postgresBin "pg_restore.exe"
$psql = Join-Path $postgresBin "psql.exe"
foreach ($command in @($createdb, $dropdb, $pgRestore, $psql)) {
    if (-not (Test-Path $command)) {
        throw "PostgreSQL restore tool was not found: $command"
    }
}

$restoreDatabase = "{0}_restore_verify_{1}" -f (
    $databaseName -replace '[^A-Za-z0-9_]', '_'
), (Get-Date -Format "yyyyMMddHHmmss")
$connectionArgs = @(
    "--host=$($settings['DB_HOST'])",
    "--port=$($settings['DB_PORT'])",
    "--username=$($settings[$userKey])"
)
function Get-TableCount {
    param([string]$Database, [string]$Table)

    $value = & $psql @connectionArgs "--dbname=$Database" --tuples-only --no-align `
        --command="SELECT COUNT(*) FROM $Table"
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to count $Table in $Database"
    }
    return [long]$value.Trim()
}

$env:PGPASSWORD = $settings[$passwordKey]
try {
    & $createdb @connectionArgs "--template=template0" $restoreDatabase
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to create isolated restore database $restoreDatabase"
    }

    & $pgRestore @connectionArgs "--dbname=$restoreDatabase" --exit-on-error `
        --no-owner --no-privileges $resolvedBackup
    if ($LASTEXITCODE -ne 0) {
        throw "pg_restore failed for $restoreDatabase"
    }

    $results = foreach ($table in $criticalTables) {
        $sourceCount = Get-TableCount -Database $databaseName -Table $table
        $restoreCount = Get-TableCount -Database $restoreDatabase -Table $table
        [pscustomobject]@{
            Table = $table
            SourceCount = $sourceCount
            RestoreCount = $restoreCount
            Matches = $sourceCount -eq $restoreCount
        }
    }
    $results | Format-Table -AutoSize
    if ($results.Matches -contains $false) {
        throw "Restored critical table counts do not match the source database"
    }
    $python = Join-Path $backendDir ".venv\Scripts\python.exe"
    if (-not (Test-Path $python)) {
        throw "Backend virtual environment was not found: $python"
    }
    $env:DB_NAME = $restoreDatabase
    $env:DB_USER = $settings[$userKey]
    $env:DB_PASSWORD = $settings[$passwordKey]
    $env:DB_HOST = $settings["DB_HOST"]
    $env:DB_PORT = $settings["DB_PORT"]
    if ($ValidationScope -in @("Legacy", "Both")) {
        $validator = Join-Path $PSScriptRoot "validate_equity_storage.py"
        & $python $validator
        if ($LASTEXITCODE -ne 0) {
            throw "Canonical storage validation failed in $restoreDatabase"
        }
        Write-Output "CANONICAL_RESTORE_VALIDATED database=$restoreDatabase"
    }
    if ($ValidationScope -in @("RealtimeV3", "Both")) {
        $v3Validator = Join-Path $PSScriptRoot `
            "validate_advanced_realtime_v3_restore.py"
        & $python $v3Validator
        if ($LASTEXITCODE -ne 0) {
            throw "Advanced Realtime V3 validation failed in $restoreDatabase"
        }
        Write-Output `
            "ADVANCED_REALTIME_V3_RESTORE_VALIDATED database=$restoreDatabase"
    }
    Write-Output `
        "RESTORE_VERIFIED database=$restoreDatabase scope=$ValidationScope tables=$($criticalTables.Count)"
} finally {
    & $dropdb @connectionArgs --if-exists --force $restoreDatabase
    Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue
    Remove-Item Env:DB_PASSWORD -ErrorAction SilentlyContinue
}