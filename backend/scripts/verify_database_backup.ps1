param(
    [Parameter(Mandatory = $true)]
    [string]$BackupPath
)

$ErrorActionPreference = "Stop"
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
foreach ($name in @("DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME")) {
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
    $settings["DB_NAME"] -replace '[^A-Za-z0-9_]', '_'
), (Get-Date -Format "yyyyMMddHHmmss")
$connectionArgs = @(
    "--host=$($settings['DB_HOST'])",
    "--port=$($settings['DB_PORT'])",
    "--username=$($settings['DB_USER'])"
)
$criticalTables = @(
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

function Get-TableCount {
    param([string]$Database, [string]$Table)

    $value = & $psql @connectionArgs "--dbname=$Database" --tuples-only --no-align `
        --command="SELECT COUNT(*) FROM $Table"
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to count $Table in $Database"
    }
    return [long]$value.Trim()
}

$env:PGPASSWORD = $settings["DB_PASSWORD"]
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
        $sourceCount = Get-TableCount -Database $settings["DB_NAME"] -Table $table
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
    $validator = Join-Path $PSScriptRoot "validate_equity_storage.py"
    if (-not (Test-Path $python)) {
        throw "Backend virtual environment was not found: $python"
    }
    $env:DB_NAME = $restoreDatabase
    $env:DB_USER = $settings["DB_USER"]
    $env:DB_PASSWORD = $settings["DB_PASSWORD"]
    $env:DB_HOST = $settings["DB_HOST"]
    $env:DB_PORT = $settings["DB_PORT"]
    & $python $validator
    if ($LASTEXITCODE -ne 0) {
        throw "Canonical storage validation failed in $restoreDatabase"
    }
    Write-Output "CANONICAL_RESTORE_VALIDATED database=$restoreDatabase"
    Write-Output "RESTORE_VERIFIED database=$restoreDatabase tables=$($criticalTables.Count)"
} finally {
    & $dropdb @connectionArgs --if-exists --force $restoreDatabase
    Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue
    Remove-Item Env:DB_PASSWORD -ErrorAction SilentlyContinue
}