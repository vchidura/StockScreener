param(
    [string]$Version = "v4",
    [ValidateSet("Full", "SchemaOnly", "MarketSchema")]
    [string]$Mode = "Full",
    [ValidateSet("gzip", "zstd")]
    [string]$Compression = "zstd",
    [ValidateRange(1, 19)]
    [int]$CompressionLevel = 19,
    [switch]$Replace
)

$ErrorActionPreference = "Stop"
$backendDir = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $backendDir ".env"
$backupDir = Join-Path $backendDir "backups"
$date = Get-Date -Format "yyyy-MM-dd"
$backupName = switch ($Mode) {
    "SchemaOnly" { "stocks_db_schema_${Version}_${date}.dump" }
    "MarketSchema" { "stocks_market_schema_${Version}_${date}.dump" }
    default { "stocks_db_backup_${Version}_${date}.dump" }
}
$backup = Join-Path $backupDir $backupName
$partial = "$backup.partial"

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
$pgDump = Join-Path $postgresBin "pg_dump.exe"
$pgRestore = Join-Path $postgresBin "pg_restore.exe"
if (-not (Test-Path $pgDump) -or -not (Test-Path $pgRestore)) {
    throw "PostgreSQL 17 backup tools were not found in $postgresBin"
}

function Get-BackupSummary {
    param([string]$Path)

    $catalog = @(& $pgRestore --list $Path)
    if ($LASTEXITCODE -ne 0 -or $catalog.Count -eq 0) {
        throw "pg_restore could not validate $Path"
    }
    $item = Get-Item $Path
    [pscustomobject]@{
        File = $item.Name
        Mode = $Mode
        Bytes = $item.Length
        MiB = [math]::Round($item.Length / 1MB, 2)
        CatalogLines = $catalog.Count
        PublicTables = @($catalog | Where-Object { $_ -match ' TABLE public ' }).Count
        PublicTableData = @($catalog | Where-Object { $_ -match ' TABLE DATA public ' }).Count
        SHA256 = (Get-FileHash $Path -Algorithm SHA256).Hash
    }
}

$activeWriters = @(
    Get-CimInstance Win32_Process -Filter "Name = 'pg_dump.exe'" |
        Where-Object {
            $_.CommandLine -and (
                $_.CommandLine.Contains($backup) -or $_.CommandLine.Contains($partial)
            )
        }
)
if ($activeWriters.Count -gt 0) {
    throw "A pg_dump process is already writing this backup"
}

if (Test-Path $backup) {
    if (-not $Replace -and (Get-Item $backup).Length -gt 0) {
        Get-BackupSummary -Path $backup | Format-List
        return
    }
    Remove-Item $backup -Force
}
Remove-Item $partial -Force -ErrorAction SilentlyContinue

$env:PGPASSWORD = $settings["DB_PASSWORD"]
$settings.Remove("DB_PASSWORD")
try {
    $compressionOption = "${Compression}:level=${CompressionLevel}"
    $dumpArgs = @(
        "--host=$($settings["DB_HOST"])",
        "--port=$($settings["DB_PORT"])",
        "--username=$($settings["DB_USER"])",
        "--dbname=$($settings["DB_NAME"])",
        "--format=custom",
        "--compress=$compressionOption",
        "--no-owner",
        "--no-privileges",
        "--file=$partial"
    )
    if ($Mode -eq "SchemaOnly") {
        $dumpArgs += "--schema-only"
    } elseif ($Mode -eq "MarketSchema") {
        $dumpArgs += @(
            "--schema-only",
            "--table=public.selected_tickers",
            "--table=public.equity_ingestion_segments",
            "--table=public.equity_bar_revisions",
            "--table=public.equity_bar_publications",
            "--table=public.equity_bar_publication_members",
            "--table=public.equity_current_bar_projection",
            "--table=public.equity_canonical_bars",
            "--table=public.equity_canonical_daily_bars",
            "--table=public.equity_canonical_hourly_bars"
        )
    }
    & $pgDump @dumpArgs
    if ($LASTEXITCODE -ne 0) {
        throw "pg_dump failed with exit code $LASTEXITCODE"
    }
} finally {
    Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue
}

$catalog = @(& $pgRestore --list $partial)
if ($LASTEXITCODE -ne 0 -or $catalog.Count -eq 0) {
    Remove-Item $partial -Force -ErrorAction SilentlyContinue
    throw "pg_restore could not validate the generated archive"
}

Move-Item $partial $backup
Get-BackupSummary -Path $backup | Format-List
