param(
    [string]$PostgresBin = "C:\Program Files\PostgreSQL\17\bin",
    [string]$DataDirectory = "C:\Program Files\PostgreSQL\17\data",
    [ValidateRange(1, 65535)]
    [int]$Port = 5432
)

$ErrorActionPreference = "Stop"

$postgres = Join-Path $PostgresBin "postgres.exe"
if (-not (Test-Path $postgres -PathType Leaf)) {
    throw "PostgreSQL server executable not found: $postgres"
}
if (-not (Test-Path $DataDirectory -PathType Container)) {
    throw "PostgreSQL data directory not found: $DataDirectory"
}

$listener = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue |
    Select-Object -First 1
if ($listener) {
    $process = Get-Process -Id $listener.OwningProcess -ErrorAction SilentlyContinue
    $processName = if ($process) { $process.ProcessName } else { "unknown" }
    Write-Output (
        "Port {0} is already listening (PID {1}, process {2}); no process started." -f
        $Port, $listener.OwningProcess, $processName
    )
    exit 0
}

$pidFile = Join-Path $DataDirectory "postmaster.pid"
if (Test-Path $pidFile -PathType Leaf) {
    throw (
        "PostgreSQL is not listening on port $Port, but $pidFile exists. " +
        "Do not delete it automatically; confirm that no postgres.exe process is running " +
        "and inspect the latest server log first."
    )
}

$arguments = @("-D", ('"{0}"' -f $DataDirectory))
$startParameters = @{
    FilePath = $postgres
    ArgumentList = $arguments
    WorkingDirectory = $PostgresBin
    WindowStyle = "Minimized"
    PassThru = $true
}
$server = Start-Process @startParameters

Write-Output "Started PostgreSQL directly as PID $($server.Id)."
Write-Output "This is not the postgresql-x64-17 Windows service."
Write-Output "Keep the PostgreSQL console running; restore it and press Ctrl+C for a controlled shutdown."
Write-Output "Verify application connectivity before starting schedulers."