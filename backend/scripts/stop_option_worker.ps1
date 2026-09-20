param()

$ErrorActionPreference = "Stop"
$scriptPath = [System.IO.Path]::GetFullPath(
    (Join-Path $PSScriptRoot "run_option_worker.py")
)
$matches = @(
    Get-CimInstance Win32_Process |
        Where-Object {
            $_.CommandLine -and
            $_.CommandLine.IndexOf($scriptPath, [StringComparison]::OrdinalIgnoreCase) -ge 0
        }
)
$workers = @($matches | Where-Object { $_.Name -eq "python.exe" })
if ($workers.Count -eq 0) {
    Write-Output "OPTIONS_WORKER_NOT_RUNNING"
    exit 0
}
$workerIds = @($workers.ProcessId)
$roots = @($workers | Where-Object { $_.ParentProcessId -notin $workerIds })
if ($roots.Count -ne 1) {
    throw "Expected exactly one Options-worker Python root; found $($roots.Count)"
}
$root = $roots[0]
$descendants = @($workers | Where-Object { $_.ProcessId -ne $root.ProcessId })
foreach ($process in ($descendants | Sort-Object ProcessId -Descending)) {
    Stop-Process -Id $process.ProcessId -Force
}
if (Get-Process -Id $root.ProcessId -ErrorAction SilentlyContinue) {
    Stop-Process -Id $root.ProcessId -Force
}
$parent = Get-CimInstance Win32_Process -Filter "ProcessId=$($root.ParentProcessId)" `
    -ErrorAction SilentlyContinue
if (
    $parent -and $parent.Name -eq "powershell.exe" -and $parent.CommandLine -and
    $parent.CommandLine.IndexOf($scriptPath, [StringComparison]::OrdinalIgnoreCase) -ge 0
) {
    Stop-Process -Id $parent.ProcessId -Force
}
$remaining = @(
    Get-CimInstance Win32_Process |
        Where-Object {
            $_.CommandLine -and
            $_.CommandLine.IndexOf($scriptPath, [StringComparison]::OrdinalIgnoreCase) -ge 0
        }
)
if ($remaining.Count -ne 0) {
    throw "Options-worker processes remain after stop: $($remaining.ProcessId -join ',')"
}
Write-Output (
    "OPTIONS_WORKER_STOPPED root={0} descendants={1}" -f
    $root.ProcessId, $descendants.Count
)