param([switch]$Plan)

$ErrorActionPreference = 'Stop'
$root = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\..'))
$reportPath = Join-Path $root 'backend\backups\options-alert-validation\latest-market-runs.json'
$report = Get-Content -LiteralPath $reportPath -Raw | ConvertFrom-Json
$firstSlot = [DateTimeOffset]::Parse($report.next_first_observable_slot_check, [System.Globalization.CultureInfo]::InvariantCulture)
$scheduledAt = $firstSlot.AddMinutes(4.5)
if ($scheduledAt -le [DateTimeOffset]::Now) { throw 'Saved next-session time is past; rerun the read-only market report first.' }
$name = 'StockScreener-OptionsMarketValidation-' + $scheduledAt.ToString('yyyyMMdd')
$python = Join-Path $root 'backend\.venv\Scripts\python.exe'
$script = Join-Path $root 'backend\scripts\report_option_market_runs.py'
$output = Join-Path $root ('backend\backups\options-alert-validation\market-open-' + $scheduledAt.ToString('yyyy-MM-dd') + '.json')
$arguments = '-u "' + $script + '" --require-market-open --output "' + $output + '"'
if (-not (Test-Path -LiteralPath $python) -or -not (Test-Path -LiteralPath $script)) { throw 'Validation executable or script is missing.' }
if ($Plan) {
    [pscustomobject]@{ TaskName=$name; ScheduledAt=$scheduledAt.ToString('o'); Mode='READ_ONLY'; Registered=$false; Output=$output } | ConvertTo-Json
    exit 0
}
$existing = Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue
if ($existing) {
    if ($existing.Actions.Count -ne 1 -or $existing.Actions[0].Execute -ne $python -or $existing.Actions[0].Arguments -ne $arguments) {
        throw 'A different task already uses this validation name; it was not modified.'
    }
    [pscustomobject]@{ TaskName=$name; Status='ALREADY_REGISTERED'; NextRunTime=(Get-ScheduledTaskInfo -TaskName $name).NextRunTime } | ConvertTo-Json
    exit 0
}
$action = New-ScheduledTaskAction -Execute $python -Argument $arguments -WorkingDirectory $root
$trigger = New-ScheduledTaskTrigger -Once -At $scheduledAt.LocalDateTime
$principal = New-ScheduledTaskPrincipal -UserId ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name) -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Minutes 5) -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
Register-ScheduledTask -TaskName $name -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Description 'One-time read-only option latest-run validation. No workers, provider requests or publication.' | Out-Null
$info = Get-ScheduledTaskInfo -TaskName $name
[pscustomobject]@{ TaskName=$name; Status='REGISTERED'; NextRunTime=$info.NextRunTime; ScheduledAt=$scheduledAt.ToString('o'); RequiresLoggedInSession=$true; Output=$output } | ConvertTo-Json