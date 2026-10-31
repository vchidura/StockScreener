param(
    [ValidateSet('Plan', 'Install', 'Remove')]
    [string]$Mode = 'Plan',
    [Parameter(Mandatory = $true)]
    [string]$TaskName,
    [Parameter(Mandatory = $true)]
    [DateTimeOffset]$ScheduledAt,
    [Parameter(Mandatory = $true)]
    [string]$Authorization,
    [Parameter(Mandatory = $true)]
    [string]$FreezeReport,
    [Parameter(Mandatory = $true)]
    [string]$Package,
    [Parameter(Mandatory = $true)]
    [string]$Bundle,
    [Parameter(Mandatory = $true)]
    [string]$AcquisitionBinding,
    [Parameter(Mandatory = $true)]
    [string]$SecretDirectory,
    [Parameter(Mandatory = $true)]
    [string]$OutputDirectory,
    [string]$DockerExecutable = 'docker.exe',
    [string]$EnvFile = '',
    [string]$Python = ''
)

$ErrorActionPreference = 'Stop'
$root = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\..'))
$marker = 'StockScreener Advanced Realtime V3 owned one-time session task'

function Resolve-RootPath([string]$Value) {
    if ([System.IO.Path]::IsPathRooted($Value)) {
        return [System.IO.Path]::GetFullPath($Value)
    }
    return [System.IO.Path]::GetFullPath((Join-Path $root $Value))
}

function Quote-Argument([string]$Value) {
    return '"' + $Value.Replace('"', '\"') + '"'
}

$pythonPath = if ($Python) {
    Resolve-RootPath $Python
} else {
    Join-Path $root 'backend\.venv\Scripts\python.exe'
}
$schedulerPath = Join-Path $root 'backend\scripts\run_advanced_realtime_session_scheduler.py'
$authorizationPath = Resolve-RootPath $Authorization
$freezePath = Resolve-RootPath $FreezeReport
$packagePath = Resolve-RootPath $Package
$bundlePath = Resolve-RootPath $Bundle
$acquisitionBindingPath = Resolve-RootPath $AcquisitionBinding
$secretDirectoryPath = Resolve-RootPath $SecretDirectory
$outputPath = Resolve-RootPath $OutputDirectory
$envPath = if ($EnvFile) {
    Resolve-RootPath $EnvFile
} else {
    Join-Path $root 'backend\.env'
}

$argumentValues = @(
    '-B',
    $schedulerPath,
    'run',
    '--database',
    'stocks_realtime_v2',
    '--confirm-database-name',
    'stocks_realtime_v2',
    '--authorization',
    $authorizationPath,
    '--freeze-report',
    $freezePath,
    '--env-file',
    $envPath,
    '--package',
    $packagePath,
    '--bundle',
    $bundlePath,
    '--acquisition-binding',
    $acquisitionBindingPath,
    '--docker-executable',
    $DockerExecutable,
    '--secret-directory',
    $secretDirectoryPath,
    '--output-directory',
    $outputPath,
    '--continuous',
    '--resume',
    '--execute'
)
$arguments = ($argumentValues | ForEach-Object { Quote-Argument $_ }) -join ' '
$requiredFiles = @(
    $pythonPath,
    $schedulerPath,
    $authorizationPath,
    $freezePath,
    $envPath,
    $packagePath,
    $bundlePath,
    $acquisitionBindingPath
)
$missingFiles = @(
    $requiredFiles | Where-Object { -not (Test-Path -LiteralPath $_ -PathType Leaf) }
)
$requiredDirectories = @($secretDirectoryPath)
$missingDirectories = @(
    $requiredDirectories | Where-Object { -not (Test-Path -LiteralPath $_ -PathType Container) }
)
$readyToInstall = (
    ($missingFiles.Count -eq 0) -and
    ($missingDirectories.Count -eq 0) -and
    (-not (Test-Path -LiteralPath $outputPath)) -and
    ($ScheduledAt -gt [DateTimeOffset]::Now)
)

$plan = [ordered]@{
    Contract = 'advanced-realtime-v3-windows-session-task-v2'
    Mode = $Mode.ToUpperInvariant()
    TaskName = $TaskName
    ScheduledAt = $ScheduledAt.ToUniversalTime().ToString('o')
    CatchUpEnabled = $false
    StartWhenAvailable = $false
    MultipleInstances = 'IgnoreNew'
    RequiresLoggedInSession = $true
    Execute = $pythonPath
    Arguments = $arguments
    WorkingDirectory = $root
    OutputDirectory = $outputPath
    RequiredFiles = $requiredFiles
    MissingFiles = $missingFiles
    RequiredDirectories = $requiredDirectories
    MissingDirectories = $missingDirectories
    ReadyToInstall = $readyToInstall
    ProviderRequests = 0
    ProductionServiceActions = 0
    Registered = $false
}

if ($Mode -eq 'Plan') {
    [pscustomobject]$plan | ConvertTo-Json -Depth 4
    exit 0
}

$existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($Mode -eq 'Remove') {
    if (-not $existing) {
        $plan.Mode = 'REMOVE'
        $plan.Status = 'ABSENT'
        [pscustomobject]$plan | ConvertTo-Json -Depth 4
        exit 0
    }
    if ($existing.Description -ne $marker) {
        throw 'Refusing to remove a task not owned by this scheduler.'
    }
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    $plan.Mode = 'REMOVE'
    $plan.Status = 'REMOVED'
    [pscustomobject]$plan | ConvertTo-Json -Depth 4
    exit 0
}

if (-not $readyToInstall) {
    throw 'Task installation requires every frozen artifact, a new output directory, and a future trigger.'
}
if ($existing) {
    throw 'A task already uses this exact name; no second coordinator was registered.'
}

$action = New-ScheduledTaskAction `
    -Execute $pythonPath `
    -Argument $arguments `
    -WorkingDirectory $root
$trigger = New-ScheduledTaskTrigger -Once -At $ScheduledAt.LocalDateTime
$principal = New-ScheduledTaskPrincipal `
    -UserId ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name) `
    -LogonType Interactive `
    -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Hours 8) `
    -MultipleInstances IgnoreNew `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Principal $principal `
    -Settings $settings `
    -Description $marker | Out-Null

$registered = Get-ScheduledTask -TaskName $TaskName
if (
    ($registered.Description -ne $marker) -or
    ($registered.Actions.Count -ne 1) -or
    ($registered.Actions[0].Execute -ne $pythonPath) -or
    ($registered.Actions[0].Arguments -ne $arguments) -or
    $registered.Settings.StartWhenAvailable -or
    ($registered.Settings.MultipleInstances -ne 'IgnoreNew')
) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    throw 'Registered task identity or catch-up policy changed; registration was rolled back.'
}

$info = Get-ScheduledTaskInfo -TaskName $TaskName
$plan.Registered = $true
$plan.Status = 'REGISTERED'
$plan.NextRunTime = $info.NextRunTime.ToString('o')
[pscustomobject]$plan | ConvertTo-Json -Depth 4
