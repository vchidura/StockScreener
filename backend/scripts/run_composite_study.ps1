<#
.SYNOPSIS
    Runs the level_retest_rejection point-in-time daily study end to end.

.DESCRIPTION
    Every stage is resumable and safe to re-run, so on any failure just invoke
    the same command again. Grouped-daily responses and point-in-time ticker
    references are cached on disk, persisted universes are skipped, and bar and
    evidence writes are keyed by deterministic identifiers.

    Stages are numbered so a run can be resumed at a known point with -FromStage
    rather than repeating completed work.

    Study definition, fixed before the run:
      detector    level_retest_rejection v1.2   (unchanged; verified 2026-09-03)
      interval    1d, sector-primary benchmark
      horizons    5d / 10d / 21d
      lineage     split-adjusted
      sessions    1034 research + 210 warm-up, ending 2026-09-03
      FDR family  16 lanes (1 scanner x 2 directions x 3 horizons, 2 outcome modes)

.EXAMPLE
    .\run_composite_study.ps1
    .\run_composite_study.ps1 -FromStage 3
    .\run_composite_study.ps1 -WhatIfOnly
#>
[CmdletBinding()]
param(
    [ValidateRange(1, 7)] [int] $FromStage = 1,
    [int]    $Sessions           = 1034,
    [int]    $WarmupSessions     = 210,
    [string] $EndDate            = '2026-09-03',
    [string] $Scanner            = 'level_retest_rejection',
    [string] $EvaluationVersion  = 'composite_scanners_daily_qualification_v1',
    # Publication time for the qualification revision; must be reviewed, not "now".
    [string] $QualificationEffectiveFrom = '2026-09-03T00:00:00+00:00',
    [switch] $WhatIfOnly
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$Backend  = Split-Path -Parent $PSScriptRoot
$Python   = Join-Path $Backend '.venv\Scripts\python.exe'
$Artifact = Join-Path $Backend 'backups\intraday'
New-Item -ItemType Directory -Force -Path $Artifact | Out-Null

$EventsFile = Join-Path $Artifact 'composite_events_1d.jsonl'
$StudyEventsFile = Join-Path $Artifact "composite_events_1d_$Scanner.jsonl"

if (-not (Test-Path $Python)) { throw "python not found at $Python" }
Push-Location $Backend

function Invoke-Stage {
    param(
        [int] $Number,
        [string] $Name,
        [string[]] $Arguments,
        [string] $LogName
    )
    if ($Number -lt $FromStage) {
        Write-Host "[$Number/7] SKIP  $Name" -ForegroundColor DarkGray
        return
    }
    Write-Host ""
    Write-Host "[$Number/7] $Name" -ForegroundColor Cyan
    Write-Host "      $Python $($Arguments -join ' ')" -ForegroundColor DarkGray
    if ($WhatIfOnly) { return }

    $log = Join-Path $Artifact $LogName
    $started = Get-Date
    # Native stderr arrives as ErrorRecords under 2>&1; with ErrorActionPreference
    # Stop that aborts the pipeline before Tee writes the traceback.
    $previous = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        & $Python @Arguments 2>&1 | Tee-Object -FilePath $log
    }
    finally {
        $ErrorActionPreference = $previous
    }
    if ($LASTEXITCODE -ne 0) {
        throw "stage $Number ($Name) exited $LASTEXITCODE; log: $log`nRe-run with -FromStage $Number after fixing."
    }
    $elapsed = (Get-Date) - $started
    Write-Host ("      done in {0:hh\:mm\:ss}; log {1}" -f $elapsed, $log) -ForegroundColor Green
}

try {
    # 1. Point-in-time universes and corporate actions. The long stage: ~1,034
    #    paginated reference fetches. Resumes from cache and persisted universes.
    #    Deliberately no --backfill-bars: bar_revision_id is derived without
    #    `adjusted`, so for any unsplit ticker the unadjusted row collides on the
    #    primary key with the adjusted row stage 3 writes. Stage 3 supplies the
    #    lineage this study reads.
    Invoke-Stage 1 'Reconstruct universes and corporate actions' @(
        'scripts\prepare_historical_signal_research.py'
        '--persist'
        '--sessions', $Sessions
        '--bar-warmup-sessions', $WarmupSessions
        '--end', $EndDate
        '--backfill-actions'
        '--output', (Join-Path $Artifact 'composite_prepare.json')
    ) 'composite_prepare.log'

    # 2. Sector classification for historical members. Must be a second pass:
    #    candidates are derived from persisted universes, so stage 1 has to finish
    #    first. Sector-primary benchmarking is wrong without this.
    Invoke-Stage 2 'Backfill historical sector references' @(
        'scripts\prepare_historical_signal_research.py'
        '--persist'
        '--backfill-sector-references'
        '--output', (Join-Path $Artifact 'composite_sectors.json')
    ) 'composite_sectors.log'

    # 3. Split-adjusted lineage for every ticker any reconstructed universe admits.
    #    Without this, stages 4 and 5 find no adjusted bars and report UNAVAILABLE.
    Invoke-Stage 3 'Ingest adjusted bars for the universe union' @(
        'scripts\ingest_adjusted_daily_bars.py'
        '--start', '2021-09-07'
        '--end', $EndDate
        '--from-reconstructed-universes'
        '--apply'
        '--output', (Join-Path $Artifact 'composite_adjusted_bars.json')
    ) 'composite_adjusted_bars.log'

    # 4. Detect signals. --adjusted must match stage 5; detecting on one lineage
    #    and measuring on another is incoherent.
    Invoke-Stage 4 'Replay composite scanner signals' @(
        'scripts\run_historical_signal_research.py'
        '--signal', 'composite-scanners-1d-v1'
        '--adjusted'
        '--end', $EndDate
        '--events-output', $EventsFile
        '--output', (Join-Path $Artifact 'composite_research.json')
    ) 'composite_research.log'

    # 5. The adapter emits every registered composite scanner, so qualifying the
    #    raw file would put all seven in one FDR family (112 lanes). Restricting
    #    to the declared scanner keeps the family at the 16 lanes fixed above.
    if (5 -lt $FromStage) {
        Write-Host "[5/7] SKIP  Restrict events to $Scanner" -ForegroundColor DarkGray
    }
    else {
        Write-Host ""
        Write-Host "[5/7] Restrict events to $Scanner" -ForegroundColor Cyan
        if (-not $WhatIfOnly) {
            if (-not (Test-Path $EventsFile)) { throw "missing events file: $EventsFile" }
            $needle = '"source_name": "' + $Scanner + '"'
            $total = 0
            $kept  = 0
            $writer = [System.IO.StreamWriter]::new($StudyEventsFile, $false)
            try {
                foreach ($line in [System.IO.File]::ReadLines($EventsFile)) {
                    $total++
                    if ($line.Contains($needle)) { $writer.WriteLine($line); $kept++ }
                }
            }
            finally { $writer.Dispose() }
            Write-Host "      kept $kept of $total events -> $StudyEventsFile" -ForegroundColor Green
            if ($kept -eq 0) { throw "no events matched $Scanner; check the scanner name" }
        }
    }

    # 6. Persist evidence, evaluate outcomes, publish qualification. Horizons come
    #    from COMPOSITE_OUTCOME_HORIZONS, not from a flag.
    Invoke-Stage 6 'Evaluate outcomes and qualify' @(
        'scripts\run_historical_signal_outcomes.py'
        '--all'
        '--adjusted'
        '--events', $StudyEventsFile
        '--evaluation-version', $EvaluationVersion
        '--qualification-effective-from', $QualificationEffectiveFrom
        '--output', (Join-Path $Artifact 'composite_qualification.json')
    ) 'composite_qualification.log'

    # 7. Read-only verification of the published record.
    Invoke-Stage 7 'Report qualification status' @(
        'scripts\run_historical_signal_outcomes.py'
        '--status'
        '--evaluation-version', $EvaluationVersion
    ) 'composite_status.log'

    if (-not $WhatIfOnly) {
        Write-Host ""
        Write-Host "Study complete. Retained record: equity_qualification_revisions." -ForegroundColor Green
        Write-Host "Working rows may be discarded once the report is reviewed:" -ForegroundColor Yellow
        Write-Host "  $Python scripts\purge_research_scanner_data.py --source-prefix $Scanner --exclude-production --apply"
        Write-Host "Keep --drop-qualification OFF unless discarding the study as invalid." -ForegroundColor Yellow
    }
}
finally {
    Pop-Location
}
