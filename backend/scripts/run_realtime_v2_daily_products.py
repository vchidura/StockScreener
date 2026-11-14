#!/usr/bin/env python3
"""R78 (Nov 14): the daily products job for one (cohort, completed session) - daily strategies and, in JOB mode, the
Dashboard - built outside the Equity window and released on the cohort's current release (`realtime/daily_products.py`).

The Equity scheduler launches it as a separate, limited process (`DailyProductsLauncher`); an operator can run it by
hand with the same arguments. Exit codes: 0 PUBLISHED / ALREADY_STORED / SUPERSEDED / COMPUTED (dry run), 3 NO_RELEASE
(retry later), 1 failure (a FAULT record is written; the previous publications stay served).
"""
from __future__ import annotations

import argparse
import json
import os
import resource
import sys
import time
import traceback
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import UUID

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.daily_products import (  # noqa: E402
    MODE_JOB, MODE_STRATEGIES, NO_RELEASE, run_daily_products,
)
from realtime.database_access import RealtimeDatabaseRole  # noqa: E402
from realtime.fault_journal import emit_fault  # noqa: E402

SERVICE = "realtime-v2-daily-products"


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cohort-id", type=UUID, required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--mode", choices=(MODE_STRATEGIES, MODE_JOB), required=True)
    parser.add_argument("--cutoff", type=datetime.fromisoformat,
                        help="input cutoff (default now); must be at or after the session close")
    parser.add_argument("--trigger", default="MANUAL")
    parser.add_argument("--report-path", type=Path)
    parser.add_argument("--dry-run", action="store_true", help="compute only; read-only transactions, no writes")
    parser.add_argument("--source-closure", action="store_true",
                        help="read the 5m/1m sources of derived daily and 1h bars (the measured fallback)")
    parser.add_argument("--force", action="store_true", help="recompute even when the stored products match")
    parser.add_argument("--prepare", action="store_true",
                        help="before the open: compute and store the products without a release")
    parser.add_argument("--dsn-file", type=Path, help="rehearsal: DSN from a file instead of the service config")
    parser.add_argument("--memory-limit-mb", type=int, default=0, help="address-space limit (RLIMIT_AS); 0 = none")
    parser.add_argument("--cpu-seconds", type=int, default=0, help="CPU-time limit (RLIMIT_CPU); 0 = none")
    parser.add_argument("--nice", type=int, default=0, help="niceness increment for this process")
    parser.add_argument("--workers", type=int, default=1,
                        help="spawned processes for the per-member setup evidence (1 = in this process)")
    args = parser.parse_args(argv)
    if args.cutoff is not None and (args.cutoff.tzinfo is None or args.cutoff.utcoffset() is None):
        parser.error("--cutoff must be timezone-aware")
    return args


def _provider(args):
    from psycopg2.pool import ThreadedConnectionPool

    from realtime.database_access_v2 import APP_ROLE, V2AccessConfig, V2ConnectionProvider
    from realtime.migrations import EXPECTED_V2_DATABASE_NAME

    if args.dsn_file is not None:
        dsn = args.dsn_file.read_text(encoding="utf-8").strip()
    else:
        from realtime.equity_pipeline_config import RealtimeV2PipelineConfig, RealtimeV2Process

        dsn = RealtimeV2PipelineConfig.from_mapping(os.environ, process=RealtimeV2Process.APP_WORKER).app_dsn
    options = "-c timezone=UTC" + (" -c default_transaction_read_only=on" if args.dry_run else "")
    pool = ThreadedConnectionPool(1, 2, dsn=dsn, options=options)
    provider = V2ConnectionProvider(
        {APP_ROLE: pool},
        V2AccessConfig(security_mode="hardened", role_prefix=EXPECTED_V2_DATABASE_NAME, statement_timeout_ms=300_000),
    )
    provider.verify()
    return provider, pool


def _apply_limits(args):
    """The job limits itself (the scheduler spawns it without a fork-time hook): a lower CPU priority, and hard caps
    on address space and CPU time so a runaway run fails alone instead of starving the windows."""
    if args.nice > 0:
        os.nice(args.nice)
    if args.memory_limit_mb > 0:
        limit = args.memory_limit_mb * 1024 * 1024
        resource.setrlimit(resource.RLIMIT_AS, (limit, limit))
    if args.cpu_seconds > 0:
        resource.setrlimit(resource.RLIMIT_CPU, (args.cpu_seconds, args.cpu_seconds + 10))


def _vm_peak_mb():
    try:
        for line in Path("/proc/self/status").read_text(encoding="utf-8").splitlines():
            if line.startswith("VmPeak:"):
                return round(int(line.split()[1]) / 1024, 1)
    except OSError:
        return None
    return None


def main(argv=None) -> int:
    args = parse_args(argv)
    _apply_limits(args)
    started = time.perf_counter()
    report = dict(service=SERVICE, cohort_id=str(args.cohort_id), session=str(args.session_date), mode=args.mode,
                  trigger=args.trigger, dry_run=args.dry_run, started_at=datetime.now(timezone.utc).isoformat())
    code = 1
    pool = None
    try:
        from realtime.stage_database_v2 import V2WorkerDatabase

        provider, pool = _provider(args)
        outcome = run_daily_products(
            V2WorkerDatabase(provider, RealtimeDatabaseRole.COMPUTE),
            V2WorkerDatabase(provider, RealtimeDatabaseRole.PUBLICATION),
            cohort_id=args.cohort_id, session=args.session_date, cutoff=args.cutoff, mode=args.mode,
            dry_run=args.dry_run, source_closure=args.source_closure, force=args.force, workers=args.workers,
            prepare=args.prepare,
        )
        report.update(outcome.report())
        code = 3 if outcome.status == NO_RELEASE else 0
    except Exception as error:  # noqa: BLE001 - one record per failed run, then a non-zero exit
        report.update(status="FAILED", error=f"{type(error).__name__}: {error}"[:500])
        emit_fault(
            kind="DAILY_PRODUCTS_FAILED", severity="ERROR", message=f"{type(error).__name__}: {error}",
            recoverable=True, action="PREVIOUS_PUBLICATIONS_STAY_SERVED", service=SERVICE,
            key=f"{args.mode}:{type(error).__name__}",
            context=dict(cohort_id=str(args.cohort_id), session=str(args.session_date), trigger=args.trigger,
                         traceback=traceback.format_exc()[-3000:]),
        )
    finally:
        if pool is not None:
            pool.closeall()
        report.update(seconds=round(time.perf_counter() - started, 2),
                      maxrss_mb=round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1),
                      vm_peak_mb=_vm_peak_mb(), limits=dict(memory_mb=args.memory_limit_mb,
                                                           cpu_seconds=args.cpu_seconds, nice=args.nice),
                      finished_at=datetime.now(timezone.utc).isoformat())
        line = json.dumps(report, default=str, sort_keys=True)
        print("DAILY_PRODUCTS " + line, flush=True)
        if args.report_path is not None:
            args.report_path.parent.mkdir(parents=True, exist_ok=True)
            args.report_path.write_text(line + "\n", encoding="utf-8")
    return code


if __name__ == "__main__":
    raise SystemExit(main())
