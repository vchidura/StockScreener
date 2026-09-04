#!/usr/bin/env python3
"""Plan or execute a fresh canonical equity-data bootstrap in explicit phases."""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import date
from pathlib import Path

from dotenv import load_dotenv


BACKEND_DIR = Path(__file__).resolve().parent.parent
REPO_ROOT = BACKEND_DIR.parent
load_dotenv(BACKEND_DIR / ".env")

PHASES = (
    "schema",
    "universe",
    "reference",
    "native-bars",
    "coverage",
    "derive",
    "publish",
    "current-signals",
    "analyze",
    "snapshots",
    "research-inputs",
    "validate",
)
CORE_PHASES = tuple(phase for phase in PHASES if phase != "research-inputs")


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--confirm-database-name", required=True)
    result.add_argument("--history-start", required=True)
    result.add_argument("--intraday-start", required=True)
    result.add_argument("--end", required=True)
    result.add_argument("--target-size", type=int, default=350)
    result.add_argument("--lookback-days", type=int, default=20)
    result.add_argument(
        "--phase",
        action="append",
        choices=(*PHASES, "all"),
        required=True,
        help="Repeat to run selected resumable phases; 'all' excludes optional research inputs.",
    )
    result.add_argument(
        "--include-fundamentals",
        action="store_true",
        help="Fetch Polygon financial statements during the reference phase.",
    )
    result.add_argument(
        "--include-research-inputs",
        action="store_true",
        help="Include point-in-time scanner research inputs when --phase all is used.",
    )
    result.add_argument("--research-sessions", type=int, default=300)
    result.add_argument("--research-warmup-sessions", type=int, default=210)
    result.add_argument(
        "--execute",
        action="store_true",
        help="Execute the plan. Without this flag, commands are printed only.",
    )
    return result


def selected_phases(args: argparse.Namespace) -> tuple[str, ...]:
    if "all" not in args.phase:
        return tuple(dict.fromkeys(args.phase))
    phases = list(CORE_PHASES)
    if args.include_research_inputs:
        phases.insert(phases.index("validate"), "research-inputs")
    return tuple(phases)


def validate_args(args: argparse.Namespace) -> None:
    database_name = os.getenv("DB_NAME", "").strip()
    if not database_name:
        raise RuntimeError("DB_NAME is required in backend/.env")
    if args.confirm_database_name != database_name:
        raise RuntimeError("--confirm-database-name must exactly equal DB_NAME")
    history_start = date.fromisoformat(args.history_start)
    intraday_start = date.fromisoformat(args.intraday_start)
    end = date.fromisoformat(args.end)
    if history_start > intraday_start or intraday_start > end:
        raise ValueError("dates must satisfy history-start <= intraday-start <= end")
    if args.target_size <= 0 or args.lookback_days <= 0:
        raise ValueError("target-size and lookback-days must be positive")
    if args.research_sessions <= 0 or args.research_warmup_sessions < 0:
        raise ValueError("research session counts are invalid")


def build_commands(args: argparse.Namespace) -> list[tuple[str, list[str]]]:
    python = sys.executable
    scripts = BACKEND_DIR / "scripts"
    materialize = str(scripts / "run_equity_materialization.py")
    commands: list[tuple[str, list[str]]] = []

    for phase in selected_phases(args):
        if phase == "schema":
            commands.append((phase, [python, str(scripts / "initialize_database.py")]))
        elif phase == "universe":
            commands.append((phase, [
                python,
                str(scripts / "discover_universe_polygon.py"),
                "--if-empty",
                "--target-size",
                str(args.target_size),
                "--lookback-days",
                str(args.lookback_days),
            ]))
        elif phase == "reference":
            commands.append((phase, [
                python, materialize, "--reference", "--date", args.end,
            ]))
            if args.include_fundamentals:
                commands.append((phase, [
                    python, materialize, "--fundamentals", "--date", args.end,
                ]))
        elif phase == "native-bars":
            commands.append((phase, [
                python, materialize, "--bars", "--interval", "30m",
                "--from-date", args.history_start, "--date", args.end,
            ]))
            for interval in ("1m", "5m", "15m"):
                commands.append((phase, [
                    python, materialize, "--bars", "--interval", interval,
                    "--from-date", args.intraday_start, "--date", args.end,
                ]))
        elif phase == "coverage":
            for interval in ("1m", "5m", "15m", "30m"):
                start = (
                    args.history_start if interval == "30m" else args.intraday_start
                )
                commands.append((phase, [
                    python, materialize, "--coverage-report", "--interval", interval,
                    "--from-date", start, "--date", args.end,
                ]))
        elif phase == "derive":
            for interval in ("1h", "1d", "1wk", "1mo"):
                commands.append((phase, [
                    python, materialize, "--derive-history", "--interval", interval,
                    "--date", args.end,
                ]))
        elif phase == "publish":
            command = [python, materialize, "--publish-bars"]
            for interval in ("1m", "5m", "15m", "30m", "1h", "1d", "1wk", "1mo"):
                command.extend(("--interval", interval))
            commands.append((phase, command))
        elif phase == "current-signals":
            commands.extend((
                (phase, [
                    python,
                    str(scripts / "generate_cross_sectional_signal.py"),
                    "--date",
                    args.end,
                ]),
                (phase, [
                    python,
                    str(scripts / "generate_market_discovery.py"),
                    "--date",
                    args.end,
                ]),
            ))
        elif phase == "analyze":
            command = [python, materialize, "--analyze"]
            for interval in ("5m", "15m", "30m", "1h", "1d", "1wk", "1mo"):
                command.extend(("--interval", interval))
            commands.append((phase, command))
        elif phase == "snapshots":
            commands.append((phase, [
                python, str(scripts / "refresh_equity_portal_snapshots.py"),
            ]))
        elif phase == "research-inputs":
            commands.append((phase, [
                python,
                str(scripts / "prepare_historical_signal_research.py"),
                "--persist",
                "--sessions",
                str(args.research_sessions),
                "--bar-warmup-sessions",
                str(args.research_warmup_sessions),
                "--end",
                args.end,
                "--backfill-actions",
                "--backfill-bars",
                "--backfill-sector-references",
                "--output",
                str(
                    BACKEND_DIR
                    / ".cache"
                    / "historical-signal-research"
                    / "bootstrap-report.json"
                ),
            ]))
        elif phase == "validate":
            commands.extend((
                (phase, [python, str(scripts / "validate_equity_storage.py")]),
                (phase, [python, str(scripts / "validate_cutover_environment.py")]),
            ))
        else:
            raise AssertionError(f"unsupported phase: {phase}")
    return commands


def main() -> int:
    args = parser().parse_args()
    validate_args(args)
    commands = build_commands(args)
    mode = "EXECUTE" if args.execute else "PLAN"
    print(
        f"BOOTSTRAP_{mode} database={args.confirm_database_name} "
        f"phases={','.join(selected_phases(args))}"
    )
    for phase, command in commands:
        print(f"[{phase}] {subprocess.list2cmdline(command)}", flush=True)
        if args.execute:
            subprocess.run(command, cwd=REPO_ROOT, check=True)
    if not args.execute:
        print("No commands executed. Add --execute after reviewing the plan.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
