#!/usr/bin/env python3
"""Offline profile of the Ticker Workspace projection on the nine-name synthetic fixture (E1/E3).

No database, no provider, no network. Reports seconds per ticker-cycle, manifest bytes per
snapshot (the hot portal cap is 16 KiB, the lineage-head bar is 10 KB) and, with --cprofile,
the heaviest functions. It measures three cases against the gates:
  cold          a fresh projection state (first cycle after a restart),
  warm          the same bars one cycle later (cutoff advanced), nothing changed,
  one_interval  one interval (1mo) revised since the previous cycle.
Run from the repository root with the backend virtual environment:
python -B backend/scripts/profile_realtime_projection.py --cold-runs 3
Add --aggregates for the aggregate manifest gates and --payload-breakdown for the per-ticker payload bytes
by section, before (charts and evidence embedded) and after (served by reference).

E2 scaling curve (cloned tickers with distinct symbols, security, instrument and bar ids; the same bar shapes):
python -B backend/scripts/profile_realtime_projection.py --scale-curve 9,50,100,200,400 --scale-report <new file>
runs each point in a fresh subprocess in ascending order, records seconds per ticker-cycle (cold, warm, one
interval revised), cycle totals, peak working set, payload and manifest bytes and the ratio to nine names (gate
1.3x), and records NOT_RUN, with the extrapolation, for any point whose linearly projected peak working set
exceeds --projection-limit-gib (default 24; the E2 envelope is 32). A point also stops itself (exit 97) if its
working set passes --rss-limit-gib. Use --quiet-wait-seconds to wait for a quiet machine before each point:
the load observed before and after each point is part of its record. `--scale N` runs one point in-process.
"""
from __future__ import annotations

import argparse
import cProfile
import gc
import hashlib
import json
import os
import pstats
import statistics
import subprocess
import sys
import threading
from dataclasses import replace
from datetime import date, datetime, timedelta
from pathlib import Path
from time import perf_counter, process_time, sleep
from uuid import NAMESPACE_URL, UUID, uuid5

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.contracts import canonical_json, canonical_sha256  # noqa: E402
import realtime.ticker_projection as ticker_projection  # noqa: E402
from tests.realtime.test_ticker_vertical_slice import ticker_fixture  # noqa: E402

TICKER_SECONDS_BAR = 0.6
MANIFEST_BYTES_BAR = 10 * 1024
VARIANCE_BAR = 0.10


def _bytes(value) -> int:
    return len(canonical_json(value).encode("utf-8"))


def _run(inputs, state=None, cpu=None):
    started, cpu_started = perf_counter(), process_time()
    projection = ticker_projection.project_realtime_ticker_workspace(inputs, state=state)
    if cpu is not None:
        cpu.append(process_time() - cpu_started)
    return perf_counter() - started, projection


def _revise_monthly(inputs, salt=0):
    """A new revision of every 1mo bar (new id and payload hash), as a provider correction would be."""
    bars = []
    for row in inputs.bars:
        if row["interval"] == "1mo":
            row = dict(
                row, bar_revision_id=uuid5(NAMESPACE_URL, f"revised:{salt}:{row['bar_revision_id']}"),
                payload_sha256=canonical_sha256(("revised", salt, row["payload_sha256"])),
                close=row["close"] + 0.01,
            )
        bars.append(row)
    return replace(inputs, bars=tuple(bars))


def _aggregate_manifest_bytes(inputs, projection):
    """Hot aggregate manifests at nine names (the 16 KiB database cap applies to each)."""
    from realtime.dashboard_projection import produce_realtime_dashboard
    from realtime.stock_alert_projection import produce_realtime_stock_alerts
    from tests.realtime.test_dashboard_vertical_slice import dashboard_fixture
    from tests.realtime.test_stock_alert_vertical_slice import alert_fixture

    measured = {
        "TICKER_WORKSPACE_SUMMARY_V1": ticker_projection.produce_realtime_ticker_workspace_summary(
            inputs, projection.snapshots, projection.summary_charts),
        "PATTERN_WATCH_V1": ticker_projection.produce_realtime_pattern_watch_publication(
            inputs, projection.snapshots),
        "DASHBOARD": produce_realtime_dashboard(dashboard_fixture()),
        "STOCK_ALERTS": produce_realtime_stock_alerts(alert_fixture()),
    }
    return {name: _bytes(snapshot.source_manifest) for name, snapshot in measured.items()}


def _payload_breakdown(inputs, projection):
    """Per-ticker payload bytes by section, before and after serving charts and evidence by reference.

    `after` is the stored payload. `before` rebuilds the payload that embedded every chart point and every
    evidence row (the release-only 5m and 15m pattern evidence included) with the old row arithmetic, so the
    two columns describe the same underlying data."""
    from realtime.equity_portal_history import build_realtime_equity_portal_history_plan
    from realtime.ticker_references import embedded_evidence_row_of

    plan = build_realtime_equity_portal_history_plan(inputs.session, equity_ticker_count=len(inputs.members))
    adjusted_sessions = frozenset(plan.adjusted_sessions)
    persisted = {
        str(row.evidence_id): row
        for result in (*projection.materializations, *projection.pattern_materializations)
        for row in result.materialization.evidence
    }
    members = {member["ticker"]: member for member in inputs.members}
    rows_by_group: dict[tuple, list] = {}
    for row in inputs.bars:
        rows_by_group.setdefault((row["ticker"], row["interval"], row["adjustment_basis"]), []).append(row)
    tickers, sections, chart_intervals = {}, {}, {}
    for snapshot in projection.snapshots:
        ticker, payload = snapshot.snapshot_key, snapshot.payload
        facts_by_interval = {}
        for interval, key in payload["chart_refs"].items():
            basis = key.partition(":")[2]
            rows = sorted(rows_by_group[(ticker, interval, basis)], key=lambda row: row["bar_start"])
            if (interval, basis) == ("1d", "ADJUSTED"):
                rows = [row for row in rows if row["session_date"] in adjusted_sessions]
            facts_by_interval[interval] = tuple(ticker_projection._fact(row, inputs) for row in rows)
        charts = {
            interval: [ticker_projection._chart_point(fact) for fact in facts]
            for interval, facts in facts_by_interval.items()
        }
        evidence = {
            identity: [embedded_evidence_row_of(persisted[ref["evidence_id"]]) for ref in refs]
            for identity, refs in payload["evidence_index"].items()
        }
        security = ticker_projection._security(members[ticker])
        for interval in ("5m", "15m"):
            if interval in facts_by_interval and len(facts_by_interval[interval]) >= 50:
                identity, result = ticker_projection.materialize_ticker_interval(
                    inputs, security, facts_by_interval[interval][-300:], (), interval=interval,
                    basis="RAW_UNADJUSTED", policy_sha256=plan.policy_sha256)
                evidence[identity] = [embedded_evidence_row_of(row) for row in result.materialization.evidence]
        before = {key: value for key, value in payload.items() if key not in {"chart_refs", "evidence_index"}}
        before.update(charts=charts, evidence=evidence)
        for name, value in before.items():
            entry = sections.setdefault(name, {"before": 0, "after": 0})
            entry["before"] += _bytes(value)
        for name, value in payload.items():
            sections.setdefault(name, {"before": 0, "after": 0})["after"] += _bytes(value)
        for interval, points in charts.items():
            chart_intervals[interval] = chart_intervals.get(interval, 0) + _bytes(points)
        manifest = _bytes(snapshot.source_manifest)
        tickers[ticker] = {
            "payload_before": _bytes(before), "payload_after": _bytes(payload), "manifest": manifest,
            "total_before": _bytes(before) + manifest, "total_after": _bytes(payload) + manifest,
        }
    count = len(tickers)
    totals = {key: sum(row[key] for row in tickers.values()) for key in next(iter(tickers.values()))}
    return {
        "tickers": count,
        "per_ticker": tickers,
        "mean_per_ticker": {key: round(value / count) for key, value in totals.items()},
        "payload_section_bytes_total": dict(sorted(sections.items(), key=lambda item: -item[1]["before"])),
        "chart_interval_bytes_before_total": dict(sorted(chart_intervals.items(), key=lambda item: -item[1])),
        "reduction_ratio": round(totals["total_before"] / totals["total_after"], 1),
        "linear_400_ticker_cycle_bytes": {
            "before": round(totals["total_before"] / count * 400), "after": round(totals["total_after"] / count * 400),
        },
    }


def _time_cases(inputs, cold_runs, cycles, gc_clock=None, on_primed=None):
    """Time the three gate cases on `inputs`: cold (fresh state each run), warm (same bars, cutoff advanced) and
    one interval revised. Returns the raw lists plus the last projections."""
    cases = dict(cold=[], cold_cpu=[], cold_gc=[], warm=[], warm_cpu=[], changed=[], changed_cpu=[])
    projection = None
    for _ in range(max(1, cold_runs)):
        gc_before = gc_clock.seconds if gc_clock else 0.0
        elapsed, projection = _run(inputs, ticker_projection.TickerProjectionState(), cases["cold_cpu"])
        cases["cold"].append(elapsed)
        cases["cold_gc"].append((gc_clock.seconds if gc_clock else 0.0) - gc_before)
    state = ticker_projection.TickerProjectionState()
    _run(inputs, state)
    if on_primed:
        on_primed()
    warm_stats, warm_projection, later = {}, None, inputs
    for _ in range(max(1, cycles)):
        later = replace(later, cutoff=later.cutoff + timedelta(minutes=5))
        elapsed, warm_projection = _run(later, state, cases["warm_cpu"])
        cases["warm"].append(elapsed)
        warm_stats = dict(state.stats)
    changed_stats = {}
    for salt in range(max(1, cycles)):
        later = replace(_revise_monthly(later, salt), cutoff=later.cutoff + timedelta(minutes=5))
        elapsed, _ = _run(later, state, cases["changed_cpu"])
        cases["changed"].append(elapsed)
        changed_stats = dict(state.stats)
    return dict(
        cases, projection=projection, warm_projection=warm_projection, later=later, state=state,
        warm_stats=warm_stats, changed_stats=changed_stats,
    )


# ---- E2 scaling curve: N cloned tickers, one fresh subprocess per point -----------------------------------------

SCALE_POINTS = (9, 50, 100, 200, 400)
GIB = 1 << 30
PROJECTION_LIMIT_GIB = 24  # shared machine; the E2 envelope is 32 GiB
E2_RATIO_BAR = 1.3
POINT_MARKER = "SCALE_POINT_JSON "
_ZERO = timedelta(0)


def _memory():
    """(resident, peak) working set of this process in bytes: psutil if installed, else GetProcessMemoryInfo."""
    try:
        import psutil
    except ImportError:
        return _memory_ctypes()
    info = psutil.Process().memory_info()
    return info.rss, getattr(info, "peak_wset", info.rss)


def _memory_ctypes():
    import ctypes
    from ctypes import wintypes

    class Counters(ctypes.Structure):
        _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD)] + [
            (name, ctypes.c_size_t) for name in (
                "PeakWorkingSetSize", "WorkingSetSize", "QuotaPeakPagedPoolUsage", "QuotaPagedPoolUsage",
                "QuotaPeakNonPagedPoolUsage", "QuotaNonPagedPoolUsage", "PagefileUsage", "PeakPagefileUsage")]

    kernel32, psapi = ctypes.WinDLL("kernel32"), ctypes.WinDLL("psapi")
    kernel32.GetCurrentProcess.restype = wintypes.HANDLE
    psapi.GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.POINTER(Counters), wintypes.DWORD]
    counters = Counters()
    counters.cb = ctypes.sizeof(counters)
    if not psapi.GetProcessMemoryInfo(kernel32.GetCurrentProcess(), ctypes.byref(counters), counters.cb):
        raise OSError("GetProcessMemoryInfo failed")
    return counters.WorkingSetSize, counters.PeakWorkingSetSize


class _GcClock:
    """Seconds spent in the cyclic garbage collector (a heap of millions of row dicts makes it visible)."""

    def __init__(self):
        self.seconds, self._started = 0.0, None

    def __call__(self, phase, info):
        if phase == "start":
            self._started = perf_counter()
        elif self._started is not None:
            self.seconds += perf_counter() - self._started
            self._started = None


def _contention(sample_seconds=1.0):
    """System CPU percent and the other busy python processes (others run DB tests on this machine)."""
    try:
        import psutil
    except ImportError:
        return dict(system_cpu_percent=None, busy_python=None)
    own = {psutil.Process().pid, psutil.Process().ppid()}
    processes = [p for p in psutil.process_iter(["pid", "name"])
                 if p.info["pid"] not in own and (p.info["name"] or "").lower().startswith(("python", "pytest"))]
    for process in processes:
        try:
            process.cpu_percent(None)
        except psutil.Error:
            pass
    system = psutil.cpu_percent(interval=sample_seconds)
    busy = []
    for process in processes:
        try:
            if process.cpu_percent(None) >= 5:
                busy.append(process.pid)
        except psutil.Error:
            pass
    return dict(system_cpu_percent=system, busy_python=len(busy))


def _wait_quiet(limit_percent, wait_seconds):
    """Wait until the machine is quiet (system CPU below `limit_percent` on three consecutive samples)."""
    deadline, quiet = perf_counter() + wait_seconds, 0
    while True:
        observed = _contention()
        quiet = quiet + 1 if (observed["system_cpu_percent"] or 0) < limit_percent else 0
        if quiet >= 3 or perf_counter() >= deadline:
            return dict(observed, contended=quiet < 3, waited_seconds=round(wait_seconds - (deadline - perf_counter())))
        sleep(5)


def _own(value):
    """A fresh object with the same value, so a cloned row holds as many distinct objects as a row read from
    the database (psycopg2 builds new floats, datetimes, strings and UUIDs for every row)."""
    kind = type(value)
    if kind is float:
        return value + 0.0
    if kind is str:
        return (value + ".")[:-1]
    if kind is datetime or kind is date:
        return value + _ZERO
    if kind is UUID:
        return UUID(int=value.int)
    return value


def _clone_ticker(generation, instrument_id, symbol, rows, member, actions, coverage):
    """One cloned member: distinct symbol, security, instrument and bar identities, the same bar shapes."""
    def ident(kind, old):
        return uuid5(NAMESPACE_URL, f"scale:{generation}:{kind}:{old}")

    security_id = ident("security", member["security_id"])
    member = dict(
        member, security_id=security_id, ticker=symbol, instrument_id=instrument_id,
        security_revision_id=ident("revision", member["security_revision_id"]),
        company_name=f"{symbol} synthetic company", cik=f"{instrument_id:010d}",
        composite_figi=f"BBG{instrument_id:09d}", share_class_figi=f"BBG{instrument_id + 100_000:09d}",
        payload_sha256=hashlib.sha256(f"{generation}:{member['payload_sha256']}".encode()).hexdigest(),
    )
    bar_ids = {}

    def bar(old):
        if old not in bar_ids:
            bar_ids[old] = ident("bar", old)
        return UUID(int=bar_ids[old].int)

    bars = []
    for row in rows:
        clone = {key: _own(value) for key, value in row.items()}
        clone.update(
            ticker=_own(symbol), security_id=UUID(int=security_id.int), instrument_id=instrument_id,
            bar_revision_id=bar(row["bar_revision_id"]),
            source_revision_ids=tuple(bar(value) for value in row["source_revision_ids"]),
            payload_sha256=hashlib.sha256(f"{generation}:{row['payload_sha256']}".encode()).hexdigest(),
        )
        if row["ingestion_segment_id"] is not None:
            clone["ingestion_segment_id"] = ident("segment", row["ingestion_segment_id"])
        bars.append(clone)
    cloned_actions = [dict(
        action, action_id=ident("action", action["action_id"]), security_id=security_id, ticker=symbol,
        payload_sha256=hashlib.sha256(f"{generation}:{action['payload_sha256']}".encode()).hexdigest(),
    ) for action in actions]
    cloned_coverage = [dict(
        row, coverage_id=ident("coverage", row["coverage_id"]), security_id=security_id,
        response_sha256=hashlib.sha256(f"{generation}:{row['response_sha256']}".encode()).hexdigest(),
        payload_sha256=hashlib.sha256(f"{generation}:{row['payload_sha256']}".encode()).hexdigest(),
    ) for row in coverage]
    return member, bars, cloned_actions, cloned_coverage


def scaled_inputs(base, count):
    """`count` tickers: the nine fixture tickers, then clones (generation g of base ticker b is ticker
    `<b><g:03d>`), in ascending order of count so a smaller input is always a prefix of a larger one. The rows
    are appended generation by generation into the final tuple; the projection indexes every row by revision id
    and by ticker, so the whole cohort is resident however the input is produced."""
    ordered = sorted(base.members, key=lambda row: row["ticker"])
    by_ticker = {}
    for row in base.bars:
        by_ticker.setdefault(row["ticker"], []).append(row)
    members, bars, actions, coverage = list(base.members), list(base.bars), list(base.actions), list(base.coverage)
    for index in range(len(ordered), count):
        generation, origin = divmod(index, len(ordered))
        source = ordered[origin]
        security = str(source["security_id"])
        member, rows, cloned_actions, cloned_coverage = _clone_ticker(
            generation, source["instrument_id"] + len(ordered) * generation,
            f"{source['ticker']}{generation:03d}", by_ticker[source["ticker"]], source,
            [row for row in base.actions if str(row["security_id"]) == security],
            [row for row in base.coverage if str(row["security_id"]) == security],
        )
        members.append(member)
        bars.extend(rows)
        actions.extend(cloned_actions)
        coverage.extend(cloned_coverage)
    members = members[:count]
    keep = {str(member["security_id"]) for member in members}
    return replace(
        base, members=tuple(members), bars=tuple(bars),
        actions=tuple(row for row in actions if str(row["security_id"]) in keep),
        coverage=tuple(row for row in coverage if str(row["security_id"]) in keep),
    )


def _watchdog(limit_gib):
    """Stop this process (exit 97) the moment its working set passes the limit: the machine is shared."""
    def watch():
        while True:
            if _memory()[0] > limit_gib * GIB:
                print(f"ABORTED_RSS working set above {limit_gib} GiB", flush=True)
                os._exit(97)
            sleep(0.25)
    threading.Thread(target=watch, daemon=True).start()


def _scale_point(count, args):
    gc_clock = _GcClock()
    gc.callbacks.append(gc_clock)
    _watchdog(args.rss_limit_gib)
    base = ticker_fixture()
    rss_base = _memory()[0]
    load = [dict(_contention(), moment="before")]
    started = perf_counter()
    inputs = scaled_inputs(base, count)
    build_seconds = perf_counter() - started
    rss_inputs = _memory()[0]
    cloned_bars = len(inputs.bars) - len(base.bars)
    primed = {}
    cases = _time_cases(
        inputs, args.cold_runs, args.cycles, gc_clock, on_primed=lambda: primed.update(rss=_memory()[0]))
    load.append(dict(_contention(), moment="after"))
    rss, peak = _memory()
    tickers, bars = len(inputs.members), len(inputs.bars)
    snapshots = cases["warm_projection"].snapshots
    payload_bytes = [_bytes(snapshot.payload) for snapshot in snapshots]
    manifest_bytes = [_bytes(snapshot.source_manifest) for snapshot in snapshots]
    median = statistics.median
    per_ticker = {
        "cold": statistics.mean(cases["cold"]) / tickers, "warm": median(cases["warm"]) / tickers,
        "one_interval": median(cases["changed"]) / tickers,
    }
    point = {
        "tickers": tickers, "status": "RUN", "bars": bars, "bars_per_ticker": round(bars / tickers),
        "runs": {"cold": len(cases["cold"]), "cycles": len(cases["warm"])},
        "seconds_per_ticker_cycle": {key: round(value, 3) for key, value in per_ticker.items()},
        "cycle_seconds_total": {
            "cold": round(statistics.mean(cases["cold"]), 2), "warm": round(median(cases["warm"]), 2),
            "one_interval": round(median(cases["changed"]), 2)},
        "cold_runs_seconds": [round(value, 2) for value in cases["cold"]],
        "cpu_seconds_per_ticker_cycle": {
            "cold": round(median(cases["cold_cpu"]) / tickers, 3), "warm": round(median(cases["warm_cpu"]) / tickers, 3),
            "one_interval": round(median(cases["changed_cpu"]) / tickers, 3)},
        "gc_seconds_per_cold_cycle": round(statistics.mean(cases["cold_gc"]), 2),
        "peak_working_set_gib": round(peak / GIB, 2), "working_set_end_gib": round(rss / GIB, 2),
        "working_set_gib": {"base_fixture": round(rss_base / GIB, 2), "inputs": round(rss_inputs / GIB, 2),
                            "state_primed": round(primed["rss"] / GIB, 2)},
        "bytes_per_bar": {
            "cloned_row_dict": round((rss_inputs - rss_base) / cloned_bars) if cloned_bars else None,
            "projection_state_and_output": round((primed["rss"] - rss_inputs) / bars),
            "peak_above_base": round((peak - rss_base) / bars)},
        "input_build_seconds": round(build_seconds, 1),
        "payload_bytes_per_ticker_mean": round(statistics.mean(payload_bytes)), "payload_bytes_total": sum(payload_bytes),
        "manifest_bytes_max": max(manifest_bytes), "manifest_bytes_total": sum(manifest_bytes),
        "state_stats_warm": cases["warm_stats"], "load": load,
    }
    print(POINT_MARKER + json.dumps(point), flush=True)
    return 0


def _projected_peak_gib(measured, bars):
    """Peak working set at `bars` by linear extrapolation of the measured (bars, peak GiB) points: a least-squares
    line through two or more points, a line through the origin for one."""
    if not measured:
        return None
    if len(measured) == 1 or len({point[0] for point in measured}) == 1:
        return measured[-1][1] / measured[-1][0] * bars
    xs, ys = [point[0] for point in measured], [point[1] for point in measured]
    mean_x, mean_y = statistics.mean(xs), statistics.mean(ys)
    slope = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys)) / sum((x - mean_x) ** 2 for x in xs)
    return mean_y + slope * (bars - mean_x)


def _scale_curve(args):
    points = sorted({int(value) for value in args.scale_curve.split(",")})
    results, measured, bars_per_ticker = [], [], None
    for count in points:
        projection = _projected_peak_gib(measured, count * bars_per_ticker) if bars_per_ticker else None
        if projection is not None and projection > args.projection_limit_gib:
            slope = (measured[-1][1] - measured[0][1]) / max(1, measured[-1][0] - measured[0][0]) * (1 << 20) if len(measured) > 1 else None
            results.append({
                "tickers": count, "status": "NOT_RUN", "bars": count * bars_per_ticker,
                "projected_peak_gib": round(projection, 1), "projection_limit_gib": args.projection_limit_gib,
                "projection_basis": [{"bars": bars, "peak_gib": peak} for bars, peak in measured],
                "peak_bytes_per_bar_measured": round(measured[-1][1] * GIB / measured[-1][0]),
                "marginal_bytes_per_bar": round(slope * 1024) if slope is not None else None,
            })
            print(f"[{count}] NOT_RUN: projected peak {projection:.1f} GiB > {args.projection_limit_gib} GiB", flush=True)
            continue
        quiet = _wait_quiet(args.quiet_cpu_percent, args.quiet_wait_seconds) if args.quiet_wait_seconds else _contention()
        cold_runs = args.cold_runs if args.cold_runs else (3 if count <= 9 else 2 if count <= 50 else 1)
        cycles = args.cycles if args.cycles else (5 if count <= 9 else 3 if count <= 50 else 2)
        command = [sys.executable, "-B", "-W", "ignore", str(Path(__file__).resolve()), "--scale", str(count),
                   "--cold-runs", str(cold_runs), "--cycles", str(cycles), "--rss-limit-gib", str(args.rss_limit_gib)]
        print(f"[{count}] running ({cold_runs} cold, {cycles} cycles); load before: {quiet}", flush=True)
        completed = subprocess.run(command, capture_output=True, text=True)
        line = next((row for row in completed.stdout.splitlines() if row.startswith(POINT_MARKER)), None)
        if completed.returncode != 0 or line is None:
            results.append({"tickers": count, "status": "ABORTED" if completed.returncode == 97 else "FAILED",
                            "returncode": completed.returncode, "tail": (completed.stdout + completed.stderr)[-600:]})
            print(f"[{count}] {results[-1]['status']} (exit {completed.returncode})", flush=True)
            break
        point = json.loads(line[len(POINT_MARKER):])
        point["start_load"] = quiet
        results.append(point)
        measured.append((point["bars"], point["peak_working_set_gib"]))
        bars_per_ticker = bars_per_ticker or point["bars_per_ticker"]
        print(f"[{count}] done: {point['seconds_per_ticker_cycle']} s/ticker, peak {point['peak_working_set_gib']} GiB", flush=True)
    nine = next((row for row in results if row["tickers"] == 9 and row["status"] == "RUN"), None)
    for row in results:
        if row["status"] == "RUN" and nine:
            row["ratio_vs_nine"] = {key: round(value / nine["seconds_per_ticker_cycle"][key], 2)
                                    for key, value in row["seconds_per_ticker_cycle"].items()}
            row["gate_e2_within_1_3x"] = all(value <= E2_RATIO_BAR for value in row["ratio_vs_nine"].values())
    report = {"e2_ratio_bar": E2_RATIO_BAR, "projection_limit_gib": args.projection_limit_gib, "points": results}
    print(json.dumps(report, indent=1))
    if args.scale_report:
        args.scale_report.write_text(json.dumps(report, indent=1), encoding="utf-8")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cold-runs", type=int, default=None)
    parser.add_argument("--cycles", type=int, default=None, help="warm and one-interval cycles to time")
    parser.add_argument("--tickers", type=int, default=0, help="use only the first N of the nine members (E2 shape)")
    parser.add_argument("--cprofile", choices=("cold", "warm"), help="print the heaviest functions of one run")
    parser.add_argument("--top", type=int, default=18)
    parser.add_argument("--cprofile-sort", choices=("cumulative", "tottime"), default="cumulative")
    parser.add_argument("--cprofile-callers", metavar="REGEX", help="also print the callers of matching functions")
    parser.add_argument("--aggregates", action="store_true", help="also measure the hot aggregate manifests")
    parser.add_argument("--payload-breakdown", action="store_true",
                        help="report per-ticker payload bytes by section, embedded bodies (before) vs references (after)")
    parser.add_argument("--scale", type=int, metavar="N",
                        help="E2: time and measure ONE point of N cloned tickers in this process (use "
                             "--scale-curve for the safe subprocess-per-point curve)")
    parser.add_argument("--scale-curve", metavar="N,N,...", nargs="?", const=",".join(map(str, SCALE_POINTS)),
                        help="E2: run ascending points, each in a fresh subprocess, skipping (NOT_RUN) any point whose "
                             "linearly extrapolated peak RSS exceeds --projection-limit-gib")
    parser.add_argument("--projection-limit-gib", type=float, default=PROJECTION_LIMIT_GIB)
    parser.add_argument("--rss-limit-gib", type=float, default=PROJECTION_LIMIT_GIB + 2,
                        help="a point aborts itself when its working set passes this (watchdog)")
    parser.add_argument("--quiet-cpu-percent", type=float, default=25.0)
    parser.add_argument("--quiet-wait-seconds", type=int, default=0,
                        help="wait up to this long for system CPU to drop below --quiet-cpu-percent before a point")
    parser.add_argument("--scale-report", type=Path, help="write the curve as JSON here")
    args = parser.parse_args()
    if args.scale_curve:
        return _scale_curve(args)
    if args.scale:
        args.cold_runs = args.cold_runs or 1
        args.cycles = args.cycles or 2
        return _scale_point(args.scale, args)
    args.cold_runs = args.cold_runs or 3
    args.cycles = args.cycles or 5
    inputs = ticker_fixture()
    if args.tickers:
        members = tuple(sorted(inputs.members, key=lambda row: row["ticker"]))[:args.tickers]
        identities = {str(member["security_id"]) for member in members}
        names = {member["ticker"] for member in members}
        inputs = replace(
            inputs, members=members, bars=tuple(row for row in inputs.bars if row["ticker"] in names),
            actions=tuple(row for row in inputs.actions if str(row["security_id"]) in identities),
            coverage=tuple(row for row in inputs.coverage if str(row["security_id"]) in identities),
        )
    tickers = len(inputs.members)
    load_before = _contention()
    gc_clock = _GcClock()
    gc.callbacks.append(gc_clock)
    cases = _time_cases(inputs, args.cold_runs, args.cycles, gc_clock)
    cold, cold_cpu, warm, warm_cpu = cases["cold"], cases["cold_cpu"], cases["warm"], cases["warm_cpu"]
    changed, changed_cpu = cases["changed"], cases["changed_cpu"]
    projection, warm_projection, later, state = (
        cases["projection"], cases["warm_projection"], cases["later"], cases["state"])
    warm_stats, changed_stats = cases["warm_stats"], cases["changed_stats"]
    load_after = _contention()
    manifest_bytes = [_bytes(snapshot.source_manifest) for snapshot in warm_projection.snapshots]
    sections: dict[str, int] = {}
    for snapshot in warm_projection.snapshots:
        for name, value in snapshot.source_manifest.items():
            sections[name] = sections.get(name, 0) + _bytes(value)
    mean_cold = statistics.mean(cold)
    spread = (max(cold) - min(cold)) / mean_cold if len(cold) > 1 else 0.0
    per_ticker = {
        "cold": mean_cold / tickers, "warm": statistics.median(warm) / tickers,
        "one_interval": statistics.median(changed) / tickers,
    }
    report = {
        "tickers": tickers,
        "input_bar_rows": len(inputs.bars),
        "cold_runs_seconds": [round(value, 3) for value in cold],
        "cold_run_spread_ratio": round(spread, 3),
        "gate_variance_met": spread <= VARIANCE_BAR,
        "warm_runs_seconds": [round(value, 3) for value in warm],
        "one_interval_runs_seconds": [round(value, 3) for value in changed],
        "seconds_per_ticker_cycle_median": {key: round(value, 3) for key, value in per_ticker.items()},
        "seconds_per_ticker_cycle_max": {
            "warm": round(max(warm) / tickers, 3), "one_interval": round(max(changed) / tickers, 3)},
        "cpu_seconds_per_ticker_cycle_median": {
            "cold": round(statistics.median(cold_cpu) / tickers, 3),
            "warm": round(statistics.median(warm_cpu) / tickers, 3),
            "one_interval": round(statistics.median(changed_cpu) / tickers, 3)},
        "gate_seconds_per_ticker": TICKER_SECONDS_BAR,
        "gate_seconds_met_on_median": {key: value <= TICKER_SECONDS_BAR for key, value in per_ticker.items()},
        "state_stats_warm": warm_stats,
        "state_stats_one_interval": changed_stats,
        "manifest_bytes_max": max(manifest_bytes),
        "manifest_bytes_total": sum(manifest_bytes),
        "gate_manifest_bytes": MANIFEST_BYTES_BAR,
        "gate_manifest_met": max(manifest_bytes) <= MANIFEST_BYTES_BAR,
        "manifest_section_bytes_total": dict(sorted(sections.items(), key=lambda item: -item[1])),
        "payload_bytes_total": sum(_bytes(snapshot.payload) for snapshot in warm_projection.snapshots),
        "gc_seconds_per_cold_cycle": round(statistics.mean(cases["cold_gc"]), 3),
        "load": {"before": load_before, "after": load_after},
    }
    if args.aggregates:
        sizes = _aggregate_manifest_bytes(inputs, warm_projection)
        report["aggregate_manifest_bytes"] = sizes
        report["gate_aggregate_manifest_met"] = {name: size <= MANIFEST_BYTES_BAR for name, size in sizes.items()}
    if args.payload_breakdown:
        report["payload_breakdown"] = _payload_breakdown(inputs, projection)
    print(json.dumps(report, indent=1))
    if args.cprofile:
        profiler = cProfile.Profile()
        target_state = ticker_projection.TickerProjectionState() if args.cprofile == "cold" else state
        profiler.enable()
        ticker_projection.project_realtime_ticker_workspace(later, state=target_state)
        profiler.disable()
        pstats.Stats(profiler).sort_stats(args.cprofile_sort).print_stats(args.top)
        if args.cprofile_callers:
            pstats.Stats(profiler).print_callers(args.cprofile_callers)


if __name__ == "__main__":
    raise SystemExit(main())
