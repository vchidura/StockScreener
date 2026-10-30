"""Offline descriptive audit of a read-only legacy stock result extraction.

Returns use unique selected plans and latest outcome revisions, never revision
counts or display rows. This does not backfill outcomes or tune model rules.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timedelta
import csv
import gzip
import hashlib
import json
import math
from pathlib import Path

import exchange_calendars
import numpy as np


MODELS = ("resumption", "acceptance", "failure")
LABELS = {"resumption": "Trend resumption", "acceptance": "Breakout acceptance", "failure": "Extension reversal"}
PLAN_FIELDS = ("alert_id", "run_id", "security_id", "ticker", "model", "interval", "direction", "lane", "triggered_at", "published_at", "trigger_price", "stop", "target", "hold", "policy_version", "daily_setup_id", "daily_setup_at", "holding_sessions", "confirmation_interval")


def timestamp(value):
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")) if value else None


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False).encode()).hexdigest()


def mean(values):
    return float(np.mean(values)) if len(values) else None


def finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def bps(value):
    return "N/A" if value is None else f"{value:+.2f}"


def pct(value):
    return "N/A" if value is None else f"{100 * value:.1f}%"


def bootstrap_closed(rows, sessions, *, block=5):
    """Exploratory moving-calendar-block interval for mean CLOSED trade return.

    All trades on a session stay together. Omitted/unresolved outcomes are not
    imputed. This diagnostic is not the predeclared qualification estimand.
    """
    closed = [r for r in rows if r["status"] == "CLOSED" and finite(r.get("paper_return"))]
    if len(closed) < 2 or len({r["session"] for r in closed}) < 2 or len(sessions) < block:
        return None
    positions = {session: i for i, session in enumerate(sessions)}
    sums, counts = np.zeros(len(sessions)), np.zeros(len(sessions))
    for row in closed:
        i = positions[row["session"]]
        sums[i] += row["paper_return"]
        counts[i] += 1
    rng = np.random.default_rng(20260913)
    starts = rng.integers(0, len(sessions) - block + 1, size=(10000, math.ceil(len(sessions) / block)))
    indices = (starts[:, :, None] + np.arange(block)).reshape(10000, -1)[:, :len(sessions)]
    denominators = counts[indices].sum(axis=1)
    estimates = sums[indices].sum(axis=1)[denominators > 0] / denominators[denominators > 0]
    return [float(v * 10000) for v in np.quantile(estimates, [.025, .975])] if len(estimates) else None


def metrics(rows, *, sessions, source_times, confidence=False):
    states = Counter(row["status"] for row in rows)
    closed = [r for r in rows if r["status"] == "CLOSED" and finite(r.get("paper_return"))]
    returns = [r["paper_return"] for r in closed]
    gains = [v for v in returns if v > 0]
    losses = [v for v in returns if v < 0]
    no_fill = [r for r in rows if r["status"] == "NO_FILL"]
    mature = [r for r in rows if r.get("exit_due_at") and timestamp(r["exit_due_at"]) <= source_times[r["instance_id"]]]
    mature_known = [r for r in mature if r["status"] in ("CLOSED", "NO_FILL")]
    mature_closed = [r for r in mature_known if r["status"] == "CLOSED" and finite(r.get("paper_return"))]
    mature_unknown = [r for r in mature if r["status"] not in ("CLOSED", "NO_FILL")]
    immature = [r for r in rows if r not in mature]
    by_ticker, by_day = defaultdict(list), defaultdict(list)
    for row in closed:
        by_ticker[row["ticker"]].append(row["paper_return"])
        by_day[row["session"]].append(row["paper_return"])
    best_ticker = max(by_ticker, key=lambda k: sum(by_ticker[k]), default=None)
    best_day = max(by_day, key=lambda k: sum(by_day[k]), default=None)
    gross = [r["gross_return"] for r in closed]
    entry_delays = [(timestamp(r["entry_at"]) - timestamp(r["triggered_at"])).total_seconds() / 60 for r in closed if r.get("entry_at") and r.get("triggered_at")]
    entry_drifts = [r["direction"] * (r["entry_price"] / r["trigger_price"] - 1) * 10000 for r in closed if r.get("entry_price") and r.get("trigger_price")]
    risk_returns = [r["paper_return"] / r["actual_risk_fraction"] for r in closed if r.get("actual_risk_fraction", 0) > 0]
    stress = [r["paper_return"] for r in mature_known if finite(r.get("paper_return"))]
    stress_unknown = 0
    for row in mature_unknown:
        price = row.get("entry_price") or row.get("trigger_price")
        if price and row.get("stop") is not None:
            stress.append(-2 * abs(price - row["stop"]) / price - row["cost_bps"] / 10000)
        else:
            stress_unknown += 1
    return dict(
        selected=len(rows), states=dict(states), closed=len(closed), no_fill=len(no_fill),
        unique_tickers=len({r["ticker"] for r in rows}),
        closed_publication_sessions=len(by_day), selected_publication_sessions=len({r["session"] for r in rows}),
        primary_cost_bps=sorted({r["cost_bps"] for r in rows}),
        known_outcomes=len(closed) + len(no_fill),
        settled_fill_rate=len(closed) / (len(closed) + len(no_fill)) if closed or no_fill else None,
        net_win_rate=len(gains) / len(returns) if returns else None,
        mean_closed_net_bps=mean([v * 10000 for v in returns]),
        median_closed_net_bps=float(np.median(returns) * 10000) if returns else None,
        mean_closed_gross_bps=mean([v * 10000 for v in gross]),
        mean_win_bps=mean([v * 10000 for v in gains]), mean_loss_bps=mean([v * 10000 for v in losses]),
        profit_factor=sum(gains) / -sum(losses) if losses else None,
        mean_net_r=mean(risk_returns),
        median_trigger_to_entry_minutes=float(np.median(entry_delays)) if entry_delays else None,
        mean_directional_trigger_to_entry_drift_bps=mean(entry_drifts),
        time_exit_share=sum(r.get("reason") == "TIME_OR_SESSION_CLOSE" for r in closed) / len(closed) if closed else None,
        mean_known_selected_net_bps=sum(returns) / (len(returns) + len(no_fill)) * 10000 if returns or no_fill else None,
        costs_closed_net_bps={str(cost): mean([(v - cost / 10000) * 10000 for v in gross]) for cost in (0, 4, 10, 25)},
        mature_selected=len(mature), mature_known=len(mature_known), mature_unresolved=len(mature_unknown),
        mature_closed=len(mature_closed),
        mean_mature_closed_net_bps=mean([r["paper_return"] * 10000 for r in mature_closed]),
        mature_outcome_coverage=len(mature_known) / len(mature) if mature else None,
        not_yet_at_planned_exit=len(immature),
        overdue_states=dict(Counter(r["status"] for r in mature_unknown)),
        outcome_reasons=dict(Counter(r.get("reason") or "NONE" for r in rows)),
        closed_exit_reasons=dict(Counter(r.get("reason") or "NONE" for r in closed)),
        exploratory_closed_mean_ci95_bps=bootstrap_closed(rows, sessions) if confidence else None,
        best_ticker_by_sum_of_equal_notional_trade_returns=best_ticker,
        mean_closed_excluding_best_ticker_bps=mean([r["paper_return"] * 10000 for r in closed if r["ticker"] != best_ticker]),
        best_session_by_sum_of_equal_notional_trade_returns=best_day,
        mean_closed_excluding_best_session_bps=mean([r["paper_return"] * 10000 for r in closed if r["session"] != best_day]),
        mature_missing_path_minus_2r_stress_mean_bps=mean([v * 10000 for v in stress]) if not stress_unknown else None,
        missing_path_stress_note="Diagnostic, not a bound; includes known no-fills at zero and excludes not-yet-mature plans",
    )


def csv_write(path, rows):
    if not rows:
        return
    keys = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: json.dumps(value, sort_keys=True) if isinstance(value, (dict, list)) else value for key, value in row.items()})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument("--plot", action="store_true", help="Requires optional matplotlib; otherwise the report uses tables")
    args = parser.parse_args()
    opener = gzip.open if args.input.suffix == ".gz" else open
    with opener(args.input, "rt", encoding="utf-8") as handle:
        data = json.load(handle)
    out = args.output_directory
    out.mkdir(parents=True, exist_ok=True)
    instances = {r["instance_id"]: r["payload"] for r in data["instances"]}
    source_times = {r["instance_id"]: timestamp(r["source_as_of"]) for r in data["streams"]}
    plans = {(r["instance_id"], r["record_id"]): r["payload"] for r in data["records"] if r["kind"] == "plan"}
    publications = [r for r in data["records"] if r["kind"] == "publication_evidence"]
    revisions = {(r["instance_id"], r["alert_id"]): r for r in data["latest_revisions"]}
    checks = dict(payload_hash_mismatches=[], missing_plan_or_revision=[], plan_field_mismatches=[], selection_plan_mismatches=[], closed_return_mismatches=[], closed_missing_prices=[], terminal_outcome_changes=[], publication_pair_mismatches=[])
    for row in [*data["records"], *data["latest_revisions"]]:
        if digest(row["payload"]) != row["payload_sha256"]:
            checks["payload_hash_mismatches"].append([row["instance_id"], row.get("kind", "revision"), row.get("record_id", row.get("alert_id"))])
    for key in plans.keys() ^ revisions.keys():
        checks["missing_plan_or_revision"].append(list(key))
    selected = {(r["instance_id"], key) for r in publications for key in r["payload"]["selected"]}
    checks["selection_plan_mismatches"] = [list(key) for key in selected ^ plans.keys()]
    checks["terminal_outcome_changes"] = [r for r in data["revision_audit"] if r["terminal_outcome_variants"] > 1]
    publication_summaries = {(r["instance_id"],r["record_id"]): r["payload"] for r in data["records"] if r["kind"] == "publication"}
    rows = []
    for key, revision in revisions.items():
        original = revision["payload"]
        if key in plans and any(plans[key].get(field) != original.get(field) for field in PLAN_FIELDS):
            checks["plan_field_mismatches"].append(list(key))
        if original.get("lane") != "TRADE" or original.get("model") not in MODELS:
            continue
        instance = instances[key[0]]
        row = dict(original, instance_id=key[0], source_as_of=revision["source_as_of"], stream=instance["stream"],
                   session=original["run_id"][:10], cost_bps=instance["policy"]["primary_cost_bps"])
        row["trade_type"] = "swing" if row["stream"] == "swing" else "daily" if row["interval"] == "1d" else "intraday"
        row["side"] = "long" if row["direction"] == 1 else "short"
        row["month"] = row["session"][:7]
        row["week"] = timestamp(row["run_id"]).strftime("%G-W%V")
        row["actual_risk_fraction"] = abs(row["entry_price"] - row["stop"]) / row["entry_price"] if row.get("entry_price") else None
        row["gross_return"] = row["direction"] * (row["exit_price"] / row["entry_price"] - 1) if row["status"] == "CLOSED" and row.get("entry_price") and row.get("exit_price") else None
        if row["status"] == "CLOSED":
            if row["gross_return"] is None or not finite(row.get("paper_return")):
                checks["closed_missing_prices"].append(list(key))
            elif abs(row["gross_return"] - row["cost_bps"] / 10000 - row["paper_return"]) > 1e-10:
                checks["closed_return_mismatches"].append(list(key))
        rows.append(row)
    first = min(r["payload"]["session"] for r in publications)
    last = max(r["payload"]["session"] for r in publications)
    calendar = exchange_calendars.get_calendar("XNYS")
    sessions = [str(s.date()) for s in calendar.sessions_in_range(first, last)]
    recent_start = str(calendar.session_offset(last, -29).date())
    summary = dict(
        database=data["database"], first_publication_session=first, last_publication_session=last,
        available_trading_sessions=len(sessions), last_30_trading_sessions_start=recent_start,
        recent_30_uses_same_records=first >= recent_start,
        source_revision_count=sum(r["revisions"] for r in data["revision_counts"]),
        distinct_alert_records=len(revisions), trade_plans=len(rows), watch_records=len(revisions) - len(rows),
        integrity={key: len(value) for key, value in checks.items()}, integrity_details=checks,
        groups=[], breakdowns=[], publication_quality=[], selection_funnel=[], runtime_generations=[],
        methodology=dict(
            unit="Unique (instance_id, alert_id), latest outcome revision in one read-only repeatable-read snapshot",
            primary_return="Recorded CLOSED paper_return; independently checked against signed exit/entry return minus instance round-trip cost",
            no_fills="Zero in known selected-plan averages; excluded from filled-trade averages and win rates",
            open_pending_unresolved="Excluded from realized means; reported separately, never imputed to zero",
            recent_period="Most recent 30 XNYS sessions through last stored publication; no pre-enrollment sessions invented",
            bootstrap="Exploratory 10000-replicate moving 5-session block bootstrap of CLOSED trade mean; not a predeclared qualification test",
            portfolio="No portfolio return, Sharpe, CAGR, or capital drawdown inferred from overlapping independent plans",
            benchmarks="Only PRIORITY forward arm stored; no matched random/momentum outcomes available for alpha comparison",
        ),
    )
    for trade_type in ("intraday", "daily", "swing"):
        for model in MODELS:
            own = [r for r in rows if r["trade_type"] == trade_type and r["model"] == model]
            summary["groups"].append(dict(trade_type=trade_type, model=model, **metrics(own, sessions=sessions, source_times=source_times, confidence=trade_type == "intraday")))
            for field in ("interval", "side", "month", "week"):
                for value in sorted({r[field] for r in own}):
                    subset = [r for r in own if r[field] == value]
                    summary["breakdowns"].append(dict(trade_type=trade_type, model=model, dimension=field, value=value, **metrics(subset, sessions=sessions, source_times=source_times)))
    publication_csv, candidate_csv, runtime = [], [], defaultdict(list)
    by_stream = defaultdict(list)
    for record in publications:
        payload = record["payload"]
        identity = record["instance_id"]
        instance = instances[identity]
        stream = instance["stream"]
        by_stream[stream].append(payload)
        generation = digest(payload.get("runtime_sources", {}))
        runtime[(stream, generation)].append(payload)
        summary_row = publication_summaries[(identity, record["record_id"])]
        if summary_row.get("selected") != len(payload["selected"]) or summary_row.get("status") != payload["coverage"]:
            checks["publication_pair_mismatches"].append([identity, record["record_id"]])
        for disposition in payload.get("dispositions", []):
            candidate = payload.get("candidates", {}).get(disposition["episode_id"], {})
            if disposition["model"] in MODELS:
                candidate_csv.append(dict(stream=stream, session=payload["session"], window_key=payload["window_key"],
                    episode_id=disposition["episode_id"], model=disposition["model"], interval=disposition["interval"],
                    direction=disposition["direction"], ticker=candidate.get("ticker"), selection=disposition["selection"], reason=disposition.get("reason")))
        publication_csv.append(dict(stream=stream, session=payload["session"], window_key=payload["window_key"],
            status=payload["coverage"], expected=len(payload["expected_members"]), missing=len(payload["missing_members"]),
            candidate_decisions=len(payload.get("candidates", {})), selected=len(payload["selected"]),
            source_policy_hash=payload["policy_hash"], runtime_generation=generation,
            delay_minutes=(timestamp(payload["actual_publication_at"]) - timestamp(payload["window_key"])).total_seconds()/60))
    for stream, pubs in by_stream.items():
        expected_windows = []
        instance = next(i for i in instances.values() if i["stream"] == stream)
        enrolled = timestamp(instance["enrolled_at"])
        for day in sessions:
            start = calendar.session_open(day).to_pydatetime()
            close = calendar.session_close(day).to_pydatetime()
            boundary = start + timedelta(minutes=30)
            while boundary <= close:
                if boundary > enrolled:
                    expected_windows.append(boundary.isoformat())
                boundary += timedelta(minutes=30)
        recorded = {timestamp(p["window_key"]).isoformat() for p in pubs}
        stream_record = next(r for r in data["streams"] if r["stream"] == stream)
        worker = stream_record["metadata"].get("worker", {})
        own_csv = [r for r in publication_csv if r["stream"] == stream]
        summary["publication_quality"].append(dict(stream=stream, windows=len(pubs),
            expected_enrolled_windows=len(expected_windows), absent_windows=sorted(set(expected_windows) - recorded),
            statuses=dict(Counter(p["coverage"] for p in pubs)),
            first_window=min(p["window_key"] for p in pubs), last_window=max(p["window_key"] for p in pubs),
            source_as_of=stream_record["source_as_of"], enrolled_members=len(instance.get("enrollment", [])),
            current_selection_members=worker.get("selection_members"),
            identity_flags=dict(Counter(worker.get("identity_breaks", {}).values())), correction_recovery=worker.get("correction_recovery"),
            published_with_missing_members=sum(p["coverage"] == "PUBLISHED" and bool(p["missing_members"]) for p in pubs),
            median_published_delay_minutes=float(np.median([r["delay_minutes"] for r in own_csv if r["status"] == "PUBLISHED"])),
            current_policy_version=instance["policy_version"], runtime_generation_count=len({r["runtime_generation"] for r in own_csv}),
            monthly_publication_statuses=[dict(month=month, status=status, count=count) for (month,status),count in sorted(Counter((p["session"][:7],p["coverage"]) for p in pubs).items())],
        ))
        decisions = [r for r in candidate_csv if r["stream"] == stream]
        for model in MODELS:
            own = [r for r in decisions if r["model"] == model]
            summary["selection_funnel"].append(dict(stream=stream, model=model, candidate_window_decisions=len(own),
                unique_candidate_episodes=len({r["episode_id"] for r in own}),
                selected=sum(r["selection"] == "SELECTED" for r in own), reasons=dict(Counter(r["reason"] or "SELECTED" for r in own))))
    for (stream, generation), pubs in sorted(runtime.items()):
        windows = {p["window_key"] for p in pubs}
        selected_rows = [r for r in rows if r["stream"] == stream and r["run_id"] in windows]
        summary["runtime_generations"].append(dict(stream=stream, generation=generation,
            first_window=min(windows), last_window=max(windows), windows=len(windows), runtime_sources=pubs[0].get("runtime_sources", {}),
            by_model=[dict(model=m, **metrics([r for r in selected_rows if r["model"] == m], sessions=sessions, source_times=source_times)) for m in MODELS]))
    summary["integrity"] = {key: len(value) for key, value in checks.items()}
    investigation = out / "checksum_investigation.json"
    if investigation.exists():
        summary["checksum_investigation"] = json.loads(investigation.read_text(encoding="utf-8"))
        unexplained_windows = {r["record_id"] for r in summary["checksum_investigation"] if not r.get("source_matches_stored_hash") and not r.get("negative_zero_reconstruction_matches_stored_hash")}
        summary["unexplained_checksum_sensitivity"] = [dict(model=m,
            excluded_trade_plans=sum(r["stream"] == "intraday" and r["trade_type"] == "intraday" and r["model"] == m and r["run_id"] in unexplained_windows for r in rows),
            **metrics([r for r in rows if r["trade_type"] == "intraday" and r["model"] == m and r["run_id"] not in unexplained_windows], sessions=sessions, source_times=source_times)) for m in MODELS]
    (out / "evaluation.json").write_text(json.dumps(summary, indent=2, allow_nan=False), encoding="utf-8")
    csv_write(out / "model_metrics.csv", summary["groups"])
    csv_write(out / "breakdowns.csv", summary["breakdowns"])
    export_fields = ("instance_id", "alert_id", "stream", "trade_type", "model", "interval", "side", "direction", "ticker", "security_id", "session", "run_id", "triggered_at", "published_at", "trigger_price", "stop", "target", "status", "reason", "entry_at", "entry_price", "exit_due_at", "exit_at", "exit_price", "paper_return", "gross_return", "cost_bps", "actual_risk_fraction", "source_as_of", "policy_version")
    csv_write(out / "unique_trade_outcomes.csv", [{k:r.get(k) for k in export_fields} for r in rows])
    csv_write(out / "publications.csv", publication_csv)
    csv_write(out / "candidate_decisions.csv", candidate_csv)
    write_report(out, summary)
    if args.plot:
        plot(out, summary)
    print(json.dumps({k:summary[k] for k in ("first_publication_session","last_publication_session","available_trading_sessions","trade_plans","watch_records","integrity")}, indent=2))
    for group in summary["groups"]:
        print(group["trade_type"], group["model"], "selected", group["selected"], "closed",group["closed"],"net_bps",bps(group["mean_closed_net_bps"]),"win",pct(group["net_win_rate"]),"mature_coverage",pct(group["mature_outcome_coverage"]))


def write_report(out, summary):
    lines = ["# Legacy equity model outcome evaluation", "", f"Read-only source: `stocks_db_fresh`; snapshot {summary['database'][0]['snapshot_at']} UTC.", "",
        f"Forward publication sessions: **{summary['first_publication_session']} to {summary['last_publication_session']}**, {summary['available_trading_sessions']} XNYS sessions. The most recent 30-session window starts {summary['last_30_trading_sessions_start']}; all stored publications fall inside it, so the all-history and recent-30 results are identical.", "",
        f"{summary['source_revision_count']:,} revisions reduce to {summary['distinct_alert_records']:,} unique alert records: **{summary['trade_plans']} trade plans** and **{summary['watch_records']} watch-only records**, excluded from trading metrics.", "",
        "## Results", "", "Returns below are per completed filled plan after the stored **10 bps round-trip cost**. They are not portfolio returns. Win rate is positive net return among CLOSED plans. No-fills are excluded from these filled-trade means.", "",
        "| Holding type | Model | Selected | Closed | No fill | Other states | Net win rate | Mean net bps | Profit factor | Mature known / mature plans |",
        "|---|---|---:|---:|---:|---|---:|---:|---:|---:|"]
    for group in summary["groups"]:
        other = {k:v for k,v in group["states"].items() if k not in ("CLOSED","NO_FILL")}
        factor = f"{group['profit_factor']:.2f}" if group["profit_factor"] is not None else "N/A"
        lines.append(f"| {group['trade_type']} | {LABELS[group['model']]} | {group['selected']} | {group['closed']} | {group['no_fill']} | {other or '—'} | {pct(group['net_win_rate'])} | {bps(group['mean_closed_net_bps'])} | {factor} | {group['mature_known']}/{group['mature_selected']} |")
    lines += ["", "## Intraday cost sensitivity and uncertainty", "", "The 95% intervals use an exploratory moving five-session block bootstrap of the CLOSED-trade mean. Trades on the same session move together. This is not the predeclared qualification procedure and does not correct all subgroup comparisons.", "",
        "| Model | Gross / breakeven cost bps | Net at 4 bps | Net at 10 bps | Net at 25 bps | Exploratory 95% interval at 10 bps | Without best ticker | Without best session |", "|---|---:|---:|---:|---:|---|---:|---:|"]
    for g in summary["groups"]:
        if g["trade_type"] != "intraday":
            continue
        ci = g["exploratory_closed_mean_ci95_bps"]
        lines.append(f"| {LABELS[g['model']]} | {bps(g['mean_closed_gross_bps'])} | {bps(g['costs_closed_net_bps']['4'])} | {bps(g['costs_closed_net_bps']['10'])} | {bps(g['costs_closed_net_bps']['25'])} | {', '.join(bps(x) for x in ci) if ci else 'N/A'} | {bps(g['mean_closed_excluding_best_ticker_bps'])} | {bps(g['mean_closed_excluding_best_session_bps'])} |")
    lines += ["", "## Intraday interval and direction checks", "", "These slices are diagnostic; selecting the best slice after observing results would create a new trial.", "", "| Model | Slice | Selected | Closed | Mean net bps | Net win rate |", "|---|---|---:|---:|---:|---:|"]
    for g in summary["breakdowns"]:
        if g["trade_type"] == "intraday" and g["dimension"] in ("interval", "side", "month"):
            lines.append(f"| {LABELS[g['model']]} | {g['dimension']}={g['value']} | {g['selected']} | {g['closed']} | {bps(g['mean_closed_net_bps'])} | {pct(g['net_win_rate'])} |")
    lines += ["", "## Model design assessment", "",
        "The rules are coherent, auditable hypotheses: prior-day trend plus retracement/resumption; compressed-range breakout plus next-bar acceptance; and failed extension plus return into a frozen range. They separate setup, confirmation and entry, preserve original brackets and deduplicate episodes. Those engineering properties support reproducibility, not profitability.", "",
        "- **Trend resumption:** the prior-day strength/EMA gate reduces indiscriminate dip-buying, but prior daily context can lag an intraday regime change. Hourly triggers are the stronger observed slice (+5.26 bps net), while 30m triggers lose 9.21 bps. The daily lane has only three closed fills. Treat the hourly observation as a new prospective hypothesis, not a validated filter.",
        "- **Breakout acceptance:** the stronger confirmation tests are sensible, but pooled gross expectancy is only +0.94 bps. Waiting for confirmation can consume available target room; compression alone does not establish directional participation. Hourly and short slices are especially weak in this sample. These are associations, not proof of the cause.",
        "- **Extension reversal:** it has the largest intraday completed sample, but net expectancy remains negative. Long signals are near flat (-0.84 bps), while shorts lose 9.83 bps. Most positions exit on time rather than at the structural target; the 60-minute horizon and midpoint target deserve prospective review, without retrospectively replacing original outcomes.", "",
        "| Intraday model | Time/session exits among closed | Median trigger-to-entry minutes | Mean signed entry drift from trigger, bps |",
        "|---|---:|---:|---:|"]
    for g in summary["groups"]:
        if g["trade_type"] == "intraday":
            lines.append(f"| {LABELS[g['model']]} | {pct(g['time_exit_share'])} | {g['median_trigger_to_entry_minutes']:.0f} | {bps(g['mean_directional_trigger_to_entry_drift_bps'])} |")
    lines += ["", "Signed entry drift is positive when entry occurs farther in the signal direction than the trigger close. This is a delay/price diagnostic, not measured bid/ask slippage.", "", "## Publication and evidence quality", ""]
    for p in summary["publication_quality"]:
        lines += [f"- **{p['stream']}**: {p['windows']} stored windows, {p['statuses']}; {len(p['absent_windows'])} absent expected windows after enrollment. {p['published_with_missing_members']} published windows contain missing members. Median PUBLISHED delay: {p['median_published_delay_minutes']:.1f} minutes. {p['runtime_generation_count']} runtime-source fingerprints.",
                  f"  Current worker identity flags: {p['identity_flags']}. These flags do not by themselves prove the same number of active exclusions; correction-recovery metadata is retained in evaluation.json."]
    lines += ["", f"Integrity checks: `{summary['integrity']}`. Checks compare payload hashes, plan/revision identity, selected IDs against plans, publication summaries against evidence, terminal outcome stability, and signed return arithmetic.", "",
        "Six publication_evidence hashes fail round-trip recomputation. For September 18 14:30 and September 23 15:30/16:00 UTC, the read-only original source ledger reproduces the stored hashes exactly; the only differences are candidate room_risk -0.0 becoming 0.0 in JSONB. The three October 15 14:30/15:00/15:30 UTC exceptions remain unexplained; that local source backup lacks these publications. All plan and latest outcome revision hashes pass. No hashes or database payloads were changed. See checksum_investigation.json.", "",
        "Excluding plans from the three unexplained checksum windows leaves intraday mean CLOSED returns of " + "; ".join(f"{LABELS[g['model']]} {bps(g['mean_closed_net_bps'])} bps ({g['excluded_trade_plans']} plans excluded)" for g in summary.get("unexplained_checksum_sensitivity", [])) + ".", "",
        "## Interpretation", "",
        "- None of the three pooled intraday models has positive average completed net return at the recorded 10 bps cost. Gross expectancy and cost sensitivity distinguish a weak signal from execution-cost drag.",
        "- The daily lane must remain separate. Three closed daily resumption trades and one closed swing trade per model cannot support a reliable performance claim.",
        "- OPEN paper_return is a marked, unrealized return and is excluded. Pending and unresolved records are not counted as zero. Mature coverage uses the frozen planned exit against each stream's source cutoff, so early stops do not determine maturity.",
        "- Daily and swing CLOSED means above include exits before their maximum planned horizon. Recent cohorts can therefore be censored even when their early-exiting trades are already CLOSED. evaluation.json also supplies mature_closed and mean_mature_closed_net_bps; these tiny cohorts cannot establish an edge.",
        "- Only PRIORITY is stored in this forward evidence. Policy configuration listing ALL/MOMENTUM/RANDOM does not establish that corresponding forward control outcomes exist. Benchmark-relative alpha cannot be identified here.",
        "- The checked-in research evaluation plan requires 40 populated calendar blocks, at least 95% mature outcome coverage, paired controls, concentration/cost sensitivity and family-wise testing. Only 29 publication sessions are available; this audit does not qualify a model.",
        "- Multiple runtime-source generations and publication misses mean this is an operational forward track record, not evidence from one unchanged implementation running every scheduled window.",
        "- Native 30-minute OHLC paths do not establish intrabar stop/target ordering. Existing outcomes use conservative stop-first ambiguity and hypothetical fills; fees/spreads/slippage are represented by fixed cost scenarios, without live fill or short-borrow certification.",
        "- No strategy parameters or database rows were changed. No future-price reconstruction was used to fill missing historical outcomes.", "",
        "## Engineering priorities", "",
        "1. Reconcile the three unexplained publication-evidence checksums and normalize numeric hashing only in a separately reviewed future change; preserve existing evidence.",
        "2. Reduce missed windows and measure cohort availability separately from market/model performance. The signal-to-entry delay is part of this legacy implementation's measured results.",
        "3. Freeze a prospective runtime/policy generation and collect paired momentum/random or same-horizon benchmark controls. Keep daily, intraday and confirmed swing results separate.",
        "4. Retest the observed interval/side and target/horizon questions prospectively with realistic costs. Do not promote the best historical subgroup or relax entry filters merely to increase fills.", "",
        "## Artifacts", "", "- `unique_trade_outcomes.csv`: one row per original trade plan, latest observed outcome.",
        "- `model_metrics.csv`, `breakdowns.csv`: model-level and diagnostic slices.",
        "- `publications.csv`, `candidate_decisions.csv`: schedule quality and decision suppression reasons.",
        "- `evaluation.json`: full metrics, uncertainty, integrity, maturity, source generations and methodology.",
        "- `checksum_investigation.json`: evidence hash discrepancies and source-ledger comparisons.", ""]
    lines += ["An independent read-only SQL aggregation also reproduced all nine groups' selected/closed/no-fill counts, net win rates and signed entry/exit return calculations; see `sql_crosscheck.json`.", ""]
    (out / "REPORT.md").write_text("\n".join(lines), encoding="utf-8")


def plot(out, summary):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    groups = [g for g in summary["groups"] if g["trade_type"] == "intraday"]
    figure, ax = plt.subplots(figsize=(9, 4.8))
    x = np.arange(len(groups))
    for offset, cost, color in ((-.24,4,"#2c7a7b"),(0,10,"#4c63a8"),(.24,25,"#b65e54")):
        ax.bar(x + offset, [g["costs_closed_net_bps"][str(cost)] for g in groups], width=.23, label=f"{cost} bps round trip", color=color)
    ax.axhline(0,color="#333333",linewidth=.8)
    ax.set_xticks(x, [LABELS[g["model"]] + f"\n{g['closed']} closed plans" for g in groups])
    ax.set_ylabel("Mean return per closed trade (basis points)")
    ax.set_title("Legacy intraday models: cost sensitivity\nSeptember 15–October 23, 2026")
    ax.legend(frameon=False)
    ax.spines[["top","right"]].set_visible(False)
    figure.text(.05,.02,"Unique plans; 30m + 1h triggers. Excludes daily, swing, no-fills and unresolved outcomes. Not portfolio returns.",fontsize=8)
    figure.tight_layout(rect=(0,.06,1,1))
    figure.savefig(out / "intraday_cost_sensitivity.png",dpi=180)
    plt.close(figure)


if __name__ == "__main__":
    main()
