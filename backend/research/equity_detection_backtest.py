"""Offline historical detections and signed forward price comparisons, never fills."""
from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict
from datetime import timedelta
import hashlib
import json
from pathlib import Path

import pandas as pd

from research.equity_models.catalog import CATALOG, DEFAULT_MODELS, digest, research_policy
from research.equity_models.data import ResearchData, Snapshot, stamp
from research.equity_models.detectors import evaluate_research


def _endpoint(data, evaluation, horizon, unit, as_of):
    session = evaluation["session"]
    at = stamp(evaluation["decision_at"])
    signal = evaluation["signal"]
    if unit == "sessions":
        endpoint_session = data.session_offset(session, horizon)
        endpoint_at = data.session_bounds(endpoint_session)[1]
    else:
        anchor = stamp(signal["reference_at"]) if unit == "minutes" else at
        endpoint_at = anchor + timedelta(**{unit: horizon})
        _, closing = data.session_bounds(session)
        if endpoint_at > closing:
            return dict(status="NOT_APPLICABLE", reason="HORIZON_CROSSES_SESSION", endpoint_at=endpoint_at.isoformat())
    common = dict(endpoint_at=endpoint_at.isoformat(), value=None, reference=signal["reference"],
                  metric="SIGNED_GROSS_PRICE_RETURN", horizon=horizon, unit=unit)
    if endpoint_at > as_of:
        return dict(common, status="PENDING")
    if unit == "seconds":
        # First subsequent quote at/after the horizon, with a bounded one-second tolerance.
        rows = [q for q in data.quotes if q["security_id"] == evaluation["security_id"] and
                endpoint_at <= q["market_at"] <= endpoint_at + timedelta(seconds=1) and q["received_at"] <= as_of]
        if not rows:
            return dict(common, status="UNAVAILABLE", reason="MISSING_QUOTE_ENDPOINT")
        row = min(rows, key=lambda q: (q["market_at"], q["sequence"]))
        price = (row["bid"] + row["ask"]) / 2
    else:
        interval = "1d" if unit == "sessions" else "5m"
        # Minute diagnostics are anchored to the completed trigger bar, not its
        # variable delivery lag. This yields exact bar endpoints, never nearest closes.
        if unit == "minutes":
            if endpoint_at <= at:
                return dict(common, status="UNAVAILABLE", reason="ENDPOINT_PRECEDES_DECISION")
        rows = [b for b in data.bars if b["security_id"] == evaluation["security_id"] and
                b["interval"] == interval and b["market_at"] == endpoint_at and b["received_at"] <= as_of]
        if not rows:
            return dict(common, status="UNAVAILABLE", reason="MISSING_EXACT_CLOSE")
        row = max(rows, key=lambda b: (b["received_at"], b["revision"]))
        price = row["close"]
    if not signal.get("adjustment_id") or row.get("adjustment_id") != signal["adjustment_id"]:
        return dict(common, status="UNAVAILABLE", reason="ADJUSTMENT_COMPARABILITY_UNPROVEN")
    return dict(common, status="AVAILABLE", value=signal["direction"] * (price / signal["reference"] - 1),
                endpoint_price=price, observed_endpoint_at=row["market_at"].isoformat(),
                endpoint_source_id=row["source_id"], endpoint_revision=row["revision"])


def measure(data, evaluations, as_of):
    outcomes = []
    for evaluation in evaluations:
        if evaluation["status"] != "MATCH":
            continue
        horizons = [(n, "sessions") for n in (5, 10, 21)]
        style = evaluation["style"]
        if style == "INTRADAY":
            horizons += [(n, "minutes") for n in (15, 30, 60, 120)]
        if style == "SCALPING":
            horizons += [(n, "seconds") for n in (1, 5, 30)]
        if style == "POSITION":
            horizons += [(n, "sessions") for n in (63, 126, 252)]
        for horizon, unit in horizons:
            outcome = _endpoint(data, evaluation, horizon, unit, as_of)
            outcomes.append({"episode_id": evaluation["signal"]["episode_id"], "model_id": evaluation["model_id"],
                             "security_id": evaluation["security_id"], "horizon": horizon, "unit": unit, **outcome})
    return outcomes


def summarize(evaluations, outcomes, models, macro_policy="required"):
    report = {}
    for model in models:
        decisions = [r for r in evaluations if r["model_id"] == model]
        measures = [r for r in outcomes if r["model_id"] == model]
        groups = defaultdict(list)
        for row in measures:
            groups[f"{row['horizon']}_{row['unit']}"].append(row)
        report[model] = dict(style=CATALOG[model].style, policy_hash=research_policy(model, macro_policy).policy_hash,
                            evaluations=len(decisions), statuses=dict(Counter(r["status"] for r in decisions)),
                            reasons=dict(Counter(reason for r in decisions for reason in r["reasons"])), horizons={})
        for horizon, rows in groups.items():
            values = [r["value"] for r in rows if r["status"] == "AVAILABLE"]
            report[model]["horizons"][horizon] = dict(
                statuses=dict(Counter(r["status"] for r in rows)), n=len(values),
                mean_bps=sum(values) / len(values) * 10000 if values else None,
                positive_fraction=sum(v > 0 for v in values) / len(values) if values else None)
    return report


def run_detection_backtest(payload, *, models=DEFAULT_MODELS, start=None, end=None, as_of=None, securities=None,
                           macro_policy="required"):
    """Replay immutable frozen inputs by receipt clock, preserving unavailable decisions.

    Indicators always use the eligible prefix, including the historical revision
    visible at that decision. Later amended bars never alter earlier evaluations.
    The initial reference implementation favors auditability over batch throughput.
    """
    models = tuple(models)
    if not models or len(set(models)) != len(models) or any(m not in CATALOG for m in models):
        raise ValueError("models must be unique registered IDs")
    research_policy(models[0], macro_policy)
    data = ResearchData(payload)
    cutoff = stamp(as_of or payload["as_of"])
    if cutoff > stamp(payload["as_of"]):
        raise ValueError("as_of cannot exceed frozen dataset cutoff")
    start = stamp(start or payload["evaluation_start"])
    end = stamp(end or payload["evaluation_end"])
    if not start <= end <= cutoff:
        raise ValueError("evaluation range must end by as_of")
    security_ids = sorted(set(securities or payload.get("evaluation_securities", [])))
    if not security_ids:
        raise ValueError("explicit evaluation_securities required; benchmarks must not be implicitly traded")
    if securities and not set(securities) <= set(payload.get("evaluation_securities", [])):
        raise ValueError("requested security is outside declared evaluation universe")
    evaluations, states, episodes = [], {}, set()
    schedule = set()
    for model in models:
        policy = CATALOG[model]
        rows = data.quotes if policy.interval == "event" else data.bars
        for row in rows:
            if row["security_id"] not in security_ids or (policy.interval != "event" and row["interval"] != policy.interval):
                continue
            at = row["received_at"].ceil("s") if policy.interval == "event" else row["received_at"]
            if start <= at <= end:
                session = str(row["market_at"].tz_convert("America/New_York").date())
                schedule.add((at, row["security_id"], model, session))
    for at, security, model, session in sorted(schedule):
        policy = CATALOG[model]
        snapshot = Snapshot(data, security, at, session, policy.interval)
        key = (security, model)
        result = evaluate_research(snapshot, model, states.get(key), macro_policy=macro_policy)
        states[key] = result["state"]
        if result["status"] == "MATCH":
            episode = result["signal"]["episode_id"]
            if episode in episodes:
                result.update(status="NO_MATCH", reasons=["EPISODE_ALREADY_RECORDED"], signal=None)
            else:
                episodes.add(episode)
        evaluations.append(result)
    covered = {(r["security_id"], r["model_id"]) for r in evaluations}
    for security in security_ids:
        for model in models:
            if (security, model) not in covered:
                evaluations.append(dict(security_id=security, model_id=model, style=CATALOG[model].style,
                                        status="UNAVAILABLE", reasons=["NO_DECISION_INPUTS"], signal=None,
                                        decision_at=end.isoformat(), state={}, gates=[], input_ids=[]))
    outcomes = measure(data, evaluations, cutoff)
    source_files = [Path(__file__), *sorted((Path(__file__).parent / "equity_models").glob("*.py"))]
    manifest = dict(schema="equity_detection_backtest_v1", dataset_sha256=data.input_hash,
                    availability_mode=payload["availability_mode"], start=start.isoformat(), end=end.isoformat(),
                    as_of=cutoff.isoformat(), macro_policy=macro_policy,
                    models=[asdict(research_policy(m, macro_policy)) for m in models],
                    source_sha256={p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in source_files},
                    securities=security_ids, execution_permission=False, publication_permission=False,
                    interpretation="Gross price diagnostics; no fills, costs, positions, portfolio P&L or profitability qualification.")
    manifest["run_id"] = digest(manifest)
    return dict(manifest=manifest, evaluations=evaluations, outcomes=outcomes,
                summary=summarize(evaluations, outcomes, models, macro_policy))


def write_run(result, directory):
    directory = Path(directory)
    contents = {}
    for name in ("manifest", "summary"):
        contents[f"{name}.json"] = json.dumps(result[name], indent=2, sort_keys=True, allow_nan=False) + "\n"
    for name in ("evaluations", "outcomes"):
        contents[f"{name}.jsonl"] = "".join(json.dumps(row, sort_keys=True, allow_nan=False) + "\n" for row in result[name])
    lines = ["# Equity detection backtest", "", result["manifest"]["interpretation"], "",
             f"Availability mode: {result['manifest']['availability_mode']}", "",
             "| Model | Evaluations | Matches | Unavailable |", "|---|---:|---:|---:|"]
    for model, row in result["summary"].items():
        lines.append(f"| {model} | {row['evaluations']} | {row['statuses'].get('MATCH', 0)} | {row['statuses'].get('UNAVAILABLE', 0)} |")
    lines += ["", "See summary.json for horizon maturity, missing endpoints and mean signed gross price returns.",
              "Positive fractions are price-direction diagnostics, not trading win rates. Overlapping episodes are dependent observations.",
              "Thresholds are research hypotheses. This run does not qualify any model for live publication.", ""]
    contents["REPORT.md"] = "\n".join(lines)
    # Preflight every artifact before creating any file. Idempotent identical reruns are allowed.
    for name, text in contents.items():
        path = directory / name
        if path.exists() and path.read_text(encoding="utf-8") != text:
            raise ValueError(f"refusing to overwrite differing artifact: {path}")
    directory.mkdir(parents=True, exist_ok=True)
    for name, text in contents.items():
        path = directory / name
        if not path.exists():
            with path.open("x", encoding="utf-8", newline="\n") as stream:
                stream.write(text)
