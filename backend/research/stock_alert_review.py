"""Read-only end-of-day review of retained stock alert selection evidence."""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime
import math
from statistics import median

from research.stock_idea_engine import priority, read_candidate


SCHEMA = "stock_alert_eod_review_v1"
MODELS = ("resumption", "acceptance", "failure")
SELECTION_STATUSES = ("SELECTED", "NOT_SELECTED", "REPEAT")
RANKABLE_REASONS = {None, "MODEL_QUOTA", "MODEL_STOCK_DUPLICATE", "ACTIVE_POSITION_CAP"}
ALLOCATION_REASONS = {"MODEL_QUOTA", "MODEL_STOCK_DUPLICATE", "ACTIVE_POSITION_CAP", "NO_REMAINING_ENTRY_SLOT"}
RANKING_BASIS = {
    "resumption": "Side-adjusted RS63 percentile, then lower extension and higher liquidity",
    "acceptance": "Lower extension, then higher reward/risk and liquidity",
    "failure": "Higher reward/risk, then lower extension and higher liquidity",
}


def _timestamp(value):
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.timestamp() if parsed.tzinfo is not None else float("-inf")
    except (TypeError, ValueError, OverflowError):
        return float("-inf")


def _selection(disposition):
    if disposition["selection"] == "SELECTED":
        return "SELECTED"
    return "REPEAT" if disposition.get("reason") == "CONTINUING_EPISODE" else "NOT_SELECTED"


def _candidate_row(candidate):
    direction = candidate["direction"]
    risk = direction * (candidate["price"] - candidate["stop"])
    room = direction * (candidate["target"] - candidate["price"])
    return dict(ticker=candidate["ticker"], security_id=candidate["security_id"], direction=direction,
        model=candidate["model"], interval=candidate["interval"], trigger_at=candidate["trigger_at"],
        trigger_price=candidate["price"], stop=candidate["stop"], target=candidate["target"],
        risk_pct=risk / candidate["price"] if candidate["price"] > 0 and risk > 0 else None,
        reward_risk=room / risk if min(risk, room) > 0 else None,
        rs_percentile=candidate["rs_rank"], extension_atr=candidate["extension"],
        momentum=candidate["momentum"], liquidity=candidate["liquidity"],
        policy_version=candidate["policy_version"])


def _finite_rank_inputs(candidate):
    return all(isinstance(candidate.get(field), (int, float)) and not isinstance(candidate.get(field), bool)
               and math.isfinite(candidate[field]) for field in ("rs_rank", "extension", "room_risk", "liquidity", "momentum"))


def _outcome_summary(rows):
    selected = [row for row in rows if row["selection_status"] == "SELECTED"]
    statuses = Counter(row.get("outcome_status") or "UNAVAILABLE" for row in selected)
    entered = statuses["OPEN"] + statuses["CLOSED"]
    resolved_entry = entered + statuses["NO_FILL"]
    measured = [row["paper_return"] for row in selected if row.get("outcome_status") == "CLOSED"
                and isinstance(row.get("paper_return"), (int, float)) and math.isfinite(row["paper_return"])]
    return dict(selected=len(selected), entered=entered, closed=statuses["CLOSED"], open=statuses["OPEN"],
        no_fill=statuses["NO_FILL"], pending=statuses["PENDING"], unavailable=statuses["UNAVAILABLE"],
        fill_rate=entered / resolved_entry if resolved_entry else None, measured=len(measured),
        positive_rate=sum(value > 0 for value in measured) / len(measured) if measured else None,
        mean_return=sum(measured) / len(measured) if measured else None,
        median_return=median(measured) if measured else None)


def _mix(rows, field):
    selected = [row for row in rows if row["selection_status"] == "SELECTED"]
    counts = Counter(row[field] for row in selected)
    return [dict(value=value, share=count / len(selected) if selected else None,
                 **_outcome_summary([row for row in selected if row[field] == value]))
            for value, count in sorted(counts.items(), key=lambda item: (-item[1], str(item[0])))]


def build_stock_eod_review(publications, outcomes, *, as_of, session_date, sessions=(), storage_ready=True,
                           search="", model=None, selection_status=None, direction=None, trade_type=None,
                           offset=0, limit=100):
    if (as_of.utcoffset() is None or model not in (None, *MODELS)
            or selection_status not in (None, *SELECTION_STATUSES) or direction not in (None, -1, 1)
            or trade_type not in (None, "INTRADAY", "SWING") or offset < 0 or not 1 <= limit <= 200):
        raise ValueError("invalid stock EOD review filters")
    records = list(publications)
    completed = [record for record in records if record["payload"].get("coverage") == "PUBLISHED"]
    missed = len(records) - len(completed)
    occurrences = []
    for record in completed:
        publication = record["payload"]
        if publication.get("window_key") != record["record_id"]:
            raise ValueError("stock EOD publication identity mismatch")
        candidates = publication.get("candidates") or {}
        dispositions = publication.get("dispositions") or []
        disposition_ids = {item["episode_id"] for item in dispositions}
        if len(disposition_ids) != len(dispositions) or not disposition_ids <= set(candidates):
            raise ValueError("stock EOD candidate/disposition membership mismatch")
        selected = set(publication.get("selected") or [])
        if selected != {item["episode_id"] for item in dispositions if item["selection"] == "SELECTED"}:
            raise ValueError("stock EOD selected membership mismatch")
        disposition_by_id = {item["episode_id"]: item for item in dispositions}
        ranks, priority_ranks = {}, {}
        for detector in MODELS:
            pool = [(episode_id, candidate) for episode_id, candidate in candidates.items()
                    if candidate["model"] == detector and disposition_by_id[episode_id]["reason"] in RANKABLE_REASONS]
            ordered = sorted(pool, key=lambda item: priority(read_candidate(item[1]), "PRIORITY", 1729, publication["window_key"]))
            ranks.update({episode_id: (rank, len(ordered)) for rank, (episode_id, _) in enumerate(ordered, 1)})
            diagnostic_pool = [(episode_id, candidate) for episode_id, candidate in candidates.items()
                               if candidate["model"] == detector and _finite_rank_inputs(candidate)]
            diagnostic_order = sorted(diagnostic_pool,
                key=lambda item: priority(read_candidate(item[1]), "PRIORITY", 1729, publication["window_key"]))
            priority_ranks.update({episode_id: (rank, len(diagnostic_order))
                for rank, (episode_id, _) in enumerate(diagnostic_order, 1)})
        for episode_id, disposition in disposition_by_id.items():
            candidate = candidates[episode_id]
            if candidate["model"] not in MODELS:
                continue
            if any(candidate[field] != disposition[field] for field in ("security_id", "model", "interval", "direction")):
                raise ValueError("stock EOD candidate/disposition fields mismatch")
            rank, pool_size = ranks.get(episode_id, (None, None))
            priority_rank, priority_pool_size = priority_ranks.get(episode_id, (None, None))
            occurrences.append(dict(instance_id=record["instance_id"], stream=record["stream"],
                strategy_label=record["label"], episode_id=episode_id, run_id=record["record_id"],
                evaluated_at=publication["actual_publication_at"], candidate=candidate,
                selection_status=_selection(disposition), selection_reason=disposition.get("reason") or "SELECTED",
                model_rank=rank, pool_size=pool_size,
                priority_rank=priority_rank, priority_pool_size=priority_pool_size))
    grouped = defaultdict(list)
    for occurrence in occurrences:
        grouped[(occurrence["instance_id"], occurrence["episode_id"])].append(occurrence)
    rows = []
    for key, values in grouped.items():
        selected = [value for value in values if value["selection_status"] == "SELECTED"]
        repeats = [value for value in values if value["selection_status"] == "REPEAT"]
        ranked = [value for value in values if value["model_rank"] is not None]
        representative = selected[0] if selected else min(ranked, key=lambda value: value["model_rank"]) if ranked else max(values, key=lambda value: _timestamp(value["evaluated_at"]))
        status = "SELECTED" if selected else "REPEAT" if repeats else "NOT_SELECTED"
        reasons = Counter(value["selection_reason"] for value in values)
        outcome = outcomes.get(key) or {}
        row = _candidate_row(representative["candidate"])
        row.update(candidate_id=f"{key[0]}:{key[1]}", episode_id=key[1], strategy_instance_id=key[0],
            trade_type="SWING" if representative["stream"] == "swing" else "INTRADAY",
            strategy_label=representative["strategy_label"], selection_status=status,
            selection_reason="SELECTED" if status == "SELECTED" else "CONTINUING_EPISODE" if status == "REPEAT" else reasons.most_common(1)[0][0],
            reason_counts=dict(sorted(reasons.items())), occurrences=len(values), repeat_occurrences=len(repeats),
            first_seen=min(value["evaluated_at"] for value in values), last_seen=max(value["evaluated_at"] for value in values),
            model_rank=min((value["model_rank"] for value in ranked), default=None),
            pool_size=max((value["pool_size"] for value in ranked), default=None),
            priority_rank=min((value["priority_rank"] for value in values if value["priority_rank"] is not None), default=None),
            priority_pool_size=max((value["priority_pool_size"] for value in values if value["priority_pool_size"] is not None), default=None),
            ranking_basis=RANKING_BASIS[representative["candidate"]["model"]],
            outcome_status=outcome.get("status") if status == "SELECTED" else None,
            paper_return=outcome.get("paper_return") if status == "SELECTED"
                and outcome.get("status") in ("OPEN", "CLOSED") else None)
        rows.append(row)
    cells = []
    for detector in MODELS:
        model_occurrences = [row for row in occurrences if row["candidate"]["model"] == detector]
        model_rows = [row for row in rows if row["model"] == detector]
        outcomes_summary = _outcome_summary(model_rows)
        reasons = Counter(row["selection_reason"] for row in model_occurrences if row["selection_status"] != "SELECTED")
        cells.append(dict(model=detector, detected_occurrences=len(model_occurrences), unique_candidates=len(model_rows),
            not_selected=sum(row["selection_status"] == "NOT_SELECTED" for row in model_rows),
            repeats=sum(row["selection_status"] == "REPEAT" for row in model_occurrences), **outcomes_summary,
            top_reasons=[dict(reason=reason, count=count) for reason, count in reasons.most_common(3)],
            ranking_basis=RANKING_BASIS[detector]))
    rank_diagnostics = []
    buckets = (("RANK_1", lambda rank: rank == 1), ("RANK_2_3", lambda rank: 2 <= rank <= 3),
               ("RANK_4_PLUS", lambda rank: rank >= 4))
    for detector in MODELS:
        selected_rows = [row for row in rows if row["model"] == detector and row["selection_status"] == "SELECTED"
                         and row["model_rank"] is not None]
        for bucket, predicate in buckets:
            values = [row for row in selected_rows if predicate(row["model_rank"])]
            if values:
                rank_diagnostics.append(dict(model=detector, bucket=bucket, **_outcome_summary(values)))
    reason_rows = defaultdict(list)
    for row in rows:
        if row["selection_status"] == "NOT_SELECTED":
            reason_rows[row["selection_reason"]].append(row)
    bottlenecks = []
    for reason, values in sorted(reason_rows.items(), key=lambda item: (-len(item[1]), item[0])):
        ranks = [row["priority_rank"] for row in values if row["priority_rank"] is not None]
        category = "ALLOCATION" if reason in ALLOCATION_REASONS else "PLAN_GEOMETRY" if reason in {
            "INSUFFICIENT_TARGET_ROOM", "ENTRY_OUTSIDE_BRACKET", "ENTRY_CHASE_OR_BOUNDARY_FAILED", "INVALID_ENTRY"} else "TIMING_OR_DATA"
        bottlenecks.append(dict(reason=reason, category=category, candidates=len(values), ranked=len(ranks),
            top_three_priority=sum(rank <= 3 for rank in ranks), median_priority=median(ranks) if ranks else None))
    outcome_summary = _outcome_summary(rows)
    total_runs = len(completed) + missed
    diagnostics = dict(coverage=dict(completed=len(completed), missed=missed,
            completion_rate=len(completed) / total_runs if total_runs else None),
        conversion=outcome_summary, rank_buckets=rank_diagnostics, bottlenecks=bottlenecks,
        direction_mix=_mix(rows, "direction"), interval_mix=_mix(rows, "interval"), model_mix=_mix(rows, "model"),
        limitations=["Rejected candidates have no counterfactual paper outcomes",
                     "Observed outcomes are descriptive and not calibrated",
                     "Missed runs are excluded from candidate and outcome comparisons"])
    signals = []
    if diagnostics["coverage"]["completion_rate"] is not None and diagnostics["coverage"]["completion_rate"] < .8:
        signals.append(dict(code="COVERAGE_LIMITED", severity="CAUTION",
            completed=len(completed), missed=missed, total_runs=total_runs,
            completion_rate=diagnostics["coverage"]["completion_rate"]))
    top_priority_blocked = sum(item["top_three_priority"] for item in bottlenecks if item["category"] == "ALLOCATION")
    allocation_blocked = sum(item["candidates"] for item in bottlenecks if item["category"] == "ALLOCATION")
    if allocation_blocked:
        signals.append(dict(code="ALLOCATION_PRESSURE", severity="CAUTION", candidates=allocation_blocked,
            top_three_priority=top_priority_blocked))
    rank_by_model = defaultdict(dict)
    for item in rank_diagnostics:
        rank_by_model[item["model"]][item["bucket"]] = item
    for detector, buckets_by_name in rank_by_model.items():
        first, next_bucket = buckets_by_name.get("RANK_1"), buckets_by_name.get("RANK_2_3")
        if (first and next_bucket and first["measured"] and next_bucket["measured"]
                and first["mean_return"] < next_bucket["mean_return"]):
            signals.append(dict(code="RANK_ORDER_INVERSION", severity="REVIEW", model=detector,
                rank1_measured=first["measured"], rank1_mean_return=first["mean_return"],
                rank2_3_measured=next_bucket["measured"], rank2_3_mean_return=next_bucket["mean_return"]))
    for cell in cells:
        resolved = cell["entered"] + cell["no_fill"]
        if resolved >= 3 and cell["fill_rate"] is not None and cell["fill_rate"] < .7:
            signals.append(dict(code="LOW_FILL_CONVERSION", severity="REVIEW", model=cell["model"],
                fill_rate=cell["fill_rate"], entered=cell["entered"], no_fill=cell["no_fill"], resolved=resolved))
    for field, values in (("direction", diagnostics["direction_mix"]), ("interval", diagnostics["interval_mix"])):
        if values and values[0]["share"] is not None and values[0]["share"] >= .7:
            signals.append(dict(code="ALLOCATION_CONCENTRATION", severity="REVIEW", dimension=field,
                value=values[0]["value"], share=values[0]["share"], selected=values[0]["selected"]))
    needle = search.strip().casefold()
    filtered = [row for row in rows if (not needle or needle in row["ticker"].casefold())
        and (model is None or row["model"] == model)
        and (selection_status is None or row["selection_status"] == selection_status)
        and (direction is None or row["direction"] == direction)
        and (trade_type is None or row["trade_type"] == trade_type)]
    filtered.sort(key=lambda row: (row["model"], row["model_rank"] is None,
        row["model_rank"] if row["model_rank"] is not None else math.inf, row["ticker"], row["direction"]))
    return dict(schema=SCHEMA, storage_ready=storage_ready, as_of=as_of.isoformat(), session_date=session_date,
        sessions=list(sessions), completed_runs=len(completed), missed_runs=missed,
        detected_occurrences=len(occurrences), unique_candidates=len(rows), models=cells,
        diagnostics=diagnostics, review_signals=signals,
        total=len(filtered), offset=offset, limit=limit, rows=filtered[offset:offset + limit],
        outcome_status="DESCRIPTIVE_CURRENT_PAPER_OUTCOMES_NOT_CALIBRATED", execution_permission=False)