"""Namespaced, immutable result contracts; independent of strategy execution."""
from copy import deepcopy

from research.stock_alerts import SCHEMA, session_dates
from research.stock_idea_engine import digest


VERSION = "stock_alert_results_v1"
PLAN_FIELDS = ("alert_id", "run_id", "security_id", "ticker", "model", "interval", "direction", "lane",
    "triggered_at", "published_at", "trigger_price", "stop", "target", "hold", "policy_version",
    "daily_setup_id", "daily_setup_at", "holding_sessions", "confirmation_interval")
OUTCOME_FIELDS = ("status", "reason", "entry_at", "entry_price", "exit_due_at", "exit_at", "exit_price", "paper_return")
OPEN_STATES = {"PENDING", "OPEN", "UNRESOLVED"}


def strategy_instance(policy, state, stream):
    if stream not in ("intraday", "swing"):
        raise ValueError("unknown forward result stream")
    identity = dict(policy_hash=digest(policy), enrolled_at=state["enrolled_at"], enrollment_sha256=digest(state["members"]))
    return dict(identity, instance_id=digest([VERSION, identity]), policy_version=policy["policy_version"], stream=stream,
        label="Swing" if stream == "swing" else "Intraday / daily v2")


def plan_record(row):
    return {key: row.get(key) for key in PLAN_FIELDS}


def validate_result_transition(previous, current):
    if plan_record(previous) != plan_record(current):
        raise ValueError("original alert plan changed")
    if previous["status"] in ("CLOSED", "NO_FILL") and any(previous.get(key) != current.get(key) for key in OUTCOME_FIELDS):
        raise ValueError("settled alert outcome changed")
    if any(previous.get(key) is not None and previous[key] != current.get(key) for key in ("entry_at", "entry_price", "exit_due_at")):
        raise ValueError("recorded entry or planned exit changed")


def namespace_snapshot(instance, snapshot):
    if snapshot["source"] != "SHADOW" or snapshot["source_id"] != instance["policy_version"]:
        raise ValueError("result source does not match strategy instance")
    result = deepcopy(snapshot)
    prefix = instance["instance_id"] + ":"
    for row in result.get("publications", []):
        row.update(original_run_id=row["run_id"], strategy_instance_id=instance["instance_id"], strategy_label=instance["label"])
        row["run_id"] = prefix + row["run_id"]
    for row in result.get("alerts", []):
        row.update(original_run_id=row["run_id"], original_alert_id=row["alert_id"], original_source_id=snapshot["source_id"],
            strategy_instance_id=instance["instance_id"], strategy_label=instance["label"])
        row["run_id"], row["alert_id"] = prefix + row["run_id"], prefix + row["alert_id"]
        row["trade_style"] = row.get("trade_style") or ("SWING" if row["interval"] == "1d" else "INTRADAY")
    for row in result.get("observations", []):
        row.update(strategy_instance_id=instance["instance_id"], run_id=prefix + row["run_id"])
    result["strategy_instance"] = dict(instance)
    return result


def combine_snapshots(snapshots, streams):
    dates = [session for snapshot in snapshots for session in snapshot.get("sessions", [])]
    return dict(schema=SCHEMA, source="SHADOW", source_id=VERSION, source_label="Combined forward shadow results",
        combined=True, status="PARTIAL" if any(stream.get("error") for stream in streams) else "READY",
        as_of=min((snapshot["as_of"] for snapshot in snapshots if snapshot.get("as_of")), default=None),
        sessions=session_dates(max(dates)) if dates else [], strategy_streams=streams,
        publications=[row for snapshot in snapshots for row in snapshot.get("publications", [])],
        alerts=[row for snapshot in snapshots for row in snapshot.get("alerts", [])],
        observations=[row for snapshot in snapshots for row in snapshot.get("observations", [])],
        hit_coverage="Strategy-specific retained candidate windows; not a confidence score",
        warnings=sorted({warning for snapshot in snapshots for warning in snapshot.get("warnings", [])}),
        outcome_policy="Original per-plan policies; returns are not a combined portfolio return",
        indicators_available=sorted({key for snapshot in snapshots for key in snapshot.get("indicators_available", [])}))