"""Read-only presentation of retained publications, independent of detection."""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from functools import lru_cache
import json
import math
from pathlib import Path

import exchange_calendars


SCHEMA = "stock_alert_view_v1"
SOURCES = ("SHADOW", "REPLAY", "LEGACY")
STATIC_TRADE_WARNINGS = frozenset({
    "Paper only", "Forward paper only", "Stop gaps can exceed planned risk",
    "Action coverage not certified", "Short borrow unverified", "Countertrend context",
})


def session_dates(latest):
    calendar = exchange_calendars.get_calendar("XNYS")
    last = calendar.date_to_session(latest, direction="previous")
    first = calendar.session_offset(last, -20)
    return [str(session.date()) for session in calendar.sessions_in_range(first, last)]


def timestamp_value(value):
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.timestamp() if parsed.tzinfo is not None else None
    except (AttributeError, TypeError, ValueError, OverflowError):
        return None


def publication_order(publication):
    timestamp = timestamp_value(publication.get("published_at"))
    return (timestamp if timestamp is not None else float("-inf"), publication["run_id"])


def history_publications(snapshot, session):
    publications = snapshot.get("publications", [])
    latest_by_strategy = {}
    if snapshot["source"] != "REPLAY":
        for publication in sorted(publications, key=publication_order):
            latest_by_strategy[publication.get("strategy_instance_id")] = publication["run_id"]
    withheld = set(latest_by_strategy.values())
    return sorted((item for item in publications if item["session"] == session
        and item["run_id"] not in withheld), key=publication_order)


def directional_risk_warnings(row):
    if row.get("lane") != "TRADE" or row.get("direction") not in (-1, 1):
        return []
    indicators = row.get("indicators") if isinstance(row.get("indicators"), dict) else {}
    values = {key: indicators.get(key) if isinstance(indicators.get(key), (int, float))
              and not isinstance(indicators.get(key), bool) and math.isfinite(indicators[key]) else None
              for key in ("ema20_distance", "ema50_distance", "ema50_slope", "momentum", "rs_percentile",
                          "rsi", "relative_volume")}
    direction, evidence = row["direction"], []
    opposing_side = "bullish" if direction == -1 else "bearish"
    side = "SHORT" if direction == -1 else "LONG"
    comparisons = (
        ("ema20_distance", "price {:+.2%} vs EMA20"),
        ("ema50_distance", "price {:+.2%} vs EMA50"),
        ("ema50_slope", "EMA50 {:+.2%} over 10 bars"),
        ("momentum", "12-minus-1 momentum {:+.2%}"),
    )
    for key, detail in comparisons:
        value = values[key]
        if value is not None and direction * value < 0:
            evidence.append(detail.format(value))
    relative_strength = values["rs_percentile"]
    if relative_strength is not None and ((direction == -1 and relative_strength >= .7)
            or (direction == 1 and relative_strength <= .3)):
        evidence.append(f"RS63 percentile {relative_strength * 100:.1f}")
    details = []
    if evidence:
        details.append(f"countertrend {side} against {opposing_side} trigger evidence (" + "; ".join(evidence) + ")")
    model = row.get("model")
    missing = [label for key, label in (("ema20_distance", "EMA20 distance"),
               ("ema50_distance", "EMA50 distance"), ("ema50_slope", "EMA50 slope"),
               ("rsi", "RSI14"), ("relative_volume", "relative volume")) if values[key] is None]
    if missing and model in ("resumption", "acceptance", "failure"):
        details.append("trigger metrics unavailable: " + ", ".join(missing))
    warnings = ["Directional context: " + "; ".join(details)] if details else []
    trigger_risk = []
    if values["rsi"] is not None and ((direction == 1 and values["rsi"] >= 70)
            or (direction == -1 and values["rsi"] <= 30)):
        trigger_risk.append(f"RSI14 {values['rsi']:.1f} ({'overbought LONG' if direction == 1 else 'oversold SHORT'})")
    if values["relative_volume"] is not None and values["relative_volume"] < 1:
        trigger_risk.append(f"relative volume {values['relative_volume']:.2f}x versus prior 20-bar mean")
    if trigger_risk:
        warnings.append("Trigger risk: " + "; ".join(trigger_risk))
    return warnings


def evidence_risk_warnings(row):
    warnings = []
    for warning in row.get("warnings") or []:
        if warning in STATIC_TRADE_WARNINGS or warning.startswith((
                "Short opposes ", "Long opposes ", "Extension-reversal model ", "Breakout-acceptance model ",
                "Trigger-time trend risk is incomplete;", "Directional context:")):
            continue
        if warning == "Latest stored price may be stale; inspect its timestamp":
            stamp = row.get("latest_price_at")
            if stamp:
                warnings.append(f"Latest price stale at source read: completed bar ended {stamp}; age exceeded 50 minutes")
            continue
        if warning == "Current stored price is stale; inspect its timestamp":
            stamp = row.get("latest_price_at")
            interval = f" {row['latest_price_interval']}" if row.get("latest_price_interval") else ""
            warnings.append(f"Current price stale: latest completed{interval} bar ended {stamp}"
                            if stamp else "Current price stale: completed-bar timestamp unavailable")
            continue
        warnings.append(warning)
    return list(dict.fromkeys(warnings))


def collapse_alert_rows(rows):
    groups = defaultdict(list)
    for row in rows:
        style = row.get("trade_style") or ("SWING" if row["interval"] == "1d" else "INTRADAY")
        opportunity = row.get("daily_setup_id") if style == "SWING" and row.get("daily_setup_id") else row.get("triggered_at") or row["alert_id"]
        groups[(row.get("strategy_instance_id"), row["security_id"], row["direction"], style, opportunity)].append(row)
    status_priority = {"OPEN": 5, "PENDING": 4, "UNRESOLVED": 3, "CLOSED": 2, "CLOSED_PAPER": 2, "NO_FILL": 1}
    interval_priority = {"30m": 0, "1h": 1, "1d": 2}
    collapsed = []
    for group in groups.values():
        primary = max(group, key=lambda row: (status_priority.get(row["status"], 0),
            timestamp_value(row.get("triggered_at")) or float("-inf"), row["alert_id"]))
        merged = dict(primary)
        variants = sorted(group, key=lambda row: (timestamp_value(row.get("triggered_at")) or float("-inf"),
            interval_priority.get(row["interval"], 99), row["model"], row["alert_id"]))
        merged.update(plan_count=len(variants),
            display_models=sorted({row["model"] for row in variants}),
            display_intervals=sorted({row["interval"] for row in variants}, key=lambda value: interval_priority.get(value, 99)),
            plan_variants=[{key: row.get(key) for key in ("alert_id", "model", "interval", "triggered_at", "trigger_price",
                "stop", "target", "status", "entry_price", "exit_price", "paper_return", "reason")} for row in variants],
            warnings=list(dict.fromkeys(warning for row in variants for warning in row.get("warnings", []))),
            hits=max((row.get("hits") for row in variants if row.get("hits") is not None), default=None),
            hit_models=sorted({model for row in variants for model in row.get("hit_models", [])}),
            hit_intervals=sorted({interval for row in variants for interval in row.get("hit_intervals", [])}))
        first_seen = [row.get("first_seen") for row in variants if timestamp_value(row.get("first_seen")) is not None]
        last_seen = [row.get("last_seen") for row in variants if timestamp_value(row.get("last_seen")) is not None]
        merged["first_seen"] = min(first_seen, key=timestamp_value) if first_seen else None
        merged["last_seen"] = max(last_seen, key=timestamp_value) if last_seen else None
        collapsed.append(merged)
    return collapsed


def alert_page(snapshot, *, session=None, view="latest", run=None, search="", direction=None,
               model=None, interval=None, status=None, lane=None, sort=None, descending=True,
               offset=0, limit=100, trade_type=None):
    if view not in ("latest", "history", "open") or (view == "open" and snapshot["source"] != "SHADOW"):
        raise ValueError("unsupported alert view")
    if trade_type not in (None, "INTRADAY", "SWING"):
        raise ValueError("unsupported trade type")
    dates = snapshot.get("sessions", [])
    if view != "open" and session and session not in dates:
        raise ValueError("session outside the retained 21-session navigation window")
    selected_date = session or (dates[-1] if dates else None)
    publications = sorted((item for item in snapshot.get("publications", []) if item["session"] == selected_date),
                          key=publication_order)
    if run and not any(item["run_id"] == run for item in publications):
        raise ValueError("publication does not belong to selected session")
    selected_run = next((item for item in publications if item["run_id"] == run), None) if run else (publications[-1] if publications else None)
    latest_by_strategy = {}
    for publication in publications:
        latest_by_strategy[publication.get("strategy_instance_id")] = publication
    latest_runs = [selected_run] if run and selected_run else list(latest_by_strategy.values())
    if snapshot.get("combined") and not run:
        selected_run = None
    withheld_run = None
    if view == "history":
        previous = history_publications(snapshot, selected_date)
        retained_ids = {item["run_id"] for item in previous}
        withheld_run = next((item for item in publications if item["run_id"] not in retained_ids), None)
        publications = previous
    eligible_runs = {item["run_id"] for item in publications} if view == "history" else {item["run_id"] for item in latest_runs}
    hit_windows = defaultdict(dict)
    if selected_date:
        hit_dates = set(session_dates(selected_date))
        for hit in snapshot.get("observations", []):
            if hit["session"] not in hit_dates or not hit["valid"]:
                continue
            key = (hit.get("strategy_instance_id"), hit["security_id"], hit["direction"])
            window = hit_windows[key].setdefault(hit["run_id"], dict(at=hit["at"], models=set(), intervals=set()))
            window["models"].update(hit["models"])
            window["intervals"].update(hit["intervals"])
    rows = []
    active_sides = defaultdict(lambda: defaultdict(int))
    for item in snapshot.get("alerts", []):
        if item["status"] in ("PENDING", "OPEN", "UNRESOLVED"):
            active_sides[item["security_id"]][item["direction"]] += 1
    for original in snapshot.get("alerts", []):
        if view == "open":
            if original["status"] not in ("PENDING", "OPEN", "UNRESOLVED"):
                continue
        elif original["run_id"] not in eligible_runs:
            continue
        style = original.get("trade_style") or ("SWING" if original["interval"] == "1d" else "INTRADAY")
        if (search and search.casefold() not in (original["ticker"] + " " + (original.get("company_name") or "")).casefold()
                or direction is not None and original["direction"] != direction
                or model and original["model"] != model or interval and original["interval"] != interval
                or status and original["status"] != status or lane and original["lane"] != lane
                or trade_type and style != trade_type):
            continue
        row = dict(original)
        row["warnings"] = [*directional_risk_warnings(row), *evidence_risk_warnings(row)]
        if snapshot.get("combined"):
            opposing = active_sides[row["security_id"]][-row["direction"]]
            row["opposing_exposure"] = opposing > 0
            if row["opposing_exposure"]:
                opposing_side = "LONG" if row["direction"] == -1 else "SHORT"
                row["warnings"] = list(dict.fromkeys([*row["warnings"],
                    f"Opposing active exposure: {opposing} {opposing_side} {'plan' if opposing == 1 else 'plans'} retained"]))
        row.update(price_return_fields(row))
        windows = hit_windows[(row.get("strategy_instance_id"), row["security_id"], row["direction"])]
        row_run = next((item for item in latest_runs if item["run_id"] == row["run_id"]), None)
        if view == "latest" and row_run:
            windows = {key: value for key, value in windows.items() if value["at"] <= row_run["published_at"]}
        details = sorted(windows.values(), key=lambda item: item["at"])
        row.update(hits=len(windows) if snapshot.get("hit_coverage") else None,
            first_seen=details[0]["at"] if details else None, last_seen=details[-1]["at"] if details else None,
            hit_models=sorted({model for item in details for model in item["models"]}),
            hit_intervals=sorted({interval for item in details for interval in item["intervals"]}))
        rows.append(row)
    rows = collapse_alert_rows(rows)
    sort = sort or ("triggered_at" if view == "history" else "published_at")
    allowed_sort = {"published_at", "triggered_at", "ticker", "direction", "model", "interval", "trigger_price", "entry_price",
                    "stop", "target", "risk_pct", "reward_risk", "hits", "latest_price", "paper_return", "price_return",
                    "atr_pct", "liquidity", "momentum", "rs_percentile", "rsi", "adx", "relative_volume",
                    "ema20_distance", "ema50_distance", "ema50_slope", "volatility", "extension_atr"}
    if sort not in allowed_sort:
        raise ValueError("unsupported sort column")
    def sort_value(row):
        value = row.get(sort, row.get("indicators", {}).get(sort))
        if sort in ("triggered_at", "published_at"):
            return timestamp_value(value)
        return value if not isinstance(value, float) or math.isfinite(value) else None
    present = sorted((row for row in rows if sort_value(row) is not None),
                     key=lambda row: (sort_value(row), row["security_id"], row["alert_id"]), reverse=descending)
    missing = sorted((row for row in rows if sort_value(row) is None), key=lambda row: row["alert_id"])
    counts = {state: sum(row["status"] == state for row in rows) for state in sorted({row["status"] for row in rows})}
    return dict(schema=SCHEMA, source=snapshot["source"], source_id=snapshot.get("source_id"),
        source_label=snapshot["source_label"], as_of=snapshot.get("as_of"), price_as_of=snapshot.get("price_as_of"), status=snapshot.get("status", "READY"),
        sessions=dates, session=selected_date, view=view, run=selected_run, runs=publications,
        withheld_run=withheld_run, sort=sort, descending=descending,
        total=len(rows), rows=(present + missing)[offset:offset + limit], offset=offset, limit=limit,
        counts=counts, hit_coverage=snapshot.get("hit_coverage"), warnings=snapshot.get("warnings", []),
        combined=snapshot.get("combined", False), strategy_streams=snapshot.get("strategy_streams", []),
        latest_runs=latest_runs, trade_type=trade_type,
        next_publication_at=snapshot.get("next_publication_at"),
        publication_mode=snapshot.get("publication_mode"), publication_window_start=snapshot.get("publication_window_start"),
        publication_deadline=snapshot.get("publication_deadline"),
        outcome_policy=snapshot.get("outcome_policy"), indicators_available=snapshot.get("indicators_available", []))


def price_return_fields(row):
    result = dict(price_return=None, price_return_basis="TRIGGER_PRICE_GROSS_NOT_EXECUTED", price_return_status="UNAVAILABLE")
    prices = (row.get("trigger_price"), row.get("latest_price"))
    if row.get("direction") not in (-1, 1) or row.get("lane") != "TRADE" or any(
        isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0 for value in prices
    ):
        return result
    try:
        trigger_at = datetime.fromisoformat(row["triggered_at"].replace("Z", "+00:00"))
        price_at = datetime.fromisoformat(row["latest_price_at"].replace("Z", "+00:00"))
        if trigger_at.tzinfo is None or price_at.tzinfo is None:
            return result
    except (KeyError, TypeError, ValueError, AttributeError):
        return result
    if price_at < trigger_at:
        return result | dict(price_return_status="PRICE_PRECEDES_TRIGGER")
    reason = str(row.get("reason") or "") + str(row.get("status") or "")
    if row.get("price_comparison_block") or any(value in reason for value in ("CORPORATE_ACTION", "IDENTITY", "INPUT_CORRECTION")):
        return result | dict(price_return_status="PRICE_BASIS_REVIEW_REQUIRED")
    value = row["direction"] * (prices[1] / prices[0] - 1)
    return result | dict(price_return=value, price_return_status="AVAILABLE") if math.isfinite(value) else result


def risk_fields(price, stop, target, direction):
    if direction not in (-1, 1) or any(value is None or not math.isfinite(value) or value <= 0 for value in (price, stop, target)):
        return dict(risk_pct=None, reward_risk=None)
    risk = direction * (price - stop)
    room = direction * (target - price)
    return dict(risk_pct=risk / price if risk > 0 else None,
                reward_risk=room / risk if min(risk, room) > 0 else None)


def empty_snapshot(source):
    return dict(schema=SCHEMA, source=source, source_label={"SHADOW": "Multi-model shadow", "REPLAY": "Historical replay", "LEGACY": "Legacy daily"}[source],
        status="AWAITING_PUBLICATION", as_of=None, sessions=[], publications=[], alerts=[], observations=[], hit_coverage=None,
        warnings=["No retained shadow publications" if source == "SHADOW" else "No retained publications"])


@lru_cache(maxsize=4)
def _read_snapshot(path, size, modified):
    with Path(path).open(encoding="utf-8") as handle:
        snapshot = json.load(handle)
    if snapshot.get("schema") != SCHEMA or snapshot.get("source") not in SOURCES:
        raise ValueError("unsupported alert snapshot schema/source")
    dates = snapshot["sessions"]
    if len(dates) > 21 or dates != sorted(set(dates)):
        raise ValueError("invalid alert navigation dates")
    return snapshot


def load_snapshot(path, source):
    if not path or not Path(path).is_file():
        return empty_snapshot(source)
    info = Path(path).stat()
    snapshot = _read_snapshot(str(path), info.st_size, info.st_mtime_ns)
    if snapshot["source"] != source:
        raise ValueError("alert source mismatch; replay cannot be served as shadow")
    return snapshot


def replay_snapshot(publications, config, source_cutoff, source_id):
    valid_reasons = {None, "CONTINUING_EPISODE", "MODEL_QUOTA", "MODEL_STOCK_DUPLICATE", "ACTIVE_POSITION_CAP",
                     "OPPOSING_FRESH_MODEL", "OPEN_POSITION_OPPOSITION"}
    records, alerts, observations = [], [], []
    for publication in publications:
        if publication["arm"] != "PRIORITY":
            continue
        run_id, at = publication["window_key"], publication["deadline"]
        records.append(dict(run_id=run_id, session=publication["session"], trigger_at=run_id, published_at=at,
            status=publication["coverage"], expected=len(publication["expected_members"]),
            missing=len(publication["missing_members"]), selected=len(publication["selected"]),
            conflicts=sum(bool(row.get("conflicts")) for row in publication["dispositions"])))
        for disposition in publication["dispositions"]:
            valid = publication["coverage"] == "PUBLISHED" and disposition.get("reason") in valid_reasons
            if valid:
                observations.append(dict(security_id=disposition["security_id"], direction=disposition["direction"],
                    session=publication["session"], run_id=run_id, at=at, valid=True,
                    models=[disposition["model"]], intervals=[disposition["interval"]]))
        dispositions = {row["episode_id"]: row for row in publication["dispositions"]}
        for episode_id in publication["selected"]:
            position = publication["outcomes"].get(episode_id)
            if position is None:
                disposition = dispositions[episode_id]
                if disposition["model"] == "discovery":
                    alerts.append(dict(alert_id=episode_id, run_id=run_id, security_id=disposition["security_id"], ticker="",
                        company_name=None, model="discovery", interval="1d", direction=disposition["direction"], lane="WATCH",
                        triggered_at=run_id, published_at=at, trigger_price=None, entry_price=None, entry_at=None,
                        stop=None, target=None, risk_pct=None, reward_risk=None, entry_risk=None, hold="Watch only",
                        exit_due_at=None, status="WATCH", reason=None, exit_at=None, exit_price=None, paper_return=None,
                        mark_price=None, mark_at=None, latest_price=None, latest_price_at=None, indicators={},
                        indicator_at=run_id, indicator_interval="1d", daily_context_at=run_id,
                        indicator_status="NO_TRADE_MODEL_POSITION", warnings=["Discovery context only"], policy_version=config["policy_version"]))
                continue
            candidate = position["candidate"]
            model = candidate["model"]
            interval = candidate["interval"]
            horizon = config["models"][model]
            swing = position.get("swing_evidence")
            warnings = ["Paper only", "Stop gaps can exceed planned risk", "Action coverage not certified"]
            if candidate["direction"] == -1:
                warnings.append("Short borrow unverified")
            if dispositions[episode_id].get("context"):
                warnings.append("Countertrend context")
            state = position["state"]
            net = position.get("net_by_cost_bps", {}).get(str(config["primary_cost_bps"])) if state == "CLOSED" else 0. if state == "NO_FILL" else None
            if state == "OPEN" and position.get("entry_price") and position.get("mark_price"):
                net = candidate["direction"] * (position["mark_price"] / position["entry_price"] - 1) - config["primary_cost_bps"] / 20000
            alerts.append(dict(alert_id=episode_id, run_id=run_id, security_id=candidate["security_id"], ticker=candidate["ticker"],
                company_name=None, model=model, interval=interval, direction=candidate["direction"], lane="TRADE",
                triggered_at=candidate["trigger_at"], published_at=at, trigger_price=candidate["price"],
                entry_price=position.get("entry_price"), entry_at=position.get("entry_at"), stop=candidate["stop"], target=candidate["target"],
                **risk_fields(candidate["price"], candidate["stop"], candidate["target"], candidate["direction"]),
                entry_risk=risk_fields(position.get("entry_price"), candidate["stop"], candidate["target"], candidate["direction"]),
                hold=f"{horizon['daily_horizon_sessions']} sessions" if interval == "1d" or swing else f"{horizon['time_cap_minutes']} min / close",
                exit_due_at=position.get("planned_exit_at"), status=state, reason=position.get("reason"),
                exit_at=position.get("exit_at"), exit_price=position.get("exit_price"), paper_return=net,
                mark_price=position.get("mark_price"), mark_at=position.get("mark_at"), latest_price=None, latest_price_at=None,
                indicators=dict(atr=candidate["activation_atr"], atr_pct=candidate["activation_atr"] / candidate["price"] if candidate["price"] else None,
                    liquidity=candidate["liquidity"], momentum=candidate["momentum"], rs_percentile=candidate["rs_rank"],
                    extension_atr=candidate["extension"]), indicator_at=candidate["trigger_at"], indicator_interval=interval,
                daily_context_at=None, indicator_status="RETAINED_CANDIDATE_FIELDS", warnings=warnings,
                policy_version=candidate["policy_version"]))
            if swing:
                alerts[-1].update(trade_style="SWING", setup_interval="1d", confirmation_interval=interval,
                    daily_setup_at=swing["daily_setup"]["trigger_at"], daily_setup_id=swing["daily_episode_id"],
                    holding_sessions=swing["holding_sessions"], holding_count="ENTRY_SESSION_IS_SESSION_ONE",
                    indicator_at=swing["daily_setup"]["trigger_at"], indicator_interval="1d",
                    daily_context_at=swing["daily_setup"]["trigger_at"], indicator_status="FROZEN_DAILY_SETUP_FIELDS")
                alerts[-1]["indicators"]["atr_pct"] = swing["daily_setup"]["activation_atr"] / swing["daily_setup"]["price"]
    return dict(schema=SCHEMA, source="REPLAY", source_id=source_id, source_label="Historical replay / covered universe",
        status="READY", as_of=source_cutoff, sessions=session_dates(config["end"]), publications=records, alerts=alerts,
        observations=observations, hit_coverage="Retained valid candidate windows only; no pre-pilot observation history",
        outcome_policy=f"Frozen plan / {config['primary_cost_bps']} bps round trip", indicators_available=[],
        warnings=["Reconstructed research, not delivered live alerts", "Prices and outcomes frozen at source cutoff", "No model qualified"])


def enrich_replay(snapshot, bundle, config):
    import pandas as pd
    from research.features import _rsi
    from research.stock_idea_models import feature_frames
    from research.stock_idea_replay import available_at, utc, valid_bar
    members = {row["security_id"] for row in snapshot["alerts"]}
    frames = feature_frames(dict(bundle, bars=[bar for bar in bundle["bars"] if bar["security_id"] in members]), config)
    indices = {}
    for key, frame in frames.items():
        grouped = frame.groupby("segment")
        frame["rsi"] = grouped.close.transform(_rsi)
        frame["relative_volume"] = frame.volume / grouped.volume.transform(lambda values: values.shift(1).rolling(20).mean()).replace(0, float("nan"))
        frame["volatility"] = grouped.close.transform(lambda values: values.pct_change(fill_method=None).rolling(21).std())
        indices[key] = frame.set_index("bar_end")
    latest = {}
    for bar in bundle["bars"]:
        if (bar["interval"] == "30m" and bar["security_id"] in members and valid_bar(bar)
                and available_at(bar, config) <= utc(snapshot["as_of"])):
            previous = latest.get(bar["security_id"])
            if previous is None or utc(bar["bar_end"]) > utc(previous["bar_end"]):
                latest[bar["security_id"]] = bar
    calendar = exchange_calendars.get_calendar("XNYS")
    for alert in snapshot["alerts"]:
        indicator_interval = alert.get("indicator_interval") or alert["interval"]
        stamp = pd.Timestamp(alert.get("indicator_at") or alert["triggered_at"])
        frame = indices.get((alert["security_id"], indicator_interval))
        if frame is not None and stamp in frame.index:
            row = frame.loc[stamp]
            if not alert["ticker"]:
                alert["ticker"] = row.ticker
                scale = row.get("execution_scale", 1.)
                alert["trigger_price"] = float(row.close * scale) if scale is not None and math.isfinite(scale) else None
            if row.ready and row.visible_at <= pd.Timestamp(alert["published_at"]):
                values = dict(rsi=row.rsi, relative_volume=row.relative_volume, volatility=row.volatility,
                    ema20_distance=row.close / row.ema20 - 1, ema50_distance=row.close / row.ema50 - 1,
                    ema50_slope=row.ema50 / row.ema50_prior10 - 1)
                alert["indicators"].update({key: float(value) if math.isfinite(value) else None for key, value in values.items()})
                alert.update(indicator_status="RECONSTRUCTED_PINNED_TRIGGER_SNAPSHOT", indicator_revision_id=row.revision_id,
                             indicator_available_at=str(row.visible_at))
        trigger_date = stamp.date().isoformat()
        daily_date = trigger_date if alert["interval"] == "1d" else str(calendar.previous_session(trigger_date).date())
        alert["daily_context_at"] = calendar.session_close(daily_date).isoformat()
        daily_frame = indices.get((alert["security_id"], "1d"))
        daily_stamp = pd.Timestamp(alert["daily_context_at"])
        if daily_frame is not None and daily_stamp in daily_frame.index:
            daily_row = daily_frame.loc[daily_stamp]
            if daily_row.ready and daily_row.visible_at <= pd.Timestamp(alert["published_at"]):
                value = daily_row.relative_volume
                alert["indicators"]["daily_rvol20"] = float(value) if math.isfinite(value) else None
        bar = latest.get(alert["security_id"])
        if bar:
            if not alert["ticker"]:
                alert["ticker"] = bar["ticker"]
            alert.update(latest_price=bar["close"], latest_price_at=bar["bar_end"], latest_price_revision_id=bar["revision_id"])
    snapshot["indicators_available"] = sorted({key for row in snapshot["alerts"] for key, value in row["indicators"].items() if value is not None})
    return snapshot