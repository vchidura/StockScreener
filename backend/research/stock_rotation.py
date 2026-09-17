"""Price-based leadership and divergence observations, never selection gates."""
from __future__ import annotations

import math
from collections import Counter, defaultdict
from functools import lru_cache
import json
from pathlib import Path

import exchange_calendars
import pandas as pd

from research.gics_sectors import SECTOR_BENCHMARK_ETF, sector_for_sic
from research.stock_alert_context import daily_price_context, dated_reference, market_context, observation, same_time_volume_context, utc


VERSION = "stock_rotation_v1"
SCHEMA = "stock_rotation_snapshot_v1"
HISTORY_SESSIONS = 5
BOND_PROXY_VERSION = "bond_etf_relative_performance_price_v1"
BOND_HORIZONS = (1, 5, 20)
BOND_STUDY_VERSION = "bond_etf_oas_comparison_v1"
CONDITIONS_SCORE_VERSION = "market_conditions_development_v1"
CONDITIONS_WEIGHTS = {"momentum": .25, "participation": .25, "volatility": .25, "credit": .25}


def development_conditions_score(spy, qqq, breadth, vix, credit, session, cutoff):
    from research.stock_idea_engine import digest
    cutoff = utc(cutoff)
    inputs = {"momentum": [spy, qqq], "participation": [breadth], "volatility": [vix], "credit": [credit]}
    fields = {"momentum": ("return20_percentile",), "participation": ("above_sma50_fraction", "above_sma200_fraction"),
        "volatility": ("percentile",), "credit": ("percentile",)}
    methods = {"momentum": "Mean SPY/QQQ 20-session return percentiles; current excluded",
        "participation": "Mean tracked-stock fractions above SMA50/SMA200",
        "volatility": "One minus VIX prior-history percentile", "credit": "One minus ICE OAS prior-history percentile"}
    components = {}
    for name, sources in inputs.items():
        result = observation("UNAVAILABLE", "REQUIRED_COMPONENT_UNAVAILABLE", session=session, weight=CONDITIONS_WEIGHTS[name], method=methods[name])
        result["input_statuses"] = [source.get("status", "UNAVAILABLE") for source in sources]
        result["input_sessions"] = [source.get("session") for source in sources]
        result["input_market_times"] = [source.get("market_time") for source in sources]
        allowed = ("READY", "PARTIAL") if name == "participation" else ("READY",)
        if any(source.get("status") not in allowed or not source.get("value") or not source.get("source_revision_ids") for source in sources):
            if any(source.get("status") == "STALE" for source in sources):
                result.update(status="STALE", reason_codes=["SOURCE_CLOSE_BEHIND_REQUIRED_SESSION"])
            components[name] = result
            continue
        if any(not source.get("available_at") or utc(source["available_at"]) > cutoff or not source.get("market_time") or utc(source["market_time"]) > cutoff for source in sources):
            components[name] = result | dict(reason_codes=["COMPONENT_NOT_AVAILABLE_AT_CUTOFF"])
            continue
        if any(source.get("session", utc(source["market_time"]).date().isoformat()) != session for source in sources):
            components[name] = result | dict(status="STALE", reason_codes=["UNPAIRED_COMPONENT_SESSION"])
            continue
        values = [source["value"].get(field) for source in sources for field in fields[name]]
        if any(type(value) not in (int, float) or not math.isfinite(value) or not 0 <= value <= 1 for value in values):
            components[name] = result | dict(reason_codes=["COMPONENT_SCORE_INPUT_INVALID_OR_INSUFFICIENT"])
            continue
        score = 100 * sum(values) / len(values)
        if name in ("volatility", "credit"):
            score = 100 - score
        components[name] = result | dict(status="PARTIAL" if any(source["status"] == "PARTIAL" for source in sources) else "READY",
            reason_codes=["PARTIAL_TRACKED_BREADTH"] if name == "participation" and breadth["status"] == "PARTIAL" else [],
            value=dict(score=score, weight=CONDITIONS_WEIGHTS[name], contribution=CONDITIONS_WEIGHTS[name] * (score - 50)),
            market_time=max(utc(source["market_time"]) for source in sources).isoformat(),
            available_at=max(utc(source["available_at"]) for source in sources).isoformat(),
            source_revision_ids=sorted({revision for source in sources for revision in source["source_revision_ids"]}),
            coverage=[dict(available=source["value"].get("return20_percentile_samples"), expected=252) if name == "momentum"
                else dict(available=source.get("timely_observations"), expected=source.get("expected_observations")) for source in sources],
            raw_normalized_inputs=values)
    policy = dict(version=CONDITIONS_SCORE_VERSION, weights=CONDITIONS_WEIGHTS, methods=methods,
        interpretation="DESCRIPTIVE_CONDITIONS_NOT_FEAR_GREED_OR_PROBABILITY", missing_policy="NO_REWEIGHTING_OR_NEUTRAL_FILL")
    policy_id = "conditions-policy:" + digest(policy)
    weights = observation("CONFIGURED", calculation_version=CONDITIONS_SCORE_VERSION, source_type="CONFIGURATION")
    weights.update(value=dict(CONDITIONS_WEIGHTS), source_revision_ids=[policy_id])
    result = observation("UNAVAILABLE", "REQUIRED_COMPONENTS_MISSING", session=session, calculation_version=CONDITIONS_SCORE_VERSION,
        development_only=True, policy=policy, components=components)
    eligible = all(component["status"] in ("READY", "PARTIAL") for component in components.values())
    if eligible:
        partial = any(component["status"] == "PARTIAL" for component in components.values())
        result.update(status="PARTIAL" if partial else "READY", reason_codes=["PARTIAL_TRACKED_BREADTH"] if partial else [],
            value=dict(score=50 + sum(component["value"]["contribution"] for component in components.values())),
            market_time=max(component["market_time"] for component in components.values()),
            available_at=max(component["available_at"] for component in components.values()),
            source_revision_ids=sorted({policy_id} | {revision for component in components.values() for revision in component["source_revision_ids"]}))
    return result, weights


def sector_option_activity_context(row, cutoff, boundary, error=None):
    result = observation("NEEDS_COVERAGE", error or "NO_CURRENT_POLICY_ETF_OPTION_MATRIX",
        calculation_version="retained_option_premium_activity_v1", units="USD_ESTIMATED_ACTIVITY",
        interpretation="CALL_PUT_ACTIVITY_NOT_BUY_SELL_FLOW")
    if row is None:
        return result
    if any(row.get(field) is None or utc(row[field]) > utc(cutoff) for field in ("market_time", "observed_at", "created_at", "available_at")):
        return result | dict(status="UNAVAILABLE", reason_codes=["OPTION_MATRIX_NOT_AVAILABLE_AT_CUTOFF"])
    result.update(market_time=utc(row["market_time"]).isoformat(), observed_at=utc(row["observed_at"]).isoformat(),
        created_at=utc(row["created_at"]).isoformat(), available_at=utc(row["available_at"]).isoformat(),
        expected_observations=row["retained_contracts"], timely_observations=row["valid_contracts"],
        source_revision_ids=[row["matrix_id"], row["batch_id"]])
    if not row["contracts"] or not row["valid_contracts"]:
        return result | dict(status="UNAVAILABLE", reason_codes=["NO_VALID_OPTION_ACTIVITY_CONTRACTS"])
    fields = ("call_volume", "put_volume", "call_premium", "put_premium")
    if any(type(row.get(field)) not in (float, int) or not math.isfinite(row[field]) or row[field] < 0 for field in fields):
        return result | dict(status="UNAVAILABLE", reason_codes=["INVALID_OPTION_ACTIVITY_AGGREGATES"])
    partial = row["valid_contracts"] != row["contracts"] or row["contracts"] != row["retained_contracts"]
    stale = utc(row["market_time"]) < utc(boundary)
    return result | dict(status="STALE" if stale else "PARTIAL" if partial else "READY",
        reason_codes=["OPTION_MATRIX_BEHIND_EXPECTED_WINDOW"] if stale else ["INCOMPLETE_PREMIUM_ACTIVITY_COVERAGE"] if partial else [],
        value={field: row[field] for field in fields} | dict(contracts=row["contracts"], valid_contracts=row["valid_contracts"],
            put_call_volume_ratio=row["put_volume"] / row["call_volume"] if row["call_volume"] > 0 else None))


def etf_creation_context(records, ticker, session, cutoff, reference):
    result = observation("NEEDS_SOURCE", "DATED_NAV_AND_SHARES_OUTSTANDING_REQUIRED", session=session,
        units="USD_ESTIMATED_NET_CREATIONS", calculation_version="etf_net_creation_estimate_v1",
        interpretation="SHARE_CHANGE_TIMES_NAV_NOT_SECTOR_CAPITAL_FLOW")
    calendar = exchange_calendars.get_calendar("XNYS")
    previous = str(calendar.previous_session(session).date())
    eligible = [row for row in records if row.get("ticker") == ticker and row.get("session") in (previous, session)
        and max(utc(row["observed_at"]), utc(row["created_at"])) <= utc(cutoff)]
    if reference is None or reference["security_type"] != "ETF" or any(utc(reference[field]) > utc(cutoff) for field in ("observed_at", "created_at")):
        return result | dict(status="UNAVAILABLE", reason_codes=["DATED_ETF_REFERENCE_REQUIRED"])
    selected = {}
    for row in eligible:
        if str(row["security_id"]) != str(reference["security_id"]):
            continue
        key = (utc(row["observed_at"]), utc(row["created_at"]), row["revision_id"])
        if row["session"] not in selected or key > selected[row["session"]][0]:
            selected[row["session"]] = (key, row)
    if set(selected) != {previous, session}:
        return result
    before, current = selected[previous][1], selected[session][1]
    if before.get("source") != current.get("source") or current.get("currency") != "USD" or before.get("currency") != "USD":
        return result | dict(status="UNAVAILABLE", reason_codes=["INCOMPARABLE_ETF_NAV_SHARE_RECORDS"])
    numeric = (before.get("shares_outstanding"), current.get("shares_outstanding"), current.get("nav"), current.get("split_factor_from_previous"))
    if any(type(value) not in (float, int) or not math.isfinite(value) or value <= 0 for value in numeric):
        return result | dict(status="UNAVAILABLE", reason_codes=["INVALID_NAV_SHARES_OR_SPLIT_FACTOR"])
    if current.get("split_review") != "VERIFIED" or not current.get("split_evidence_id") or current.get("nav_basis") != "EX_DISTRIBUTION":
        return result | dict(status="UNAVAILABLE", reason_codes=["ETF_SPLIT_OR_DISTRIBUTION_BASIS_UNVERIFIED"])
    close = calendar.session_close(session).isoformat()
    if utc(close) > utc(cutoff) or any(utc(row["market_time"]) != calendar.session_close(row["session"]) for row in (before, current)):
        return result | dict(status="UNAVAILABLE", reason_codes=["UNPAIRED_ETF_VALUATION_TIMES"])
    change = current["shares_outstanding"] - before["shares_outstanding"] * current["split_factor_from_previous"]
    return result | dict(status="READY", reason_codes=[], market_time=close,
        available_at=max(utc(row[field]) for row in (before, current, reference) for field in ("observed_at", "created_at")).isoformat(),
        source_revision_ids=sorted({before["revision_id"], current["revision_id"], current["split_evidence_id"], str(reference["revision_id"])}),
        value=dict(net_creations_usd=change * current["nav"], share_change=change, nav=current["nav"],
            shares_outstanding=current["shares_outstanding"], previous_session=previous))


def bond_etf_relative_performance(hyg, lqd, references, cutoff):
    cutoff = utc(cutoff)
    result = observation("UNAVAILABLE", "BOND_PRICE_CONTEXT_UNAVAILABLE", calculation_version=BOND_PROXY_VERSION,
        price_basis="RAW_ACTION_GATED", units="FRACTION_DIFFERENCE", interpretation="HYG_MINUS_LQD_NOT_OAS",
        limitations=["Price only; distributions excluded", "Not duration neutral; not a credit spread or fund flow"])
    contexts = (hyg, lqd)
    if any(context is None or context.get("status") != "READY" for context in contexts):
        return result
    matched = []
    for ticker, context in zip(("HYG", "LQD"), contexts):
        identity = context.get("security_id")
        reference = dated_reference(references, identity, ticker, cutoff) if identity else None
        if context.get("ticker") != ticker or not reference or reference["security_type"] != "ETF":
            return result | dict(reason_codes=["BOND_SECURITY_IDENTITY_MISMATCH"])
        matched.append(reference)
    if hyg["security_id"] == lqd["security_id"]:
        return result | dict(reason_codes=["BOND_SECURITY_IDENTITY_MISMATCH"])
    if len({(context["session"], utc(context["market_time"]), context.get("price_basis")) for context in contexts}) != 1 or hyg.get("price_basis") != "RAW_ACTION_GATED":
        return result | dict(reason_codes=["INCOMPARABLE_BOND_PRICE_WINDOWS"])
    if any(not context.get("source_revision_ids") or not context.get("available_at")
            or any(context.get(field) is None or utc(context[field]) > cutoff for field in ("market_time", "available_at", "observed_at", "created_at"))
            for context in contexts):
        return result | dict(reason_codes=["BOND_INPUT_NOT_AVAILABLE_AT_CUTOFF"])
    if any(type(context["value"].get(f"return{horizon}")) not in (int, float)
            or not math.isfinite(context["value"][f"return{horizon}"]) for context in contexts for horizon in BOND_HORIZONS):
        return result | dict(reason_codes=["BOND_RETURN_UNAVAILABLE"])
    value = {f"{name}_return{horizon}": context["value"][f"return{horizon}"]
        for name, context in zip(("hyg", "lqd"), contexts) for horizon in BOND_HORIZONS}
    value.update({f"relative{horizon}": value[f"hyg_return{horizon}"] - value[f"lqd_return{horizon}"] for horizon in BOND_HORIZONS})
    return result | dict(status="READY", reason_codes=[], value=value, session=hyg["session"],
        market_time=utc(hyg["market_time"]).isoformat(),
        observed_at=max(utc(context["observed_at"]) for context in contexts).isoformat(),
        created_at=max(utc(context["created_at"]) for context in contexts).isoformat(),
        available_at=max([utc(context["available_at"]) for context in contexts] +
            [utc(reference[field]) for reference in matched for field in ("effective_from", "observed_at", "created_at")]).isoformat(),
        source_revision_ids=sorted({str(reference["revision_id"]) for reference in matched} |
            {revision for context in contexts for revision in context["source_revision_ids"]}),
        securities={ticker: context["security_id"] for ticker, context in zip(("HYG", "LQD"), contexts)})


def bond_comparison_statistics(rows):
    matched = [row for row in rows if row["status"] == "MATCHED"]
    nonzero = [row for row in matched if row["proxy_fraction"] != 0 and row["oas_tightening_bps"] != 0]
    proxy = pd.Series([row["proxy_fraction"] for row in matched], dtype=float)
    tightening = pd.Series([row["oas_tightening_bps"] for row in matched], dtype=float)
    estimable = len(matched) >= 3 and proxy.nunique() > 1 and tightening.nunique() > 1
    pearson = float(proxy.corr(tightening)) if estimable else None
    spearman = float(proxy.rank(method="average").corr(tightening.rank(method="average"))) if estimable else None
    return dict(expected_pairs=len(rows), matched_pairs=len(matched), excluded_pairs=len(rows) - len(matched),
        coverage_fraction=len(matched) / len(rows) if rows else None,
        pearson=pearson if pearson is not None and math.isfinite(pearson) else None,
        spearman=spearman if spearman is not None and math.isfinite(spearman) else None,
        correlation_status="ESTIMABLE_DESCRIPTIVE_ONLY" if estimable else "NOT_ESTIMABLE",
        directional_agreement=sum(row["proxy_fraction"] * row["oas_tightening_bps"] > 0 for row in nonzero) / len(nonzero) if nonzero else None,
        nonzero_pairs=len(nonzero), tied_or_zero_pairs=len(matched) - len(nonzero),
        first_matched=matched[0]["session"] if matched else None, last_matched=matched[-1]["session"] if matched else None,
        exclusions=dict(Counter(reason for row in rows for reason in row["reason_codes"])))


def compare_bond_proxy_with_oas(inputs):
    from equity.market_context_source import VERSION as fred_version, fred_context
    from research.stock_idea_engine import digest
    if inputs.get("schema") != BOND_STUDY_VERSION or inputs.get("price_basis") != "RAW_ACTION_GATED" or inputs.get("oas_units") != "PERCENT":
        raise ValueError("incompatible bond study contract or units")
    if inputs.get("input_sha256") != digest({key: value for key, value in inputs.items() if key != "input_sha256"}):
        raise ValueError("bond study input hash mismatch")
    if inputs["database_snapshot"].get("read_only") != "on" or inputs["database_snapshot"].get("isolation") != "repeatable read":
        raise ValueError("bond study requires a read-only consistent capture")
    cutoff = utc(inputs["cutoff"])
    calendar = exchange_calendars.get_calendar("XNYS")
    end = calendar.date_to_session(inputs["session"])
    if calendar.session_close(end) > cutoff:
        raise ValueError("bond study session has not completed at cutoff")
    dates = [str(session.date()) for session in calendar.sessions_in_range(calendar.session_offset(end, -252), end)]
    if inputs["sessions"] != dates or set(inputs["prices"]) != {"HYG", "LQD"}:
        raise ValueError("bond study population or calendar differs")
    contexts, prices = {}, {}
    for ticker in ("HYG", "LQD"):
        source = inputs["prices"][ticker]
        if len(source["bars"]) > 1000:
            raise ValueError("bond price history exceeds bounds")
        if len({row["revision_id"] for row in source["bars"]}) != len(source["bars"]):
            raise ValueError("duplicate bond price revisions")
        value = daily_price_context(source["bars"], source["security_id"], dates[-1], cutoff, inputs["actions"], include_history=True, expected_ticker=ticker)
        history = value.pop("history", [])
        contexts[ticker] = dict(value, security_id=source["security_id"], ticker=ticker)
        prices[ticker] = {row["session"]: row["close"] for row in history}
    current = bond_etf_relative_performance(contexts["HYG"], contexts["LQD"], inputs["references"], cutoff)
    oas, reference = {}, dict(status=inputs["reference_status"], series_id="BAMLH0A0HYM2", received_at=None, response_sha256=None)
    record = inputs.get("oas_record")
    if record is not None:
        if inputs.get("reference_status") != "APPROVED_RETAINED" or record.get("source") != "FRED" or record.get("version") != fred_version or record.get("series_id") != "BAMLH0A0HYM2":
            raise ValueError("unapproved or incorrect OAS reference")
        if len(record["observations"]) > 600 or utc(record["received_at"]) > cutoff:
            raise ValueError("OAS reference exceeds bound or capture cutoff")
        values = fred_context(record, cutoff, include_history=True)
        oas = {row["session"]: row["value"] for row in values.get("history", [])}
        reference.update(status="AVAILABLE" if oas else "NO_ELIGIBLE_HISTORY", received_at=record["received_at"],
            response_sha256=record["response_sha256"], last_observation=max(oas) if oas else None)
    rows, summary = [], {}
    midpoint = len(dates) // 2
    for horizon in BOND_HORIZONS:
        horizon_rows = []
        for index in range(horizon, len(dates)):
            window = dates[index - horizon:index + 1]
            reasons = []
            if current["status"] != "READY":
                reasons.append("BOND_INPUT_CONTEXT_UNAVAILABLE")
            if not oas:
                reasons.append("OAS_REFERENCE_UNAVAILABLE")
            elif any(date not in oas for date in window):
                reasons.append("MISSING_EXPECTED_OAS_OBSERVATION")
            row = dict(horizon=horizon, start_session=window[0], session=window[-1], status="EXCLUDED" if reasons else "MATCHED",
                reason_codes=reasons, nonoverlapping=index % horizon == 0,
                half="FIRST" if index < midpoint else "SECOND" if index - horizon >= midpoint else "CROSSES_MIDPOINT",
                hyg_return=None, lqd_return=None, proxy_fraction=None, proxy_pp=None,
                oas_start_bps=oas.get(window[0]), oas_end_bps=oas.get(window[-1]), oas_change_bps=None, oas_tightening_bps=None,
                known_distribution_dates=sorted({str(action["effective_date"]) for action in inputs["actions"]
                    if action["action_type"] == "DIVIDEND" and str(action["security_id"]) in {context["security_id"] for context in contexts.values()}
                    and window[0] < str(action["effective_date"]) <= window[-1]
                    and max(utc(action["first_observed_at"]), utc(action["created_at"])) <= cutoff}))
            if current["status"] == "READY":
                row.update({name.lower() + "_return": prices[name][window[-1]] / prices[name][window[0]] - 1 for name in prices})
                row["proxy_fraction"] = row["hyg_return"] - row["lqd_return"]
                row["proxy_pp"] = 100 * row["proxy_fraction"]
            if not reasons:
                row["oas_change_bps"] = row["oas_end_bps"] - row["oas_start_bps"]
                row["oas_tightening_bps"] = -row["oas_change_bps"]
            horizon_rows.append(row)
        summary[str(horizon)] = dict(rolling=bond_comparison_statistics(horizon_rows),
            nonoverlapping=bond_comparison_statistics([row for row in horizon_rows if row["nonoverlapping"]]),
            first_half=bond_comparison_statistics([row for row in horizon_rows if row["half"] == "FIRST"]),
            second_half=bond_comparison_statistics([row for row in horizon_rows if row["half"] == "SECOND"]))
        rows.extend(horizon_rows)
    disagreements = sorted([row for row in rows if row["status"] == "MATCHED" and row["proxy_fraction"] * row["oas_tightening_bps"] < 0],
        key=lambda row: (-abs(row["proxy_fraction"]), row["session"], row["horizon"]))[:10]
    return dict(schema=BOND_STUDY_VERSION, input_sha256=inputs["input_sha256"], cutoff=inputs["cutoff"],
        period_start=dates[0], period_end=dates[-1], current_proxy=current,
        status="COMPARED" if any(row["status"] == "MATCHED" for row in rows) else "COMPARISON_PENDING",
        reference=reference, summary=summary, rows=rows, largest_direction_disagreements=disagreements,
        limitations=["Price-only ETF returns; not ICE OAS replication or duration-neutral credit stress",
            "Distributions not adjusted; known events only, distribution coverage not certified",
            "Rolling 5/20-session windows overlap; descriptive association, not independent samples or trading validation",
            "Historical observations reconstructed at capture; no original decision-time availability claim",
            "Fixed horizons and weights; no fitted OAS estimate, p-values, or prediction claims"])


def verify_bond_comparison(inputs, report):
    from research.stock_idea_engine import digest
    if report.get("report_sha256") != digest({key: value for key, value in report.items() if key != "report_sha256"}):
        raise ValueError("bond comparison report hash mismatch")
    expected = compare_bond_proxy_with_oas(inputs)
    if {key: value for key, value in report.items() if key != "report_sha256"} != expected:
        raise ValueError("bond comparison does not reproduce frozen inputs")
    return dict(status="VERIFIED", comparison_status=report["status"], rows=len(report["rows"]),
        matched_pairs={key: value["rolling"]["matched_pairs"] for key, value in report["summary"].items()})


def bond_comparison_view(inputs, report):
    from research.stock_idea_engine import digest
    verify_bond_comparison(inputs, report)
    view = {key: report[key] for key in ("schema", "status", "cutoff", "period_start", "period_end", "reference", "summary", "limitations", "input_sha256", "report_sha256", "current_proxy", "largest_direction_disagreements")}
    view["view_sha256"] = digest(view)
    return view


@lru_cache(maxsize=2)
def _read_bond_comparison_view(path, modified_ns, size):
    from research.stock_idea_engine import digest
    if size > 250_000:
        raise ValueError("bond comparison view exceeds bound")
    view = json.loads(Path(path).read_text(encoding="utf-8"))
    if view.get("schema") != BOND_STUDY_VERSION or view.get("view_sha256") != digest({key: value for key, value in view.items() if key != "view_sha256"}):
        raise ValueError("bond comparison view is incompatible or corrupt")
    return view


def load_bond_comparison_view(path, now):
    try:
        stat = path.stat()
    except FileNotFoundError:
        return dict(status="COMPARISON_PENDING", reason_codes=["NO_FROZEN_COMPARISON_REPORT"])
    view = _read_bond_comparison_view(str(path.resolve()), stat.st_mtime_ns, stat.st_size)
    if utc(view["cutoff"]) > utc(now):
        raise ValueError("bond comparison cutoff is in the future")
    return view


def rotation_state(relative20, relative_change5):
    if relative20 == 0 or relative_change5 == 0:
        return "TRANSITION"
    if relative20 > 0:
        return "LEADING_STRENGTHENING" if relative_change5 > 0 else "LEADING_WEAKENING"
    return "LAGGING_IMPROVING" if relative_change5 > 0 else "LAGGING_DETERIORATING"


def relative_rotation(asset, benchmark):
    result = dict(status="UNAVAILABLE", reason_codes=["PAIRED_PRICE_CONTEXT_UNAVAILABLE"], value=None,
        session=asset.get("session"), source_revision_ids=[], available_at=None, price_basis=asset.get("price_basis"))
    if asset.get("status") != "READY" or benchmark.get("status") != "READY":
        return result
    if asset["session"] != benchmark["session"] or asset["market_time"] != benchmark["market_time"] or asset.get("price_basis") != benchmark.get("price_basis"):
        return result | dict(reason_codes=["INCOMPARABLE_PRICE_WINDOWS"])
    fields = ("return1", "return5", "return20", "return_previous5")
    if any(not isinstance(context["value"].get(field), (int, float)) or not math.isfinite(context["value"][field])
           for context in (asset, benchmark) for field in fields):
        return result | dict(reason_codes=["ROTATION_INPUT_UNAVAILABLE"])
    relative = {f"relative{horizon}": asset["value"][f"return{horizon}"] - benchmark["value"][f"return{horizon}"] for horizon in (1, 5, 20)}
    previous5 = asset["value"]["return_previous5"] - benchmark["value"]["return_previous5"]
    change5 = relative["relative5"] - previous5
    return result | dict(status="READY", reason_codes=[], available_at=max(asset["available_at"], benchmark["available_at"]),
        source_revision_ids=sorted(set(asset["source_revision_ids"] + benchmark["source_revision_ids"])),
        value=dict(relative, previous_relative5=previous5, relative_change5=change5,
            state=rotation_state(relative["relative20"], change5),
            absolute_return5=asset["value"]["return5"], absolute_return20=asset["value"]["return20"],
            benchmark_return5=benchmark["value"]["return5"], absolute_trend=asset["value"]["direction"],
            emerging_leadership=relative["relative20"] < 0 and relative["relative5"] > 0 and change5 > 0))


def summarize_rotation_history(history):
    calendar = exchange_calendars.get_calendar("XNYS")
    ordered = sorted(history, key=lambda row: row["session"])
    if len({row["session"] for row in ordered}) != len(ordered):
        raise ValueError("duplicate session revisions cannot confirm rotation")
    previous = None
    consecutive = 0
    confirmed = None
    rows = []
    for observation in ordered:
        current = observation.get("value", {}).get("state") if observation.get("status") == "READY" else None
        contiguous = previous is not None and str(calendar.next_session(previous["session"]).date()) == observation["session"]
        compatible = previous is not None and previous.get("series_key") == observation.get("series_key")
        reset = not contiguous or not compatible or observation.get("source_changed", False) or current is None or current == "TRANSITION"
        if reset:
            consecutive, confirmed = 0, None
        prior_state = previous.get("value", {}).get("state") if previous and previous.get("value") else None
        consecutive = (consecutive + 1 if prior_state == current and not reset else 1) if current and current != "TRANSITION" else 0
        changed_from = None
        if consecutive >= 2:
            changed_from = confirmed if confirmed != current else None
            confirmed = current
        rows.append(dict(observation, persistence=dict(observations=consecutive,
            status="CONFIRMED" if current and confirmed == current and consecutive >= 2 else "PROVISIONAL" if current and current != "TRANSITION" else "UNAVAILABLE" if not current else "TRANSITION",
            confirmed_state=confirmed, changed_from=changed_from,
            coverage="BOUNDED_RECONSTRUCTED_HISTORY_NOT_ORIGINAL_OBSERVATIONS")))
        previous = observation
    return rows


def divergence_annotation(stock, sector, spy, qqq, horizon=5):
    if horizon not in (1, 5, 20):
        raise ValueError("unsupported divergence horizon")
    contexts = (stock, sector, spy, qqq)
    result = dict(status="UNAVAILABLE", reason_codes=["DIVERGENCE_INPUT_UNAVAILABLE"], value=None, horizon_sessions=horizon)
    if any(item is None or item.get("status") != "READY" for item in contexts):
        return result
    if len({(item["session"], utc(item["market_time"]), item.get("price_basis")) for item in contexts}) != 1:
        return result | dict(reason_codes=["INCOMPARABLE_PRICE_WINDOWS"])
    stock_return, sector_return, spy_return, qqq_return = [item["value"].get(f"return{horizon}") for item in contexts]
    if any(not isinstance(value, (int, float)) or not math.isfinite(value) for value in (stock_return, sector_return, spy_return, qqq_return)):
        return result
    market_down = spy_return < 0 and qqq_return < 0
    market_up = spy_return > 0 and qqq_return > 0
    labels = []
    if market_down and sector_return > 0 and stock_return > 0:
        labels.append("COUNTER_MARKET_SECTOR_LEADERSHIP")
    elif market_down and sector_return < 0 and stock_return > 0:
        labels.append("STOCK_SPECIFIC_STRENGTH")
    elif market_down and sector_return < 0 and stock_return < 0 and stock_return > max(sector_return, spy_return, qqq_return):
        labels.append("RELATIVE_RESILIENCE_NOT_RISING")
    if market_up and sector_return < 0:
        labels.append("STOCK_VS_SECTOR_DIVERGENCE" if stock_return > 0 else "SECTOR_WEAKNESS_IN_RISING_MARKET" if stock_return < 0 else "STOCK_FLAT_WEAK_SECTOR")
    if market_up and sector_return > 0 and stock_return > 0 and stock_return < min(sector_return, spy_return, qqq_return):
        labels.append("RISING_RELATIVE_LAGGARD")
    if not market_up and not market_down:
        labels.append("MIXED_BENCHMARK_MOVEMENT")
    return result | dict(status="READY", reason_codes=[], value=dict(labels=labels or ["NO_NAMED_DIVERGENCE"],
        stock_return=stock_return, sector_return=sector_return, spy_return=spy_return, qqq_return=qqq_return,
        stock_minus_sector=stock_return - sector_return, sector_minus_spy=sector_return - spy_return),
        source_revision_ids=sorted({key for item in contexts for key in item["source_revision_ids"]}),
        available_at=max(item["available_at"] for item in contexts))


def tracked_breadth(members, session):
    stocks = [row for row in members if row["security_type"] == "CS"]
    ready = [row for row in stocks if row["context"]["status"] == "READY" and row["context"].get("session") == session
        and all(isinstance(row["context"]["value"].get(field), (int, float)) and math.isfinite(row["context"]["value"][field])
            for field in ("close", "sma50", "sma200", "return1"))]
    result = observation("INSUFFICIENT_HISTORY", "NO_ELIGIBLE_COMMON_STOCK_HISTORY", session=session,
        expected_observations=len(stocks), timely_observations=len(ready),
        coverage_fraction=len(ready) / len(stocks) if stocks else 0,
        unknown_security_types=sum(row["security_type"] is None for row in members),
        population="CURRENT_ENROLLED_COMMON_STOCKS_NOT_FULL_MARKET")
    if not ready:
        return result
    values = [row["context"]["value"] for row in ready]
    advancing = sum(row["return1"] > 0 for row in values)
    declining = sum(row["return1"] < 0 for row in values)
    contexts = [row["context"] for row in ready]
    return result | dict(status="READY" if len(ready) == len(stocks) else "PARTIAL",
        reason_codes=[] if len(ready) == len(stocks) else ["INCOMPLETE_TRACKED_STOCK_COVERAGE"],
        value=dict(advancing=advancing, declining=declining, unchanged=len(ready) - advancing - declining,
            above_sma50_fraction=sum(row["close"] > row["sma50"] for row in values) / len(ready),
            above_sma200_fraction=sum(row["close"] > row["sma200"] for row in values) / len(ready)),
        market_time=max(row["market_time"] for row in contexts),
        available_at=max(row["available_at"] for row in contexts),
        source_revision_ids=sorted({revision for row in ready for revision in
            row["context"]["source_revision_ids"] + ([row["reference_id"]] if row.get("reference_id") else [])}))


def build_rotation_snapshot(members, facts, cutoff):
    calendar = exchange_calendars.get_calendar("XNYS")
    cutoff = utc(cutoff)
    session = calendar.date_to_session(cutoff.date(), direction="previous")
    if calendar.session_close(session).to_pydatetime() > cutoff:
        session = calendar.previous_session(session)
    dates = [str(item.date()) for item in calendar.sessions_in_range(calendar.session_offset(session, 1 - HISTORY_SESSIONS), session)]
    by_security, references = defaultdict(list), defaultdict(list)
    for row in facts["bars"]:
        by_security[row["security_id"]].append(row)
    for row in facts["references"]:
        references[row["ticker"]].append(row)
    cache, lineage = {}, {}
    def reference(ticker):
        rows = [row for row in references[ticker] if max(utc(row["effective_from"]), utc(row["observed_at"]), utc(row["created_at"])) <= cutoff]
        latest = max(rows, key=lambda row: (utc(row["effective_from"]), utc(row["observed_at"]), str(row["revision_id"])), default=None)
        return latest if latest and latest["active"] else None
    def context(ticker, target, identity=None):
        ref = reference(ticker)
        identity = identity or (ref["security_id"] if ref else None)
        if not ref or ref["security_id"] != identity:
            return observation("UNAVAILABLE", "SECURITY_REFERENCE_UNAVAILABLE", session=target)
        key = (identity, target)
        if key not in cache:
            cache[key] = daily_price_context(by_security[identity], identity, target, cutoff, facts["actions"])
        return cache[key]
    def compact(context_value):
        from research.stock_idea_engine import digest
        result = dict(context_value)
        ids = result.pop("source_revision_ids", [])
        ref = digest(ids)
        lineage[ref] = ids
        result["lineage_ref"] = ref
        return result
    benchmarks = {ticker: context(ticker, dates[-1]) for ticker in ("SPY", "QQQ")}
    sectors = []
    for name, ticker in SECTOR_BENCHMARK_ETF.items():
        history = []
        for target in dates:
            value = relative_rotation(context(ticker, target), context("SPY", target))
            value.update(session=target, series_key=ticker + ":SPY:RAW_ACTION_GATED")
            history.append(value)
        history = summarize_rotation_history(history)
        sectors.append(dict(ticker=ticker, sector=name, context=compact(context(ticker, dates[-1])),
            rotation=compact(history[-1]), history=[compact(row) for row in history]))
    stocks, breadth_members = [], []
    for member in members:
        identity, ticker = member["security_id"], member["ticker"]
        ref = dated_reference(references[ticker], identity, ticker, cutoff)
        sector = sector_for_sic(ref.get("sic_code")) if ref and ref["security_type"] == "CS" else None
        proxy = SECTOR_BENCHMARK_ETF.get(sector)
        stock = context(ticker, dates[-1], identity)
        proxy_context = context(proxy, dates[-1]) if proxy else None
        rotation = relative_rotation(stock, proxy_context) if proxy_context else observation(
            "NOT_APPLICABLE" if ref and ref["security_type"] != "CS" else "UNAVAILABLE", "NO_DATED_STOCK_SECTOR_PROXY")
        breadth_members.append(dict(security_type=ref["security_type"] if ref else None, context=stock,
            reference_id=ref["revision_id"] if ref else None))
        stocks.append(dict(security_id=identity, ticker=ticker, security_type=ref["security_type"] if ref else None,
            reference_id=ref["revision_id"] if ref else None, sector=sector, proxy=proxy,
            context=compact(stock), relative=compact(rotation),
            divergence=compact(divergence_annotation(stock, proxy_context, benchmarks["SPY"], benchmarks["QQQ"]))))
    volatility = benchmarks["SPY"]
    volatility = dict(volatility, value=dict(annualized_volatility20=volatility["value"]["realized_volatility20"],
        return_basis="SIMPLE_DAILY_RETURNS_SAMPLE_STD_20_SQRT_252")) if volatility["status"] == "READY" else volatility
    additional = dict(tracked_breadth=tracked_breadth(breadth_members, dates[-1]), realized_volatility=volatility,
        vix=observation("NEEDS_SOURCE", "FRED_VIXCLS_NOT_CAPTURED"),
        credit=observation("NEEDS_SOURCE", "FRED_HIGH_YIELD_OAS_NOT_CAPTURED"),
        intraday_volume=observation("NOT_IMPLEMENTED", "COMPARABLE_INTRADAY_VOLUME_NOT_BUILT"),
        sentiment=observation("NOT_IMPLEMENTED", "COMPOSITE_SPECIFICATION_NOT_ACTIVATED"),
        weights=observation("NOT_IMPLEMENTED", "WEIGHT_CONFIGURATION_NOT_ACTIVATED"),
        sector_options=observation("NEEDS_COVERAGE", "SECTOR_ETF_OPTIONS_COHORT_NOT_VALIDATED"),
        etf_flows=observation("NEEDS_SOURCE", "DATED_NAV_AND_SHARES_OUTSTANDING_REQUIRED"))
    additional.update(facts.get("macro_context", {}))
    additional["sentiment"], additional["weights"] = development_conditions_score(
        benchmarks["SPY"], benchmarks["QQQ"], additional["tracked_breadth"], additional["vix"], additional["credit"], dates[-1], cutoff)
    score_components = additional["sentiment"].pop("components")
    option_rows = {row["underlying"]: row for row in facts.get("sector_option_activity", [])}
    option_activity = {ticker: sector_option_activity_context(option_rows.get(ticker), cutoff,
        facts.get("volume_boundary", cutoff), facts.get("sector_option_error"))
        for ticker in sorted({"SPY", "QQQ"} | set(SECTOR_BENCHMARK_ETF.values()))}
    etf_creations = {ticker: etf_creation_context(facts.get("etf_records", []), ticker, dates[-1], cutoff, reference(ticker))
        for ticker in sorted({"SPY", "QQQ"} | set(SECTOR_BENCHMARK_ETF.values()))}
    ready_flows = [value for value in etf_creations.values() if value["status"] == "READY"]
    additional["etf_flows"] = observation("PARTIAL" if ready_flows else "NEEDS_SOURCE",
        "ETF_CREATION_RECORD_COVERAGE_GAPS" if ready_flows else facts.get("etf_records_error") or "DATED_NAV_AND_SHARES_OUTSTANDING_REQUIRED",
        expected_observations=len(etf_creations), timely_observations=len(ready_flows))
    if ready_flows:
        additional["etf_flows"].update(status="READY" if len(ready_flows) == len(etf_creations) else "PARTIAL",
            value=dict(covered_proxies=len(ready_flows), expected_proxies=len(etf_creations)),
            source_revision_ids=sorted({revision for value in ready_flows for revision in value["source_revision_ids"]}),
            market_time=min(value["market_time"] for value in ready_flows), available_at=max(value["available_at"] for value in ready_flows))
    sector_activity = [option_activity[ticker] for ticker in SECTOR_BENCHMARK_ETF.values()]
    usable = [value for value in sector_activity if value["status"] in ("READY", "PARTIAL")]
    if usable:
        additional["sector_options"].update(status="READY" if len(usable) == len(sector_activity) and all(value["status"] == "READY" for value in usable) else "PARTIAL",
            reason_codes=[] if len(usable) == len(sector_activity) else ["SECTOR_OPTION_COVERAGE_GAPS"],
            value=dict(covered_sectors=len(usable), expected_sectors=len(sector_activity)),
            source_revision_ids=sorted({revision for value in usable for revision in value["source_revision_ids"]}),
            market_time=min(value["market_time"] for value in usable), available_at=max(value["available_at"] for value in usable))
    else:
        additional["sector_options"]["reason_codes"] = [facts.get("sector_option_error") or "NO_TIMELY_SECTOR_ETF_OPTION_MATRICES"]
    bond_contexts = []
    for ticker in ("HYG", "LQD"):
        ref = reference(ticker)
        identity = ref["security_id"] if ref else None
        value = daily_price_context(by_security[identity], identity, dates[-1], cutoff, facts["actions"], expected_ticker=ticker) if identity else observation("UNAVAILABLE", "SECURITY_REFERENCE_UNAVAILABLE")
        bond_contexts.append(dict(value, ticker=ticker, security_id=identity))
    additional["bond_etf_relative_performance"] = bond_etf_relative_performance(*bond_contexts, facts["references"], cutoff)
    volumes = {}
    if facts.get("volume_boundary"):
        for ticker in sorted({"SPY", "QQQ"} | set(SECTOR_BENCHMARK_ETF.values())):
            ref = reference(ticker)
            volumes[ticker] = same_time_volume_context(facts.get("volume_bars", []), ref["security_id"],
                facts["volume_boundary"], cutoff, facts["actions"]) if ref else observation("UNAVAILABLE", "SECURITY_REFERENCE_UNAVAILABLE")
        ready = [value for value in volumes.values() if value["status"] == "READY"]
        additional["intraday_volume"] = observation("READY" if len(ready) == len(volumes) else "PARTIAL" if ready else "INSUFFICIENT_HISTORY",
            None if len(ready) == len(volumes) else "COMPARABLE_VOLUME_COVERAGE_GAPS", expected_observations=len(volumes), timely_observations=len(ready))
        if ready:
            additional["intraday_volume"].update(value=dict(ready_proxies=len(ready), expected_proxies=len(volumes)),
                source_revision_ids=sorted({revision for value in ready for revision in value["source_revision_ids"]}),
                market_time=facts["volume_boundary"], available_at=max(value["available_at"] for value in ready))
    return dict(schema=SCHEMA, calculation_version=VERSION, as_of=cutoff.isoformat(), session=dates[-1],
        history_sessions=dates, capture_mode="RECONSTRUCTED_AS_OF_CAPTURE", price_basis="RAW_ACTION_GATED",
        mapping_basis="SIC_DERIVED_PROXY_NOT_LICENSED_GICS", refresh_mode="ONE_SHOT_CAPTURE",
        market=compact(market_context(benchmarks["SPY"], benchmarks["QQQ"])),
        benchmarks={ticker: compact(value) for ticker, value in benchmarks.items()}, sectors=sectors, stocks=stocks,
        additional_context={key: compact(value) for key, value in additional.items()},
        score_components={key: compact(value) for key, value in score_components.items()},
        sector_option_activity={ticker: compact(value) for ticker, value in option_activity.items()},
        etf_creations={ticker: compact(value) for ticker, value in etf_creations.items()},
        intraday_volumes={ticker: compact(value) for ticker, value in volumes.items()},
        coverage=dict(expected_sectors=len(sectors), ready_sectors=sum(row["rotation"]["status"] == "READY" for row in sectors),
            expected_stocks=len(stocks), stock_states=dict(Counter(row["relative"]["status"] for row in stocks))),
        lineage=lineage, warnings=["Price leadership, not fund flows or expected returns",
            "Five-session trails are reconstructed at capture time; persistence is bounded to that history",
            "Raw prices are action-gated, not total returns; ETF distributions can affect comparisons",
            "Annotations only; no alert selection or probability model"])


def verify_rotation_snapshot(snapshot):
    from research.stock_idea_engine import digest
    if snapshot.get("schema") != SCHEMA or snapshot.get("calculation_version") != VERSION:
        raise ValueError("incompatible rotation snapshot")
    if snapshot.get("snapshot_sha256") != digest({key: value for key, value in snapshot.items() if key != "snapshot_sha256"}):
        raise ValueError("rotation snapshot hash mismatch")
    cutoff = utc(snapshot["as_of"])
    if snapshot["database_snapshot"]["read_only"] != "on" or snapshot["database_snapshot"]["isolation"] != "repeatable read":
        raise ValueError("rotation capture must use a read-only consistent snapshot")
    if not snapshot.get("original_publications_unchanged"):
        raise ValueError("source publications changed during capture")
    if sorted(row["ticker"] for row in snapshot["sectors"]) != sorted(SECTOR_BENCHMARK_ETF.values()):
        raise ValueError("rotation proxy population differs")
    if len({row["security_id"] for row in snapshot["stocks"]}) != len(snapshot["stocks"]):
        raise ValueError("duplicate stock identity")
    lineage = snapshot["lineage"]
    if snapshot.get("lineage_encoding") == "CATALOG_INDEX_V1":
        catalog = snapshot["source_revision_catalog"]
        if len(catalog) != len(set(catalog)) or any(type(index) is not int or not 0 <= index < len(catalog) for ids in lineage.values() for index in ids):
            raise ValueError("rotation revision catalog is invalid")
        lineage = {ref: [catalog[index] for index in ids] for ref, ids in lineage.items()}
    if any(digest(ids) != ref for ref, ids in lineage.items()):
        raise ValueError("rotation lineage hash mismatch")
    components = snapshot.get("score_components")
    if components:
        if set(components) != set(CONDITIONS_WEIGHTS) or snapshot["additional_context"]["weights"]["value"] != CONDITIONS_WEIGHTS:
            raise ValueError("development score weights differ from fixed policy")
        for name, component in components.items():
            if component["weight"] != CONDITIONS_WEIGHTS[name]:
                raise ValueError("development component weight mismatch")
            if component["value"] is not None:
                score = component["value"]["score"]
                if not math.isfinite(score) or not 0 <= score <= 100 or not math.isclose(component["value"]["contribution"], CONDITIONS_WEIGHTS[name] * (score - 50), abs_tol=1e-10):
                    raise ValueError("development component contribution does not reconcile")
        composite = snapshot["additional_context"]["sentiment"]
        if composite["value"] is not None:
            if not all(component["status"] in ("READY", "PARTIAL") for component in components.values()) or not math.isclose(
                    composite["value"]["score"], 50 + sum(component["value"]["contribution"] for component in components.values()), abs_tol=1e-10):
                raise ValueError("development composite does not reconcile")
            if any(component["status"] == "PARTIAL" for component in components.values()) and composite["status"] != "PARTIAL":
                raise ValueError("development composite conceals partial coverage")
    factors = [snapshot["market"], *snapshot["benchmarks"].values(), *snapshot.get("additional_context", {}).values(),
        *snapshot.get("score_components", {}).values(), *snapshot.get("sector_option_activity", {}).values(),
        *snapshot.get("etf_creations", {}).values(), *snapshot.get("intraday_volumes", {}).values()]
    for row in snapshot["sectors"]:
        factors.extend([row["context"], row["rotation"], *row["history"]])
    for row in snapshot["stocks"]:
        factors.extend(row[field] for field in ("context", "relative", "divergence"))
    for factor in factors:
        if factor["lineage_ref"] not in lineage:
            raise ValueError("rotation lineage missing")
        for field in ("market_time", "observed_at", "created_at", "available_at"):
            if factor.get(field) is not None and utc(factor[field]) > cutoff:
                raise ValueError("rotation fact exceeds capture cutoff")
        if factor["status"] in ("READY", "PARTIAL") and (factor["value"] is None or not lineage[factor["lineage_ref"]]):
            raise ValueError("ready rotation fact lacks evidence")
    return dict(status="VERIFIED", fact_records=len(factors), source_lineage_sets=len(lineage),
        original_publications_unchanged=True, coverage=snapshot["coverage"])


def compact_rotation_lineage(snapshot):
    from research.stock_idea_engine import digest
    verify_rotation_snapshot(snapshot)
    compacted = {key: value for key, value in snapshot.items() if key != "snapshot_sha256"}
    if snapshot.get("lineage_encoding") != "CATALOG_INDEX_V1":
        catalog = sorted({revision for ids in snapshot["lineage"].values() for revision in ids})
        positions = {revision: index for index, revision in enumerate(catalog)}
        compacted.update(lineage_encoding="CATALOG_INDEX_V1", source_revision_catalog=catalog,
            lineage={ref: [positions[revision] for revision in ids] for ref, ids in snapshot["lineage"].items()})
    sectors = []
    for sector in snapshot["sectors"]:
        history = summarize_rotation_history(sector["history"])
        sectors.append(dict(sector, history=history, rotation=history[-1]))
    compacted.update(sectors=sectors, source_snapshot_sha256=snapshot["snapshot_sha256"],
        persistence_version="CONSECUTIVE_TWO_OBSERVATIONS_V1")
    compacted["snapshot_sha256"] = digest(compacted)
    verify_rotation_snapshot(compacted)
    return compacted


@lru_cache(maxsize=2)
def _read_rotation_snapshot(path, modified_ns, size):
    if size > 20_000_000:
        raise ValueError("rotation snapshot exceeds bounded reader size")
    snapshot = json.loads(Path(path).read_text(encoding="utf-8"))
    verify_rotation_snapshot(snapshot)
    return snapshot


def load_rotation_page(path, expected_session, now):
    path = Path(path)
    try:
        stat = path.stat()
    except FileNotFoundError:
        return dict(status="AWAITING_FIRST_SNAPSHOT", sectors=[], stocks=[], benchmarks={})
    snapshot = _read_rotation_snapshot(str(path.resolve()), stat.st_mtime_ns, stat.st_size)
    age = (utc(now) - utc(snapshot["as_of"])).total_seconds()
    if age < 0:
        raise ValueError("rotation snapshot is from a future capture")
    stale = snapshot["session"] < expected_session
    fields = ("schema", "calculation_version", "session", "as_of", "generated_at", "history_sessions", "capture_mode", "price_basis",
        "mapping_basis", "refresh_mode", "market", "benchmarks", "sectors", "stocks", "coverage", "warnings", "snapshot_sha256", "additional_context", "intraday_volumes", "score_components", "sector_option_activity", "etf_creations")
    result = dict({field: snapshot.get(field) for field in fields}, status="STALE" if stale else "AVAILABLE",
        stale=stale, capture_age_seconds=age, expected_session=expected_session)
    from equity.api import expected_materialized_market_time
    volume_boundary = expected_materialized_market_time(utc(now), "30m")
    def freshness(factor, intraday=False):
        if factor.get("market_time") and factor["status"] in ("READY", "PARTIAL") and (
                utc(factor["market_time"]) < volume_boundary if intraday else utc(factor["market_time"]).date().isoformat() < expected_session):
            return dict(factor, status="STALE", captured_status=factor["status"], reason_codes=factor["reason_codes"] + ["STORED_CONTEXT_BEHIND_EXPECTED_WINDOW"])
        return factor
    result["additional_context"] = {key: freshness(factor, key in ("intraday_volume", "sector_options")) for key, factor in (snapshot.get("additional_context") or {}).items()}
    result["score_components"] = {key: freshness(factor) for key, factor in (snapshot.get("score_components") or {}).items()}
    result["sector_option_activity"] = {key: freshness(factor, True) for key, factor in (snapshot.get("sector_option_activity") or {}).items()}
    result["etf_creations"] = {key: freshness(factor) for key, factor in (snapshot.get("etf_creations") or {}).items()}
    result["intraday_volumes"] = {key: freshness(factor, True) for key, factor in (snapshot.get("intraday_volumes") or {}).items()}
    try:
        result["bond_comparison"] = load_bond_comparison_view(path.parent / "bond-comparison.json", now)
    except (ValueError, OSError, KeyError, TypeError):
        result["bond_comparison"] = dict(status="UNAVAILABLE", reason_codes=["FROZEN_COMPARISON_REPORT_INVALID"])
    return result


def rotation_refresh_due(snapshot, expected_session, expected_boundary, now, *, macro_configured=False, macro_states=None):
    if not snapshot or "additional_context" not in snapshot:
        return True
    age = (utc(now) - utc(snapshot["as_of"])).total_seconds()
    if age < 300:
        return False
    for name, state in (macro_states or {}).items():
        previous = snapshot["additional_context"].get(name, {}).get("status")
        previous = previous if previous in ("NEEDS_CONFIGURATION", "NEEDS_APPROVAL") else "CONFIGURED"
        if previous != state:
            return True
    if snapshot["session"] != expected_session or snapshot["market"]["status"] != "READY":
        return True
    for ticker in ("SPY", "QQQ"):
        volume = snapshot.get("intraday_volumes", {}).get(ticker, {})
        if volume.get("boundary") != utc(expected_boundary).isoformat() or "EXACT_CUMULATIVE_VOLUME_WINDOW_MISSING" in volume.get("reason_codes", []):
            return True
    return macro_configured and age >= 4 * 3600


SHARED_ALERT_FACTORS = ("spy", "qqq", "sector_rotation", "stock_relative_rotation", "stock_divergence",
    "tracked_breadth", "spy_realized_volatility", "vix", "credit", "conditions_score", "market_volume", "sector_volume")


def archived_rotation_context(directory, cutoff, session, universe_sha256):
    cutoff = utc(cutoff)
    latest_name = cutoff.strftime("%Y%m%dT%H%M%S%fZ") + ".json"
    candidates = sorted((path for path in (Path(directory) / "captures").glob("*.json")
        if path.name <= latest_name and path.name[:8].isdigit()), reverse=True)[:8]
    for path in candidates:
        try:
            stat = path.stat()
            snapshot = _read_rotation_snapshot(str(path.resolve()), stat.st_mtime_ns, stat.st_size)
            available = max(utc(snapshot[field]) for field in ("as_of", "generated_at", "projected_at"))
            if available <= cutoff and snapshot["session"] == session and snapshot.get("universe_sha256") == universe_sha256:
                return snapshot
        except (OSError, ValueError, KeyError, TypeError):
            continue
    return None


def shared_alert_context(snapshot, member, cutoff, boundary):
    missing = {key: observation("NOT_COVERED", "NO_ELIGIBLE_ARCHIVED_MARKET_SNAPSHOT") for key in SHARED_ALERT_FACTORS}
    if snapshot is None:
        return missing
    cutoff, boundary = utc(cutoff), utc(boundary)
    available = max(utc(snapshot[field]) for field in ("as_of", "generated_at", "projected_at"))
    if available > cutoff:
        return missing
    stock = next((row for row in snapshot["stocks"] if row["security_id"] == member["security_id"]
        and row["ticker"] == member["ticker"] and row["reference_id"] == member.get("reference_id")), None)
    sector = next((row for row in snapshot["sectors"] if stock and row["ticker"] == stock.get("proxy")), None)
    def factor(value, *, intraday=False):
        if value is None:
            return observation("UNAVAILABLE", "ARCHIVED_MEMBER_OR_PROXY_UNAVAILABLE")
        result = observation(value["status"])
        result.update({key: item for key, item in value.items() if key not in ("source_revision_ids", "lineage_ref")})
        source_time = value.get("market_time") or snapshot["benchmarks"]["SPY"].get("market_time")
        result.update(source_revision_ids=[snapshot["snapshot_sha256"]], market_time=source_time,
            available_at=max(available, utc(value["available_at"]) if value.get("available_at") else available).isoformat(),
            source_snapshot_sha256=snapshot["snapshot_sha256"], source_lineage_ref=value.get("lineage_ref"),
            source_snapshot_session=snapshot["session"], source_snapshot_captured_at=snapshot["as_of"])
        if result["available_at"] and utc(result["available_at"]) > cutoff:
            return observation("UNAVAILABLE", "ARCHIVED_FACT_AFTER_CUTOFF")
        if intraday and result["status"] in ("READY", "PARTIAL") and (not source_time or utc(source_time) < boundary):
            result.update(status="STALE", reason_codes=result.get("reason_codes", []) + ["ARCHIVED_INTRADAY_WINDOW_BEHIND_ALERT"])
        return result
    result = {ticker.lower(): factor(snapshot["benchmarks"].get(ticker)) for ticker in ("SPY", "QQQ")}
    result.update(sector_rotation=factor(sector["rotation"] if sector else None),
        stock_relative_rotation=factor(stock["relative"] if stock else None), stock_divergence=factor(stock["divergence"] if stock else None))
    for target, original in (("tracked_breadth", "tracked_breadth"), ("spy_realized_volatility", "realized_volatility"),
            ("vix", "vix"), ("credit", "credit"), ("conditions_score", "sentiment")):
        result[target] = factor(snapshot.get("additional_context", {}).get(original))
    result["market_volume"] = factor(snapshot.get("intraday_volumes", {}).get("SPY"), intraday=True)
    result["sector_volume"] = factor(snapshot.get("intraday_volumes", {}).get(stock.get("proxy")) if stock else None, intraday=True)
    return result