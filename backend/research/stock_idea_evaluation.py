"""Independent, predeclared evaluation of the stock idea publication policy."""
from __future__ import annotations

import math

import numpy as np

from research.stock_idea_engine import digest, read_candidate


def holm_adjust(p_values):
    values = [1. if value is None else float(value) for value in p_values]
    if any(not math.isfinite(value) or not 0 <= value <= 1 for value in values):
        raise ValueError("p-values must be finite and between zero and one")
    order = sorted(range(len(values)), key=lambda index: (values[index], index))
    adjusted, previous = [1.] * len(values), 0.
    for rank, index in enumerate(order):
        previous = max(previous, min(1., (len(values) - rank) * values[index]))
        adjusted[index] = previous
    return adjusted


def block_statistics(observations, sessions, block_sessions, plan):
    if block_sessions < 1 or len(set(sessions)) != len(sessions):
        raise ValueError("invalid calendar block schedule")
    ordinal = {session: index for index, session in enumerate(sessions)}
    if any(session not in ordinal or not math.isfinite(value) for session, value in observations):
        raise ValueError("observations must be finite and inside the declared calendar")
    complete_blocks = len(sessions) // block_sessions
    sums, counts = np.zeros(complete_blocks), np.zeros(complete_blocks, dtype=int)
    for session, value in observations:
        block = ordinal[session] // block_sessions
        if block < complete_blocks:
            sums[block] += value
            counts[block] += 1
    count = int(counts.sum())
    populated = int(np.count_nonzero(counts))
    mean = float(sums.sum() / count) if count else None
    result = dict(mean=float(np.mean([value for _, value in observations])) if observations else None,
        inference_mean=mean, observations=len(observations), inference_observations=count,
        block_sessions=block_sessions, complete_calendar_blocks=complete_blocks,
        populated_blocks=populated, trailing_partial_sessions=len(sessions) % block_sessions,
        interval95=None, raw_p=None, estimable=False, reason="INSUFFICIENT_BLOCKS")
    if populated < plan["minimum_populated_blocks"]:
        return result
    residual = sums[counts > 0] - mean * counts[counts > 0]
    if not np.isfinite(residual).all() or np.std(residual) <= 1e-15:
        return result | dict(reason="DEGENERATE_BLOCK_VARIANCE")
    generator = np.random.default_rng(plan["bootstrap_seed"])
    estimates, null_estimates = [], []
    centered = sums - mean * counts
    for _ in range(plan["bootstrap_replicates"]):
        selected = generator.integers(0, complete_blocks, size=complete_blocks)
        denominator = counts[selected].sum()
        if denominator:
            estimates.append(float(sums[selected].sum() / denominator))
            null_estimates.append(float(centered[selected].sum() / denominator))
    tail = (1 - plan["confidence_level"]) / 2
    interval = np.quantile(estimates, [tail, 1 - tail]).tolist()
    probability = (1 + sum(value >= mean for value in null_estimates)) / (len(null_estimates) + 1)
    return result | dict(interval95=interval, raw_p=probability, estimable=True, reason=None)


def validate_plan(plan, config):
    if (plan["inherit_previous_qualifications"] or plan["allow_production_publication"]
            or not plan["historical_dates_are_development"]
            or plan["signal_policy_version"] != config["policy_version"]
            or plan["random_seeds"] != config["random_seeds"]
            or plan["primary_cost_bps"] != config["primary_cost_bps"]):
        raise ValueError("evaluation must remain independent, research-only and consistent with the frozen signal policy")
    family_size = len(plan["models"]) * len(plan["trigger_intervals"]) * len(plan["directions"]) * len(plan["primary_claims"])
    if family_size != plan["primary_family_size"] or family_size != 72:
        raise ValueError("all 72 primary claims must remain in the testing family")
    if plan["multiple_testing"] != "HOLM_FWER" or plan["minimum_populated_blocks"] < 40:
        raise ValueError("the approved inference guardrails cannot be relaxed")


def audit_publications(publications, config):
    from collections import Counter, defaultdict
    from research.stock_idea_replay import publication_windows, utc
    expected = {boundary.isoformat(): deadline for _, boundary, deadline in publication_windows(config)}
    arms = {"ALL", "PRIORITY", "MOMENTUM"} | {f"RANDOM_{seed}" for seed in config["random_seeds"]}
    seen, problems, sources = set(), [], {}
    for publication in publications:
        key = (publication["arm"], publication["window_key"])
        if key in seen or publication["arm"] not in arms or publication["window_key"] not in expected:
            problems.append("DUPLICATE_OR_UNDECLARED_PUBLICATION")
            continue
        seen.add(key)
        if utc(publication["deadline"]) != expected[publication["window_key"]]:
            problems.append("PUBLICATION_DEADLINE_MISMATCH")
        rows = {row["episode_id"]: row for row in publication["dispositions"]}
        source = (tuple(sorted(rows)), tuple(sorted(publication["expected_members"])))
        if sources.setdefault(publication["window_key"], source) != source:
            problems.append("DIFFERENT_SOURCE_CANDIDATES_OR_COHORT_ACROSS_ARMS")
        selected = publication["selected"]
        if (len(selected) != len(set(selected)) or not set(selected) <= set(rows)
                or set(selected) != {episode_id for episode_id, row in rows.items() if row["selection"] == "SELECTED"}):
            problems.append("SELECTED_DISPOSITION_MISMATCH")
            continue
        by_model = defaultdict(list)
        for episode_id in selected:
            by_model[rows[episode_id]["model"]].append(rows[episode_id]["security_id"])
        if publication["arm"] != "ALL" and any(len(names) > 3 or len(set(names)) != len(names) for names in by_model.values()):
            problems.append("MODEL_QUOTA_OR_DISTINCT_STOCK_VIOLATION")
        displayed = [episode_id for item in publication["display"] for episode_id in item["episode_ids"]]
        if Counter(displayed) != Counter(selected):
            problems.append("DISPLAY_REFILL_OR_ATTRIBUTION_MISMATCH")
        for episode_id, position in publication["outcomes"].items():
            if episode_id not in selected:
                problems.append("UNSELECTED_POSITION_IN_ARM")
            if position.get("entry_at") and utc(position["entry_at"]) <= utc(publication["deadline"]):
                problems.append("ENTRY_NOT_AFTER_PUBLICATION")
            if position["state"] == "NO_FILL" and (position.get("gross") != 0 or position.get("cost_applies")):
                problems.append("NO_FILL_EXPOSURE_OR_COST")
    if seen != {(arm, window) for arm in arms for window in expected}:
        problems.append("MISSING_PUBLICATIONS")
    for arm in arms:
        selected = [episode_id for publication in publications if publication["arm"] == arm for episode_id in publication["selected"]]
        if len(selected) != len(set(selected)):
            problems.append("REPEATED_EPISODE_SELECTED")
    return dict(valid=not problems, problem_counts=dict(Counter(problems)),
                expected_publications=len(arms) * len(expected), actual_publications=len(publications))


def window_measure(publication, cell, config, plan, source_cutoff, *, stress=False, exclude_security=None):
    from research.stock_idea_replay import execution_times, utc
    cost = plan["primary_cost_bps"] / 10000
    selected = [row for row in publication["dispositions"] if row["selection"] == "SELECTED"
                and row["model"] == cell["model"] and row["direction"] == cell["direction"]
                and (cell["interval"] == "POOLED" or row["interval"] == cell["interval"])
                and row["security_id"] != exclude_security]
    values, mature, resolved, pending, unaccounted = [], 0, 0, 0, 0
    security_contributions = {}
    for row in selected:
        position = publication["outcomes"].get(row["episode_id"])
        if position is None:
            unaccounted += 1
            continue
        candidate = read_candidate(position["candidate"])
        _, maturity = execution_times(candidate, utc(publication["deadline"]), config)
        if maturity > source_cutoff:
            pending += 1
            continue
        mature += 1
        known = (position["state"] in ("CLOSED", "NO_FILL") and position.get("gross") is not None
                 and math.isfinite(position["gross"]))
        if known:
            value = position["gross"] - cost * bool(position["cost_applies"])
            resolved += 1
        elif stress:
            price = position.get("entry_price") or candidate.price
            risk = candidate.direction * (price - candidate.stop) / price if price and price > 0 else None
            if risk is None or not math.isfinite(risk) or risk <= 0:
                continue
            value = -2 * risk - cost
        else:
            continue
        values.append(value)
        security_contributions[row["security_id"]] = security_contributions.get(row["security_id"], 0.) + value
    complete = bool(selected) and not pending and not unaccounted and len(values) == mature
    return dict(value=float(np.mean(values)) if complete else None, selected=len(selected), mature=mature,
        resolved=resolved, pending=pending, unaccounted=unaccounted,
        contributions={security: value / len(selected) for security, value in security_contributions.items()} if complete else {})


def evaluate_publications(publications, config, plan, source_cutoff):
    from collections import defaultdict
    import exchange_calendars
    from research.stock_idea_replay import utc
    validate_plan(plan, config)
    cutoff = utc(source_cutoff)
    calendar = exchange_calendars.get_calendar(config["calendar"])
    sessions = [str(session.date()) for session in calendar.sessions_in_range(config["start"], config["end"])]
    ordinal = {session: index for index, session in enumerate(sessions)}
    audit = audit_publications(publications, config)
    indexed = {(publication["arm"], publication["window_key"]): publication for publication in publications}
    primary = sorted((publication for publication in publications if publication["arm"] == "PRIORITY"), key=lambda item: item["window_key"])
    claims, cells = [], []
    for model in plan["models"]:
        for interval in plan["trigger_intervals"]:
            block_size = plan["block_sessions"]["intraday"] if interval in ("30m", "1h") else plan["block_sessions"]["daily" if interval == "1d" else "pooled"][model]
            for direction in plan["directions"]:
                cell = dict(model=model, interval=interval, direction=direction)
                observations = {name: [] for name in plan["primary_claims"]}
                stressed = {name: [] for name in plan["primary_claims"]}
                coverage = defaultdict(lambda: dict(selected=0, mature=0, resolved=0, pending=0, unaccounted=0))
                contributions = defaultdict(float)
                window_cache = []
                required_arms = ["PRIORITY", "MOMENTUM"] + [f"RANDOM_{seed}" for seed in plan["random_seeds"]]
                for publication in primary:
                    measures, stress_measures = {}, {}
                    for arm in required_arms:
                        peer = indexed.get((arm, publication["window_key"]))
                        if peer is None:
                            continue
                        measure = window_measure(peer, cell, config, plan, cutoff)
                        measures[arm] = measure["value"]
                        stress_measures[arm] = window_measure(peer, cell, config, plan, cutoff, stress=True)["value"]
                        for name in coverage[arm]:
                            coverage[arm][name] += measure[name]
                        if arm == "PRIORITY":
                            for security, value in measure["contributions"].items():
                                contributions[security] += value
                    window_cache.append(publication)

                    def add_values(means, sink):
                        value = means.get("PRIORITY")
                        if value is None:
                            return
                        sink["NET_RETURN"].append((publication["session"], value))
                        if means.get("MOMENTUM") is not None:
                            sink["VS_MOMENTUM"].append((publication["session"], value - means["MOMENTUM"]))
                        random = [means.get(f"RANDOM_{seed}") for seed in plan["random_seeds"]]
                        if all(item is not None for item in random):
                            sink["VS_RANDOM_MEAN"].append((publication["session"], value - float(np.mean(random))))

                    add_values(measures, observations)
                    add_values(stress_measures, stressed)
                best_security = max(contributions, key=lambda security: (contributions[security], security)) if contributions else None
                without_security = {name: [] for name in plan["primary_claims"]}
                if best_security is not None:
                    for publication in window_cache:
                        means = {arm: window_measure(indexed[(arm, publication["window_key"])], cell, config, plan, cutoff,
                                 exclude_security=best_security)["value"] for arm in required_arms if (arm, publication["window_key"]) in indexed}
                        add_values(means, without_security)
                cell_claims = []
                for name in plan["primary_claims"]:
                    statistics = block_statistics(observations[name], sessions, block_size, plan)
                    totals = defaultdict(float)
                    for session, value in observations[name]:
                        totals[ordinal[session] // block_size] += value
                    best_block = max(totals, key=lambda block: (totals[block], -block)) if totals else None
                    remaining = [value for session, value in observations[name] if ordinal[session] // block_size != best_block]
                    risk_views = dict(missing_path_stress_mean=float(np.mean([value for _, value in stressed[name]])) if stressed[name] else None,
                        without_best_security_mean=float(np.mean([value for _, value in without_security[name]])) if without_security[name] else None,
                        without_best_block_mean=float(np.mean(remaining)) if remaining else None,
                        removed_security=best_security, removed_block=best_block)
                    sensitivity = [block_statistics(observations[name], sessions, block_size * multiplier, plan)
                                   for multiplier in plan["block_length_sensitivity_multipliers"]]
                    claim = dict(cell, claim=name, statistics=statistics, robustness=risk_views, block_sensitivity=sensitivity)
                    claims.append(claim)
                    cell_claims.append(len(claims) - 1)
                for arm, counts in coverage.items():
                    counts["mature_resolution_fraction"] = counts["resolved"] / counts["mature"] if counts["mature"] else None
                cells.append(dict(cell, claims=cell_claims, coverage=dict(coverage), windows=len(primary)))
    adjusted = holm_adjust([claim["statistics"]["raw_p"] for claim in claims])
    for claim, probability in zip(claims, adjusted):
        claim["holm_adjusted_p"] = probability
    for cell in cells:
        selected_claims = [claims[index] for index in cell["claims"]]
        coverage_ok = all(counts["mature_resolution_fraction"] is not None
                          and counts["mature_resolution_fraction"] >= plan["minimum_mature_outcome_coverage"]
                          and not counts["unaccounted"] for counts in cell["coverage"].values()) and bool(cell["coverage"])
        if not audit["valid"]:
            status = "INVALID_PUBLICATION_AUDIT"
        elif not coverage_ok:
            status = "INSUFFICIENT_OUTCOME_COVERAGE"
        elif not all(claim["statistics"]["estimable"] for claim in selected_claims):
            status = "INSUFFICIENT_INDEPENDENT_BLOCKS"
        elif not all(claim["statistics"]["inference_mean"] > 0 and claim["statistics"]["interval95"][0] > 0
                     and claim["holm_adjusted_p"] <= plan["family_alpha"] for claim in selected_claims):
            status = "NOT_STATISTICALLY_SUPPORTED"
        elif not all(claim["robustness"][name] is not None and claim["robustness"][name] > 0 for claim in selected_claims
                     for name in ("missing_path_stress_mean", "without_best_security_mean", "without_best_block_mean")):
            status = "ROBUSTNESS_NOT_SUPPORTED"
        elif any(view["estimable"] and view["interval95"][0] <= 0
                 for claim in selected_claims for view in claim["block_sensitivity"]):
            status = "BLOCK_LENGTH_SENSITIVE"
        else:
            status = "PASS_RESEARCH_ONLY"
        cell["status"] = status
    return dict(study_id=plan["study_id"], evaluation_version=plan["evaluation_version"], evaluation_plan_sha256=digest(plan),
        evidence_scope=plan["evidence_scope"], source_cutoff=source_cutoff, inherited_qualification_ids=[],
        publication_audit=audit, primary_family_size=len(claims), claims=claims, cells=cells,
        canonical_qualification_publication=False, live_activation_authorized=False,
        confidence_intervals="Marginal calendar-block intervals; primary claim decisions use full-family Holm adjusted p-values",
        interpretation="Sequential policy comparisons conditional on matched windows; not isolated detector alpha, not full-market evidence",
        discovery_status="CONTEXT_ONLY_NOT_A_TRADE_MODEL")