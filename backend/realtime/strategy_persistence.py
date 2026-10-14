"""Producer-side recurrence descriptors over exact stored scanner evaluations."""
from .contracts import canonical_sha256
from .dashboard_projection import DAILY_SCANNER_CONTRACTS
from equity.portal_scanners import HOURLY_SCANNER_CONTRACTS


def persistence_matches(contract, rows):
    contract = next((daily for daily, hourly in HOURLY_SCANNER_CONTRACTS.items() if hourly == contract), contract)
    if contract == "SCAN_MA_1D_9_21":
        return [row for row in rows if "Crossover" in row["signal"] or "Recent" in row["signal"]]
    if contract == "SCAN_FIBONACCI_1D_5":
        return [row for row in rows if row["signal"] != "Between Levels"]
    return rows


def _identity(contract, row):
    contract = next((daily for daily, hourly in HOURLY_SCANNER_CONTRACTS.items() if hourly == contract), contract)
    fields = ("gap_date", "gap_low", "gap_high") if contract == "SCAN_GAPS_1D" else (
        "gap_date", "fvg_low", "fvg_high") if contract == "SCAN_FVG_1D_50" else (
        "swing_high_date", "swing_low_date", "swing_high", "swing_low") if contract == "SCAN_FIBONACCI_1D_5" else (
        "crossover_date",) if contract == "SCAN_MA_1D_9_21" else ("ticker",)
    return canonical_sha256((contract, row["ticker"], [(field, row.get(field)) for field in fields]))


def _direction(row):
    if row.get("gap_direction"):
        return row["gap_direction"]
    if row.get("trend_direction"):
        return row["trend_direction"]
    text = row.get("signal", row.get("fvg_type", ""))
    if "Bull" in text:
        return "BULLISH"
    if "Bear" in text:
        return "BEARISH"
    return None


def _metric(row, keys):
    return next((row[key] for key in keys if isinstance(row.get(key), (int, float))), None)


def _distance(row):
    low, high = (row.get("gap_low"), row.get("gap_high")) if "gap_low" in row else (row.get("fvg_low"), row.get("fvg_high"))
    close = row.get("last_close")
    if all(isinstance(value, (int, float)) for value in (low, high, close)) and close > 0:
        lower, upper = sorted((low, high))
        return round(max(lower - close, close - upper, 0) / close * 100, 4)
    value = _metric(row, ("distance_pct", "dist_to_ema21_pct", "ma_spread_pct"))
    return abs(value) if value is not None else None


def build_strategy_persistence(history, tickers, sessions, *, interval="1d"):
    indexed = {item["session"]: item for item in history}
    results = []
    for ticker in tickers:
        for contract in (DAILY_SCANNER_CONTRACTS.values() if interval == "1d" else HOURLY_SCANNER_CONTRACTS.values()):
            cells, seen, run = [], False, 0
            for session in sessions:
                item = indexed.get(session)
                member = next((row for row in item["payload"]["analysis"][contract]["members"]
                               if row["ticker"] == ticker), None) if item else None
                native = [row for row in item["payload"]["scanners"][contract]["results"]
                          if row["ticker"] == ticker] if item else []
                matches = persistence_matches(contract, native)
                evaluated = member and member["status"] in {"COMPLETE", "COMPLETE_EMPTY"}
                state = ("MATCH" if matches else "NO_MATCH") if evaluated else member["status"] if member else "UNAVAILABLE"
                identities = sorted({_identity(contract, row) for row in matches})
                previous = cells[-1] if cells else None
                known_previous = previous and previous["state"] in {"MATCH", "NO_MATCH"}
                transition = "UNKNOWN"
                if evaluated and known_previous:
                    if matches:
                        transition = ("CONTINUING" if set(identities) & set(previous["event_ids"]) else "NEW") if previous["state"] == "MATCH" else "REAPPEARED" if seen else "NEW"
                    else:
                        transition = "DISAPPEARED" if previous["state"] == "MATCH" else "UNCHANGED"
                elif evaluated and previous is None:
                    transition = "FIRST_OBSERVED"
                run = run + 1 if matches and evaluated else 0
                directions = sorted({_direction(row) for row in matches if _direction(row)})
                row = matches[0] if matches else {}
                setup = next((value for value in item["payload"].get("setup_summary", {}).get("results", [])
                              if value["ticker"] == ticker), {}) if item else {}
                native_volume = _metric(row, ("volume_ratio", "relative_volume", "formation_relative_volume", "rel_vol"))
                manifest = item["source_manifest"] if item else {}
                cells.append(dict(session=session, state=state, reason=member.get("reason") if member else "EVALUATION_NOT_RETAINED",
                    transition=transition, consecutive_run=run, event_ids=identities, direction=directions,
                    direction_changed=bool(known_previous and previous["direction"] and directions and previous["direction"] != directions),
                    native_rows=native, evidence_ref=manifest.get("evidence_refs", {}).get(ticker, {}).get(contract),
                    input_sha256=manifest.get("source_windows", {}).get(ticker, {}).get("input_sha256"),
                    market_time=item["payload"]["market_time"] if item else None,
                    observed_at=item["payload"]["generated_at"] if item else None,
                    available_bars=member["available_bars"] if member else 0,
                    required_bars=member["required_bars"] if member else None,
                    distance=_distance(row),
                    magnitude=_metric(row, ("gap_pct", "swing_size_pct", "score", "price_change_since_cross_pct", "fvg_pct")),
                    maturity=_metric(row, ("days_since_cross", "streak_count", "gap_age_sessions")),
                    volume=native_volume if native_volume is not None else setup.get("relative_volume") if evaluated else None))
                seen = seen or bool(matches and evaluated)
            evaluated = sum(cell["state"] in {"MATCH", "NO_MATCH"} for cell in cells)
            results.append(dict(ticker=ticker, contract=contract, sessions=cells,
                matched=sum(cell["state"] == "MATCH" for cell in cells), evaluated=evaluated,
                coverage=f"{evaluated}/{len(sessions)}", consecutive_run=cells[-1]["consecutive_run"],
                transition=cells[-1]["transition"]))
    return dict(sessions=sessions, window_unit="XNYS_TRADING_SESSIONS", rows=results)