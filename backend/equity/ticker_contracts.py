"""Pure presentation contracts shared by canonical and realtime producers."""
import math


PATTERN_WATCH_INTERVALS = ("5m", "15m", "30m", "1h", "1d", "1wk", "1mo")


def setup_summary_row(row: dict) -> dict:
    payload = row["payload"] or {}
    technicals = payload.get("technicals") or {}
    momentum = payload.get("momentum") or {}
    direction = payload.get("direction") or {}
    alignment = payload.get("ema_alignment") or {}
    return {
        "ticker": row["ticker"],
        "trend": alignment.get("primary"),
        "trend_detail": alignment.get("primary_detail"),
        "confirm_trend": alignment.get("confirm"),
        "confirm_interval": alignment.get("confirm_interval"),
        "multi_tf_agree": alignment.get("multi_tf_agree"),
        "momentum": momentum.get("state"),
        "momentum_detail": momentum.get("detail"),
        "bias": direction.get("bias"),
        "conviction": direction.get("conviction"),
        "bull_signals": direction.get("bull_signals"),
        "bear_signals": direction.get("bear_signals"),
        "rsi": technicals.get("rsi"),
        "rsi_state": technicals.get("rsi_state"),
        "stoch_k": technicals.get("stoch_k"),
        "adx": technicals.get("adx"),
        "macd_state": technicals.get("macd_state"),
        "macd_histogram": technicals.get("macd_histogram"),
        "trend_consistency": technicals.get("trend_consistency"),
        "atr_pct": technicals.get("atr_pct"),
        "relative_volume": technicals.get("relative_volume"),
        "volume_trend_state": technicals.get("volume_trend_state"),
        "volume_pressure": technicals.get("volume_pressure"),
        "historical_volatility_pct": technicals.get("historical_volatility_pct"),
        "historical_volatility_state": technicals.get("historical_volatility_state"),
        "range_position_pct": technicals.get("range_position_pct"),
        "price_vs_vwap": technicals.get("price_vs_vwap"),
        "golden_cross": (payload.get("golden_cross") or {}).get("type"),
        "signals": payload.get("signals") or [],
    }


def sort_pattern_watch_rows(rows: list[dict], intervals=PATTERN_WATCH_INTERVALS) -> None:
    readiness_rank = {"AT_EDGE": 0, "NEAR_EDGE": 1, "FORMING": 2}
    grade_rank = {"STRONG_GEOMETRY": 0, "VALID_GEOMETRY": 1}
    rows.sort(key=lambda row: (
        readiness_rank[row["pattern"]["readiness"]],
        grade_rank[row["pattern"]["grade"]],
        row["pattern"]["edge_distance_atr"],
        -(row["pattern"]["upper_touches"] + row["pattern"]["lower_touches"]),
        intervals.index(row["interval"]),
    ))


def build_ticker_confluence_zones(setups: dict[str, dict]) -> list[dict]:
    daily = setups.get("1d") or next(iter(setups.values()), None)
    if not daily:
        return []
    current_price = float(daily["last_close"])
    daily_atr = float(daily["technicals"].get("atr") or 0)
    tolerance = max(current_price * 0.003, daily_atr * 0.25)
    references = []

    def family_of(source: str) -> str:
        value = source.lower()
        if "fib" in value:
            return "fibonacci"
        if "volume pivot" in value:
            return "volume_pivot"
        if "ema" in value or "sma" in value or "moving" in value:
            return "moving_average"
        if "gap" in value or "fvg" in value:
            return "gap_fvg"
        if "vwap" in value:
            return "vwap"
        return "price_action"

    def add_reference(interval: str, label: str, low: float, high: float,
                      source: str, family: str | None = None,
                      qualifier: str | None = None):
        if not all(math.isfinite(value) and value > 0 for value in (low, high)):
            return
        lo, hi = sorted((float(low), float(high)))
        references.append({
            "interval": interval, "label": label, "low": round(lo, 2),
            "high": round(hi, 2), "price": round((lo + hi) / 2, 2),
            "source": source, "family": family or family_of(source),
            "qualifier": qualifier,
        })

    for interval, setup in setups.items():
        technicals = setup["technicals"]
        for label, key, family in (
            ("EMA 8", "ema8", "moving_average"),
            ("EMA 21", "ema21", "moving_average"),
            ("EMA 50", "ema50", "moving_average"),
            ("SMA 50", "ma50", "moving_average"),
            ("SMA 100", "ma100", "moving_average"),
            ("SMA 200", "ma200", "moving_average"),
            ("VWAP 20", "vwap", "vwap"),
        ):
            value = technicals.get(key)
            if value is not None:
                add_reference(interval, label, value, value, label, family)
        for level in setup.get("targets", []) + setup.get("stops", []):
            label = level["level"]
            if level["source"] == "ATR":
                atr = float(technicals.get("atr") or 0)
                multiple = abs(float(level["price"]) - float(setup["last_close"])) / atr if atr > 0 else 0
                sign = "+" if float(level["price"]) >= float(setup["last_close"]) else "−"
                label = f"ATR {sign}{multiple:.0f}×"
            add_reference(interval, label, level["price"], level["price"], level["source"])
        for zone in setup.get("zones", []):
            add_reference(interval, zone["name"], zone["low"], zone["high"], zone["source"],
                          qualifier=zone.get("qualifier"))
        fibonacci = setup.get("strategy_results", {}).get("fibonacci")
        if fibonacci:
            for level in fibonacci.get("active_leg", {}).get("levels", []):
                add_reference(interval, f"Provisional Fib {level['name']}",
                              level["price"], level["price"], "Fibonacci provisional", "fibonacci")
            for level in fibonacci.get("target_levels", []):
                add_reference(interval, f"Confirmed Fib {level['name']}",
                              level["price"], level["price"], "Fibonacci confirmed", "fibonacci")

    unique = {}
    for reference in references:
        key = (reference["interval"], reference["family"], reference["label"],
               reference["low"], reference["high"])
        unique[key] = reference
    references = sorted(unique.values(), key=lambda item: item["price"])
    clusters = []
    for reference in references:
        candidates = [cluster for cluster in clusters
                      if abs(reference["price"] - cluster["center"]) <= tolerance]
        if not candidates:
            clusters.append({"low": reference["low"], "high": reference["high"],
                             "center": reference["price"], "references": [reference]})
            continue
        cluster = min(candidates, key=lambda item: abs(reference["price"] - item["center"]))
        cluster["low"] = min(cluster["low"], reference["low"])
        cluster["high"] = max(cluster["high"], reference["high"])
        cluster["references"].append(reference)
        cluster["center"] = sum(item["price"] for item in cluster["references"]) / len(cluster["references"])

    zones = []
    for cluster in clusters:
        low, high = cluster["low"], cluster["high"]
        midpoint = (low + high) / 2
        families = sorted({item["family"] for item in cluster["references"]})
        intervals = sorted({item["interval"] for item in cluster["references"]},
                           key=lambda value: {"1h": 0, "1d": 1, "1wk": 2}.get(value, 9))
        role = "ACTIVE" if low <= current_price <= high else "SUPPORT" if high < current_price else "RESISTANCE"
        strength = ("STRONG_CONFLUENCE" if len(intervals) >= 2 and len(families) >= 2
                    else "CONFLUENCE" if len(families) >= 2 or len(intervals) >= 2 else "SINGLE_REFERENCE")
        confirmations = []
        for interval, setup in setups.items():
            for pattern in setup.get("candlestick_patterns", []):
                if pattern["high"] >= low - tolerance and pattern["low"] <= high + tolerance:
                    confirmations.append({**pattern, "interval": interval})
        zones.append({
            "low": round(low, 2), "high": round(high, 2), "midpoint": round(midpoint, 2),
            "distance_pct": round((midpoint - current_price) / current_price * 100, 2),
            "role": role, "strength": strength, "intervals": intervals, "families": families,
            "references": cluster["references"], "confirmations": confirmations,
        })
    return sorted(zones, key=lambda zone: abs(zone["distance_pct"]))
