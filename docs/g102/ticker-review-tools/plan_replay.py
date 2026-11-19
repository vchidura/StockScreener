"""Replays frontend buildTradePlan (tickerPlan.ts, c46f988) over served setups. Usage: plan_replay.py <dir>"""
import collections
import glob
import json
import os
import sys


def side_of(bias):
    return {"Bullish": "LONG", "Bearish": "SHORT"}.get(bias)


def build_plan(setup):
    side = side_of(setup["direction"]["bias"])
    if not side:
        return None, "neutral"
    entry = setup["last_close"]
    below = sorted([s for s in setup.get("stops") or [] if s["price"] < entry], key=lambda s: -s["price"])
    above = sorted([t for t in setup.get("targets") or [] if t["price"] > entry], key=lambda t: t["price"])
    atr = setup["technicals"]["atr"]
    short_stops = [l for l in above if l["source"] != "ATR"]
    short_targets = [l for l in below if l["source"] != "ATR"]
    if atr and atr > 0:
        short_stops.append({"level": "ATR Stop (1R)", "price": entry + atr, "source": "ATR"})
        if entry - 2 * atr > 0:
            short_targets.append({"level": "ATR Target (2R)", "price": entry - 2 * atr, "source": "ATR"})
    short_stops.sort(key=lambda l: l["price"])
    short_targets.sort(key=lambda l: -l["price"])
    stop = below[0] if side == "LONG" and below else (short_stops[0] if side == "SHORT" and short_stops else None)
    target = above[0] if side == "LONG" and above else (short_targets[0] if side == "SHORT" and short_targets else None)
    if not stop or not target:
        return None, "missing"
    risk = abs(entry - stop["price"])
    reward = abs(target["price"] - entry)
    if risk <= 0:
        return None, "zero-risk"
    return {
        "side": side, "entry": entry, "stop": stop, "target": target,
        "rr": reward / risk, "stopAtr": risk / atr if atr else None,
    }, "ok"


def main(folder):
    counts = collections.Counter()
    rows = []
    for path in sorted(glob.glob(os.path.join(folder, "*.json"))):
        name = os.path.basename(path)[:-5]
        if name in ("summary", "portal_current"):
            continue
        data = json.load(open(path))
        setups = (data.get("multi") or {}).get("setups") or {}
        for interval, setup in setups.items():
            if not setup:
                continue
            plan, why = build_plan(setup)
            bias = setup["direction"]["bias"]
            served_stops = [(s["level"], s["price"], s["source"]) for s in setup.get("stops") or []]
            served_targets = [(t["level"], t["price"], t["source"]) for t in setup.get("targets") or []]
            counts[(bias, why, plan["stop"]["source"] if plan else None, plan["target"]["source"] if plan else None)] += 1
            rows.append((name, interval, bias, setup["last_close"], setup["technicals"]["atr"], why,
                         plan and (plan["stop"]["level"], round(plan["stop"]["price"], 2), plan["target"]["level"],
                                   round(plan["target"]["price"], 2), round(plan["rr"], 4), round(plan["stopAtr"], 3)),
                         served_stops, served_targets))
    for key, value in sorted(counts.items(), key=lambda kv: -kv[1]):
        print(value, key)
    print()
    for row in rows:
        if row[2] == "Bearish":
            print(row[:7])
            print("   served stops  ", row[7])
            print("   served targets", row[8])
            break
    print()
    long_rows = [r for r in rows if r[2] == "Bullish" and r[6]]
    tiny = [r for r in long_rows if r[6][5] < 1]
    print("LONG plans", len(long_rows), "stop<1ATR (blocked 'Not tradeable')", len(tiny))
    under1 = [r for r in long_rows if r[6][4] < 1]
    print("LONG plans with rr<1 (blocked)", len(under1))
    short_rows = [r for r in rows if r[2] == "Bearish" and r[6]]
    print("SHORT plans", len(short_rows), "rr values", sorted({r[6][4] for r in short_rows}))
    print("SHORT plans with rr<2 by float error", sum(1 for r in short_rows if r[6][4] < 2))
    for r in long_rows[:6]:
        print(r[:7])


main(sys.argv[1])
