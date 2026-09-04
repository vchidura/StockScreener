#!/usr/bin/env python3
"""Verify persisted intraday scanner evidence against raw canonical bars.

Deliberately shares no code with research.intraday_scanners: every condition is
recomputed here from the canonical bar view. A detector bug and this verifier
agreeing would otherwise prove nothing.
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor


ORB = "INTRADAY_OPENING_RANGE_BREAKOUT_CONTINUATION"
ORB_FAIL = "INTRADAY_FAILED_OPENING_BREAKOUT_REVERSAL"
VWAP = "INTRADAY_VWAP_RECLAIM_REJECTION"
PULLBACK = "INTRADAY_TREND_PULLBACK"

BREAKOUT_BUFFER_ATR = 0.10
CLOSE_LOCATION_BULLISH = 0.65
CLOSE_LOCATION_BEARISH = 0.35
VOLUME_BASELINE_SESSIONS = 20
ATR_PERIOD = 14
TOLERANCE = 1e-6


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--tickers", type=int, default=10)
    result.add_argument("--output", type=Path)
    return result


def sampled_tickers(limit: int) -> list[str]:
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT ticker, count(*) AS n
            FROM equity_evidence
            WHERE interval = '30m'
              AND source_name LIKE 'INTRADAY%%'
            GROUP BY 1 ORDER BY md5(ticker)
            LIMIT %s
            """,
            (limit,),
        )
        return [row["ticker"] for row in cursor.fetchall()]


def load_bars(ticker: str) -> pd.DataFrame:
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT bar_revision_id, session_date, bar_start, bar_end,
                   open_price, high_price, low_price, close_price, volume, vwap
            FROM equity_canonical_bars
            WHERE ticker = %s AND interval = '30m'
            ORDER BY bar_start
            """,
            (ticker,),
        )
        frame = pd.DataFrame([dict(row) for row in cursor.fetchall()])
    if frame.empty:
        return frame
    for column in ("open_price", "high_price", "low_price", "close_price",
                   "volume", "vwap"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["bar_start"] = pd.to_datetime(frame["bar_start"], utc=True)
    frame["bar_end"] = pd.to_datetime(frame["bar_end"], utc=True)
    frame["slot"] = frame.groupby("session_date").cumcount()
    frame["session_bars"] = frame.groupby("session_date")["slot"].transform("size")
    return frame.reset_index(drop=True)


def load_events(ticker: str) -> list[dict]:
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT evidence_id, source_name, direction, market_time, payload
            FROM equity_evidence
            WHERE ticker = %s AND interval = '30m'
              AND source_name LIKE 'INTRADAY%%'
            ORDER BY market_time
            """,
            (ticker,),
        )
        return [dict(row) for row in cursor.fetchall()]


def true_range_atr(frame: pd.DataFrame) -> pd.Series:
    previous_close = frame["close_price"].shift(1)
    span = pd.concat([
        frame["high_price"] - frame["low_price"],
        (frame["high_price"] - previous_close).abs(),
        (frame["low_price"] - previous_close).abs(),
    ], axis=1).max(axis=1)
    return span.ewm(alpha=1.0 / ATR_PERIOD, adjust=False,
                    min_periods=ATR_PERIOD).mean()


def slot_volume_baseline(frame: pd.DataFrame) -> pd.Series:
    return frame.groupby("slot")["volume"].transform(
        lambda values: values.shift(1)
        .rolling(VOLUME_BASELINE_SESSIONS, min_periods=VOLUME_BASELINE_SESSIONS)
        .median()
    )


def session_vwap(frame: pd.DataFrame) -> pd.Series:
    poisoned = (frame["vwap"].isna() | frame["volume"].isna()).groupby(
        frame["session_date"]
    ).cummax()
    weighted = (frame["vwap"] * frame["volume"]).fillna(0.0)
    numerator = weighted.groupby(frame["session_date"]).cumsum()
    denominator = frame["volume"].fillna(0.0).groupby(frame["session_date"]).cumsum()
    return (numerator / denominator.replace(0, pd.NA)).mask(poisoned)


def verify_ticker(ticker: str, failures: dict[str, int], counts: dict[str, int]) -> None:
    bars = load_bars(ticker)
    events = load_events(ticker)
    if bars.empty or not events:
        return
    bars["atr"] = true_range_atr(bars)
    bars["baseline"] = slot_volume_baseline(bars)
    bars["session_vwap"] = session_vwap(bars)
    span = bars["high_price"] - bars["low_price"]
    bars["close_location"] = (
        (bars["close_price"] - bars["low_price"]) / span.where(span > 0)
    )
    by_bar_id = {row["bar_revision_id"]: index for index, row in bars.iterrows()}
    opening = bars[bars["slot"] == 0].set_index("session_date")
    episodes: dict[tuple, list[int]] = defaultdict(list)

    def fail(code: str) -> None:
        failures[code] = failures.get(code, 0) + 1

    for event in events:
        payload = event["payload"]
        source = event["source_name"]
        direction = event["direction"]
        counts[source] = counts.get(source, 0) + 1
        signal_bar_id = payload.get("signal_bar_id")
        index = by_bar_id.get(signal_bar_id)
        if index is None:
            fail(f"{source}:signal_bar_not_in_canonical_bars")
            continue
        row = bars.loc[index]
        signal_time = pd.Timestamp(event["market_time"])

        if row["bar_end"] != signal_time:
            fail(f"{source}:signal_time_is_not_signal_bar_end")
        if row["slot"] >= row["session_bars"] - 1:
            fail(f"{source}:signalled_on_final_session_bar")
        for bar_id in payload.get("source_bar_revision_ids", []):
            source_index = by_bar_id.get(bar_id)
            if source_index is None:
                fail(f"{source}:source_bar_missing")
            elif bars.loc[source_index, "bar_end"] > signal_time:
                fail(f"{source}:source_bar_after_signal")

        metadata = payload.get("metadata") or payload
        session = row["session_date"]

        if source in (ORB, ORB_FAIL):
            open_bar = opening.loc[session]
            if abs(float(metadata["opening_range_high"]) - float(open_bar["high_price"])) > TOLERANCE:
                fail(f"{source}:opening_range_high_mismatch")
            if abs(float(metadata["opening_range_low"]) - float(open_bar["low_price"])) > TOLERANCE:
                fail(f"{source}:opening_range_low_mismatch")

        if source == ORB:
            level = float(metadata["opening_range_high"] if direction == 1
                          else metadata["opening_range_low"])
            distance = direction * (float(row["close_price"]) - level)
            if distance <= 0:
                fail(f"{ORB}:close_not_beyond_opening_range")
            elif distance < BREAKOUT_BUFFER_ATR * float(row["atr"]) - TOLERANCE:
                fail(f"{ORB}:breakout_buffer_not_met")
            if not float(row["volume"]) > float(row["baseline"]):
                fail(f"{ORB}:relative_volume_not_met")
            episodes[(session, ORB, direction)].append(index)

        if source == ORB_FAIL:
            break_side = 1 if metadata["break_side"] == "upside" else -1
            level = float(metadata["opening_range_high"] if break_side == 1
                          else metadata["opening_range_low"])
            offset = break_side * (float(row["close_price"]) - level)
            if offset > 0:
                fail(f"{ORB_FAIL}:close_still_outside_range")
            elif offset == 0:
                fail(f"{ORB_FAIL}:close_exactly_at_level_boundary")
            if direction != -break_side:
                fail(f"{ORB_FAIL}:direction_does_not_oppose_break")
            earlier = bars[(bars["session_date"] == session)
                           & (bars["slot"] < row["slot"]) & (bars["slot"] > 0)]
            outside = (earlier["close_price"] > level) if break_side == 1 else (
                earlier["close_price"] < level)
            if not bool(outside.any()):
                fail(f"{ORB_FAIL}:no_prior_outside_close")
            episodes[(session, ORB_FAIL, break_side)].append(index)

        if source == VWAP:
            previous = bars.loc[index - 1]
            if previous["session_date"] != session:
                fail(f"{VWAP}:prior_bar_outside_session")
            else:
                current_vwap = float(row["session_vwap"])
                prior_vwap = float(previous["session_vwap"])
                crossed = (
                    float(row["close_price"]) > current_vwap
                    and float(previous["close_price"]) < prior_vwap
                ) if direction == 1 else (
                    float(row["close_price"]) < current_vwap
                    and float(previous["close_price"]) > prior_vwap
                )
                if not crossed:
                    fail(f"{VWAP}:no_vwap_cross")
                if abs(float(metadata["session_vwap"]) - current_vwap) > 1e-4:
                    fail(f"{VWAP}:session_vwap_mismatch")
            location = float(row["close_location"])
            if direction == 1 and location < CLOSE_LOCATION_BULLISH - TOLERANCE:
                fail(f"{VWAP}:close_location_not_met")
            if direction == -1 and location > CLOSE_LOCATION_BEARISH + TOLERANCE:
                fail(f"{VWAP}:close_location_not_met")
            if not float(row["volume"]) > float(row["baseline"]):
                fail(f"{VWAP}:relative_volume_not_met")
            episodes[(session, VWAP, direction)].append(index)

        if source == PULLBACK:
            pullback_ids = metadata.get("pullback_bar_ids", [])
            for bar_id in pullback_ids:
                pullback_index = by_bar_id.get(bar_id)
                if pullback_index is None:
                    fail(f"{PULLBACK}:pullback_bar_missing")
                elif not float(bars.loc[pullback_index, "volume"]) < float(
                    bars.loc[pullback_index, "baseline"]
                ):
                    fail(f"{PULLBACK}:pullback_volume_not_contracted")
            if metadata.get("ema_fast") is not None and metadata.get("ema_mid") is not None:
                stacked = (
                    float(metadata["ema_fast"]) > float(metadata["ema_mid"])
                    > float(metadata["ema_slow"])
                ) if direction == 1 else (
                    float(metadata["ema_fast"]) < float(metadata["ema_mid"])
                    < float(metadata["ema_slow"])
                )
                if not stacked:
                    fail(f"{PULLBACK}:ema_stack_not_aligned")

    for key, positions in sorted(episodes.items(), key=lambda item: str(item[0])):
        session, source, lane = key
        if len(positions) < 2:
            continue
        session_bars = bars[bars["session_date"] == session]
        for earlier, later in zip(positions, positions[1:]):
            window = session_bars[(session_bars["slot"] > bars.loc[earlier, "slot"])
                                  & (session_bars["slot"] < bars.loc[later, "slot"])]
            if source == VWAP:
                broken = bool((
                    (window["close_price"] < window["session_vwap"]) if lane == 1
                    else (window["close_price"] > window["session_vwap"])
                ).any())
                code = f"{VWAP}:same_direction_cross_without_intervening_reversal"
            else:
                level = float(
                    opening.loc[session, "high_price"] if lane == 1
                    else opening.loc[session, "low_price"]
                )
                if source == ORB_FAIL:
                    # A second reversal requires a fresh break back outside.
                    separator = (window["close_price"] > level) if lane == 1 else (
                        window["close_price"] < level)
                    code = f"{ORB_FAIL}:episode_not_separated_by_new_break"
                else:
                    separator = (window["close_price"] <= level) if lane == 1 else (
                        window["close_price"] >= level)
                    code = f"{source}:episode_not_separated_by_return_inside"
                broken = bool(separator.any())
            if not broken:
                fail(code)


def main() -> int:
    arguments = parser().parse_args()
    tickers = sampled_tickers(arguments.tickers)
    failures: dict[str, int] = {}
    counts: dict[str, int] = {}
    for position, ticker in enumerate(tickers, 1):
        verify_ticker(ticker, failures, counts)
        print(f"verified {position}/{len(tickers)} {ticker}", flush=True)
    report = {
        "tickers": tickers,
        "events_checked": dict(sorted(counts.items())),
        "total_events": sum(counts.values()),
        "failures": dict(sorted(failures.items())),
        "total_failures": sum(failures.values()),
        "verdict": "PASS" if not failures else "FAIL",
    }
    rendered = json.dumps(report, indent=2)
    if arguments.output:
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        arguments.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
