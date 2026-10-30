"""Provider-free, point-in-time research data and feature computation.

External facts are versioned assertions, never inferred from missing values.
Historical reconstruction must be explicitly labeled by the exporter.
"""
from __future__ import annotations

from dataclasses import dataclass
from copy import deepcopy
from datetime import timedelta
import math

import exchange_calendars
import numpy as np
import pandas as pd

from .catalog import digest


class Unavailable(Exception):
    pass


def stamp(value):
    value = pd.Timestamp(value)
    if value.tzinfo is None:
        raise ValueError("timestamps must have explicit timezone")
    return value.tz_convert("UTC")


def number(value):
    if isinstance(value, bool):
        raise ValueError("boolean is not a numeric feature")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("nonfinite numeric input")
    return result


def wilder(values, period=14):
    """Arithmetic seed followed by Wilder recursion; missing inputs reset the seed."""
    out, seed, previous = [], [], None
    for value in values:
        if not math.isfinite(float(value)):
            seed, previous = [], None
            out.append(float("nan"))
            continue
        if previous is None:
            seed.append(float(value))
            if len(seed) == period:
                previous = sum(seed) / period
        else:
            previous = ((period - 1) * previous + value) / period
        out.append(previous if previous is not None else float("nan"))
    return pd.Series(out, index=values.index, dtype=float)


def indicators(frame):
    frame = frame.copy()
    close, high, low = frame.close, frame.high, frame.low
    previous = close.shift(1)
    tr = pd.concat([high - low, (high - previous).abs(), (low - previous).abs()], axis=1).max(axis=1)
    tr.iloc[0] = np.nan  # preceding close is required
    atr = wilder(tr)
    up, down = high.diff(), -low.diff()
    plus = up.where((up > down) & (up > 0), 0.0)
    minus = down.where((down > up) & (down > 0), 0.0)
    plus.iloc[0] = minus.iloc[0] = np.nan
    frame["atr"] = atr
    frame["atr_prior"] = atr.shift(1)
    frame["plus_di"] = 100 * wilder(plus) / atr
    frame["minus_di"] = 100 * wilder(minus) / atr
    denom = frame.plus_di + frame.minus_di
    dx = 100 * (frame.plus_di - frame.minus_di).abs() / denom.replace(0, np.nan)
    frame["adx"] = wilder(dx)
    frame["adx_change3"] = frame.adx - frame.adx.shift(3)
    for span in (20, 50, 200):
        frame[f"ema{span}"] = close.ewm(span=span, adjust=False).mean()
    frame["ema50_prior10"] = frame.ema50.shift(10)
    for period in (100, 200):
        frame[f"sma{period}"] = close.rolling(period).mean()
    frame["sma200_prior20"] = frame.sma200.shift(20)
    for period in (20, 63):
        frame[f"return{period}"] = close / close.shift(period) - 1
    frame["compression"] = tr.rolling(5).mean().shift(1) / tr.rolling(20).mean().shift(1)
    frame["volume_median20"] = frame.volume.rolling(20).median().shift(1)
    for period in (20, 60):
        frame[f"liquidity{period}"] = (close * frame.volume).rolling(period).median().shift(1)
    return frame


class ResearchData:
    def __init__(self, payload):
        payload = deepcopy(payload)
        if payload.get("schema") != "equity_research_dataset_v1":
            raise ValueError("unsupported dataset schema")
        if payload.get("availability_mode") not in {"OBSERVED", "HISTORICAL_RECONSTRUCTED"}:
            raise ValueError("explicit availability_mode required")
        self.payload = payload
        self.input_hash = digest(payload)
        self.calendar = exchange_calendars.get_calendar("XNYS")
        self.bars = self._records(payload.get("bars", []), "bar")
        self.facts = self._records(payload.get("facts", []), "fact")
        self.quotes = self._records(payload.get("quotes", []), "quote")

    def _records(self, records, kind):
        result, identities = [], set()
        for original in records:
            row = dict(original)
            for key in ("security_id", "source_id", "revision"):
                if not isinstance(row.get(key), str) or not row[key]:
                    raise ValueError(f"missing {kind} {key}")
            identity = (row["source_id"], row["revision"])
            if identity in identities:
                raise ValueError(f"duplicate {kind} source revision")
            identities.add(identity)
            for key in ("market_at", "available_at", "received_at"):
                row[key] = stamp(row[key])
            if not row["market_at"] <= row["available_at"] <= row["received_at"]:
                raise ValueError(f"noncausal {kind} clocks")
            if kind == "bar":
                row["start"] = stamp(row["start"])
                if row.get("completed") is not True or row["start"] >= row["market_at"]:
                    raise ValueError("completed bars only")
                if row.get("interval") not in {"5m", "30m", "1h", "1d"}:
                    raise ValueError("unsupported bar interval")
                if not row.get("adjustment_id") or not row.get("segment_id"):
                    raise ValueError("bar adjustment and continuity segment evidence required")
                for key in ("open", "high", "low", "close", "volume"):
                    row[key] = number(row[key])
                if not (0 < row["low"] <= min(row["open"], row["close"]) <=
                        max(row["open"], row["close"]) <= row["high"]) or row["volume"] < 0:
                    raise ValueError("invalid OHLCV")
                session = str(row["start"].tz_convert("America/New_York").date())
                opening, closing = self.session_bounds(session)
                duration = {"5m": 300, "30m": 1800, "1h": 3600}.get(row["interval"])
                if duration:
                    if not (opening <= row["start"] < row["market_at"] <= closing) or (
                            row["market_at"] - row["start"]).total_seconds() != duration:
                        raise ValueError("bar is not a full RTH interval")
                    if (row["start"] - opening).total_seconds() % duration:
                        raise ValueError("bar is not anchored to exchange open")
                elif row["start"] != opening or row["market_at"] != closing:
                    raise ValueError("daily bar must span its exact RTH session")
                row["session"] = session
                if row.get("vwap") is not None:
                    row["vwap"] = number(row["vwap"])
                    if not row["low"] <= row["vwap"] <= row["high"]:
                        raise ValueError("native trade VWAP outside bar")
                if row.get("total_return_close") is not None:
                    row["total_return_close"] = number(row["total_return_close"])
                    if row["total_return_close"] <= 0 or not row.get("total_return_basis_id"):
                        raise ValueError("positive certified total-return index required")
            elif kind == "fact":
                row["valid_until"] = stamp(row["valid_until"])
                if row["valid_until"] <= row["market_at"] or not row.get("kind"):
                    raise ValueError("invalid fact validity/kind")
                if not isinstance(row.get("values"), dict) or not row.get("methodology") or not row.get("input_ids"):
                    raise ValueError("fact values/methodology/input lineage required")
                if not isinstance(row["input_ids"], list) or any(not isinstance(x, str) or not x for x in row["input_ids"]):
                    raise ValueError("fact input_ids must be nonempty strings")
                digest(row["values"])
            else:
                for key in ("bid", "ask", "bid_size", "ask_size", "tick_size"):
                    row[key] = number(row[key])
                if not 0 < row["bid"] < row["ask"] or min(row["bid_size"], row["ask_size"], row["tick_size"]) <= 0:
                    raise ValueError("invalid quote")
                if type(row.get("sequence")) is not int or not row.get("venue"):
                    raise ValueError("sequenced quote venue required")
            result.append(row)
        return tuple(result)

    def session_bounds(self, session):
        return self.calendar.session_open(session), self.calendar.session_close(session)

    def session_offset(self, session, n):
        return str(self.calendar.session_offset(session, n).date())

    def fact(self, security, kind, at):
        eligible = [r for r in self.facts if r["security_id"] == security and r["kind"] == kind
                    and r["received_at"] <= at < r["valid_until"] and r["market_at"] <= at]
        if not eligible:
            raise Unavailable(f"MISSING_OR_STALE_{kind.upper()}")
        return max(eligible, key=lambda r: (r["market_at"], r["received_at"], r["revision"]))

    def history(self, security, interval, at):
        eligible = [r for r in self.bars if r["security_id"] == security and r["interval"] == interval
                    and r["received_at"] <= at]
        selected = {}
        for row in sorted(eligible, key=lambda r: (r["received_at"], r["revision"])):
            selected[row["market_at"]] = row
        if not selected:
            raise Unavailable(f"MISSING_{interval}_BARS")
        records = sorted(selected.values(), key=lambda r: r["market_at"])
        # Never bridge an exporter-certified corporate-action/identity break.
        last = records[-1]
        cut = max((i for i, r in enumerate(records) if (r["segment_id"], r["adjustment_id"]) !=
                   (last["segment_id"], last["adjustment_id"])), default=-1)
        records = records[cut + 1:]
        for left, right in zip(records, records[1:]):
            if interval == "1d":
                continuous = right["session"] == self.session_offset(left["session"], 1)
            elif left["session"] == right["session"]:
                continuous = left["market_at"] == right["start"]
            else:
                _, close = self.session_bounds(left["session"])
                opening, _ = self.session_bounds(right["session"])
                seconds = {"5m": 300, "30m": 1800, "1h": 3600}[interval]
                continuous = (right["session"] == self.session_offset(left["session"], 1)
                              and (close - left["market_at"]).total_seconds() < seconds
                              and right["start"] == opening)
            if not continuous:
                # Latest contiguous tail only; warmup must restart after a missing bar.
                records = records[records.index(right):]
        frame = indicators(pd.DataFrame(records))
        if interval != "1d":
            frame = self._session_features(frame)
        return frame

    def _session_features(self, frame):
        frame = frame.copy()
        frame["slot"] = frame.start.dt.tz_convert("America/New_York").dt.strftime("%H:%M")
        frame["short_session"] = [int((self.session_bounds(s)[1] - self.session_bounds(s)[0]).total_seconds())
                                  for s in frame.session]
        frame["cum_volume"] = frame.groupby("session").volume.cumsum()
        frame["session_vwap"] = np.nan
        for session, group in frame.groupby("session"):
            opening, _ = self.session_bounds(session)
            if group.start.iloc[0] != opening:
                frame.loc[group.index, "cum_volume"] = np.nan
            # No proxy from typical price. Entire session prefix must have native VWAP.
            if group.start.iloc[0] != opening or "vwap" not in group or group.vwap.isna().any():
                continue
            frame.loc[group.index, "session_vwap"] = (group.vwap * group.volume).cumsum() / group.volume.cumsum().replace(0, np.nan)
        frame["slot_rvol"] = np.nan
        frame["cumulative_rvol"] = np.nan
        for _, indices in frame.groupby(["slot", "short_session"]).groups.items():
            group = frame.loc[indices]
            for field, source in (("slot_rvol", "volume"), ("cumulative_rvol", "cum_volume")):
                baseline = group[source].shift(1).rolling(20, min_periods=15).median()
                frame.loc[indices, field] = group[source] / baseline.replace(0, np.nan)
        return frame

    def latest_quote(self, security, at, max_age):
        rows = [r for r in self.quotes if r["security_id"] == security and r["received_at"] <= at]
        if not rows:
            raise Unavailable("MISSING_QUOTES")
        row = max(rows, key=lambda r: (r["market_at"], r["sequence"]))
        if (at - row["market_at"]).total_seconds() > max_age or (at - row["received_at"]).total_seconds() > max_age:
            raise Unavailable("STALE_QUOTE")
        return row


@dataclass
class Snapshot:
    data: ResearchData
    security: str
    at: pd.Timestamp
    session: str
    interval: str

    def __post_init__(self):
        self._histories = {}
        self.lineage = set()

    def history(self, interval=None, security=None, prior_day=False):
        interval, security = interval or self.interval, security or self.security
        key = (interval, security, prior_day)
        if key not in self._histories:
            cutoff = self.at
            if prior_day:
                cutoff = min(cutoff, self.data.session_bounds(self.session)[0] - timedelta(microseconds=1))
            frame = self.data.history(security, interval, cutoff)
            if prior_day and frame.session.iloc[-1] != self.data.session_offset(self.session, -1):
                raise Unavailable("STALE_DAILY_CONTEXT")
            self.lineage.update(f"{r.source_id}:{r.revision}" for r in frame.itertuples())
            self._histories[key] = frame
        return self._histories[key]

    def facts(self, kind):
        record = self.data.fact(self.security, kind, self.at)
        if kind == "relative_strength" and self.interval != "1d":
            if record["market_at"] >= self.data.session_bounds(self.session)[0]:
                raise Unavailable("RELATIVE_STRENGTH_MUST_USE_PRIOR_DAILY_CONTEXT")
        self.lineage.add(f"{record['source_id']}:{record['revision']}")
        self.lineage.update(record["input_ids"])
        return record["values"]

    def require(self, kind, *keys):
        values = self.facts(kind)
        if any(key not in values or values[key] is None for key in keys):
            raise Unavailable(f"INCOMPLETE_{kind.upper()}")
        return [values[key] for key in keys]


def ready_value(row, name):
    value = row.get(name)
    if value is None or not math.isfinite(float(value)):
        raise Unavailable(f"MISSING_FEATURE_{name.upper()}")
    return float(value)
