"""
Point-in-time feature construction for cross-sectional equity research.

Timing contract (the thing the 5-layer pipeline currently violates):
    Every feature for (ticker, date t) uses data available at or before the
    CLOSE of day t. The label is the return from close(t) to close(t+1).
    Nothing here may read a bar dated after t.
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Feature columns produced by build_features, in a stable order.
FEATURE_COLUMNS = [
    "mom_12_1",
    "mom_12_6",
    "mom_6_1",
    "rev_5",
    "rev_21",
    "vol_21",
    "vol_ratio",
    "dist_ma50",
    "dist_ma200",
    "volume_ratio",
    "range_pct",
    "rsi_14",
    "gap_overnight",
    "liquidity",
    "close_strength",
]


def load_daily_panel(start: str | None = None, end: str | None = None,
                     tickers: list[str] | None = None) -> pd.DataFrame:
    """Load the full daily panel in one query. Returns long format sorted by ticker, date."""
    import sys
    from pathlib import Path

    backend_dir = Path(__file__).resolve().parent.parent
    if str(backend_dir) not in sys.path:
        sys.path.insert(0, str(backend_dir))
    from database import get_db_cursor

    clauses = []
    params: list = []
    if start:
        clauses.append("datetime >= %s")
        params.append(start)
    if end:
        clauses.append("datetime <= %s")
        params.append(end)
    if tickers:
        clauses.append("ticker = ANY(%s)")
        params.append(tickers)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    sql = f"""
        SELECT ticker,
               datetime::date AS date,
               open_price AS open,
               high,
               low,
               close_price AS close,
               volume
        FROM stock_prices_daily
        {where}
        ORDER BY ticker, datetime
    """
    with get_db_cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    if not rows:
        return pd.DataFrame(columns=["ticker", "date", "open", "high", "low", "close", "volume"])

    df = pd.DataFrame([dict(r) for r in rows])
    df["date"] = pd.to_datetime(df["date"])
    for col in ("open", "high", "low", "close", "volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)

    df = df.dropna(subset=["close"])
    df = df[df["close"] > 0]
    logger.info("Loaded panel | rows=%s | tickers=%s | %s..%s",
                len(df), df["ticker"].nunique(), df["date"].min().date(), df["date"].max().date())
    return df.sort_values(["ticker", "date"]).reset_index(drop=True)


def _rsi(close: pd.Series, window: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.rolling(window).mean()
    avg_loss = loss.rolling(window).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - 100 / (1 + rs)


def build_features(panel: pd.DataFrame) -> pd.DataFrame:
    """Attach point-in-time features. All windows end at day t inclusive."""
    df = panel.sort_values(["ticker", "date"]).copy()
    g = df.groupby("ticker", group_keys=False)

    close = df["close"]
    df["ret_1"] = g["close"].pct_change()

    # Momentum skips the most recent month to avoid contaminating with short-term reversal.
    df["mom_12_1"] = g["close"].transform(lambda s: s.shift(21) / s.shift(252) - 1)
    df["mom_12_6"] = g["close"].transform(lambda s: s.shift(126) / s.shift(252) - 1)
    df["mom_6_1"] = g["close"].transform(lambda s: s.shift(21) / s.shift(126) - 1)

    # Reversal is signed so that a higher value means "expected to bounce".
    df["rev_5"] = -g["close"].transform(lambda s: s / s.shift(5) - 1)
    df["rev_21"] = -g["close"].transform(lambda s: s / s.shift(21) - 1)

    vol21 = g["ret_1"].transform(lambda s: s.rolling(21).std())
    vol5 = g["ret_1"].transform(lambda s: s.rolling(5).std())
    df["vol_21"] = vol21
    df["vol_ratio"] = vol5 / vol21.replace(0, np.nan)

    ma50 = g["close"].transform(lambda s: s.rolling(50).mean())
    ma200 = g["close"].transform(lambda s: s.rolling(200).mean())
    df["dist_ma50"] = close / ma50 - 1
    df["dist_ma200"] = close / ma200 - 1

    avg_vol20 = g["volume"].transform(lambda s: s.rolling(20).mean())
    df["volume_ratio"] = df["volume"] / avg_vol20.replace(0, np.nan)

    df["range_pct"] = (df["high"] - df["low"]) / close
    # Exact from the daily bar; reconstructing it from hourly only adds noise.
    df["close_strength"] = (close - df["low"]) / (df["high"] - df["low"]).replace(0, np.nan)
    df["rsi_14"] = g["close"].transform(_rsi)
    df["gap_overnight"] = df["open"] / g["close"].shift(1) - 1
    df["liquidity"] = np.log1p(g["close"].transform(lambda s: s) * avg_vol20)

    return df


# Risk controls. Deliberately NOT in FEATURE_COLUMNS: these are what we
# neutralize against to find out whether anything survives beyond beta and size.
RISK_COLUMNS = ["beta_60", "liquidity", "vol_21"]

# Derived from intraday (hourly) bars, one value per ticker-day, all computed
# from bars at or before that day's close. close_strength is deliberately NOT
# here: it is fully determined by the daily OHLC bar.
HOURLY_FEATURE_COLUMNS = [
    "h1_return",
    "h1_breakout",
    "vwap_dev",
    "intraday_vol",
    "last_hour_ret",
]


def load_hourly_features(start: str | None = None, end: str | None = None,
                         min_bars: int = 4) -> pd.DataFrame:
    """
    One feature row per ticker-day, reduced in Postgres.

    Aggregating server-side returns ~200k rows instead of ~1.4M; doing this in
    pandas peaked at 2.3 GB.
    """
    import sys
    from pathlib import Path

    backend_dir = Path(__file__).resolve().parent.parent
    if str(backend_dir) not in sys.path:
        sys.path.insert(0, str(backend_dir))
    from database import get_db_cursor

    clauses = []
    params: list = []
    if start:
        clauses.append("datetime >= %s")
        params.append(start)
    if end:
        clauses.append("datetime <= %s")
        params.append(end)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(min_bars)

    sql = f"""
        WITH bars AS (
            SELECT ticker,
                   (datetime AT TIME ZONE 'America/New_York')::date AS d,
                   datetime,
                   open_price, high, low, close_price, volume,
                   ROW_NUMBER() OVER (
                       PARTITION BY ticker, (datetime AT TIME ZONE 'America/New_York')::date
                       ORDER BY datetime
                   ) AS rn,
                   COUNT(*) OVER (
                       PARTITION BY ticker, (datetime AT TIME ZONE 'America/New_York')::date
                   ) AS n_bars
            FROM stock_prices_hourly
            {where}
        ),
        agg AS (
            SELECT ticker, d,
                   MAX(n_bars)                                          AS n_bars,
                   MIN(close_price) FILTER (WHERE rn = 1)               AS h1_close,
                   MIN(open_price)  FILTER (WHERE rn = 1)               AS h1_open,
                   MIN(high)        FILTER (WHERE rn = 1)               AS h1_high,
                   MIN(low)         FILTER (WHERE rn = 1)               AS h1_low,
                   MAX(high)        FILTER (WHERE rn > 1)               AS later_high,
                   MIN(low)         FILTER (WHERE rn > 1)               AS later_low,
                   MAX(high)                                            AS day_high,
                   MIN(low)                                             AS day_low,
                   MIN(close_price) FILTER (WHERE rn = n_bars)          AS day_close,
                   MIN(open_price)  FILTER (WHERE rn = n_bars)          AS last_open,
                   SUM(close_price * volume)                            AS pv_sum,
                   SUM(volume)                                          AS vol_sum,
                   STDDEV_SAMP(close_price / NULLIF(prev_close, 0) - 1) AS intraday_vol
            FROM (
                SELECT *, LAG(close_price) OVER (
                             PARTITION BY ticker, d ORDER BY datetime) AS prev_close
                FROM bars
            ) x
            GROUP BY ticker, d
        )
        SELECT agg.ticker,
               agg.d AS date,
               agg.h1_close / NULLIF(agg.h1_open, 0) - 1                AS h1_return,
               CASE
                 WHEN agg.later_high > agg.h1_high
                   THEN (agg.later_high - agg.h1_high) / NULLIF(agg.h1_high, 0)
                 WHEN agg.later_low  < agg.h1_low
                   THEN (agg.later_low  - agg.h1_low)  / NULLIF(agg.h1_low, 0)
                 ELSE 0
               END                                                      AS h1_breakout,
               dd.close_price
                 / NULLIF(agg.pv_sum / NULLIF(agg.vol_sum, 0), 0) - 1   AS vwap_dev,
               agg.intraday_vol,
               dd.close_price / NULLIF(agg.last_open, 0) - 1            AS last_hour_ret
        FROM agg
        -- Anchor to the official close; the last 1h bar misses the closing auction.
        JOIN stock_prices_daily dd
          ON dd.ticker = agg.ticker AND dd.datetime::date = agg.d
        WHERE agg.n_bars >= %s
        ORDER BY agg.ticker, agg.d
    """
    with get_db_cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    if not rows:
        return pd.DataFrame(columns=["ticker", "date"] + HOURLY_FEATURE_COLUMNS)

    df = pd.DataFrame([dict(r) for r in rows])
    df["date"] = pd.to_datetime(df["date"])
    for col in HOURLY_FEATURE_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
    logger.info("Hourly features | ticker-days=%s | tickers=%s",
                len(df), df["ticker"].nunique())
    return df


def load_sector_map() -> dict[str, str]:
    """Ticker -> sector. Returns {} when no metadata table is present."""
    import sys
    from pathlib import Path

    backend_dir = Path(__file__).resolve().parent.parent
    if str(backend_dir) not in sys.path:
        sys.path.insert(0, str(backend_dir))
    from database import get_db_cursor

    for table, col in (("ticker_metadata", "sector"), ("selected_tickers", "sector")):
        try:
            with get_db_cursor() as cur:
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = %s AND column_name = %s",
                    (table, col),
                )
                if not cur.fetchone():
                    continue
                cur.execute(f"SELECT ticker, {col} AS sector FROM {table} WHERE {col} IS NOT NULL")
                mapping = {r["ticker"]: r["sector"] for r in cur.fetchall()}
            if len(mapping) >= 20:
                logger.info("Sector map loaded | table=%s | tickers=%s", table, len(mapping))
                return mapping
            if mapping:
                logger.warning("Sector map in %s covers only %s ticker(s); "
                               "skipping sector neutralisation", table, len(mapping))
        except Exception as exc:  # metadata is optional; never block research on it
            logger.debug("Sector lookup failed on %s: %s", table, exc)
    logger.warning("No usable sector metadata; sector neutralisation will be skipped")
    return {}


def add_market_beta(df: pd.DataFrame, window: int = 60) -> pd.DataFrame:
    """Rolling beta against the equal-weight universe return."""
    wide = df.pivot_table(index="date", columns="ticker", values="ret_1")
    market = wide.mean(axis=1)
    market_var = market.rolling(window).var()
    beta = wide.rolling(window).cov(market).div(market_var, axis=0)
    long_beta = beta.stack(future_stack=True).rename("beta_60").reset_index()
    return df.merge(long_beta, on=["date", "ticker"], how="left")


def add_forward_labels(df: pd.DataFrame, horizon: int = 1) -> pd.DataFrame:
    """Forward return from close(t) to close(t+horizon), plus its cross-sectional rank."""
    out = df.sort_values(["ticker", "date"]).copy()
    g = out.groupby("ticker", group_keys=False)
    out[f"fwd_ret_{horizon}"] = g["close"].transform(lambda s: s.shift(-horizon) / s - 1)
    return out


def cross_sectional_rank(df: pd.DataFrame, col: str, out_col: str | None = None) -> pd.DataFrame:
    """Rank within each date, scaled to [-0.5, 0.5]. Removes market-wide moves."""
    out_col = out_col or f"{col}_cs"
    out = df.copy()
    out[out_col] = out.groupby("date")[col].transform(
        lambda s: s.rank(pct=True) - 0.5 if s.notna().sum() > 1 else np.nan
    )
    return out


def cross_sectional_zscore(df: pd.DataFrame, cols: list[str], clip: float = 3.0) -> pd.DataFrame:
    """Standardise each feature within each date, then winsorise."""
    out = df.copy()
    for col in cols:
        grouped = out.groupby("date")[col]
        mean = grouped.transform("mean")
        std = grouped.transform("std")
        out[col] = ((out[col] - mean) / std.replace(0, np.nan)).clip(-clip, clip)
    return out


def prepare_dataset(start: str | None = None, end: str | None = None,
                    horizon: int = 1, min_names_per_day: int = 50,
                    include_hourly: bool = False) -> pd.DataFrame:
    """Full pipeline: load, engineer, label, normalise. Returns a model-ready frame."""
    panel = load_daily_panel(start, end)
    if panel.empty:
        return panel

    df = build_features(panel)
    df = add_market_beta(df)
    df = add_forward_labels(df, horizon)
    label_raw = f"fwd_ret_{horizon}"

    feature_cols = list(FEATURE_COLUMNS)
    if include_hourly:
        hourly = load_hourly_features(start, end)
        if hourly.empty:
            logger.warning("No hourly features available; continuing daily-only")
        else:
            df = df.merge(hourly, on=["ticker", "date"], how="inner")
            feature_cols += HOURLY_FEATURE_COLUMNS

    sectors = load_sector_map()
    df["sector"] = df["ticker"].map(sectors).fillna("UNKNOWN") if sectors else "UNKNOWN"

    needed = feature_cols + [c for c in RISK_COLUMNS if c not in feature_cols]
    df = df.dropna(subset=needed + [label_raw])

    # Thin cross-sections make ranks unstable.
    counts = df.groupby("date")["ticker"].transform("size")
    df = df[counts >= min_names_per_day]

    df = cross_sectional_rank(df, label_raw, "label")
    df = cross_sectional_zscore(df, needed)
    df = df.dropna(subset=needed + ["label"])

    logger.info("Dataset ready | rows=%s | days=%s | tickers=%s",
                len(df), df["date"].nunique(), df["ticker"].nunique())
    return df.sort_values(["date", "ticker"]).reset_index(drop=True)


def prepare_live_cross_section(as_of: str | None = None, lookback_days: int = 500,
                               min_names: int = 50,
                               feature_cols: list[str] | None = None) -> pd.DataFrame:
    """
    Features for the most recent trading day at or before `as_of`, with no labels.

    Deliberately shares build_features/add_market_beta with prepare_dataset so the
    production signal cannot drift from what was validated in research.
    """
    start = None
    if as_of:
        start = (pd.Timestamp(as_of) - pd.Timedelta(days=lookback_days * 2)).strftime("%Y-%m-%d")
    panel = load_daily_panel(start, as_of)
    if panel.empty:
        return panel

    df = build_features(panel)
    df = add_market_beta(df)

    sectors = load_sector_map()
    df["sector"] = df["ticker"].map(sectors).fillna("UNKNOWN") if sectors else "UNKNOWN"

    model_features = feature_cols or FEATURE_COLUMNS
    needed = model_features + [c for c in RISK_COLUMNS if c not in model_features]
    df = df.dropna(subset=needed)
    if df.empty:
        return df

    latest = df["date"].max()
    cross = df[df["date"] == latest]
    if len(cross) < min_names:
        logger.warning("Cross-section too thin on %s | names=%s", latest.date(), len(cross))
        return cross.iloc[0:0]

    # Standardise using only this day's cross-section, exactly as in research.
    cross = cross_sectional_zscore(cross, needed)
    cross = cross.dropna(subset=needed)
    logger.info("Live cross-section | date=%s | names=%s", latest.date(), len(cross))
    return cross.reset_index(drop=True)
