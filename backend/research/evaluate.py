"""
Evaluation for cross-sectional alpha signals.

Metrics are the ones systematic equity desks actually use:
  IC      — daily Spearman correlation between prediction and forward return
  IC IR   — mean(IC) / std(IC); the signal's t-stat scales as IR * sqrt(days)
  Deciles — mean forward return per predicted decile; a real signal is monotonic
  Baselines — always-long and single-factor, so skill is measured against the
              naive alternative rather than against 50%.
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

TRADING_DAYS = 252


def daily_ic(df: pd.DataFrame, pred_col: str, ret_col: str) -> pd.Series:
    """Spearman IC per date. Rank-then-Pearson avoids a scipy dependency."""
    def _ic(group: pd.DataFrame) -> float:
        if len(group) < 10:
            return np.nan
        a = group[pred_col].rank()
        b = group[ret_col].rank()
        if a.std() == 0 or b.std() == 0:
            return np.nan
        return a.corr(b)

    return df.groupby("date")[[pred_col, ret_col]].apply(_ic).dropna()


def ic_summary(ic: pd.Series) -> dict:
    if ic.empty:
        return {"days": 0, "ic_mean": np.nan, "ic_std": np.nan, "ic_ir": np.nan,
                "t_stat": np.nan, "pct_positive": np.nan}
    mean, std = ic.mean(), ic.std()
    ir = mean / std if std and not np.isnan(std) else np.nan
    return {
        "days": int(len(ic)),
        "ic_mean": float(mean),
        "ic_std": float(std),
        "ic_ir": float(ir) if ir == ir else np.nan,
        "t_stat": float(ir * np.sqrt(len(ic))) if ir == ir else np.nan,
        "pct_positive": float((ic > 0).mean() * 100),
    }


def decile_table(df: pd.DataFrame, pred_col: str, ret_col: str, n: int = 10,
                 periods_per_year: float = TRADING_DAYS) -> pd.DataFrame:
    """Mean forward return per predicted decile, computed within each date."""
    work = df.copy()

    def _bucket(s: pd.Series) -> pd.Series:
        if s.notna().sum() < n:
            return pd.Series(np.nan, index=s.index)
        return pd.qcut(s.rank(method="first"), n, labels=False, duplicates="drop")

    work["decile"] = work.groupby("date")[pred_col].transform(_bucket)
    work = work.dropna(subset=["decile"])

    per_day = work.groupby(["date", "decile"])[ret_col].mean().unstack()
    table = pd.DataFrame({
        "mean_ret_bps": per_day.mean() * 10_000,
        "ann_ret_pct": per_day.mean() * periods_per_year * 100,
        "sharpe": per_day.mean() / per_day.std() * np.sqrt(periods_per_year),
        "n_obs": per_day.notna().sum(),
    })
    table.index = [f"D{int(i) + 1}" for i in table.index]
    return table


def _decile_weights(df: pd.DataFrame, pred_col: str, n: int) -> pd.DataFrame:
    """Long top decile, short bottom decile, equal weight, each side summing to 1."""
    work = df.copy()

    def _bucket(s: pd.Series) -> pd.Series:
        if s.notna().sum() < n:
            return pd.Series(np.nan, index=s.index)
        return pd.qcut(s.rank(method="first"), n, labels=False, duplicates="drop")

    work["decile"] = work.groupby("date")[pred_col].transform(_bucket)
    work = work.dropna(subset=["decile"])
    top, bottom = work["decile"].max(), work["decile"].min()

    work = work[work["decile"].isin([top, bottom])].copy()
    side = np.where(work["decile"] == top, 1.0, -1.0)
    counts = work.groupby(["date", "decile"])["ticker"].transform("size")
    work["weight"] = side / counts
    return work


def long_short_stats(df: pd.DataFrame, pred_col: str, ret_col: str, n: int = 10,
                     cost_bps: float = 0.0,
                     periods_per_year: float = TRADING_DAYS) -> dict:
    """
    Equal-weight top-minus-bottom decile, rebalanced every period in `df`.

    `cost_bps` is a one-way cost applied to traded notional, so a full rotation
    of both sides costs roughly 4x that per rebalance.
    """
    work = _decile_weights(df, pred_col, n)
    if work.empty:
        return {"days": 0, "gross_ann_pct": np.nan, "net_ann_pct": np.nan,
                "gross_sharpe": np.nan, "net_sharpe": np.nan,
                "turnover": np.nan, "hit_rate": np.nan}

    gross = work.groupby("date").apply(
        lambda g: float((g["weight"] * g[ret_col]).sum()), include_groups=False
    ).dropna()

    # Turnover: total absolute weight change between consecutive rebalances.
    pivot = work.pivot_table(index="date", columns="ticker", values="weight", fill_value=0.0)
    traded = pivot.diff().abs().sum(axis=1)
    traded.iloc[0] = pivot.iloc[0].abs().sum()

    cost_rate = cost_bps / 10_000.0
    net = (gross - traded.reindex(gross.index).fillna(0.0) * cost_rate).dropna()

    def _sharpe(s: pd.Series) -> float:
        return float(s.mean() / s.std() * np.sqrt(periods_per_year)) if len(s) > 1 and s.std() else np.nan

    return {
        "obs": int(len(gross)),
        "gross_ann_pct": float(gross.mean() * periods_per_year * 100),
        "net_ann_pct": float(net.mean() * periods_per_year * 100),
        "gross_sharpe": _sharpe(gross),
        "net_sharpe": _sharpe(net),
        "turnover": float(traded.mean()),
        "hit_rate": float((net > 0).mean() * 100),
    }


def top_decile_stats(df: pd.DataFrame, pred_col: str, ret_col: str, n: int = 10,
                     cost_bps: float = 0.0,
                     periods_per_year: float = TRADING_DAYS) -> dict:
    """Equal-weight top decile, including alpha versus its eligible universe."""
    work = df.copy()

    def _bucket(s: pd.Series) -> pd.Series:
        if s.notna().sum() < n:
            return pd.Series(np.nan, index=s.index)
        return pd.qcut(s.rank(method="first"), n, labels=False, duplicates="drop")

    work["decile"] = work.groupby("date")[pred_col].transform(_bucket)
    work = work.dropna(subset=["decile"])
    work = work[work["decile"] == work["decile"].max()].copy()
    if work.empty:
        return {"obs": 0, "net_ann_pct": np.nan, "net_sharpe": np.nan,
                "turnover": np.nan, "alpha_ann_pct": np.nan,
                "alpha_t_stat": np.nan, "alpha_sharpe": np.nan}

    counts = work.groupby("date")["ticker"].transform("size")
    work["weight"] = 1.0 / counts
    gross = work.groupby("date").apply(
        lambda group: float((group["weight"] * group[ret_col]).sum()),
        include_groups=False,
    ).dropna()

    pivot = work.pivot_table(index="date", columns="ticker", values="weight", fill_value=0.0)
    traded = pivot.diff().abs().sum(axis=1)
    traded.iloc[0] = pivot.iloc[0].abs().sum()
    net = gross - traded.reindex(gross.index).fillna(0.0) * cost_bps / 10_000.0

    baseline = df.groupby("date")[ret_col].mean().reindex(net.index)
    relative = (net - baseline).dropna()

    def _sharpe(series: pd.Series) -> float:
        return (float(series.mean() / series.std() * np.sqrt(periods_per_year))
                if len(series) > 1 and series.std() else np.nan)

    alpha_t = (float(relative.mean() / (relative.std() / np.sqrt(len(relative))))
               if len(relative) > 1 and relative.std() else np.nan)
    return {
        "obs": int(len(net)),
        "net_ann_pct": float(net.mean() * periods_per_year * 100),
        "net_sharpe": _sharpe(net),
        "turnover": float(traded.mean()),
        "alpha_ann_pct": float(relative.mean() * periods_per_year * 100),
        "alpha_t_stat": alpha_t,
        "alpha_sharpe": _sharpe(relative),
    }


def baseline_always_long(df: pd.DataFrame, ret_col: str,
                         periods_per_year: float = TRADING_DAYS) -> dict:
    """Equal-weight the whole universe — the bar any signal must clear."""
    per_period = df.groupby("date")[ret_col].mean().dropna()
    if per_period.empty:
        return {"obs": 0, "gross_ann_pct": np.nan, "net_ann_pct": np.nan,
                "gross_sharpe": np.nan, "net_sharpe": np.nan,
                "turnover": 0.0, "hit_rate": np.nan}
    sharpe = (per_period.mean() / per_period.std() * np.sqrt(periods_per_year)
              if per_period.std() else np.nan)
    ann = float(per_period.mean() * periods_per_year * 100)
    return {
        "obs": int(len(per_period)),
        "gross_ann_pct": ann,
        "net_ann_pct": ann,
        "gross_sharpe": float(sharpe) if sharpe == sharpe else np.nan,
        "net_sharpe": float(sharpe) if sharpe == sharpe else np.nan,
        "turnover": 0.0,
        "hit_rate": float((per_period > 0).mean() * 100),
    }


def neutralize(df: pd.DataFrame, pred_col: str, factor_cols: list[str] | None = None,
               group_col: str | None = None, out_col: str = "pred_neutral") -> pd.DataFrame:
    """
    Strip risk exposure from a prediction, one cross-section at a time.

    Demeans within `group_col` (e.g. sector), then takes the OLS residual of the
    prediction on `factor_cols` (e.g. beta, size, vol). What survives is the part
    of the signal that is not explained by those exposures.
    """
    out = df.copy()
    factor_cols = [c for c in (factor_cols or []) if c in out.columns]

    def _per_date(group: pd.DataFrame) -> pd.Series:
        pred = group[pred_col].astype(float)
        if group_col and group_col in group.columns and group[group_col].nunique() > 1:
            pred = pred - pred.groupby(group[group_col]).transform("mean")
        if not factor_cols or len(group) <= len(factor_cols) + 1:
            return pred
        x = group[factor_cols].astype(float).to_numpy()
        x = np.column_stack([np.ones(len(x)), x])
        y = pred.to_numpy()
        mask = np.isfinite(y) & np.isfinite(x).all(axis=1)
        if mask.sum() <= x.shape[1]:
            return pred
        beta, *_ = np.linalg.lstsq(x[mask], y[mask], rcond=None)
        return pd.Series(y - x @ beta, index=group.index)

    # Build explicitly rather than via groupby.apply: with a single date the
    # latter returns a DataFrame instead of a Series.
    pieces = [_per_date(group) for _, group in out.groupby("date", sort=False)]
    out[out_col] = pd.concat(pieces).reindex(out.index) if pieces else np.nan
    return out


def _fit_ridge(x: np.ndarray, y: np.ndarray, alpha: float) -> np.ndarray:
    """Closed-form ridge on centred data; intercept is irrelevant for ranking."""
    n_features = x.shape[1]
    xtx = x.T @ x + alpha * np.eye(n_features)
    try:
        return np.linalg.solve(xtx, x.T @ y)
    except np.linalg.LinAlgError:
        return np.linalg.pinv(xtx) @ (x.T @ y)


def purged_walk_forward(
    df: pd.DataFrame,
    feature_cols: list[str],
    label_col: str = "label",
    train_days: int = 252,
    test_days: int = 21,
    embargo_days: int = 2,
    alpha: float = 10.0,
    expanding: bool = True,
    carry_cols: list[str] | None = None,
) -> pd.DataFrame:
    """
    Roll a ridge model forward in time, never training on data at or after the
    test window. `embargo_days` drops the bars whose forward label would overlap
    the test period.
    """
    dates = np.array(sorted(df["date"].unique()))
    if len(dates) < train_days + embargo_days + test_days:
        logger.warning("Not enough history for walk-forward | days=%s", len(dates))
        return pd.DataFrame()

    predictions = []
    start = train_days

    while start + embargo_days + test_days <= len(dates):
        train_end_idx = start
        test_start_idx = start + embargo_days
        test_end_idx = min(test_start_idx + test_days, len(dates))

        train_lo = 0 if expanding else max(0, train_end_idx - train_days)
        train_dates = dates[train_lo:train_end_idx]
        test_dates = dates[test_start_idx:test_end_idx]

        train = df[df["date"].isin(train_dates)]
        test = df[df["date"].isin(test_dates)]
        if train.empty or test.empty:
            start += test_days
            continue

        x_train = train[feature_cols].to_numpy(dtype=float)
        y_train = train[label_col].to_numpy(dtype=float)
        beta = _fit_ridge(x_train, y_train, alpha)

        block = test[["date", "ticker", label_col]].copy()
        block["pred"] = test[feature_cols].to_numpy(dtype=float) @ beta
        for extra in [c for c in test.columns if c.startswith("fwd_ret_")]:
            block[extra] = test[extra].to_numpy()
        for extra in carry_cols or []:
            if extra in test.columns:
                block[extra] = test[extra].to_numpy()
        predictions.append(block)

        start += test_days

    if not predictions:
        return pd.DataFrame()

    out = pd.concat(predictions, ignore_index=True)
    logger.info("Walk-forward complete | folds=%s | test rows=%s", len(predictions), len(out))
    return out


def feature_ic_scan(df: pd.DataFrame, feature_cols: list[str], ret_col: str) -> pd.DataFrame:
    """Standalone IC for every feature — shows which carry signal before modelling."""
    rows = []
    for col in feature_cols:
        summary = ic_summary(daily_ic(df, col, ret_col))
        summary["feature"] = col
        rows.append(summary)
    out = pd.DataFrame(rows).set_index("feature")
    return out.sort_values("ic_mean", key=abs, ascending=False)


def persist_run(record: dict) -> int | None:
    """Append one run to research_runs. Never let logging break the research."""
    import sys
    from pathlib import Path

    backend_dir = Path(__file__).resolve().parent.parent
    if str(backend_dir) not in sys.path:
        sys.path.insert(0, str(backend_dir))

    def _clean(v):
        if v is None:
            return None
        if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
            return None
        return v

    try:
        from database import get_db_cursor

        with get_db_cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS research_runs (
                    run_id SERIAL PRIMARY KEY,
                    run_at TIMESTAMPTZ DEFAULT NOW(),
                    features TEXT NOT NULL,
                    horizon_days SMALLINT NOT NULL,
                    rebalance_days SMALLINT NOT NULL,
                    cost_bps REAL NOT NULL,
                    neutralised BOOLEAN NOT NULL DEFAULT TRUE,
                    data_start DATE, data_end DATE,
                    rows_used INTEGER, tickers INTEGER, test_periods INTEGER,
                    ic_mean DOUBLE PRECISION, ic_ir DOUBLE PRECISION,
                    ic_t_stat DOUBLE PRECISION, decile_spread DOUBLE PRECISION,
                    ls_net_return DOUBLE PRECISION, ls_net_sharpe DOUBLE PRECISION,
                    turnover DOUBLE PRECISION,
                    base_return DOUBLE PRECISION, base_sharpe DOUBLE PRECISION,
                    verdict VARCHAR(32), checks_passed SMALLINT, checks_total SMALLINT
                )
            """)
            cur.execute("""
                ALTER TABLE research_runs
                    ADD COLUMN IF NOT EXISTS activity_filter VARCHAR(32)
                        NOT NULL DEFAULT 'none',
                    ADD COLUMN IF NOT EXISTS top_net_return DOUBLE PRECISION,
                    ADD COLUMN IF NOT EXISTS top_net_sharpe DOUBLE PRECISION,
                    ADD COLUMN IF NOT EXISTS top_turnover DOUBLE PRECISION,
                    ADD COLUMN IF NOT EXISTS top_alpha_return DOUBLE PRECISION,
                    ADD COLUMN IF NOT EXISTS top_alpha_t_stat DOUBLE PRECISION,
                    ADD COLUMN IF NOT EXISTS top_alpha_sharpe DOUBLE PRECISION
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_research_runs_at "
                        "ON research_runs (run_at DESC)")
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = current_schema() AND table_name = 'research_runs'"
            )
            available = {row["column_name"] for row in cur.fetchall()}
            unknown = set(record) - available
            if unknown:
                raise ValueError(
                    "research_runs has no columns for: " + ", ".join(sorted(unknown))
                )
            cols = list(record.keys())
            cur.execute(
                f"INSERT INTO research_runs ({', '.join(cols)}) "
                f"VALUES ({', '.join(['%s'] * len(cols))}) RETURNING run_id",
                [_clean(record[c]) for c in cols],
            )
            return cur.fetchone()["run_id"]
    except Exception as exc:
        logger.error("Could not persist research run: %s", exc)
        raise


def recent_runs(features: str | None = None, limit: int = 10) -> pd.DataFrame:
    """Past runs, newest first — use to spot IC decay for a given feature set."""
    import sys
    from pathlib import Path

    backend_dir = Path(__file__).resolve().parent.parent
    if str(backend_dir) not in sys.path:
        sys.path.insert(0, str(backend_dir))
    from database import get_db_cursor

    where = "WHERE features = %s" if features else ""
    params = [features, limit] if features else [limit]
    sql = f"""
         SELECT run_at, features, activity_filter, horizon_days, test_periods,
             ic_mean, ic_t_stat, ls_net_sharpe, top_net_sharpe,
             top_alpha_return, top_alpha_t_stat, base_sharpe, verdict
        FROM research_runs
        {where}
        ORDER BY run_at DESC LIMIT %s
    """
    with get_db_cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    return pd.DataFrame([dict(r) for r in rows])
