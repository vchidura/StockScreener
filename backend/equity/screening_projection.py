"""Opt-in bounded projection preparation, separate from all reader requests."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import exchange_calendars
import numpy as np
import pandas as pd

from database import get_db_cursor
from equity.technicals import detect_setup_candlesticks
from equity.stock_discovery import stock_features
from equity.screening_gaps import project_gaps
from equity.screening_hourly import attach_hourly
from research.screening import FIELDS, PATTERNS, VERSION, UNIVERSE, FIELD_SET_VERSION, STATE_FIELDS, GAP_VERSION, GAP_WINDOW, GAP_CATALOG, digest


SNAPSHOT_TYPE = "SCREENING_DAILY_V1"


def project_security(member, bars, actions, session, ordinals):
    identity = str(member["security_id"])
    frame = pd.DataFrame(bars)
    if not frame.empty:
        frame = frame.loc[frame.session_date <= session].sort_values("session_date").tail(253)
    result = dict(security_id=identity, ticker=member["ticker"], session=str(session),
                  company_name=member.get("company_name"), source_bar_id=str(member["selected_bar_revision_id"]) if member["selected_bar_revision_id"] else None,
                  reference_id=str(member["security_revision_id"]) if member.get("security_revision_id") else None,
                  eligible=False, values={}, missing={}, patterns={}, quality=["KNOWN_ACTIONS_ONLY_NOT_CERTIFIED"])

    def window(size):
        if member.get("status", "SELECTED") != "SELECTED" or result["source_bar_id"] is None:
            return None, "SOURCE_PUBLICATION_MEMBER_UNAVAILABLE"
        if frame.empty or frame.session_date.iloc[-1] != session or str(frame.bar_revision_id.iloc[-1]) != result["source_bar_id"]:
            return None, "MISSING_EXACT_PUBLICATION_BAR"
        if len(frame) < size:
            return None, "INSUFFICIENT_HISTORY"
        selected = frame.tail(size)
        if not selected.security_id.astype(str).eq(identity).all():
            return None, "IDENTITY_CHANGED"
        if selected.session_date.map(ordinals).isna().any() or selected.session_date.map(ordinals).diff().iloc[1:].ne(1).any():
            return None, "MISSING_EXPECTED_SESSION"
        prices = selected[["open", "high", "low", "close", "volume"]].astype(float)
        if not np.isfinite(prices).all().all() or (prices[["open", "high", "low", "close"]] <= 0).any().any() or (prices.volume < 0).any():
            return None, "INVALID_BAR"
        if (prices.high < prices[["open", "close", "low"]].max(axis=1)).any() or (prices.low > prices[["open", "close", "high"]].min(axis=1)).any():
            return None, "INVALID_BAR_GEOMETRY"
        if size > 1 and any(selected.session_date.iloc[0] < action["effective_date"] <= session and action["action_type"] in ("SPLIT", "MERGER", "SPINOFF", "SYMBOL_CHANGE") for action in actions):
            return None, "CORPORATE_ACTION_REVIEW"
        return prices, None

    latest, issue = window(1)
    result["eligible"] = latest is not None and member.get("security_type") in ("CS", "ETF", "ETV")
    if not result["eligible"]:
        result["quality"].append(issue or "INSTRUMENT_IDENTITY_UNAVAILABLE")
    for name, spec in FIELDS.items():
        if name == "instrument_type":
            result["values"][name] = member.get("security_type") if member.get("security_type") in ("CS", "ETF", "ETV") else None
            continue
        if name == "momentum_percentile" or name in STATE_FIELDS:
            continue
        prices, issue = window(spec["warmup_sessions"])
        value = None
        if prices is not None:
            close, volume = prices.close, prices.volume
            if name == "price":
                value = close.iloc[-1]
            elif name == "volume":
                value = volume.iloc[-1]
            elif name == "change":
                value = close.iloc[-1] / close.iloc[-2] - 1
            elif name == "dollar_volume_20":
                value = (close * volume).iloc[:-1].mean()
            elif name == "relative_volume_20":
                denominator = volume.iloc[:-1].mean()
                value = volume.iloc[-1] / denominator if denominator > 0 else None
            elif name in ("vs_ema20", "vs_ema50"):
                span = spec["warmup_sessions"]
                value = close.iloc[-1] / close.ewm(span=span, adjust=False).mean().iloc[-1] - 1
            elif name == "vs_sma200":
                value = close.iloc[-1] / close.mean() - 1
            elif name in ("momentum_12_1", "momentum_6_1", "momentum_3_1"):
                value = close.iloc[-22] / close.iloc[0] - 1
            elif name == "realized_volatility_21":
                value = close.pct_change(fill_method=None).iloc[1:].std(ddof=1) * np.sqrt(252)
            elif name == "vs_prior_high20":
                value = close.iloc[-1] / prices.high.iloc[:-1].max() - 1
        result["values"][name] = float(value) if value is not None and np.isfinite(value) else None
        if result["values"][name] is None:
            result["missing"][name] = issue or "INVALID_DENOMINATOR"
    prices, issue = window(253)
    discovery = None
    if prices is not None:
        source = frame.copy()
        source["ordinal"] = source.session_date.map(ordinals)
        source["raw_close"], source["raw_volume"] = source.close, source.volume
        discovery = stock_features(source, session, identity)
        issue = discovery["exclusion"] or None
    for name, source_name in (("discovery_state", "state"), ("discovery_trend", "trend")):
        result["values"][name] = discovery[source_name] if discovery and discovery["eligible"] else None
        if result["values"][name] is None:
            result["missing"][name] = issue or "DISCOVERY_STATE_UNAVAILABLE"
    prices, issue = window(2)
    if prices is not None:
        detected = {observation["name"] for observation in detect_setup_candlesticks(prices, input_includes_forming_bar=False)}
        for name, spec in PATTERNS.items():
            result["patterns"][name] = dict(present=spec["label"] in detected, direction=spec["direction"],
                                            interval="1d", state="OCCURRENCE", age=0 if spec["label"] in detected else None,
                                            version=VERSION, session=str(session))
    prices, issue = window(GAP_WINDOW)
    result["gaps"] = dict(version=GAP_VERSION, status="READY" if prices is not None and result["eligible"] else "UNAVAILABLE",
        reason=issue or (None if result["eligible"] else "INSTRUMENT_IDENTITY_UNAVAILABLE"),
        episodes=project_gaps(frame.tail(GAP_WINDOW), identity) if prices is not None and result["eligible"] else [])
    return result


def finalize(rows, manifest, lineage):
    if len(rows) != manifest["expected_members"] or len({row["security_id"] for row in rows}) != len(rows):
        raise ValueError("Full unique expected membership required for publication")
    ranked = sorted((row for row in rows if row["eligible"] and row["values"].get("momentum_12_1") is not None),
                    key=lambda row: (row["values"]["momentum_12_1"], row["security_id"]))
    for row in rows:
        row["values"]["momentum_percentile"] = None
        row["missing"]["momentum_percentile"] = row["missing"].get("momentum_12_1", "INELIGIBLE_RANK_POPULATION")
    for index, row in enumerate(ranked):
        row["values"]["momentum_percentile"] = index / (len(ranked) - 1) if len(ranked) > 1 else .5
        row["missing"].pop("momentum_percentile", None)
    payload = dict(manifest, version=VERSION, field_set_version=FIELD_SET_VERSION, universe=UNIVERSE, interval="1d", rows=rows, lineage=lineage,
                   rank_population=len(ranked), field_coverage={name: sum(row["values"].get(name) is not None for row in rows) for name in FIELDS},
                   pattern_coverage={name: sum(name in row["patterns"] for row in rows) for name in PATTERNS},
                   capture_mode="RECONSTRUCTED_FROM_RETAINED_PUBLICATION", action_coverage="KNOWN_ACTIONS_ONLY_NOT_CERTIFIED",
                   persistence_status="UNAVAILABLE_UNTIL_FIVE_EXPECTED_SCREENING_PUBLICATIONS")
    payload["gap_contract"] = GAP_CATALOG
    payload["gap_coverage"] = dict(ready=sum(row.get("gaps", {}).get("status") == "READY" for row in rows),
        episodes=sum(len(row.get("gaps", {}).get("episodes", [])) for row in rows))
    payload["generation"] = digest(payload)
    return payload


def validate_publication_members(publication, members):
    if not 0 < publication["expected_members"] <= 1000 or len(members) != publication["expected_members"]:
        raise ValueError("Full bounded source publication membership is required")
    if len({str(member["security_id"]) for member in members}) != len(members) or len({member["ticker"] for member in members}) != len(members):
        raise ValueError("Duplicate source publication member")
    selected = [member for member in members if member["status"] == "SELECTED"]
    if len(selected) != publication["selected_members"] or any(not member["selected_bar_revision_id"] for member in selected):
        raise ValueError("Source selected-member count or bar identity is inconsistent")


def build_latest(session_date: date | None = None, *, allow_degraded: bool = False):
    calendar = exchange_calendars.get_calendar("XNYS")
    if session_date is not None and not calendar.is_session(str(session_date)):
        raise ValueError("Requested date is not an exchange session")
    if allow_degraded and session_date is None:
        raise ValueError("Degraded-source preparation requires an explicit session")
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        cursor.execute("SET LOCAL statement_timeout='60s'")
        cursor.execute("""SELECT * FROM equity_bar_publications WHERE interval='1d' AND session_scope='RTH'
                        AND adjusted=FALSE AND ((status='COMPLETE' AND selected_members=expected_members)
                            OR (%s AND status='DEGRADED')) AND (%s::date IS NULL OR market_time::date=%s::date)
                        AND published_at<=now() AND market_time<=now()
                        ORDER BY market_time DESC,published_at DESC,created_at DESC LIMIT 1""", (allow_degraded, session_date, session_date))
        publication = cursor.fetchone()
        if not publication:
            raise ValueError("No compatible retained daily bar publication for the requested session")
        session, cutoff = publication["market_time"].date(), publication["published_at"]
        if not 0 < publication["expected_members"] <= 1000:
            raise ValueError("Expected tracked cohort exceeds 1,000-member bounded build")
        cursor.execute("""SELECT member.*,reference.security_revision_id,reference.company_name,reference.security_type
            FROM equity_bar_publication_members member LEFT JOIN LATERAL (
              SELECT security_revision_id,company_name,security_type FROM equity_security_reference_revisions ref
              WHERE ref.security_id=member.security_id AND ref.ticker=member.ticker AND ref.effective_from<=%s
                AND (ref.source_as_of_date IS NULL OR ref.source_as_of_date<=%s) AND ref.observed_at<=%s AND ref.created_at<=%s
              ORDER BY effective_from DESC,observed_at DESC,security_revision_id LIMIT 1
            ) reference ON TRUE WHERE member.publication_id=%s ORDER BY member.ticker""",
            (publication["market_time"], session, cutoff, cutoff, publication["publication_id"]))
        members = [dict(row) for row in cursor.fetchall()]
        validate_publication_members(publication, members)
        start = calendar.session_offset(str(session), -252).date()
        ordinals = {stamp.date(): index for index, stamp in enumerate(calendar.sessions_in_range(start, session))}
        rows, lineage = [], {}
        for offset in range(0, len(members), 32):
            batch = members[offset:offset + 32]
            tickers = [member["ticker"] for member in batch]
            selected_ids = [member["selected_bar_revision_id"] for member in batch if member["status"] == "SELECTED"]
            cursor.execute("""SELECT DISTINCT ON(ticker,session_date) ticker,security_id,session_date,
                open_price::float8 AS open,high_price::float8 AS high,low_price::float8 AS low,
                close_price::float8 AS close,volume::float8,bar_revision_id
                FROM equity_bar_revisions WHERE ticker=ANY(%s::text[]) AND interval='1d' AND session_scope='RTH'
                AND adjusted=FALSE AND is_final AND session_date BETWEEN %s AND %s
                AND (session_date<%s OR bar_revision_id=ANY(%s::uuid[]))
                AND system_observed_at<=%s AND created_at<=%s
                ORDER BY ticker,session_date,CASE source_kind WHEN 'RECONCILED' THEN 0 WHEN 'DERIVED' THEN 1 ELSE 2 END,
                system_observed_at DESC,created_at DESC,bar_revision_id""", (tickers, start, session, session, selected_ids, cutoff, cutoff))
            bars = [dict(row) for row in cursor.fetchall()]
            cursor.execute("""SELECT corporate_action_id AS action_id,ticker,action_type,effective_date FROM equity_corporate_actions
                WHERE ticker=ANY(%s::text[]) AND effective_date BETWEEN %s AND %s
                AND first_observed_at<=%s AND created_at<=%s""", (tickers, start, session, cutoff, cutoff))
            actions = [dict(row) for row in cursor.fetchall()]
            for member in batch:
                own_bars = [bar for bar in bars if bar["ticker"] == member["ticker"]]
                own_actions = [action for action in actions if action["ticker"] == member["ticker"]]
                rows.append(project_security(member, own_bars, own_actions, session, ordinals))
                lineage[str(member["security_id"])] = dict(bars=[str(bar["bar_revision_id"]) for bar in own_bars],
                                                          actions=[str(action["action_id"]) for action in own_actions])
        manifest = dict(session=str(session), market_time=publication["market_time"].isoformat(),
                source_cutoff=cutoff.isoformat(), source_publication_id=str(publication["publication_id"]),
                previous_expected_session=str(calendar.previous_session(str(session)).date()),
                expected_members=publication["expected_members"],
                processing_policy="screening_complete_expected_members_v1",
                source_publication_status=publication["status"], source_selected_members=publication["selected_members"],
                source_unavailable_members=[dict(ticker=member["ticker"], security_id=str(member["security_id"]), status=member["status"])
                                for member in members if member["status"] != "SELECTED"],
                code_hashes={name: digest(Path(__file__).with_name(name).read_text(encoding="utf-8")) for name in ("screening_projection.py", "technicals.py", "stock_discovery.py")},
                gap_code_hashes={"projector": digest(Path(__file__).with_name("screening_gaps.py").read_text(encoding="utf-8")),
                         "detectors": digest(Path(__file__).parents[1].joinpath("screeners.py").read_text(encoding="utf-8"))},
                contract_hash=digest(dict(fields=FIELDS, patterns=PATTERNS)))
        return attach_hourly(cursor, publication, members, finalize(rows, manifest, lineage), calendar)