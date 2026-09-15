"""Offline hourly context on exact completed exchange slots, separate from daily rules."""
from datetime import timedelta
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

import numpy as np
import pandas as pd


from research.screening import HOURLY_VERSION, HOURLY_CATALOG, HOURLY_FIELDS, REFRESHED_HOURLY_CATALOG, HOURLY_REFRESH_POLICY, digest
from equity.derivation import derive_canonical_bars
from equity.repositories import _bar_from_row, _security_from_row

HOURLY_WINDOWS = {name: spec["warmup_sessions"] for name, spec in HOURLY_FIELDS.items()}


def hourly_slots(calendar, watermark):
    session = calendar.date_to_session(str(watermark.date()), direction="previous")
    first = calendar.session_offset(session, -9)
    slots = []
    for session_date in calendar.sessions_in_range(first, session):
        start = calendar.session_open(session_date).to_pydatetime()
        close = calendar.session_close(session_date).to_pydatetime()
        while start < close:
            end = min(start + timedelta(hours=1), close)
            if end <= watermark:
                slots.append((start, end))
            start = end
    return slots[-23:]


def project_hourly(identity, selected_id, bars, actions, slots, *, source_ready=True):
    result = dict(values={}, missing={})
    indexed = {bar["bar_end"]: bar for bar in bars}
    for name, size in HOURLY_WINDOWS.items():
        expected = slots[-size:]
        window = [indexed.get(end) for _, end in expected]
        issue = None
        if not source_ready:
            issue = "HOURLY_PUBLICATION_UNAVAILABLE"
        elif not selected_id:
            issue = "HOURLY_MEMBER_UNAVAILABLE"
        elif not window or window[-1] is None or str(window[-1]["bar_revision_id"]) != str(selected_id):
            issue = "MISSING_EXACT_HOURLY_BAR"
        elif len(expected) != size or any(bar is None for bar in window):
            issue = "MISSING_EXPECTED_HOURLY_SLOT"
        elif any(bar["bar_start"] != start or bar["bar_end"] != end for (start, end), bar in zip(expected, window)):
            issue = "INVALID_HOURLY_SLOT"
        elif any(str(bar["security_id"]) != str(identity) for bar in window):
            issue = "HOURLY_IDENTITY_CHANGED"
        if issue is None:
            prices = pd.DataFrame(window)[["open", "high", "low", "close", "volume"]].astype(float)
            if not np.isfinite(prices).all().all() or (prices[["open", "high", "low", "close"]] <= 0).any().any() or (prices.volume < 0).any():
                issue = "INVALID_HOURLY_BAR"
            elif (prices.high < prices[["open", "close", "low"]].max(axis=1)).any() or (prices.low > prices[["open", "close", "high"]].min(axis=1)).any():
                issue = "INVALID_HOURLY_GEOMETRY"
            elif size > 1 and any(expected[0][0].date() <= action["effective_date"] <= expected[-1][1].date()
                                  and action["action_type"] in ("SPLIT", "MERGER", "SPINOFF", "SYMBOL_CHANGE") for action in actions):
                issue = "HOURLY_CORPORATE_ACTION_REVIEW"
        result["values"][name] = None
        if issue:
            result["missing"][name] = issue
            continue
        closes = prices.close
        if name == "close":
            value = closes.iloc[-1]
        elif name == "change":
            value = closes.iloc[-1] / closes.iloc[-2] - 1
        else:
            average = closes.ewm(span=20, adjust=False).mean()
            value = closes.iloc[-1] / average.iloc[-1] - 1 if name == "vs_ema20" else average.iloc[-1] / average.iloc[-4] - 1
        result["values"][name] = float(value)
    return result


def complete_retained_slots(security, bars, sources, slots, cutoff):
    existing = {bar["bar_end"] for bar in bars}
    missing = {end for _, end in slots[:-1]} - existing
    if not missing or not sources or security is None:
        return bars, {}
    compatible = [bar for bar in sources if bar.security_id == security.security_id and bar.ticker == security.ticker
                  and bar.system_observed_at <= cutoff and bar.bar_end <= cutoff]
    derived = derive_canonical_bars(security, compatible, target_interval="1h", observed_at=cutoff,
        ingestion_segment_id=uuid5(NAMESPACE_URL, f"{HOURLY_VERSION}:retained-30m:{cutoff.isoformat()}"))
    result, lineage = list(bars), {}
    for bar in derived:
        if bar.bar_end not in missing:
            continue
        result.append(dict(bar_start=bar.bar_start, bar_end=bar.bar_end, security_id=bar.security_id,
            bar_revision_id=bar.bar_revision_id, open=float(bar.open_price), high=float(bar.high_price),
            low=float(bar.low_price), close=float(bar.close_price), volume=float(bar.volume)))
        lineage[str(bar.bar_revision_id)] = [str(identity) for identity in bar.source_bar_revision_ids]
    return sorted(result, key=lambda bar: bar["bar_end"]), lineage


def attach_hourly(cursor, daily_publication, members, payload, calendar, *, hourly_publication=None):
    refreshed = hourly_publication is not None
    watermark = hourly_publication["market_time"] if refreshed else daily_publication["market_time"]
    cutoff = hourly_publication["published_at"] if refreshed else daily_publication["published_at"]
    if refreshed and watermark < daily_publication["market_time"]:
        raise ValueError("Hourly publication must not precede the daily anchor")
    slots = hourly_slots(calendar, watermark)
    if not slots or slots[-1][1] != watermark:
        raise ValueError("Watermark must end an expected hourly slot")
    if refreshed:
        publication = hourly_publication
        if publication["interval"] != "1h" or publication["session_scope"] != "RTH" or publication["adjusted"] or publication["status"] not in ("COMPLETE", "DEGRADED"):
            raise ValueError("Unsupported hourly publication")
    else:
        cursor.execute("""SELECT * FROM equity_bar_publications WHERE interval='1h' AND session_scope='RTH'
            AND adjusted=FALSE AND status IN ('COMPLETE','DEGRADED') AND market_time<=%s
            AND published_at<=%s AND created_at<=%s
            ORDER BY market_time DESC,published_at DESC,created_at DESC LIMIT 1""", (watermark, cutoff, cutoff))
        publication = cursor.fetchone()
    source_ready = publication is not None and publication["market_time"] == watermark
    source_members = {}
    if source_ready:
        cursor.execute("SELECT * FROM equity_bar_publication_members WHERE publication_id=%s", (publication["publication_id"],))
        available = [dict(row) for row in cursor.fetchall()]
        if len(available) != publication["expected_members"] or not 0 < len(available) <= 1000:
            raise ValueError("Hourly publication membership is incomplete or exceeds its bound")
        source_members = {(str(member["security_id"]), member["ticker"]): member for member in available}
        if len(source_members) != len(available) or sum(member["status"] == "SELECTED" for member in available) != publication["selected_members"]:
            raise ValueError("Hourly publication membership does not reconcile")
    contexts, lineage = {}, {}
    for offset in range(0, len(members), 32):
        batch = members[offset:offset + 32]
        tickers = [member["ticker"] for member in batch]
        selected = {str(member["security_id"]): source_members.get((str(member["security_id"]), member["ticker"]), {}) for member in batch}
        selected_ids = [member["selected_bar_revision_id"] for member in selected.values() if member.get("status") == "SELECTED" and member.get("selected_bar_revision_id")]
        bars, actions, source_bars, references = [], [], [], {}
        if source_ready:
            hourly_cutoff = publication["published_at"]
            cursor.execute("""SELECT DISTINCT ON(ticker,bar_end) ticker,security_id,bar_revision_id,bar_start,bar_end,
                open_price::float8 AS open,high_price::float8 AS high,low_price::float8 AS low,
                close_price::float8 AS close,volume::float8 FROM equity_bar_revisions
                WHERE ticker=ANY(%s::text[]) AND interval='1h' AND session_scope='RTH' AND adjusted=FALSE AND is_final
                AND bar_start>=%s AND bar_end<=%s AND (bar_end<%s OR bar_revision_id=ANY(%s::uuid[]))
                AND system_observed_at<=%s AND created_at<=%s
                ORDER BY ticker,bar_end,CASE source_kind WHEN 'RECONCILED' THEN 0 WHEN 'DERIVED' THEN 1 ELSE 2 END,
                system_observed_at DESC,created_at DESC,bar_revision_id""",
                (tickers, slots[0][0], watermark, watermark, selected_ids, hourly_cutoff, hourly_cutoff))
            bars = [dict(row) for row in cursor.fetchall()]
            cursor.execute("""SELECT ticker,action_type,effective_date FROM equity_corporate_actions
                WHERE ticker=ANY(%s::text[]) AND effective_date BETWEEN %s AND %s AND first_observed_at<=%s AND created_at<=%s""",
                (tickers, min(slots[0][0].date(), daily_publication["market_time"].date()), watermark.date(), hourly_cutoff, hourly_cutoff))
            actions = [dict(row) for row in cursor.fetchall()]
            missing_tickers = [ticker for ticker in tickers if not {end for _, end in slots[:-1]}.issubset({bar["bar_end"] for bar in bars if bar["ticker"] == ticker})]
            if missing_tickers:
                cursor.execute("""SELECT DISTINCT ON(ticker,bar_start) * FROM equity_bar_revisions
                    WHERE ticker=ANY(%s::text[]) AND interval='30m' AND session_scope='RTH' AND adjusted=FALSE AND is_final
                    AND bar_start>=%s AND bar_end<%s AND system_observed_at<=%s AND created_at<=%s
                    ORDER BY ticker,bar_start,CASE source_kind WHEN 'RECONCILED' THEN 0 WHEN 'NATIVE_REST' THEN 1 ELSE 2 END,
                    system_observed_at DESC,created_at DESC,bar_revision_id""",
                    (missing_tickers, slots[0][0], watermark, hourly_cutoff, hourly_cutoff))
                source_bars = [_bar_from_row(dict(row)) for row in cursor.fetchall()]
                cursor.execute("SELECT * FROM equity_security_reference_revisions WHERE security_revision_id=ANY(%s::uuid[])",
                    ([member["security_revision_id"] for member in batch if member["ticker"] in missing_tickers and member.get("security_revision_id")],))
                references = {str(row["security_id"]): _security_from_row(dict(row)) for row in cursor.fetchall()}
        for member in batch:
            identity = str(member["security_id"])
            own = [bar for bar in bars if bar["ticker"] == member["ticker"]]
            own, reconstructed = complete_retained_slots(references.get(identity), own,
                [bar for bar in source_bars if bar.ticker == member["ticker"]], slots, publication["published_at"]) if source_ready else (own, {})
            selected_id = selected[identity].get("selected_bar_revision_id") if selected[identity].get("status") == "SELECTED" else None
            contexts[identity] = project_hourly(identity, selected_id, own,
                [action for action in actions if action["ticker"] == member["ticker"]], slots, source_ready=source_ready)
            if refreshed and any(action["ticker"] == member["ticker"] and daily_publication["market_time"].date() < action["effective_date"] <= watermark.date()
                                 and action["action_type"] in ("SPLIT", "MERGER", "SPINOFF", "SYMBOL_CHANGE") for action in actions):
                contexts[identity] = dict(values={name: None for name in HOURLY_FIELDS},
                    missing={name: "DAILY_HOURLY_CORPORATE_ACTION_REVIEW" for name in HOURLY_FIELDS})
            lineage[identity] = dict(bars=[str(bar["bar_revision_id"]) for bar in own], selected_bar_id=str(selected_id) if selected_id else None,
                                     reconstructed_from_30m=reconstructed)
    for row in payload["rows"]:
        row["hourly"] = contexts[row["security_id"]]
    payload["hourly_contract"] = REFRESHED_HOURLY_CATALOG if refreshed else HOURLY_CATALOG
    payload["hourly_source"] = dict(status="READY" if source_ready else "STALE_AT_CUTOFF" if publication else "UNAVAILABLE",
        expected_market_time=watermark.isoformat(), market_time=publication["market_time"].isoformat() if publication else None,
        source_cutoff=publication["published_at"].isoformat() if publication else None,
        source_publication_id=str(publication["publication_id"]) if publication else None,
        slot_start=slots[-1][0].isoformat(), slot_minutes=int((slots[-1][1] - slots[-1][0]).total_seconds() / 60),
        snapshot_cutoff=daily_publication["published_at"].isoformat(), code_hash=digest(Path(__file__).read_text(encoding="utf-8")),
        derivation_code_hash=digest(Path(__file__).with_name("derivation.py").read_text(encoding="utf-8")),
        reconstructed_slots=sum(len(source["reconstructed_from_30m"]) for source in lineage.values()))
    if refreshed:
        payload["hourly_refresh_policy"] = HOURLY_REFRESH_POLICY
        payload["hourly_source"]["capture"] = REFRESHED_HOURLY_CATALOG["capture"]
    payload["hourly_coverage"] = {name: sum(context["values"][name] is not None for context in contexts.values()) for name in HOURLY_FIELDS}
    payload["hourly_lineage"] = lineage
    payload.pop("generation")
    payload["generation"] = digest(payload)
    return payload