"""Source-separated read adapters for the Alerts workspace; no capture on GET."""
from datetime import datetime, timedelta, timezone
import logging
import math
import os
from pathlib import Path
from zoneinfo import ZoneInfo

from psycopg2 import Error as DatabaseError

from database import get_db_cursor
from research.stock_alerts import SCHEMA, empty_snapshot, history_publications, load_snapshot, session_dates


def load_alert_view(source, *, combined=None):
    if source == "LEGACY":
        return legacy_view()
    if source == "SHADOW":
        marker = Path(__file__).resolve().parents[1] / "backups/stock-alert-results/reader.json"
        if combined is True or (combined is None and marker.is_file()):
            from equity.stock_alert_results import load_shared_results
            return load_shared_results()
        path = os.getenv("STOCK_ALERT_SHADOW_VIEW") or str(Path(__file__).resolve().parents[1] / "backups/equity-shadow/stock-ideas-forward-v2/alerts-view.json")
        return load_snapshot(path, source)
    path = os.getenv("STOCK_ALERT_REPLAY_VIEW") or str(Path(__file__).resolve().parents[1] / "backups/stock-idea-independent-v1/alerts-view.json")
    return load_snapshot(path, source)


def forward_session_snapshot(snapshot, *, as_of=None):
    if snapshot.get("source") != "SHADOW":
        return snapshot
    now = as_of or datetime.now(timezone.utc)
    dates = session_dates(now.astimezone(ZoneInfo("America/New_York")).date().isoformat())
    return dict(snapshot, sessions=dates, current_session=dates[-1])


def current_alert_schedules(snapshot, *, as_of=None):
    if snapshot.get("source") != "SHADOW":
        return []
    now = as_of or datetime.now(timezone.utc)
    session = now.astimezone(ZoneInfo("America/New_York")).date().isoformat()
    backend = Path(__file__).resolve().parents[1]
    sources = [("swing" if snapshot.get("source_id") == "stock_ideas_forward_swing_v1" else "intraday", snapshot)]
    if snapshot.get("combined"):
        sources = [("intraday", Path(os.getenv("STOCK_ALERT_SHADOW_VIEW") or backend / "backups/equity-shadow/stock-ideas-forward-v2/alerts-view.json")),
            ("swing", backend / "backups/equity-shadow/stock-ideas-swing-v1/alerts-view.json")]
    result = []
    for stream, source in sources:
        row = dict(stream=stream, label="Swing" if stream == "swing" else "Intraday", status="UNAVAILABLE")
        try:
            if isinstance(source, Path):
                if source.stat().st_size > 20_000_000:
                    raise ValueError("worker schedule snapshot exceeds bound")
                source = load_snapshot(source, "SHADOW")
            source_time = datetime.fromisoformat(source["as_of"])
            start = datetime.fromisoformat(source["publication_window_start"])
            end = datetime.fromisoformat(source["publication_deadline"])
            if any(value.utcoffset() is None for value in (source_time, start, end)) or source_time > now or end < start:
                raise ValueError("worker schedule clocks invalid")
            runs = [run for run in source.get("publications", []) if run["session"] == session]
            latest = max(runs, key=lambda run: run["trigger_at"], default=None)
            row.update(status="OVERDUE" if end < now else "RETAINED", source_as_of=source_time.isoformat(),
                publication_window_start=start.isoformat(), publication_deadline=end.isoformat(),
                skipped_windows=sum(run["status"] == "MISSED_PUBLICATION" for run in runs),
                latest_run={key: latest[key] for key in ("trigger_at", "published_at", "status", "selected")} if latest else None)
        except (OSError, ValueError, KeyError, TypeError):
            pass
        result.append(row)
    return result


def attach_alert_context(page):
    if page.get("combined"):
        from equity.stock_alert_results import attach_shared_context
        return attach_shared_context(page)
    from research.stock_alert_annotations import attach_publication_context
    path = os.getenv("STOCK_ALERT_SHADOW_VIEW") or str(Path(__file__).resolve().parents[1] / "backups/equity-shadow/stock-ideas-forward-v2/alerts-view.json")
    return attach_publication_context(page, Path(path).parent / "alert-context")


def history_snapshot_for_date(snapshot, session=None):
    if snapshot["source"] != "SHADOW" or snapshot.get("combined"):
        return snapshot
    dates = snapshot.get("sessions", [])
    selected = session or (dates[-1] if dates else None)
    if dates and selected not in dates:
        return snapshot
    publications = snapshot.get("publications", [])
    if any(row["session"] == selected for row in publications):
        return snapshot
    enrolled = snapshot.get("enrolled_at")
    first_forward_session = (datetime.fromisoformat(enrolled.replace("Z", "+00:00"))
        .astimezone(ZoneInfo("America/New_York")).date().isoformat()) if enrolled else min(
            (row["session"] for row in publications), default=None)
    if first_forward_session and selected and selected >= first_forward_session:
        return snapshot
    replay = load_alert_view("REPLAY")
    if selected and selected not in replay.get("sessions", []):
        return snapshot
    return dict(replay, sessions=list(dates or replay.get("sessions", [])))


def current_history_prices(snapshot, session=None, *, now=None, view="history"):
    if snapshot["source"] != "SHADOW":
        return snapshot
    dates = snapshot.get("sessions", [])
    selected_date = session or (dates[-1] if dates else None)
    if view != "open" and selected_date not in dates:
        return snapshot
    runs = {row["run_id"] for row in history_publications(snapshot, selected_date)}
    selected = [row for row in snapshot.get("alerts", []) if row["status"] in ("PENDING", "OPEN", "UNRESOLVED")] if view == "open" else [row for row in snapshot.get("alerts", []) if row["run_id"] in runs]
    selected_ids = {row["alert_id"] for row in selected}
    identities = sorted({(row["security_id"], row["ticker"]) for row in selected})
    if not identities:
        return snapshot
    if len(identities) > 1000:
        raise ValueError("current alert prices exceed the bounded identity read")
    now = now or datetime.now(timezone.utc)
    from equity.api import expected_materialized_market_time
    expected = expected_materialized_market_time(now, "5m")
    quotes, actions, failed = {}, {}, False
    try:
        with get_db_cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout='10s'")
            cursor.execute("""WITH requested AS (
                SELECT * FROM unnest(%s::text[],%s::text[]) AS member(security_id,ticker)
            ), latest AS (
                SELECT requested.security_id,quote.* FROM requested
                CROSS JOIN unnest(ARRAY['5m','15m','30m','1h','1d']) AS frame(interval)
                CROSS JOIN LATERAL (
                    SELECT close_price::float8 AS latest_price,bar_end AS latest_price_at,
                        bar_revision_id::text AS latest_price_revision_id,interval AS latest_price_interval,
                        system_observed_at AS latest_price_observed_at,created_at AS latest_price_created_at
                    FROM equity_bar_revisions bar
                    WHERE bar.ticker=requested.ticker AND bar.security_id=requested.security_id::uuid
                      AND bar.interval=frame.interval AND bar.session_scope='RTH' AND NOT bar.adjusted AND bar.is_final
                      AND bar.bar_start>=%s AND bar.bar_end<=%s AND bar.system_observed_at<=%s AND bar.created_at<=%s
                      AND bar.close_price>0 AND bar.volume>0
                    ORDER BY bar.bar_end DESC,CASE bar.source_kind WHEN 'RECONCILED' THEN 0 WHEN 'DERIVED' THEN 1 ELSE 2 END,
                        bar.system_observed_at DESC,bar.created_at DESC,bar.bar_revision_id LIMIT 1
                ) quote
            ) SELECT DISTINCT ON(security_id) * FROM latest
              ORDER BY security_id,latest_price_at DESC,
                array_position(ARRAY['5m','15m','30m','1h','1d'],latest_price_interval::text),latest_price_revision_id""",
                    ([identity[0] for identity in identities], [identity[1] for identity in identities], now - timedelta(days=7), expected, now, now))
            quotes = {row["security_id"]: dict(row) for row in cursor.fetchall()
                        if math.isfinite(row["latest_price"]) and row["latest_price"] > 0 and row["latest_price_at"] <= expected}
            earliest = min(datetime.fromisoformat(row["triggered_at"].replace("Z", "+00:00")).date() for row in selected)
            cursor.execute("""SELECT security_id::text,effective_date,action_type FROM equity_corporate_actions
                WHERE security_id=ANY(%s::uuid[]) AND effective_date>=%s AND effective_date<=%s
                  AND action_type IN ('SPLIT','MERGER','SYMBOL_CHANGE','SPINOFF')
                  AND first_observed_at<=%s AND created_at<=%s ORDER BY effective_date,action_type""",
                ([identity[0] for identity in identities], earliest, now.date(), now, now))
            for action in cursor.fetchall():
                actions.setdefault(action["security_id"], []).append(dict(action))
    except DatabaseError:
        logging.getLogger(__name__).warning("Current alert price read unavailable", exc_info=True)
        quotes, failed = {}, True
    records = []
    for original in snapshot.get("alerts", []):
        if original["alert_id"] not in selected_ids:
            records.append(original)
            continue
        quote = quotes.get(original["security_id"], {})
        row = dict(original, latest_price=None, latest_price_at=None, price_comparison_block=None,
            latest_price_source="CANONICAL_FINAL_RTH", latest_price_checked_at=now.isoformat(),
            warnings=[warning for warning in original.get("warnings", []) if warning != "Latest stored price may be stale; inspect its timestamp"])
        if quote:
            row.update({key: value.isoformat() if isinstance(value, datetime) else value for key, value in quote.items() if key != "security_id"})
            trigger_date = datetime.fromisoformat(row["triggered_at"].replace("Z", "+00:00")).date()
            if any(trigger_date <= action["effective_date"] <= quote["latest_price_at"].date() for action in actions.get(row["security_id"], [])):
                row["price_comparison_block"] = "KNOWN_CORPORATE_ACTION"
                row["warnings"].append("Trigger-to-current price comparison unavailable across a known corporate action")
            if quote["latest_price_at"] < expected:
                row["warnings"].append("Current stored price is stale; inspect its timestamp")
        else:
            row["warnings"].append("Current stored price is unavailable")
        records.append(row)
    warnings = list(snapshot.get("warnings", []))
    if failed:
        warnings.append("Current price refresh unavailable; retained paper outcomes are unchanged")
    return dict(snapshot, alerts=records, warnings=warnings, price_as_of=now.isoformat())


def legacy_view():
    from equity.stock_discovery import VERSION, SNAPSHOT_SOURCE, ALERT_SOURCE, MARK_SOURCE
    from equity.api import expected_materialized_market_time
    now = datetime.now(timezone.utc)
    expected_mark = expected_materialized_market_time(now, "30m")
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout='15s'")
        cursor.execute("""SELECT market_time,MAX(observed_at) AS published_at,COUNT(*) AS expected
            FROM equity_evidence WHERE source_name=%s AND source_version=%s AND observed_at<=%s
              AND market_time>=%s GROUP BY market_time ORDER BY market_time""",
                       (SNAPSHOT_SOURCE, VERSION, now, now - timedelta(days=70)))
        runs = [dict(row) for row in cursor.fetchall()]
        if not runs:
            return empty_snapshot("LEGACY")
        dates = session_dates(str(runs[-1]["market_time"].date()))
        cursor.execute("""SELECT alert.evidence_id,alert.security_id,alert.ticker,alert.direction,alert.market_time,
                alert.observed_at,alert.payload,mark.payload AS paper,mark.observed_at AS evaluated_at
            FROM equity_evidence alert
            LEFT JOIN LATERAL(SELECT payload,observed_at FROM equity_evidence mark
                WHERE mark.source_name=%s AND mark.source_version=%s AND mark.payload->>'alert_id'=alert.evidence_id::text
                  AND mark.observed_at<=%s AND mark.created_at<=%s
                ORDER BY mark.observed_at DESC,mark.created_at DESC,mark.evidence_id LIMIT 1) mark ON TRUE
            WHERE alert.source_name=%s AND alert.source_version=%s AND alert.market_time>=%s::date
              AND alert.observed_at<=%s AND alert.created_at<=%s
            ORDER BY alert.observed_at,alert.evidence_id LIMIT 10001""",
            (MARK_SOURCE, VERSION, now, now, ALERT_SOURCE, VERSION, dates[0], now, now))
        records = [dict(row) for row in cursor.fetchall()]
        if len(records) > 10000:
            raise ValueError("legacy alert view exceeds its bounded read limit")
        identities = sorted({str(row["security_id"]) for row in records})
        quotes = {}
        if identities:
            cursor.execute("""SELECT DISTINCT ON(security_id) security_id::text,close_price::float8 AS price,bar_end
                FROM equity_bar_revisions WHERE security_id=ANY(%s::uuid[]) AND session_scope='RTH'
                  AND interval='30m' AND NOT adjusted AND is_final AND bar_end<=%s
                  AND bar_start>=%s AND system_observed_at<=%s AND created_at<=%s
                ORDER BY security_id,bar_end DESC,system_observed_at DESC,created_at DESC,bar_revision_id""",
                (identities, now, now - timedelta(days=7), now, now))
            quotes = {str(row["security_id"]): dict(row) for row in cursor.fetchall()}
    publications = [dict(run_id=row["market_time"].isoformat(), session=str(row["market_time"].date()),
        trigger_at=row["market_time"].isoformat(), published_at=row["published_at"].isoformat(), status="LEGACY_CAPTURE",
        expected=row["expected"], missing=None, selected=sum(record["market_time"] == row["market_time"] for record in records), conflicts=None)
        for row in runs if str(row["market_time"].date()) in dates]
    alerts = []
    for record in records:
        data, paper = record["payload"], record["paper"] or {}
        status = paper.get("status", "WAITING_FOR_EVALUATION")
        quote = quotes.get(str(record["security_id"]), {})
        closed = status == "CLOSED_PAPER"
        warnings = ["Legacy daily policy", "Structural stop/target not retained", "Recurrence observations unavailable"]
        if not quote or quote["bar_end"] < expected_mark:
            warnings.append("Latest stock price is stale or unavailable")
        mark_time = datetime.fromisoformat(paper["mark_time"]) if paper.get("mark_time") else None
        if status == "OPEN_PAPER" and (mark_time is None or mark_time < expected_mark):
            warnings.append("Paper mark is stale; P/L uses the last retained mark")
        alerts.append(dict(alert_id=str(record["evidence_id"]), run_id=record["market_time"].isoformat(),
            security_id=str(record["security_id"]), ticker=record["ticker"], company_name=None, direction=record["direction"],
            model="legacy_daily", interval="1d", lane="WATCH" if record["direction"] == 0 else "TRADE",
            triggered_at=record["market_time"].isoformat(), published_at=record["observed_at"].isoformat(),
            trigger_price=data.get("signal_price"), entry_price=paper.get("entry_price"), entry_at=paper.get("entry_time"),
            stop=None, target=None, risk_pct=None, reward_risk=None, entry_risk=None, hold="21 sessions",
            exit_due_at=None, status=status, reason=None, exit_price=paper.get("mark_price") if closed else None,
            exit_at=paper.get("mark_time") if closed else None, paper_return=paper.get("return_fraction"),
            latest_price=quote.get("price"), latest_price_at=quote["bar_end"].isoformat() if quote else None,
            mark_price=paper.get("mark_price"), mark_at=paper.get("mark_time"),
            indicators=dict(momentum=data.get("momentum"), sector=data.get("sector")),
            indicator_at=record["market_time"].isoformat(), indicator_interval="1d", daily_context_at=record["market_time"].isoformat(),
            indicator_status="LEGACY_RETAINED_FIELDS", warnings=warnings,
            policy_version=VERSION))
    return dict(schema=SCHEMA, source="LEGACY", source_id=VERSION, source_label="Legacy daily / paper tracking",
        status="READY", as_of=now.isoformat(), sessions=dates, publications=publications, alerts=alerts,
        observations=[], hit_coverage=None, outcome_policy="Legacy next-session / 10 bps round trip",
        indicators_available=["momentum", "sector"], warnings=["Legacy daily policy, separate from multi-model research", "Price and paper-mark timestamps are independent"])