"""Verify retained Screening and forward Alerts after a completed session."""
import argparse
from contextlib import closing
from datetime import date, datetime, timezone
import json
from pathlib import Path
import sqlite3
import sys
import zlib

from dotenv import load_dotenv
import exchange_calendars
import pandas as pd

BACKEND_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from equity.screening_api import load_generation, publication_index, query
from equity.stock_alert_views import load_alert_view
from research.screening import Query, digest
from research.stock_idea_forward import READINESS_POLICY, next_boundary
from research.stock_idea_replay import utc


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session", type=date.fromisoformat, required=True)
    parser.add_argument("--state-dir", type=Path,
                        default=BACKEND_DIR / "backups/equity-shadow/stock-ideas-forward-v1")
    args = parser.parse_args()
    calendar = exchange_calendars.get_calendar("XNYS")
    session = pd.Timestamp(args.session)
    if not calendar.is_session(session):
        parser.error("Session must be an XNYS trading date")
    close = calendar.session_close(session).to_pydatetime()
    if datetime.now(timezone.utc) < close:
        parser.error("Session must be completed")

    index = publication_index()
    assert index, "No screening publication"
    generation = load_generation(str(index[0]["snapshot_id"]))
    assert generation["session"] == args.session.isoformat(), "Screening daily anchor is not updated"
    assert utc(generation["market_time"]) == close
    assert utc(generation["hourly_source"]["market_time"]) == close
    assert digest({key: value for key, value in generation.items()
                   if key not in {"generation", "observed_at"}}) == generation["generation"]
    assert len(generation["rows"]) == generation["expected_members"]
    assert len({row["security_id"] for row in generation["rows"]}) == generation["expected_members"]
    response = query(Query(generation=generation["generation"], limit=5))
    assert response["universe_count"] == generation["expected_members"]
    assert response["universe_count"] == sum(response[key] for key in
        ("matched_count", "unknown_count", "nonmatch_count"))
    print(json.dumps(dict(screening=dict(
        session=generation["session"], generation=generation["generation"],
        members=generation["expected_members"], matched=response["matched_count"],
        unknown=response["unknown_count"], daily_cutoff=generation["source_cutoff"],
        hourly_market_time=generation["hourly_source"]["market_time"],
        stale=response["stale"], hourly_stale=response.get("hourly_stale"),
        published_at=generation["observed_at"], integrity="PASS",
    )), indent=2), flush=True)

    path = (args.state_dir / "forward.sqlite").resolve()
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as connection:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        checkpoint = connection.execute("SELECT payload FROM forward_checkpoint").fetchone()
        assert checkpoint, "No retained forward enrollment"
        state = json.loads(zlib.decompress(checkpoint[0]))
        publications = [json.loads(zlib.decompress(row[0])) for row in connection.execute(
            "SELECT payload FROM forward_publications ORDER BY window_key")]
        closing_run = next((row for row in publications if row["window_key"] == close.isoformat()), None)
        assert closing_run, "Forward worker has not yet recorded the closing boundary"
        assert state["dispatch_policy"] == READINESS_POLICY
        assert utc(state["next_boundary"]) >= next_boundary(close)
        if closing_run["coverage"] == "MISSED_PUBLICATION":
            assert not closing_run["selected"], "A missed run must not select plans"
            assert utc(closing_run["actual_publication_at"]) > utc(closing_run["latest_dispatch_at"])
            assert connection.execute("SELECT COUNT(*) FROM forward_outbox WHERE window_key=?",
                                      (closing_run["window_key"],)).fetchone()[0] == 0
        else:
            assert utc(closing_run["actual_publication_at"]) <= utc(closing_run["latest_dispatch_at"])
    view = load_alert_view("SHADOW")
    assert view["worker"]["enrolled_members"] == len(state["members"])
    assert utc(view["worker"]["next_boundary"]) >= next_boundary(close)
    assert any(row["run_id"] == close.isoformat() for row in view["publications"])
    print(json.dumps(dict(forward=dict(
        enrolled_at=state["enrolled_at"], members=len(state["members"]),
        retained_publications=len(publications), closing_boundary=close.isoformat(),
        closing_status=closing_run["coverage"], closing_selected=len(closing_run["selected"]),
        recorded_at=closing_run["actual_publication_at"], deadline=closing_run["latest_dispatch_at"],
        next_boundary=state["next_boundary"], view_checked_at=view["worker"]["checked_at"],
        dispatch_policy=state["dispatch_policy"]["version"], integrity="PASS",
    )), indent=2), flush=True)


if __name__ == "__main__":
    main()