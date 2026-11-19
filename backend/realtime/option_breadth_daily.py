"""G106 H1: the daily close chain snapshot (`rt.option_breadth_daily`, migration 055).

`rt.option_breadth_current` keeps one row per contract and session, replaced at every chain capture (G75), so each
session's last capture survives there and nothing else does. After the session close this module copies a session's
rows once into the daily table: history (IV rank and percentile, day-over-day open interest change, skew history,
outcome research) only has value once it has accumulated.

- `snapshot_session_cursor`: one session from the current table, INSERT ... SELECT ... ON CONFLICT DO NOTHING, so a
  re-run writes nothing new. It refuses a session that is still open: before the exchange close plus
  `SNAPSHOT_MARGIN` (the last capture of a window can land until the decision stop, about 21:05 UTC in winter). Any
  session already past that point (a backfill) is accepted.
- `load_csv_session_cursor`: the same rows from a CSV copy of the current table (`COPY (SELECT * FROM
  rt.option_breadth_current WHERE session_date = X ...) TO STDOUT WITH (FORMAT csv, HEADER true)`, optionally gzip), for
  a session whose current rows are gone.
- `ClosedSessionSnapshotter`: the automatic call in the Options scheduler once the session's decision stop has passed,
  behind `REALTIME_V3_OPTIONS_BREADTH_DAILY_SNAPSHOT` (default OFF: nothing is read or written).

Every row keeps its capture time (`observed_at`) and says whether it belongs to its underlying's last capture of the
session (`final_capture`, the G102 R55 rule); `snapshot_at`, `snapshot_source` and `source_reference` say when and from
what it was copied. The CLI is `scripts/snapshot_option_breadth_daily.py`.
"""
from __future__ import annotations

import gzip
import hashlib
import io
import json
import logging
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Mapping

UTC = timezone.utc
_LOGGER = logging.getLogger(__name__)

SNAPSHOT_SWITCH = "REALTIME_V3_OPTIONS_BREADTH_DAILY_SNAPSHOT"
SNAPSHOT_MARGIN = timedelta(minutes=20)
CURRENT_TABLE_SOURCE = "CURRENT_TABLE"
CSV_SOURCE = "CSV_COPY"
CURRENT_TABLE_REFERENCE = "rt.option_breadth_current"

# The current table's columns, in its order (migration 052); a CSV copy's header must equal this.
BREADTH_COLUMNS = (
    "session_date", "contract_ticker", "underlying", "contract_type", "expiration_date", "strike_price", "day_volume",
    "open_interest", "last_trade_price", "last_trade_at", "bid", "ask", "quote_at", "implied_volatility", "delta",
    "gamma", "theta", "vega", "underlying_price", "local_iv", "local_delta", "local_gamma", "local_theta_per_day",
    "local_vega_per_vol_point", "local_rho_per_rate_point", "spot", "risk_free_rate", "dividend_yield",
    "valuation_basis", "greeks_reason", "equity_cutoff", "observed_at",
)
_COLUMN_LIST = ",".join(BREADTH_COLUMNS)
_DAILY_COLUMNS = _COLUMN_LIST + ",final_capture,snapshot_at,snapshot_source,source_reference"
_CSV_STAGE = "option_breadth_csv_stage"


class SessionStillOpen(ValueError):
    """The session has not closed (plus the margin) yet: its chain can still change."""


SNAPSHOT_SETTING_EVENT = "OPTIONS_BREADTH_DAILY_SNAPSHOT_SETTING"
SNAPSHOT_MODES = ("OFF", "ON")


def parse_snapshot_mode(raw: object) -> str:
    """`REALTIME_V3_OPTIONS_BREADTH_DAILY_SNAPSHOT`: unset or blank is OFF; OFF or ON in any case, spaces trimmed.
    Anything else raises, so the Options scheduler refuses to start (as its other switches do)."""
    if raw is not None and not isinstance(raw, str):
        raise ValueError(f"{SNAPSHOT_SWITCH} must be OFF or ON, not {raw!r}")
    value = (raw or "").strip().upper() or "OFF"
    if value not in SNAPSHOT_MODES:
        raise ValueError(f"{SNAPSHOT_SWITCH} must be OFF or ON, not {raw!r}")
    return value


def snapshot_mode(environment: Mapping[str, str] | None = None) -> str:
    return parse_snapshot_mode((os.environ if environment is None else environment).get(SNAPSHOT_SWITCH))


def snapshot_enabled(environment: Mapping[str, str] | None = None) -> bool:
    return snapshot_mode(environment) == "ON"


def setting_line(mode: str) -> str:
    """The scheduler's start line when the switch is ON (OFF logs nothing)."""
    return SNAPSHOT_SETTING_EVENT + " " + json.dumps(
        {"mode": mode, "setting": SNAPSHOT_SWITCH, "margin_minutes": int(SNAPSHOT_MARGIN.total_seconds() // 60)},
        sort_keys=True, separators=(",", ":"),
    )


def _calendar(calendar=None):
    if calendar is not None:
        return calendar
    from options.calendar import OptionExchangeCalendar

    return OptionExchangeCalendar()


def _utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def snapshot_allowed_at(session_date: date, *, calendar=None) -> datetime:
    """The exchange close of `session_date` plus the margin; ValueError when it is not an exchange session."""
    try:
        close = _calendar(calendar).session_close(session_date)
    except Exception as error:  # noqa: BLE001 - exchange_calendars raises its own types for a non-session
        raise ValueError(f"{session_date} is not an exchange session") from error
    return _utc(close, "session close") + SNAPSHOT_MARGIN


def require_closed_session(session_date: date, *, now: datetime, calendar=None) -> datetime:
    allowed = snapshot_allowed_at(session_date, calendar=calendar)
    if _utc(now, "now") < allowed:
        raise SessionStillOpen(
            f"session {session_date} is still open: the snapshot is allowed from {allowed.isoformat()}"
        )
    return allowed


def _rows(cursor) -> list[tuple]:
    return [tuple(row.values()) if isinstance(row, Mapping) else tuple(row) for row in cursor.fetchall()]


def _counts_by_underlying(cursor, relation: str, session_date: date) -> dict[str, int]:
    cursor.execute(
        f"SELECT underlying,count(*) FROM {relation} WHERE session_date=%s GROUP BY underlying ORDER BY underlying",
        (session_date,),
    )
    return {str(underlying): int(count) for underlying, count in _rows(cursor)}


def _missing_count(cursor, session_date: date) -> int:
    cursor.execute(
        "SELECT count(*) FROM rt.option_breadth_current AS current WHERE current.session_date=%s "
        "AND NOT EXISTS (SELECT 1 FROM rt.option_breadth_daily AS daily "
        "WHERE daily.session_date=current.session_date AND daily.contract_ticker=current.contract_ticker)",
        (session_date,),
    )
    return int(_rows(cursor)[0][0])


def _insert_report(cursor) -> dict[str, dict[str, int]]:
    return {
        str(underlying): {"inserted": int(inserted), "inserted_final_capture": int(final)}
        for underlying, inserted, final in _rows(cursor)
    }


def _report(*, session_date, source, reference, source_counts, inserted, daily_counts, missing) -> dict[str, object]:
    by_underlying = {}
    for underlying in sorted(set(source_counts) | set(daily_counts) | set(inserted)):
        written = inserted.get(underlying, {"inserted": 0, "inserted_final_capture": 0})
        by_underlying[underlying] = {
            "source_rows": source_counts.get(underlying, 0),
            **written,
            "already_present": source_counts.get(underlying, 0) - written["inserted"],
            "daily_rows": daily_counts.get(underlying, 0),
        }
    source_rows = sum(source_counts.values())
    written = sum(item["inserted"] for item in inserted.values())
    return {
        "session_date": session_date.isoformat(),
        "snapshot_source": source,
        "source_reference": reference,
        "source_rows": source_rows,
        "inserted": written,
        "already_present": source_rows - written,
        "daily_rows": sum(daily_counts.values()),
        "missing_from_daily": missing,
        "complete": missing == 0 and source_rows > 0,
        "by_underlying": by_underlying,
    }


def snapshot_session_cursor(cursor, *, session_date: date, now: datetime, calendar=None) -> dict[str, object]:
    """Copy one closed session's rows from the current table (idempotent). Raises SessionStillOpen for an open one."""
    require_closed_session(session_date, now=now, calendar=calendar)
    snapshot_at = _utc(now, "now")
    source_counts = _counts_by_underlying(cursor, "rt.option_breadth_current", session_date)
    cursor.execute(
        "WITH inserted AS ("
        f"INSERT INTO rt.option_breadth_daily ({_DAILY_COLUMNS}) "
        f"SELECT {','.join('current.' + name for name in BREADTH_COLUMNS)},"
        "current.observed_at=max(current.observed_at) OVER (PARTITION BY current.underlying),"
        "%s,%s,%s FROM rt.option_breadth_current AS current WHERE current.session_date=%s "
        "ON CONFLICT (session_date,contract_ticker) DO NOTHING RETURNING underlying,final_capture) "
        "SELECT underlying,count(*),count(*) FILTER (WHERE final_capture) FROM inserted "
        "GROUP BY underlying ORDER BY underlying",
        (snapshot_at, CURRENT_TABLE_SOURCE, CURRENT_TABLE_REFERENCE, session_date),
    )
    inserted = _insert_report(cursor)
    return _report(
        session_date=session_date, source=CURRENT_TABLE_SOURCE, reference=CURRENT_TABLE_REFERENCE,
        source_counts=source_counts, inserted=inserted,
        daily_counts=_counts_by_underlying(cursor, "rt.option_breadth_daily", session_date),
        missing=_missing_count(cursor, session_date),
    )


def retained_sessions_cursor(cursor) -> tuple[date, ...]:
    cursor.execute("SELECT DISTINCT session_date FROM rt.option_breadth_current ORDER BY session_date")
    return tuple(row[0] for row in _rows(cursor))


def backfill_retained_cursor(cursor, *, now: datetime, calendar=None) -> list[dict[str, object]]:
    """Every session still in the current table: closed ones are copied, an open one is reported and left alone."""
    results = []
    for session_date in retained_sessions_cursor(cursor):
        try:
            results.append(snapshot_session_cursor(cursor, session_date=session_date, now=now, calendar=calendar))
        except SessionStillOpen as error:
            results.append({"session_date": session_date.isoformat(), "status": "SKIPPED_OPEN", "reason": str(error)})
    return results


def _open_csv(path: Path) -> io.TextIOBase:
    raw = path.open("rb")
    magic = raw.read(2)
    raw.seek(0)
    stream = gzip.GzipFile(fileobj=raw) if magic == b"\x1f\x8b" else raw
    return io.TextIOWrapper(stream, encoding="utf-8", newline="")


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def load_csv_session_cursor(
    cursor, *, path: Path | str, now: datetime, session_date: date | None = None, calendar=None,
) -> dict[str, object]:
    """Load one session from a CSV copy of the current table (gzip or plain). The file must hold exactly one session
    (the requested one when `session_date` is given) with the current table's header; idempotent like the copy."""
    path = Path(path)
    snapshot_at = _utc(now, "now")
    with _open_csv(path) as handle:
        header = handle.readline().strip().split(",")
    if tuple(header) != BREADTH_COLUMNS:
        raise ValueError(f"{path.name}: the header is not rt.option_breadth_current's columns")
    with _open_csv(path) as handle:
        cursor.execute(
            f"CREATE TEMP TABLE {_CSV_STAGE} (LIKE rt.option_breadth_current) ON COMMIT DROP"
        )
        cursor.copy_expert(f"COPY {_CSV_STAGE} ({_COLUMN_LIST}) FROM STDIN WITH (FORMAT csv, HEADER true)", handle)
    cursor.execute(f"SELECT DISTINCT session_date FROM {_CSV_STAGE} ORDER BY 1")
    sessions = tuple(row[0] for row in _rows(cursor))
    if len(sessions) != 1 or (session_date is not None and sessions[0] != session_date):
        cursor.execute(f"DROP TABLE {_CSV_STAGE}")
        raise ValueError(f"{path.name}: expected one session {session_date or ''}, found {list(map(str, sessions))}")
    found = sessions[0]
    try:
        require_closed_session(found, now=snapshot_at, calendar=calendar)
    except SessionStillOpen:
        cursor.execute(f"DROP TABLE {_CSV_STAGE}")
        raise
    reference = f"csv:{path.name[:80]}:sha256:{_file_sha256(path)}"
    source_counts = _counts_by_underlying(cursor, _CSV_STAGE, found)
    cursor.execute(
        "WITH inserted AS ("
        f"INSERT INTO rt.option_breadth_daily ({_DAILY_COLUMNS}) "
        f"SELECT {','.join('stage.' + name for name in BREADTH_COLUMNS)},"
        "stage.observed_at=max(stage.observed_at) OVER (PARTITION BY stage.underlying),"
        f"%s,%s,%s FROM {_CSV_STAGE} AS stage "
        "ON CONFLICT (session_date,contract_ticker) DO NOTHING RETURNING underlying,final_capture) "
        "SELECT underlying,count(*),count(*) FILTER (WHERE final_capture) FROM inserted "
        "GROUP BY underlying ORDER BY underlying",
        (snapshot_at, CSV_SOURCE, reference),
    )
    inserted = _insert_report(cursor)
    cursor.execute(
        f"SELECT count(*) FROM {_CSV_STAGE} AS stage WHERE NOT EXISTS (SELECT 1 FROM rt.option_breadth_daily AS daily "
        "WHERE daily.session_date=stage.session_date AND daily.contract_ticker=stage.contract_ticker)"
    )
    missing = int(_rows(cursor)[0][0])
    cursor.execute(f"DROP TABLE {_CSV_STAGE}")
    return _report(
        session_date=found, source=CSV_SOURCE, reference=reference, source_counts=source_counts, inserted=inserted,
        daily_counts=_counts_by_underlying(cursor, "rt.option_breadth_daily", found), missing=missing,
    )


def plan_session_cursor(cursor, *, session_date: date, now: datetime, calendar=None) -> dict[str, object]:
    """Dry run: what a snapshot of `session_date` would write, and whether it is allowed now. Reads only."""
    try:
        allowed = snapshot_allowed_at(session_date, calendar=calendar)
    except ValueError as error:
        return {"session_date": session_date.isoformat(), "status": "NOT_A_SESSION", "reason": str(error)}
    source = _counts_by_underlying(cursor, "rt.option_breadth_current", session_date)
    daily = _counts_by_underlying(cursor, "rt.option_breadth_daily", session_date)
    missing = _missing_count(cursor, session_date)
    return {
        "session_date": session_date.isoformat(),
        "status": "OPEN" if _utc(now, "now") < allowed else "CLOSED",
        "allowed_from": allowed.isoformat(),
        "source_rows": sum(source.values()),
        "daily_rows": sum(daily.values()),
        "would_insert": missing,
        "by_underlying": {name: {"source_rows": count, "daily_rows": daily.get(name, 0)} for name, count in source.items()},
    }


class ClosedSessionSnapshotter:
    """The automatic call in the Options scheduler (the process that writes the current table): once its cycle reports
    no open session (the decision stop has passed), the newest session in the current table is copied once it is past
    its close plus the margin. Once per session per process (the copy is idempotent anyway); a failure is reported and
    retried after `retry_seconds`; with nothing to do it looks again after `idle_seconds` (the scheduler polls every
    2 s while it waits). With the switch off it returns None without touching the database."""

    def __init__(self, provider, *, clock: Callable[[], datetime] = lambda: datetime.now(UTC), calendar=None,
                 environment: Mapping[str, str] | None = None, retry_seconds: float = 300.0,
                 idle_seconds: float = 600.0):
        self._provider = provider
        self._clock = clock
        self._calendar = calendar
        self._environment = environment
        self._retry = timedelta(seconds=retry_seconds)
        self._idle = timedelta(seconds=idle_seconds)
        self._done: set[date] = set()
        self._next_attempt: datetime | None = None
        # Read once: the process's environment does not change, and an invalid value raises here (the scheduler has
        # already refused it at start, in parse_args).
        self.enabled = snapshot_enabled(environment)

    def maybe_run(self) -> dict[str, object] | None:
        if not self.enabled:
            return None
        from .database_access_v2 import APP_ROLE

        now = _utc(self._clock(), "clock")
        if self._next_attempt is not None and now < self._next_attempt:
            return None
        try:
            with self._provider.transaction(APP_ROLE, read_only=True) as unit:
                unit.cursor.execute(
                    "SELECT max(session_date) FROM rt.option_breadth_current WHERE session_date<=%s", (now.date(),)
                )
                session_date = _rows(unit.cursor)[0][0]
            if session_date is None or session_date in self._done:
                self._next_attempt = now + self._idle
                return None
            self._calendar = _calendar(self._calendar)
            allowed = snapshot_allowed_at(session_date, calendar=self._calendar)
            if now < allowed:
                self._next_attempt = min(allowed, now + self._idle)
                return None
            with self._provider.transaction(APP_ROLE) as unit:
                result = snapshot_session_cursor(
                    unit.cursor, session_date=session_date, now=now, calendar=self._calendar,
                )
        except Exception as error:  # noqa: BLE001 - the snapshot never stops the scheduler; retried later
            self._next_attempt = now + self._retry
            _LOGGER.warning("option_breadth_daily_snapshot_failed error=%s", error)
            return {"status": "BREADTH_DAILY_SNAPSHOT_FAILED", "reason": f"{type(error).__name__}: {str(error)[:200]}"}
        self._done.add(session_date)
        self._next_attempt = now + self._idle
        summary = {key: value for key, value in result.items() if key != "by_underlying"}
        return {"status": "BREADTH_DAILY_SNAPSHOT", **summary}


def report_json(value) -> str:
    return json.dumps(value, sort_keys=True, default=str)
