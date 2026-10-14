"""Real-schema fixtures for the delta screening loader (disposable V2 databases only).

Builds a cohort, manifest, securities, instruments and subscription members on the real V2 tables and inserts
bar revisions through rt.bar_catalog and rt.bar_revisions with explicit clocks (created_at is set, never the
insert time, so tests control visibility). Used by test_screening_delta_loader_db.py and
profile_screening_delta_loader.py.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from uuid import UUID, uuid4

import exchange_calendars
from psycopg2.extras import execute_values

UTC = timezone.utc
RAW, ADJ = "RAW_UNADJUSTED", "ADJUSTED"
SESSION = date(2026, 10, 2)
LENGTH = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "1h": 60, "1d": 390}
LONG_AGO = datetime(2025, 1, 1, tzinfo=UTC)
TIER = "MASSIVE_DEVELOPER"


def digest() -> str:
    return uuid4().hex + uuid4().hex


@dataclass(frozen=True)
class World:
    cohort_id: UUID
    manifest_id: UUID
    tickers: tuple[str, ...]
    securities: dict


@dataclass
class Bar:
    ticker: str
    interval: str
    start: datetime
    basis: str = RAW
    kind: str = "NATIVE_REST"
    observed: datetime | None = None
    created: datetime | None = None
    replay: datetime | None = None
    published: datetime | None = None
    sources: tuple = ()
    rid: str | None = None
    scope: str = "RTH"
    final: bool = True
    session: date | None = None
    length: timedelta | None = None
    price: float = 100.0
    ident: str = field(default="", init=False)

    @property
    def end(self) -> datetime:
        return self.start + (self.length or timedelta(minutes=LENGTH[self.interval]))


def ensure_history_partitions(cursor) -> None:
    """rt.bar_revisions partitions reach 400 days back; the 283-session daily history needs a little more."""
    cursor.execute("SELECT rt.ensure_partitions('rt.bar_revisions', %s)", (date(2026, 3, 1),))


def create_world(cursor, tickers=("AAPL", "MSFT"), *, stock_count: int | None = None) -> World:
    profile, manifest, cohort = uuid4(), uuid4(), uuid4()
    count = len(tickers) if stock_count is None else stock_count
    cursor.execute(
        "INSERT INTO rt.capability_profiles (profile_id, config_sha256, schema_sha256, account_alias, "
        "capabilities_sha256, universe_sha256, checked_at, expires_at, status, created_at, profile_sha256, "
        "provider_tier, security_policy_sha256, preflight_mode, provider_requests, action_authorized) "
        "VALUES (%s,%s,%s,'fixture',%s,%s,%s,%s,'PASS',%s,%s,%s,%s,'PASSIVE',false,false)",
        (str(profile), digest(), digest(), digest(), digest(), LONG_AGO, LONG_AGO + timedelta(days=3650), LONG_AGO,
         digest(), TIER, digest()))
    cursor.execute(
        "INSERT INTO rt.subscription_manifests (manifest_id, manifest_sha256, universe_sha256, session_date, "
        "effective_at, profile_id, manifest_kind, stock_count, option_underlying_count, member_count, "
        "allocation_policy_sha256, created_at) VALUES (%s,%s,%s,%s,%s,%s,'EQUITY_VALIDATION',%s,0,%s,%s,%s)",
        (str(manifest), digest(), digest(), LONG_AGO.date(), LONG_AGO, str(profile), count, count, digest(),
         LONG_AGO))
    cursor.execute(
        "INSERT INTO rt.cohorts (cohort_id, cohort_sha256, manifest_id, configuration_sha256, "
        "model_bundle_sha256, valuation_sha256, effective_at, mode, created_at) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,'SHADOW',%s)",
        (str(cohort), digest(), str(manifest), digest(), digest(), digest(), LONG_AGO, LONG_AGO))
    securities = {}
    for ticker in tickers:
        security, revision = uuid4(), uuid4()
        cursor.execute("INSERT INTO ref.securities (security_id, asset_type, source_origin, created_at) "
                       "VALUES (%s,'STOCK','fixture',%s)", (str(security), LONG_AGO))
        cursor.execute(
            "INSERT INTO ref.security_revisions (revision_id, security_id, ticker, calendar_id, effective_from, "
            "observed_at, payload_sha256) VALUES (%s,%s,%s,'XNYS',%s,%s,%s)",
            (str(revision), str(security), ticker, LONG_AGO, LONG_AGO, digest()))
        cursor.execute("INSERT INTO rt.instruments (asset_class, security_id) VALUES ('EQUITY', %s) "
                       "RETURNING instrument_id", (str(security),))
        instrument = cursor.fetchone()
        instrument = instrument["instrument_id"] if hasattr(instrument, "keys") else instrument[0]
        cursor.execute("INSERT INTO rt.subscription_members (manifest_id, instrument_id, channel, priority, "
                       "purpose) VALUES (%s,%s,'AM',1,'PRIMARY')", (str(manifest), instrument))
        securities[ticker] = (security, instrument)
    return World(cohort, manifest, tuple(tickers), securities)


CATALOG_SQL = ("INSERT INTO rt.bar_catalog (bar_revision_id, cohort_id, provider_tier, instrument_id, interval, "
               "bar_start, bar_end, observed_at, session_date) VALUES %s")
REVISION_SQL = (
    "INSERT INTO rt.bar_revisions (bar_revision_id, cohort_id, provider_tier, security_id, instrument_id, ticker, "
    "interval, session_date, session_scope, adjustment_basis, bar_start, bar_end, open_price, high_price, "
    "low_price, close_price, volume, source_kind, availability_mode, is_final, observed_at, replay_available_at, "
    "provider_published_at, ingestion_segment_id, source_revision_ids, payload_sha256, created_at) VALUES %s")
REVISION_TEMPLATE = "(" + ",".join(["%s"] * 24) + ",%s::uuid[],%s,%s)"


def insert_bars(cursor, world: World, bars, *, page_size: int = 2000) -> list[str]:
    """Inserts the bars (catalog first); returns their revision ids in order. A bar's observed_at must not be
    earlier than its end (a table CHECK)."""
    segment = str(uuid4())
    catalog, revisions, ids = [], [], []
    for bar in bars:
        security, instrument = world.securities[bar.ticker]
        revision_id = bar.rid or str(uuid4())
        bar.ident = revision_id
        ids.append(revision_id)
        observed = bar.observed or bar.end + timedelta(seconds=30)
        created = bar.created or observed
        session = bar.session or bar.start.date()
        catalog.append((revision_id, str(world.cohort_id), TIER, instrument, bar.interval, bar.start, bar.end,
                        observed, session))
        revisions.append((
            revision_id, str(world.cohort_id), TIER, str(security), instrument, bar.ticker, bar.interval, session,
            bar.scope, bar.basis, bar.start, bar.end, bar.price, bar.price + 1, bar.price - 1, bar.price + 0.5,
            1000, bar.kind, "LIVE_OBSERVED", bar.final, observed, bar.replay, bar.published, segment,
            [str(source) for source in bar.sources], digest(), created))
    if catalog:
        execute_values(cursor, CATALOG_SQL, catalog, page_size=page_size)
        execute_values(cursor, REVISION_SQL, revisions, template=REVISION_TEMPLATE, page_size=page_size)
    return ids


def history_bars(world: World, *, session: date = SESSION, rng=None, sessions_1m: int = 5):
    """A realistic retained history per ticker (about 13,500 bars): 283 adjusted daily bars, weekly and monthly
    adjusted bars, and for the last 90 sessions raw 5m (native), 15m/30m/1h/1d (derived from their 5m or 30m
    sources) and 1m for the last few sessions. Every bar is final with observed_at = end + 30 s."""
    from realtime.equity_portal_history import build_realtime_equity_portal_history_plan

    calendar = exchange_calendars.get_calendar("XNYS")
    plan = build_realtime_equity_portal_history_plan(session)
    raw_sessions = [d.date() for d in calendar.sessions_in_range(
        calendar.session_offset(str(session), -89), str(session))]
    daily_sessions = [d.date() for d in calendar.sessions_in_range(
        calendar.session_offset(str(session), -282), str(session))]
    out: list[Bar] = []
    for ticker in world.tickers:
        for day in daily_sessions:
            open_, close = (calendar.session_open(str(day)).to_pydatetime(),
                            calendar.session_close(str(day)).to_pydatetime())
            out.append(Bar(ticker, "1d", open_, ADJ, session=day, length=close - open_))
        weeks = [d for d in daily_sessions if d.weekday() == 4 and plan.weekly_start <= d <= plan.weekly_end]
        for day in weeks:
            first = next(d for d in daily_sessions if d >= day - timedelta(days=4))
            start = calendar.session_open(str(first)).to_pydatetime()
            out.append(Bar(ticker, "1wk", start, ADJ, session=day,
                           length=calendar.session_close(str(day)).to_pydatetime() - start))
        months = {}
        for day in daily_sessions:
            if day >= plan.monthly_start:
                months[(day.year, day.month)] = day
        for day in months.values():
            first = date(day.year, day.month, 1)
            start = calendar.session_open(str(next(d for d in daily_sessions if d >= first))).to_pydatetime()
            out.append(Bar(ticker, "1mo", start, ADJ, session=day,
                           length=calendar.session_close(str(day)).to_pydatetime() - start))
        # Raw intraday bars for the last 90 sessions; the derived ones reference their sources by id.
        for day in raw_sessions:
            open_ = calendar.session_open(str(day)).to_pydatetime()
            close = calendar.session_close(str(day)).to_pydatetime()
            five = []
            moment = open_
            while moment < close:
                bar = Bar(ticker, "5m", moment, session=day)
                bar.ident = str(uuid4())
                bar.rid = bar.ident
                five.append(bar)
                moment += timedelta(minutes=5)
            out.extend(five)

            def derive(interval, group, source_bars):
                made = []
                for index in range(0, len(source_bars), group):
                    chunk = source_bars[index:index + group]
                    bar = Bar(ticker, interval, chunk[0].start, kind="DERIVED", session=day,
                              length=chunk[-1].end - chunk[0].start, sources=tuple(c.rid for c in chunk))
                    bar.rid = str(uuid4())
                    made.append(bar)
                return made

            fifteen = derive("15m", 3, five)
            thirty = derive("30m", 6, five)
            hourly = derive("1h", 12, five)
            daily = derive("1d", len(thirty), thirty)
            out.extend((*fifteen, *thirty, *hourly, *daily))
            if day in raw_sessions[-sessions_1m:]:
                moment = open_
                while moment < close:
                    out.append(Bar(ticker, "1m", moment, session=day))
                    moment += timedelta(minutes=1)
    return out
