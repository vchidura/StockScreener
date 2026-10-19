"""Offline proof of the delta screening loader: it must equal the full load at every cutoff.

The fake cursor below evaluates the visibility rules, the history window, the source precedence and the
winner order in plain Python (written from the specification, not from the loader) and answers each statement
the repository issues. The full load, `load_realtime_screening_input`, is the oracle; the SQL itself is
exercised on the real schema by test_screening_delta_loader_db.py.
"""
from __future__ import annotations

import random
from datetime import date, datetime, timedelta, timezone
from uuid import UUID

import exchange_calendars
import pytest

import realtime.screening_delta_loader as delta_loader
from realtime.screening_delta_loader import ScreeningInputLoader, _outranks
from realtime.screening_repository import close_source_lineage, load_realtime_screening_input

UTC = timezone.utc
SESSION = date(2026, 10, 2)
NEXT_SESSION = date(2026, 10, 5)
OPEN = datetime(2026, 10, 2, 13, 30, tzinfo=UTC)
PREVIOUS_OPEN = datetime(2026, 10, 1, 13, 30, tzinfo=UTC)
MINUTE = timedelta(minutes=1)
RAW, ADJ = "RAW_UNADJUSTED", "ADJUSTED"
LENGTH = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "1h": 60, "1d": 390}
INTERVAL_ORDER = ["1m", "5m", "15m", "30m", "1h", "1d", "1wk", "1mo"]


@pytest.mark.parametrize("continuity", [None, False, True])
def test_loader_preserves_explicit_identity_continuity_scope(monkeypatch, continuity):
    captured = {}
    marker = object()

    def load(_cursor, **scope):
        captured.update(scope)
        return marker

    monkeypatch.setattr(delta_loader, "load_screening_input", load)
    loader = ScreeningInputLoader()
    options = {} if continuity is None else {"allow_late_identity_continuity": continuity}
    assert loader.load(
        object(), cohort_id=UUID(int=1), session=SESSION,
        cutoff=OPEN, source_generation=1, **options,
    ) is marker
    assert captured["allow_late_identity_continuity"] is (continuity is True)
    assert captured["bar_loader"].__self__ is loader

BASE_COLUMNS = (
    "bar_revision_id", "cohort_id", "provider_tier", "security_id", "instrument_id", "ticker", "interval",
    "session_date", "session_scope", "adjustment_basis", "bar_start", "bar_end", "open", "high", "low", "close",
    "volume", "source_kind", "source_revision_ids", "observed_at", "replay_available_at", "is_final",
    "payload_sha256",
)
PORTAL_COLUMNS = (
    "availability_mode", "vwap", "transaction_count", "ingestion_segment_id", "journal_generation",
    "stream_session_id", "source_offset", "provider_published_at",
)


def micros(value: datetime) -> int:
    return (value - OPEN) // timedelta(microseconds=1)


def rank_of(row: dict, portal: bool) -> int:
    kind, interval, basis = row["source_kind"], row["interval"], row["adjustment_basis"]
    if portal:
        if basis == ADJ and interval in ("1d", "1wk", "1mo") and kind == "NATIVE_REST":
            return 5
        if basis == RAW and interval in ("1m", "5m") and kind == "NATIVE_REST":
            return 5
        if basis == RAW and interval in ("15m", "30m", "1h", "1d") and kind == "DERIVED":
            return 5
    return {"RECONCILED": 4, "NATIVE_REST": 3, "DERIVED": 2}.get(kind, 1)


def order_key(row: dict, portal: bool):
    return (-rank_of(row, portal), -micros(row["observed_at"]), -micros(row["created_at"]), row["bar_revision_id"])


def visible_at(row: dict, cutoff: datetime, known_by: datetime | None = None) -> bool:
    known_by = known_by or cutoff
    return (
        row["observed_at"] <= known_by and row["created_at"] <= known_by
        and (row["replay_available_at"] is None or row["replay_available_at"] <= known_by)
        and (row["provider_published_at"] is None or row["provider_published_at"] <= known_by)
        and row["bar_end"] <= cutoff
    )


def in_window(row: dict, history: tuple, portal: bool) -> bool:
    interval, basis, day = row["interval"], row["adjustment_basis"], row["session_date"]
    if portal:
        return (
            (basis == ADJ and interval == "1d" and history[0] <= day <= history[1])
            or (basis == ADJ and interval == "1wk" and history[2] <= day <= history[3])
            or (basis == ADJ and interval == "1mo" and history[4] <= day <= history[5])
            or (basis == RAW and interval == "1d" and history[6] <= day <= history[7])
            or (basis == RAW and interval in ("1m", "5m", "15m", "30m", "1h")
                and history[8] <= day <= history[9])
        )
    return (
        (interval == "1d" and basis == ADJ and history[0] <= day <= history[1])
        or (interval == "1d" and basis == RAW and history[2] <= day <= history[3])
        or (interval == "1h" and basis == RAW and row["bar_start"] >= history[4] and row["bar_end"] <= history[5])
    )


class FakeDatabase:
    def __init__(self, tickers=("AAPL", "MSFT"), tier="MASSIVE_DEVELOPER"):
        self.cohort_id = UUID(int=1)
        self.tier = tier
        self.tickers = tuple(sorted(tickers))
        self.manifest_sha256 = "a" * 64
        self.generation = 1
        self.instrument_offset = 0
        self.rows: list[dict] = []
        self.fail_on: set[str] = set()
        self.counter = 0

    @staticmethod
    def collation(row):
        # Differs from Python's string order (the intervals are chronological), so a loader that sorted
        # series itself would disagree with the database.
        return row["ticker"], INTERVAL_ORDER.index(row["interval"]), row["adjustment_basis"]

    @property
    def members(self):
        return [dict(security_id=UUID(int=100 + index), ticker=ticker,
                     instrument_id=index + 1 + self.instrument_offset)
                for index, ticker in enumerate(self.tickers)]

    def manifest(self):
        return dict(manifest_id=UUID(int=2), manifest_sha256=self.manifest_sha256,
                    stock_count=len(self.tickers), provider_tier=self.tier)

    def new_id(self) -> str:
        self.counter += 1
        return str(UUID(int=10_000 + self.counter))

    def add(self, *, ticker="AAPL", interval="5m", basis=RAW, start=OPEN, kind="NATIVE_REST", length=None,
            observed=None, created=None, replay=None, published=None, sources=(), rid=None, session=None,
            scope="RTH", final=True, committed=True, tier=None) -> dict:
        end = start + (length or timedelta(minutes=LENGTH[interval]))
        observed = observed or end + timedelta(seconds=30)
        row = dict(
            bar_revision_id=rid or self.new_id(), cohort_id=self.cohort_id, provider_tier=tier or self.tier,
            security_id=UUID(int=100 + self.tickers.index(ticker)) if ticker in self.tickers else UUID(int=999),
            instrument_id=self.tickers.index(ticker) + 1 if ticker in self.tickers else 99, ticker=ticker,
            interval=interval, session_date=session or start.date(), session_scope=scope, adjustment_basis=basis,
            bar_start=start, bar_end=end, open=100.0, high=101.0, low=99.0, close=100.5, volume=1000.0,
            source_kind=kind, source_revision_ids=[str(source) for source in sources], observed_at=observed,
            replay_available_at=replay, is_final=final, payload_sha256=f"{self.counter:064x}",
            availability_mode="LIVE_OBSERVED", vwap=None, transaction_count=None, ingestion_segment_id=None,
            journal_generation=None, stream_session_id=None, source_offset=None, provider_published_at=published,
            created_at=created or observed, _committed=committed,
        )
        self.rows.append(row)
        return row

    @staticmethod
    def commit(*rows):
        for row in rows:
            row["_committed"] = True

    def cursor(self):
        return FakeCursor(self)

    def scope(self, cutoff, *, session=SESSION, portal=True, details=False, known_by=None):
        return dict(cohort_id=self.cohort_id, session=session, cutoff=cutoff, source_generation=self.generation,
                    include_portal_history=portal, include_strategy_action_details=details, known_by=known_by)

    def full(self, cutoff, **kw):
        return load_realtime_screening_input(self.cursor(), **self.scope(cutoff, **kw))


class FakeCursor:
    def __init__(self, database: FakeDatabase):
        self.db = database
        self.kinds: list[str] = []
        self.returned: list[int] = []
        self.statements: list[str] = []
        self.params: list[tuple] = []
        self._result = []

    def execute(self, sql, params=()):
        kind = self._kind(sql)
        self.kinds.append(kind)
        self.statements.append(sql)
        self.params.append(tuple(params))
        if kind in self.db.fail_on:
            raise RuntimeError(f"injected failure at {kind}")
        self._result = getattr(self, "_" + kind)(sql, params)
        self.returned.append(len(self._result) if isinstance(self._result, list) else 1)

    def fetchone(self):
        return self._result

    def fetchall(self):
        return self._result

    @staticmethod
    def _kind(sql):
        if sql.startswith("SELECT manifest.manifest_id"):
            return "manifest"
        if "FROM rt.subscription_members" in sql:
            return "members"
        if sql.startswith("SELECT DISTINCT ON (ticker, interval, adjustment_basis, bar_start)"):
            return "winners_state" if " AS visible_total" in sql else "winners"
        if sql.startswith("SELECT counted.visible_total"):
            return "delta"
        if "bar_revision_id=ANY(%s::uuid[])" in sql:
            return "sources"
        if "FROM ref.corporate_action_revisions AS action" in sql:
            return "actions"
        if "FROM ref.corporate_action_coverage" in sql:
            return "coverage"
        raise AssertionError(f"unexpected statement: {sql[:80]}")

    def _manifest(self, sql, params):
        assert params[0] == str(self.db.cohort_id)
        return self.db.manifest()

    def _members(self, sql, params):
        return self.db.members

    def _actions(self, sql, params):
        return []

    def _coverage(self, sql, params):
        return []

    @staticmethod
    def _clocks(clock_params):
        """(cutoff, known_by) from the five visibility parameters: four recording clocks, then bar_end."""
        assert len(clock_params) == 5 and len(set(clock_params[:4])) == 1
        known_by, cutoff = clock_params[0], clock_params[4]
        if cutoff.tzinfo is None:  # the database reads a naive timestamp in the session zone
            cutoff, known_by = cutoff.replace(tzinfo=UTC), known_by.replace(tzinfo=UTC)
        return cutoff, known_by

    def _candidates(self, params, portal):
        cohort, tier, tickers, *rest = params
        history = tuple(rest[5:])
        assert len(history) == (10 if portal else 6)
        cutoff, known_by = self._clocks(rest[:5])
        return [row for row in self.db.rows if row["_committed"]
                and str(row["cohort_id"]) == cohort and row["provider_tier"] == tier
                and row["ticker"] in tickers and row["session_scope"] == "RTH" and row["is_final"]
                and visible_at(row, cutoff, known_by) and in_window(row, history, portal)]

    @staticmethod
    def _project(row, portal):
        columns = BASE_COLUMNS + (PORTAL_COLUMNS if portal else ()) + ("created_at",)
        return {name: (list(row[name]) if name == "source_revision_ids" else row[name]) for name in columns}

    def _winners(self, sql, params):
        portal = "interval='1wk'" in sql
        with_state = " AS visible_total" in sql
        size = 3 + 5 + (10 if portal else 6)
        assert len(params) == size * (2 if with_state else 1)
        if with_state:
            assert params[:size] == params[size:]
        candidates = self._candidates(params[:size], portal)
        best: dict[tuple, dict] = {}
        for row in candidates:
            key = (row["ticker"], row["interval"], row["adjustment_basis"], row["bar_start"])
            if key not in best or order_key(row, portal) < order_key(best[key], portal):
                best[key] = row
        winners = sorted(best.values(), key=lambda row: (self.db.collation(row), row["bar_start"]))
        out = []
        for row in winners:
            item = self._project(row, portal)
            if with_state:
                item["src_rank"] = rank_of(row, portal)
                item["visible_total"] = len(candidates)
            out.append(item)
        return out

    _winners_state = _winners

    def _delta(self, sql, params):
        portal = "interval='1wk'" in sql
        size = 3 + 5 + (10 if portal else 6)
        assert len(params) == 2 * size + 5
        assert params[:size] == params[size:2 * size]
        previous_cutoff, previous_known_by = self._clocks(params[2 * size:])
        current = self._candidates(params[:size], portal)
        new = [row for row in current if not visible_at(row, previous_cutoff, previous_known_by)]
        if not new:
            empty = {name: None for name in (*BASE_COLUMNS, *(PORTAL_COLUMNS if portal else ()), "created_at")}
            return [{"visible_total": len(current), **empty, "src_rank": None}]
        return [{"visible_total": len(current), **self._project(row, portal), "src_rank": rank_of(row, portal)}
                for row in new]

    def _sources(self, sql, params):
        portal = "availability_mode" in sql
        ids, cohort, tier, *clocks = params
        cutoff, known_by = self._clocks(clocks)
        assert sql.rstrip().endswith("ORDER BY bar_revision_id")
        wanted = set(ids)
        rows = [row for row in self.db.rows if row["_committed"] and row["bar_revision_id"] in wanted
                and str(row["cohort_id"]) == cohort and row["provider_tier"] == tier
                and visible_at(row, cutoff, known_by) and row["is_final"]]
        return [self._project(row, portal) for row in sorted(rows, key=lambda row: row["bar_revision_id"])]


def outcome(call):
    try:
        return "ok", call()
    except Exception as error:  # the comparison includes the failure the full load raises
        return "error", type(error).__name__, str(error)


def agree(database, loader, cutoff, **kw):
    """Loads through the delta loader and the full load; they must match. Returns (result or None, cursor)."""
    cursor = database.cursor()
    delta = outcome(lambda: loader.load(cursor, **database.scope(cutoff, **kw)))
    full = outcome(lambda: database.full(cutoff, **kw))
    assert delta == full
    return (delta[1] if delta[0] == "ok" else None), cursor


def seeded(database, *, count=3):
    """Three 5m bars in the session and one bar in each other series per ticker (a series that appears
    later forces a full reload, so the tests that exercise the delta path need their series to exist)."""
    for ticker in database.tickers:
        for index in range(count):
            database.add(ticker=ticker, start=OPEN + index * 5 * MINUTE)
        for interval in ("1m", "15m", "1h"):
            database.add(ticker=ticker, interval=interval, start=PREVIOUS_OPEN + 30 * MINUTE)
        for basis in (ADJ, RAW):
            database.add(ticker=ticker, interval="1d", basis=basis, start=PREVIOUS_OPEN, session=date(2026, 10, 1))


def at(minutes: int) -> datetime:
    return OPEN + timedelta(minutes=minutes)


def ids_of(result):
    return [row["bar_revision_id"] for row in result.bars]


@pytest.fixture
def database():
    database = FakeDatabase()
    seeded(database)
    return database


def test_cold_load_equals_full_and_reports_a_full_read(database):
    loader = ScreeningInputLoader()
    result, cursor = agree(database, loader, at(20))
    assert result.bars and "winners_state" in cursor.kinds
    assert loader.last_report.mode == "full" and loader.last_report.reason == "NO_STATE"
    assert loader.last_report.bar_rows_read == len(result.bars) == loader.retained_bars
    assert cursor.kinds[:2] == ["manifest", "members"] and cursor.kinds[-2:] == ["actions", "coverage"]


def test_no_change_reads_no_bars_and_shares_the_retained_rows(database):
    loader = ScreeningInputLoader()
    first, _ = agree(database, loader, at(20))
    second, cursor = agree(database, loader, at(25))
    assert loader.last_report.mode == "delta" and loader.last_report.bar_rows_read == 0
    assert "winners" not in cursor.kinds and "winners_state" not in cursor.kinds and "sources" not in cursor.kinds
    assert second.bars == first.bars and all(a is b for a, b in zip(first.bars, second.bars))


def test_equal_cutoff_reuses_state_and_is_still_exact(database):
    loader = ScreeningInputLoader()
    agree(database, loader, at(20))
    _, cursor = agree(database, loader, at(20))
    assert loader.last_report.mode == "delta" and "delta" in cursor.kinds and "winners" not in cursor.kinds


def test_one_new_final_bar_reads_only_that_row(database):
    loader = ScreeningInputLoader()
    agree(database, loader, at(20))
    database.add(start=at(20))
    result, cursor = agree(database, loader, at(30))
    assert loader.last_report.mode == "delta" and loader.last_report.bar_rows_read == 1
    assert cursor.returned[cursor.kinds.index("delta")] == 1
    assert any(row["bar_start"] == at(20) for row in result.bars)


def test_late_visible_row_is_caught_when_observed_at_crosses_into_the_window(database):
    loader = ScreeningInputLoader()
    agree(database, loader, at(20))
    # Inserted long ago (created_at old) but only observed later: it must appear exactly when observed_at <= cutoff.
    late = database.add(start=at(25), observed=at(40), created=OPEN, rid="00000000-0000-0000-0000-00000000f001")
    result, _ = agree(database, loader, at(30))
    assert late["bar_revision_id"] not in ids_of(result)
    result, _ = agree(database, loader, at(45))
    assert loader.last_report.mode == "delta" and loader.last_report.bar_rows_read == 1
    assert late["bar_revision_id"] in ids_of(result)


@pytest.mark.parametrize("clock", ["observed_at", "created_at", "replay_available_at", "provider_published_at",
                                    "bar_end"])
def test_each_visibility_clock_crossing_the_window_is_a_delta_row(database, clock):
    loader = ScreeningInputLoader()
    agree(database, loader, at(20))
    start = at(30)
    fields = dict(observed=start + 6 * MINUTE, created=start + 6 * MINUTE)
    crossing = at(50)
    if clock == "observed_at":
        fields = dict(observed=crossing, created=start)
    elif clock == "created_at":
        fields["created"] = crossing
    elif clock == "replay_available_at":
        fields["replay"] = crossing
    elif clock == "provider_published_at":
        fields["published"] = crossing
    else:
        # Isolates the bar_end clock (the real table's CHECK implies observed_at >= bar_end).
        fields = dict(observed=start, created=start, length=20 * MINUTE)
    row = database.add(start=start, **fields)
    before, _ = agree(database, loader, at(40))
    assert row["bar_revision_id"] not in ids_of(before) and loader.last_report.bar_rows_read == 0
    after, _ = agree(database, loader, at(60))
    assert loader.last_report.mode == "delta" and loader.last_report.bar_rows_read == 1
    assert row["bar_revision_id"] in ids_of(after)


def test_null_optional_clocks_do_not_block_or_cross(database):
    loader = ScreeningInputLoader()
    agree(database, loader, at(20))
    database.add(start=at(25), replay=None, published=None)
    agree(database, loader, at(40))
    assert loader.last_report.bar_rows_read == 1
    agree(database, loader, at(80))
    assert loader.last_report.bar_rows_read == 0


OLD_AT = at(100)
STAMP = {"id": "00000000-0000-0000-0000-00000000000"}
# (id, portal flavor, interval, basis, fields of the old row, fields of the new row, the new row wins).
# The old row is visible at OLD_AT; the new row only becomes visible after it (its replay clock crosses,
# or a clock the case overrides). The portal flavor ranks NATIVE_REST 5m/1m and DERIVED 15m+ bars first.
PRECEDENCE_CASES = [
    ("portal_derived_15m_beats_native", True, "15m", RAW, dict(kind="NATIVE_REST"), dict(kind="DERIVED"), True),
    ("portal_derived_15m_beats_reconciled", True, "15m", RAW, dict(kind="RECONCILED"), dict(kind="DERIVED"), True),
    ("portal_native_15m_loses_to_derived", True, "15m", RAW, dict(kind="DERIVED"), dict(kind="NATIVE_REST"), False),
    ("portal_native_5m_beats_reconciled", True, "5m", RAW, dict(kind="RECONCILED"), dict(kind="NATIVE_REST"), True),
    ("portal_reconciled_5m_loses_to_native", True, "5m", RAW, dict(kind="NATIVE_REST"), dict(kind="RECONCILED"), False),
    ("portal_reconciled_beats_native_30m", True, "30m", RAW, dict(kind="NATIVE_REST"), dict(kind="RECONCILED"), True),
    ("portal_derived_beats_stream_5m", True, "5m", RAW, dict(kind="REALTIME_STREAM"), dict(kind="DERIVED"), True),
    ("portal_stream_loses_to_derived_5m", True, "5m", RAW, dict(kind="DERIVED"), dict(kind="REALTIME_STREAM"), False),
    ("portal_adjusted_daily_native_beats_reconciled", True, "1d", ADJ, dict(kind="RECONCILED"), dict(kind="NATIVE_REST"), True),
    ("portal_adjusted_daily_reconciled_loses_to_native", True, "1d", ADJ, dict(kind="NATIVE_REST"), dict(kind="RECONCILED"), False),
    ("plain_reconciled_beats_native_daily", False, "1d", RAW, dict(kind="NATIVE_REST"), dict(kind="RECONCILED"), True),
    ("plain_native_beats_derived_daily", False, "1d", RAW, dict(kind="DERIVED"), dict(kind="NATIVE_REST"), True),
    ("plain_derived_beats_stream_daily", False, "1d", RAW, dict(kind="REALTIME_STREAM"), dict(kind="DERIVED"), True),
    ("plain_derived_loses_to_native_daily", False, "1d", RAW, dict(kind="NATIVE_REST"), dict(kind="DERIVED"), False),
    ("portal_later_observed_wins", True, "5m", RAW, dict(), dict(observed=OLD_AT + 2 * MINUTE), True),
    ("plain_later_observed_wins", False, "1d", RAW, dict(), dict(observed=OLD_AT + 2 * MINUTE), True),
    ("portal_earlier_observed_loses", True, "5m", RAW, dict(), dict(observed=OLD_AT - 5 * MINUTE), False),
    ("plain_earlier_observed_loses", False, "1d", RAW, dict(), dict(observed=OLD_AT - 5 * MINUTE), False),
    ("portal_observed_tie_later_created_wins", True, "5m", RAW, dict(), dict(created=OLD_AT + 2 * MINUTE), True),
    ("plain_observed_tie_later_created_wins", False, "1d", RAW, dict(), dict(created=OLD_AT + 2 * MINUTE), True),
    ("portal_observed_tie_earlier_created_loses", True, "5m", RAW, dict(created=OLD_AT - MINUTE), dict(created=OLD_AT - 2 * MINUTE), False),
    ("plain_observed_tie_earlier_created_loses", False, "1d", RAW, dict(created=OLD_AT - MINUTE), dict(created=OLD_AT - 2 * MINUTE), False),
    ("portal_full_tie_lower_id_wins", True, "5m", RAW, dict(rid=STAMP["id"] + "9"), dict(rid=STAMP["id"] + "3"), True),
    ("plain_full_tie_lower_id_wins", False, "1d", RAW, dict(rid=STAMP["id"] + "9"), dict(rid=STAMP["id"] + "3"), True),
    ("portal_full_tie_higher_id_loses", True, "5m", RAW, dict(rid=STAMP["id"] + "3"), dict(rid=STAMP["id"] + "9"), False),
    ("plain_full_tie_higher_id_loses", False, "1d", RAW, dict(rid=STAMP["id"] + "3"), dict(rid=STAMP["id"] + "9"), False),
]


@pytest.mark.parametrize("case", PRECEDENCE_CASES, ids=lambda case: case[0])
def test_revision_supersedes_or_loses_by_each_precedence_rule(case):
    _, portal, interval, basis, old_fields, new_fields, new_wins = case
    database = FakeDatabase()
    seeded(database)
    start = at(60) if interval != "1d" else datetime(2026, 9, 30, 13, 30, tzinfo=UTC)
    common = dict(interval=interval, basis=basis, start=start, session=start.date())
    base = dict(common, observed=OLD_AT - 3 * MINUTE, created=OLD_AT - 3 * MINUTE)
    old = database.add(**{**base, **old_fields})
    loader = ScreeningInputLoader()
    agree(database, loader, OLD_AT, portal=portal)
    new = database.add(**{**base, "replay": OLD_AT + MINUTE, **new_fields})
    result, _ = agree(database, loader, OLD_AT + 10 * MINUTE, portal=portal)
    assert loader.last_report.mode == "delta" and loader.last_report.bar_rows_read == 1
    winner = next(row for row in result.bars if row["ticker"] == "AAPL" and row["interval"] == interval
                  and row["adjustment_basis"] == basis and row["bar_start"] == start)
    assert winner["bar_revision_id"] == (new if new_wins else old)["bar_revision_id"]
    # Visibility only grows: the loser stays beaten at any later cutoff.
    later, _ = agree(database, loader, OLD_AT + 300 * MINUTE, portal=portal)
    assert any(row["bar_revision_id"] == winner["bar_revision_id"] for row in later.bars)


def test_the_python_order_matches_the_specified_order_on_random_pairs():
    rng = random.Random(7)
    for _ in range(2000):
        rows = []
        for _ in range(2):
            rows.append(dict(
                source_kind=rng.choice(["NATIVE_REST", "DERIVED", "RECONCILED", "REALTIME_STREAM"]),
                interval=rng.choice(["1m", "5m", "15m", "1d"]), adjustment_basis=rng.choice([RAW, ADJ]),
                observed_at=OPEN + rng.randint(0, 3) * MINUTE, created_at=OPEN + rng.randint(0, 3) * MINUTE,
                bar_revision_id=str(UUID(int=rng.randint(0, 2))),
            ))
        if rows[0]["bar_revision_id"] == rows[1]["bar_revision_id"]:
            continue
        for portal in (True, False):
            a, b = rows
            assert _outranks(rank_of(a, portal), a, rank_of(b, portal), b) == (order_key(a, portal) < order_key(b, portal))


def test_a_beaten_revision_never_wins_at_a_later_cutoff(database):
    loader = ScreeningInputLoader()
    beaten = database.add(start=at(40), kind="DERIVED", observed=at(50), created=at(50))
    winner = database.add(start=at(40), kind="NATIVE_REST", observed=at(52), created=at(52))
    winners = []
    for minutes in (51, 53, 60, 90, 200):
        result, _ = agree(database, loader, at(minutes))
        winners.append(next(r["bar_revision_id"] for r in result.bars
                            if r["bar_start"] == at(40) and r["ticker"] == "AAPL" and r["interval"] == "5m"))
    assert winners == [beaten["bar_revision_id"]] + [winner["bar_revision_id"]] * 4
    for state in loader._entries.values():
        state.check_invariants()


def test_a_superseded_winner_that_a_derived_bar_references_stays_as_a_source_row(database):
    loader = ScreeningInputLoader()
    source = database.add(start=at(40), observed=at(46), created=at(46))
    derived = database.add(start=at(40), interval="15m", kind="DERIVED", sources=[source["bar_revision_id"]],
                           observed=at(56), created=at(56))
    result, _ = agree(database, loader, at(60))
    assert source["bar_revision_id"] in ids_of(result)
    replacement = database.add(start=at(40), observed=at(66), created=at(66))
    result, _ = agree(database, loader, at(70))
    assert loader.last_report.mode == "delta"
    ids = ids_of(result)
    assert replacement["bar_revision_id"] in ids and derived["bar_revision_id"] in ids
    assert ids.index(source["bar_revision_id"]) > ids.index(replacement["bar_revision_id"])
    for state in loader._entries.values():
        state.check_invariants()


def test_derived_bar_whose_source_revision_arrives_later(database):
    loader = ScreeningInputLoader()
    agree(database, loader, at(20))
    source = database.add(start=at(30), scope="EXTENDED", observed=at(45), created=at(45))
    database.add(start=at(30), interval="15m", kind="DERIVED", sources=[source["bar_revision_id"]],
                 observed=at(46), created=at(40))
    agree(database, loader, at(47))
    assert loader.last_report.mode == "delta" and loader.last_report.source_rows_read == 1
    # A derived bar whose source is still invisible: both loads fail the same way, the state is dropped,
    # and once the source is visible both succeed (the next call is a full read, then deltas again).
    hidden = database.add(start=at(60), scope="EXTENDED", observed=at(95), created=at(95))
    database.add(start=at(60), interval="15m", kind="DERIVED", sources=[hidden["bar_revision_id"]],
                 observed=at(80), created=at(80))
    failure, _ = agree(database, loader, at(85))
    assert failure is None and not loader._entries
    agree(database, loader, at(100))
    assert loader.last_report.mode == "full" and loader.last_report.reason == "NO_STATE"
    agree(database, loader, at(110))
    assert loader.last_report.mode == "delta"


def test_source_closure_is_reused_and_only_unknown_ids_are_queried(database):
    loader = ScreeningInputLoader()
    sources = [database.add(start=OPEN + index * MINUTE, interval="1m", scope="EXTENDED") for index in range(3)]
    database.add(start=OPEN, interval="15m", kind="DERIVED", sources=[s["bar_revision_id"] for s in sources],
                 observed=at(30), created=at(30))
    agree(database, loader, at(31))
    assert loader.last_report.source_rows_read == 3
    _, cursor = agree(database, loader, at(35))
    assert "sources" not in cursor.kinds and loader.last_report.source_rows_read == 0
    extra = database.add(start=at(5), interval="1m", scope="EXTENDED")
    database.add(start=at(15), interval="15m", kind="DERIVED", sources=[extra["bar_revision_id"]],
                 observed=at(40), created=at(40))
    result, cursor = agree(database, loader, at(45))
    assert loader.last_report.source_rows_read == 1 and cursor.kinds.count("sources") == 1
    assert len(ids_of(result)) == len(set(ids_of(result)))


@pytest.mark.parametrize("depth, fails", [(1, False), (7, False), (8, True)])
def test_source_chain_depth_matches_the_full_load(database, depth, fails):
    loader = ScreeningInputLoader()
    agree(database, loader, at(20))
    previous = None
    for level in range(depth):
        previous = database.add(start=OPEN + (level + 1) * MINUTE, interval="1m", scope="EXTENDED",
                                sources=[previous["bar_revision_id"]] if previous else [])
    database.add(start=at(15), interval="15m", kind="DERIVED", sources=[previous["bar_revision_id"]],
                 observed=at(50), created=at(50))
    result, _ = agree(database, loader, at(60))
    assert (result is None) == fails


def test_cutoff_going_backwards_forces_a_full_reload(database):
    late = database.add(start=at(30), observed=at(40), created=at(40))
    loader = ScreeningInputLoader()
    forward, _ = agree(database, loader, at(60))
    assert late["bar_revision_id"] in ids_of(forward)
    backward, cursor = agree(database, loader, at(30))
    assert loader.last_report.mode == "full" and loader.last_report.reason == "CUTOFF_REGRESSED"
    assert "winners_state" in cursor.kinds and late["bar_revision_id"] not in ids_of(backward)
    agree(database, loader, at(60))
    assert loader.last_report.mode == "delta" and loader.last_report.bar_rows_read == 1


def test_new_session_rolls_the_window_with_a_full_reload():
    calendar = exchange_calendars.get_calendar("XNYS")
    edge = calendar.session_offset(str(SESSION), -282).date()
    database = FakeDatabase()
    seeded(database)
    edge_bar = database.add(interval="1d", basis=ADJ, session=edge,
                            start=datetime(edge.year, edge.month, edge.day, 13, 30, tzinfo=UTC))
    loader = ScreeningInputLoader()
    first, _ = agree(database, loader, at(60))
    assert edge_bar["bar_revision_id"] in ids_of(first)
    next_open = datetime(2026, 10, 5, 13, 30, tzinfo=UTC)
    database.add(start=next_open)
    rolled, _ = agree(database, loader, next_open + 20 * MINUTE, session=NEXT_SESSION)
    assert loader.last_report.mode == "full" and loader.last_report.reason == "WINDOW_CHANGED"
    assert edge_bar["bar_revision_id"] in ids_of(rolled)
    database.add(start=next_open + 20 * MINUTE)
    agree(database, loader, next_open + 40 * MINUTE, session=NEXT_SESSION)
    assert loader.last_report.mode == "delta" and loader.last_report.bar_rows_read == 1
    assert len(loader._entries) == 1


@pytest.mark.parametrize("change, reason", [
    ("tickers", "TICKERS_CHANGED"), ("tier", "NO_STATE"), ("generation", "SOURCE_GENERATION_CHANGED"),
    ("manifest", "MANIFEST_SHA256_CHANGED"), ("members", "MEMBERS_CHANGED"),
])
def test_scope_changes_force_a_full_reload(database, change, reason):
    loader = ScreeningInputLoader()
    agree(database, loader, at(20))
    if change == "tickers":
        database.tickers = ("AAPL", "MSFT", "NVDA")
        database.add(ticker="NVDA", start=OPEN)
    elif change == "tier":
        database.tier = "MASSIVE_ADVANCED"
        database.add(start=OPEN, tier="MASSIVE_ADVANCED", kind="REALTIME_STREAM")
    elif change == "generation":
        database.generation = 2
    elif change == "manifest":
        database.manifest_sha256 = "b" * 64
    else:
        database.instrument_offset = 50
    agree(database, loader, at(30))
    assert loader.last_report.mode == "full" and loader.last_report.reason == reason


def test_flavors_use_separate_slots_and_the_lru_is_bounded(database):
    loader = ScreeningInputLoader()
    agree(database, loader, at(20), portal=True)
    agree(database, loader, at(20), portal=False)
    assert loader.last_report.reason == "NO_STATE" and len(loader._entries) == 2
    agree(database, loader, at(30), portal=True)
    assert loader.last_report.mode == "delta"
    agree(database, loader, at(30), portal=True, details=True)
    assert loader.last_report.reason == "NO_STATE" and len(loader._entries) == 2
    agree(database, loader, at(40), portal=False)  # the least recently used slot was evicted
    assert loader.last_report.reason == "NO_STATE"
    with pytest.raises(ValueError):
        ScreeningInputLoader(max_entries=0)


def test_new_series_forces_a_full_reload_and_database_series_order_is_kept(database):
    loader = ScreeningInputLoader()
    first, _ = agree(database, loader, at(20))
    groups = [(r["ticker"], r["interval"], r["adjustment_basis"]) for r in first.bars]
    assert groups == sorted(groups, key=lambda g: (g[0], INTERVAL_ORDER.index(g[1]), g[2]))
    assert groups != sorted(groups)  # the fake's collation differs from Python's string order
    database.add(interval="30m", start=at(0), kind="DERIVED", observed=at(30), created=at(30))
    agree(database, loader, at(31))
    assert loader.last_report.reason == "NEW_SERIES"
    database.add(interval="15m", start=at(15), observed=at(46), created=at(46), kind="DERIVED")
    database.add(start=OPEN - 5 * MINUTE, observed=at(46), created=at(46))
    result, _ = agree(database, loader, at(50))
    assert loader.last_report.mode == "delta" and loader.last_report.bar_rows_read == 2
    starts = [r["bar_start"] for r in result.bars
              if r["ticker"] == "AAPL" and r["interval"] == "5m" and r["adjustment_basis"] == RAW]
    assert starts == sorted(starts) and starts[0] == OPEN - 5 * MINUTE


def test_a_row_committed_late_with_old_clocks_is_caught_by_the_integrity_count(database):
    loader = ScreeningInputLoader()
    agree(database, loader, at(20))
    straggler = database.add(start=at(5), observed=at(20), created=at(20), committed=False)
    agree(database, loader, at(25))
    assert loader.last_report.mode == "delta" and loader.last_report.bar_rows_read == 0
    database.commit(straggler)
    result, _ = agree(database, loader, at(30))
    assert loader.last_report.mode == "full" and loader.last_report.reason == "VISIBLE_SET_DRIFT"
    assert straggler["bar_revision_id"] in ids_of(result)
    agree(database, loader, at(35))
    assert loader.last_report.mode == "delta"


def test_a_failure_while_loading_drops_the_state_and_the_next_call_is_exact(database):
    loader = ScreeningInputLoader()
    agree(database, loader, at(20))
    database.add(start=at(20))
    database.fail_on = {"delta"}
    with pytest.raises(RuntimeError, match="injected"):
        loader.load(database.cursor(), **database.scope(at(30)))
    assert not loader._entries
    database.fail_on = set()
    agree(database, loader, at(30))
    assert loader.last_report.reason == "NO_STATE"


def test_reset_forgets_state(database):
    loader = ScreeningInputLoader()
    agree(database, loader, at(20))
    loader.reset()
    assert loader.retained_bars == 0 and loader.last_report is None
    agree(database, loader, at(30))
    assert loader.last_report.reason == "NO_STATE"


def test_naive_cutoff_fails_like_the_full_load(database):
    loader = ScreeningInputLoader()
    naive = OPEN.replace(tzinfo=None) + 20 * MINUTE
    with pytest.raises(ValueError, match="timezone-aware"):
        loader.load(database.cursor(), **database.scope(naive))
    with pytest.raises(ValueError, match="timezone-aware"):
        database.full(naive)
    assert not loader._entries


def test_the_incremental_source_index_survives_replacement_chains(database):
    loader = ScreeningInputLoader()
    a = database.add(start=at(15), observed=at(20), created=at(20))
    database.add(start=OPEN, interval="15m", kind="DERIVED", sources=[a["bar_revision_id"]],
                 observed=at(22), created=at(22))
    agree(database, loader, at(25))
    (state,) = loader._entries.values()
    assert state.unresolved == set() and a["bar_revision_id"] in state.source_refs
    b = database.add(start=at(15), observed=at(28), created=at(28))
    agree(database, loader, at(30))
    assert state.unresolved == {a["bar_revision_id"]}
    database.add(start=OPEN, interval="15m", kind="DERIVED", sources=[b["bar_revision_id"], a["bar_revision_id"]],
                 observed=at(34), created=at(34))
    agree(database, loader, at(36))
    assert loader.last_report.mode == "delta"
    state.check_invariants()


def test_the_delta_statement_is_the_shared_visibility_predicate_plus_the_not_visible_clocks(database):
    cursor = database.cursor()
    loader = ScreeningInputLoader()
    loader.load(cursor, **database.scope(at(20)))
    loader.load(cursor, **database.scope(at(30)))
    sql = cursor.statements[cursor.kinds.index("delta")]
    for clock in ("observed_at", "created_at", "replay_available_at", "provider_published_at", "bar_end"):
        assert f"{clock}<=%s" in sql and f"{clock}>%s" in sql
    assert "(replay_available_at IS NULL OR replay_available_at<=%s)" in sql
    assert "(provider_published_at IS NULL OR provider_published_at<=%s)" in sql
    assert "ORDER BY" not in sql
    winners = cursor.statements[cursor.kinds.index("winners_state")]
    assert winners.count("SELECT count(*) FROM rt.bar_revisions") == 1


def test_the_full_load_statement_shape_is_unchanged(database):
    cursor = database.cursor()
    load_realtime_screening_input(cursor, **database.scope(at(20)))
    assert cursor.kinds == ["manifest", "members", "winners", "actions", "coverage"]
    winners = cursor.statements[2]
    assert winners.startswith("SELECT DISTINCT ON (ticker, interval, adjustment_basis, bar_start) bar_revision_id,")
    assert "src_rank" not in winners and "visible_total" not in winners
    assert winners.endswith("observed_at DESC, created_at DESC, bar_revision_id")


def test_close_source_lineage_discovers_in_rounds_and_rejects_missing_evidence():
    rows = {"a": dict(bar_revision_id="a", source_revision_ids=["b", "x"]),
            "b": dict(bar_revision_id="b", source_revision_ids=["c"]),
            "c": dict(bar_revision_id="c", source_revision_ids=[])}
    fetched = []

    def fetch(ids):
        fetched.append(ids)
        return [rows[i] for i in ids if i in rows]

    tail = close_source_lineage({"a"}, {"x"}, fetch)
    assert [row["bar_revision_id"] for row in tail] == ["a", "b", "c"] and fetched == [["a"], ["b"], ["c"]]
    with pytest.raises(ValueError, match="incomplete"):
        close_source_lineage({"zz"}, set(), fetch)


def random_world(rng: random.Random):
    database = FakeDatabase()
    plans = [("5m", RAW), ("15m", RAW), ("1d", RAW), ("1d", ADJ), ("1h", RAW), ("1m", RAW)]
    rows: list[dict] = []

    def add_random():
        ticker = rng.choice(database.tickers)
        interval, basis = rng.choice(plans)
        start = OPEN + rng.randint(0, 12) * 5 * MINUTE
        session = start.date()
        if interval == "1d" and basis == ADJ and rng.random() < 0.5:
            session, start = date(2026, 10, 1), PREVIOUS_OPEN
        end = start + timedelta(minutes=LENGTH[interval])
        observed = end + timedelta(minutes=rng.choice([0, 0, 1, 5, 30, 90, 200]))
        created = observed + timedelta(minutes=rng.choice([-5, 0, 0, 3, 40, 150]))
        replay = None if rng.random() < 0.5 else observed + timedelta(minutes=rng.choice([-3, 0, 2, 60]))
        published = None if rng.random() < 0.7 else observed + timedelta(minutes=rng.choice([-2, 0, 4, 100]))
        kind = rng.choice(["NATIVE_REST", "NATIVE_REST", "DERIVED", "RECONCILED", "REALTIME_STREAM"])
        sources = []
        if kind in ("DERIVED", "RECONCILED") and rows and rng.random() < 0.7:
            sources = rng.sample(rows, k=min(len(rows), rng.randint(1, 3)))
            if rng.random() < 0.75:
                # Usually make the sources visible whenever this row is, so the load succeeds.
                for source in sources:
                    observed = max(observed, source["observed_at"])
                    created = max(created, source["created_at"])
                    end = max(end, source["bar_end"])
                    if source["replay_available_at"]:
                        replay = max(replay or source["replay_available_at"], source["replay_available_at"])
                    if source["provider_published_at"]:
                        published = max(published or source["provider_published_at"],
                                        source["provider_published_at"])
        row = database.add(
            ticker=ticker, interval=interval, basis=basis, start=start, kind=kind, observed=observed,
            created=created, replay=replay, published=published, session=session,
            sources=[s["bar_revision_id"] for s in sources], rid=str(UUID(int=rng.getrandbits(128))),
            scope="RTH" if rng.random() < 0.9 else "EXTENDED", final=rng.random() < 0.95,
            committed=rng.random() < 0.97,
        )
        row["bar_end"] = max(row["bar_end"], end)
        rows.append(row)

    for _ in range(rng.randint(15, 45)):
        add_random()
    return database, add_random


@pytest.mark.parametrize("seed", range(60))
def test_random_streams_of_revisions_match_the_full_load(seed):
    rng = random.Random(seed)
    database, add_random = random_world(rng)
    loader = ScreeningInputLoader()
    cutoff = OPEN + timedelta(minutes=rng.randint(0, 120))
    for _ in range(14):
        for row in database.rows:
            if not row["_committed"] and rng.random() < 0.5:
                row["_committed"] = True
        for _ in range(rng.randint(0, 6)):
            add_random()
        cutoff = max(OPEN, cutoff + timedelta(minutes=rng.choice([0, 5, 10, 15, 30, 30, 60, -20])))
        agree(database, loader, cutoff, portal=bool(seed % 5))
        for state in loader._entries.values():
            state.check_invariants()


def test_the_random_streams_exercise_the_delta_path_substantially():
    deltas = total = 0
    reasons = set()
    for seed in range(60, 90):
        rng = random.Random(seed)
        database, add_random = random_world(rng)
        loader = ScreeningInputLoader()
        cutoff = OPEN + timedelta(minutes=rng.randint(0, 60))
        for _ in range(10):
            for _ in range(rng.randint(0, 4)):
                add_random()
            cutoff += timedelta(minutes=rng.choice([5, 10, 30, 60]))
            previous = loader.last_report
            agree(database, loader, cutoff)
            if loader.last_report is previous:  # both loads failed the same way
                continue
            total += 1
            deltas += loader.last_report.mode == "delta"
            reasons.add(loader.last_report.reason)
    assert deltas >= total * 0.3, (deltas, total)
    assert {"NO_STATE", "NEW_SERIES"} <= reasons


# ---- the second clock: known_by (when the system must have recorded a bar) next to the market cutoff ----

RELEASE_DELAY = timedelta(minutes=15)
BUDGET = timedelta(seconds=90)


def window(index: int):
    """(cutoff, release_at, known_by): a window's market boundary, its ingest start and its deadline."""
    cutoff = at(30 + 15 * index)
    return cutoff, cutoff + RELEASE_DELAY, cutoff + RELEASE_DELAY + BUDGET


def fetched_in_window(database, index: int, *, ticker="AAPL", count=3, **fields):
    """5m bars that ended by the window's cutoff and were fetched and inserted at its release (after it)."""
    cutoff, release, _ = window(index)
    rows = []
    for number in range(count):
        end = cutoff - 5 * number * MINUTE
        stamp = release + timedelta(seconds=20 + number)
        rows.append(database.add(ticker=ticker, start=end - 5 * MINUTE, observed=stamp, created=stamp, **fields))
    return rows


def test_a_default_known_by_reproduces_the_single_clock_statements_exactly(database):
    recorded = []
    for known_by in (None, at(20)):
        cursor = database.cursor()
        load_realtime_screening_input(cursor, **database.scope(at(20), known_by=known_by))
        recorded.append((cursor.kinds, cursor.statements, cursor.params))
    assert recorded[0] == recorded[1]
    kinds, _, params = recorded[0]
    assert params[kinds.index("winners")][3:8] == (at(20),) * 5


def test_a_bar_fetched_after_the_cutoff_but_before_known_by_is_visible_in_its_own_window(database):
    cutoff, release, known_by = window(0)
    fetched = fetched_in_window(database, 0)
    stale = database.full(cutoff)
    assert not {row["bar_revision_id"] for row in fetched} & set(ids_of(stale))
    loader = ScreeningInputLoader()
    result, cursor = agree(database, loader, cutoff, known_by=known_by)
    assert {row["bar_revision_id"] for row in fetched} <= set(ids_of(result))
    # The identity of the input stays the market cutoff.
    assert result.cutoff == cutoff
    winners = next(p for k, p in zip(cursor.kinds, cursor.params) if k == "winners_state")
    assert winners[3:8] == (known_by,) * 4 + (cutoff,)


def test_a_bar_inserted_after_known_by_is_not_visible_until_a_later_window(database):
    loader = ScreeningInputLoader()
    cutoff, _, known_by = window(0)
    agree(database, loader, cutoff, known_by=known_by)
    late = database.add(start=cutoff - 5 * MINUTE, observed=known_by + 5 * timedelta(seconds=1),
                        created=known_by + 6 * timedelta(seconds=1))
    result, _ = agree(database, loader, cutoff, known_by=known_by)
    assert late["bar_revision_id"] not in ids_of(result)
    assert loader.last_report.mode == "delta" and loader.last_report.bar_rows_read == 0
    cutoff1, _, known_by1 = window(1)
    result, _ = agree(database, loader, cutoff1, known_by=known_by1)
    assert late["bar_revision_id"] in ids_of(result)
    assert loader.last_report.mode == "delta" and loader.last_report.bar_rows_read == 1


def test_a_bar_that_ends_after_the_cutoff_stays_invisible_however_early_it_was_recorded(database):
    loader = ScreeningInputLoader()
    cutoff, _, known_by = window(0)
    agree(database, loader, cutoff, known_by=known_by)
    running = database.add(start=cutoff - 2 * MINUTE, observed=cutoff + 3 * MINUTE, created=cutoff + 3 * MINUTE)
    result, _ = agree(database, loader, cutoff, known_by=known_by)
    assert running["bar_revision_id"] not in ids_of(result) and loader.last_report.bar_rows_read == 0
    # The cutoff passes its end while known_by is unchanged: bar_end alone makes it newly visible.
    result, _ = agree(database, loader, cutoff + 5 * MINUTE, known_by=known_by + 5 * MINUTE)
    assert running["bar_revision_id"] in ids_of(result) and loader.last_report.bar_rows_read == 1


def test_consecutive_windows_read_only_what_each_window_fetched(database):
    loader = ScreeningInputLoader()
    for index in range(4):
        cutoff, _, known_by = window(index)
        fetched = fetched_in_window(database, index)
        stale = database.full(cutoff)
        assert not {row["bar_revision_id"] for row in fetched} & set(ids_of(stale))  # the single-clock defect
        result, _ = agree(database, loader, cutoff, known_by=known_by)
        assert {row["bar_revision_id"] for row in fetched} <= set(ids_of(result))
        if index:
            assert loader.last_report.mode == "delta" and loader.last_report.bar_rows_read == 3
    for state in loader._entries.values():
        state.check_invariants()


def test_known_by_advancing_alone_reuses_state_and_finds_what_was_recorded_in_between(database):
    loader = ScreeningInputLoader()
    cutoff, _, known_by = window(0)
    agree(database, loader, cutoff, known_by=known_by)
    row = database.add(start=cutoff - 5 * MINUTE, observed=known_by + 10 * timedelta(seconds=1),
                       created=known_by + 10 * timedelta(seconds=1))
    result, _ = agree(database, loader, cutoff, known_by=known_by + MINUTE)
    assert loader.last_report.mode == "delta" and loader.last_report.bar_rows_read == 1
    assert row["bar_revision_id"] in ids_of(result)


def test_either_clock_going_backwards_forces_a_full_reload(database):
    loader = ScreeningInputLoader()
    cutoff, _, known_by = window(1)
    agree(database, loader, cutoff, known_by=known_by)
    agree(database, loader, cutoff, known_by=known_by - MINUTE)
    assert loader.last_report.mode == "full" and loader.last_report.reason == "KNOWN_BY_REGRESSED"
    agree(database, loader, cutoff - 5 * MINUTE, known_by=known_by + 30 * MINUTE)
    assert loader.last_report.mode == "full" and loader.last_report.reason == "CUTOFF_REGRESSED"
    agree(database, loader, cutoff, known_by=known_by + 30 * MINUTE)
    assert loader.last_report.mode == "delta"


def test_the_source_closure_is_read_under_known_by_too(database):
    loader = ScreeningInputLoader()
    cutoff, release, known_by = window(0)
    stamp = release + timedelta(seconds=20)
    source = database.add(start=cutoff - 15 * MINUTE, interval="1m", scope="EXTENDED", observed=stamp, created=stamp)
    derived = database.add(start=cutoff - 15 * MINUTE, interval="15m", kind="DERIVED",
                           sources=[source["bar_revision_id"]], observed=stamp, created=stamp)
    agree(database, loader, at(20), known_by=at(20))
    result, _ = agree(database, loader, cutoff, known_by=known_by)
    assert loader.last_report.mode == "delta" and loader.last_report.source_rows_read == 1
    assert {source["bar_revision_id"], derived["bar_revision_id"]} <= set(ids_of(result))
    assert database.full(cutoff).bars != result.bars  # without known_by the derived bar is invisible


def test_a_row_inserted_after_the_load_but_before_known_by_is_a_late_arrival_that_forces_a_reload(database):
    """known_by lies after the wall clock of the load, so a row recorded later with created_at <= known_by is
    already 'visible' to the next delta's old clocks yet was never loaded; the integrity count catches it."""
    loader = ScreeningInputLoader()
    cutoff, release, known_by = window(0)
    fetched_in_window(database, 0)
    agree(database, loader, cutoff, known_by=known_by)
    straggler = database.add(start=at(22), observed=release + 60 * timedelta(seconds=1),
                             created=release + 61 * timedelta(seconds=1), committed=False)
    agree(database, loader, cutoff, known_by=known_by)
    database.commit(straggler)
    cutoff1, _, known_by1 = window(1)
    result, _ = agree(database, loader, cutoff1, known_by=known_by1)
    assert loader.last_report.mode == "full" and loader.last_report.reason == "VISIBLE_SET_DRIFT"
    assert straggler["bar_revision_id"] in ids_of(result)


@pytest.mark.parametrize("bad, message", [
    (OPEN.replace(tzinfo=None) + 40 * MINUTE, "known_by must be timezone-aware"),
    (at(10), "known_by must not precede the cutoff"),
])
def test_known_by_must_be_aware_and_not_before_the_cutoff(database, bad, message):
    loader = ScreeningInputLoader()
    for load in (lambda cursor: loader.load(cursor, **database.scope(at(20), known_by=bad)),
                 lambda cursor: load_realtime_screening_input(cursor, **database.scope(at(20), known_by=bad))):
        cursor = database.cursor()
        with pytest.raises(ValueError, match=message):
            load(cursor)
        assert cursor.kinds == []
    assert not loader._entries


def known_by_stream(seed: int, steps: int):
    """Yields (database, loader, cutoff, known_by) per cycle of a random world; known_by mostly advances with
    the cutoff but sometimes stays or falls back, so every reuse rule is exercised."""
    rng = random.Random(5000 + seed)
    database, add_random = random_world(rng)
    loader = ScreeningInputLoader()
    cutoff = OPEN + timedelta(minutes=rng.randint(0, 120))
    known_by = cutoff
    for _ in range(steps):
        for row in database.rows:
            if not row["_committed"] and rng.random() < 0.5:
                row["_committed"] = True
        for _ in range(rng.randint(0, 6)):
            add_random()
        cutoff = max(OPEN, cutoff + timedelta(minutes=rng.choice([0, 5, 10, 15, 15, 30, 60, -20])))
        offset = timedelta(minutes=rng.choice([0, 0, 2, 16, 16, 30]), seconds=rng.choice([0, 30]))
        known_by = max(known_by, cutoff + offset) if rng.random() < 0.85 else cutoff + offset
        yield database, loader, cutoff, known_by, rng


@pytest.mark.parametrize("seed", range(60))
def test_random_streams_with_both_clocks_match_the_full_load(seed):
    for database, loader, cutoff, known_by, _ in known_by_stream(seed, 14):
        agree(database, loader, cutoff, known_by=known_by, portal=bool(seed % 5))
        for state in loader._entries.values():
            state.check_invariants()


def test_the_two_clock_streams_exercise_the_delta_path_substantially():
    deltas = total = 0
    reasons = set()
    for seed in range(60, 90):
        for database, loader, cutoff, known_by, _ in known_by_stream(seed, 10):
            previous = loader.last_report
            agree(database, loader, cutoff, known_by=known_by)
            if loader.last_report is previous:
                continue
            total += 1
            deltas += loader.last_report.mode == "delta"
            reasons.add(loader.last_report.reason)
    # Rows inserted between cycles carry arbitrary clocks, and with known_by ahead of the cutoff many are late
    # arrivals (already visible under the old clocks): those correctly force a full reload.
    assert deltas >= total * 0.15, (deltas, total)
    assert {"NO_STATE", "KNOWN_BY_REGRESSED", "CUTOFF_REGRESSED", "VISIBLE_SET_DRIFT"} <= reasons
