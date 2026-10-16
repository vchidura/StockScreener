"""DB-backed proof that the delta screening loader equals the full load on the real V2 schema.

Opt in with STOCKS_V2_DB_TESTS=1 and run WITHOUT the offline I/O guard plugin. Uses the disposable database
test_realtime_v2_delta_a only (always dropped). The settings never leave tests/realtime/v2_database.py.
The fake-cursor suite (test_screening_delta_loader.py) proves the loader's logic; this one proves that the
shared SQL (visibility predicate, "became visible" clauses, source precedence, count) means what the logic
assumes, on real rows with real NULL clocks, partitions and constraints.
"""
from __future__ import annotations

import random
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
from psycopg2.extras import RealDictCursor

from realtime.screening_delta_loader import ScreeningInputLoader
from realtime.screening_repository import (
    BarClocks,
    BarScope,
    _bar_candidate_params,
    _bar_candidate_sql,
    load_realtime_screening_input,
    load_realtime_stock_alert_input,
    screening_history_window,
    select_new_bars,
)
from tests.realtime.screening_delta_fixtures import (
    ADJ,
    RAW,
    SESSION,
    Bar,
    create_world,
    ensure_history_partitions,
    insert_bars,
)
from tests.realtime.v2_database import disposable_v2_database, requires_v2_database

pytestmark = requires_v2_database

DATABASE = "test_realtime_v2_delta_a"
UTC = timezone.utc
OPEN = datetime(2026, 10, 2, 13, 30, tzinfo=UTC)
PREVIOUS_OPEN = datetime(2026, 10, 1, 13, 30, tzinfo=UTC)
NEXT_OPEN = datetime(2026, 10, 5, 13, 30, tzinfo=UTC)
MINUTE = timedelta(minutes=1)


class _Pool:
    """Hides the pool behind a repr that carries nothing sensitive."""

    def __init__(self, pool):
        self._pool = pool

    def __repr__(self) -> str:
        return "<disposable pool>"

    @contextmanager
    def transaction(self, *, repeatable_read: bool = False):
        connection = self._pool.getconn()
        try:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                if repeatable_read:
                    cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                yield cursor
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            self._pool.putconn(connection)

    @contextmanager
    def writer(self):
        """A connection whose transaction the test commits (or not) itself."""
        connection = self._pool.getconn()
        try:
            yield connection
        finally:
            connection.rollback()
            self._pool.putconn(connection)


@pytest.fixture(scope="module")
def pool():
    with disposable_v2_database(DATABASE) as connection_pool:
        wrapped = _Pool(connection_pool)
        with wrapped.transaction() as cursor:
            ensure_history_partitions(cursor)
        yield wrapped


def at(minutes: int) -> datetime:
    return OPEN + timedelta(minutes=minutes)


def seeded_world(pool, tickers=("AAPL", "MSFT")):
    with pool.transaction() as cursor:
        world = create_world(cursor, tickers)
        bars = []
        for ticker in tickers:
            bars += [Bar(ticker, "5m", at(5 * index)) for index in range(3)]
            bars += [Bar(ticker, interval, PREVIOUS_OPEN + 30 * MINUTE) for interval in ("1m", "15m", "1h")]
            bars += [Bar(ticker, "1d", PREVIOUS_OPEN, basis, session=date(2026, 10, 1)) for basis in (ADJ, RAW)]
        insert_bars(cursor, world, bars)
    return world


def test_stock_alert_loader_reads_daily_and_thirty_minute_bars_only(pool):
    world = seeded_world(pool, ("AAPL",))
    add(pool, world, Bar("AAPL", "30m", PREVIOUS_OPEN, session=date(2026, 10, 1)))
    with pool.transaction() as cursor:
        inputs = load_realtime_stock_alert_input(
            cursor,
            cohort_id=world.cohort_id,
            session=SESSION,
            cutoff=at(20),
            source_generation=1,
        )
    assert {row["interval"] for row in inputs.bars} == {"1d", "30m"}


def add(pool, world, *bars):
    with pool.transaction() as cursor:
        return insert_bars(cursor, world, list(bars))


def scope(world, cutoff, *, session=SESSION, portal=True, details=False, known_by=None):
    return dict(cohort_id=world.cohort_id, session=session, cutoff=cutoff, source_generation=1,
                include_portal_history=portal, include_strategy_action_details=details, known_by=known_by)


def outcome(call):
    try:
        return "ok", call()
    except Exception as error:  # the comparison includes the failure the full load raises
        return "error", type(error).__name__, str(error)


def agree(pool, world, loader, cutoff, *, repeatable_read=False, **kw):
    """Delta and full loads at one cutoff must match exactly (bars, order, row dicts, key order, value types)."""
    with pool.transaction(repeatable_read=repeatable_read) as cursor:
        delta = outcome(lambda: loader.load(cursor, **scope(world, cutoff, **kw)))
        full = outcome(lambda: load_realtime_screening_input(cursor, **scope(world, cutoff, **kw)))
    assert delta == full
    if delta[0] == "ok":
        assert [list(row) for row in delta[1].bars] == [list(row) for row in full[1].bars]
        assert [[type(v) for v in row.values()] for row in delta[1].bars] == \
               [[type(v) for v in row.values()] for row in full[1].bars]
        return delta[1]
    return None


def ids_of(result):
    return [str(row["bar_revision_id"]) for row in result.bars]


def test_cold_no_change_new_bar_and_repeatable_read(pool):
    world = seeded_world(pool)
    loader = ScreeningInputLoader()
    first = agree(pool, world, loader, at(20))
    assert loader.last_report.mode == "full" and loader.last_report.reason == "NO_STATE" and first.bars
    agree(pool, world, loader, at(25), repeatable_read=True)
    assert loader.last_report.mode == "delta" and loader.last_report.bar_rows_read == 0
    (new_id,) = add(pool, world, Bar("AAPL", "5m", at(20)))
    result = agree(pool, world, loader, at(30), repeatable_read=True)
    assert loader.last_report.mode == "delta" and loader.last_report.bar_rows_read == 1
    assert new_id in ids_of(result)
    agree(pool, world, loader, at(30))
    assert loader.last_report.bar_rows_read == 0


@pytest.mark.parametrize("clock", ["observed_at", "created_at", "replay_available_at", "provider_published_at"])
def test_each_visibility_clock_crossing_the_window_is_found_by_the_delta_sql(pool, clock):
    world = seeded_world(pool)
    loader = ScreeningInputLoader()
    agree(pool, world, loader, at(20))
    start, crossing = at(30), at(50)
    fields = dict(observed=start + 6 * MINUTE, created=start + 6 * MINUTE)
    if clock == "observed_at":
        fields = dict(observed=crossing, created=start)
    elif clock == "created_at":
        fields["created"] = crossing
    elif clock == "replay_available_at":
        fields["replay"] = crossing
    else:
        fields["published"] = crossing
    (row_id,) = add(pool, world, Bar("AAPL", "5m", start, **fields))
    before = agree(pool, world, loader, at(40))
    assert row_id not in ids_of(before) and loader.last_report.bar_rows_read == 0
    after = agree(pool, world, loader, at(60))
    assert loader.last_report.mode == "delta" and loader.last_report.bar_rows_read == 1
    assert row_id in ids_of(after)


def test_null_optional_clocks_never_cross(pool):
    world = seeded_world(pool)
    loader = ScreeningInputLoader()
    agree(pool, world, loader, at(20))
    add(pool, world, Bar("AAPL", "5m", at(25), replay=None, published=None))
    agree(pool, world, loader, at(40))
    assert loader.last_report.bar_rows_read == 1
    agree(pool, world, loader, at(90))
    assert loader.last_report.bar_rows_read == 0


OLD_AT = at(100)
LOW_ID, HIGH_ID = "LOW", "HIGH"  # replaced per test by two fresh ids in that order (the catalog id is global)
# (id, portal flavor, interval, basis, old fields, new fields, new wins); DERIVED and RECONCILED rows need sources.
PRECEDENCE_CASES = [
    ("portal_derived_15m_beats_native", True, "15m", RAW, dict(kind="NATIVE_REST"), dict(kind="DERIVED"), True),
    ("portal_derived_15m_beats_reconciled", True, "15m", RAW, dict(kind="RECONCILED"), dict(kind="DERIVED"), True),
    ("portal_native_15m_loses_to_derived", True, "15m", RAW, dict(kind="DERIVED"), dict(kind="NATIVE_REST"), False),
    ("portal_native_5m_beats_reconciled", True, "5m", RAW, dict(kind="RECONCILED"), dict(kind="NATIVE_REST"), True),
    ("portal_reconciled_5m_loses_to_native", True, "5m", RAW, dict(kind="NATIVE_REST"), dict(kind="RECONCILED"), False),
    ("portal_native_5m_beats_derived", True, "5m", RAW, dict(kind="DERIVED"), dict(kind="NATIVE_REST"), True),
    ("portal_reconciled_beats_native_30m", True, "30m", RAW, dict(kind="NATIVE_REST"), dict(kind="RECONCILED"), True),
    ("portal_adjusted_native_beats_reconciled", True, "1d", ADJ, dict(kind="RECONCILED"), dict(kind="NATIVE_REST"), True),
    ("portal_adjusted_reconciled_loses_to_native", True, "1d", ADJ, dict(kind="NATIVE_REST"), dict(kind="RECONCILED"), False),
    ("plain_reconciled_beats_native", False, "1d", RAW, dict(kind="NATIVE_REST"), dict(kind="RECONCILED"), True),
    ("plain_native_beats_derived", False, "1d", RAW, dict(kind="DERIVED"), dict(kind="NATIVE_REST"), True),
    ("plain_derived_loses_to_native", False, "1d", RAW, dict(kind="NATIVE_REST"), dict(kind="DERIVED"), False),
    ("portal_later_observed_wins", True, "5m", RAW, dict(), dict(observed=OLD_AT + 2 * MINUTE), True),
    ("portal_earlier_observed_loses", True, "5m", RAW, dict(), dict(observed=OLD_AT - 5 * MINUTE), False),
    ("plain_later_observed_wins", False, "1d", RAW, dict(), dict(observed=OLD_AT + 2 * MINUTE), True),
    ("portal_tie_later_created_wins", True, "5m", RAW, dict(), dict(created=OLD_AT + 2 * MINUTE), True),
    ("portal_tie_earlier_created_loses", True, "5m", RAW, dict(created=OLD_AT - MINUTE), dict(created=OLD_AT - 2 * MINUTE), False),
    ("plain_tie_earlier_created_loses", False, "1d", RAW, dict(created=OLD_AT - MINUTE), dict(created=OLD_AT - 2 * MINUTE), False),
    ("portal_full_tie_lower_id_wins", True, "5m", RAW, dict(rid=HIGH_ID), dict(rid=LOW_ID), True),
    ("portal_full_tie_higher_id_loses", True, "5m", RAW, dict(rid=LOW_ID), dict(rid=HIGH_ID), False),
    ("plain_full_tie_lower_id_wins", False, "1d", RAW, dict(rid=HIGH_ID), dict(rid=LOW_ID), True),
]


@pytest.mark.parametrize("case", PRECEDENCE_CASES, ids=lambda case: case[0])
def test_precedence_rules_on_the_real_sql(pool, case):
    _, portal, interval, basis, old_fields, new_fields, new_wins = case
    world = seeded_world(pool)
    with pool.transaction() as cursor:
        cursor.execute("SELECT bar_revision_id FROM rt.bar_revisions WHERE cohort_id=%s AND ticker='AAPL' "
                       "AND interval='5m' ORDER BY bar_start LIMIT 1", (str(world.cohort_id),))
        source = (str(cursor.fetchone()["bar_revision_id"]),)
    start = at(60) if interval != "1d" else datetime(2026, 9, 30, 13, 30, tzinfo=UTC)
    base = dict(basis=basis, session=start.date(), observed=OLD_AT - 3 * MINUTE, created=OLD_AT - 3 * MINUTE)
    fresh = dict(zip((LOW_ID, HIGH_ID), sorted(str(uuid4()) for _ in range(2))))

    def build(fields, **extra):
        merged = {**base, **extra, **fields}
        if "rid" in merged:
            merged["rid"] = fresh[merged["rid"]]
        return Bar("AAPL", interval, start, sources=source if merged.get("kind") in ("DERIVED", "RECONCILED") else (),
                   **merged)

    (old_id,) = add(pool, world, build(old_fields))
    loader = ScreeningInputLoader()
    agree(pool, world, loader, OLD_AT, portal=portal)
    (new_id,) = add(pool, world, build(new_fields, replay=OLD_AT + MINUTE))
    result = agree(pool, world, loader, OLD_AT + 10 * MINUTE, portal=portal)
    assert loader.last_report.mode == "delta" and loader.last_report.bar_rows_read == 1
    winner = next(row for row in result.bars if row["ticker"] == "AAPL" and row["interval"] == interval
                  and row["adjustment_basis"] == basis and row["bar_start"] == start)
    assert str(winner["bar_revision_id"]) == (new_id if new_wins else old_id)
    later = agree(pool, world, loader, OLD_AT + 300 * MINUTE, portal=portal)
    assert str(winner["bar_revision_id"]) in ids_of(later)


def test_derived_bars_and_their_source_revisions(pool):
    world = seeded_world(pool)
    loader = ScreeningInputLoader()
    agree(pool, world, loader, at(20))
    # A source outside the screened scope (EXTENDED) is read by id; its derived bar is a normal candidate.
    (source_id,) = add(pool, world, Bar("AAPL", "1m", at(30), scope="EXTENDED", observed=at(45), created=at(45)))
    (derived_id,) = add(pool, world, Bar("AAPL", "15m", at(30), kind="DERIVED", sources=(source_id,),
                                         observed=at(46), created=at(46)))
    result = agree(pool, world, loader, at(47))
    assert loader.last_report.mode == "delta" and loader.last_report.source_rows_read == 1
    assert source_id in ids_of(result) and derived_id in ids_of(result)
    agree(pool, world, loader, at(50))
    assert loader.last_report.source_rows_read == 0
    # A derived bar whose source is still invisible makes both loads fail the same way, then both succeed.
    (hidden_id,) = add(pool, world, Bar("AAPL", "1m", at(60), scope="EXTENDED", observed=at(95), created=at(95)))
    add(pool, world, Bar("AAPL", "15m", at(60), kind="DERIVED", sources=(hidden_id,), observed=at(80), created=at(80)))
    assert agree(pool, world, loader, at(85)) is None
    agree(pool, world, loader, at(100))
    assert loader.last_report.mode == "full" and loader.last_report.reason == "NO_STATE"
    agree(pool, world, loader, at(110))
    assert loader.last_report.mode == "delta"


def test_a_superseded_winner_that_a_derived_bar_references_stays_as_a_source_row(pool):
    world = seeded_world(pool)
    loader = ScreeningInputLoader()
    (source_id,) = add(pool, world, Bar("AAPL", "5m", at(40), observed=at(46), created=at(46)))
    add(pool, world, Bar("AAPL", "15m", at(40), kind="DERIVED", sources=(source_id,), observed=at(56), created=at(56)))
    agree(pool, world, loader, at(60))
    (replacement_id,) = add(pool, world, Bar("AAPL", "5m", at(40), observed=at(66), created=at(66)))
    result = agree(pool, world, loader, at(70))
    assert loader.last_report.mode == "delta"
    ids = ids_of(result)
    assert ids.index(source_id) > ids.index(replacement_id)


def test_cutoff_going_backwards_and_a_new_session_force_full_reloads(pool):
    world = seeded_world(pool)
    (late_id,) = add(pool, world, Bar("AAPL", "5m", at(30), observed=at(40), created=at(40)))
    loader = ScreeningInputLoader()
    assert late_id in ids_of(agree(pool, world, loader, at(60)))
    assert late_id not in ids_of(agree(pool, world, loader, at(30)))
    assert loader.last_report.reason == "CUTOFF_REGRESSED"
    agree(pool, world, loader, at(60))
    assert loader.last_report.mode == "delta" and loader.last_report.bar_rows_read == 1
    add(pool, world, Bar("AAPL", "5m", NEXT_OPEN))
    agree(pool, world, loader, NEXT_OPEN + 20 * MINUTE, session=date(2026, 10, 5))
    assert loader.last_report.mode == "full" and loader.last_report.reason == "WINDOW_CHANGED"
    add(pool, world, Bar("AAPL", "5m", NEXT_OPEN + 20 * MINUTE))
    agree(pool, world, loader, NEXT_OPEN + 40 * MINUTE, session=date(2026, 10, 5))
    assert loader.last_report.mode == "delta" and loader.last_report.bar_rows_read == 1


def test_a_new_series_forces_a_full_reload(pool):
    world = seeded_world(pool)
    loader = ScreeningInputLoader()
    agree(pool, world, loader, at(20))
    add(pool, world, Bar("AAPL", "30m", at(0), observed=at(31), created=at(31)))
    agree(pool, world, loader, at(32))
    assert loader.last_report.reason == "NEW_SERIES"


def test_members_must_match_the_manifest_like_the_full_load(pool):
    with pool.transaction() as cursor:
        world = create_world(cursor, ("AAPL", "MSFT"), stock_count=3)
    loader = ScreeningInputLoader()
    assert agree(pool, world, loader, at(20)) is None


def test_a_row_committed_late_with_old_clocks_is_caught_by_the_integrity_count(pool):
    world = seeded_world(pool)
    loader = ScreeningInputLoader()
    agree(pool, world, loader, at(20))
    with pool.writer() as connection:
        with connection.cursor() as cursor:
            (straggler,) = insert_bars(cursor, world, [Bar("AAPL", "5m", at(5), observed=at(20), created=at(20))])
        agree(pool, world, loader, at(25))  # the writer's transaction is still open
        assert loader.last_report.mode == "delta" and loader.last_report.bar_rows_read == 0
        connection.commit()
    result = agree(pool, world, loader, at(30))
    assert loader.last_report.mode == "full" and loader.last_report.reason == "VISIBLE_SET_DRIFT"
    assert straggler in ids_of(result)
    agree(pool, world, loader, at(35))
    assert loader.last_report.mode == "delta"


def random_bars(rng: random.Random, tickers, existing: list, count: int):
    """Random revisions with every clock combination the visibility rules distinguish (NULL optional clocks,
    clocks on either side of the cutoffs). Derived and reconciled rows reference earlier rows; usually their
    clocks are raised so a visible derived bar has visible sources, sometimes not (the load then fails)."""
    plans = [("5m", RAW), ("15m", RAW), ("1d", RAW), ("1d", ADJ), ("1h", RAW), ("1m", RAW)]
    lengths = {"1m": 1, "5m": 5, "15m": 15, "1h": 60, "1d": 390}
    bars: list[Bar] = []
    for _ in range(count):
        interval, basis = rng.choice(plans)
        start = OPEN + rng.randint(0, 12) * 5 * MINUTE
        session = start.date()
        if interval == "1d" and basis == ADJ and rng.random() < 0.5:
            session, start = date(2026, 10, 1), PREVIOUS_OPEN
        end = start + timedelta(minutes=lengths[interval])
        observed = end + timedelta(minutes=rng.choice([0, 0, 1, 5, 30, 90, 200]))
        created = observed + timedelta(minutes=rng.choice([-5, 0, 0, 3, 40, 150]))
        replay = None if rng.random() < 0.5 else observed + timedelta(minutes=rng.choice([-3, 0, 2, 60]))
        published = None if rng.random() < 0.7 else observed + timedelta(minutes=rng.choice([-2, 0, 4, 100]))
        kind, sources = rng.choice(["NATIVE_REST", "NATIVE_REST", "DERIVED", "RECONCILED"]), ()
        earlier = existing + bars
        if kind != "NATIVE_REST":
            if not earlier or rng.random() < 0.2:
                kind = "NATIVE_REST"
            else:
                chosen = rng.sample(earlier, k=min(len(earlier), rng.randint(1, 3)))
                sources = tuple(bar.rid for bar in chosen)
                if rng.random() < 0.75:
                    for source in chosen:
                        observed = max(observed, source.observed)
                        created = max(created, source.created)
                        replay = max(filter(None, (replay, source.replay)), default=None)
                        published = max(filter(None, (published, source.published)), default=None)
        bars.append(Bar(rng.choice(tickers), interval, start, basis, kind=kind, observed=observed, created=created,
                        replay=replay, published=published, sources=sources, session=session,
                        scope="RTH" if rng.random() < 0.9 else "EXTENDED", final=rng.random() < 0.95,
                        rid=str(UUID(int=rng.getrandbits(128)))))
    return bars


def test_random_revision_streams_match_the_full_load_on_real_rows(pool):
    deltas = total = 0
    for seed in range(8):
        rng = random.Random(seed)
        tickers = ("AAPL", "MSFT")
        with pool.transaction() as cursor:
            world = create_world(cursor, tickers)
        existing: list[Bar] = []
        loader = ScreeningInputLoader()
        cutoff = OPEN + timedelta(minutes=rng.randint(0, 120))
        known_by = cutoff
        for step in range(12):
            batch = random_bars(rng, tickers, existing, rng.randint(15, 40) if step == 0 else rng.randint(0, 6))
            with pool.transaction() as cursor:
                insert_bars(cursor, world, batch)
            existing += batch
            cutoff = max(OPEN, cutoff + timedelta(minutes=rng.choice([0, 5, 10, 15, 30, 30, 60, -20])))
            if seed % 2:  # two clocks: known_by usually advances with the cutoff, sometimes falls back
                offset = timedelta(minutes=rng.choice([0, 2, 16, 16, 30]))
                known_by = max(known_by, cutoff + offset) if rng.random() < 0.85 else cutoff + offset
            previous = loader.last_report
            agree(pool, world, loader, cutoff, portal=bool(seed % 4), known_by=known_by if seed % 2 else None)
            if loader.last_report is not previous:
                total += 1
                deltas += loader.last_report.mode == "delta"
    assert deltas >= total * 0.15, (deltas, total)


def expected_rank(row, portal: bool) -> int:
    kind, interval, basis = row["source_kind"], row["interval"], row["adjustment_basis"]
    if portal:
        if basis == ADJ and interval in ("1d", "1wk", "1mo") and kind == "NATIVE_REST":
            return 5
        if basis == RAW and interval in ("1m", "5m") and kind == "NATIVE_REST":
            return 5
        if basis == RAW and interval in ("15m", "30m", "1h", "1d") and kind == "DERIVED":
            return 5
    return {"RECONCILED": 4, "NATIVE_REST": 3, "DERIVED": 2}.get(kind, 1)


@pytest.mark.parametrize("portal", [True, False])
def test_the_delta_select_is_exactly_visible_now_minus_visible_before(pool, portal):
    """On real rows with NULL optional clocks and both clocks moving forward: select_new_bars returns
    V(c1, k1) minus V(c0, k0) and counts V(c1, k1)."""
    rng = random.Random(1000 + portal)
    tickers = ("AAPL", "MSFT")
    with pool.transaction() as cursor:
        world = create_world(cursor, tickers)
    batch = random_bars(rng, tickers, [], 400)
    for bar in batch:
        bar.sources, bar.kind = (), "NATIVE_REST"
    with pool.transaction() as cursor:
        insert_bars(cursor, world, batch)
    _, history = screening_history_window(SESSION, include_portal_history=portal)
    bar_scope = BarScope(world.cohort_id, "MASSIVE_DEVELOPER", tickers, portal, history)
    checked = 0
    with pool.transaction() as cursor:
        def visible(clocks):
            cursor.execute("SELECT bar_revision_id FROM rt.bar_revisions WHERE " + _bar_candidate_sql(portal),
                           _bar_candidate_params(bar_scope, clocks))
            return {str(row["bar_revision_id"]) for row in cursor.fetchall()}

        for _ in range(40):
            c0 = OPEN + timedelta(minutes=rng.randint(0, 400))
            k0 = c0 + timedelta(minutes=rng.choice([0, 0, 2, 16, 60]))
            c1 = c0 + timedelta(minutes=rng.choice([0, 1, 5, 30, 200]))
            k1 = max(k0, c1) + timedelta(minutes=rng.choice([0, 0, 1, 16, 90]))
            before, after = visible(BarClocks(c0, k0)), visible(BarClocks(c1, k1))
            assert before <= after
            ranked, total = select_new_bars(cursor, bar_scope, BarClocks(c0, k0), BarClocks(c1, k1))
            assert {str(row["bar_revision_id"]) for row, _ in ranked} == after - before
            assert len(ranked) == len(after - before) and total == len(after)
            assert all(rank == expected_rank(row, portal) for row, rank in ranked)
            checked += len(ranked)
    assert checked > 0


def window_clocks(index: int):
    cutoff = at(30 + 15 * index)
    return cutoff, cutoff + timedelta(minutes=15, seconds=90)


def test_a_bar_fetched_after_the_cutoff_but_before_known_by_is_visible_in_its_own_window(pool):
    world = seeded_world(pool)
    loader = ScreeningInputLoader()
    cutoff, known_by = window_clocks(0)
    release_stamp = cutoff + timedelta(minutes=15, seconds=20)
    (fetched_id,) = add(pool, world, Bar("AAPL", "5m", cutoff - 5 * MINUTE, observed=release_stamp,
                                          created=release_stamp + timedelta(seconds=1)))
    with pool.transaction() as cursor:
        stale = load_realtime_screening_input(cursor, **scope(world, cutoff))
    assert fetched_id not in ids_of(stale)  # the single clock misses it
    result = agree(pool, world, loader, cutoff, known_by=known_by)
    assert fetched_id in ids_of(result) and result.cutoff == cutoff and result.known_by == known_by
    (late_id,) = add(pool, world, Bar("AAPL", "5m", cutoff - 10 * MINUTE, observed=known_by + timedelta(seconds=5),
                                       created=known_by + timedelta(seconds=6)))
    result = agree(pool, world, loader, cutoff, known_by=known_by)
    assert late_id not in ids_of(result) and loader.last_report.bar_rows_read == 0
    cutoff1, known_by1 = window_clocks(1)
    result = agree(pool, world, loader, cutoff1, known_by=known_by1)
    assert late_id in ids_of(result)
    assert loader.last_report.mode == "delta" and loader.last_report.bar_rows_read == 1


@pytest.mark.parametrize("clock", ["observed_at", "created_at", "replay_available_at", "provider_published_at",
                                    "bar_end"])
def test_each_clock_crosses_on_its_own_bound_under_two_clocks(pool, clock):
    """Recording clocks cross known_by, bar_end crosses the cutoff: each makes the bar newly visible alone."""
    world = seeded_world(pool)
    loader = ScreeningInputLoader()
    cutoff0, known_by0 = at(30), at(46)
    cutoff1, known_by1 = at(40), at(56)
    agree(pool, world, loader, cutoff0, known_by=known_by0)
    quiet = at(33)  # after cutoff0 but before known_by0: recorded, so only bar_end can hold a bar back
    start = at(28) if clock == "bar_end" else at(22)  # ends at(33) (after cutoff0) or at(27) (before it)
    fields = dict(observed=quiet, created=quiet)
    if clock == "observed_at":
        fields = dict(observed=known_by0 + timedelta(minutes=2), created=quiet)
    elif clock == "created_at":
        fields["created"] = known_by0 + timedelta(minutes=2)
    elif clock == "replay_available_at":
        fields["replay"] = known_by0 + timedelta(minutes=2)
    elif clock == "provider_published_at":
        fields["published"] = known_by0 + timedelta(minutes=2)
    (row_id,) = add(pool, world, Bar("AAPL", "5m", start, **fields))
    before = agree(pool, world, loader, cutoff0, known_by=known_by0)
    assert row_id not in ids_of(before) and loader.last_report.bar_rows_read == 0
    after = agree(pool, world, loader, cutoff1, known_by=known_by1)
    assert row_id in ids_of(after)
    assert loader.last_report.mode == "delta" and loader.last_report.bar_rows_read == 1


def test_the_source_closure_is_read_under_known_by_on_the_real_sql(pool):
    world = seeded_world(pool)
    loader = ScreeningInputLoader()
    cutoff, known_by = window_clocks(0)
    stamp = cutoff + timedelta(minutes=15, seconds=20)
    agree(pool, world, loader, at(20), known_by=at(20))
    (source_id,) = add(pool, world, Bar("AAPL", "1m", cutoff - 15 * MINUTE, scope="EXTENDED", observed=stamp,
                                         created=stamp))
    (derived_id,) = add(pool, world, Bar("AAPL", "15m", cutoff - 15 * MINUTE, kind="DERIVED", sources=(source_id,),
                                          observed=stamp, created=stamp))
    result = agree(pool, world, loader, cutoff, known_by=known_by)
    assert loader.last_report.mode == "delta" and loader.last_report.source_rows_read == 1
    assert {source_id, derived_id} <= set(ids_of(result))


def test_known_by_must_be_aware_and_not_before_the_cutoff_on_the_real_loaders(pool):
    world = seeded_world(pool)
    loader = ScreeningInputLoader()
    for bad, message in ((OPEN.replace(tzinfo=None) + timedelta(minutes=40), "known_by must be timezone-aware"),
                         (at(10), "known_by must not precede the cutoff")):
        with pool.transaction() as cursor:
            with pytest.raises(ValueError, match=message):
                loader.load(cursor, **scope(world, at(20), known_by=bad))
            with pytest.raises(ValueError, match=message):
                load_realtime_screening_input(cursor, **scope(world, at(20), known_by=bad))
