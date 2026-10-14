"""Profile of the delta screening loader against the full load on a disposable V2 database (no provider).

Builds a realistic retained bar set (about 13,500 bars per ticker) on the real tables of
test_realtime_v2_delta_bench, then times the full load (`load_realtime_screening_input`) against the delta
loader: cold, then warm with 0, 5 and 100 new bars per ticker. Also reports the database-side plan of the delta
statement, bytes per retained bar, and (with --indexes) the effect of the indexes the delta statement could use.
Run from the repository root, without the offline I/O guard:
    backend\\.venv\\Scripts\\python.exe -B backend/tests/realtime/profile_screening_delta_loader.py
"""
from __future__ import annotations

import argparse
import gc
import statistics
import sys
import tracemalloc
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import perf_counter

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from psycopg2.extras import RealDictCursor  # noqa: E402

from realtime.screening_delta_loader import ScreeningInputLoader  # noqa: E402
from realtime.screening_repository import BarClocks, load_realtime_screening_input, select_new_bars  # noqa: E402
from tests.realtime.screening_delta_fixtures import (  # noqa: E402
    LENGTH,
    SESSION,
    create_world,
    ensure_history_partitions,
    history_bars,
    insert_bars,
)
from tests.realtime.v2_database import disposable_v2_database  # noqa: E402

DATABASE = "test_realtime_v2_delta_bench"
UTC = timezone.utc
COLD_CUTOFF = datetime(2026, 10, 2, 15, 0, tzinfo=UTC)


class Pool:
    def __init__(self, pool):
        self._pool = pool

    def __repr__(self) -> str:
        return "<disposable pool>"

    @contextmanager
    def transaction(self):
        connection = self._pool.getconn()
        try:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                yield cursor
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            self._pool.putconn(connection)

    @contextmanager
    def writing(self):
        connection = self._pool.getconn()
        try:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                yield cursor
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            self._pool.putconn(connection)


def scope(world, cutoff):
    return dict(cohort_id=world.cohort_id, session=SESSION, cutoff=cutoff, source_generation=1,
                include_portal_history=True)


def order(bar):
    """Insertion order that keeps every source before the bars derived from it."""
    return bar.end, LENGTH.get(bar.interval, 100_000)


def timed(pool, call):
    with pool.transaction() as cursor:
        started = perf_counter()
        result = call(cursor)
        return perf_counter() - started, result


class Recorder:
    """Captures the delta statement so it can be EXPLAINed."""

    def __init__(self):
        self.sql = self.params = None

    def execute(self, sql, params=()):
        self.sql, self.params = sql, params

    def fetchall(self):
        return [{"visible_total": 0, "bar_revision_id": None, "src_rank": None}]


class Timing:
    """Cursor proxy that adds up database execution and row-fetch time per statement kind."""

    KINDS = (("SELECT manifest.", "manifest"), ("FROM rt.subscription_members", "members"),
             ("SELECT DISTINCT ON", "winners"), ("SELECT counted.", "delta"), ("bar_revision_id=ANY", "sources"),
             ("corporate_action_revisions", "actions"), ("corporate_action_coverage", "coverage"))

    def __init__(self, cursor):
        self._cursor, self._kind = cursor, None
        self.seconds: dict[str, list] = {}

    def execute(self, sql, params=()):
        self._kind = next(kind for prefix, kind in self.KINDS if prefix in sql)
        started = perf_counter()
        self._cursor.execute(sql, params)
        self.seconds.setdefault(self._kind, [0.0, 0.0, 0])[0] += perf_counter() - started

    def fetchall(self):
        started = perf_counter()
        rows = self._cursor.fetchall()
        entry = self.seconds.setdefault(self._kind, [0.0, 0.0, 0])
        entry[1] += perf_counter() - started
        entry[2] += len(rows)
        return rows

    def fetchone(self):
        started = perf_counter()
        row = self._cursor.fetchone()
        self.seconds.setdefault(self._kind, [0.0, 0.0, 0])[1] += perf_counter() - started
        return row

    def report(self) -> str:
        return ", ".join(f"{kind} {entry[0] * 1000:.0f}+{entry[1] * 1000:.0f} ms ({entry[2]} rows)"
                         for kind, entry in self.seconds.items())


def breakdown(pool, label, call):
    with pool.transaction() as cursor:
        timing = Timing(cursor)
        started = perf_counter()
        call(timing)
        total = perf_counter() - started
    print(f"  {label}: {total * 1000:.0f} ms = {timing.report()}")


def explain(pool, world, c0, c1, label):
    from realtime.screening_repository import BarScope, screening_history_window

    recorder = Recorder()
    _, history = screening_history_window(SESSION, include_portal_history=True)
    bar_scope = BarScope(world.cohort_id, "MASSIVE_DEVELOPER", world.tickers, True, history)
    select_new_bars(recorder, bar_scope, BarClocks(c0, c0), BarClocks(c1, c1))
    sql, size = recorder.sql, 3 + 5 + len(history)
    parts = {
        "count only": (sql[sql.index("(SELECT count(*)") + 1:sql.index(") AS counted")], recorder.params[:size]),
        "new rows only": (sql[sql.index("LEFT JOIN (") + len("LEFT JOIN ("):sql.index(") AS new_bars")],
                          recorder.params[size:]),
        "both (as issued)": (sql, recorder.params),
    }
    summary = []
    for name, (statement, params) in parts.items():
        with pool.transaction() as cursor:
            cursor.execute("EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF) " + statement, params)
            lines = [row["QUERY PLAN"] for row in cursor.fetchall()]
        nodes = {}
        for line in lines:
            text = line.strip().lstrip("->").strip()
            for kind in ("Seq Scan", "Bitmap Heap Scan", "Index Scan", "Index Only Scan"):
                if text.startswith(kind) or text.startswith("Parallel " + kind):
                    nodes[kind] = nodes.get(kind, 0) + 1
        times = [line.split(":")[1].strip() for line in lines if "Execution Time" in line]
        summary.append(f"{name} {times[0] if times else '?'} {nodes}")
    print(f"  EXPLAIN ANALYZE [{label}]: " + "; ".join(summary))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tickers", type=int, default=3)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--indexes", action="store_true", help="also measure with candidate indexes")
    args = parser.parse_args()
    names = ("AAPL", "MSFT", "NVDA", "AMZN", "TSLA", "SPY", "QQQ", "IWM", "NFLX")[:args.tickers]
    with disposable_v2_database(DATABASE) as raw_pool:
        pool = Pool(raw_pool)
        with pool.writing() as cursor:
            ensure_history_partitions(cursor)
            world = create_world(cursor, names)
        started = perf_counter()
        every = history_bars(world)
        history = [b for b in every if not (b.session == SESSION and b.end > COLD_CUTOFF)]
        pending = sorted((b for b in every if b.session == SESSION and b.end > COLD_CUTOFF), key=order)
        with pool.writing() as cursor:
            insert_bars(cursor, world, history, page_size=5000)
            cursor.execute("ANALYZE rt.bar_revisions")
            cursor.execute("ANALYZE rt.bar_catalog")
        per_ticker = len(history) // len(names)
        print(f"fixture: {len(history)} bars ({per_ticker} per ticker), {len(pending)} pending bars; "
              f"built in {perf_counter() - started:.1f}s")

        loader = ScreeningInputLoader()
        results = []

        def record(name, full_seconds, full, delta_seconds, delta):
            assert full == delta, f"{name}: the delta result differs from the full load"
            read = loader.last_report.bar_rows_read + loader.last_report.source_rows_read
            results.append((name, full_seconds, len(full.bars), delta_seconds, read, loader.last_report.mode))

        for _ in range(2):  # the full load, twice, to see the repeat spread
            full_seconds, full = timed(pool, lambda c: load_realtime_screening_input(c, **scope(world, COLD_CUTOFF)))
        loader.reset()
        delta_seconds, delta = timed(pool, lambda c: loader.load(c, **scope(world, COLD_CUTOFF)))
        record("cold", full_seconds, full, delta_seconds, delta)

        cutoff = COLD_CUTOFF
        for _ in range(5):
            cutoff += timedelta(seconds=1)
            full_seconds, full = timed(pool, lambda c: load_realtime_screening_input(c, **scope(world, cutoff)))
            delta_seconds, delta = timed(pool, lambda c: loader.load(c, **scope(world, cutoff)))
            record("warm 0 new", full_seconds, full, delta_seconds, delta)

        by_ticker = {name: [b for b in pending if b.ticker == name and b.interval != "1m"] for name in names}
        by_ticker_1m = {name: [b for b in pending if b.ticker == name and b.interval == "1m"] for name in names}
        cursors = {name: 0 for name in names}

        def take(count):
            chosen = []
            for name in names:
                rows = sorted(by_ticker[name] + by_ticker_1m[name], key=order)
                stop = cursors[name] + count
                # Never split bars that end together: a later insert with older clocks would be a late row.
                while stop < len(rows) and rows[stop].end == rows[stop - 1].end:
                    stop += 1
                chosen += rows[cursors[name]:stop]
                cursors[name] = stop
            return chosen

        for label, count in (("warm 5 new/ticker", 5), ("warm 100 new/ticker", 100)):
            for _ in range(args.trials):
                chosen = take(count)
                with pool.writing() as cursor:
                    insert_bars(cursor, world, chosen)
                cutoff = max(b.end for b in chosen) + timedelta(minutes=1)
                full_seconds, full = timed(pool, lambda c: load_realtime_screening_input(c, **scope(world, cutoff)))
                delta_seconds, delta = timed(pool, lambda c: loader.load(c, **scope(world, cutoff)))
                record(label, full_seconds, full, delta_seconds, delta)

        print(f"{'case':22} {'full s':>8} {'full rows':>10} {'delta s':>8} {'delta rows':>11} {'speed-up':>9}  mode")
        by_case = {}
        for row in results:
            by_case.setdefault(row[0], []).append(row)
        for name, rows in by_case.items():
            full_s = statistics.median(r[1] for r in rows)
            delta_s = statistics.median(r[3] for r in rows)
            print(f"{name:22} {full_s:8.3f} {rows[-1][2]:10d} {delta_s:8.3f} {rows[-1][4]:11d} "
                  f"{full_s / delta_s:8.1f}x  {rows[-1][5]} (n={len(rows)})")

        print("where the time goes (execute + fetch per statement, REPEATABLE READ, same cutoff):")
        breakdown(pool, "full load", lambda c: load_realtime_screening_input(c, **scope(world, cutoff)))
        breakdown(pool, "delta, nothing new", lambda c: loader.load(c, **scope(world, cutoff)))
        print("database-side cost of the delta statement (EXPLAIN ANALYZE, whole cohort):")
        explain(pool, world, cutoff, cutoff + timedelta(minutes=15), "no change")
        explain(pool, world, COLD_CUTOFF, cutoff, "5..100 new per ticker")

        if args.indexes:
            with pool.writing() as cursor:
                for column, partial in (("observed_at", ""), ("created_at", ""), ("bar_end", ""),
                                        ("replay_available_at", " WHERE replay_available_at IS NOT NULL"),
                                        ("provider_published_at", " WHERE provider_published_at IS NOT NULL")):
                    cursor.execute(f"CREATE INDEX bench_bar_{column} ON rt.bar_revisions ({column}){partial}")
                cursor.execute("ANALYZE rt.bar_revisions")
            print("with the five single-column indexes (observed_at, created_at, bar_end, replay_available_at and "
                  "provider_published_at partial):")
            explain(pool, world, cutoff, cutoff + timedelta(minutes=15), "no change")
            explain(pool, world, COLD_CUTOFF, cutoff, "5..100 new per ticker")
            chosen = take(5)
            with pool.writing() as cursor:
                insert_bars(cursor, world, chosen)
            cutoff = max(b.end for b in chosen) + timedelta(minutes=1)
            full_seconds, full = timed(pool, lambda c: load_realtime_screening_input(c, **scope(world, cutoff)))
            delta_seconds, delta = timed(pool, lambda c: loader.load(c, **scope(world, cutoff)))
            record("indexed, 5 new", full_seconds, full, delta_seconds, delta)
            print(f"  indexed, 5 new/ticker: full {full_seconds:.3f}s, delta {delta_seconds:.3f}s")

        # Memory: bytes per retained bar, rows alone and rows plus the loader's index structures.
        loader.reset()
        gc.collect()
        tracemalloc.start()
        before = tracemalloc.get_traced_memory()[0]
        with pool.transaction() as cursor:
            full = load_realtime_screening_input(cursor, **scope(world, cutoff))
        gc.collect()
        rows_bytes = tracemalloc.get_traced_memory()[0] - before
        bars = len(full.bars)
        del full
        gc.collect()
        with pool.transaction() as cursor:
            kept = loader.load(cursor, **scope(world, cutoff))
        del kept
        gc.collect()
        retained = tracemalloc.get_traced_memory()[0] - before
        tracemalloc.stop()
        print(f"memory: {bars} bars; row dicts {rows_bytes / bars:.0f} B per bar "
              f"({rows_bytes / 1e6:.1f} MB); loader retention (rows kept alive plus index) {retained / bars:.0f} B "
              f"per bar ({retained / 1e6:.1f} MB), index overhead {(retained - rows_bytes) / bars:.0f} B per bar")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
