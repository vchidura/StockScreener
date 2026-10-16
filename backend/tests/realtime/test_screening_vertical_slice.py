from contextlib import contextmanager
from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal
import json
from pathlib import Path
import subprocess
import sys
from uuid import UUID, NAMESPACE_URL, uuid5
from unittest.mock import MagicMock

import exchange_calendars
from fastapi.testclient import TestClient
import pytest

from realtime.contracts import EQUITY_VALIDATION_UNIVERSE, canonical_sha256
from realtime.portal_api import RealtimePortalRepositories, create_realtime_portal_app
from realtime.portal_compatibility_repository import RealtimeScreeningRepository
from realtime.portal_projection_repository import persist_screening_generation_cursor
from realtime.portal_projection import build_screening_generation
from realtime.preflight import ProviderTier
from realtime.equity_bar_adapter import adapt_developer_equity_bar
from realtime.screening_projection import RealtimeScreeningInput, produce_realtime_screening
from realtime.screening_repository import load_realtime_screening_input
from research.screening import FIELDS, PATTERNS, VERSION, GapDetailRequest, Query, SnapshotRowRequest
from equity.derivation import derive_canonical_bars
from tests.realtime.test_equity_validation_history import COHORT_ID, OBSERVED_AT, _bar, _facts, _security


def screening_fixture(session=date(2026, 10, 2), *, current=True):
    calendar = exchange_calendars.get_calendar("XNYS")
    facts, first, _ = _facts()
    modified = []
    for fact in facts:
        b = fact.revision
        if b.ticker == "SPY" and b.session_date == date(2026, 10, 2):
            if b.interval in {"1h", "1d"} and not b.adjusted:
                continue
            sha = canonical_sha256(("synthetic-gap", b.payload_sha256))
            fact = replace(fact, revision=replace(
                b, bar_revision_id=uuid5(NAMESPACE_URL, sha), payload_sha256=sha,
                open_price=Decimal("104"), high_price=Decimal("107"),
                low_price=Decimal("103"), close_price=Decimal("106"),
            ))
        modified.append(fact)
    source = [f.revision for f in modified if f.revision.ticker == "SPY" and f.revision.interval == "30m"
              and f.revision.session_date == date(2026, 10, 2)]
    for interval in ("1d", "1h"):
        modified.extend(adapt_developer_equity_bar(b, cohort_id=COHORT_ID,
                                                  instrument_id=next(f.instrument_id for f in facts if f.revision.ticker == "SPY"))
                        for b in derive_canonical_bars(
                            _security("SPY"), source, target_interval=interval, observed_at=OBSERVED_AT,
                            ingestion_segment_id=uuid5(NAMESPACE_URL, "synthetic-derived-gap"),
                        ))
    earlier = calendar.previous_session(str(first))
    extra = tuple(_bar(ticker, index, "1d", calendar.session_open(earlier).to_pydatetime(),
                       calendar.session_close(earlier).to_pydatetime(), adjusted=True, index=-1)
                  for index, ticker in enumerate(EQUITY_VALIDATION_UNIVERSE.equity_tickers, 1))
    start = calendar.session_offset(str(session), -282).date()
    rows, members, actions, coverage = [], [], [], []
    for index, ticker in enumerate(EQUITY_VALIDATION_UNIVERSE.equity_tickers, 1):
        security = _security(ticker)
        members.append(dict(
            security_id=security.security_id, ticker=ticker, instrument_id=index,
            security_revision_id=security.security_revision_id, company_name=f"{ticker} synthetic company",
            security_type="ETF" if ticker in {"SPY", "QQQ", "IWM"} else "CS",
            observed_at=calendar.session_close(earlier).to_pydatetime(),
            payload_sha256=security.payload_sha256,
        ))
        coverage.append(dict(
            coverage_id=uuid5(NAMESPACE_URL, f"split-coverage:{ticker}"), security_id=security.security_id,
            action_type="SPLIT", window_start=earlier.date(), window_end=date(2026, 10, 2),
            received_at=calendar.session_close(earlier).to_pydatetime(),
            response_action_count=1 if ticker == "NFLX" else 0,
            response_sha256=canonical_sha256(("split-coverage", ticker)),
            payload_sha256=canonical_sha256(("split-coverage-payload", ticker)),
        ))
        if ticker == "NFLX":
            actions.append(dict(
                action_id=uuid5(NAMESPACE_URL, "synthetic-nflx-split"), security_id=security.security_id,
                ticker=ticker, action_type="SPLIT", effective_date=first + timedelta(days=30),
                split_from="1", split_to="10", received_at=calendar.session_close(earlier).to_pydatetime(),
            ))
    for fact in (*extra, *modified):
        b = fact.revision
        if not start <= b.session_date <= session:
            continue
        row = dict(
            bar_revision_id=b.bar_revision_id, security_id=b.security_id, instrument_id=fact.instrument_id,
            ticker=b.ticker, cohort_id=fact.cohort_id, provider_tier=fact.provider_tier.value,
            interval=b.interval, session_date=b.session_date, session_scope=b.session_scope.value,
            adjustment_basis="ADJUSTED" if b.adjusted else "RAW_UNADJUSTED",
            bar_start=b.bar_start, bar_end=b.bar_end, open=float(b.open_price), high=float(b.high_price),
            low=float(b.low_price), close=float(b.close_price), volume=float(b.volume),
            source_kind=b.source_kind.value, source_revision_ids=b.source_bar_revision_ids,
            observed_at=b.bar_end + timedelta(minutes=1), replay_available_at=b.bar_end + timedelta(minutes=1),
            is_final=b.is_final, payload_sha256=b.payload_sha256,
        )
        rows.append(row)
    return RealtimeScreeningInput(
        cohort_id=COHORT_ID, provider_tier=ProviderTier.MASSIVE_DEVELOPER, source_manifest_sha256="a" * 64,
        source_generation=2 if current else 1, session=session,
        cutoff=OBSERVED_AT if current else OBSERVED_AT - timedelta(days=1),
        members=tuple(members), bars=tuple(rows), actions=tuple(actions), coverage=tuple(coverage),
    )


def test_intraday_asof_projection_uses_prior_daily_session_and_completed_intraday_bars():
    inputs = screening_fixture()
    calendar = exchange_calendars.get_calendar("XNYS")
    cutoff = calendar.session_open(str(inputs.session)).to_pydatetime() + timedelta(hours=2)
    asof = replace(
        inputs,
        cutoff=cutoff,
        bars=tuple(row for row in inputs.bars if row["bar_end"] <= cutoff),
        known_by=cutoff + timedelta(minutes=15),
    )

    generation = produce_realtime_screening(asof)

    assert generation.generated_at == cutoff
    assert generation.manifest["projection_mode"] == "INTRADAY_ASOF_V1"
    assert generation.manifest["session_status"] == "PARTIAL"
    assert generation.manifest["daily_as_of_session"] == str(
        calendar.previous_session(str(inputs.session)).date()
    )
    assert generation.status == "COMPLETE"
    assert len(generation.rows) == len(inputs.members)


class ScreeningStore:
    """SQL-boundary fixture: real projection, writer and readers, no PostgreSQL/provider."""
    def __init__(self):
        self.generations = {}
        self.rows = {}
        self.current = None
        self.statements = []
        self.result = []

    @contextmanager
    def cursor(self):
        yield self

    def execute(self, sql, params=()):
        self.statements.append(sql)
        self.result = []
        if sql.startswith("SET"):
            return
        if sql.startswith("INSERT INTO rt.screening_generations"):
            keys = ("generation_id", "cohort_id", "provider_tier", "session_date", "source_manifest_sha256",
                    "expected_members", "actual_members", "status", "generated_at", "manifest")
            record = dict(zip(keys, params))
            record["manifest"] = json.loads(record["manifest"])
            self.generations.setdefault(record["generation_id"], record)
        elif sql.startswith("INSERT INTO rt.screening_current"):
            incoming = self.generations[params[1]]
            previous = self.generations.get(self.current)
            compatible = params[4]
            assert compatible == params[6] and params[5] == params[7] == VERSION
            if previous:
                if previous["manifest"].get("version") == VERSION and not compatible:
                    return
                ordered = incoming["session_date"] > previous["session_date"] or (
                    incoming["session_date"] == previous["session_date"] and incoming["generated_at"] > previous["generated_at"])
                replacing_legacy = compatible and previous["manifest"].get("version") != VERSION and incoming["generated_at"] > previous["generated_at"]
                if not ordered and not replacing_legacy:
                    return
            self.current = params[1]
        elif "FROM rt.screening_current AS current" in sql or "FROM rt.portal_cohort_current" in sql:
            if self.current:
                r = self.generations[self.current]
                if not params or str(params[0]) == r["cohort_id"]:
                    self.result = [dict(cohort_id=r["cohort_id"], provider_tier=r["provider_tier"])]
        elif "FROM rt.screening_generations WHERE generation_id=" in sql:
            self.result = [self.generations[params[0]]]
        elif "FROM rt.screening_generations WHERE cohort_id=" in sql:
            records = [r for r in self.generations.values() if r["cohort_id"] == params[0] and r["generated_at"] <= params[1]
                       and r["manifest"].get("version") == params[2]]
            if len(params) > 3:
                records = [r for r in records if (params[3] is None or r["manifest"]["generation"] == params[3])
                           and (params[5] is None or str(r["session_date"]) == str(params[5]))]
            self.result = sorted(records, key=lambda r: (r["session_date"], r["generated_at"]), reverse=True)
        elif "FROM rt.screening_rows" in sql:
            self.result = sorted(self.rows.get(params[0], []), key=lambda r: (r["ticker"], r["security_id"]))
        else:
            raise AssertionError(f"Unexpected SQL: {sql}")

    def values(self, cursor, sql, rows, *, page_size):
        assert cursor is self and sql.startswith("INSERT INTO rt.screening_rows") and page_size == 100
        for generation, security, ticker, eligible, sha, payload in rows:
            records = self.rows.setdefault(generation, [])
            if not any(r["security_id"] == security for r in records):
                records.append(dict(security_id=security, ticker=ticker, eligible=eligible,
                                    payload_sha256=sha, payload=json.loads(payload)))

    def fetchone(self):
        return self.result[0] if self.result else None

    def fetchall(self):
        return self.result

    def publish(self, generation):
        persist_screening_generation_cursor(self, generation, values_executor=self.values)


def current_bars_of(inputs):
    """What `rt.bar_current` holds for an input: the bar loader of an offline fixture, same contract as
    `load_current_bar_heads_cursor` (rows per (security id, group key), ordered by bar start)."""
    def load(cursor, *, cohort_id, provider_tier, targets, with_sources=False):
        result = {}
        for _, security_id, ticker, key, first, last in targets:
            interval, _, basis = key.partition(":")
            result[(str(security_id), key)] = [
                dict(row) for row in sorted(
                    (b for b in inputs.bars if b["ticker"] == ticker and b["interval"] == interval
                     and b["adjustment_basis"] == basis and first <= b["bar_start"] and b["bar_end"] <= last),
                    key=lambda b: b["bar_start"])
            ]
        return result
    return load


def screening_app(bar_loader=None):
    store = ScreeningStore()
    previous = produce_realtime_screening(screening_fixture(date(2026, 10, 1), current=False))
    inputs = screening_fixture()
    current = produce_realtime_screening(inputs)
    store.publish(previous)
    store.publish(current)
    reader = RealtimeScreeningRepository(store.cursor, bar_loader=bar_loader or current_bars_of(inputs))
    app = create_realtime_portal_app(
        RealtimePortalRepositories(None, None, None, reader), clock=lambda: OBSERVED_AT,
    )
    return app, store, previous, current


def test_nine_ticker_real_projection_persists_complete_frontend_contracts_and_details():
    app, store, previous, current = screening_app()
    store.publish(current)
    assert len(store.generations) == 2
    with TestClient(app) as client:
        params = {"cohort_id": str(COHORT_ID)}
        catalog = client.get("/api/stocks/screening/catalog", params=params)
        assert catalog.status_code == 200
        data = catalog.json()
        assert set(data["fields"]) == set(FIELDS)
        assert set(data["patterns"]) == set(PATTERNS)
        assert data["gaps"]["fields"]["direction"]["options"] == ["UP", "DOWN"]
        assert data["hourly"]["version"] == "screening_hourly_context_v1"
        response = client.post("/api/stocks/screening/query", params=params, json=Query().model_dump())
        assert response.status_code == 200, response.text
        result = response.json()
        assert result["universe_count"] == result["matched_count"] == 9
        assert result["generation"]["session"] == "2026-10-02"
        assert result["generation"]["generation"] == current.manifest["generation"]
        assert result["comparison"]["status"] == "READY"
        assert set(result["generation"]["field_coverage"]) == set(FIELDS)
        assert all(set(r["values"]) == set(FIELDS) for r in result["rows"])
        assert all(set(r["patterns"]) == set(PATTERNS) for r in result["rows"])
        assert all(source["bar_lineage"]["groups"]["1d:ADJUSTED"]["count"] == 283 for source in current.manifest["lineage"].values())
        assert all(source["bar_lineage"]["groups"]["1d:RAW_UNADJUSTED"]["count"] >= 20 for source in current.manifest["lineage"].values())
        assert all(source["bar_lineage"]["groups"]["1h:RAW_UNADJUSTED"]["count"] == 23
                   for source in current.manifest["hourly_lineage"].values())
        assert not any("source_bars" in source or "bars" in source for source in current.manifest["lineage"].values())
        spy = next(r for r in result["rows"] if r["ticker"] == "SPY")
        request = dict(generation=current.manifest["generation"], security_id=spy["security_id"])
        gap = client.post("/api/stocks/screening/gap-details", params=params, json=request)
        hourly = client.post("/api/stocks/screening/hourly-details", params=params, json=request)
        assert gap.status_code == hourly.status_code == 200
        assert gap.json()["episode_count"] == spy["gap_summary"]["episode_count"] > 0
        assert hourly.json()["values"] == spy["hourly"]["values"]
        # The per-bar hourly lineage is recomposed from the bars at the stored head (same shape as when it was embedded).
        lineage = hourly.json()["lineage"]
        head = current.manifest["hourly_lineage"][spy["security_id"]]["bar_lineage"]["groups"]["1h:RAW_UNADJUSTED"]
        assert len(lineage["bars"]) == head["count"] == 23
        assert (lineage["bars"][0], lineage["bars"][-1]) == (head["first_revision_id"], head["last_revision_id"])
        assert lineage["selected_bar_id"] == lineage["bars"][-1] and lineage["reconstructed_from_30m"] == {}
        assert list(lineage["derived_source_revision_ids"]) == lineage["bars"]
        assert gap.json()["matching_episodes"][0]["episode_id"] == spy["gap_summary"]["representative"]["episode_id"]
        request = Query(predicate={"filters": [{"field": "price", "min": 102.}]}, new_only=True)
        filtered = client.post("/api/stocks/screening/query", params=params, json=request.model_dump()).json()
        assert filtered["matched_count"] == filtered["result_count"] == 1
        assert filtered["rows"][0]["ticker"] == "SPY" and filtered["rows"][0]["new_status"] == "NEW"
        first_page = client.post("/api/stocks/screening/query", params=params, json=Query(limit=2, sort="price", descending=True).model_dump()).json()
        next_page = client.post("/api/stocks/screening/query", params=params, json=Query(limit=2, offset=2, sort="price", descending=True).model_dump()).json()
        assert first_page["rows"][0]["ticker"] == "SPY"
        assert not {r["security_id"] for r in first_page["rows"]} & {r["security_id"] for r in next_page["rows"]}
        assert first_page["matched_count"] == next_page["matched_count"] == 9
        for row in current.rows:
            assert row.payload["values"]["price"] is not None
            assert row.payload["missing"].get("discovery_state") == "LOW_LIQUIDITY"
    assert all("equity_bar_" not in sql and "equity_portal_" not in sql for sql in store.statements)
    assert any("legacy.manifest->>'version' IS DISTINCT FROM" in sql for sql in store.statements)


@pytest.mark.parametrize("drift", ["history", "basis", "cohort", "identity", "coverage", "actions", "split", "quote", "hourly_lineage", "reference"])
def test_producer_fails_closed_on_missing_history_basis_action_and_identity(drift):
    inputs = screening_fixture()
    bars = [dict(b) for b in inputs.bars]
    member = inputs.members[0]
    target = next(b for b in bars if b["ticker"] == member["ticker"] and b["interval"] == "1d" and b["adjustment_basis"] == "ADJUSTED")
    if drift == "history":
        bars.remove(target)
    elif drift == "basis":
        target["adjustment_basis"] = "RAW_UNADJUSTED"
    elif drift == "cohort":
        target["cohort_id"] = UUID(int=0)
    elif drift == "identity":
        target["security_id"] = UUID(int=0)
    elif drift == "quote":
        target = next(b for b in bars if b["ticker"] == member["ticker"] and b["interval"] == "1d"
                      and b["adjustment_basis"] == "RAW_UNADJUSTED" and b["session_date"] == inputs.session)
        target.update(close=90., low=89.)
    elif drift == "hourly_lineage":
        target = next(b for b in bars if b["ticker"] == member["ticker"] and b["interval"] == "1h")
        target["source_revision_ids"] = ()
    elif drift == "coverage":
        inputs = replace(inputs, coverage=())
    elif drift == "actions":
        inputs = replace(inputs, actions=())
    elif drift == "reference":
        inputs = replace(inputs, members=(dict(member, security_type=None), *inputs.members[1:]))
    elif drift == "split":
        split = inputs.actions[0]
        target = next(b for b in bars if b["ticker"] == "NFLX" and b["interval"] == "1d"
                      and b["adjustment_basis"] == "ADJUSTED" and b["session_date"] >= split["effective_date"])
        inputs = replace(inputs, actions=(dict(split, effective_date=target["session_date"]),))
        target.update(close=10., open=10., high=11., low=9.)
    with pytest.raises(ValueError):
        produce_realtime_screening(replace(inputs, bars=tuple(bars)))


def test_api_fails_closed_on_hash_corruption_wrong_cohort_and_invalid_requests():
    app, store, previous, current = screening_app()
    with TestClient(app) as client:
        route = "/api/stocks/screening/query"
        assert client.post(route, params={"cohort_id": str(UUID(int=0))}, json=Query().model_dump()).status_code == 404
        assert client.post(route, params={"cohort_id": str(COHORT_ID)}, json={"predicate": {"filters": [{"field": "sql", "min": 0.}]}}).status_code == 422
        store.rows[str(current.generation_id)][0]["payload"]["values"]["price"] = 1000.
        assert client.post(route, params={"cohort_id": str(COHORT_ID)}, json=Query().model_dump()).status_code == 503


def test_real_repository_loader_pins_manifest_references_cutoff_and_causal_sources():
    fixture = screening_fixture()
    calendar = exchange_calendars.get_calendar("XNYS")
    nominal_start = calendar.session_offset(str(fixture.session), -19).date()
    from equity.screening_hourly import hourly_slots
    first_slot = hourly_slots(calendar, calendar.session_close(str(fixture.session)).to_pydatetime())[0][0]
    selected = [b for b in fixture.bars if (
        b["interval"] == "1d" and (b["adjustment_basis"] == "ADJUSTED" or b["session_date"] >= nominal_start)
        or b["interval"] == "1h" and b["bar_start"] >= first_slot
    )]
    selected_ids = {b["bar_revision_id"] for b in selected}
    source_ids = {source for b in selected for source in b["source_revision_ids"]} - selected_ids
    sources = [b for b in fixture.bars if b["bar_revision_id"] in source_ids]
    cursor = MagicMock()
    cursor.fetchone.return_value = dict(
        manifest_id=UUID(int=9), manifest_sha256=fixture.source_manifest_sha256,
        stock_count=9, provider_tier=fixture.provider_tier.value,
    )
    cursor.fetchall.side_effect = (fixture.members, selected, sources, fixture.actions, fixture.coverage)
    loaded = load_realtime_screening_input(
        cursor, cohort_id=fixture.cohort_id, session=fixture.session, cutoff=fixture.cutoff,
        source_generation=fixture.source_generation,
    )
    generation = produce_realtime_screening(loaded)
    assert len(generation.rows) == 9
    assert all(r.payload["eligible"] for r in generation.rows)
    queries = [call.args[0] for call in cursor.execute.call_args_list]
    assert "rt.subscription_manifests" in queries[0] and "rt.cohorts" in queries[0]
    assert "ref.security_revisions" in queries[1]
    # V2 members have no feed column; stocks are the EQUITY instruments declaring the AM channel.
    assert "instrument.asset_class='EQUITY' AND member.channel='AM'" in queries[1]
    assert "member.feed" not in queries[1]
    assert "FROM rt.bar_revisions" in queries[2] and "FROM rt.bar_catalog" not in queries[2]
    assert "created_at<=%s" in queries[2] and "replay_available_at<=%s" in queries[2]
    assert "bar_revision_id=ANY" in queries[3] and "cohort_id=%s" in queries[3]
    assert not any("equity_bar_revisions" in sql for sql in queries)


def test_category_pattern_gap_hourly_and_unknown_query_semantics_reconcile_details():
    app, store, previous, current = screening_app()
    with TestClient(app) as client:
        params = {"cohort_id": str(COHORT_ID)}

        def query(**kwargs):
            response = client.post("/api/stocks/screening/query", params=params, json=Query(**kwargs).model_dump())
            assert response.status_code == 200, response.text
            return response.json()

        assert query(predicate={"filters": [{"field": "instrument_type", "values": ["ETF"]}]})["matched_count"] == 3
        assert query(predicate={"patterns": [{"id": "doji", "direction": 0}], "pattern_mode": "NONE"})["matched_count"] == 9
        unknown = query(predicate={"filters": [{"field": "discovery_trend", "values": ["UP"]}]})
        assert unknown["matched_count"] == 0 and unknown["unknown_count"] == 9
        hourly = query(predicate={"hourly": {"filters": [{"field": "close", "min": 102.}]}})
        assert [r["ticker"] for r in hourly["rows"]] == ["SPY"]
        assert hourly["comparison"]["status"] == "HOURLY_COMPARISON_NOT_ENABLED"
        assert query(predicate={"hourly": {"filters": [{"field": "close", "min": 102.}]}}, new_only=True)["result_count"] == 0
        gaps = query(predicate={"gap": {"filters": [{"field": "direction", "values": ["UP"]}]}})
        assert gaps["matched_count"] == 1
        row = gaps["rows"][0]
        detail = client.post("/api/stocks/screening/gap-details", params=params, json=dict(
            generation=current.manifest["generation"], security_id=row["security_id"],
            gap={"filters": [{"field": "direction", "values": ["UP"]}]},
        )).json()
        assert len(detail["matching_episodes"]) == row["gap_summary"]["matching_count"]
        assert detail["matching_episodes"][0]["episode_id"] == row["gap_summary"]["representative"]["episode_id"]


def test_standalone_api_import_cannot_import_canonical_database_or_main():
    root = Path(__file__).resolve().parents[2]
    script = (
        "import sys, importlib.abc\n"
        "class Guard(importlib.abc.MetaPathFinder):\n"
        " def find_spec(self, fullname, path=None, target=None):\n"
        "  if fullname in ('database', 'main'): raise AssertionError(fullname)\n"
        "sys.meta_path.insert(0, Guard())\n"
        "import realtime.portal_api\n"
        "import realtime.screening_repository\n"
        "assert 'database' not in sys.modules and 'main' not in sys.modules\n"
    )
    completed = subprocess.run([sys.executable, "-B", "-c", script], cwd=root, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stderr


def test_projection_cli_plan_has_zero_io_and_requires_explicit_target_confirmation(monkeypatch, capsys):
    from scripts import project_realtime_screening as cli
    args = ["--cohort-id", str(COHORT_ID), "--session", "2026-10-02",
            "--cutoff", OBSERVED_AT.isoformat(), "--source-generation", "1"]
    def forbidden(*args, **kwargs):
        raise AssertionError("Plan must not load environment, open a pool or produce")
    monkeypatch.setattr(cli, "load_dotenv", forbidden)
    monkeypatch.setattr(cli.pool, "SimpleConnectionPool", forbidden)
    assert cli.main(args) == 0
    assert "NOT_EXECUTED" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        cli.parse_args([*args, "--execute"])


def test_real_daily_state_projection_uses_nominal_liquidity_without_raw_253_extension():
    inputs = screening_fixture()
    ticker = "AAPL"
    identities = {b["bar_revision_id"]: uuid5(NAMESPACE_URL, f"liquid-fixture:{b['bar_revision_id']}")
                  for b in inputs.bars if b["ticker"] == ticker}
    bars = []
    for original in inputs.bars:
        bar = dict(original)
        if bar["ticker"] == ticker:
            bar["volume"] *= 1000
            bar["payload_sha256"] = canonical_sha256(("liquid-fixture", bar["payload_sha256"]))
            bar["bar_revision_id"] = identities[bar["bar_revision_id"]]
            bar["source_revision_ids"] = tuple(identities[value] for value in bar["source_revision_ids"])
        bars.append(bar)
    generation = produce_realtime_screening(replace(inputs, bars=tuple(bars)))
    row = next(r.payload for r in generation.rows if r.ticker == ticker)
    assert row["values"]["discovery_state"] == row["values"]["discovery_trend"] == "MIXED"
    assert "discovery_state" not in row["missing"] and "discovery_trend" not in row["missing"]
    groups = generation.manifest["lineage"][row["security_id"]]["bar_lineage"]["groups"]
    assert groups["1d:ADJUSTED"]["count"] == 283 and groups["1d:RAW_UNADJUSTED"]["count"] >= 20


def test_full_generation_replaces_wall_clock_validation_stub_and_cannot_regress_to_it():
    full = produce_realtime_screening(screening_fixture())
    store = ScreeningStore()
    def legacy(session, generated, source_generation):
        return build_screening_generation(
            cohort_id=COHORT_ID, provider_tier=ProviderTier.MASSIVE_DEVELOPER,
            session_date=session, expected_members=9, generated_at=generated,
            manifest={"manifest_kind": "EQUITY_VALIDATION", "source_generation": source_generation},
            rows=tuple((r.security_id, r.ticker, False, {"ticker": r.ticker}) for r in full.rows),
        )
    store.publish(legacy(date(2026, 10, 4), OBSERVED_AT - timedelta(minutes=1), 1))
    store.publish(full)
    assert store.current == str(full.generation_id)
    store.publish(legacy(date(2026, 10, 5), OBSERVED_AT + timedelta(minutes=1), 3))
    assert store.current == str(full.generation_id)
    older_full = produce_realtime_screening(screening_fixture(date(2026, 10, 1), current=False))
    store.publish(older_full)
    assert store.current == str(full.generation_id)
