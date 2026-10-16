from contextlib import contextmanager
from copy import deepcopy
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from functools import lru_cache
import json
import math
from uuid import UUID, NAMESPACE_URL, uuid5

from fastapi.testclient import TestClient
import pytest

from realtime.contracts import EQUITY_VALIDATION_UNIVERSE, canonical_json, canonical_sha256
from realtime.equity_portal_history import (
    build_realtime_equity_portal_history_plan, build_realtime_equity_portal_history_package,
)
from realtime.equity_validation_history import SplitAction, SplitCoverage
from realtime.portal_api import RealtimePortalRepositories, create_realtime_portal_app
from realtime.portal_compatibility_repository import BoundedCache
from realtime.portal_projection import build_portal_snapshot
from realtime.portal_projection_repository import RETENTION_HOT_AND_KEPT, persist_portal_snapshot_cursor
from realtime.preflight import ProviderTier
from realtime.screening_projection import RealtimeScreeningInput
from realtime.screening_repository import load_realtime_screening_input
from realtime.ticker_projection import (
    TICKER_WORKSPACE_VERSION, _chart_point, _fact, _intraday_expected_count,
    produce_realtime_ticker_workspace,
    project_realtime_ticker_workspace,
    produce_realtime_ticker_workspace_summary,
)
from realtime.ticker_repository import (
    RealtimeTickerWorkspaceRepository,
    materialize_realtime_ticker_workspace,
)
import realtime.ticker_repository as ticker_repository
from realtime.database_access import RealtimeDatabaseRole
from realtime.worker_database import RealtimeProcessDatabase
from tests.realtime.test_equity_validation_history import COHORT_ID, OBSERVED_AT, _bar, _security
from tests.realtime.portal_v2_support import PortalV2Statements

EVIDENCE_RESOLUTION_PREFIXES = (
    "SELECT cohort_id, evidence_id, evidence_type, market_time, payload_sha256",
    "SELECT evidence.cohort_id, evidence.evidence_id, evidence.evidence_type",
)
from unittest.mock import MagicMock
import exchange_calendars
from pathlib import Path


@lru_cache(maxsize=1)
def ticker_fixture():
    plan = build_realtime_equity_portal_history_plan(date(2026, 10, 2))
    calendar = exchange_calendars.get_calendar("XNYS")
    facts, members, actions, coverage = [], [], [], []
    for instrument, ticker in enumerate(EQUITY_VALIDATION_UNIVERSE.equity_tickers, 1):
        security = _security(ticker)
        members.append(dict(
            security_id=security.security_id, ticker=ticker, instrument_id=instrument,
            security_revision_id=security.security_revision_id, company_name=f"{ticker} synthetic company",
            security_type="ETF" if ticker in {"SPY", "QQQ", "IWM"} else "CS",
            exchange=security.primary_exchange, effective_from=security.effective_from,
            observed_at=security.observed_at, payload_sha256=security.payload_sha256,
            active=True, cik=f"{instrument:010d}", composite_figi=f"BBG{instrument:09d}",
            share_class_figi=f"BBG{instrument + 100:09d}", sic_code="3571",
            sic_description="Electronic Computers", sector="Information Technology",
            industry="Electronic Computers", list_date=date(1980, 12, 12), delisted_date=None,
            weighted_shares=1000000, free_float=None, free_float_percent=None,
            market_cap=1000000000,
        ))
        split_date = plan.adjusted_sessions[20]
        if ticker == "NFLX":
            actions.append(dict(
                action_id=uuid5(NAMESPACE_URL, "ticker-nflx-split"), security_id=security.security_id,
                ticker=ticker, action_type="SPLIT", effective_date=split_date, split_from="1", split_to="10",
                received_at=OBSERVED_AT, payload_sha256=canonical_sha256(("split", ticker)),
            ))
        coverage.append(dict(
            coverage_id=uuid5(NAMESPACE_URL, f"ticker-coverage:{ticker}"), security_id=security.security_id,
            action_type="SPLIT", window_start=plan.adjusted_sessions[0], window_end=plan.session_date,
            received_at=OBSERVED_AT, response_action_count=1 if ticker == "NFLX" else 0,
            response_sha256=canonical_sha256(("coverage-response", ticker)),
            payload_sha256=canonical_sha256(("coverage", ticker)),
        ))
        native = []
        for index, session in enumerate(plan.adjusted_sessions):
            start = calendar.session_open(str(session)).to_pydatetime()
            end = calendar.session_close(str(session)).to_pydatetime()
            close = Decimal(str(round(100 + instrument * 5 + index * .02 + math.sin(index / 7) * 5, 4)))

            def fact(interval, begin, finish, slot, adjusted):
                original = _bar(ticker, instrument, interval, begin, finish, adjusted=adjusted, index=slot)
                revision = replace(
                    original.revision, open_price=close - 1, high_price=close + 2,
                    low_price=close - 2, close_price=close, vwap=close,
                    system_observed_at=finish + timedelta(minutes=1), replay_available_at=OBSERVED_AT,
                )
                return replace(original, revision=revision)

            native.append(fact("1d", start, end, index, True))
            if session in plan.raw_sessions:
                cursor, slot = start, 0
                while cursor < end:
                    finish = min(cursor + timedelta(minutes=5), end)
                    native.append(fact("5m", cursor, finish, index * 100 + slot, False))
                    cursor, slot = finish, slot + 1
            if session in plan.one_minute_sessions:
                cursor, slot = start, 0
                while cursor < end:
                    finish = min(cursor + timedelta(minutes=1), end)
                    native.append(fact("1m", cursor, finish, index * 1000 + slot, False))
                    cursor, slot = finish, slot + 1
        for index, session in enumerate(plan.monthly_sessions):
            month_start = date(session.year, session.month, 1)
            sessions = calendar.sessions_in_range(str(month_start), str(session))
            start = calendar.session_open(sessions[0]).to_pydatetime()
            end = calendar.session_close(sessions[-1]).to_pydatetime()
            monthly = fact("1mo", start, end, index, True)
            native.append(replace(monthly, revision=replace(monthly.revision, session_date=session)))
        for index, session in enumerate(plan.weekly_sessions):
            iso = session.isocalendar()
            week_start = date.fromisocalendar(iso.year, iso.week, 1)
            sessions = calendar.sessions_in_range(str(week_start), str(session))
            start = calendar.session_open(sessions[0]).to_pydatetime()
            end = calendar.session_close(sessions[-1]).to_pydatetime()
            weekly = fact("1wk", start, end, index, True)
            native.append(replace(weekly, revision=replace(weekly.revision, session_date=session)))
        package = build_realtime_equity_portal_history_package(
            security, native, plan=plan, observed_at=OBSERVED_AT,
            ingestion_segment_id=uuid5(NAMESPACE_URL, f"ticker-derived:{ticker}"),
            split_coverage=SplitCoverage(ticker, plan.adjusted_sessions[0], plan.session_date, OBSERVED_AT),
            split_actions=(SplitAction(ticker, split_date, "1", "10"),) if ticker == "NFLX" else (),
        )
        facts.extend(package.facts)
    bars = []
    for fact in facts:
        b = fact.revision
        bars.append(dict(
            bar_revision_id=b.bar_revision_id, security_id=b.security_id, instrument_id=fact.instrument_id,
            ticker=b.ticker, cohort_id=fact.cohort_id, provider_tier=fact.provider_tier.value,
            interval=b.interval, session_date=b.session_date, session_scope=b.session_scope.value,
            adjustment_basis="ADJUSTED" if b.adjusted else "RAW_UNADJUSTED",
            bar_start=b.bar_start, bar_end=b.bar_end, open=float(b.open_price), high=float(b.high_price),
            low=float(b.low_price), close=float(b.close_price), volume=float(b.volume), vwap=float(b.vwap) if b.vwap else None,
            transaction_count=b.transaction_count, source_kind=b.source_kind.value,
            source_revision_ids=b.source_bar_revision_ids, observed_at=b.system_observed_at,
            replay_available_at=b.replay_available_at, provider_published_at=b.provider_published_at,
            availability_mode=b.availability_mode.value, ingestion_segment_id=b.ingestion_segment_id,
            is_final=b.is_final, payload_sha256=b.payload_sha256,
        ))
    return RealtimeScreeningInput(
        cohort_id=COHORT_ID, provider_tier=ProviderTier.MASSIVE_DEVELOPER, source_manifest_sha256="a" * 64,
        source_generation=3, session=plan.session_date, cutoff=OBSERVED_AT, members=tuple(members),
        bars=tuple(bars), actions=tuple(actions), coverage=tuple(coverage),
    )


def test_recovery_calendar_enumerates_every_completed_decision_interval():
    from realtime.ticker_projection import completed_ticker_decision_windows

    windows = completed_ticker_decision_windows(date(2026, 10, 9))
    assert len(windows) == 22
    assert {interval: sum(candidate == interval for candidate, _ in windows)
            for interval in ("30m", "1h", "1d", "1wk", "1mo")} == {
                "30m": 13, "1h": 7, "1d": 1, "1wk": 1, "1mo": 0,
            }
    assert all(end <= datetime(2026, 10, 9, 20, tzinfo=timezone.utc) for _, end in windows)
    early = completed_ticker_decision_windows(date(2026, 11, 27))
    assert sum(interval == "30m" for interval, _ in early) == 7
    assert sum(interval == "1h" for interval, _ in early) == 4


def test_partial_session_intraday_counts_replace_the_oldest_retained_session():
    calendar = exchange_calendars.get_calendar("XNYS")
    session = date(2026, 10, 15)
    cutoff = datetime(2026, 10, 15, 19, 15, tzinfo=timezone.utc)
    plan = build_realtime_equity_portal_history_plan(session)

    assert (plan.raw_sessions[0], plan.raw_sessions[-1], len(plan.raw_sessions)) == (
        date(2026, 6, 9), session, 90,
    )
    assert (plan.one_minute_sessions[0], plan.one_minute_sessions[-1],
            len(plan.one_minute_sessions)) == (date(2026, 10, 9), session, 5)
    assert {
        interval: _intraday_expected_count(calendar, plan, session, cutoff, interval)
        for interval in ("1m", "5m", "15m", "30m", "1h")
    } == {
        "1m": 1905,
        "5m": 7011,
        "15m": 2337,
        "30m": 1168,
        "1h": 628,
    }


def test_recovery_history_separates_market_cutoff_and_late_known_by(monkeypatch):
    import realtime.ticker_projection as producer

    inputs = ticker_fixture()
    captured = []

    def capture(**scope):
        captured.append(scope)
        return scope

    monkeypatch.setattr(producer, "materialize_realtime_equity_evidence", capture)
    results = producer.project_realtime_ticker_evidence_history(inputs)
    assert len(results) == 9 * 22
    for scope in captured:
        market_cutoff = scope["bars"][-1].revision.bar_end
        assert all(fact.revision.bar_end <= market_cutoff
                   for fact in (*scope["bars"], *scope["confirmation_bars"]))
        assert scope["observed_at"] > market_cutoff
        assert scope["observed_at"] <= inputs.cutoff
    first = next(scope for scope in captured
                 if scope["security"].ticker == "AAPL" and scope["bars"][-1].revision.interval == "30m")
    assert first["bars"][-1].revision.bar_end.hour == 14
    assert all(fact.revision.bar_end <= first["bars"][-1].revision.bar_end for fact in first["bars"])


def test_recovery_history_rejects_facts_not_yet_known():
    from realtime.ticker_projection import project_realtime_ticker_evidence_history

    inputs = ticker_fixture()
    future = {**inputs.bars[0], "created_at": inputs.cutoff + timedelta(seconds=1)}
    with pytest.raises(ValueError, match="known-by time"):
        project_realtime_ticker_evidence_history(replace(inputs, bars=(future, *inputs.bars[1:])))


def test_weekly_lineage_outside_daily_chart_window_does_not_expand_daily_chart():
    inputs = ticker_fixture()
    plan = build_realtime_equity_portal_history_plan(inputs.session)
    calendar = exchange_calendars.get_calendar("XNYS")
    previous = calendar.previous_session(str(plan.adjusted_sessions[0])).date()
    original = next(row for row in inputs.bars if row["ticker"] == "AAPL"
                    and row["interval"] == "1d" and row["adjustment_basis"] == "ADJUSTED")
    lineage = {**original, "bar_revision_id": uuid5(NAMESPACE_URL, "older-weekly-daily-lineage"),
               "session_date": previous, "bar_start": calendar.session_open(str(previous)).to_pydatetime(),
               "bar_end": calendar.session_close(str(previous)).to_pydatetime()}
    projection = project_realtime_ticker_workspace(replace(inputs, bars=(*inputs.bars, lineage)))
    aapl = next(snapshot for snapshot in projection.snapshots if snapshot.snapshot_key == "AAPL")
    assert len(projection.summary_charts["AAPL"]["1d"]) == 283
    assert aapl.source_manifest["bar_lineage"]["groups"]["1d:ADJUSTED"]["count"] == 283
    assert set(aapl.payload["multi"]["setups"]) == {"30m", "1h", "1d", "1wk", "1mo"}


@lru_cache(maxsize=1)
def ticker_projection():
    return project_realtime_ticker_workspace(ticker_fixture())


@lru_cache(maxsize=1)
def ticker_snapshots():
    return ticker_projection().snapshots


def old_embedded_charts(ticker, chart_refs):
    """The `charts` a workspace payload embedded before it was served by reference, rebuilt from the
    fixture bars with the producer's own fact arithmetic (independent of the reader's row path)."""
    inputs = ticker_fixture()
    return {
        interval: [_chart_point(_fact(row, inputs))
                   for row in stored_bars()[(ticker, interval, key.partition(":")[2])]]
        for interval, key in chart_refs.items()
    }


def _old_evidence_row(row):
    """The row expression the producer used to embed each evidence row, verbatim."""
    return {
        "evidence_id": str(row.evidence_id), "payload_sha256": row.payload_sha256,
        "payload": json.loads(row.payload_json), "source_revision_ids": [str(value) for value in row.source_revision_ids],
        "market_time": row.market_time.isoformat(), "observed_at": row.observed_at.isoformat(),
        "quality_codes": row.quality_codes, "evidence_type": row.evidence_type.value,
        "analysis_run_id": str(row.analysis_run_id), "security_revision_id": str(row.security_revision_id),
        "latest_bar_revision_id": str(row.latest_bar_revision_id), "source_name": row.source_name,
        "source_version": row.source_version, "payload_schema_version": row.payload_schema_version,
        "valid_until": row.valid_until.isoformat() if row.valid_until else None,
        "quality_state": row.quality_state.value, "evidence_role": row.evidence_role.value,
        "fundamental_report_ids": [str(value) for value in row.fundamental_report_ids],
    }


def old_embedded_evidence(snapshot, projection=None):
    """The `evidence` mapping (identity -> rows in materialization order) the payload embedded before it was
    served by reference, rebuilt from the materialized rows. Identities that were release-only (5m and 15m
    pattern views, never persisted) have no durable body and are not part of the index."""
    projection = projection or ticker_projection()
    results = {
        tuple(str(row.evidence_id) for row in result.materialization.evidence): result
        for result in (*projection.materializations, *projection.pattern_materializations)
    }
    return {
        identity: [_old_evidence_row(row) for row in
                   results[tuple(ref["evidence_id"] for ref in refs)].materialization.evidence]
        for identity, refs in snapshot.payload["evidence_index"].items()
    }


def as_stored_json(value):
    return json.loads(canonical_json(value))


def test_ticker_projection_exposes_stable_supported_materialization_closures():
    projection = ticker_projection()
    assert len(projection.materializations) == 9 * 5
    assert len(projection.pattern_materializations) == 9 * 5
    assert {result.interval for result in projection.pattern_materializations} == {"30m", "1h", "1d", "1wk", "1mo"}
    assert all(len(result.bar_revision_ids) <= 300 for result in projection.pattern_materializations)
    assert {
        result.interval for result in projection.materializations
    } == {"30m", "1h", "1d", "1wk", "1mo"}

    embedded = {
        item["evidence_id"]: item
        for snapshot in projection.snapshots
        for rows in snapshot.payload["evidence_index"].values()
        for item in rows
    }
    persisted = {
        str(row.evidence_id): row
        for result in (*projection.materializations, *projection.pattern_materializations)
        for row in result.materialization.evidence
    }
    assert embedded.keys() == persisted.keys()
    for evidence_id, row in persisted.items():
        assert embedded[evidence_id] == dict(
            evidence_id=evidence_id, evidence_type=row.evidence_type.value, payload_sha256=row.payload_sha256,
        )

    repeated = project_realtime_ticker_workspace(replace(
        ticker_fixture(), source_generation=4,
        cutoff=ticker_fixture().cutoff + timedelta(minutes=1),
    ))
    original_ids = {
        str(row.evidence_id)
        for result in projection.materializations
        for row in result.materialization.evidence
    }
    repeated_ids = {
        str(row.evidence_id)
        for result in repeated.materializations
        for row in result.materialization.evidence
    }
    assert repeated_ids == original_ids
    assert {result.input_sha256 for result in repeated.pattern_materializations} == {
        result.input_sha256 for result in projection.pattern_materializations
    }


def test_ticker_evidence_storage_profile_is_bounded(record_property):
    rows = [
        row
        for result in ticker_projection().materializations
        for row in result.materialization.evidence
    ]
    by_interval = {}
    for result in ticker_projection().materializations:
        profile = by_interval.setdefault(
            result.interval, {
                "rows": 0,
                "payload_bytes": 0,
                "legacy_source_revision_references": 0,
                "source_window_revision_ids": 0,
                "dependency_edges": 0,
            }
        )
        profile["source_window_revision_ids"] += (
            len(result.bar_revision_ids)
            + len(result.confirmation_bar_revision_ids)
        )
        for row in result.materialization.evidence:
            profile["rows"] += 1
            profile["payload_bytes"] += len(row.payload_json.encode("utf-8"))
            profile["legacy_source_revision_references"] += len(row.source_revision_ids)
            profile["dependency_edges"] += sum(
                value in {
                    evidence.evidence_id
                    for evidence in result.materialization.evidence
                }
                for value in row.source_revision_ids
            )
    profile = {
        "materializations": len(ticker_projection().materializations),
        "rows": len(rows),
        "payload_bytes": sum(len(row.payload_json.encode("utf-8")) for row in rows),
        "legacy_source_revision_references": sum(
            len(row.source_revision_ids) for row in rows
        ),
        "source_window_revision_ids": sum(
            len(result.bar_revision_ids) + len(result.confirmation_bar_revision_ids)
            for result in ticker_projection().materializations
        ),
        "dependency_edges": sum(
            interval["dependency_edges"] for interval in by_interval.values()
        ),
        "feature_rows": sum(row.evidence_type.value == "FEATURE_SNAPSHOT" for row in rows),
        "setup_rows": sum(row.evidence_type.value == "TRADE_SETUP" for row in rows),
        "by_interval": by_interval,
    }
    record_property("technical_evidence_profile", json.dumps(profile, sort_keys=True))

    assert profile["materializations"] == 9 * 5
    assert profile["feature_rows"] == 9 * 5
    assert profile["setup_rows"] == 9 * 5
    assert set(by_interval) <= {"30m", "1h", "1d", "1wk", "1mo"}
    assert max(len(row.payload_json.encode("utf-8")) for row in rows) < 1024 * 1024


@lru_cache(maxsize=1)
def stored_bars():
    """(ticker, interval, basis) -> the final RTH bars `rt.bar_current` holds for them, ordered by bar start."""
    groups = {}
    for row in ticker_fixture().bars:
        if row["session_scope"] == "RTH" and row["is_final"]:
            groups.setdefault((row["ticker"], row["interval"], row["adjustment_basis"]), []).append(row)
    return {key: sorted(rows, key=lambda row: row["bar_start"]) for key, rows in groups.items()}


def _numeric(value):
    return Decimal(f"{value:.8f}")


@lru_cache(maxsize=1)
def stored_evidence():
    """evidence id -> the row the V2 evidence tables hold for it. `source_revision_ids` is empty in the
    table by design: the lineage is the window role (and the bars of its window) or the dependency rows."""
    rows = {}
    projection = ticker_projection()
    for result in (*projection.materializations, *projection.pattern_materializations):
        primary, confirmation = tuple(result.bar_revision_ids), tuple(result.confirmation_bar_revision_ids)
        for row in result.materialization.evidence:
            sources = tuple(row.source_revision_ids)
            role = "PRIMARY" if sources and sources == primary else (
                "CONFIRMATION" if sources and sources == confirmation else None)
            rows[row.evidence_id] = dict(
                evidence_id=row.evidence_id, evidence_type=row.evidence_type.value,
                evidence_role=row.evidence_role.value, market_time=row.market_time,
                observed_at=row.observed_at, valid_until=row.valid_until, source_name=row.source_name,
                source_version=row.source_version, payload_schema_version=row.payload_schema_version,
                analysis_run_id=row.analysis_run_id, latest_bar_revision_id=row.latest_bar_revision_id,
                security_revision_id=row.security_revision_id,
                fundamental_report_ids=list(row.fundamental_report_ids),
                quality_state=row.quality_state.value, quality_codes=list(row.quality_codes),
                payload_json=row.payload_json, payload_sha256=row.payload_sha256, source_window_role=role,
                primary_bar_revision_ids=list(primary), confirmation_bar_revision_ids=list(confirmation),
                security_id=row.security_id, dependencies=list(sources) if role is None else [],
            )
    return rows


class TickerStore(PortalV2Statements):
    def __init__(self):
        self.statements = []
        self.init_portal()
        self.result = []
        self.reference_drift = self.action_drift = False
        self.reference_expired = False
        self.screening_available = True
        self.missing_evidence = self.corrupt_evidence = False
        self.revised_bars = set()
        self.retired_bars = set()
        self.vanished_evidence = set()
        self.tampered_evidence = set()

    @contextmanager
    def cursor(self):
        yield self

    def _bar_rows(self, params):
        ticker, ranges = params[4], params[5:]
        rows = []
        for interval, basis, first, last in zip(*[iter(ranges)] * 4):
            group = [row for row in stored_bars()[(ticker, interval, basis)]
                     if first <= row["bar_start"] and row["bar_end"] <= last]
            if (ticker, interval) in self.retired_bars:
                group = group[:-1]
            for position, row in enumerate(group):
                revision, payload_sha256 = row["bar_revision_id"], row["payload_sha256"]
                if (ticker, interval) in self.revised_bars and position == len(group) - 1:
                    revision, payload_sha256 = uuid5(NAMESPACE_URL, f"revised:{revision}"), "e" * 64
                rows.append(dict(
                    interval=interval, adjustment_basis=basis, bar_revision_id=revision,
                    bar_start=row["bar_start"], bar_end=row["bar_end"], open_price=_numeric(row["open"]),
                    high_price=_numeric(row["high"]), low_price=_numeric(row["low"]),
                    close_price=_numeric(row["close"]), volume=_numeric(row["volume"]),
                    payload_sha256=payload_sha256,
                ))
        return rows

    def _evidence_rows(self, security_id, evidence_ids, known_by):
        return [row for evidence_id in evidence_ids
                if (row := stored_evidence().get(UUID(str(evidence_id)))) is not None
                and row["evidence_id"] not in self.vanished_evidence
                and str(row["security_id"]) == str(security_id) and row["observed_at"] <= known_by]

    def execute(self, sql, params=()):
        self.statements.append(sql)
        self.result = []
        if not self.screening_available and "rt.screening_current" in sql:
            return
        if sql.startswith("SET"):
            return
        if self.portal_execute(sql, params):
            return
        if sql.startswith("SELECT snapshot.*"):
            key = (params[0], params[1], "PLATFORM") if "snapshot_key='PLATFORM'" in sql else params[:3]
            cutoff = params[2] if key[-1] == "PLATFORM" else params[3]
            row = self.snapshots.get(params[-1])
            if row and row["generated_at"] <= cutoff:
                if (row["cohort_id"], row["snapshot_type"], row["snapshot_key"]) == key:
                    self.result = [dict(row, cohort_manifest_sha256="a" * 64)]
        elif sql.startswith("SELECT snapshot.snapshot_id, snapshot.payload_sha256"):
            row = self.snapshots.get(params[-1])
            if row and row["generated_at"] <= params[3]:
                if (row["cohort_id"], row["snapshot_type"], row["snapshot_key"]) != params[:3]:
                    return
                manifest = row["source_manifest"]
                self.result = [dict(
                    snapshot_id=row["snapshot_id"], payload_sha256=row["payload_sha256"],
                    source_manifest_sha256=row["source_manifest_sha256"], generated_at=row["generated_at"],
                    provider_tier=row["provider_tier"], source_generation=row["source_generation"],
                    cohort_manifest_sha256="a" * 64, reference=manifest["reference"],
                    manifest_version=manifest["version"], manifest_cohort_id=manifest["cohort_id"],
                    manifest_cohort_sha256=manifest["source_manifest_sha256"],
                    manifest_provider_tier=manifest["provider_tier"],
                    manifest_source_generation=manifest["source_generation"],
                )]
        elif sql.startswith("SELECT snapshot.snapshot_key, snapshot.generated_at"):
            for snapshot_id in params[2]:
                row = self.snapshots.get(snapshot_id)
                if row and (row["cohort_id"], row["snapshot_type"]) == (str(params[0]), params[1]) and row["generated_at"] <= params[3]:
                    self.result.append(dict(
                        snapshot_key=row["snapshot_key"],
                        generated_at=row["generated_at"],
                        chart_market_times=row["payload"]["chart_market_times"],
                    ))
            self.result.sort(key=lambda item: item["snapshot_key"])
        elif sql.startswith("SELECT snapshot.snapshot_key AS ticker, snapshot.payload->'multi'"):
            for snapshot_id in params[2]:
                row = self.snapshots.get(str(snapshot_id))
                if row and row["cohort_id"] == params[0]:
                    payload = row["payload"]
                    self.result.append(dict(
                        ticker=row["snapshot_key"], multi=payload["multi"],
                        evidence=payload.get("evidence_index", payload.get("evidence")),
                        reference=row["source_manifest"]["reference"]))
        elif sql.startswith("SELECT snapshot.snapshot_key AS ticker"):
            for snapshot_id in params[2]:
                row = self.snapshots.get(str(snapshot_id))
                if row and (row["cohort_id"], row["snapshot_type"]) == params[:2] and row["generated_at"] <= params[3]:
                    self.result.append(dict(ticker=row["snapshot_key"], snapshot_id=row["snapshot_id"],
                                            payload_sha256=row["payload_sha256"]))
            self.result.sort(key=lambda item: item["ticker"])
        elif sql.startswith(EVIDENCE_RESOLUTION_PREFIXES):
            if self.missing_evidence:
                return
            for row in self._evidence_rows(params[1], params[2], params[3]):
                self.result.append(dict(
                    cohort_id=params[0], evidence_id=str(row["evidence_id"]), evidence_type=row["evidence_type"],
                    market_time=row["market_time"],
                    payload_sha256="0" * 64 if self.corrupt_evidence else row["payload_sha256"]))
        elif sql.startswith("SELECT evidence.evidence_id AS evidence_id"):
            if self.missing_evidence:
                return
            for row in self._evidence_rows(params[2], params[3], params[4]):
                body = {key: value for key, value in row.items() if key not in {"payload_json", "dependencies", "security_id"}}
                payload = json.loads(row["payload_json"])
                if row["evidence_id"] in self.tampered_evidence:
                    payload["tampered"] = True
                self.result.append(dict(
                    body, payload=payload,
                    payload_sha256="0" * 64 if self.corrupt_evidence else row["payload_sha256"]))
        elif sql.startswith("SELECT dependency.evidence_id AS evidence_id"):
            for evidence_id in params[1]:
                for order, dependency in enumerate(stored_evidence()[UUID(evidence_id)]["dependencies"]):
                    self.result.append(dict(evidence_id=UUID(evidence_id), dependency_order=order,
                                            dependency_evidence_id=dependency))
        elif sql.startswith("SELECT cur.interval AS interval"):
            self.result = self._bar_rows(params)
        elif sql.startswith("SELECT security_id, ticker, revision_id"):
            if self.reference_expired and params[1] > OBSERVED_AT:
                return
            for reference in sorted(ticker_fixture().members, key=lambda row: row["ticker"]):
                if str(reference["security_id"]) in params[0]:
                    self.result.append(dict(
                        security_id=reference["security_id"], ticker=reference["ticker"],
                        revision_id="wrong" if self.reference_drift else reference["security_revision_id"],
                        payload_sha256=reference["payload_sha256"],
                    ))
        elif sql.startswith("SELECT revision_id"):
            if self.reference_expired and params[2] > OBSERVED_AT:
                return
            reference = next(row for row in ticker_fixture().members if str(row["security_id"]) == params[0])
            self.result = [dict(revision_id="wrong" if self.reference_drift else reference["security_revision_id"],
                                payload_sha256=reference["payload_sha256"])]
        elif sql.startswith("SELECT EXISTS"):
            self.result = [dict(action_drift=self.action_drift)]
        else:
            raise AssertionError(f"Unexpected SQL: {sql}")

    def fetchone(self):
        return self.result[0] if self.result else None

    def fetchall(self):
        return self.result

    def publish(self, snapshot, **kwargs):
        kept = persist_portal_snapshot_cursor(self, snapshot, **kwargs)
        self.advance(snapshot)
        return kept


def ticker_app(*, clock=lambda: OBSERVED_AT, snapshots=None):
    store = TickerStore()
    snapshots = tuple(snapshots or ticker_snapshots())
    for snapshot in snapshots:
        store.publish(snapshot)
    store.publish(produce_realtime_ticker_workspace_summary(
        ticker_fixture(), snapshots, ticker_projection().summary_charts))
    store.publish(ticker_repository.produce_realtime_pattern_watch_publication(ticker_fixture(), snapshots))
    reader = RealtimeTickerWorkspaceRepository(store.cursor)
    app = create_realtime_portal_app(RealtimePortalRepositories(None, None, None, ticker_workspace=reader), clock=clock)
    return app, store


def test_retained_ticker_browse_does_not_require_current_screening():
    app, store = ticker_app()
    store.screening_available = False
    snapshot = next(row for row in ticker_snapshots() if row.snapshot_key == "TSLA")
    params = {"cohort_id": str(COHORT_ID)}
    with TestClient(app) as client:
        assert client.get("/api/stock/TSLA/workspace-publication", params=params).status_code == 503
        for pin in (None, str(snapshot.snapshot_id)):
            retained = {**params, "read_mode": "retained"}
            if pin is not None:
                retained["snapshot_id"] = pin
            publication = client.get("/api/stock/TSLA/workspace-publication", params=retained)
            assert publication.status_code == 200, publication.text
            assert publication.json()["snapshot_id"] == str(snapshot.snapshot_id)
            assert publication.json()["payload_sha256"] == snapshot.payload_sha256
            assert client.get("/api/stock/TSLA/chart", params=retained).status_code == 200
        assert client.get("/api/stock/TSLA/quote", params=params).status_code == 503


def test_ticker_payload_is_bound_before_current_pointer_rotation(monkeypatch):
    app, store = ticker_app()
    reader = RealtimeTickerWorkspaceRepository(store.cursor)
    original = next(snapshot for snapshot in ticker_snapshots() if snapshot.snapshot_key == "AAPL")
    current_key = (str(COHORT_ID), TICKER_WORKSPACE_VERSION, "AAPL")
    identity = reader._snapshot_identity

    def rotate(*args, **kwargs):
        selected = identity(*args, **kwargs)
        store.current[current_key] = "missing-new-release"
        return selected

    monkeypatch.setattr(reader, "_snapshot_identity", rotate)
    result = reader.read("AAPL", "quote", cohort_id=COHORT_ID, available_by=OBSERVED_AT)
    assert result == original.payload["quote"]
    assert any("snapshot.snapshot_id=%s" in sql for sql in store.statements)


def test_ticker_explicit_pin_does_not_substitute_another_ticker():
    app, store = ticker_app()
    reader = RealtimeTickerWorkspaceRepository(store.cursor)
    snapshot = next(snapshot for snapshot in ticker_snapshots() if snapshot.snapshot_key == "AAPL")
    assert reader.read("AAPL", "quote", cohort_id=COHORT_ID, snapshot_id=snapshot.snapshot_id,
                       available_by=OBSERVED_AT) == snapshot.payload["quote"]
    with pytest.raises(LookupError):
        reader.read("MSFT", "quote", cohort_id=COHORT_ID, snapshot_id=snapshot.snapshot_id, available_by=OBSERVED_AT)
    with pytest.raises(ValueError, match="explicit cohort"):
        reader.read("AAPL", "quote", snapshot_id=snapshot.snapshot_id, available_by=OBSERVED_AT)


def test_pin_older_than_the_hot_window_returns_the_current_view_with_a_notice():
    app, store = ticker_app()
    current = next(snapshot for snapshot in ticker_snapshots() if snapshot.snapshot_key == "AAPL")
    expired = uuid5(NAMESPACE_URL, "workspace-left-the-hot-window")
    with TestClient(app) as client:
        params = dict(cohort_id=str(COHORT_ID))
        pinned = client.get("/api/stock/AAPL/workspace-publication",
                            params=dict(params, snapshot_id=str(expired)))
        assert pinned.status_code == 200, pinned.text
        body = pinned.json()
        assert body["snapshot_id"] == str(current.snapshot_id)
        assert body["notice_code"] == "PINNED_WORKSPACE_EXPIRED"
        assert body["requested_snapshot_id"] == str(expired)
        assert "hot window" in body["notice"]
        unpinned = client.get("/api/stock/AAPL/workspace-publication", params=params).json()
        assert "notice" not in unpinned
        assert body["payload_sha256"] == unpinned["payload_sha256"]
        in_window = client.get("/api/stock/AAPL/workspace-publication",
                               params=dict(params, snapshot_id=str(current.snapshot_id))).json()
        assert "notice" not in in_window and in_window["snapshot_id"] == str(current.snapshot_id)
        # The catalog row and body vanish together when the hot partition is dropped.
        store.expire(current.snapshot_id)
        assert client.get("/api/stock/AAPL/workspace-publication", params=dict(
            params, snapshot_id=str(current.snapshot_id))).status_code == 503


def test_session_close_summary_is_self_contained_after_its_workspaces_expire():
    app, store = ticker_app()
    snapshots = ticker_snapshots()
    summary = produce_realtime_ticker_workspace_summary(
        ticker_fixture(), snapshots, ticker_projection().summary_charts)
    kept = store.publish(summary, retention=RETENTION_HOT_AND_KEPT,
                         kept_snapshot=ticker_repository._session_close_summary(summary, snapshots))
    assert kept.snapshot_id != summary.snapshot_id
    assert store.snapshots[str(kept.snapshot_id)]["snapshot_class"] == "SESSION_CLOSE"
    assert store.current[(str(COHORT_ID), "TICKER_WORKSPACE_SUMMARY_V1", "PLATFORM")] == str(summary.snapshot_id)
    params = dict(cohort_id=str(COHORT_ID), read_mode="retained")
    with TestClient(app) as client:
        hot = client.get("/api/tickers/workspace-publication", params=params).json()
        for workspace in snapshots:
            store.expire(workspace.snapshot_id)
        start = len(store.statements)
        response = client.get("/api/tickers/workspace-publication",
                              params=dict(params, snapshot_id=str(kept.snapshot_id)))
        assert response.status_code == 200, response.text
        frozen = response.json()
        assert frozen["publication"]["snapshot_id"] == str(kept.snapshot_id)
        assert len(frozen["workspaces"]) == 9
        assert frozen["setup_evidence_refs"] == hot["setup_evidence_refs"]
        assert frozen["overview"] == hot["overview"]
        assert not any("evidence" in statement or "TICKER_WORKSPACE" in statement
                       for statement in store.statements[start:])
        assert client.get("/api/tickers/workspace-publication", params=dict(
            params, snapshot_id=str(summary.snapshot_id))).status_code == 503


def test_ticker_snapshot_cache_is_bounded_and_keyed_by_snapshot_identity():
    app, store = ticker_app()
    reader = RealtimeTickerWorkspaceRepository(store.cursor)
    reader._snapshot_cache = BoundedCache(2)
    for ticker in ("AAPL", "MSFT", "SPY"):
        reader.read(ticker, "quote", cohort_id=COHORT_ID, available_by=OBSERVED_AT)
    assert len(reader._snapshot_cache) == 2
    loads = sum(sql.startswith("SELECT snapshot.*") for sql in store.statements)
    reader.read("SPY", "quote", cohort_id=COHORT_ID, available_by=OBSERVED_AT)
    assert sum(sql.startswith("SELECT snapshot.*") for sql in store.statements) == loads


def test_pinned_api_slices_and_summary_survive_pointer_rotation():
    app, store = ticker_app()
    snapshot = next(snapshot for snapshot in ticker_snapshots() if snapshot.snapshot_key == "AAPL")
    summary = produce_realtime_ticker_workspace_summary(
        ticker_fixture(), ticker_snapshots(), ticker_projection().summary_charts)
    store.current[(str(COHORT_ID), TICKER_WORKSPACE_VERSION, "AAPL")] = "rotated"
    store.current[(str(COHORT_ID), "TICKER_WORKSPACE_SUMMARY_V1", "PLATFORM")] = "rotated"
    with TestClient(app) as client:
        params = dict(cohort_id=str(COHORT_ID), snapshot_id=str(snapshot.snapshot_id))
        for route in ("quote", "trade-setup/multi", "workspace-publication"):
            response = client.get(f"/api/stock/AAPL/{route}", params=params)
            assert response.status_code == 200, response.text
        publication = client.get("/api/stock/AAPL/workspace-publication", params=params).json()
        assert publication["snapshot_id"] == str(snapshot.snapshot_id)
        context = client.get("/api/stocks/context", params={**params, "ticker": "AAPL"})
        assert context.status_code == 200, context.text
        assert context.json()["cohort"]["snapshot_id"] == str(snapshot.snapshot_id)
        response = client.get("/api/tickers/workspace-publication", params={**params, "snapshot_id": str(summary.snapshot_id)})
        assert response.status_code == 200, response.text
        assert len(response.json()["workspaces"]) == 9
        assert response.json()["publication"]["snapshot_id"] == str(summary.snapshot_id)
        rejected = client.get("/api/tickers/workspace-publication", params=params)
        assert rejected.status_code != 200


@pytest.mark.parametrize("read_mode", ["current", "retained"])
@pytest.mark.parametrize("defect", ["missing_evidence", "corrupt_evidence"])
def test_release_provenance_fails_closed_on_missing_or_corrupt_persisted_evidence(defect, read_mode):
    app, store = ticker_app()
    setattr(store, defect, True)
    with TestClient(app) as client:
        response = client.get("/api/stock/AAPL/workspace-publication", params=dict(cohort_id=str(COHORT_ID), read_mode=read_mode))
        assert response.status_code == 422
        assert "immutable identity/hash" in response.json()["detail"]


def test_release_provenance_contains_exact_durable_refs_without_current_resolution():
    app, store = ticker_app()
    start = len(store.statements)
    with TestClient(app) as client:
        assert client.get("/api/stock/AAPL/workspace-publication").status_code == 422
        result = client.get("/api/stock/AAPL/workspace-publication", params=dict(cohort_id=str(COHORT_ID)))
        assert result.status_code == 200, result.text
        refs = result.json()["evidence_refs"]
        assert set(refs) == {"30m", "1h", "1d", "1wk", "1mo"}
        assert all(ref["evidence_type"] == "TRADE_SETUP" and len(ref["payload_sha256"]) == 64 for ref in refs.values())
        assert all(ref["cohort_id"] == str(COHORT_ID) for ref in refs.values())
    assert not any("equity_evidence_current" in statement for statement in store.statements[start:])


def test_nine_ticker_real_domain_writer_and_original_api_contract_parity():
    app, store = ticker_app()
    assert len(store.snapshots) == 11
    read_start = len(store.statements)
    with TestClient(app) as client:
        for snapshot in ticker_snapshots():
            ticker, stored = snapshot.snapshot_key, as_stored_json(snapshot.payload)
            assert "charts" not in stored and "evidence" not in stored
            payload = dict(
                stored, charts=as_stored_json(old_embedded_charts(ticker, stored["chart_refs"])),
                evidence=as_stored_json(old_embedded_evidence(snapshot)),
            )
            params = {"cohort_id": str(COHORT_ID)}
            for route, expected in (
                (f"/api/stock/{ticker}/quote", payload["quote"]),
                (f"/api/stock/{ticker}/trade-setup/multi", payload["multi"]),
                (f"/api/equity/security/{ticker}", payload["profile"]),
                (f"/api/chart-patterns/ticker/{ticker}", payload["cross_patterns"]),
            ):
                response = client.get(route, params=params)
                assert response.status_code == 200, response.text
                result = response.json()
                if "setup_read_metrics" in result:
                    for interval, metrics in result["setup_read_metrics"].items():
                        assert metrics["status"] == "READY"
                        expected["setup_read_metrics"][interval] = metrics
                assert result == expected
            assert set(payload["multi"]["setups"]) == {"30m", "1h", "1d", "1wk", "1mo"}
            assert payload["profile"]["security"]["sector"] == "Information Technology"
            assert payload["profile"]["security"]["market_cap"] == 1000000000
            assert "1mo" not in payload["multi"]["errors"]
            assert len(payload["charts"]["1d"]) == 283
            assert payload["charts"]["1d"][-1]["close"] == payload["quote"]["price"]
            for interval in ("1m", "5m", "15m", "30m", "1h", "1d", "1wk", "1mo"):
                response = client.get(f"/api/stock/{ticker}/chart", params=dict(params, interval=interval, period="5y"))
                assert response.status_code == 200, response.text
                result = response.json()
                assert result == payload["charts"][interval][-len(result):]
            for interval in ("5m", "15m", "30m", "1h", "1d", "1wk", "1mo"):
                for suffix, key in (("chart-patterns", "patterns"), ("price-channel", "channels")):
                    response = client.get(f"/api/stock/{ticker}/{suffix}", params=dict(params, interval=interval, refresh=True))
                    assert response.status_code == 200, response.text
                    assert response.json() == payload[key][interval]
            response = client.get("/api/stocks/context", params=dict(params, ticker=ticker))
            assert response.status_code == 200
            payload["context"]["cohort"].update(
                snapshot_id=str(snapshot.snapshot_id), payload_sha256=snapshot.payload_sha256,
            )
            assert response.json() == payload["context"]
            assert payload["evidence"]
            assert any(item["evidence_type"] == "TRADE_SETUP" for rows in payload["evidence"].values() for item in rows)
    read_end = len(store.statements)
    for snapshot in ticker_snapshots():
        store.publish(snapshot)
    assert len(store.snapshots) == 11
    assert all(sql.startswith(("SELECT", "SET")) for sql in store.statements[read_start:read_end])
    assert not any("public." in sql or "canonical" in sql for sql in store.statements)


def test_all_tickers_falls_back_to_strict_ticker_workspace_summary():
    app, _ = ticker_app()
    with TestClient(app) as client:
        response = client.get(
            "/api/tickers/workspace-publication", params={"cohort_id": str(COHORT_ID)},
        )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert len(payload["overview"]) == payload["setup"]["count"] == 9
    assert {row["ticker"] for row in payload["overview"]} == set(EQUITY_VALIDATION_UNIVERSE.equity_tickers)
    assert payload["publication"]["strategy_ticker_available"] is False
    assert payload["publication"]["missing_producers"]["dashboard"] == "DASHBOARD_PUBLICATION_UNAVAILABLE"
    assert payload["setup"]["publication"] == payload["publication"]
    assert all(row["wsma_50"] is not None for row in payload["overview"])
    assert all(row["wsma_200"] is not None for row in payload["overview"])
    assert all(row["dist_200w"] is not None for row in payload["overview"])


def test_ticker_reader_validates_reader_visible_provider_tier_binding():
    source = Path(__file__).parents[2].joinpath(
        "realtime", "ticker_repository.py",
    ).read_text(encoding="utf-8")

    assert "rt.screening_current AS screening_current" in source
    assert "rt.screening_generations AS screening" in source
    assert "snapshot.provider_tier=screening.provider_tier" in source
    assert "rt.capability_profiles" not in source
    assert "cohort.profile_id" not in source


def test_ticker_producer_retains_derived_weekly_fallback_for_ma50_only():
    source = Path(__file__).parents[2].joinpath(
        "realtime", "ticker_projection.py",
    ).read_text(encoding="utf-8")

    assert 'interval == "1wk" and len(stored) == expected_windows' in source
    assert 'missing["weekly_200"] = "NATIVE_ADJUSTED_WEEKLY_HISTORY_UNAVAILABLE"' in source


def test_ticker_get_reader_only_validates_and_projects_stored_payloads():
    source = Path(__file__).parents[2].joinpath(
        "realtime", "ticker_repository.py",
    ).read_text(encoding="utf-8").split("class RealtimeTickerWorkspaceRepository", 1)[1]

    assert "payload = self._snapshot(ticker, cutoff, cohort_id, snapshot_id, read_mode)" in source
    assert "sort_pattern_watch_rows" not in source
    for prohibited in (
        "materialize_realtime_equity_evidence",
        "produce_realtime_ticker_workspace(",
        "produce_realtime_ticker_workspace_summary(",
        "setup_summary_row(",
        "numpy",
        "pandas",
    ):
        assert prohibited not in source


def test_ticker_reader_reuses_validated_snapshot_but_rechecks_reference_drift():
    app, store = ticker_app()
    params = {"cohort_id": str(COHORT_ID)}
    with TestClient(app) as client:
        assert client.get("/api/stock/TSLA/chart", params=params).status_code == 200
        assert client.get("/api/stock/TSLA/quote", params=params).status_code == 200
        assert client.get("/api/equity/security/TSLA", params=params).status_code == 200
        assert sum(sql.startswith("SELECT snapshot.*") for sql in store.statements) == 1
        store.reference_drift = True
        assert client.get("/api/stock/TSLA/quote", params=params).status_code == 503


@pytest.mark.parametrize("age", [timedelta(days=1), timedelta(days=45)])
def test_retained_ticker_browse_survives_reference_and_market_expiry(age):
    app, store = ticker_app(clock=lambda: OBSERVED_AT + age)
    store.reference_expired = True
    snapshot = next(row for row in ticker_snapshots() if row.snapshot_key == "TSLA")
    params = {"cohort_id": str(COHORT_ID), "snapshot_id": str(snapshot.snapshot_id)}
    with TestClient(app) as client:
        assert client.get("/api/stock/TSLA/workspace-publication", params=params).status_code == 503
        retained = {**params, "read_mode": "retained"}
        response = client.get("/api/stock/TSLA/workspace-publication", params=retained)
        assert response.status_code == 200, response.text
        publication = response.json()
        assert publication["snapshot_id"] == str(snapshot.snapshot_id)
        assert publication["payload_sha256"] == snapshot.payload_sha256
        assert publication["market_time"] == snapshot.payload["quote"]["as_of"]
        assert publication["generated_at"] == snapshot.payload["multi"]["as_of"]
        assert publication["read_mode"] == "RETAINED"
        assert publication["execution_permission"] is False
        assert publication["provider_fetch_performed"] is False
        assert publication["current_data_available"] is False
        assert publication["reference_status"] == "UNAVAILABLE"
        for route in ("chart", "quote", "trade-setup/multi"):
            response = client.get(f"/api/stock/TSLA/{route}", params=retained)
            assert response.status_code == 200, response.text


def test_retained_summary_and_pattern_browse_keep_original_members_after_expiry():
    app, store = ticker_app(clock=lambda: OBSERVED_AT + timedelta(days=45))
    store.reference_expired = True
    store.current[(str(COHORT_ID), TICKER_WORKSPACE_VERSION, "AAPL")] = "rotated"
    params = {"cohort_id": str(COHORT_ID), "read_mode": "retained"}
    with TestClient(app) as client:
        for route in ("/api/tickers/workspace-publication", "/api/chart-patterns/scan"):
            response = client.get(route, params=params)
            assert response.status_code == 200, response.text
            publication = response.json()["publication"]
            assert publication["read_mode"] == "RETAINED"
            assert publication["current_data_available"] is False
            assert publication["execution_permission"] is False
            assert len(publication.get("workspaces", response.json().get("workspaces", []))) == 9
        assert client.get("/api/chart-patterns/scan", params={"cohort_id": str(COHORT_ID)}).status_code == 503


def test_retained_reads_still_reject_invalid_original_reference_and_payload():
    for defect in ("reference_drift", "payload"):
        app, store = ticker_app()
        if defect == "reference_drift":
            store.reference_drift = True
        else:
            key = (str(COHORT_ID), TICKER_WORKSPACE_VERSION, "TSLA")
            store.snapshots[store.current[key]]["payload"]["quote"]["price"] = -1
        with TestClient(app) as client:
            response = client.get("/api/stock/TSLA/workspace-publication",
                                  params={"cohort_id": str(COHORT_ID), "read_mode": "retained"})
            assert response.status_code == (503 if defect == "reference_drift" else 422)


def test_ticker_materialization_requires_separate_publication_role():
    compute = object.__new__(RealtimeProcessDatabase)
    compute._process_plan = MagicMock(role=RealtimeDatabaseRole.COMPUTE)
    not_publication = object.__new__(RealtimeProcessDatabase)
    not_publication._process_plan = MagicMock(role=RealtimeDatabaseRole.COMPUTE)

    with pytest.raises(ValueError, match="PUBLICATION"):
        materialize_realtime_ticker_workspace(compute, not_publication)


def test_ticker_materialization_persists_evidence_before_portal(monkeypatch):
    compute = object.__new__(RealtimeProcessDatabase)
    compute._process_plan = MagicMock(role=RealtimeDatabaseRole.COMPUTE)
    publication = object.__new__(RealtimeProcessDatabase)
    publication._process_plan = MagicMock(role=RealtimeDatabaseRole.PUBLICATION)
    projection = ticker_projection()
    summary = MagicMock(snapshot_id=uuid5(NAMESPACE_URL, "ticker-summary"))
    events = []

    @contextmanager
    def transaction(database, **options):
        owner = "compute" if database is compute else "publication"
        events.append(("transaction", owner, options))
        yield f"{owner}-cursor"

    monkeypatch.setattr(RealtimeProcessDatabase, "transaction", transaction)
    monkeypatch.setattr(
        ticker_repository, "load_realtime_screening_input",
        lambda cursor, **scope: ticker_fixture(),
    )
    monkeypatch.setattr(
        ticker_repository, "_load_ticker_calendar_inputs",
        lambda cursor, members, cutoff: ((), ()),
    )
    monkeypatch.setattr(
        ticker_repository, "_load_ticker_fundamental_inputs",
        lambda cursor, members, cutoff: ((), ()),
    )
    monkeypatch.setattr(
        ticker_repository, "project_realtime_ticker_workspace",
        lambda inputs, **context: projection,
    )
    monkeypatch.setattr(
        ticker_repository, "produce_realtime_ticker_workspace_summary",
        lambda inputs, snapshots, charts: summary,
    )
    monkeypatch.setattr(
        ticker_repository, "persist_realtime_equity_materialization_batch_cursor",
        lambda cursor, writes: events.append((
            "evidence_batch", cursor, len(writes),
        )),
    )
    monkeypatch.setattr(
        ticker_repository, "persist_screening_generation_cursor",
        lambda cursor, generation: events.append(("screening", cursor, generation.generation_id)),
    )
    monkeypatch.setattr(
        ticker_repository, "persist_portal_snapshot_cursor",
        lambda cursor, snapshot: events.append(("portal", cursor, snapshot.snapshot_id)),
    )

    snapshots = materialize_realtime_ticker_workspace(
        compute, publication, cohort_id=COHORT_ID,
    )

    assert snapshots == projection.snapshots
    assert [event[1] for event in events if event[0] == "transaction"] == [
        "compute", "compute", "compute", "compute", "publication",
    ]
    evidence_positions = [
        index for index, event in enumerate(events)
        if event[0] == "evidence_batch"
    ]
    portal_positions = [index for index, event in enumerate(events) if event[0] == "portal"]
    assert [events[index][2] for index in evidence_positions] == [9 * 5, 9 * 5]
    assert len(portal_positions) == 11
    assert max(evidence_positions) < min(portal_positions)
    # The screening generation the workspaces name is persisted once, by the compute role, after the evidence.
    screening_positions = [index for index, event in enumerate(events) if event[0] == "screening"]
    assert len(screening_positions) == 1
    assert events[screening_positions[0]][1:] == ("compute-cursor", projection.screening_generation.generation_id)
    assert max(evidence_positions) < screening_positions[0] < min(portal_positions)


def test_session_close_materialization_keeps_summary_and_pattern_only_and_publishes_both_releases(monkeypatch):
    from realtime.portal_projection_repository import PortalReleaseReceipt, session_close_snapshot
    from realtime.ticker_projection import PATTERN_WATCH_PUBLICATION_VERSION
    compute = object.__new__(RealtimeProcessDatabase)
    compute._process_plan = MagicMock(role=RealtimeDatabaseRole.COMPUTE)
    publication = object.__new__(RealtimeProcessDatabase)
    publication._process_plan = MagicMock(role=RealtimeDatabaseRole.PUBLICATION)
    projection = ticker_projection()
    receipt = PortalReleaseReceipt(uuid5(NAMESPACE_URL, "receipt"), COHORT_ID, ProviderTier.MASSIVE_DEVELOPER,
                                   OBSERVED_AT, "9" * 64)
    persisted, releases = [], []

    @contextmanager
    def transaction(database, **options):
        yield "compute-cursor" if database is compute else "publication-cursor"

    def persist(cursor, snapshot, **options):
        persisted.append((snapshot.snapshot_type, options.get("retention")))
        if options.get("retention") == RETENTION_HOT_AND_KEPT:
            return options.get("kept_snapshot") or session_close_snapshot(snapshot)

    monkeypatch.setattr(RealtimeProcessDatabase, "transaction", transaction)
    monkeypatch.setattr(ticker_repository, "load_realtime_screening_input", lambda cursor, **scope: ticker_fixture())
    monkeypatch.setattr(ticker_repository, "_load_ticker_calendar_inputs", lambda cursor, members, cutoff: ((), ()))
    monkeypatch.setattr(ticker_repository, "_load_ticker_fundamental_inputs", lambda cursor, members, cutoff: ((), ()))
    monkeypatch.setattr(ticker_repository, "project_realtime_ticker_workspace", lambda inputs, **context: projection)
    monkeypatch.setattr(ticker_repository, "persist_realtime_equity_materialization_batch_cursor",
                        lambda cursor, writes: None)
    monkeypatch.setattr(ticker_repository, "persist_screening_generation_cursor", lambda cursor, generation: None)
    monkeypatch.setattr(ticker_repository, "persist_portal_snapshot_cursor", persist)
    monkeypatch.setattr(ticker_repository, "publish_portal_release_cursor",
                        lambda cursor, active, snapshots, **options: releases.append((snapshots, options)))

    materialize_realtime_ticker_workspace(compute, publication, receipt=receipt, session_close=True,
                                          cohort_id=COHORT_ID)

    assert [row for row in persisted if row[1] is None] == [(TICKER_WORKSPACE_VERSION, None)] * 9
    assert sorted(row for row in persisted if row[1]) == [
        (PATTERN_WATCH_PUBLICATION_VERSION, RETENTION_HOT_AND_KEPT),
        ("TICKER_WORKSPACE_SUMMARY_V1", RETENTION_HOT_AND_KEPT),
    ]
    assert len(releases) == 2
    assert len(releases[0][0]) == 11 and releases[0][1] == {}
    kept = releases[1][0]
    assert sorted(row.snapshot_type for row in kept) == [PATTERN_WATCH_PUBLICATION_VERSION, "TICKER_WORKSPACE_SUMMARY_V1"]
    assert releases[1][1]["release_class"] == "SESSION_CLOSE"
    summary = next(row for row in kept if row.snapshot_type == "TICKER_WORKSPACE_SUMMARY_V1")
    assert set(summary.payload["setup_evidence_refs"]) == set(EQUITY_VALIDATION_UNIVERSE.equity_tickers)
    assert len(summary.payload["workspaces"]) == 9
    assert summary.source_manifest["setup_evidence_set"]["count"] == 9
    assert "references" not in summary.source_manifest and "workspaces" not in summary.source_manifest


def test_pattern_bundle_has_distinct_window_policy_without_changing_setup_policy():
    projection = ticker_projection()
    result = projection.pattern_materializations[0]
    policy = "a" * 64
    pattern = ticker_repository._ticker_feature_definition(result, policy, pattern_watch=True)
    setup = ticker_repository._ticker_feature_definition(result, policy)
    assert pattern.bundle_sha256 != setup.bundle_sha256
    assert pattern.dependency_spec["primary_window"] == 300
    assert setup.dependency_spec["primary_window"] == 500
    assert pattern.dependency_spec["version"] == "realtime-pattern-watch-input-v1"


def test_pattern_publication_presorts_views_and_retains_empty_analysis_receipts():
    from realtime.ticker_projection import produce_realtime_pattern_watch_publication
    inputs = ticker_fixture()
    snapshots = ticker_snapshots()
    publication = produce_realtime_pattern_watch_publication(inputs, snapshots)
    daily = publication.payload["views"]["1d"]
    assert daily["status"] == "COMPLETE"
    assert daily["scanned"] == 9
    assert len(daily["analysis_refs"]) == 9
    assert all(sum(ref["evidence_type"] == "FEATURE_SNAPSHOT" for ref in refs) == 1
               for refs in daily["analysis_refs"].values())
    assert publication.payload["views"]["15m"]["evidence_storage"] == "RELEASE_ONLY"
    assert publication.source_manifest["primary_window"] == 300
    empty = []
    for snapshot in snapshots:
        payload = deepcopy(snapshot.payload)
        payload["patterns"]["1d"]["patterns"] = []
        empty.append(replace(snapshot, payload=payload))
    empty_publication = produce_realtime_pattern_watch_publication(inputs, empty)
    empty_daily = empty_publication.payload["views"]["1d"]
    assert empty_daily["status"] == "COMPLETE"
    assert empty_daily["results"] == [] and empty_daily["matched_tickers"] == 0
    assert len(empty_daily["analysis_refs"]) == 9
    missing_payload = deepcopy(empty[0].payload)
    del missing_payload["patterns"]["1d"]
    missing = [replace(empty[0], payload=missing_payload), *empty[1:]]
    unavailable = produce_realtime_pattern_watch_publication(inputs, missing).payload["views"]["1d"]
    assert unavailable["status"] == "UNAVAILABLE" and unavailable["unavailable_tickers"] == [empty[0].snapshot_key]


def test_pattern_publication_reader_is_stored_only_and_pinned(monkeypatch):
    app, store = ticker_app()
    publication = ticker_repository.produce_realtime_pattern_watch_publication(ticker_fixture(), ticker_snapshots())
    monkeypatch.setattr(ticker_repository, "sort_pattern_watch_rows", lambda *args: pytest.fail("GET must not sort patterns"), raising=False)
    reader = RealtimeTickerWorkspaceRepository(store.cursor)
    start = len(store.statements)
    actual = reader.scan_patterns(interval="1d", limit=1, available_by=OBSERVED_AT, cohort_id=COHORT_ID)
    assert actual["results"] == publication.payload["views"]["1d"]["results"][:1]
    assert actual["scanned"] == 9 and actual["read_source"] == "PATTERN_WATCH_V1"
    assert actual["publication"]["snapshot_id"] == str(publication.snapshot_id)
    assert all(statement.startswith(("SELECT", "SET")) for statement in store.statements[start:])
    assert not any("SELECT snapshot.snapshot_id, snapshot.payload_sha256" in statement for statement in store.statements[start:])
    key = (str(COHORT_ID), "PATTERN_WATCH_V1", "PLATFORM")
    store.current[key] = "missing"
    pinned = reader.scan_patterns(interval="1d", limit=1, available_by=OBSERVED_AT, cohort_id=COHORT_ID,
                                  snapshot_id=publication.snapshot_id)
    assert pinned["results"] == actual["results"]
    with pytest.raises(LookupError):
        reader.scan_patterns(interval="1d", limit=1, available_by=OBSERVED_AT, cohort_id=COHORT_ID)


def test_pattern_scan_without_new_publication_never_rebuilds_from_ticker_snapshots(monkeypatch):
    app, store = ticker_app()
    store.current.pop((str(COHORT_ID), "PATTERN_WATCH_V1", "PLATFORM"))
    reader = RealtimeTickerWorkspaceRepository(store.cursor)
    monkeypatch.setattr(reader, "_snapshot", lambda *args: pytest.fail("Missing release must not aggregate ticker payloads"))
    with pytest.raises(LookupError, match="no stored publication"):
        reader.scan_patterns(interval="1d", limit=100, available_by=OBSERVED_AT, cohort_id=COHORT_ID)


@pytest.mark.parametrize("defect", ["missing_evidence", "corrupt_evidence", "payload", "reference", "action", "wrong_pin"])
def test_pattern_publication_reader_rejects_integrity_drift(defect):
    app, store = ticker_app()
    if defect in {"missing_evidence", "corrupt_evidence"}:
        setattr(store, defect, True)
    elif defect == "payload":
        row = next(row for row in store.snapshots.values() if row["snapshot_type"] == "PATTERN_WATCH_V1")
        row["payload"]["views"]["1d"]["scanned"] = 99
    elif defect == "reference":
        store.reference_drift = True
    elif defect == "action":
        store.action_drift = True
    reader = RealtimeTickerWorkspaceRepository(store.cursor)
    with pytest.raises((ValueError, LookupError)):
        reader.scan_patterns(interval="1d", limit=100, available_by=OBSERVED_AT, cohort_id=COHORT_ID,
                             snapshot_id=ticker_snapshots()[0].snapshot_id if defect == "wrong_pin" else None)


def test_pattern_publication_reader_keeps_complete_empty_distinct_from_missing():
    from realtime.ticker_projection import produce_realtime_pattern_watch_publication
    app, store = ticker_app()
    snapshots = []
    for snapshot in ticker_snapshots():
        payload = deepcopy(snapshot.payload)
        payload["patterns"]["1d"]["patterns"] = []
        snapshots.append(replace(snapshot, payload=payload))
    publication = produce_realtime_pattern_watch_publication(ticker_fixture(), snapshots)
    store.publish(publication)
    reader = RealtimeTickerWorkspaceRepository(store.cursor)
    result = reader.scan_patterns(interval="1d", limit=100, available_by=OBSERVED_AT, cohort_id=COHORT_ID,
                                  snapshot_id=publication.snapshot_id)
    assert result["results"] == [] and result["scanned"] == 9
    row = store.snapshots[str(publication.snapshot_id)]
    row["payload"]["views"]["1d"]["status"] = "UNAVAILABLE"
    row["payload_sha256"] = canonical_sha256(row["payload"])
    with pytest.raises(LookupError, match="complete stored interval"):
        reader.scan_patterns(interval="1d", limit=100, available_by=OBSERVED_AT, cohort_id=COHORT_ID,
                             snapshot_id=publication.snapshot_id)


@pytest.mark.parametrize("failure", ["wrong_cohort", "payload", "manifest", "reference", "action"])
def test_stored_contract_fails_closed(failure):
    app, store = ticker_app()
    row = next(row for row in store.snapshots.values() if row["snapshot_key"] == "SPY")
    if failure in {"payload", "manifest"}:
        row["payload" if failure == "payload" else "source_manifest"]["ticker"] = "DRIFT"
    elif failure == "reference":
        store.reference_drift = True
    elif failure == "action":
        store.action_drift = True
    cohort = str(uuid5(NAMESPACE_URL, "wrong-cohort")) if failure == "wrong_cohort" else str(COHORT_ID)
    with TestClient(app) as client:
        response = client.get("/api/stock/SPY/quote", params={"cohort_id": cohort})
        assert response.status_code in {422, 503}


def test_missing_intervals_calendar_expiry_and_periods_never_fallback():
    app, _ = ticker_app()
    params = {"cohort_id": str(COHORT_ID)}
    with TestClient(app) as client:
        assert client.get("/api/stock/SPY/chart", params=dict(params, interval="1m")).status_code == 200
        calendar = client.get("/api/options/calendar/SPY", params=params)
        assert calendar.status_code == 200
        assert calendar.json()["available"] is False
        assert calendar.json()["reason"] == "EVENT_CALENDAR_COVERAGE_INCOMPLETE"
        assert client.get("/api/stock/SPY/chart", params=dict(params, period="bogus")).status_code == 422
        full = client.get("/api/stock/SPY/chart", params=dict(params, interval="5m", period="5y")).json()
        short = client.get("/api/stock/SPY/chart", params=dict(params, interval="5m", period="1d")).json()
        assert 0 < len(short) < len(full)
        assert short == full[-len(short):]
    expired, expired_store = ticker_app(clock=lambda: OBSERVED_AT + timedelta(days=12))
    for row in expired_store.snapshots.values():
        row["valid_until"] = OBSERVED_AT
    with TestClient(expired) as client:
        assert client.get("/api/stock/SPY/quote", params=params).status_code == 200
        assert client.get("/api/stock/SPY/chart", params=params).status_code == 200
        multi = client.get("/api/stock/SPY/trade-setup/multi", params=params)
        assert multi.status_code == 200
        payload = multi.json()
        assert set(payload["setups"]) == {"30m", "1h", "1d", "1wk", "1mo"}
        assert any(
            metrics["status"] == "EXPIRED" and metrics["is_serveable"] is False
            for metrics in payload["setup_read_metrics"].values()
        )
        freshness = client.get("/api/equity/materialization-status", params=params)
        assert freshness.status_code == 200
        assert freshness.json()["status"] == "EXPIRED"
        assert freshness.json()["source"] == "TICKER_WORKSPACE_V1"
        assert freshness.json()["ticker_count"] == 9
        assert freshness.json()["serving_latest_available"] is True


def test_serveable_stale_setups_retain_stored_confluence_zones():
    app, _ = ticker_app(clock=lambda: OBSERVED_AT + timedelta(days=1))
    with TestClient(app) as client:
        response = client.get(
            "/api/stock/AAPL/trade-setup/multi",
            params={"cohort_id": str(COHORT_ID)},
        )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert any(
        metrics["status"] == "STALE" and metrics["is_serveable"]
        for metrics in payload["setup_read_metrics"].values()
    )
    assert payload["confluence_zones"]


def test_ticker_profile_projects_exact_retained_calendar_and_fundamentals():
    inputs = ticker_fixture()
    security_id = next(row["security_id"] for row in inputs.members if row["ticker"] == "AAPL")
    event_time = datetime(2026, 10, 4, 20, 0, tzinfo=timezone.utc)
    market_events = (
        dict(
            revision_id=uuid5(NAMESPACE_URL, "ticker-calendar-aapl"), security_id=security_id,
            event_key="aapl-q3", event_type="EARNINGS", event_date=event_time.date(),
            announced_at=None, event_timezone="America/New_York", time_precision="DATE_ONLY",
            status="SCHEDULED", source="finnhub", observed_at=OBSERVED_AT,
            received_at=OBSERVED_AT, valid_until=OBSERVED_AT + timedelta(days=30),
            payload_sha256="b" * 64,
        ),
        dict(
            revision_id=uuid5(NAMESPACE_URL, "ticker-calendar-fed"), security_id=None,
            event_key="fomc-oct", event_type="FED_RATE_DECISION", event_date=event_time.date(),
            announced_at=None, event_timezone="America/New_York", time_precision="DATE_ONLY",
            status="SCHEDULED", source="federal_reserve", observed_at=OBSERVED_AT,
            received_at=OBSERVED_AT, valid_until=OBSERVED_AT + timedelta(days=30),
            payload_sha256="c" * 64,
        ),
    )
    event_coverage = (
        dict(
            coverage_id=uuid5(NAMESPACE_URL, "ticker-calendar-aapl-coverage"), security_id=security_id,
            event_type="EARNINGS", window_start=inputs.session - timedelta(days=1),
            window_end=inputs.session + timedelta(days=30), source="finnhub", verdict="COVERED",
            checked_at=OBSERVED_AT, valid_until=OBSERVED_AT + timedelta(days=30), evidence_sha256="d" * 64,
        ),
        dict(
            coverage_id=uuid5(NAMESPACE_URL, "ticker-calendar-fed-coverage"), security_id=None,
            event_type="FED_RATE_DECISION", window_start=inputs.session - timedelta(days=1),
            window_end=inputs.session + timedelta(days=30), source="federal_reserve", verdict="COVERED",
            checked_at=OBSERVED_AT, valid_until=OBSERVED_AT + timedelta(days=30), evidence_sha256="e" * 64,
        ),
    )
    fundamental_reports = (
        dict(
            report_id=uuid5(NAMESPACE_URL, "ticker-fundamental-aapl"), security_id=security_id,
            security_revision_id=next(
                row["security_revision_id"] for row in inputs.members if row["ticker"] == "AAPL"
            ),
            cik="0000000001", accession_number="0000000001-26-000001", form_type="10-Q",
            timeframe="quarterly", fiscal_year=2026, fiscal_quarter=3,
            period_end=date(2026, 9, 30), filing_date=date(2026, 10, 1),
            availability_time=OBSERVED_AT - timedelta(days=1), observed_at=OBSERVED_AT,
            source="POLYGON_FINANCIALS_V1", source_key="aapl-2026-q3",
            metrics={
                "revenue": "100000000", "gross_profit": "40000000",
                "operating_income": "25000000", "net_income": "20000000",
                "diluted_eps": "2.50", "free_cash_flow": "18000000",
                "total_equity": "75000000",
            },
            payload_sha256="f" * 64, quality_codes=("FILING_AVAILABILITY_DATE_ONLY",),
        ),
    )
    fundamental_coverage = (
        dict(
            coverage_id=uuid5(NAMESPACE_URL, "ticker-fundamental-aapl-coverage"),
            security_id=security_id, window_start=date(2021, 10, 1), window_end=inputs.session,
            source="massive", report_count=1, response_sha256="1" * 64,
            checked_at=OBSERVED_AT, valid_until=OBSERVED_AT + timedelta(days=30),
        ),
    )
    snapshots = produce_realtime_ticker_workspace(
        inputs, market_events=market_events, market_event_coverage=event_coverage,
        fundamental_reports=fundamental_reports, fundamental_coverage=fundamental_coverage,
    )
    app, _ = ticker_app(snapshots=snapshots)
    with TestClient(app) as client:
        response = client.get(
            "/api/options/calendar/AAPL",
            params={"cohort_id": str(COHORT_ID), "days_forward": 30},
        )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["available"] is True
    assert payload["reason"] is None
    assert payload["provider_fetch_performed"] is False
    assert payload["data"]["earnings_state"] == "BLOCKED"
    assert payload["data"]["fed_state"] == "BLOCKED"
    assert [row["event_type"] for row in payload["data"]["events"]] == [
        "EARNINGS", "FED_RATE_DECISION",
    ]
    with TestClient(app) as client:
        profile = client.get(
            "/api/equity/security/AAPL",
            params={"cohort_id": str(COHORT_ID), "report_limit": 12},
        )
    assert profile.status_code == 200, profile.text
    reports = profile.json()["fundamental_reports"]
    assert len(reports) == 1
    assert reports[0]["fiscal_year"] == 2026
    assert reports[0]["revenue"] == "100000000"
    assert "fundamentals" not in profile.json()["missing_producers"]


@pytest.mark.parametrize("failure", ["basis", "lineage", "identity", "future", "split", "coverage", "reference"])
def test_producer_rejects_evidence_drift(failure):
    inputs = ticker_fixture()
    ticker = "NFLX" if failure == "split" else "AAPL"
    member = next(row for row in inputs.members if row["ticker"] == ticker)
    inputs = replace(
        inputs, members=(member,), bars=tuple(row for row in inputs.bars if row["ticker"] == ticker),
        actions=tuple(row for row in inputs.actions if row["ticker"] == ticker),
        coverage=tuple(row for row in inputs.coverage if row["security_id"] == member["security_id"]),
    )
    if failure == "coverage":
        inputs = replace(inputs, coverage=())
    elif failure == "reference":
        members = deepcopy(inputs.members)
        members[0]["security_type"] = None
        inputs = replace(inputs, members=members)
    elif failure == "split":
        actions = deepcopy(inputs.actions)
        actions[0]["effective_date"] = inputs.session
        inputs = replace(inputs, actions=actions)
    else:
        rows = deepcopy(inputs.bars)
        row = next(row for row in rows if row["interval"] == "1wk")
        if failure == "basis":
            row["adjustment_basis"] = "RAW_UNADJUSTED"
        elif failure == "lineage":
            row["source_kind"] = "DERIVED"
        elif failure == "identity":
            row["instrument_id"] += 1
        elif failure == "future":
            row["observed_at"] = OBSERVED_AT + timedelta(minutes=1)
        inputs = replace(inputs, bars=rows)
    with pytest.raises(ValueError):
        produce_realtime_ticker_workspace(inputs)


def test_portal_loader_reads_exact_retained_intervals_and_causal_clocks():
    inputs = ticker_fixture()
    cursor = MagicMock()
    cursor.fetchone.return_value = dict(manifest_id=UUID(int=1), manifest_sha256="a" * 64,
                                       stock_count=9, provider_tier=inputs.provider_tier.value)
    cursor.fetchall.side_effect = [inputs.members, inputs.bars, inputs.actions, inputs.coverage]
    loaded = load_realtime_screening_input(
        cursor, cohort_id=COHORT_ID, session=inputs.session, cutoff=OBSERVED_AT,
        source_generation=3, include_portal_history=True,
    )
    assert loaded == inputs
    bar_sql = cursor.execute.call_args_list[2].args[0]
    assert "interval IN ('1m','5m','15m','30m','1h')" in bar_sql
    assert "adjustment_basis='RAW_UNADJUSTED' AND interval='1d'" in bar_sql
    assert "interval='1wk' AND session_date BETWEEN %s AND %s" in bar_sql
    assert "interval='1mo' AND session_date BETWEEN %s AND %s" in bar_sql
    assert "interval IN ('15m','30m','1h','1d') AND source_kind='DERIVED' THEN 5" in bar_sql
    assert "interval='1d' AND source_kind='NATIVE_REST' THEN 5" in bar_sql
    assert "interval IN ('1wk','1mo') AND source_kind='DERIVED' THEN 5" in bar_sql
    assert "provider_published_at<=%s" in bar_sql and "created_at<=%s" in bar_sql
    action_sql = cursor.execute.call_args_list[-2].args[0]
    assert "newer.source_key=action.source_key" in action_sql
    assert "newer.action_type=action.action_type" in action_sql
    assert "newer.received_at<=%s AND newer.observed_at<=%s" in action_sql
    assert "observation_ties" in action_sql


def test_portal_loader_rejects_conflicting_action_observation_ties():
    inputs = ticker_fixture()
    cursor = MagicMock()
    cursor.fetchone.return_value = dict(manifest_id=UUID(int=1), manifest_sha256="a" * 64,
                                       stock_count=9, provider_tier=inputs.provider_tier.value)
    actions = tuple({**row, "observation_ties": 2} for row in inputs.actions)
    cursor.fetchall.side_effect = [inputs.members, inputs.bars, actions]
    with pytest.raises(ValueError, match="corporate-action observations conflict"):
        load_realtime_screening_input(
            cursor, cohort_id=COHORT_ID, session=inputs.session, cutoff=OBSERVED_AT,
            source_generation=3, include_portal_history=True,
        )


def test_native_5m_window_drift_fails_closed_even_when_long_derived_intervals_are_absent():
    inputs = ticker_fixture()
    member = next(row for row in inputs.members if row["ticker"] == "AAPL")
    plan = build_realtime_equity_portal_history_plan(inputs.session)
    rows = [deepcopy(row) for row in inputs.bars if row["ticker"] == "AAPL"
            and (row["adjustment_basis"] == "ADJUSTED" or row["interval"] == "5m"
                 or row["session_date"] >= plan.raw_sessions[-20])]
    first = next(row for row in rows if row["interval"] == "5m" and row["session_date"] == plan.raw_start)
    first["bar_start"] += timedelta(minutes=1)
    first["bar_end"] += timedelta(minutes=1)
    focused = replace(inputs, members=(member,), bars=tuple(rows), actions=(),
                      coverage=tuple(row for row in inputs.coverage if row["security_id"] == member["security_id"]))
    with pytest.raises(ValueError, match="native 5m session windows"):
        produce_realtime_ticker_workspace(focused)


def test_stored_workspace_carries_references_not_bodies():
    from realtime.lineage_head import BOUNDED_WORKSPACE_PAYLOAD_BYTES, require_bounded_payload

    for snapshot in ticker_snapshots():
        payload, groups = snapshot.payload, snapshot.source_manifest["bar_lineage"]["groups"]
        assert "charts" not in payload and "evidence" not in payload
        assert canonical_sha256(payload) == snapshot.payload_sha256
        assert len(canonical_json(payload)) < 80_000
        assert set(payload["chart_refs"]) == set(payload["chart_market_times"]) == {
            "1m", "5m", "15m", "30m", "1h", "1d", "1wk", "1mo"}
        for interval, key in payload["chart_refs"].items():
            assert groups[key]["last_bar_end"] == payload["chart_market_times"][interval]
        assert all(set(ref) == {"evidence_id", "evidence_type", "payload_sha256"}
                   for rows in payload["evidence_index"].values() for ref in rows)
        embedded = dict(payload, charts=old_embedded_charts(snapshot.snapshot_key, payload["chart_refs"]))
        with pytest.raises(ValueError, match="by-reference payload"):
            require_bounded_payload(embedded, name="embedded")
        assert len(canonical_json(embedded)) > BOUNDED_WORKSPACE_PAYLOAD_BYTES


def test_summary_is_fed_by_the_in_process_charts_the_stored_payload_only_references():
    projection = ticker_projection()
    summary = produce_realtime_ticker_workspace_summary(
        ticker_fixture(), projection.snapshots, projection.summary_charts)
    aapl = next(row for row in summary.payload["overview"] if row["ticker"] == "AAPL")
    daily = old_embedded_charts("AAPL", {"1d": "1d:ADJUSTED"})["1d"]
    assert (aapl["close"], aapl["open"], aapl["volume"]) == (
        daily[-1]["close"], daily[-1]["open"], daily[-1]["volume"])
    assert set(projection.summary_charts["AAPL"]) == {"1d", "1wk"}
    with pytest.raises(KeyError):
        produce_realtime_ticker_workspace_summary(ticker_fixture(), projection.snapshots, {})
    # jsonb stores no negative zero, so a stored -0.0 would read back as 0.0 and fail its checksum.
    assert not [value for row in summary.payload["overview"] for value in row.values()
                if isinstance(value, float) and value == 0 and math.copysign(1, value) < 0]


def test_reader_composes_the_charts_and_evidence_the_payload_used_to_embed():
    from realtime.ticker_references import compose_evidence

    app, store = ticker_app()
    start = len(store.statements)
    reader = RealtimeTickerWorkspaceRepository(store.cursor)
    for snapshot in ticker_snapshots():
        ticker = snapshot.snapshot_key
        stored, manifest = as_stored_json(snapshot.payload), as_stored_json(snapshot.source_manifest)
        loaded = reader._snapshot(ticker, OBSERVED_AT, COHORT_ID)
        # The workspace is read back compact; the bodies are composed on demand, never kept in its entry.
        assert "charts" not in loaded and "evidence" not in loaded
        assert {key: value for key, value in loaded.items() if key not in {"_chart_source", "evidence_refs"}
                } == {**stored, "context": loaded["context"]}
        old_charts = old_embedded_charts(ticker, stored["chart_refs"])
        for interval, expected in old_charts.items():
            chart = reader.read(ticker, "chart", interval=interval, period="5y", available_by=OBSERVED_AT,
                                cohort_id=COHORT_ID)
            assert as_stored_json(chart) == as_stored_json(expected)
        with store.cursor() as cursor:
            bodies = ticker_repository._verified_evidence_bodies(
                cursor, manifest, stored, cohort_id=COHORT_ID, cutoff=OBSERVED_AT)
        assert as_stored_json(compose_evidence(stored["evidence_index"], bodies)) == as_stored_json(
            old_embedded_evidence(snapshot))
    assert all(sql.startswith(("SELECT", "SET")) for sql in store.statements[start:])
    assert not any("canonical" in sql or "public." in sql for sql in store.statements)


def test_a_chart_is_composed_when_it_is_read_and_cached_per_snapshot_and_interval():
    app, store = ticker_app()
    reader = RealtimeTickerWorkspaceRepository(store.cursor)
    reader._chart_cache = BoundedCache(2)
    start = len(store.statements)

    def bar_reads():
        return [sql for sql in store.statements[start:] if sql.startswith("SELECT cur.interval")]

    read = lambda interval: reader.read(  # noqa: E731
        "AAPL", "chart", interval=interval, period="5y", available_by=OBSERVED_AT, cohort_id=COHORT_ID)
    reader.read("AAPL", "quote", available_by=OBSERVED_AT, cohort_id=COHORT_ID)
    assert bar_reads() == []
    first = read("1d")
    assert len(bar_reads()) == 1 and read("1d") == first and len(bar_reads()) == 1
    read("5m"), read("1mo")
    assert len(bar_reads()) == 3 and len(reader._chart_cache) == 2
    read("1d")
    assert len(bar_reads()) == 4
    assert all(sql.count("cur.bar_start>=%s") == 1 for sql in bar_reads())


@pytest.mark.parametrize("interval", ["1m", "5m", "1d", "1wk", "1mo"])
@pytest.mark.parametrize("defect", ["revised", "retired"])
def test_reader_fails_closed_when_a_published_bar_was_revised_or_retired(defect, interval):
    app, store = ticker_app()
    getattr(store, f"{defect}_bars").add(("AAPL", interval))
    params = {"cohort_id": str(COHORT_ID), "period": "5y"}
    with TestClient(app) as client:
        response = client.get("/api/stock/AAPL/chart", params=dict(params, interval=interval))
        assert response.status_code == 503
        assert "bar lineage changed; republish required" in response.json()["detail"]
        other = "15m" if interval != "15m" else "30m"
        assert client.get("/api/stock/AAPL/chart", params=dict(params, interval=other)).status_code == 200
        assert client.get("/api/stock/MSFT/chart", params=dict(params, interval=interval)).status_code == 200
        assert client.get("/api/stock/AAPL/quote", params=params).status_code == 200


@pytest.mark.parametrize("read_mode", ["current", "retained"])
@pytest.mark.parametrize("defect", ["vanished", "tampered"])
def test_reader_verifies_every_referenced_evidence_row_not_only_the_setups(defect, read_mode):
    app, store = ticker_app()
    snapshot = next(row for row in ticker_snapshots() if row.snapshot_key == "AAPL")
    other = next(UUID(ref["evidence_id"]) for rows in snapshot.payload["evidence_index"].values()
                 for ref in rows if ref["evidence_type"] != "TRADE_SETUP")
    getattr(store, f"{defect}_evidence").add(other)
    with TestClient(app) as client:
        response = client.get("/api/stock/AAPL/quote", params={"cohort_id": str(COHORT_ID), "read_mode": read_mode})
        assert response.status_code == 422
        assert "immutable identity/hash" in response.json()["detail"]


def test_reader_still_serves_a_workspace_published_with_embedded_bodies():
    snapshot = next(row for row in ticker_snapshots() if row.snapshot_key == "AAPL")
    legacy_payload = {key: value for key, value in snapshot.payload.items()
                      if key not in {"chart_refs", "evidence_index"}}
    legacy_payload.update(
        charts=old_embedded_charts("AAPL", snapshot.payload["chart_refs"]), evidence=old_embedded_evidence(snapshot))
    legacy = build_portal_snapshot(
        cohort_id=snapshot.cohort_id, provider_tier=snapshot.provider_tier, snapshot_type=snapshot.snapshot_type,
        snapshot_key=snapshot.snapshot_key, source_generation=snapshot.source_generation,
        source_manifest=snapshot.source_manifest, payload=legacy_payload, generated_at=snapshot.generated_at)
    app, store = ticker_app(snapshots=[legacy if row.snapshot_key == "AAPL" else row for row in ticker_snapshots()])
    params = {"cohort_id": str(COHORT_ID)}
    with TestClient(app) as client:
        chart = client.get("/api/stock/AAPL/chart", params=dict(params, interval="1d", period="5y"))
        assert chart.status_code == 200, chart.text
        assert chart.json() == as_stored_json(legacy_payload["charts"]["1d"])
        publication = client.get("/api/stock/AAPL/workspace-publication", params=params)
        assert publication.status_code == 200, publication.text
        assert set(publication.json()["evidence_refs"]) == {"30m", "1h", "1d", "1wk", "1mo"}
    assert not any("rt.bar_current" in sql for sql in store.statements)


def test_compact_and_embedded_payloads_resolve_the_same_setup_references():
    snapshot = next(row for row in ticker_snapshots() if row.snapshot_key == "AAPL")
    legacy = dict(snapshot.payload, evidence=old_embedded_evidence(snapshot))
    legacy.pop("evidence_index")
    compact = ticker_repository._embedded_setup_refs(snapshot.payload, cohort_id=COHORT_ID)
    assert compact == ticker_repository._embedded_setup_refs(legacy, cohort_id=COHORT_ID)
