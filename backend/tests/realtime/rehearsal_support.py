"""Support for the V2 pipeline rehearsal (test_v2_pipeline_rehearsal_db.py): history facts, reference facts,
control-plane seed and bar-row conversion. No database or network access happens at import time.

The history is the bar set of `ticker_fixture()` (test_ticker_vertical_slice.py) rebuilt for a chosen session and
two tickers: native 1d/1wk/1mo (adjusted) and 5m/1m (raw), plus the derived 15m/30m/1h/1d (raw) bars the
history package builds. Each fixture row is kept as it is (derived rows stay derived, with their
source_bar_revision_ids); only the clocks are rewritten the way a REST fetch would stamp them: every bar is
observed when it was fetched (`max(bar_end + 1 minute, fetched_at)`), never before it ended.
"""
from __future__ import annotations

import math
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import NAMESPACE_URL, UUID, uuid5

import exchange_calendars

from equity.domain import BarAvailabilityMode, BarSessionScope, BarSourceKind, EquityBarRevision
from realtime.acquisition_handoff import DeveloperAcquisitionFactKind, DeveloperEquityBarAcquisitionFact
from realtime.acquisition_ledger import (
    PriceBasis,
    ProviderRequestCompletion,
    ProviderRequestLane,
    ProviderRequestOutcome,
    ProviderRequestReservation,
    ProviderRequestState,
    RecordedProviderRequest,
    RequestCompleteness,
)
from realtime.acquisition_producer import CompletedDeveloperSourceBatch, DeveloperSourceResult
from realtime.contracts import canonical_sha256
from realtime.equity_bar_adapter import RealtimeEquityBarFact, adapt_developer_equity_bar
from realtime.equity_portal_history import (
    build_realtime_equity_portal_history_package,
    build_realtime_equity_portal_history_plan,
)
from realtime.equity_validation_history import SplitCoverage
from realtime.reference_facts import ReferenceFact, ReferenceFactRelation
from realtime.reference_seed import ReferenceKind, ReferenceSeedMember, SeedMemberStatus, build_reference_seed_manifest
from realtime.reference_service import ReferenceServiceResult
from tests.realtime.test_equity_validation_history import _security

UTC = timezone.utc
FIRST_TICKERS = ("AAPL", "AMZN")  # the first two of EQUITY_VALIDATION_UNIVERSE
MAX_FACTS_PER_BATCH = 1_000


def next_session_on_or_after(day: date) -> date:
    calendar = exchange_calendars.get_calendar("XNYS")
    return calendar.date_to_session(str(day), direction="next").date()


def session_bounds(session: date) -> tuple[datetime, datetime]:
    calendar = exchange_calendars.get_calendar("XNYS")
    return (calendar.session_open(str(session)).to_pydatetime(), calendar.session_close(str(session)).to_pydatetime())


def _revision(ticker, interval, start, end, *, adjusted, index, security_id, close, observed_at, segment):
    payload_sha256 = canonical_sha256((ticker, interval, start, adjusted, index))
    return EquityBarRevision(
        bar_revision_id=uuid5(NAMESPACE_URL, f"rehearsal-bar:{payload_sha256}"), security_id=security_id,
        ticker=ticker, interval=interval, session_date=start.date(), bar_start=start, bar_end=end,
        open_price=close - 1, high_price=close + 2, low_price=close - 2, close_price=close, volume=Decimal("1000"),
        vwap=close, transaction_count=10, source_kind=BarSourceKind.NATIVE_REST,
        availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED, is_final=True,
        system_observed_at=observed_at, replay_available_at=observed_at, adjusted=adjusted,
        payload_sha256=payload_sha256, ingestion_segment_id=segment, session_scope=BarSessionScope.RTH)


def build_history(session: date, tickers, *, cohort_id: UUID, instrument_ids: dict, fetched_at: datetime):
    """{ticker: [RealtimeEquityBarFact]} for `session`, each bar observed at max(bar_end + 1m, fetched_at)."""
    plan = build_realtime_equity_portal_history_plan(session, equity_ticker_count=len(tickers))
    calendar = exchange_calendars.get_calendar("XNYS")
    result = {}
    for ticker in tickers:
        instrument = instrument_ids[ticker]
        security = _security(ticker)
        slot_base = list(FIRST_TICKERS).index(ticker) + 1 if ticker in FIRST_TICKERS else 1
        segment = uuid5(NAMESPACE_URL, f"rehearsal-segment:{ticker}")
        native = []

        def fact(interval, begin, finish, slot, adjusted, close):
            observed = max(finish + timedelta(minutes=1), fetched_at)
            revision = _revision(ticker, interval, begin, finish, adjusted=adjusted, index=slot,
                                 security_id=security.security_id, close=close, observed_at=observed,
                                 segment=segment)
            return adapt_developer_equity_bar(revision, cohort_id=cohort_id, instrument_id=instrument)

        for index, day in enumerate(plan.adjusted_sessions):
            start = calendar.session_open(str(day)).to_pydatetime()
            end = calendar.session_close(str(day)).to_pydatetime()
            close = Decimal(str(round(100 + slot_base * 5 + index * .02 + math.sin(index / 7) * 5, 4)))
            native.append(fact("1d", start, end, index, True, close))
            if day in plan.raw_sessions:
                cursor, slot = start, 0
                while cursor < end:
                    finish = min(cursor + timedelta(minutes=5), end)
                    native.append(fact("5m", cursor, finish, index * 100 + slot, False, close))
                    cursor, slot = finish, slot + 1
            if day in plan.one_minute_sessions:
                cursor, slot = start, 0
                while cursor < end:
                    finish = min(cursor + timedelta(minutes=1), end)
                    native.append(fact("1m", cursor, finish, index * 1000 + slot, False, close))
                    cursor, slot = finish, slot + 1
        closes = {}
        for index, day in enumerate(plan.adjusted_sessions):
            closes[day] = Decimal(str(round(100 + slot_base * 5 + index * .02 + math.sin(index / 7) * 5, 4)))
        for index, day in enumerate(plan.monthly_sessions):
            sessions = calendar.sessions_in_range(str(date(day.year, day.month, 1)), str(day))
            start = calendar.session_open(sessions[0]).to_pydatetime()
            end = calendar.session_close(sessions[-1]).to_pydatetime()
            monthly = fact("1mo", start, end, index, True, closes.get(day, Decimal("100")))
            native.append(replace(monthly, revision=replace(monthly.revision, session_date=day)))
        for index, day in enumerate(plan.weekly_sessions):
            iso = day.isocalendar()
            week_start = date.fromisocalendar(iso.year, iso.week, 1)
            sessions = calendar.sessions_in_range(str(week_start), str(day))
            start = calendar.session_open(sessions[0]).to_pydatetime()
            end = calendar.session_close(sessions[-1]).to_pydatetime()
            weekly = fact("1wk", start, end, index, True, closes.get(day, Decimal("100")))
            native.append(replace(weekly, revision=replace(weekly.revision, session_date=day)))
        package = build_realtime_equity_portal_history_package(
            security, native, plan=plan, observed_at=max(item.revision.system_observed_at for item in native),
            ingestion_segment_id=uuid5(NAMESPACE_URL, f"rehearsal-derived:{ticker}"),
            split_coverage=SplitCoverage(ticker, plan.adjusted_sessions[0], plan.session_date, fetched_at),
            split_actions=())
        derived = [item for item in package.facts if item.revision.source_kind is BarSourceKind.DERIVED]
        observed = {str(item.revision.bar_revision_id): item.revision.system_observed_at for item in native}
        restamped = []
        for item in derived:
            stamp = max(item.revision.bar_end + timedelta(minutes=1), fetched_at,
                        *(observed[str(source)] for source in item.revision.source_bar_revision_ids
                          if str(source) in observed))
            restamped.append(replace(item, revision=replace(
                item.revision, system_observed_at=stamp, replay_available_at=stamp)))
        result[ticker] = [*native, *restamped]
    return result


def instrument_dicts(history):
    return {ticker: facts[0].instrument_id for ticker, facts in history.items()}


def bar_row(fact: RealtimeEquityBarFact) -> dict:
    """The row dict the screening loader returns for a bar (without created_at, the database's insert time)."""
    b = fact.revision
    return dict(
        bar_revision_id=b.bar_revision_id, security_id=b.security_id, instrument_id=fact.instrument_id,
        ticker=b.ticker, cohort_id=fact.cohort_id, provider_tier=fact.provider_tier.value, interval=b.interval,
        session_date=b.session_date, session_scope=b.session_scope.value,
        adjustment_basis="ADJUSTED" if b.adjusted else "RAW_UNADJUSTED", bar_start=b.bar_start, bar_end=b.bar_end,
        open=float(b.open_price), high=float(b.high_price), low=float(b.low_price), close=float(b.close_price),
        volume=float(b.volume), vwap=float(b.vwap) if b.vwap else None, transaction_count=b.transaction_count,
        source_kind=b.source_kind.value, source_revision_ids=b.source_bar_revision_ids,
        observed_at=b.system_observed_at, replay_available_at=b.replay_available_at,
        provider_published_at=b.provider_published_at, availability_mode=b.availability_mode.value,
        ingestion_segment_id=b.ingestion_segment_id, is_final=b.is_final, payload_sha256=b.payload_sha256)


def chunked_batches(history, ticker, *, segment_prefix: str):
    """The ticker's facts ordered by bar end and cut into batches of at most 1,000 facts (the acquisition batch
    bound); every batch is one provider batch, so its facts share one ingestion segment."""
    # Natives first, then derived bars by width: a derived bar never precedes the bars it is built from.
    rank = {"15m": 1, "30m": 2, "1h": 3, "1d": 4}
    facts = sorted(history[ticker], key=lambda item: (
        item.revision.bar_end, rank.get(item.revision.interval, 0) if item.revision.source_kind is
        BarSourceKind.DERIVED else 0, item.revision.interval, str(item.revision.bar_revision_id)))
    chunks = []
    for offset in range(0, len(facts), MAX_FACTS_PER_BATCH):
        segment = uuid5(NAMESPACE_URL, f"{segment_prefix}:{ticker}:{offset}")
        chunks.append(tuple(
            DeveloperEquityBarAcquisitionFact(
                item.instrument_id, replace(item.revision, ingestion_segment_id=segment))
            for item in facts[offset:offset + MAX_FACTS_PER_BATCH]))
    return chunks


def members_for(history, instrument_ids):
    members = []
    for ticker in sorted(history):
        security = _security(ticker)
        members.append(dict(
            security_id=security.security_id, ticker=ticker, instrument_id=instrument_ids[ticker],
            security_revision_id=security.security_revision_id, company_name=security.company_name,
            security_type=security.security_type, exchange=security.primary_exchange,
            effective_from=security.effective_from, observed_at=security.observed_at,
            payload_sha256=security.payload_sha256, active=True, cik=None, composite_figi=None,
            share_class_figi=None, sic_code=None, sic_description=None, sector=None, industry=None,
            list_date=None, delisted_date=None, weighted_shares=None, free_float=None, free_float_percent=None,
            market_cap=None))
    return tuple(members)


def split_coverage_rows(history, session, *, received_at):
    plan = build_realtime_equity_portal_history_plan(session, equity_ticker_count=len(history))
    rows = []
    for ticker in sorted(history):
        security = _security(ticker)
        rows.append(dict(
            coverage_id=coverage_id(ticker), security_id=security.security_id, action_type="SPLIT",
            window_start=plan.adjusted_sessions[0], window_end=plan.session_date, received_at=received_at,
            response_action_count=0, response_sha256=canonical_sha256(("coverage-response", ticker)),
            payload_sha256=canonical_sha256(("coverage", ticker))))
    return rows


def coverage_id(ticker: str) -> UUID:
    return uuid5(NAMESPACE_URL, f"rehearsal-split-coverage:{ticker}")


def reference_facts(tickers, session, *, base: datetime, valid_until: datetime):
    """Durable facts for every READY member the seed manifest of the rehearsal declares. Ids are fixed, so a
    repeated refresh re-uses the same rows (ON CONFLICT DO NOTHING)."""
    plan = build_realtime_equity_portal_history_plan(session, equity_ticker_count=len(tickers))

    def uid(*parts):
        return uuid5(NAMESPACE_URL, "rehearsal-reference:" + ":".join(parts))

    facts = []
    for ticker in sorted(tickers):
        security = _security(ticker)
        facts.append(ReferenceFact.build(
            ReferenceFactRelation.SECURITY, "SECURITY", ticker, security_id=security.security_id,
            revision_id=security.security_revision_id, ticker=ticker, asset_type="STOCK",
            source_origin="rehearsal", exchange=security.primary_exchange, calendar_id="XNYS",
            effective_from=security.effective_from, effective_to=None, observed_at=security.observed_at,
            payload_sha256=security.payload_sha256, company_name=security.company_name,
            security_type=security.security_type, active=True))
        for action in ("SPLIT", "DIVIDEND"):
            facts.append(ReferenceFact.build(
                ReferenceFactRelation.CORPORATE_ACTION_COVERAGE, "CORPORATE_ACTION_COVERAGE", ticker,
                coverage_id=coverage_id(ticker) if action == "SPLIT" else uid("dividend", ticker),
                security_id=security.security_id, action_type=action, window_start=plan.adjusted_sessions[0],
                window_end=plan.session_date, source="massive", source_key=f"{action.lower()}:{ticker}",
                response_action_count=0, response_sha256=canonical_sha256(("coverage-response", ticker)),
                observed_at=base, received_at=base, payload_sha256=canonical_sha256(("coverage", ticker, action))))
        facts.append(ReferenceFact.build(
            ReferenceFactRelation.MARKET_EVENT_COVERAGE, "EARNINGS_COVERAGE", ticker,
            coverage_id=uid("earnings", ticker), security_id=security.security_id, event_type="EARNINGS",
            window_start=plan.session_date, window_end=plan.session_date + timedelta(days=120), source="finnhub",
            verdict="KNOWN_NO_EVENT", checked_at=base, valid_until=valid_until,
            evidence_sha256=canonical_sha256(("earnings", ticker))))
    facts.append(ReferenceFact.build(
        ReferenceFactRelation.MARKET_EVENT_COVERAGE, "FOMC_COVERAGE", "GLOBAL", coverage_id=uid("fomc"),
        security_id=None, event_type="FED_RATE_DECISION", window_start=plan.session_date,
        window_end=plan.session_date + timedelta(days=120), source="federal_reserve", verdict="KNOWN_NO_EVENT",
        checked_at=base, valid_until=valid_until, evidence_sha256=canonical_sha256("fomc")))
    facts.append(ReferenceFact.build(
        ReferenceFactRelation.RATE_OBSERVATION, "RATE_CURVE", "USD", observation_id=uid("rate"),
        rate_date=plan.session_date, tenor_days=30, annual_rate=0.04, source="treasury", source_key="30d",
        source_observed_at=base, received_at=base, payload_sha256=canonical_sha256("rate")))
    return tuple(facts)


def revise_last_bars(history, *, count: int, observed_at: datetime, label: str):
    """{ticker: (facts,)}: revisions of each ticker's last `count` native 1m bars. A revision keeps the bar
    identity (interval, basis, start), supersedes the stored revision, nudges the close and is observed
    later, as a provider restatement would be."""
    revised = {}
    for ticker, facts in history.items():
        minutes = sorted((item for item in facts if item.revision.interval == "1m"),
                         key=lambda item: item.revision.bar_start)[-count:]
        segment = uuid5(NAMESPACE_URL, f"rehearsal-revision:{label}:{ticker}")
        revised[ticker] = tuple(
            DeveloperEquityBarAcquisitionFact(item.instrument_id, replace(
                item.revision,
                bar_revision_id=uuid5(NAMESPACE_URL, f"rehearsal-revision:{label}:{item.revision.bar_revision_id}"),
                close_price=item.revision.close_price + Decimal("0.5"),
                high_price=item.revision.high_price + Decimal("0.5"), system_observed_at=observed_at,
                replay_available_at=observed_at, ingestion_segment_id=segment,
                supersedes_bar_revision_id=item.revision.bar_revision_id,
                payload_sha256=canonical_sha256(("rehearsal-revision", label, item.revision.payload_sha256))))
            for item in minutes)
    return revised


def seed_members(tickers, *, created_at: datetime):
    """Every member READY (equities need real earnings coverage, not NOT_APPLICABLE)."""
    def member(kind, key):
        return ReferenceSeedMember(kind, key, f"rehearsal:{kind.value}:{key}", "a" * 64, SeedMemberStatus.READY,
                                   created_at, created_at + timedelta(seconds=1), ())

    rows = []
    for ticker in sorted(tickers):
        rows.extend((member(ReferenceKind.SECURITY, ticker), member(ReferenceKind.CORPORATE_ACTION_COVERAGE, ticker),
                     member(ReferenceKind.EARNINGS_COVERAGE, ticker)))
    rows.append(member(ReferenceKind.FOMC_COVERAGE, "GLOBAL"))
    rows.append(member(ReferenceKind.RATE_CURVE, "USD"))
    return tuple(rows)


class RehearsalReferenceService:
    """Stands in for ReferenceSeedService: a READY manifest plus the durable facts the ticker projection reads.
    It never touches a provider; `requests` records what the REFERENCE runner asked."""

    def __init__(self, tickers, session, *, facts_base: datetime, facts_valid_until: datetime) -> None:
        self.tickers, self.session = tuple(sorted(tickers)), session
        self.base, self.valid_until = facts_base, facts_valid_until
        self.requests = []

    def run_once(self, request):
        self.requests.append(request)
        manifest = build_reference_seed_manifest(
            request.universe, session_date=request.session_date,
            members=seed_members(self.tickers, created_at=request.created_at),
            source_generations=(("federal_reserve", "e" * 64), ("finnhub", "b" * 64), ("massive", "c" * 64),
                                ("treasury", "d" * 64)),
            created_at=request.created_at, effective_at=request.effective_at, valid_until=request.valid_until)
        facts = reference_facts(self.tickers, self.session, base=self.base, valid_until=self.valid_until)
        return ReferenceServiceResult(manifest, 0, 0, facts)


class ChunkSource:
    """An equity source that serves what the test queued: one batch and one COMPLETE ledger request per
    ticker, built when `collect` is called so the ledger ids are the stage run's own. `collect` fails if
    nothing is queued (a stage must not ask for data the test did not plan)."""

    fact_kind = DeveloperAcquisitionFactKind.EQUITY_BAR

    def __init__(self) -> None:
        self.queued = None
        self.requests = []
        self.collected = []

    def queue(self, label: str, per_ticker: dict) -> None:
        self.queued = (label, {ticker: tuple(facts) for ticker, facts in per_ticker.items()})

    def collect(self, request):
        if self.queued is None:
            raise AssertionError("the source was asked for data nobody queued")
        label, per_ticker = self.queued
        self.requests.append(request)
        self.collected.append(label)
        batches, ledger = [], []
        for ticker in sorted(per_ticker):
            facts = per_ticker[ticker]
            segment = facts[0].revision.ingestion_segment_id
            observed = max(item.revision.system_observed_at for item in facts)
            request_id = uuid5(NAMESPACE_URL, f"rehearsal-request:{label}:{ticker}:{request.stage_run_id}")
            batches.append(CompletedDeveloperSourceBatch(
                DeveloperAcquisitionFactKind.EQUITY_BAR, segment, observed - timedelta(seconds=1), observed,
                facts, request_id=request_id))
            ledger.append(RecordedProviderRequest(
                ProviderRequestReservation(
                    request_id, "developer", ProviderRequestLane.EQUITY_HISTORY, "aggs_minute",
                    canonical_sha256(("rehearsal-request", label, ticker)), observed - timedelta(seconds=1),
                    cohort_id=request.cohort_id, window_id=request.window_id, stage_run_id=request.stage_run_id),
                ProviderRequestCompletion(
                    ProviderRequestState.COMPLETE, started_at=observed - timedelta(seconds=1),
                    completed_at=observed, outcome=ProviderRequestOutcome.OK, http_status=200,
                    response_bytes=1_000 * len(facts), row_count=len(facts), pages_fetched=1,
                    requested_from=min(item.revision.bar_start for item in facts),
                    requested_to=max(item.revision.bar_end for item in facts),
                    price_basis=PriceBasis.RAW_FINALIZED_MINUTE_CLOSE_V1,
                    completeness=RequestCompleteness.COMPLETE)))
        return DeveloperSourceResult("massive_developer", tuple(batches), len(batches),
                                     sum(1_000 * len(per_ticker[ticker]) for ticker in per_ticker), tuple(ledger))
