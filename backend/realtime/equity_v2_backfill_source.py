"""Exact retained-plus-approved-provider source for the V2-10 nine-ticker backfill."""
from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlparse
from uuid import NAMESPACE_URL, UUID, uuid5

import exchange_calendars
from options.data.polygon_http import PolygonHttpResponse

from .acquisition_ledger import (
    PriceBasis,
    ProviderRequestCompletion,
    ProviderRequestLane,
    ProviderRequestOutcome,
    ProviderRequestReservation,
    ProviderRequestState,
    RecordedProviderRequest,
    RequestCompleteness,
)
from .contracts import EQUITY_VALIDATION_UNIVERSE, canonical_sha256
from .equity_interval_derivation import derive_realtime_interval_facts
from .equity_portal_history import (
    RealtimeEquityPortalHistoryPlan,
    build_realtime_equity_portal_history_plan,
)
from .equity_validation_source import PolygonEquityValidationHistorySource
from .preflight import ProviderTier
from .ticker_projection import _fact

APPROVED_SESSION_DATE = date(2026, 10, 14)
APPROVED_CURRENT_SESSIONS = (
    date(2026, 10, 12),
    date(2026, 10, 13),
    date(2026, 10, 14),
)
APPROVED_WEEKLY_START = date(2021, 10, 15)
APPROVED_WEEKLY_END = date(2025, 8, 22)
APPROVED_REQUEST_CEILING = 36
APPROVED_TICKERS = EQUITY_VALIDATION_UNIVERSE.equity_tickers


def _intraday_count(sessions: tuple[date, ...], minutes: int) -> int:
    calendar = exchange_calendars.get_calendar("XNYS")
    return sum(
        int(
            (
                calendar.session_close(str(session))
                - calendar.session_open(str(session))
            ).total_seconds()
            // 60
        )
        // minutes
        for session in sessions
    )


def build_approved_gap_plan() -> RealtimeEquityPortalHistoryPlan:
    full = build_realtime_equity_portal_history_plan(APPROVED_SESSION_DATE)
    weekly = tuple(
        session
        for session in full.weekly_sessions
        if APPROVED_WEEKLY_START <= session <= APPROVED_WEEKLY_END
    )
    if (
        weekly[0] != APPROVED_WEEKLY_START
        or weekly[-1] != APPROVED_WEEKLY_END
        or len(weekly) != 202
        or tuple(full.raw_sessions[-3:]) != APPROVED_CURRENT_SESSIONS
        or tuple(full.one_minute_sessions[-3:]) != APPROVED_CURRENT_SESSIONS
    ):
        raise ValueError("approved V2-10 provider gap no longer matches the calendar plan")
    hourly = 0
    calendar = exchange_calendars.get_calendar("XNYS")
    for session in APPROVED_CURRENT_SESSIONS:
        minutes = int(
            (
                calendar.session_close(str(session))
                - calendar.session_open(str(session))
            ).total_seconds()
            // 60
        )
        hourly += (minutes + 59) // 60
    counts = (
        ("1d", "ADJUSTED", len(APPROVED_CURRENT_SESSIONS)),
        ("1wk", "ADJUSTED", len(weekly)),
        ("1mo", "ADJUSTED", 0),
        ("1m", "RAW_UNADJUSTED", _intraday_count(APPROVED_CURRENT_SESSIONS, 1)),
        ("5m", "RAW_UNADJUSTED", _intraday_count(APPROVED_CURRENT_SESSIONS, 5)),
        ("15m", "RAW_UNADJUSTED", _intraday_count(APPROVED_CURRENT_SESSIONS, 15)),
        ("30m", "RAW_UNADJUSTED", _intraday_count(APPROVED_CURRENT_SESSIONS, 30)),
        ("1h", "RAW_UNADJUSTED", hourly),
        ("1d", "RAW_UNADJUSTED", len(APPROVED_CURRENT_SESSIONS)),
    )
    expected_per_ticker = sum(count for _, _, count in counts)
    return replace(
        full,
        adjusted_sessions=APPROVED_CURRENT_SESSIONS,
        raw_sessions=APPROVED_CURRENT_SESSIONS,
        one_minute_sessions=APPROVED_CURRENT_SESSIONS,
        weekly_sessions=weekly,
        raw_start=APPROVED_CURRENT_SESSIONS[0],
        raw_end=APPROVED_CURRENT_SESSIONS[-1],
        one_minute_start=APPROVED_CURRENT_SESSIONS[0],
        one_minute_end=APPROVED_CURRENT_SESSIONS[-1],
        weekly_start=weekly[0],
        weekly_end=weekly[-1],
        expected_counts_per_ticker=counts,
        expected_provider_requests=APPROVED_REQUEST_CEILING,
        expected_facts_per_ticker=expected_per_ticker,
        expected_total_facts=expected_per_ticker * len(APPROVED_TICKERS),
    )


@dataclass(frozen=True, slots=True)
class ProviderGapResult:
    facts: tuple
    request_evidence_by_segment: dict[UUID, RecordedProviderRequest]
    logical_provider_requests: int
    external_provider_requests: int
    cache_hits: int
    response_bytes: int


class AccountingPolygonTransport:
    """Record each bounded aggregate request without exposing its authorization header."""

    def __init__(
        self,
        delegate,
        *,
        cohort_id: UUID,
        clock,
        request_ceiling: int = APPROVED_REQUEST_CEILING,
        cache_directory: Path | None = None,
        request_identity_suffix: str = "",
    ) -> None:
        self.delegate = delegate
        self.cohort_id = cohort_id
        self.clock = clock
        self.request_ceiling = request_ceiling
        self.requests: list[RecordedProviderRequest] = []
        self.external_requests = 0
        self.cache_hits = 0
        self.cache_directory = cache_directory.resolve() if cache_directory is not None else None
        self.request_identity_suffix = request_identity_suffix.strip()
        if self.cache_directory is not None:
            self.cache_directory.mkdir(parents=True, exist_ok=True)

    def _cache_paths(self, url, params):
        identity = canonical_sha256({
            "url": url,
            "params": sorted((str(key), str(value)) for key, value in params.items()),
        })
        assert self.cache_directory is not None
        return (
            self.cache_directory / f"{identity}.body",
            self.cache_directory / f"{identity}.meta.json",
        )

    def get(self, url, params):
        if len(self.requests) >= self.request_ceiling:
            raise RuntimeError("approved V2-10 provider request ceiling reached")
        started = self.clock().astimezone(timezone.utc)
        response = None
        body_path = metadata_path = None
        if self.cache_directory is not None:
            body_path, metadata_path = self._cache_paths(url, params)
            if body_path.is_file() and metadata_path.is_file():
                metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
                response = PolygonHttpResponse(
                    int(metadata["status_code"]),
                    body_path.read_bytes(),
                    tuple(tuple(row) for row in metadata["headers"]),
                )
                self.cache_hits += 1
        if response is None:
            response = self.delegate.get(url, params)
            self.external_requests += 1
            if body_path is not None and metadata_path is not None and response.status_code == 200:
                temporary_body = body_path.with_suffix(".body.tmp")
                temporary_metadata = metadata_path.with_suffix(".json.tmp")
                temporary_body.write_bytes(response.body)
                temporary_metadata.write_text(json.dumps({
                    "status_code": response.status_code,
                    "headers": response.headers,
                }, sort_keys=True), encoding="utf-8")
                temporary_body.replace(body_path)
                temporary_metadata.replace(metadata_path)
        completed = self.clock().astimezone(timezone.utc)
        scope = {
            "url": url,
            "params": sorted((str(key), str(value)) for key, value in params.items()),
            "attempt": 1,
            "identity_suffix": self.request_identity_suffix,
        }
        request_sha256 = canonical_sha256(scope)
        request_id = uuid5(
            NAMESPACE_URL,
            f"v2-10-provider-request:{self.cohort_id}:{request_sha256}",
        )
        try:
            payload = json.loads(response.body)
            row_count = len(payload.get("results", ())) if isinstance(payload, dict) else 0
        except (TypeError, ValueError, json.JSONDecodeError):
            row_count = 0
        ok = response.status_code == 200
        reservation = ProviderRequestReservation(
            request_id=request_id,
            account_alias="MASSIVE_DEVELOPER",
            lane=ProviderRequestLane.EQUITY_HISTORY,
            endpoint_class="EQUITY_HISTORY",
            request_sha256=request_sha256,
            reserved_at=started,
            cohort_id=self.cohort_id,
        )
        completion = ProviderRequestCompletion(
            state=ProviderRequestState.COMPLETE if ok else ProviderRequestState.FAILED,
            started_at=started,
            completed_at=completed,
            outcome=ProviderRequestOutcome.OK if ok else (
                ProviderRequestOutcome.HTTP_429
                if response.status_code == 429
                else ProviderRequestOutcome.HTTP_ERROR
            ),
            http_status=response.status_code,
            response_bytes=len(response.body),
            row_count=row_count,
            pages_fetched=1,
            price_basis=(
                PriceBasis.ADJUSTED_BAR_START_V1
                if str(params.get("adjusted", "")).lower() == "true"
                else PriceBasis.RAW_FINALIZED_MINUTE_CLOSE_V1
            ),
            completeness=(
                RequestCompleteness.COMPLETE
                if ok
                else RequestCompleteness.INCOMPLETE
            ),
            failure_code=None if ok else f"HTTP_{response.status_code}",
        )
        self.requests.append(RecordedProviderRequest(reservation, completion))
        return response


def _accounting_transport(source) -> AccountingPolygonTransport:
    transport = source.transport
    accounting = transport
    while not isinstance(accounting, AccountingPolygonTransport):
        accounting = getattr(accounting, "delegate", None)
        if accounting is None:
            raise TypeError("provider gap source lacks request accounting")
    return accounting


def _single_request(source, operation):
    accounting = _accounting_transport(source)
    before = len(accounting.requests)
    facts = tuple(operation())
    if len(accounting.requests) != before + 1:
        raise RuntimeError("approved provider operation did not use exactly one request")
    evidence = accounting.requests[-1]
    segments = {fact.revision.ingestion_segment_id for fact in facts}
    if None in segments or len(segments) != 1:
        raise ValueError("approved provider operation changed segment lineage")
    return facts, segments.pop(), evidence


def collect_approved_provider_gaps(
    source: PolygonEquityValidationHistorySource,
) -> ProviderGapResult:
    if (
        source.universe != EQUITY_VALIDATION_UNIVERSE
        or source.limits.maximum_requests != APPROVED_REQUEST_CEILING
        or source.limits.maximum_pages_per_request != 1
        or source.limits.maximum_retries_per_request != 0
        or source.limits.maximum_workers != 1
    ):
        raise ValueError("provider gap source changed the approved request bounds")
    plan = build_approved_gap_plan()
    observed_at = source.clock().astimezone(timezone.utc)
    facts = []
    evidence_by_segment = {}
    for identity in source.identities:
        daily, segment, evidence = _single_request(
            source,
            lambda identity=identity: source._collect_adjusted_daily_identity(
                identity, plan=plan, observed_at=observed_at),
        )
        evidence_by_segment[segment] = evidence
        weekly, segment, evidence = _single_request(
            source,
            lambda identity=identity: source._collect_native_weekly_identity(
                identity, plan=plan, observed_at=observed_at),
        )
        evidence_by_segment[segment] = evidence
        one, segment, evidence = _single_request(
            source,
            lambda identity=identity: source._collect_native_one_minute_identity(
                identity, plan=plan, observed_at=observed_at),
        )
        evidence_by_segment[segment] = evidence
        five, five_segment, five_evidence = _single_request(
            source,
            lambda identity=identity: source._collect_native_five_minute_identity(
                identity, plan=plan, observed_at=observed_at),
        )
        evidence_by_segment[five_segment] = five_evidence
        derived = []
        for interval in ("15m", "30m", "1h", "1d"):
            segment = uuid5(
                NAMESPACE_URL,
                f"v2-10-gap-derived:{source.cohort_id}:{identity.ticker}:{interval}:{plan.sha256}",
            )
            interval_facts = derive_realtime_interval_facts(
                identity.security,
                five,
                target_interval=interval,
                observed_at=observed_at,
                ingestion_segment_id=segment,
            )
            evidence_by_segment[segment] = five_evidence
            derived.extend(interval_facts)
        facts.extend((*daily, *weekly, *one, *five, *derived))
    if (
        source.provider_requests != APPROVED_REQUEST_CEILING
        or len(facts) != plan.expected_total_facts
    ):
        raise RuntimeError("approved provider gap request or fact count changed")
    accounting = _accounting_transport(source)
    return ProviderGapResult(
        tuple(facts),
        evidence_by_segment,
        source.provider_requests,
        accounting.external_requests,
        accounting.cache_hits,
        source.response_bytes,
    )


def load_retained_v1_facts(
    cursor,
    *,
    cohort_id: UUID,
    session_date: date = APPROVED_SESSION_DATE,
) -> tuple:
    plan = build_realtime_equity_portal_history_plan(session_date)
    expected_sessions = {
        ("1d", "ADJUSTED"): set(plan.adjusted_sessions),
        ("1wk", "ADJUSTED"): set(plan.weekly_sessions),
        ("1mo", "ADJUSTED"): set(plan.monthly_sessions),
        ("1m", "RAW_UNADJUSTED"): set(plan.one_minute_sessions),
        ("5m", "RAW_UNADJUSTED"): set(plan.raw_sessions),
        ("15m", "RAW_UNADJUSTED"): set(plan.raw_sessions),
        ("30m", "RAW_UNADJUSTED"): set(plan.raw_sessions),
        ("1h", "RAW_UNADJUSTED"): set(plan.raw_sessions),
        ("1d", "RAW_UNADJUSTED"): set(plan.raw_sessions),
    }
    all_sessions = set().union(*expected_sessions.values())
    cursor.execute(
        "SELECT revision.bar_revision_id,current.cohort_id,revision.security_id,"
        "revision.instrument_id,revision.ticker,revision.interval,revision.session_date,"
        "revision.session_scope,revision.adjustment_basis,revision.bar_start,revision.bar_end,"
        "revision.open_price AS open,revision.high_price AS high,revision.low_price AS low,"
        "revision.close_price AS close,revision.volume,revision.vwap,revision.transaction_count,"
        "revision.source_kind,revision.availability_mode,revision.is_final,"
        "revision.observed_at,revision.replay_available_at,revision.provider_published_at,"
        "revision.ingestion_segment_id,revision.source_revision_ids,revision.payload_sha256 "
        "FROM rt.bar_current AS current "
        "JOIN rt.bar_revisions AS revision ON revision.bar_revision_id=current.bar_revision_id "
        "AND revision.cohort_id=current.cohort_id AND revision.provider_tier=current.provider_tier "
        "WHERE current.cohort_id=%s AND current.provider_tier='MASSIVE_DEVELOPER' "
        "AND revision.ticker=ANY(%s) AND revision.session_date BETWEEN %s AND %s "
        "AND revision.is_final ORDER BY revision.ticker,revision.interval,"
        "revision.adjustment_basis,revision.bar_start,revision.bar_revision_id",
        (
            str(cohort_id),
            list(APPROVED_TICKERS),
            min(all_sessions),
            max(all_sessions),
        ),
    )
    inputs = SimpleNamespace(provider_tier=ProviderTier.MASSIVE_DEVELOPER)
    facts = []
    for row in cursor.fetchall():
        family = (row["interval"], row["adjustment_basis"])
        if row["session_date"] in expected_sessions.get(family, ()):
            facts.append(_fact(row, inputs))
    return tuple(facts)


def load_retained_v1_lineage_support(cursor, facts) -> tuple:
    selected = {fact.revision.bar_revision_id for fact in facts}
    referenced = {
        source
        for fact in facts
        for source in fact.revision.source_bar_revision_ids
    }
    missing = tuple(sorted(referenced - selected, key=str))
    if not missing:
        return ()
    cursor.execute(
        "SELECT revision.bar_revision_id,revision.cohort_id,revision.security_id,"
        "revision.instrument_id,revision.ticker,revision.interval,revision.session_date,"
        "revision.session_scope,revision.adjustment_basis,revision.bar_start,revision.bar_end,"
        "revision.open_price AS open,revision.high_price AS high,revision.low_price AS low,"
        "revision.close_price AS close,revision.volume,revision.vwap,revision.transaction_count,"
        "revision.source_kind,revision.availability_mode,revision.is_final,"
        "revision.observed_at,revision.replay_available_at,revision.provider_published_at,"
        "revision.ingestion_segment_id,revision.source_revision_ids,revision.payload_sha256 "
        "FROM rt.bar_revisions AS revision "
        "WHERE revision.bar_revision_id=ANY(%s::uuid[]) "
        "ORDER BY revision.ticker,revision.interval,revision.bar_start,revision.bar_revision_id",
        ([str(value) for value in missing],),
    )
    inputs = SimpleNamespace(provider_tier=ProviderTier.MASSIVE_DEVELOPER)
    support = tuple(_fact(row, inputs) for row in cursor.fetchall())
    if (
        len(support) != len(missing)
        or {fact.revision.bar_revision_id for fact in support} != set(missing)
        or any(
            not fact.revision.is_final
            or fact.revision.source_kind.value != "NATIVE_REST"
            for fact in support
        )
    ):
        raise ValueError("retained V1 lineage support is incomplete or not immutable native history")
    return support


def validate_exact_backfill_facts(
    facts,
    *,
    session_date: date = APPROVED_SESSION_DATE,
    lineage_support=(),
) -> dict:
    plan = build_realtime_equity_portal_history_plan(session_date)
    calendar = exchange_calendars.get_calendar("XNYS")
    sessions = {
        ("1d", "ADJUSTED"): plan.adjusted_sessions,
        ("1wk", "ADJUSTED"): plan.weekly_sessions,
        ("1mo", "ADJUSTED"): plan.monthly_sessions,
        ("1m", "RAW_UNADJUSTED"): plan.one_minute_sessions,
        ("5m", "RAW_UNADJUSTED"): plan.raw_sessions,
        ("15m", "RAW_UNADJUSTED"): plan.raw_sessions,
        ("30m", "RAW_UNADJUSTED"): plan.raw_sessions,
        ("1h", "RAW_UNADJUSTED"): plan.raw_sessions,
        ("1d", "RAW_UNADJUSTED"): plan.raw_sessions,
    }
    observed = {}
    all_facts = (*facts, *lineage_support)
    revision_ids = {fact.revision.bar_revision_id for fact in all_facts}
    if len(revision_ids) != len(all_facts):
        raise ValueError("V2 backfill facts repeat a bar revision")
    for fact in facts:
        revision = fact.revision
        family = (
            revision.interval,
            "ADJUSTED" if revision.adjusted else "RAW_UNADJUSTED",
        )
        key = (revision.ticker, family, revision.session_date)
        observed[key] = observed.get(key, 0) + 1
        if (
            revision.source_kind.value == "DERIVED"
            and (
                not revision.source_bar_revision_ids
                or any(source not in revision_ids for source in revision.source_bar_revision_ids)
            )
        ):
            raise ValueError("V2 backfill derived lineage is incomplete")
    expected_total = 0
    for ticker in APPROVED_TICKERS:
        for family, expected_sessions in sessions.items():
            for market_date in expected_sessions:
                if family[0] in {"1d", "1wk", "1mo"}:
                    expected = 1
                else:
                    minutes = int(
                        (
                            calendar.session_close(str(market_date))
                            - calendar.session_open(str(market_date))
                        ).total_seconds()
                        // 60
                    )
                    width = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "1h": 60}[family[0]]
                    expected = (minutes + 59) // 60 if family[0] == "1h" else minutes // width
                actual = observed.get((ticker, family, market_date), 0)
                if actual != expected:
                    raise ValueError(
                        f"V2 backfill coverage mismatch: {ticker} {family} {market_date} "
                        f"expected={expected} actual={actual}"
                    )
                expected_total += expected
    if len(facts) != expected_total:
        raise ValueError("V2 backfill contains facts outside the exact history contract")
    return {
        "tickers": len(APPROVED_TICKERS),
        "facts": len(facts),
        "bar_revisions": len(revision_ids),
        "lineage_support_revisions": len(lineage_support),
        "derived_lineage_complete": True,
    }
