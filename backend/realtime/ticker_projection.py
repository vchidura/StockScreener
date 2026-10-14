"""Provider-free Ticker Workspace contracts over retained realtime evidence."""
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal
from functools import lru_cache
import json
from math import isfinite
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5
from zoneinfo import ZoneInfo

import exchange_calendars

from equity.domain import (
    BarAvailabilityMode, BarSessionScope, BarSourceKind, EquityBarRevision,
    EvidenceType, SecurityReferenceRevision,
)
from equity.materialization import SETUP_VERSION
from equity.ticker_contracts import (
    PATTERN_WATCH_INTERVALS, build_ticker_confluence_zones, setup_summary_row,
    sort_pattern_watch_rows,
)
from research.forming_patterns import summarize_cross_frame_patterns

from .contracts import canonical_sha256
from .equity_bar_adapter import RealtimeEquityBarFact
from .equity_interval_derivation import derive_realtime_interval_facts
from .equity_materialization_adapter import (
    RealtimeEquityMaterializationResult,
    materialize_realtime_equity_evidence,
)
from .equity_portal_history import build_realtime_equity_portal_history_plan
from .lineage_head import (
    assemble_lineage_head, group_head, reference_set, require_bounded_manifest, require_bounded_payload,
    revision_chain_sha256, rows_identity, workspace_set,
)
from .portal_projection import build_portal_snapshot
from .screening_projection import RealtimeScreeningInput, produce_realtime_screening
from .ticker_references import chart_basis, chart_group_key, evidence_ref


TICKER_WORKSPACE_VERSION = "TICKER_WORKSPACE_V1"
TICKER_WORKSPACE_SUMMARY_VERSION = "TICKER_WORKSPACE_SUMMARY_V1"
PATTERN_WATCH_PUBLICATION_VERSION = "PATTERN_WATCH_V1"
SETUP_INTERVALS = ("30m", "1h", "1d", "1wk", "1mo")
# The chart bodies the summary reads. They are not part of the stored workspace payload, which carries only
# references; the projection hands them to the summary producer in process.
SUMMARY_CHART_INTERVALS = ("1d", "1wk")
MISSING_PRODUCERS = {
    "1m": "NATIVE_1M_HISTORY_NOT_RETAINED",
    "fundamentals": "REALTIME_FUNDAMENTAL_REPORT_PRODUCER_UNAVAILABLE",
    "profile_fields": "REALTIME_EXTENDED_SECURITY_REFERENCE_UNAVAILABLE",
    "calendar": "REALTIME_TICKER_CALENDAR_PROJECTION_UNAVAILABLE",
}

CALENDAR_MODEL_VERSION = "realtime_ticker_calendar_v1"
OPTIONS_DATA_TIER = "15-MINUTE DELAYED RESEARCH DATA"


@dataclass(frozen=True, slots=True)
class RealtimeTickerWorkspaceProjection:
    """`snapshots` are the compact, by-reference workspaces that get hashed and stored. `summary_charts`
    (ticker -> interval -> chart points, only `SUMMARY_CHART_INTERVALS`) is the in-process side of what
    they reference: it feeds the summary producer and is never stored. `screening_generation` is the
    screening generation the workspaces' `cohort.generation` names; the compute stage persists it so the
    reader's current mode can resolve the cohort."""

    snapshots: tuple
    materializations: tuple[RealtimeEquityMaterializationResult, ...]
    pattern_materializations: tuple[RealtimeEquityMaterializationResult, ...] = ()
    summary_charts: dict = field(default_factory=dict)
    screening_generation: object = None


class TickerProjectionState:
    """In-process incremental state for the Ticker Workspace projection (V2-9, R0).

    A content-addressed memo, never a source of truth: every entry is keyed by a digest of the exact
    inputs it was computed from, and the projection recomputes that key from the current inputs each
    cycle, so a stale entry cannot be returned. Entries unused by the latest successful cycle are
    dropped, which bounds memory to one cycle's working set; a failed cycle keeps the previous set
    untouched. One projection runs at a time per deployment: the state is not thread-safe.
    """

    def __init__(self):
        self._previous = {}
        self._current = {}
        self.stats = Counter()

    @contextmanager
    def cycle(self):
        self._current = {}
        self.stats = Counter()
        completed = False
        try:
            yield self
            completed = True
        finally:
            if completed:
                self._previous = self._current
            self._current = {}

    def known(self, kind, key):
        slot = (kind, key)
        return slot in self._current or slot in self._previous

    def remember(self, kind, key, build):
        slot = (kind, key)
        if slot in self._current:
            return self._current[slot]
        if slot in self._previous:
            value = self._previous[slot]
            self.stats[f"{kind}_reused"] += 1
        else:
            value = build()
            self.stats[f"{kind}_built"] += 1
        self._current[slot] = value
        return value

    def facts(self, rows, inputs):
        """Bar facts for rows, rebuilding only revisions not seen in the previous cycle. A revision is
        immutable, so (tier, revision id, payload hash) fully identifies the fact."""
        current, previous, tier = self._current, self._previous, inputs.provider_tier.value
        built = reused = 0
        facts = []
        for row in rows:
            slot = ("fact", tier, row["bar_revision_id"], row["payload_sha256"])
            fact = current.get(slot)
            if fact is None:
                fact = previous.get(slot)
                if fact is None:
                    fact = _fact(row, inputs)
                    built += 1
                else:
                    reused += 1
                current[slot] = fact
            facts.append(fact)
        self.stats["fact_built"] += built
        self.stats["fact_reused"] += reused
        return tuple(facts)

    def __len__(self):
        return len(self._previous)


def _security(member):
    def decimal_value(name):
        value = member.get(name)
        return Decimal(str(value)) if value is not None else None

    return SecurityReferenceRevision(
        security_revision_id=UUID(str(member["security_revision_id"])),
        security_id=UUID(str(member["security_id"])), ticker=member["ticker"],
        active=bool(member.get("active", True)),
        company_name=member["company_name"], security_type=member["security_type"],
        primary_exchange=member["exchange"], effective_from=member["effective_from"],
        observed_at=member["observed_at"], payload_sha256=member["payload_sha256"],
        source="REALTIME_REFERENCE_V1", raw_payload_json="{}",
        cik=member.get("cik"), composite_figi=member.get("composite_figi"),
        share_class_figi=member.get("share_class_figi"), sic_code=member.get("sic_code"),
        sic_description=member.get("sic_description"), sector=member.get("sector"),
        industry=member.get("industry"), list_date=member.get("list_date"),
        delisted_date=member.get("delisted_date"), weighted_shares=decimal_value("weighted_shares"),
        free_float=decimal_value("free_float"),
        free_float_percent=float(member["free_float_percent"]) if member.get("free_float_percent") is not None else None,
        market_cap=decimal_value("market_cap"),
    )


def _uuid(value):
    # Rows read from the database already carry UUID objects; only other representations are parsed.
    return value if type(value) is UUID else UUID(str(value))


def _fact(row, inputs):
    return RealtimeEquityBarFact(
        cohort_id=_uuid(row["cohort_id"]), provider_tier=inputs.provider_tier,
        instrument_id=row["instrument_id"],
        journal_generation=row.get("journal_generation"), stream_session_id=row.get("stream_session_id"),
        source_offset=row.get("source_offset"),
        revision=EquityBarRevision(
            bar_revision_id=_uuid(row["bar_revision_id"]),
            security_id=_uuid(row["security_id"]), ticker=row["ticker"],
            interval=row["interval"], session_date=row["session_date"],
            bar_start=row["bar_start"], bar_end=row["bar_end"],
            open_price=Decimal(str(row["open"])), high_price=Decimal(str(row["high"])),
            low_price=Decimal(str(row["low"])), close_price=Decimal(str(row["close"])),
            volume=Decimal(str(row["volume"])),
            vwap=Decimal(str(row["vwap"])) if row.get("vwap") is not None else None,
            transaction_count=row.get("transaction_count"), source_kind=BarSourceKind(row["source_kind"]),
            availability_mode=BarAvailabilityMode(row["availability_mode"]),
            is_final=row["is_final"], system_observed_at=row["observed_at"],
            replay_available_at=row["replay_available_at"],
            provider_published_at=row.get("provider_published_at"),
            adjusted=row["adjustment_basis"] == "ADJUSTED", payload_sha256=row["payload_sha256"],
            ingestion_segment_id=row["ingestion_segment_id"],
            source_bar_revision_ids=tuple([_uuid(value) for value in row["source_revision_ids"]]),
            session_scope=BarSessionScope(row["session_scope"]),
        ),
    )


def _close(left, right, rtol=1e-8, atol=1e-8):
    """`numpy.isclose` for two floats (finite values by tolerance, others only when equal)."""
    if isfinite(left) and isfinite(right):
        return abs(left - right) <= atol + rtol * abs(right)
    return left == right


def _same_aggregate(actual, expected):
    left, right = actual.revision, expected.revision
    return (
        (left.bar_start, left.bar_end, left.adjusted, left.source_bar_revision_ids)
        == (right.bar_start, right.bar_end, right.adjusted, right.source_bar_revision_ids)
        and _close(float(left.open_price), float(right.open_price))
        and _close(float(left.high_price), float(right.high_price))
        and _close(float(left.low_price), float(right.low_price))
        and _close(float(left.close_price), float(right.close_price))
        and _close(float(left.volume), float(right.volume))
    )


def _chart_point(fact):
    bar = fact.revision
    return dict(
        time=int(bar.bar_start.timestamp()), open=round(float(bar.open_price), 2),
        high=round(float(bar.high_price), 2), low=round(float(bar.low_price), 2),
        close=round(float(bar.close_price), 2), volume=int(bar.volume),
    )


def _manifest_value(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {key: _manifest_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_manifest_value(item) for item in value]
    return value


def _calendar_projection(security, inputs, market_events, market_event_coverage):
    cutoff = inputs.cutoff
    decision_start = cutoff - timedelta(days=1)
    decision_end = cutoff + timedelta(hours=72)
    horizon_end = cutoff + timedelta(days=120)
    security_id = str(security.security_id)
    events = []
    for row in market_events:
        row_security_id = str(row["security_id"]) if row.get("security_id") is not None else None
        if row_security_id not in {None, security_id}:
            continue
        if (
            row["observed_at"] > cutoff or row["received_at"] > cutoff
            or row["valid_until"] <= cutoff
        ):
            raise ValueError("Ticker calendar event availability drift")
        if not decision_start.date() <= row["event_date"] <= horizon_end.date():
            continue
        event_timezone = ZoneInfo(row["event_timezone"])
        scheduled_time = datetime.combine(row["event_date"], time(12), event_timezone).astimezone(timezone.utc)
        events.append(dict(
            market_event_id=str(row["revision_id"]), event_type=row["event_type"],
            affected_underlying=security.ticker if row_security_id is not None else None,
            scheduled_time=scheduled_time.isoformat(), source=row["source"], source_key=row["event_key"],
            announcement_time=row["announced_at"].isoformat() if row.get("announced_at") else None,
            source_observed_at=row["observed_at"].isoformat(),
            first_observed_at=row["received_at"].isoformat(), confidence=row["time_precision"],
            status=row["status"], payload_sha256=row["payload_sha256"],
        ))
    events.sort(key=lambda row: (row["scheduled_time"], row["event_type"], row["source_key"]))

    coverage = []
    coverage_verdicts = {}
    for row in market_event_coverage:
        row_security_id = str(row["security_id"]) if row.get("security_id") is not None else None
        if row_security_id not in {None, security_id}:
            continue
        if row["checked_at"] > cutoff or row["valid_until"] <= cutoff:
            raise ValueError("Ticker calendar coverage availability drift")
        if row["window_start"] > decision_end.date() or row["window_end"] < decision_start.date():
            continue
        affected_underlying = security.ticker if row_security_id is not None else None
        coverage.append(dict(
            coverage_id=str(row["coverage_id"]), event_type=row["event_type"],
            affected_underlying=affected_underlying, window_start=str(row["window_start"]),
            window_end=str(row["window_end"]), source=row["source"],
            source_key=str(row["coverage_id"]), source_observed_at=row["checked_at"].isoformat(),
            first_observed_at=row["checked_at"].isoformat(), payload_sha256=row["evidence_sha256"],
        ))
        coverage_verdicts.setdefault(row["event_type"], set()).add(row["verdict"])
    coverage.sort(key=lambda row: (row["event_type"], row["source"], row["coverage_id"]))

    decision_events = [
        row for row in events
        if row["status"] != "CANCELED"
        and datetime.fromisoformat(row["scheduled_time"]).date() <= decision_end.date()
    ]

    def state(event_type, *, not_applicable=False):
        if not_applicable:
            return "NOT_APPLICABLE"
        verdicts = coverage_verdicts.get(event_type, set())
        if not verdicts or verdicts == {"UNAVAILABLE"}:
            return "UNAVAILABLE"
        if any(row["event_type"] == event_type for row in decision_events):
            return "BLOCKED"
        return "CLEAR"

    asset_type = "ETF" if security.security_type == "ETF" else "STOCK"
    earnings_state = state("EARNINGS", not_applicable=asset_type == "ETF")
    fed_state = state("FED_RATE_DECISION")
    available = fed_state != "UNAVAILABLE" and (asset_type == "ETF" or earnings_state != "UNAVAILABLE")
    observed_values = [
        datetime.fromisoformat(row["first_observed_at"])
        for row in events + coverage
    ]
    sources = sorted({row["source"] for row in events + coverage})
    return dict(
        available=available,
        reason=None if available else "EVENT_CALENDAR_COVERAGE_INCOMPLETE",
        data_tier=OPTIONS_DATA_TIER, generated_at=cutoff.isoformat(), as_of=cutoff.isoformat(),
        observed_at=max(observed_values).isoformat() if observed_values else None,
        policy_sha256=canonical_sha256((CALENDAR_MODEL_VERSION, events, coverage)),
        model_version=CALENDAR_MODEL_VERSION, provider_fetch_performed=False,
        execution_permission=False,
        data=dict(
            underlyer=security.ticker, asset_type=asset_type,
            configured_source=", ".join(sources) if sources else None,
            decision_window_start=decision_start.isoformat(), decision_window_end=decision_end.isoformat(),
            earnings_state=earnings_state, fed_state=fed_state, events=events, coverage=coverage,
        ),
    )


def _fundamental_projection(security, inputs, fundamental_reports):
    reports = []
    for row in fundamental_reports:
        if str(row["security_id"]) != str(security.security_id):
            continue
        if row["availability_time"] > inputs.cutoff or row["observed_at"] > inputs.cutoff:
            raise ValueError("Ticker fundamental report availability drift")
        metrics = dict(row["metrics"])
        reports.append(dict(
            fundamental_report_id=str(row["report_id"]), timeframe=row["timeframe"],
            fiscal_year=row.get("fiscal_year"), fiscal_quarter=row.get("fiscal_quarter"),
            period_end=str(row["period_end"]),
            filing_date=str(row["filing_date"]) if row.get("filing_date") else None,
            availability_time=row["availability_time"].isoformat(), source=row["source"],
            quality_codes=list(row.get("quality_codes") or ()), **metrics,
        ))
    reports.sort(
        key=lambda row: (row["availability_time"], row["period_end"], row["fundamental_report_id"]),
        reverse=True,
    )
    return reports


def _has_fundamental_coverage(security, inputs, fundamental_coverage):
    rows = [
        row for row in fundamental_coverage
        if str(row["security_id"]) == str(security.security_id)
    ]
    if any(row["checked_at"] > inputs.cutoff or row["valid_until"] <= inputs.cutoff for row in rows):
        raise ValueError("Ticker fundamental coverage availability drift")
    return bool(rows)


def materialize_ticker_interval(
    inputs, security, bars, confirmations=(), *, interval, basis, policy_sha256, state=None,
):
    observed_at = max(fact.revision.system_observed_at for fact in (*bars, *confirmations))
    identity = canonical_sha256((
        inputs.cohort_id, inputs.provider_tier.value, security.security_revision_id,
        interval, basis, policy_sha256,
        tuple(fact.revision.bar_revision_id for fact in bars),
        tuple(fact.revision.bar_revision_id for fact in confirmations), observed_at,
    ))

    def build():
        return materialize_realtime_equity_evidence(
            analysis_run_id=uuid5(NAMESPACE_URL, f"ticker-workspace:{identity}"),
            security=security, bars=bars, confirmation_bars=confirmations, observed_at=observed_at,
        )

    # The identity names the exact bars and security revision; the chains add each bar's payload
    # hash, so an equal key is an equal result.
    key = ":".join((
        identity,
        revision_chain_sha256((f.revision.bar_revision_id, f.revision.payload_sha256) for f in bars),
        revision_chain_sha256((f.revision.bar_revision_id, f.revision.payload_sha256) for f in confirmations),
    ))
    result = build() if state is None else state.remember("materialization", key, build)
    return identity, result


def completed_ticker_decision_windows(session):
    calendar = exchange_calendars.get_calendar("XNYS")
    if not calendar.is_session(str(session)):
        raise ValueError("Ticker evidence history requires an XNYS session")
    opened = calendar.session_open(str(session)).to_pydatetime()
    closed = calendar.session_close(str(session)).to_pydatetime()
    windows = []
    for interval, width in (("30m", 30), ("1h", 60)):
        end = min(opened + timedelta(minutes=width), closed)
        while True:
            windows.append((interval, end))
            if end == closed:
                break
            end = min(end + timedelta(minutes=width), closed)
    windows.append(("1d", closed))
    following = calendar.next_session(str(session)).date()
    if following.isocalendar()[:2] != session.isocalendar()[:2]:
        windows.append(("1wk", closed))
    if (following.year, following.month) != (session.year, session.month):
        windows.append(("1mo", closed))
    return tuple(sorted(windows, key=lambda row: (row[1], SETUP_INTERVALS.index(row[0]))))


def _fact_chain(group):
    return revision_chain_sha256((fact.revision.bar_revision_id, fact.revision.payload_sha256) for fact in group)


def _session_slices(group):
    slices = {}
    for fact in group:
        slices.setdefault(fact.revision.session_date, []).append(fact)
    return slices


class _DerivationVerifier:
    """Verifies one ticker's stored derived intervals against their source bars, reusing verdicts.

    Intraday derivation (5m to 15m, 30m, 1h and 1d) is computed per session, so a verdict is keyed by
    (security revision, interval, session, source chain, stored chain): a new bar re-derives only its
    own session, never the retained history. Weekly derivation spans sessions and keeps one verdict
    per whole group. A verdict that raised is never stored.
    """

    def __init__(self, state, security, inputs, segment):
        self._state, self._security, self._inputs, self._segment = state, security, inputs, segment
        self._derived = {}
        self._slices = {}

    def _derive(self, interval, sources):
        if not sources:
            return ()
        return derive_realtime_interval_facts(
            self._security, sources, target_interval=interval,
            observed_at=self._inputs.cutoff, ingestion_segment_id=self._segment,
        )

    def _sliced(self, label, group):
        if label not in self._slices:
            by_session = _session_slices(group)
            self._slices[label] = (by_session, {s: _fact_chain(rows) for s, rows in by_session.items()})
        return self._slices[label]

    def _prefetch(self, interval, source_by_session, sessions):
        todo = [session for session in sessions if (interval, session) not in self._derived]
        if not todo:
            return
        derived = self._derive(interval, tuple(fact for s in todo for fact in source_by_session.get(s, ())))
        for session in todo:
            self._derived[(interval, session)] = []
        for fact in derived:
            self._derived.setdefault((interval, fact.revision.session_date), []).append(fact)

    def _sessions_total(self, kind, interval, source_label, sources, stored_label=None, stored=None):
        source_by, source_chain = self._sliced(source_label, sources)
        stored_by, stored_chain = self._sliced(stored_label, stored) if stored is not None else ({}, {})
        sessions = sorted(source_by.keys() | stored_by.keys())
        revision = self._security.security_revision_id
        keys = {s: (revision, interval, s, source_chain.get(s), stored_chain.get(s)) for s in sessions}
        self._prefetch(interval, source_by, [s for s in sessions if not self._state.known(kind, keys[s])])

        def build(session):
            expected = self._derived[(interval, session)]
            if stored is not None:
                rows = stored_by.get(session, ())
                by_start = {fact.revision.bar_start: fact for fact in expected}
                if len({fact.revision.bar_start for fact in rows}) != len(rows):
                    raise ValueError("Ticker interval source windows are duplicated")
                if any(fact.revision.source_kind is not BarSourceKind.DERIVED
                       or fact.revision.bar_start not in by_start
                       or not _same_aggregate(fact, by_start[fact.revision.bar_start]) for fact in rows):
                    raise ValueError(f"Ticker derived interval lineage/aggregate drift: {interval}")
            return len(expected)

        return sum(self._state.remember(kind, keys[s], lambda s=s: build(s)) for s in sessions)

    def windows(self, interval, source_label, sources):
        """Number of complete derived windows the sources support."""
        return self._sessions_total("derived_windows", interval, source_label, sources)

    def verify_sessions(self, interval, source_label, sources, stored_label, stored):
        """Check stored per-session derived bars; returns the expected window count."""
        return self._sessions_total("lineage_session", interval, source_label, sources, stored_label, stored)

    def verify_whole(self, interval, sources, stored):
        def build():
            expected = self._derive(interval, sources)
            by_start = {fact.revision.bar_start: fact for fact in expected}
            if len({fact.revision.bar_start for fact in stored}) != len(stored):
                raise ValueError("Ticker interval source windows are duplicated")
            if any(fact.revision.source_kind is not BarSourceKind.DERIVED
                   or fact.revision.bar_start not in by_start
                   or not _same_aggregate(fact, by_start[fact.revision.bar_start]) for fact in stored):
                raise ValueError(f"Ticker derived interval lineage/aggregate drift: {interval}")
            return len(expected)

        key = (self._security.security_revision_id, interval, _fact_chain(sources), _fact_chain(stored))
        return self._state.remember("lineage", key, build)


def project_realtime_ticker_evidence_history(inputs):
    plan = build_realtime_equity_portal_history_plan(inputs.session, equity_ticker_count=len(inputs.members))
    if any(
        row["bar_end"] > inputs.cutoff or row["observed_at"] > inputs.recorded_by
        or any(row.get(key) is not None and row[key] > inputs.recorded_by
               for key in ("created_at", "replay_available_at", "provider_published_at"))
        for row in inputs.bars
    ):
        raise ValueError("Ticker evidence history exceeds its actual known-by time")
    results = []
    for member in sorted(inputs.members, key=lambda row: row["ticker"]):
        security = _security(member)
        facts = tuple(_fact(row, inputs) for row in inputs.bars if row["ticker"] == security.ticker)
        for interval, market_cutoff in completed_ticker_decision_windows(inputs.session):
            basis = "ADJUSTED" if interval in {"1d", "1wk", "1mo"} else "RAW_UNADJUSTED"
            primary = tuple(sorted((fact for fact in facts
                if fact.revision.interval == interval and fact.revision.adjusted == (basis == "ADJUSTED")
                and fact.revision.bar_end <= market_cutoff), key=lambda fact: fact.revision.bar_start))
            if interval in {"30m", "1h"}:
                primary = primary[-500:]
            elif interval == "1d":
                primary = primary[-len(plan.adjusted_sessions):]
            if len(primary) < 50 or primary[-1].revision.bar_end != market_cutoff:
                raise ValueError(f"Ticker history lacks completed {interval} coverage for {security.ticker}")
            confirmation_interval = {"1h": "1d", "1d": "1h", "1wk": "1d", "1mo": "1wk"}.get(interval)
            confirmations = tuple(sorted((fact for fact in facts
                if fact.revision.interval == confirmation_interval
                and fact.revision.adjusted == (confirmation_interval != "1h")
                and fact.revision.bar_end <= market_cutoff), key=lambda fact: fact.revision.bar_start))
            if confirmation_interval == "1d":
                confirmations = confirmations[-len(plan.adjusted_sessions):]
            results.append(materialize_ticker_interval(
                inputs, security, primary, confirmations, interval=interval, basis=basis,
                policy_sha256=plan.policy_sha256,
            )[1])
    return tuple(results)


@lru_cache(maxsize=1)
def _ticker_code_sha256():
    return canonical_sha256(Path(__file__).read_text(encoding="utf-8"))


def project_realtime_ticker_workspace(
    inputs: RealtimeScreeningInput, *, market_events=(), market_event_coverage=(),
    fundamental_reports=(), fundamental_coverage=(), state: TickerProjectionState | None = None,
):
    """Pass a long-lived `state` to reuse unchanged facts, derivation checks and interval
    materializations across cycles. The result is identical with or without it."""
    state = TickerProjectionState() if state is None else state
    with state.cycle():
        return _project_realtime_ticker_workspace(
            inputs, market_events=market_events, market_event_coverage=market_event_coverage,
            fundamental_reports=fundamental_reports, fundamental_coverage=fundamental_coverage,
            state=state,
        )


def _project_realtime_ticker_workspace(
    inputs, *, market_events, market_event_coverage, fundamental_reports, fundamental_coverage, state,
):
    plan = build_realtime_equity_portal_history_plan(inputs.session, equity_ticker_count=len(inputs.members))
    allowed = {(interval, basis) for interval, basis, _ in plan.expected_counts_per_ticker}
    if any((row["interval"], row["adjustment_basis"]) not in allowed for row in inputs.bars):
        raise ValueError("Ticker retained interval/basis contract drift")
    keys = [str(row["bar_revision_id"]) for row in inputs.bars]
    indexed = dict(zip(keys, inputs.bars))
    screening_sessions = frozenset(plan.adjusted_sessions[-283:])
    selected = {
        key for key, row in indexed.items()
        if row["interval"] == "1d" and row["adjustment_basis"] == "ADJUSTED"
        and row["session_date"] in screening_sessions
        or row["interval"] == "1d" and row["session_date"] >= plan.raw_sessions[-20]
        or row["interval"] == "1h" and row["session_date"] >= plan.raw_sessions[-20]
    }
    pending = list(selected)
    while pending:
        row = indexed[pending.pop()]
        for source in row["source_revision_ids"]:
            key = str(source)
            if key not in indexed:
                raise ValueError("Ticker causal bar evidence is missing")
            if key not in selected:
                selected.add(key)
                pending.append(key)
    # Reuse the established exact-reference, action, split, scale and daily-cohort gates.
    screening = produce_realtime_screening(replace(
        inputs, bars=tuple(row for key, row in zip(keys, inputs.bars) if key in selected),
    ))
    code_sha256 = _ticker_code_sha256()
    adjusted_session_set = frozenset(plan.adjusted_sessions)
    rows_by_ticker = {}
    for row in inputs.bars:
        rows_by_ticker.setdefault(row["ticker"], []).append(row)
    snapshots = []
    materializations = []
    pattern_materializations = []
    summary_charts = {}
    provider_tier, cutoff = inputs.provider_tier.value, inputs.cutoff
    recorded_by = inputs.recorded_by  # known_by, else the cutoff: the bound of every recording clock below
    for member in sorted(inputs.members, key=lambda row: row["ticker"]):
        security = _security(member)
        own = rows_by_ticker.get(security.ticker, [])
        security_text, cohort_text = str(security.security_id), str(inputs.cohort_id)
        for row in own:
            row_security, row_cohort = row["security_id"], row["cohort_id"]
            replay, published, created = (
                row.get("replay_available_at"), row.get("provider_published_at"), row.get("created_at"))
            if (
                (row_security != security.security_id if type(row_security) is UUID else str(row_security) != security_text)
                or (row_cohort != inputs.cohort_id if type(row_cohort) is UUID else str(row_cohort) != cohort_text)
                or row["provider_tier"] != provider_tier
                or row["instrument_id"] != member["instrument_id"]
                or not row["is_final"] or row["session_scope"] != "RTH"
                or row["bar_end"] > cutoff or row["observed_at"] > recorded_by
                or (replay is not None and replay > recorded_by)
                or (published is not None and published > recorded_by)
                or (created is not None and created > recorded_by)
            ):
                raise ValueError(f"Ticker source identity/availability drift: {security.ticker}")
        facts = state.facts(own, inputs)
        if len({fact.revision.bar_revision_id for fact in facts}) != len(facts):
            raise ValueError("Ticker source revisions are duplicated")
        buckets = {}
        for fact in facts:
            buckets.setdefault((fact.revision.interval, fact.revision.adjusted), []).append(fact)
        groups = {
            (interval, basis): tuple(sorted(
                (fact for fact in buckets.get((interval, basis == "ADJUSTED"), ())
                 if (interval, basis) != ("1d", "ADJUSTED")
                 or fact.revision.session_date in adjusted_session_set),
                key=lambda fact: fact.revision.bar_start,
            ))
            for interval, basis, _ in plan.expected_counts_per_ticker
        }
        chains = {
            key: revision_chain_sha256(
                (fact.revision.bar_revision_id, fact.revision.payload_sha256) for fact in group
            ) for key, group in groups.items() if group
        }
        missing = dict(MISSING_PRODUCERS)
        profile_reports = _fundamental_projection(security, inputs, fundamental_reports)
        fundamentals_covered = _has_fundamental_coverage(security, inputs, fundamental_coverage)
        if profile_reports and not fundamentals_covered:
            raise ValueError("Ticker fundamental reports require retained coverage")
        if fundamentals_covered:
            missing.pop("fundamentals")
        available = {}
        five = groups[("5m", "RAW_UNADJUSTED")]
        if five:
            if len(five) != plan.expected_count("5m", "RAW_UNADJUSTED"):
                missing["raw_history"] = "NATIVE_5M_HISTORY_INCOMPLETE"
            elif tuple(sorted({fact.revision.session_date for fact in five})) != plan.raw_sessions:
                raise ValueError("Ticker native 5m sessions drift")
            elif any(fact.revision.source_kind not in {BarSourceKind.NATIVE_REST, BarSourceKind.REALTIME_STREAM}
                     for fact in five):
                raise ValueError("Ticker 5m history must be native")
            else:
                available["5m"] = five
        else:
            missing["raw_history"] = "NATIVE_5M_HISTORY_NOT_RETAINED"
        one = groups[("1m", "RAW_UNADJUSTED")]
        if (
            len(one) == plan.expected_count("1m", "RAW_UNADJUSTED")
            and tuple(sorted({fact.revision.session_date for fact in one})) == plan.one_minute_sessions
            and all(fact.revision.source_kind is BarSourceKind.NATIVE_REST for fact in one)
        ):
            available["1m"] = one
            missing.pop("1m")
        else:
            missing["chart:1m"] = "NATIVE_1M_HISTORY_INCOMPLETE"
        monthly = groups[("1mo", "ADJUSTED")]
        if (
            len(monthly) == plan.expected_count("1mo", "ADJUSTED")
            and tuple(fact.revision.session_date for fact in monthly) == plan.monthly_sessions
            and all(fact.revision.source_kind is BarSourceKind.NATIVE_REST for fact in monthly)
        ):
            available["1mo"] = monthly
        else:
            missing["chart:1mo"] = "NATIVE_ADJUSTED_MONTHLY_HISTORY_INCOMPLETE"
        own_actions = [row for row in inputs.actions if str(row["security_id"]) == str(security.security_id)]
        if any(row["action_type"] == "SPLIT" and plan.raw_start <= row["effective_date"] <= plan.raw_end
               for row in own_actions):
            raise ValueError("Ticker raw split window requires review")
        adjusted = groups[("1d", "ADJUSTED")]
        segment = uuid5(NAMESPACE_URL, f"ticker-lineage-check:{inputs.cohort_id}:{security.ticker}")
        verifier = _DerivationVerifier(state, security, inputs, segment)
        if "5m" in available and verifier.windows("1d", "5m", available["5m"]) != len(plan.raw_sessions):
            raise ValueError("Ticker native 5m session windows are incomplete")
        adjusted_sources = tuple(sorted(buckets.get(("1d", True), ()), key=lambda fact: fact.revision.bar_start))
        for interval, basis, expected_count in plan.expected_counts_per_ticker:
            if (interval, basis) == ("1d", "ADJUSTED") or interval in {"1m", "5m", "1mo"}:
                continue
            stored = groups[(interval, basis)]
            sources = adjusted_sources if basis == "ADJUSTED" else available.get("5m", ())
            if not stored or not sources:
                missing[f"chart:{interval}"] = "RETAINED_INTERVAL_UNAVAILABLE"
                continue
            if interval == "1wk" and all(
                fact.revision.source_kind is BarSourceKind.NATIVE_REST for fact in stored
            ):
                if (
                    len(stored) != expected_count
                    or tuple(fact.revision.session_date for fact in stored) != plan.weekly_sessions
                ):
                    raise ValueError("Ticker native adjusted weekly history drift")
                available[interval] = stored
                continue
            expected_windows = (
                verifier.verify_whole(interval, sources, stored) if basis == "ADJUSTED"
                else verifier.verify_sessions(interval, "5m", sources, f"{interval}:{basis}", stored)
            )
            if len(stored) != expected_count:
                if interval == "1wk" and len(stored) == expected_windows:
                    available[interval] = stored
                    missing["weekly_200"] = "NATIVE_ADJUSTED_WEEKLY_HISTORY_UNAVAILABLE"
                    continue
                missing[f"chart:{interval}"] = "RETAINED_INTERVAL_INCOMPLETE"
                continue
            available[interval] = stored

        if (
            len(adjusted) != plan.expected_count("1d", "ADJUSTED")
            or tuple(fact.revision.session_date for fact in adjusted) != plan.adjusted_sessions
        ):
            raise ValueError("Ticker adjusted daily chart history drift")
        available["1d"] = adjusted

        setups, metrics, errors, evidence_index, pattern_results, channels, pattern_refs = {}, {}, {}, {}, {}, {}, {}

        def compute(interval, bars, confirmations=(), *, basis, persisted):
            # Only evidence that compute_realtime_ticker_cycle persists is referenced: the 5m and 15m
            # pattern views are release-only (their patterns and channels live in the payload itself),
            # so no durable body exists for the evidence rows they materialize.
            identity, result = materialize_ticker_interval(
                inputs, security, bars, confirmations, interval=interval,
                basis=basis, policy_sha256=plan.policy_sha256, state=state,
            )
            if persisted:
                evidence_index[identity] = [evidence_ref(row) for row in result.materialization.evidence]
            return result

        for interval in SETUP_INTERVALS:
            bars = adjusted if interval == "1d" else available.get(interval, ())
            basis = "ADJUSTED" if interval in {"1d", "1wk", "1mo"} else "RAW_UNADJUSTED"
            if len(bars) < 50:
                errors[interval] = "SETUP_HISTORY_SHORT" if bars else "RETAINED_INTERVAL_UNAVAILABLE"
                missing[f"setup:{interval}"] = errors[interval]
                continue
            if interval in {"30m", "1h"}:
                bars = bars[-500:]
            confirmation_interval = {"1h": "1d", "1d": "1h", "1wk": "1d", "1mo": "1wk"}.get(interval)
            confirmations = (
                adjusted if interval == "1wk" else available.get(confirmation_interval, ())
            ) if confirmation_interval else ()
            result = compute(interval, bars, confirmations, basis=basis, persisted=True)
            materializations.append(result)
            setup_evidence = next(row for row in result.materialization.evidence if row.evidence_type is EvidenceType.TRADE_SETUP)
            setups[interval] = json.loads(setup_evidence.payload_json)
            setups[interval]["computed_at"] = inputs.cutoff.isoformat()
            metrics[interval] = dict(
                status="READY", read_mode="MATERIALIZED", source="MATERIALIZED_CURRENT_PROJECTION",
                process_started_at=inputs.cutoff.isoformat(), analysis_run_id=str(setup_evidence.analysis_run_id),
                evidence_id=str(setup_evidence.evidence_id), market_time=setup_evidence.market_time.isoformat(),
                observed_at=setup_evidence.observed_at.isoformat(), published_at=inputs.cutoff.isoformat(),
                is_serveable=True, price_basis=basis,
            )
        for interval in PATTERN_WATCH_INTERVALS:
            bars = available.get(interval, ())
            if len(bars) < 50:
                missing[f"patterns:{interval}"] = "PATTERN_HISTORY_UNAVAILABLE"
                continue
            basis = "ADJUSTED" if interval in {"1d", "1wk", "1mo"} else "RAW_UNADJUSTED"
            result = compute(interval, bars[-300:], basis=basis, persisted=interval in SETUP_INTERVALS)
            if interval in SETUP_INTERVALS:
                pattern_materializations.append(result)
                pattern_refs[interval] = [dict(
                    cohort_id=str(inputs.cohort_id), evidence_id=str(row.evidence_id),
                    evidence_type=row.evidence_type.value, market_time=row.market_time.isoformat(),
                    observed_at=row.observed_at.isoformat(), payload_sha256=row.payload_sha256,
                ) for row in result.materialization.evidence if row.evidence_type in {
                    EvidenceType.FEATURE_SNAPSHOT, EvidenceType.PATTERN_OBSERVATION, EvidenceType.PRICE_CHANNEL,
                }]
            patterns = [json.loads(row.payload_json) for row in result.materialization.evidence
                        if row.evidence_type is EvidenceType.PATTERN_OBSERVATION]
            channel = next((json.loads(row.payload_json) for row in result.materialization.evidence
                            if row.evidence_type is EvidenceType.PRICE_CHANNEL), None)
            last_close = round(float(bars[-1].revision.close_price), 2)
            pattern_results[interval] = dict(
                ticker=security.ticker, interval=interval, status="FORMING_RESEARCH",
                last_close=last_close, patterns=patterns, computed_at=inputs.cutoff.isoformat(),
            )
            channels[interval] = dict(
                ticker=security.ticker, interval=interval, status="CHANNEL_RESEARCH",
                last_close=last_close, channel=channel, computed_at=inputs.cutoff.isoformat(),
            )
        cross_rows = [
            dict(ticker=security.ticker, sector=security.sector, interval=interval,
                 last_close=response["last_close"], pattern=pattern, channel=channels[interval]["channel"])
            for interval, response in pattern_results.items() for pattern in response["patterns"]
        ]
        sort_pattern_watch_rows(cross_rows)
        raw_daily = groups[("1d", "RAW_UNADJUSTED")]
        latest = (available.get("5m") or raw_daily)[-1].revision
        previous = next((fact.revision for fact in reversed(raw_daily) if fact.revision.session_date < latest.session_date), None)
        previous_close = round(float(previous.close_price), 2) if previous else None
        price = round(float(latest.close_price), 2)
        change = round(price - previous_close, 2) if previous_close is not None else None
        screening_row = next(row.payload for row in screening.rows if row.ticker == security.ticker)
        values = screening_row["values"]
        frames = {
            interval: dict(
                status="READY" if interval in setups else "UNAVAILABLE",
                reason=None if interval in setups else errors[interval],
                evidence_id=metrics[interval]["evidence_id"] if interval in setups else None,
                market_time=metrics[interval]["market_time"] if interval in setups else None,
                available_at=inputs.cutoff.isoformat(),
                trend=setups[interval]["ema_alignment"]["primary"] if interval in setups else None,
                momentum=setups[interval]["momentum"]["state"] if interval in setups else None,
                close=setups[interval]["last_close"] if interval in setups else None,
            ) for interval in ("1d", "1h", "30m")
        }
        # A chart is the whole retained history of one lineage-head group, which the manifest's bar_lineage
        # already identifies, so the payload only names the group and the reader composes the points.
        if any(bars is not groups[(interval, chart_basis(interval))] for interval, bars in available.items()):
            raise ValueError("Ticker chart history is not a lineage-head group")
        chart_refs = {interval: chart_group_key(interval) for interval in available}
        summary_charts[security.ticker] = {
            interval: [_chart_point(fact) for fact in available[interval]]
            for interval in SUMMARY_CHART_INTERVALS if interval in available
        }
        payload = dict(
            version=TICKER_WORKSPACE_VERSION, ticker=security.ticker, missing_producers=missing,
            provider_fetch_performed=False, execution_permission=False,
            profile=dict(ticker=security.ticker, security={
                key: (
                    float(getattr(security, key))
                    if isinstance(getattr(security, key), Decimal)
                    else getattr(security, key)
                ) for key in (
                    "company_name", "security_type", "primary_exchange", "sector", "industry",
                    "sic_description", "list_date", "market_cap", "free_float", "free_float_percent",
                    "weighted_shares", "cik", "composite_figi", "share_class_figi",
                )
            }, fundamental_reports=profile_reports, missing_producers=missing),
            calendar=_calendar_projection(security, inputs, market_events, market_event_coverage),
            chart_refs=chart_refs,
            chart_market_times={interval: bars[-1].revision.bar_end.isoformat() for interval, bars in available.items()},
            quote=dict(ticker=security.ticker, price=price, previous_close=previous_close, change=change,
                       change_percent=round(change / previous_close * 100, 2) if previous_close else None,
                       as_of=latest.bar_end.isoformat(), trade_date=str(latest.session_date),
                       source="5m" if latest.interval == "5m" else "daily"),
            multi=dict(ticker=security.ticker, setups=setups, errors=errors, setup_read_metrics=metrics,
                       setup_sources={interval: "MATERIALIZED_CURRENT_PROJECTION" for interval in setups},
                       confluence_zones=build_ticker_confluence_zones({key: value for key, value in setups.items() if key in {"1h", "1d", "1wk"}}),
                       as_of=inputs.cutoff.isoformat()),
            patterns=pattern_results, channels=channels, pattern_evidence_refs=pattern_refs,
            cross_patterns=dict(interval="all", scanned=1, matched_tickers=1 if cross_rows else 0,
                                results=cross_rows, cross_frame=summarize_cross_frame_patterns(cross_rows),
                                computed_at=inputs.cutoff.isoformat(), missing_intervals=sorted(set(PATTERN_WATCH_INTERVALS) - pattern_results.keys())),
            context=dict(
                schema="stock_equity_context_v1", as_of=inputs.cutoff.isoformat(), status="READY",
                cohort=dict(generation=screening.manifest["generation"], universe=screening.manifest["universe"],
                            expected_members=len(inputs.members), eligible_ranked=screening.manifest["rank_population"],
                            session=str(inputs.session), market_time=screening.manifest["market_time"],
                            available_at=inputs.cutoff.isoformat(), ranking="RAW_12_MINUS_1_MOMENTUM_PERCENTILE", status="READY"),
                rows=[dict(security_id=str(security.security_id), ticker=security.ticker,
                           company_name=security.company_name, sector=security.sector,
                           daily=dict(status="READY" if values.get("momentum_percentile") is not None else "UNAVAILABLE",
                                      percentile=values.get("momentum_percentile"), momentum=values.get("momentum_12_1"),
                                      price=values["price"], trend=values.get("discovery_trend"), state=values.get("discovery_state")),
                           frames=frames)], execution_permission=False,
            ),
            evidence_index=evidence_index,
        )
        own_security_id = str(security.security_id)
        input_identity = dict(
            reference=_manifest_value(dict(member)),
            actions=rows_identity(own_actions),
            coverage=rows_identity([row for row in inputs.coverage if str(row["security_id"]) == own_security_id]),
            market_events=rows_identity([
                row for row in market_events
                if row.get("security_id") is None or str(row["security_id"]) == own_security_id
            ]),
            market_event_coverage=rows_identity([
                row for row in market_event_coverage
                if row.get("security_id") is None or str(row["security_id"]) == own_security_id
            ]),
            fundamental_reports=rows_identity([
                row for row in fundamental_reports if str(row["security_id"]) == own_security_id
            ]),
            fundamental_coverage=rows_identity([
                row for row in fundamental_coverage if str(row["security_id"]) == own_security_id
            ]),
            bar_lineage=assemble_lineage_head({
                f"{interval}:{basis}": group_head(
                    count=len(group), first_revision_id=group[0].revision.bar_revision_id,
                    last_revision_id=group[-1].revision.bar_revision_id,
                    first_bar_start=group[0].revision.bar_start, last_bar_end=group[-1].revision.bar_end,
                    chain_sha256=chains[(interval, basis)],
                ) for (interval, basis), group in groups.items() if group
            }),
        )
        source_manifest = dict(
            version=TICKER_WORKSPACE_VERSION, cohort_id=str(inputs.cohort_id),
            provider_tier=inputs.provider_tier.value, source_generation=inputs.source_generation,
            source_manifest_sha256=inputs.source_manifest_sha256,
            # Per ticker: a change to one ticker's bars no longer changes every other ticker's hash.
            input_sha256=canonical_sha256(dict(
                input_identity, session=inputs.session, cutoff=inputs.cutoff,
                history_plan_sha256=plan.sha256,
            )),
            history_policy_sha256=plan.policy_sha256, history_plan_sha256=plan.sha256,
            setup_version=SETUP_VERSION,
            analysis_windows=dict(intraday_setup_bars=500, finalized_pattern_bars=300),
            code_sha256=code_sha256, **input_identity,
        )
        require_bounded_manifest(source_manifest, name=f"Ticker workspace {security.ticker}")
        require_bounded_payload(payload, name=f"Ticker workspace {security.ticker}")
        snapshots.append(build_portal_snapshot(
            cohort_id=inputs.cohort_id, provider_tier=inputs.provider_tier,
            snapshot_type=TICKER_WORKSPACE_VERSION, snapshot_key=security.ticker,
            source_generation=inputs.source_generation, source_manifest=source_manifest,
            payload=payload, generated_at=inputs.cutoff,
        ))
    return RealtimeTickerWorkspaceProjection(
        snapshots=tuple(snapshots), materializations=tuple(materializations),
        pattern_materializations=tuple(pattern_materializations), summary_charts=summary_charts,
        screening_generation=screening,
    )


def produce_realtime_ticker_workspace(
    inputs: RealtimeScreeningInput, *, market_events=(), market_event_coverage=(),
    fundamental_reports=(), fundamental_coverage=(),
):
    return project_realtime_ticker_workspace(
        inputs,
        market_events=market_events,
        market_event_coverage=market_event_coverage,
        fundamental_reports=fundamental_reports,
        fundamental_coverage=fundamental_coverage,
    ).snapshots


def produce_realtime_pattern_watch_publication(inputs, snapshots):
    snapshots = tuple(sorted(snapshots, key=lambda snapshot: snapshot.snapshot_key))
    if (len(snapshots) != len(inputs.members)
            or {snapshot.snapshot_key for snapshot in snapshots} != {member["ticker"] for member in inputs.members}
            or any(snapshot.snapshot_type != TICKER_WORKSPACE_VERSION or snapshot.cohort_id != inputs.cohort_id
                   or snapshot.provider_tier != inputs.provider_tier or snapshot.source_generation != inputs.source_generation
                   for snapshot in snapshots)):
        raise ValueError("Pattern Watch publication requires one exact same-generation workspace per member")
    views = {}
    for interval in PATTERN_WATCH_INTERVALS:
        rows, missing, market_times, analysis_refs = [], [], [], {}
        for snapshot in snapshots:
            payload = snapshot.payload
            patterns = payload["patterns"].get(interval)
            channel = payload["channels"].get(interval)
            if patterns is None or channel is None:
                missing.append(snapshot.snapshot_key)
                continue
            market_times.append(payload["chart_market_times"][interval])
            if interval in SETUP_INTERVALS:
                refs = payload["pattern_evidence_refs"].get(interval, ())
                if sum(ref["evidence_type"] == EvidenceType.FEATURE_SNAPSHOT.value for ref in refs) != 1:
                    raise ValueError("Pattern Watch completed analysis receipt is missing")
                analysis_refs[snapshot.snapshot_key] = list(refs)
            rows.extend(dict(ticker=snapshot.snapshot_key, sector=payload["profile"]["security"].get("sector"),
                             interval=interval, last_close=patterns["last_close"], pattern=pattern, channel=channel["channel"])
                        for pattern in patterns["patterns"])
        sort_pattern_watch_rows(rows)
        views[interval] = dict(
            interval=interval, status="UNAVAILABLE" if missing else "COMPLETE", unavailable_tickers=missing,
            scanned=len(snapshots), matched_tickers=len({row["ticker"] for row in rows}),
            results=rows, cross_frame=None, computed_at=inputs.cutoff.isoformat(),
            market_time=min(market_times) if market_times else None, analysis_refs=analysis_refs,
            evidence_storage="IMMUTABLE_EVIDENCE" if interval in SETUP_INTERVALS else "RELEASE_ONLY",
        )
    references = [snapshot.source_manifest["reference"] for snapshot in snapshots]
    workspaces = [dict(ticker=snapshot.snapshot_key, snapshot_id=str(snapshot.snapshot_id),
                       payload_sha256=snapshot.payload_sha256) for snapshot in snapshots]
    manifest = dict(
        version=PATTERN_WATCH_PUBLICATION_VERSION, cohort_id=str(inputs.cohort_id),
        provider_tier=inputs.provider_tier.value, source_generation=inputs.source_generation,
        source_manifest_sha256=inputs.source_manifest_sha256,
        reference_set=reference_set(references), workspace_set=workspace_set(workspaces),
        window_policy="realtime-pattern-watch-input-v1", primary_window=300,
    )
    require_bounded_manifest(manifest, name="Pattern Watch")
    return build_portal_snapshot(
        cohort_id=inputs.cohort_id, provider_tier=inputs.provider_tier,
        snapshot_type=PATTERN_WATCH_PUBLICATION_VERSION, snapshot_key="PLATFORM",
        source_generation=inputs.source_generation, source_manifest=manifest,
        payload=dict(version=PATTERN_WATCH_PUBLICATION_VERSION, cohort_id=str(inputs.cohort_id),
                     views=views, references=references, workspaces=workspaces,
                     provider_fetch_performed=False, execution_permission=False),
        generated_at=inputs.cutoff,
    )


def _without_negative_zero(row):
    # jsonb has no negative zero, so a stored -0.0 would read back as 0.0 and fail its payload checksum.
    return {key: value + 0.0 if isinstance(value, float) else value for key, value in row.items()}


def produce_realtime_ticker_workspace_summary(inputs, snapshots, charts):
    """`charts` is the projection's `summary_charts`: the 1d and 1wk chart points the stored workspaces
    only reference."""
    snapshots = tuple(sorted(snapshots, key=lambda snapshot: snapshot.snapshot_key))
    if (
        len(snapshots) != len(inputs.members)
        or {snapshot.snapshot_key for snapshot in snapshots} != {member["ticker"] for member in inputs.members}
        or any(snapshot.snapshot_type != TICKER_WORKSPACE_VERSION for snapshot in snapshots)
    ):
        raise ValueError("Ticker Workspace summary requires one exact snapshot per member")
    overview, setup_rows = [], []
    references, workspaces = [], []
    for snapshot in snapshots:
        payload = snapshot.payload
        chart = charts[snapshot.snapshot_key]["1d"]
        weekly = charts[snapshot.snapshot_key]["1wk"]
        quote = payload["quote"]
        profile = payload["profile"]["security"]
        setup = payload["multi"]["setups"]["1d"]
        technicals = setup["technicals"]
        latest = chart[-1]
        recent = chart[-252:]
        weekly_closes = [float(point["close"]) for point in weekly]
        wsma50 = sum(weekly_closes[-50:]) / 50 if len(weekly_closes) >= 50 else None
        wsma200 = sum(weekly_closes[-200:]) / 200 if len(weekly_closes) >= 200 else None
        high = max(point["high"] for point in recent)
        low = min(point["low"] for point in recent)
        ma200 = technicals.get("ma200")
        close = float(latest["close"])
        overview.append(_without_negative_zero(dict(
            ticker=payload["ticker"], sector=profile.get("sector"), date=quote["trade_date"],
            open=latest["open"], high=latest["high"], low=latest["low"], close=close,
            volume=int(latest["volume"]), chg_pct=quote.get("change_percent"),
            rel_vol=technicals.get("relative_volume"), high_52w=round(high, 2),
            pct_from_high=round((close / high - 1) * 100, 2) if high else None,
            low_52w=round(low, 2), pct_from_low=round((close / low - 1) * 100, 2) if low else None,
            sma_20=technicals.get("ma20"), sma_50=technicals.get("ma50"), sma_200=ma200,
            dist_200=round((close / ma200 - 1) * 100, 2) if ma200 else None,
            wsma_50=round(wsma50, 4) if wsma50 is not None else None,
            wsma_200=round(wsma200, 4) if wsma200 is not None else None,
            dist_200w=round((close / wsma200 - 1) * 100, 2) if wsma200 else None,
        )))
        setup_rows.append(setup_summary_row({"ticker": payload["ticker"], "payload": setup}))
        references.append(snapshot.source_manifest["reference"])
        workspaces.append(dict(
            ticker=snapshot.snapshot_key, snapshot_id=str(snapshot.snapshot_id),
            payload_sha256=snapshot.payload_sha256,
        ))
    market_time = min(snapshot.payload["chart_market_times"]["1d"] for snapshot in snapshots)
    payload = dict(
        version=TICKER_WORKSPACE_SUMMARY_VERSION,
        cohort_id=str(inputs.cohort_id), session=str(inputs.session),
        market_time=market_time, generated_at=inputs.cutoff.isoformat(),
        overview=overview,
        setup=dict(
            interval="1d", source=TICKER_WORKSPACE_VERSION, analysis_run_id=None,
            computed_at=inputs.cutoff.isoformat(), market_time=market_time,
            count=len(setup_rows), results=setup_rows,
        ),
        missing_producers={
            "dashboard": "DASHBOARD_PUBLICATION_UNAVAILABLE",
            "weekly_200": "WEEKLY_200_HISTORY_UNAVAILABLE" if any(row["wsma_200"] is None for row in overview) else None,
        },
        references=references, workspaces=workspaces,
        provider_fetch_performed=False, execution_permission=False,
    )
    manifest = dict(
        version=TICKER_WORKSPACE_SUMMARY_VERSION,
        cohort_id=str(inputs.cohort_id), provider_tier=inputs.provider_tier.value,
        source_generation=inputs.source_generation,
        source_manifest_sha256=inputs.source_manifest_sha256,
        reference_set=reference_set(references), workspace_set=workspace_set(workspaces),
    )
    require_bounded_manifest(manifest, name="Ticker Workspace summary")
    return build_portal_snapshot(
        cohort_id=inputs.cohort_id, provider_tier=inputs.provider_tier,
        snapshot_type=TICKER_WORKSPACE_SUMMARY_VERSION, snapshot_key="PLATFORM",
        source_generation=inputs.source_generation, source_manifest=manifest,
        payload=payload, generated_at=inputs.cutoff,
    )
