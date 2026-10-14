from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import time as clock_module
from typing import Mapping
from urllib.parse import urlsplit
from uuid import NAMESPACE_URL, UUID, uuid5

from market_events import build_public_calendar_document
from options.model_inputs import parse_treasury_curve

from .contracts import DEVELOPER_FORWARD_UNIVERSE, canonical_sha256
from .reference_adapters import _calendar_facts
from .reference_facts import (
    ReferenceFact,
    ReferenceFactRelation,
    validate_reference_facts,
)
from .reference_repository import persist_reference_facts_cursor
from .reference_seed import ReferenceKind
from .reference_service import ReferenceSeedRequest
from .reference_transports import (
    FederalReserveReferenceTransport,
    FinnhubReferenceTransport,
    ReferenceHttpResponse,
    ReferenceTransportLimits,
    RequestsReferenceHttpTransport,
    TreasuryReferenceTransport,
)


HISTORICAL_REFERENCE_BACKFILL_VERSION = "advanced_realtime_h1_v1"
HISTORICAL_EARNINGS_UNAVAILABLE_POLICY_VERSION = "historical_earnings_unavailable_v1"
ETF_TICKERS = frozenset({"IWM", "QQQ", "SPY"})
MAXIMUM_PROVIDER_REQUESTS = 17
MAXIMUM_RESPONSE_BYTES = 2_000_000
MAXIMUM_TOTAL_BYTES = 16_000_000
MAXIMUM_ROWS = 50_000
MAXIMUM_APPLY_DELAY_SECONDS = 60 * 60


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class HistoricalReferenceBackfillPlan:
    start_date: date = date(2025, 8, 25)
    end_date: date = date(2026, 10, 8)
    tickers: tuple[str, ...] = DEVELOPER_FORWARD_UNIVERSE.equity_tickers
    maximum_provider_requests: int = MAXIMUM_PROVIDER_REQUESTS
    maximum_response_bytes: int = MAXIMUM_RESPONSE_BYTES
    maximum_total_bytes: int = MAXIMUM_TOTAL_BYTES
    maximum_rows: int = MAXIMUM_ROWS

    def __post_init__(self) -> None:
        if self.end_date < self.start_date:
            raise ValueError("historical reference window is reversed")
        if tuple(sorted(self.tickers)) != DEVELOPER_FORWARD_UNIVERSE.equity_tickers:
            raise ValueError("historical reference tickers changed the reviewed universe")
        if (
            self.maximum_provider_requests != MAXIMUM_PROVIDER_REQUESTS
            or self.maximum_response_bytes != MAXIMUM_RESPONSE_BYTES
            or self.maximum_total_bytes != MAXIMUM_TOTAL_BYTES
            or self.maximum_rows != MAXIMUM_ROWS
        ):
            raise ValueError("historical reference authorization bounds changed")

    @property
    def finnhub_windows(self) -> tuple[tuple[date, date], ...]:
        rows = []
        start = self.start_date
        while start <= self.end_date:
            end = min(start + timedelta(days=29), self.end_date)
            rows.append((start, end))
            start = end + timedelta(days=1)
        return tuple(rows)

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class HistoricalProviderReceipt:
    host: str
    path: str
    parameters: tuple[tuple[str, str], ...]
    status_code: int
    response_bytes: int
    response_sha256: str
    received_at: datetime

    def __post_init__(self) -> None:
        received = _utc(self.received_at, "received_at")
        if (
            not self.host
            or not self.path.startswith("/")
            or self.status_code != 200
            or not 0 <= self.response_bytes <= MAXIMUM_RESPONSE_BYTES
            or len(self.response_sha256) != 64
        ):
            raise ValueError("historical provider receipt is invalid")
        object.__setattr__(self, "received_at", received)


@dataclass(frozen=True, slots=True)
class HistoricalReferenceBackfillResult:
    plan: HistoricalReferenceBackfillPlan
    acquired_at: datetime
    receipts: tuple[HistoricalProviderReceipt, ...]
    facts: tuple[ReferenceFact, ...]

    def __post_init__(self) -> None:
        acquired = _utc(self.acquired_at, "acquired_at")
        validate_reference_facts(self.facts)
        relations = {fact.relation for fact in self.facts}
        expected = {
            ReferenceFactRelation.MARKET_EVENT,
            ReferenceFactRelation.MARKET_EVENT_COVERAGE,
            ReferenceFactRelation.RATE_OBSERVATION,
        }
        if (
            len(self.receipts) != self.plan.maximum_provider_requests
            or sum(row.response_bytes for row in self.receipts) > self.plan.maximum_total_bytes
            or len(self.facts) > self.plan.maximum_rows
            or not relations <= expected
            or ReferenceFactRelation.MARKET_EVENT_COVERAGE not in relations
            or ReferenceFactRelation.RATE_OBSERVATION not in relations
        ):
            raise ValueError("historical reference result changed authorized bounds")
        object.__setattr__(self, "acquired_at", acquired)

    @property
    def provider_requests(self) -> int:
        return len(self.receipts)

    @property
    def response_bytes(self) -> int:
        return sum(row.response_bytes for row in self.receipts)


class RecordingReferenceHttpTransport:
    def __init__(self, delegate, *, clock=lambda: datetime.now(timezone.utc)) -> None:
        self.delegate = delegate
        self.clock = clock
        self.receipts: list[HistoricalProviderReceipt] = []

    def get(
        self,
        url: str,
        params: Mapping[str, str],
        headers: Mapping[str, str],
    ) -> ReferenceHttpResponse:
        if len(self.receipts) >= MAXIMUM_PROVIDER_REQUESTS:
            raise ValueError("historical reference request ceiling exhausted")
        if any(key.lower() in {"apikey", "api_key", "token", "authorization"} for key in params):
            raise ValueError("historical reference request parameters contain a credential")
        response = self.delegate.get(url, params, headers)
        parsed = urlsplit(url)
        receipt = HistoricalProviderReceipt(
            parsed.hostname or "",
            parsed.path,
            tuple(sorted((str(key), str(value)) for key, value in params.items())),
            response.status_code,
            len(response.body),
            hashlib.sha256(response.body).hexdigest(),
            self.clock(),
        )
        self.receipts.append(receipt)
        if sum(row.response_bytes for row in self.receipts) > MAXIMUM_TOTAL_BYTES:
            raise ValueError("historical reference aggregate byte ceiling exceeded")
        return response


def _window_datetimes(plan: HistoricalReferenceBackfillPlan) -> tuple[datetime, datetime]:
    return (
        datetime.combine(plan.start_date, time.min, timezone.utc),
        datetime.combine(plan.end_date, time.max, timezone.utc),
    )


def _etf_earnings_coverage_fact(
    ticker: str,
    request: ReferenceSeedRequest,
    plan: HistoricalReferenceBackfillPlan,
) -> ReferenceFact:
    evidence = {
        "ticker": ticker,
        "verdict": "NOT_APPLICABLE",
        "window_start": plan.start_date,
        "window_end": plan.end_date,
    }
    digest = canonical_sha256(evidence)
    return ReferenceFact.build(
        ReferenceFactRelation.MARKET_EVENT_COVERAGE,
        ReferenceKind.EARNINGS_COVERAGE.value,
        ticker,
        coverage_id=uuid5(
            NAMESPACE_URL,
            f"realtime-market-event-coverage:finnhub:historical-etf:{ticker}:"
            f"{request.created_at.isoformat()}:{digest}",
        ),
        security_id=uuid5(NAMESPACE_URL, f"realtime-security:{ticker}"),
        event_type="EARNINGS",
        window_start=plan.start_date,
        window_end=plan.end_date,
        source="finnhub",
        verdict="NOT_APPLICABLE",
        checked_at=request.created_at,
        valid_until=request.valid_until,
        evidence_sha256=digest,
    )


def _rate_facts(xml_documents, *, plan, acquired_at) -> tuple[ReferenceFact, ...]:
    observations = tuple(
        row
        for xml in xml_documents
        for row in parse_treasury_curve(xml, received_at=acquired_at)
        if plan.start_date <= row.rate_date <= plan.end_date
    )
    facts = tuple(
        ReferenceFact.build(
            ReferenceFactRelation.RATE_OBSERVATION,
            ReferenceKind.RATE_CURVE.value,
            "USD",
            observation_id=row.rate_observation_id,
            rate_date=row.rate_date,
            tenor_days=row.tenor_days,
            annual_rate=row.annual_rate,
            source=row.source,
            source_key=row.source_key,
            source_observed_at=row.source_observed_at,
            received_at=row.first_observed_at,
            payload_sha256=row.payload_sha256,
        )
        for row in observations
    )
    if not facts or min(dict(row.values)["rate_date"] for row in facts) > plan.start_date:
        raise ValueError("historical Treasury coverage does not reach the requested start")
    if max(dict(row.values)["rate_date"] for row in facts) < plan.end_date:
        raise ValueError("historical Treasury coverage does not reach the requested end")
    return facts


def acquire_historical_reference_backfill(
    plan: HistoricalReferenceBackfillPlan,
    *,
    finnhub_api_key: str,
    http=None,
    clock=lambda: datetime.now(timezone.utc),
    sleep=clock_module.sleep,
) -> HistoricalReferenceBackfillResult:
    if not isinstance(plan, HistoricalReferenceBackfillPlan):
        raise TypeError("historical reference acquisition requires a typed plan")
    if not finnhub_api_key.strip():
        raise ValueError("Finnhub API key is required")
    recorder = RecordingReferenceHttpTransport(
        http or RequestsReferenceHttpTransport(),
        clock=clock,
    )
    limits = ReferenceTransportLimits(
        maximum_requests=MAXIMUM_PROVIDER_REQUESTS,
        maximum_response_bytes=MAXIMUM_RESPONSE_BYTES,
        maximum_total_bytes=MAXIMUM_TOTAL_BYTES,
        maximum_rows=MAXIMUM_ROWS,
        maximum_pages=1,
    )
    finnhub = FinnhubReferenceTransport(
        finnhub_api_key,
        recorder,
        limits=limits,
        maximum_window_days=30,
        sleep=sleep,
    )
    federal_reserve = FederalReserveReferenceTransport(recorder, limits=limits)
    treasury = TreasuryReferenceTransport(recorder, limits=limits)

    earnings_rows = finnhub.fetch(plan.start_date, plan.end_date)
    fomc = federal_reserve.fetch()
    treasury_xml = tuple(
        treasury.fetch_xml(year)
        for year in range(plan.start_date.year, plan.end_date.year + 1)
    )
    acquired_at = _utc(clock(), "acquired_at")
    request = ReferenceSeedRequest(
        DEVELOPER_FORWARD_UNIVERSE,
        plan.end_date,
        acquired_at,
        acquired_at,
        acquired_at + timedelta(hours=6),
    )
    window_start, window_end = _window_datetimes(plan)
    companies = tuple(ticker for ticker in plan.tickers if ticker not in ETF_TICKERS)
    earnings_document = build_public_calendar_document(
        company_tickers=companies,
        window_start=window_start,
        window_end=window_end,
        observed_at=acquired_at,
        earnings_rows=earnings_rows,
        fomc=None,
    )
    fomc_document = build_public_calendar_document(
        company_tickers=(),
        window_start=window_start,
        window_end=window_end,
        observed_at=acquired_at,
        earnings_rows=None,
        fomc=fomc,
    )
    facts = tuple((
        *_calendar_facts(
            earnings_document,
            request,
            source="finnhub",
            member_kind=ReferenceKind.EARNINGS_COVERAGE,
        ),
        *(
            _etf_earnings_coverage_fact(ticker, request, plan)
            for ticker in sorted(ETF_TICKERS)
        ),
        *_calendar_facts(
            fomc_document,
            request,
            source="federal_reserve",
            member_kind=ReferenceKind.FOMC_COVERAGE,
        ),
        *_rate_facts(treasury_xml, plan=plan, acquired_at=acquired_at),
    ))
    return HistoricalReferenceBackfillResult(
        plan,
        acquired_at,
        tuple(recorder.receipts),
        facts,
    )


def _encode_value(value):
    if isinstance(value, datetime):
        return {"datetime": _utc(value, "artifact datetime").isoformat()}
    if isinstance(value, date):
        return {"date": value.isoformat()}
    if isinstance(value, UUID):
        return {"uuid": str(value)}
    if isinstance(value, Decimal):
        return {"decimal": str(value)}
    if isinstance(value, tuple):
        return {"tuple": [_encode_value(item) for item in value]}
    if isinstance(value, list):
        return {"list": [_encode_value(item) for item in value]}
    if isinstance(value, Mapping):
        return {"mapping": {str(key): _encode_value(item) for key, item in value.items()}}
    if value is None or type(value) in {str, int, float, bool}:
        return value
    raise TypeError(f"unsupported historical artifact value: {type(value).__name__}")


def _decode_value(value):
    if not isinstance(value, dict):
        return value
    if len(value) != 1:
        raise ValueError("historical artifact encoding must be exact")
    key, item = next(iter(value.items()))
    readers = {
        "datetime": datetime.fromisoformat,
        "date": date.fromisoformat,
        "uuid": UUID,
        "decimal": Decimal,
        "tuple": lambda rows: tuple(_decode_value(row) for row in rows),
        "list": lambda rows: [_decode_value(row) for row in rows],
        "mapping": lambda rows: {name: _decode_value(row) for name, row in rows.items()},
    }
    if key not in readers:
        raise ValueError("historical artifact encoding is unknown")
    return readers[key](item)


def encode_historical_reference_artifact(result: HistoricalReferenceBackfillResult) -> str:
    payload = {
        "version": HISTORICAL_REFERENCE_BACKFILL_VERSION,
        "plan": _encode_value({
            "start_date": result.plan.start_date,
            "end_date": result.plan.end_date,
            "tickers": result.plan.tickers,
            "maximum_provider_requests": result.plan.maximum_provider_requests,
            "maximum_response_bytes": result.plan.maximum_response_bytes,
            "maximum_total_bytes": result.plan.maximum_total_bytes,
            "maximum_rows": result.plan.maximum_rows,
        }),
        "acquired_at": _encode_value(result.acquired_at),
        "receipts": _encode_value(tuple({
            "host": row.host,
            "path": row.path,
            "parameters": row.parameters,
            "status_code": row.status_code,
            "response_bytes": row.response_bytes,
            "response_sha256": row.response_sha256,
            "received_at": row.received_at,
        } for row in result.receipts)),
        "facts": [
            {
                "relation": fact.relation.value,
                "member_kind": fact.member_kind,
                "member_key": fact.member_key,
                "values": _encode_value(dict(fact.values)),
            }
            for fact in result.facts
        ],
    }
    payload_sha256 = canonical_sha256(payload)
    return json.dumps(
        {"payload": payload, "payload_sha256": payload_sha256},
        indent=2,
        sort_keys=True,
        ensure_ascii=True,
    ) + "\n"


def decode_historical_reference_artifact(text: str) -> HistoricalReferenceBackfillResult:
    artifact = json.loads(text)
    if set(artifact) != {"payload", "payload_sha256"}:
        raise ValueError("historical reference artifact fields changed")
    payload = artifact["payload"]
    if canonical_sha256(payload) != artifact["payload_sha256"]:
        raise ValueError("historical reference artifact checksum drift")
    if payload.get("version") != HISTORICAL_REFERENCE_BACKFILL_VERSION:
        raise ValueError("historical reference artifact version changed")
    plan_values = _decode_value(payload["plan"])
    plan = HistoricalReferenceBackfillPlan(**plan_values)
    receipts = tuple(
        HistoricalProviderReceipt(**row)
        for row in _decode_value(payload["receipts"])
    )
    facts = tuple(
        ReferenceFact.build(
            ReferenceFactRelation(row["relation"]),
            row["member_kind"],
            row["member_key"],
            **_decode_value(row["values"]),
        )
        for row in payload["facts"]
    )
    return HistoricalReferenceBackfillResult(
        plan,
        _decode_value(payload["acquired_at"]),
        receipts,
        facts,
    )


def historical_fact_counts(facts) -> dict[str, int]:
    return {
        relation.value: sum(fact.relation is relation for fact in facts)
        for relation in (
            ReferenceFactRelation.MARKET_EVENT,
            ReferenceFactRelation.MARKET_EVENT_COVERAGE,
            ReferenceFactRelation.RATE_OBSERVATION,
        )
    }


def historical_reference_blockers(result: HistoricalReferenceBackfillResult) -> tuple[str, ...]:
    company_tickers = set(result.plan.tickers) - set(ETF_TICKERS)
    earnings_tickers = {
        fact.member_key
        for fact in result.facts
        if fact.relation is ReferenceFactRelation.MARKET_EVENT
        and dict(fact.values)["event_type"] == "EARNINGS"
        and dict(fact.values)["source"] == "finnhub"
    }
    unavailable_tickers = {
        fact.member_key
        for fact in result.facts
        if fact.relation is ReferenceFactRelation.MARKET_EVENT_COVERAGE
        and dict(fact.values)["event_type"] == "EARNINGS"
        and dict(fact.values)["source"] == "finnhub"
        and dict(fact.values)["verdict"] == "UNAVAILABLE"
    }
    blockers = []
    missing = tuple(sorted(company_tickers - earnings_tickers - unavailable_tickers))
    if missing:
        blockers.append(
            "HISTORICAL_EARNINGS_PROVIDER_COVERAGE_UNAVAILABLE:" + ",".join(missing)
        )
    return tuple(blockers)


def mark_historical_earnings_unavailable(
    result: HistoricalReferenceBackfillResult,
) -> tuple[HistoricalReferenceBackfillResult, tuple[str, ...]]:
    company_tickers = set(result.plan.tickers) - set(ETF_TICKERS)
    earnings_tickers = {
        fact.member_key
        for fact in result.facts
        if fact.relation is ReferenceFactRelation.MARKET_EVENT
        and dict(fact.values)["event_type"] == "EARNINGS"
        and dict(fact.values)["source"] == "finnhub"
    }
    unavailable = tuple(sorted(company_tickers - earnings_tickers))
    facts = []
    for fact in result.facts:
        values = dict(fact.values)
        if (
            fact.relation is ReferenceFactRelation.MARKET_EVENT_COVERAGE
            and fact.member_key in unavailable
            and values["event_type"] == "EARNINGS"
            and values["source"] == "finnhub"
        ):
            evidence = {
                "version": HISTORICAL_EARNINGS_UNAVAILABLE_POLICY_VERSION,
                "ticker": fact.member_key,
                "window_start": values["window_start"],
                "window_end": values["window_end"],
                "original_evidence_sha256": values["evidence_sha256"],
                "reason": "HISTORICAL_EARNINGS_PROVIDER_COVERAGE_UNAVAILABLE",
            }
            digest = canonical_sha256(evidence)
            values.update(
                coverage_id=uuid5(
                    NAMESPACE_URL,
                    f"realtime-market-event-coverage:finnhub:historical-unavailable:"
                    f"{fact.member_key}:{result.acquired_at.isoformat()}:{digest}",
                ),
                verdict="UNAVAILABLE",
                evidence_sha256=digest,
            )
            fact = ReferenceFact.build(
                fact.relation,
                fact.member_kind,
                fact.member_key,
                **values,
            )
        facts.append(fact)
    transformed = HistoricalReferenceBackfillResult(
        result.plan,
        result.acquired_at,
        result.receipts,
        tuple(facts),
    )
    if historical_reference_blockers(transformed):
        raise ValueError("historical earnings unavailable policy did not resolve coverage")
    return transformed, unavailable


def _target_counts(cursor) -> tuple[int, int, int]:
    cursor.execute(
        "SELECT (SELECT COUNT(*) FROM ref.market_event_revisions),"
        "(SELECT COUNT(*) FROM ref.market_event_coverage),"
        "(SELECT COUNT(*) FROM ref.risk_free_rate_observations)"
    )
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError("historical reference target counts are unavailable")
    return tuple(int(value) for value in row)


def _seed_current(cursor):
    cursor.execute(
        "SELECT generation,seed_id::text,effective_at FROM rt.seed_current "
        "WHERE scope='MARKET_REFERENCES'"
    )
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError("historical reference current generation is unavailable")
    return tuple(row)


def _assert_new_identities(cursor, result: HistoricalReferenceBackfillResult) -> None:
    checks = (
        (
            ReferenceFactRelation.MARKET_EVENT,
            "SELECT revision_id::text FROM ref.market_event_revisions WHERE revision_id=ANY(%s::uuid[])",
        ),
        (
            ReferenceFactRelation.MARKET_EVENT_COVERAGE,
            "SELECT coverage_id::text FROM ref.market_event_coverage WHERE coverage_id=ANY(%s::uuid[])",
        ),
        (
            ReferenceFactRelation.RATE_OBSERVATION,
            "SELECT observation_id::text FROM ref.risk_free_rate_observations WHERE observation_id=ANY(%s::uuid[])",
        ),
    )
    for relation, statement in checks:
        identities = [str(fact.identity[1]) for fact in result.facts if fact.relation is relation]
        if not identities:
            continue
        cursor.execute(statement, (identities,))
        existing = tuple(cursor.fetchall())
        if existing:
            raise ValueError(f"historical {relation.value} identity already exists")


def persist_historical_reference_backfill_cursor(
    cursor,
    result: HistoricalReferenceBackfillResult,
    *,
    database: str,
    role_name: str,
) -> dict:
    if not isinstance(result, HistoricalReferenceBackfillResult):
        raise TypeError("historical reference apply requires a typed artifact")
    blockers = historical_reference_blockers(result)
    if blockers:
        raise ValueError(";".join(blockers))
    cursor.execute(
        "SELECT current_database(),current_user,current_role,"
        "current_setting('transaction_read_only'),clock_timestamp()"
    )
    control = cursor.fetchone()
    if control is None or tuple(control[:4]) != (database, role_name, role_name, "off"):
        raise RuntimeError("historical reference target/role/write state mismatch")
    applied_at = _utc(control[4], "database clock")
    if not result.acquired_at <= applied_at <= result.acquired_at + timedelta(
        seconds=MAXIMUM_APPLY_DELAY_SECONDS
    ):
        raise ValueError("historical reference artifact apply window expired")
    cursor.execute(
        "SELECT database_name::text,lineage_name FROM rt.database_identity"
    )
    if tuple(cursor.fetchone() or ()) != (database, "advanced_realtime_v1"):
        raise RuntimeError("historical reference database lineage mismatch")

    seed_before = _seed_current(cursor)
    if seed_before[0] != 7:
        raise ValueError("historical reference apply requires retained generation 7")
    counts_before = _target_counts(cursor)
    _assert_new_identities(cursor, result)

    security_ids = sorted({
        str(values["security_id"])
        for fact in result.facts
        for values in (dict(fact.values),)
        if values.get("security_id") is not None
    })
    cursor.execute(
        "SELECT security_id::text FROM ref.securities WHERE security_id=ANY(%s::uuid[]) "
        "ORDER BY security_id::text",
        (security_ids,),
    )
    if tuple(row[0] for row in cursor.fetchall()) != tuple(security_ids):
        raise ValueError("historical reference artifact security identities are incomplete")

    persist_reference_facts_cursor(cursor, result.facts)
    counts_after = _target_counts(cursor)
    fact_counts = historical_fact_counts(result.facts)
    expected_after = (
        counts_before[0] + fact_counts[ReferenceFactRelation.MARKET_EVENT.value],
        counts_before[1] + fact_counts[ReferenceFactRelation.MARKET_EVENT_COVERAGE.value],
        counts_before[2] + fact_counts[ReferenceFactRelation.RATE_OBSERVATION.value],
    )
    if counts_after != expected_after:
        raise ValueError("historical reference target count delta changed")
    seed_after = _seed_current(cursor)
    if seed_after != seed_before:
        raise ValueError("historical reference apply changed generation 7")
    return {
        "applied_at": applied_at,
        "role": role_name,
        "fact_counts": fact_counts,
        "target_counts_before": counts_before,
        "target_counts_after": counts_after,
        "seed_current_before": seed_before,
        "seed_current_after": seed_after,
    }