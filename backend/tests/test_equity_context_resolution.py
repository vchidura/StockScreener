import json
import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from equity.context import build_equity_context
from equity.domain import (
    ContextStatus,
    DecisionWatermark,
    EquityEvidence,
    EvidenceRole,
    EvidenceType,
    LifecycleStatus,
    QualityState,
    SecurityReferenceRevision,
)
from equity.polygon import canonical_json, sha256_json


UTC = timezone.utc
HASH = "a" * 64
MARKET_TIME = datetime(2026, 8, 28, 15, 0, tzinfo=UTC)


def security():
    return SecurityReferenceRevision(
        security_revision_id=uuid4(), security_id=uuid4(), ticker="AAPL",
        active=True, company_name="Apple Inc.", security_type="CS",
        cik="0000320193", composite_figi="BBG000B9XRY4",
        share_class_figi="BBG001S5N8V8", primary_exchange="XNAS",
        sic_code="3571", sic_description="ELECTRONIC COMPUTERS",
        sector="Information Technology", industry="Technology Hardware",
        list_date=date(1980, 12, 12), delisted_date=None,
        weighted_shares=Decimal("15000000000"),
        free_float=Decimal("14500000000"), free_float_percent=96.67,
        market_cap=Decimal("2800000000000"),
        source="POLYGON_TICKER_OVERVIEW_V3",
        effective_from=datetime(2026, 8, 1, tzinfo=UTC),
        observed_at=datetime(2026, 8, 1, 1, tzinfo=UTC),
        payload_sha256=HASH, raw_payload_json="{}",
    )


SECURITY = security()


def evidence(
    evidence_type,
    role,
    payload,
    *,
    direction=None,
    qualification_id=None,
    market_time=MARKET_TIME,
    interval="30m",
    status=LifecycleStatus.SNAPSHOT,
    quality_state=QualityState.COMPLETE,
):
    digest = sha256_json(payload)
    return EquityEvidence(
        evidence_id=uuid4(), evidence_key=f"key:{uuid4()}",
        lifecycle_key=f"life:{uuid4()}", evidence_type=evidence_type,
        evidence_role=role, security_id=SECURITY.security_id, ticker="AAPL",
        interval=interval, direction=direction, lifecycle_status=status,
        strength=None, market_time=market_time,
        observed_at=market_time + timedelta(seconds=1),
        valid_until=market_time + timedelta(hours=2), source_name="source",
        source_version="1.0", payload_schema_version="1.0",
        analysis_run_id=uuid4(), latest_bar_revision_id=uuid4(),
        security_revision_id=SECURITY.security_revision_id,
        fundamental_report_ids=(), source_revision_ids=(),
        quality_state=quality_state, quality_codes=(),
        qualification_revision_id=qualification_id,
        payload_json=canonical_json(payload), payload_sha256=digest,
    )


def watermark():
    return DecisionWatermark(MARKET_TIME, MARKET_TIME + timedelta(seconds=5))


def test_unqualified_scanner_cannot_establish_option_direction():
    feature = evidence(
        EvidenceType.FEATURE_SNAPSHOT, EvidenceRole.REGIME,
        {"ema_direction": "BULLISH"}, direction=1,
    )
    scanner = evidence(
        EvidenceType.SCANNER_RESULT, EvidenceRole.DIRECTION,
        {"trigger": "breakout"}, direction=1, status=LifecycleStatus.MATCH,
    )

    context, links = build_equity_context(
        security=SECURITY, strategy_horizon="INTRADAY_30M",
        watermark=watermark(), evidence=(feature, scanner),
        robust_qualification_ids=frozenset(),
        context_policy_version="context_v1", context_policy_sha256=HASH,
    )

    assert context.status is ContextStatus.DEGRADED
    assert context.ema_direction == "BULLISH"
    assert context.qualified_direction is None
    assert "QUALIFIED_DIRECTION_UNAVAILABLE" in context.reason_codes
    assert len(links) == 2


def test_robust_direction_and_fundamentals_build_complete_context():
    qualification_id = uuid4()
    feature = evidence(
        EvidenceType.FEATURE_SNAPSHOT, EvidenceRole.REGIME,
        {"ema_direction": "BULLISH"}, direction=1,
    )
    scanner = evidence(
        EvidenceType.SCANNER_RESULT, EvidenceRole.DIRECTION,
        {"trigger": "breakout"}, direction=1,
        qualification_id=qualification_id, status=LifecycleStatus.MATCH,
    )
    fundamental = evidence(
        EvidenceType.FUNDAMENTAL_SNAPSHOT, EvidenceRole.RISK,
        {
            "market_cap": "2800000000000", "shares_outstanding": "15000000000",
            "free_float": "14500000000", "dividend_yield": 0.004,
            "enterprise_value": "2850000000000", "ebitda": "120000000000",
            "operating_income": "110000000000", "free_cash_flow": "95000000000",
        },
        interval=None,
    )

    context, _ = build_equity_context(
        security=SECURITY, strategy_horizon="INTRADAY_30M",
        watermark=watermark(), evidence=(feature, scanner, fundamental),
        robust_qualification_ids=frozenset({qualification_id}),
        context_policy_version="context_v1", context_policy_sha256=HASH,
    )

    assert context.status is ContextStatus.COMPLETE
    assert context.qualified_direction == "BULLISH"
    assert context.direction_qualification_id == qualification_id
    assert context.dividend_yield == 0.004
    assert context.ebitda == Decimal("120000000000")


def test_conflicting_robust_directions_fail_closed():
    bull_qualification = uuid4()
    bear_qualification = uuid4()
    feature = evidence(
        EvidenceType.FEATURE_SNAPSHOT, EvidenceRole.REGIME,
        {"ema_direction": "NEUTRAL"}, direction=0,
    )
    bullish = evidence(
        EvidenceType.SCANNER_RESULT, EvidenceRole.DIRECTION,
        {}, direction=1, qualification_id=bull_qualification,
        status=LifecycleStatus.MATCH,
    )
    bearish = evidence(
        EvidenceType.SCANNER_RESULT, EvidenceRole.DIRECTION,
        {}, direction=-1, qualification_id=bear_qualification,
        status=LifecycleStatus.MATCH,
    )

    context, _ = build_equity_context(
        security=SECURITY, strategy_horizon="INTRADAY_30M",
        watermark=watermark(), evidence=(feature, bullish, bearish),
        robust_qualification_ids=frozenset({bull_qualification, bear_qualification}),
        context_policy_version="context_v1", context_policy_sha256=HASH,
    )

    assert context.status is ContextStatus.CONFLICTED
    assert context.qualified_direction is None
    assert json.loads(context.conflict_state_json)["reasons"] == [
        "QUALIFIED_DIRECTION_CONFLICT"
    ]


def test_unqualified_setup_conflict_is_advisory_not_blocking():
    qualification_id = uuid4()
    feature = evidence(
        EvidenceType.FEATURE_SNAPSHOT, EvidenceRole.REGIME,
        {"ema_direction": "BULLISH"}, direction=1,
    )
    scanner = evidence(
        EvidenceType.SCANNER_RESULT, EvidenceRole.DIRECTION,
        {}, direction=1, qualification_id=qualification_id,
        status=LifecycleStatus.MATCH,
    )
    setup = evidence(
        EvidenceType.TRADE_SETUP, EvidenceRole.SETUP,
        {"direction_state": "CONFLICTED"}, status=LifecycleStatus.CONFLICTED,
        quality_state=QualityState.RESEARCH_ONLY,
    )

    context, _ = build_equity_context(
        security=SECURITY, strategy_horizon="INTRADAY_30M",
        watermark=watermark(), evidence=(feature, scanner, setup),
        robust_qualification_ids=frozenset({qualification_id}),
        context_policy_version="context_v2", context_policy_sha256=HASH,
    )

    conflict_state = json.loads(context.conflict_state_json)
    assert context.status is ContextStatus.DEGRADED
    assert context.qualified_direction == "BULLISH"
    assert conflict_state["reasons"] == []
    assert conflict_state["advisory_reasons"] == ["UNQUALIFIED_SETUP_CONFLICT"]


def test_future_evidence_is_excluded_even_when_passed_to_resolver():
    feature = evidence(
        EvidenceType.FEATURE_SNAPSHOT, EvidenceRole.REGIME,
        {"ema_direction": "BULLISH"}, direction=1,
        market_time=MARKET_TIME + timedelta(minutes=30),
    )

    context, links = build_equity_context(
        security=SECURITY, strategy_horizon="INTRADAY_30M",
        watermark=watermark(), evidence=(feature,),
        robust_qualification_ids=frozenset(),
        context_policy_version="context_v1", context_policy_sha256=HASH,
    )

    assert context.status is ContextStatus.UNAVAILABLE
    assert links == ()
    assert "FEATURE_CONTEXT_UNAVAILABLE" in context.reason_codes