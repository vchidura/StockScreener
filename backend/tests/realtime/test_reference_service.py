from datetime import date, datetime, timedelta, timezone

import pytest

from realtime.contracts import CANARY_UNIVERSE
from realtime.reference_seed import (
    ReferenceKind,
    ReferenceSeedMember,
    SeedMemberStatus,
    SeedStatus,
)
from realtime.reference_service import (
    ReferenceSeedRequest,
    ReferenceSeedService,
    ReferenceSourceGeneration,
)


UTC = timezone.utc
NOW = datetime(2026, 10, 5, 12, 0, tzinfo=UTC)
ETFS = {"SPY", "QQQ", "IWM"}


def _member(kind, key, source, status=SeedMemberStatus.READY):
    reasons = () if status is SeedMemberStatus.READY else ("VERIFIED_ETF",)
    return ReferenceSeedMember(
        kind,
        key,
        f"{source}:{kind.value}:{key}",
        "a" * 64,
        status,
        NOW,
        NOW + timedelta(seconds=1),
        reasons,
    )


class FakeSource:
    def __init__(self, source, members, requests=1, response_bytes=100):
        self.source = source
        self.members = tuple(members)
        self.requests = requests
        self.response_bytes = response_bytes

    def collect(self, request):
        assert request.universe == CANARY_UNIVERSE
        return ReferenceSourceGeneration(
            self.source,
            {"massive": "a", "finnhub": "b", "federal_reserve": "c", "treasury": "d"}[self.source] * 64,
            self.members,
            self.requests,
            self.response_bytes,
        )


def _sources(*, omit_earnings=None):
    massive = []
    finnhub = []
    for ticker in CANARY_UNIVERSE.equity_tickers:
        massive.extend(
            (
                _member(ReferenceKind.SECURITY, ticker, "massive"),
                _member(ReferenceKind.FUNDAMENTALS, ticker, "massive"),
                _member(ReferenceKind.CORPORATE_ACTION_COVERAGE, ticker, "massive"),
                _member(ReferenceKind.OPTION_CATALOG, ticker, "massive"),
                _member(ReferenceKind.OI_COVERAGE, ticker, "massive"),
            )
        )
        if ticker != omit_earnings:
            finnhub.append(
                _member(
                    ReferenceKind.EARNINGS_COVERAGE,
                    ticker,
                    "finnhub",
                    SeedMemberStatus.NOT_APPLICABLE if ticker in ETFS else SeedMemberStatus.READY,
                )
            )
    return (
        FakeSource("massive", massive, requests=12, response_bytes=800),
        FakeSource("finnhub", finnhub, requests=1, response_bytes=200),
        FakeSource(
            "federal_reserve",
            (_member(ReferenceKind.FOMC_COVERAGE, "GLOBAL", "federal_reserve"),),
            response_bytes=300,
        ),
        FakeSource(
            "treasury",
            (_member(ReferenceKind.RATE_CURVE, "USD", "treasury"),),
            response_bytes=400,
        ),
    )


def _request():
    return ReferenceSeedRequest(
        CANARY_UNIVERSE,
        date(2026, 10, 5),
        NOW,
        NOW + timedelta(minutes=1),
        NOW + timedelta(hours=6),
    )


def test_reference_service_builds_ready_generation_from_exact_sources():
    result = ReferenceSeedService(_sources()).run_once(_request())

    assert result.manifest.status is SeedStatus.READY
    assert len(result.manifest.members) == 50
    assert result.provider_requests == 15
    assert result.response_bytes == 1700
    assert dict(result.manifest.source_generations) == {
        "federal_reserve": "c" * 64,
        "finnhub": "b" * 64,
        "massive": "a" * 64,
        "treasury": "d" * 64,
    }


def test_reference_service_missing_source_or_member_fails_closed():
    with pytest.raises(ValueError, match="each reviewed source exactly once"):
        ReferenceSeedService(_sources()[:-1])

    result = ReferenceSeedService(_sources(omit_earnings="AAPL")).run_once(_request())
    assert result.manifest.status is SeedStatus.UNAVAILABLE
    assert "MISSING_EARNINGS_COVERAGE:AAPL" in result.manifest.reason_codes


def test_reference_service_rejects_duplicate_cross_source_member_identity():
    sources = list(_sources())
    duplicate = _member(ReferenceKind.SECURITY, "SPY", "finnhub")
    sources[1] = FakeSource("finnhub", (*sources[1].members, duplicate))

    with pytest.raises(ValueError, match="member identities must be unique"):
        ReferenceSeedService(tuple(sources)).run_once(_request())