from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from equity.domain import SecurityReferenceRevision
from equity.materialization import materialize_equity_evidence
from realtime.equity_bar_adapter import (
    RealtimeEquityBarFact,
    adapt_developer_equity_bar,
)
from realtime.equity_materialization_adapter import (
    materialize_realtime_equity_evidence,
)
from realtime.preflight import ProviderTier
from tests.realtime.test_equity_bar_adapter import (
    COHORT,
    SECURITY,
    _developer_revision,
)


UTC = timezone.utc
NOW = datetime(2026, 10, 5, 14, 31, tzinfo=UTC)


def _security():
    return SecurityReferenceRevision(
        security_revision_id=uuid4(),
        security_id=SECURITY,
        ticker="SPY",
        active=True,
        company_name="SPDR S&P 500 ETF Trust",
        security_type="ETF",
        cik=None,
        composite_figi="BBG000BDTBL9",
        share_class_figi=None,
        primary_exchange="ARCX",
        sic_code=None,
        sic_description=None,
        sector=None,
        industry=None,
        list_date=date(1993, 1, 22),
        delisted_date=None,
        weighted_shares=None,
        free_float=None,
        free_float_percent=None,
        market_cap=None,
        source="MASSIVE_TICKER_OVERVIEW_V3",
        effective_from=datetime(2026, 1, 1, tzinfo=UTC),
        observed_at=datetime(2026, 1, 1, tzinfo=UTC),
        payload_sha256="a" * 64,
        raw_payload_json="{}",
    )


def _facts():
    base = _developer_revision()
    facts = []
    for index in range(3):
        start = base.bar_start - timedelta(minutes=2 - index)
        revision = replace(
            base,
            bar_revision_id=uuid4(),
            bar_start=start,
            bar_end=start + timedelta(minutes=1),
            system_observed_at=start + timedelta(minutes=1),
            close_price=Decimal("101") + index,
            high_price=Decimal("102") + index,
            open_price=Decimal("100") + index,
            low_price=Decimal("99") + index,
            payload_sha256=f"{index + 1:064x}",
        )
        facts.append(adapt_developer_equity_bar(
            revision,
            cohort_id=COHORT,
            instrument_id=1,
        ))
    return tuple(facts)


def test_realtime_adapter_runs_existing_materializer_unchanged():
    security = _security()
    facts = _facts()
    analysis_run_id = uuid4()
    observed_at = facts[-1].revision.bar_end

    adapted = materialize_realtime_equity_evidence(
        analysis_run_id=analysis_run_id,
        security=security,
        bars=facts,
        observed_at=observed_at,
    )
    direct = materialize_equity_evidence(
        analysis_run_id=analysis_run_id,
        security=security,
        interval="1m",
        bars=tuple(row.revision for row in facts),
        observed_at=observed_at,
    )

    assert adapted.materialization == direct
    assert adapted.provider_tier is ProviderTier.MASSIVE_DEVELOPER
    assert adapted.bar_revision_ids == tuple(
        row.revision.bar_revision_id for row in facts
    )
    assert adapted.confirmation_bar_revision_ids == ()
    assert len(adapted.input_sha256) == 64


def test_realtime_adapter_rejects_mixed_cohort_or_tier_inputs():
    facts = _facts()
    mixed = RealtimeEquityBarFact(
        cohort_id=UUID("99999999-9999-9999-9999-999999999999"),
        provider_tier=ProviderTier.MASSIVE_DEVELOPER,
        instrument_id=1,
        revision=facts[-1].revision,
    )

    with pytest.raises(ValueError, match="cannot mix"):
        materialize_realtime_equity_evidence(
            analysis_run_id=uuid4(),
            security=_security(),
            bars=(*facts[:-1], mixed),
            observed_at=NOW,
        )


def test_partial_hourly_confirmation_exposes_exact_source_subset_for_persistence():
    import exchange_calendars
    from realtime.equity_evidence_repository import _prepare_realtime_equity_materialization
    from realtime.ticker_repository import _ticker_feature_definition

    calendar = exchange_calendars.get_calendar("XNYS")
    template = _facts()[0]
    observed = datetime(2026, 10, 10, 23, tzinfo=UTC)
    facts = []
    for session in calendar.sessions_in_range("2026-10-05", "2026-10-09"):
        opened = calendar.session_open(session).to_pydatetime()
        count = 1 if session.date() == date(2026, 10, 9) else 13
        for index in range(count):
            start = opened + timedelta(minutes=30 * index)
            price = Decimal(100 + len(facts))
            facts.append(replace(template, revision=replace(
                template.revision, bar_revision_id=uuid4(), interval="30m", session_date=session.date(),
                bar_start=start, bar_end=start + timedelta(minutes=30), system_observed_at=observed,
                open_price=price, close_price=price + 1, low_price=price - 1, high_price=price + 2,
                payload_sha256=f"{len(facts) + 1:064x}",
            )))
    security = _security()
    analysis = uuid4()
    adapted = materialize_realtime_equity_evidence(
        analysis_run_id=analysis, security=security, bars=facts, observed_at=observed,
    )
    confirmation = next(row for row in adapted.materialization.evidence if row.source_name == "EMA_CONFIRMATION")
    assert adapted.confirmation_bar_revision_ids == confirmation.source_revision_ids
    assert len(adapted.confirmation_bar_revision_ids) == len(facts) - 1
    assert facts[-1].revision.bar_revision_id not in adapted.confirmation_bar_revision_ids
    prepared = _prepare_realtime_equity_materialization(
        adapted, _ticker_feature_definition(adapted, "a" * 64), generation=1,
    )
    assert prepared.source_roles[confirmation.evidence_id] == "CONFIRMATION"
    direct = materialize_equity_evidence(
        analysis_run_id=analysis, security=security, interval="30m",
        bars=tuple(fact.revision for fact in facts), observed_at=observed,
    )
    assert adapted.materialization == direct