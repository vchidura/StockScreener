"""Daily setup evidence for the original All Tickers published read contract."""
from datetime import timedelta
import json
from uuid import NAMESPACE_URL, uuid5

import exchange_calendars

from equity.domain import EvidenceType
from equity.ticker_contracts import setup_summary_row

from .contracts import canonical_sha256
from .equity_materialization_adapter import materialize_realtime_equity_evidence
from .ticker_projection import _fact, _security


def produce_daily_setup_summary(inputs, *, materializations=None, excluded_tickers=()):
    calendar = exchange_calendars.get_calendar("XNYS")
    analysis_run_id = uuid5(NAMESPACE_URL, f"realtime-daily-summary:{canonical_sha256(inputs)}")
    rows, evidence, missing = [], [], {}
    for member in sorted(inputs.members, key=lambda row: row["ticker"]):
        ticker = member["ticker"]
        if ticker in excluded_tickers:
            continue
        own = [row for row in inputs.bars if row["ticker"] == ticker]
        daily = sorted((row for row in own if row["interval"] == "1d"
                        and row["adjustment_basis"] == "ADJUSTED"), key=lambda row: row["bar_start"])
        if not daily:
            missing[ticker] = "DAILY_INPUT_MISSING"
            continue
        sessions = [row["session_date"] for row in daily[-20:]]
        hourly = sorted((row for row in own if row["interval"] == "1h"
                         and row["adjustment_basis"] == "RAW_UNADJUSTED"
                         and row["session_date"] in sessions), key=lambda row: row["bar_start"])
        expected = []
        for session in sessions:
            start = calendar.session_open(str(session)).to_pydatetime()
            end = calendar.session_close(str(session)).to_pydatetime()
            while start < end:
                finish = min(start + timedelta(hours=1), end)
                expected.append((start, finish))
                start = finish
        if [(row["bar_start"], row["bar_end"]) for row in hourly] != expected:
            hourly = []
            missing[ticker] = "EXACT_20_SESSION_1H_CONFIRMATION_UNAVAILABLE"
        result = materialize_realtime_equity_evidence(
            analysis_run_id=analysis_run_id,
            security=_security(member), bars=tuple(_fact(row, inputs) for row in daily),
            confirmation_bars=tuple(_fact(row, inputs) for row in hourly), observed_at=inputs.cutoff,
        )
        if materializations is not None:
            materializations.append(result)
        setup = next(row for row in result.materialization.evidence if row.evidence_type is EvidenceType.TRADE_SETUP)
        payload = json.loads(setup.payload_json)
        rows.append(setup_summary_row(dict(ticker=ticker, payload=payload)))
        evidence.append(dict(
            ticker=ticker, evidence_id=str(setup.evidence_id), analysis_run_id=str(setup.analysis_run_id),
            security_revision_id=str(setup.security_revision_id), source_version=setup.source_version,
            market_time=setup.market_time.isoformat(), observed_at=setup.observed_at.isoformat(),
            payload_sha256=setup.payload_sha256, payload=payload,
            source_revision_ids=[str(value) for value in result.bar_revision_ids],
            confirmation_revision_ids=[str(value) for value in result.confirmation_bar_revision_ids],
        ))
        if materializations is not None:
            evidence[-1]["dependency_evidence_ids"] = [str(value) for value in setup.source_revision_ids]
            evidence[-1]["dependency_payload_sha256"] = {str(row.evidence_id): row.payload_sha256
                for row in result.materialization.evidence if row.evidence_id in setup.source_revision_ids}
    return rows, evidence, missing


def produce_hourly_setup_summary(inputs, *, market_time, materializations=None, excluded_tickers=()):
    analysis_run_id = uuid5(NAMESPACE_URL, f"realtime-hourly-summary:{market_time.isoformat()}:{canonical_sha256(inputs)}")
    rows, evidence, missing = [], [], {}
    for member in sorted(inputs.members, key=lambda row: row["ticker"]):
        ticker = member["ticker"]
        if ticker in excluded_tickers:
            continue
        own = [row for row in inputs.bars if row["ticker"] == ticker and row["bar_end"] <= market_time]
        primary = sorted((row for row in own if row["interval"] == "1h" and row["adjustment_basis"] == "RAW_UNADJUSTED"),
                         key=lambda row: row["bar_start"])
        if not primary:
            missing[ticker] = "HOURLY_INPUT_MISSING"
            continue
        confirmation = sorted((row for row in own if row["interval"] == "1d" and row["adjustment_basis"] == "RAW_UNADJUSTED"),
                              key=lambda row: row["bar_start"])
        calendar = exchange_calendars.get_calendar("XNYS")
        expected_session = calendar.date_to_session(str(inputs.session), direction="previous")
        if calendar.session_close(expected_session).to_pydatetime() > market_time:
            expected_session = calendar.previous_session(expected_session)
        expected = [stamp.date() for stamp in calendar.sessions_in_range(str(confirmation[0]["session_date"]), expected_session)] if confirmation else []
        if not confirmation or [row["session_date"] for row in confirmation] != expected:
            confirmation = []
            missing[ticker] = "EXACT_COMPLETED_DAILY_CONFIRMATION_UNAVAILABLE"
        result = materialize_realtime_equity_evidence(analysis_run_id=analysis_run_id, security=_security(member),
            bars=tuple(_fact(row, inputs) for row in primary), confirmation_bars=tuple(_fact(row, inputs) for row in confirmation),
            observed_at=inputs.cutoff)
        if materializations is not None:
            materializations.append(result)
        setup = next(row for row in result.materialization.evidence if row.evidence_type is EvidenceType.TRADE_SETUP)
        payload = json.loads(setup.payload_json)
        rows.append(setup_summary_row(dict(ticker=ticker, payload=payload)))
        evidence.append(dict(ticker=ticker, evidence_id=str(setup.evidence_id), analysis_run_id=str(setup.analysis_run_id),
            security_revision_id=str(setup.security_revision_id), source_version=setup.source_version,
            market_time=setup.market_time.isoformat(), observed_at=setup.observed_at.isoformat(), payload_sha256=setup.payload_sha256,
            payload=payload, source_revision_ids=[str(value) for value in result.bar_revision_ids],
            confirmation_revision_ids=[str(value) for value in result.confirmation_bar_revision_ids],
            dependency_evidence_ids=[str(value) for value in setup.source_revision_ids],
            dependency_payload_sha256={str(row.evidence_id): row.payload_sha256 for row in result.materialization.evidence
                                      if row.evidence_id in setup.source_revision_ids}))
    return rows, evidence, missing
