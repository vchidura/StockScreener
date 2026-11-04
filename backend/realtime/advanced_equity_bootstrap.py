"""Explicit one-time history bootstrap through ordinary C4 chunk completion."""
from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta
from uuid import UUID

from options.calendar import OptionExchangeCalendar

from .advanced_equity_preparation import (
    EquityPreparationChunk,
    EquityPreparationClaim,
    EquityPreparationLimits,
    EquityPreparationStage,
    PreparationTransactionProvider,
    _coverage_with_count,
    _json_object,
    _preparation_provider_request_slots,
)
from .advanced_realtime_desired_state import DesiredMemberKind
from .contracts import canonical_sha256
from .database_access_v2 import APP_ROLE
from .equity_history import (
    EquityHistoryIdentityError,
    EquityHistoryRead,
    equity_history_coordinates,
    load_equity_history_cursor,
)


class VerifiedEquityBootstrapHistory:
    def __init__(
        self,
        provider: PreparationTransactionProvider,
        *,
        limits: EquityPreparationLimits = EquityPreparationLimits(),
        calendar: OptionExchangeCalendar | None = None,
    ) -> None:
        self.provider = provider
        self.limits = limits
        self.calendar = calendar or OptionExchangeCalendar("XNYS")

    def __call__(
        self,
        claim: EquityPreparationClaim,
        checked_at: datetime,
    ) -> tuple[EquityPreparationChunk | None, tuple[date, ...]]:
        if (
            claim.stage not in {
                EquityPreparationStage.ADJUSTED_DAILY,
                EquityPreparationStage.NATIVE_INTRADAY,
                EquityPreparationStage.DERIVED_INTERVALS,
            }
            or claim.security_id is None
            or claim.instrument_id is None
        ):
            return None, ()
        derived = claim.stage is EquityPreparationStage.DERIVED_INTERVALS
        index_key = "target_index" if derived else "interval_index"
        index = int(claim.watermark.get(index_key, 0))
        session_index = int(claim.watermark.get("session_index", 0))
        if derived:
            intervals = tuple(str(value) for value in claim.target["target_intervals"])
            sessions = tuple(
                date.fromisoformat(str(value))
                for value in claim.target["source_sessions"]
            )
        else:
            if "interval_sessions" not in claim.target:
                return None, ()
            by_interval = _json_object(claim.target["interval_sessions"], "history sessions")
            intervals = tuple(by_interval)
            if index >= len(intervals):
                raise EquityHistoryIdentityError("history watermark is exhausted")
            sessions = tuple(
                date.fromisoformat(str(value))
                for value in by_interval[intervals[index]]
            )
        if index >= len(intervals) or session_index >= len(sessions):
            raise EquityHistoryIdentityError("history watermark is exhausted")
        interval = intervals[index]
        adjusted = claim.stage is EquityPreparationStage.ADJUSTED_DAILY
        if adjusted and interval == "1wk":
            if len(sessions) > self.limits.maximum_facts_per_chunk:
                raise EquityHistoryIdentityError("weekly revalidation exceeds the configured fact bound")
            return None, sessions[session_index:]
        largest_session = max(
            len(equity_history_coordinates(self.calendar, interval, session))
            for session in sessions[session_index:]
        )
        if largest_session > self.limits.maximum_facts_per_chunk:
            raise EquityHistoryIdentityError("bootstrap session exceeds the configured fact bound")
        maximum = max(1, self.limits.maximum_facts_per_chunk // largest_session)
        selected = sessions[session_index:session_index + maximum]
        with self.provider.transaction(
            APP_ROLE, read_only=True, dict_cursor=True,
        ) as unit:
            history = load_equity_history_cursor(
                unit.cursor,
                cohort_id=claim.source_cohort_id,
                security_id=claim.security_id,
                ticker=claim.ticker,
                interval=interval,
                adjusted=adjusted,
                sessions=selected,
                checked_at=checked_at,
                source_kind="DERIVED" if derived else "NATIVE_REST",
                adjustments_through=date.fromisoformat(str(
                    claim.target.get("history_through_session", sessions[-1])
                )),
            )
            sources = (
                load_equity_history_cursor(
                    unit.cursor,
                    cohort_id=claim.source_cohort_id,
                    security_id=claim.security_id,
                    ticker=claim.ticker,
                    interval="5m",
                    adjusted=False,
                    sessions=selected,
                    checked_at=checked_at,
                    source_kind="NATIVE_REST",
                )
                if derived and history.rows else None
            )
        valid = []
        for session in selected:
            rows = tuple(row for row in history.rows if row["session_date"] == session)
            coordinates = equity_history_coordinates(self.calendar, interval, session)
            complete = (
                len(rows) == len(coordinates)
                and {(row["bar_start"], row["bar_end"]) for row in rows}
                == set(coordinates)
                and not (adjusted and any(row["stale_adjustment_lineage"] for row in rows))
            )
            if complete and sources is not None:
                source_coordinates = equity_history_coordinates(self.calendar, "5m", session)
                source_rows = tuple(
                    row for row in sources.rows if row["session_date"] == session
                )
                complete = (
                    len(source_rows) == len(source_coordinates)
                    and {(row["bar_start"], row["bar_end"]) for row in source_rows}
                    == set(source_coordinates)
                    and all(
                        tuple(str(value) for value in row["source_revision_ids"])
                        == tuple(
                            str(source["bar_revision_id"]) for source in source_rows
                            if source["bar_start"] >= row["bar_start"]
                            and source["bar_end"] <= row["bar_end"]
                        )
                        for row in rows
                    )
                )
            valid.append(complete)
        reuse = valid[0]
        count = next(
            (offset for offset, value in enumerate(valid) if value != reuse),
            len(valid),
        )
        chosen = selected[:count]
        if not reuse:
            return None, chosen[:20 if adjusted else 1]
        adopted = EquityHistoryRead(
            tuple(row for row in history.rows if row["session_date"] in chosen),
            history.binding_sha256,
        )
        next_session = session_index + count
        next_index = index
        if next_session == len(sessions):
            next_index += 1
            next_session = 0
        return EquityPreparationChunk(
            stage=claim.stage,
            watermark_after={index_key: next_index, "session_index": next_session},
            coverage_after=_coverage_with_count(
                claim.coverage,
                f"{interval}:{'ADJUSTED' if adjusted else 'RAW_UNADJUSTED'}",
                len(adopted.rows),
            ),
            source_lineage={
                "provider": "STORED",
                "owner": "advanced_equity_preparation",
                "mode": "ONE_TIME_BOOTSTRAP",
                "request_sha256": canonical_sha256({
                    "target": claim.target,
                    "watermark": claim.watermark,
                    "sessions": chosen,
                }),
                "response_sha256": canonical_sha256(adopted.lineage),
                "adopted_history": adopted.lineage,
            },
            complete=next_index == len(intervals),
        ), ()


def plan_equity_bootstrap(
    provider: PreparationTransactionProvider,
    *,
    revision_id: UUID,
    checked_at: datetime,
    limits: EquityPreparationLimits = EquityPreparationLimits(),
) -> dict[str, object]:
    """Project chunk coordinates in memory; never claim or complete stored work."""
    with provider.transaction(APP_ROLE, read_only=True, dict_cursor=True) as unit:
        unit.cursor.execute(
            "SELECT work.*,revision.source_cohort_id "
            "FROM rt.realtime_equity_preparation_work AS work "
            "JOIN rt.realtime_desired_revisions AS revision USING(revision_id) "
            "WHERE work.revision_id=%s ORDER BY work.ticker,work.stage_order",
            (str(revision_id),),
        )
        work_rows = tuple(unit.cursor.fetchall())
        if not work_rows:
            raise EquityHistoryIdentityError("bootstrap revision has no preparation work")
        source_ids = {str(row["source_cohort_id"]) for row in work_rows}
        if len(source_ids) != 1:
            raise EquityHistoryIdentityError("bootstrap source cohorts diverged")
        unit.cursor.execute(
            "SELECT DISTINCT ON (reference.ticker) reference.ticker,"
            "instrument.security_id,member.instrument_id "
            "FROM rt.cohorts AS cohort "
            "JOIN rt.subscription_members AS member USING(manifest_id) "
            "JOIN rt.instruments AS instrument USING(instrument_id) "
            "JOIN ref.security_revisions AS reference USING(security_id) "
            "WHERE cohort.cohort_id=%s AND member.channel='AM' "
            "AND reference.active AND reference.observed_at<=%s "
            "AND reference.effective_from<=%s "
            "AND (reference.effective_to IS NULL OR reference.effective_to>%s) "
            "ORDER BY reference.ticker,reference.observed_at DESC,reference.revision_id",
            (next(iter(source_ids)), checked_at, checked_at, checked_at),
        )
        identities = {str(row["ticker"]): row for row in unit.cursor.fetchall()}
    history = VerifiedEquityBootstrapHistory(provider, limits=limits)
    members = {}
    for work in work_rows:
        ticker = str(work["ticker"])
        member = members.setdefault(ticker, {
            "consumed_requests": 0, "consumed_bytes": 0,
            "estimated_stored_chunks": 0, "estimated_missing_chunks": 0,
            "estimated_provider_requests": 0, "estimated_provider_chunks": 0,
            "missing": [],
        })
        member["consumed_requests"] += int(work["provider_request_count"])
        member["consumed_bytes"] += int(work["response_bytes"])
        if work["status"] == "COMPLETE":
            continue
        if work["stage"] in {"REFERENCE", "TECHNICAL_WARMUP"}:
            member["estimated_missing_chunks"] += 1
            member["estimated_provider_requests"] += _preparation_provider_request_slots(
                EquityPreparationStage(work["stage"])
            )
            member["estimated_provider_chunks"] += int(work["stage"] == "REFERENCE")
            continue
        identity = identities.get(ticker)
        if identity is None and (work["security_id"] is None or work["instrument_id"] is None):
            raise EquityHistoryIdentityError(f"bootstrap planning identity unavailable: {ticker}")
        security_id = (
            work["security_id"] if work["security_id"] is not None else identity["security_id"]
        )
        instrument_id = (
            work["instrument_id"] if work["instrument_id"] is not None else identity["instrument_id"]
        )
        claim = EquityPreparationClaim(
            revision_id=revision_id, intent_id=UUID(str(work["intent_id"])),
            security_id=UUID(str(security_id)),
            instrument_id=int(instrument_id),
            member_kind=DesiredMemberKind(work["member_kind"]), ticker=ticker,
            stage=EquityPreparationStage(work["stage"]), stage_order=int(work["stage_order"]),
            target=dict(work["target"]), watermark=dict(work["watermark"]),
            coverage=dict(work["coverage"]), attempts=int(work["attempts"]),
            failure_streak=int(work["failure_streak"]),
            lease_owner=UUID(int=1), lease_epoch=0,
            lease_until=checked_at + timedelta(seconds=limits.lease_seconds),
            source_cohort_id=UUID(str(work["source_cohort_id"])),
        )
        for _chunk in range(10_000):
            stored, missing = history(claim, checked_at)
            if stored is not None:
                member["estimated_stored_chunks"] += 1
                if stored.complete:
                    break
                claim = replace(claim, watermark=stored.watermark_after, coverage=stored.coverage_after)
                continue
            if not missing:
                raise EquityHistoryIdentityError("bootstrap plan failed to advance its coordinates")
            derived = claim.stage is EquityPreparationStage.DERIVED_INTERVALS
            index_key = "target_index" if derived else "interval_index"
            index = int(claim.watermark.get(index_key, 0))
            interval_sessions = claim.target.get("interval_sessions", {})
            intervals = (
                tuple(claim.target["target_intervals"])
                if derived else tuple(interval_sessions)
            )
            sessions = (
                claim.target["source_sessions"] if derived else interval_sessions[intervals[index]]
            )
            member["estimated_missing_chunks"] += 1
            member["estimated_provider_requests"] += _preparation_provider_request_slots(claim.stage)
            member["estimated_provider_chunks"] += int(
                _preparation_provider_request_slots(claim.stage) > 0
            )
            member["missing"].append({
                "stage": claim.stage.value, "interval": intervals[index],
                "sessions": [str(session) for session in missing],
            })
            session_index = int(claim.watermark.get("session_index", 0)) + len(missing)
            if session_index == len(sessions):
                index += 1
                session_index = 0
            if index == len(intervals):
                break
            claim = replace(claim, watermark={index_key: index, "session_index": session_index})
        else:
            raise EquityHistoryIdentityError("bootstrap planning chunk bound exceeded")
    for member in members.values():
        member["estimated_maximum_response_bytes"] = (
            member["estimated_provider_chunks"] * limits.maximum_response_bytes_per_chunk
        )
        member["fits_remaining_request_budget"] = (
            member["consumed_requests"] + member["estimated_provider_requests"]
            <= limits.maximum_requests_per_member
        )
        member["fits_remaining_response_budget"] = (
            member["consumed_bytes"] + member["estimated_maximum_response_bytes"]
            <= limits.maximum_response_bytes_per_member
        )
    return {
        "mode": "READ_ONLY_BOOTSTRAP_PLAN",
        "revision_id": str(revision_id), "checked_at": checked_at.isoformat(),
        "database_writes": 0, "provider_requests": 0, "members": members,
        "fits_remaining_request_budgets": all(
            member["fits_remaining_request_budget"] for member in members.values()
        ),
        "fits_remaining_response_budgets": all(
            member["fits_remaining_response_budget"] for member in members.values()
        ),
        "account_admission": "EXISTING_COORDINATED_RESERVATIONS",
        "reference_refresh_may_change_adjustment_validity": True,
        "consumer_acceptance": "NOT_CHECKED",
    }
