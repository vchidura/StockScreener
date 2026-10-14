from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from uuid import NAMESPACE_URL, UUID, uuid5

from equity.domain import SecurityReferenceRevision
from options.calendar import OptionExchangeCalendar
from options.dual_origin import ActivitySource
from options.domain import OptionContractSnapshot
from options.credit_detection import O3_CREDIT_POLICY, O3_CREDIT_POLICY_SHA256
from options.dual_origin import assess_options_credit_canonical_trend
from options.intraday_participation import (
    build_canonical_trend_evidence,
    latest_completed_30m_window,
)
from options.strategies.domain import (
    CandidateKind,
    CandidateLeg,
    CandidateStatus,
    OptionCandidate,
    OptionSide,
    StructureRiskClass,
    StructureType,
    candidate_identity,
)
from options.strategies.payoff import evaluate_terminal_payoff

from .contracts import canonical_sha256
from .detector_adapters import (
    StableDetectorAdapterResult,
    run_canonical_stock_signal_adapter,
    run_o1_adapter,
    run_o2_adapter,
    run_o3_adapter,
)
from .developer_cycle_compute import DeveloperDetectorInvocation
from .preflight import DetectorFamily, ProviderTier


def _unavailable(
    detector_id: str,
    adapter_id: str,
    cohort_id: UUID,
    input_sha256: str,
    reason: str,
):
    return lambda: StableDetectorAdapterResult(
        adapter_id=adapter_id,
        cohort_id=cohort_id,
        provider_tier=ProviderTier.MASSIVE_DEVELOPER,
        input_sha256=input_sha256,
        status="UNAVAILABLE",
        output=None,
        reason_codes=(reason,),
    )


def build_target_developer_detector_bindings(
    *,
    cohort_id: UUID,
    ticker: str,
    security: SecurityReferenceRevision,
    bars: tuple,
    option_snapshots: tuple[OptionContractSnapshot, ...],
    split_clear: bool,
    checked_at: datetime,
) -> tuple[DeveloperDetectorInvocation, ...]:
    if not option_snapshots:
        raise ValueError("Developer detector binding requires option snapshots")
    snapshot = max(
        option_snapshots,
        key=lambda row: (
            (row.day_volume or 0) + (row.open_interest or 0),
            row.market_data_time,
            row.contract_ticker,
        ),
    )
    input_identity = {
        "cohort_id": cohort_id,
        "ticker": ticker,
        "security_revision_id": security.security_revision_id,
        "bars": tuple(
            (row.revision.bar_revision_id, row.revision.payload_sha256) for row in bars
        ),
        "option_snapshots": tuple(
            (row.snapshot_id, row.normalized_payload_sha256)
            for row in option_snapshots
        ),
        "checked_at": checked_at,
    }
    hashes = {
        detector: canonical_sha256({**input_identity, "detector": detector})
        for detector in (
            DetectorFamily.S1.value,
            DetectorFamily.S2.value,
            DetectorFamily.O1.value,
            DetectorFamily.O2.value,
            DetectorFamily.O3.value,
        )
    }
    market_cutoff = max(row.revision.bar_end for row in bars)
    stock_bars = tuple(
        (row.revision, row.revision.system_observed_at) for row in bars
    )
    session_close = OptionExchangeCalendar().session_close(
        market_cutoff.date()
    )
    observed_at = snapshot.revised_observed_at or snapshot.first_observed_at
    recorded_at = max(observed_at, snapshot.first_observed_at)
    activity = ActivitySource(
        security_id=security.security_id,
        underlyer=ticker,
        contract_id=snapshot.contract_id,
        contract_ticker=snapshot.contract_ticker,
        contract_type=snapshot.contract_type.value,
        expiration_date=snapshot.expiration_date,
        expiration_cutoff=snapshot.expiration_cutoff,
        snapshot_id=snapshot.snapshot_id,
        snapshot_payload_sha256=snapshot.normalized_payload_sha256,
        batch_id=snapshot.batch_id,
        matrix_id=uuid5(
            NAMESPACE_URL,
            f"developer-cycle-matrix:{cohort_id}:{ticker}:{snapshot.batch_id}",
        ),
        configuration_sha256=snapshot.valuation_policy_sha256,
        market_policy_sha256=canonical_sha256({
            "version": "developer-cycle-option-activity-v1",
            "valuation_policy_sha256": snapshot.valuation_policy_sha256,
        }),
        volume_session=snapshot.market_data_time.date(),
        day_volume=snapshot.day_volume,
        open_interest=snapshot.open_interest,
        oi_settlement_session=OptionExchangeCalendar().previous_session(
            snapshot.market_data_time.date()
        ),
        oi_observed_at=observed_at,
        market_time=snapshot.market_data_time,
        observed_at=observed_at,
        recorded_at=recorded_at,
        received_at=max(checked_at, recorded_at),
        revised_observed_at=snapshot.revised_observed_at,
    )

    def bar_row(row):
        revision = row.revision
        return {
            "bar_revision_id": revision.bar_revision_id,
            "bar_end": revision.bar_end,
            "session_date": revision.session_date,
            "high_price": revision.high_price,
            "low_price": revision.low_price,
            "close_price": revision.close_price,
            "volume": revision.volume,
            "created_at": revision.system_observed_at,
            "system_observed_at": revision.system_observed_at,
        }

    trend_rows = tuple(
        bar_row(row) for row in bars if row.revision.interval == "30m"
    )
    daily_rows = tuple(
        bar_row(row) for row in bars if row.revision.interval == "1d"
    )
    o3_reason = None
    o3_work = None
    try:
        required_window = latest_completed_30m_window(activity.market_time)
        stock = build_canonical_trend_evidence(
            security_id=security.security_id,
            underlyer=ticker,
            session_date=activity.volume_session,
            required_window=required_window,
            trend_bars=trend_rows,
            daily_bars=daily_rows,
            split_clear=split_clear,
            received_at=activity.received_at,
            session_close=session_close,
        )
        if stock.status != "READY" or stock.ema50_slope10_atr == 0:
            raise ValueError("CANONICAL_TREND_DIRECTION_UNAVAILABLE")
        direction = 1 if stock.ema50_slope10_atr > 0 else -1
        contract_type = "PUT" if direction == 1 else "CALL"
        eligible = tuple(
            row for row in option_snapshots
            if row.contract_type.value == contract_type
            and O3_CREDIT_POLICY["minimum_entry_dte"]
            <= (row.expiration_date - row.market_data_time.date()).days
            <= O3_CREDIT_POLICY["maximum_entry_dte"]
            and (row.open_interest or 0) >= O3_CREDIT_POLICY["minimum_leg_open_interest"]
            and (row.day_volume or 0) >= O3_CREDIT_POLICY["minimum_leg_day_volume"]
            and row.model_mark is not None
            and all(value is not None for value in (
                row.local_iv,
                row.local_delta,
                row.local_gamma,
                row.local_theta_per_day,
                row.local_vega_per_vol_point,
                row.local_rho_per_rate_point,
            ))
        )
        grouped = {}
        for row in eligible:
            grouped.setdefault(row.expiration_date, []).append(row)
        pair = None
        invalidation = Decimal(str(stock.prior_session_close))
        for expiration in sorted(grouped):
            ordered = sorted(grouped[expiration], key=lambda row: row.strike)
            if direction == 1:
                candidates = [row for row in ordered if row.strike <= invalidation]
                if len(candidates) >= 2:
                    pair = (candidates[-2], candidates[-1])
                    break
            else:
                candidates = [row for row in ordered if row.strike >= invalidation]
                if len(candidates) >= 2:
                    pair = (candidates[0], candidates[1])
                    break
        if pair is None:
            raise ValueError("O3_CREDIT_VERTICAL_UNAVAILABLE")
        long_snapshot, short_snapshot = (
            pair if direction == 1 else (pair[1], pair[0])
        )
        activity_snapshot = short_snapshot
        activity_observed = (
            activity_snapshot.revised_observed_at
            or activity_snapshot.first_observed_at
        )
        o3_activity = ActivitySource.model_validate({
            **activity.model_dump(),
            "contract_id": activity_snapshot.contract_id,
            "contract_ticker": activity_snapshot.contract_ticker,
            "contract_type": activity_snapshot.contract_type.value,
            "expiration_date": activity_snapshot.expiration_date,
            "expiration_cutoff": activity_snapshot.expiration_cutoff,
            "snapshot_id": activity_snapshot.snapshot_id,
            "snapshot_payload_sha256": activity_snapshot.normalized_payload_sha256,
            "batch_id": activity_snapshot.batch_id,
            "day_volume": activity_snapshot.day_volume,
            "open_interest": activity_snapshot.open_interest,
            "market_time": activity_snapshot.market_data_time,
            "observed_at": activity_observed,
            "recorded_at": activity_observed,
            "received_at": max(checked_at, activity_observed),
            "revised_observed_at": activity_snapshot.revised_observed_at,
        })
        decision = assess_options_credit_canonical_trend(
            o3_activity,
            stock,
            direction=direction,
            market_cutoff=o3_activity.market_time,
            decision_at=o3_activity.received_at,
        )
        if decision.disposition != "CONFIRMED":
            raise ValueError("O3_OPTIONS_FIRST_DIRECTION_UNCONFIRMED")

        def leg(index, row, side):
            return CandidateLeg(
                leg_index=index,
                snapshot_id=row.snapshot_id,
                contract_id=row.contract_id,
                contract_ticker=row.contract_ticker,
                side=side,
                ratio=1,
                multiplier=row.shares_per_contract,
                expiration_date=row.expiration_date,
                strike=row.strike,
                contract_type=row.contract_type,
                spot=row.spot,
                time_to_expiration_years=row.time_to_expiration_years,
                risk_free_rate=row.risk_free_rate,
                dividend_yield=row.dividend_yield,
                model_mark=row.model_mark,
                local_iv=row.local_iv,
                local_delta=row.local_delta,
                local_gamma=row.local_gamma,
                local_theta_per_day=row.local_theta_per_day,
                local_vega_per_vol_point=row.local_vega_per_vol_point,
                local_rho_per_rate_point=row.local_rho_per_rate_point,
                source_market_time=row.market_data_time,
                mark_source=row.mark_source.value,
                model_version=row.model_version,
                quality_flags=tuple(flag.value for flag in row.quality_flags),
                valuation_policy_version=row.valuation_policy_version,
                valuation_policy_sha256=row.valuation_policy_sha256,
            )

        legs = (
            leg(0, long_snapshot, OptionSide.BUY),
            leg(1, short_snapshot, OptionSide.SELL),
        )
        structure = (
            StructureType.PUT_CREDIT_VERTICAL
            if direction == 1
            else StructureType.CALL_CREDIT_VERTICAL
        )
        matrix_id = uuid5(
            NAMESPACE_URL,
            f"developer-cycle-o3-matrix:{cohort_id}:{ticker}:"
            f"{o3_activity.market_time.isoformat()}",
        )
        candidate_id, identity_sha256 = candidate_identity(
            matrix_id,
            "SPREAD_RANGE_LOCATOR",
            O3_CREDIT_POLICY["version"],
            structure,
            tuple(row.contract_id for row in legs),
            "CANONICAL_TREND_ACTIVITY",
        )
        payoff = evaluate_terminal_payoff(legs)
        candidate = OptionCandidate(
            candidate_id=candidate_id,
            identity_sha256=identity_sha256,
            matrix_id=matrix_id,
            strategy_name="SPREAD_RANGE_LOCATOR",
            strategy_version=O3_CREDIT_POLICY["version"],
            underlyer=ticker,
            candidate_kind=CandidateKind.MULTI_LEG,
            strategy_archetype="REWARD_RISK",
            persona_tags=("O3",),
            structure_type=structure,
            structure_risk_class=StructureRiskClass.DEFINED_RISK_CREDIT,
            expiration_date=long_snapshot.expiration_date,
            rank=1,
            status=CandidateStatus.SELECTED,
            primary_metric_name="CANONICAL_TREND_ACTIVITY",
            primary_metric_value=abs(stock.ema50_slope10_atr),
            rank_components={},
            primary_evidence={
                "activity_contract_id": o3_activity.contract_id,
                "trend_source_sha256": stock.sha256,
            },
            legs=legs,
            net_premium=payoff.net_premium,
            collateral_required=None,
            capital_at_risk=payoff.maximum_loss,
            maximum_profit=payoff.maximum_profit,
            maximum_loss=payoff.maximum_loss,
            return_on_collateral=None,
            return_on_risk=(
                float(payoff.maximum_profit / payoff.maximum_loss)
                if payoff.maximum_profit is not None and payoff.maximum_loss
                else None
            ),
            breakevens=payoff.breakevens,
            execution_eligibility=None,
            reason_codes=(),
            management_policy_version=None,
            management_policy={},
            policy_sha256=O3_CREDIT_POLICY_SHA256,
            model_version=long_snapshot.model_version,
            context_snapshot_id=None,
            iv_context_id=None,
            market_data_time=max(row.market_data_time for row in pair),
            observed_time=max(
                row.revised_observed_at or row.first_observed_at for row in pair
            ),
            valid_until=min(
                decision.valid_until,
                checked_at + timedelta(minutes=15),
            ),
        )
        o3_work = (
            candidate,
            decision,
            o3_activity.contract_id,
            pair,
            invalidation,
            o3_activity.market_time,
        )
    except (TypeError, ValueError, ArithmeticError) as error:
        o3_reason = str(error) or type(error).__name__
    return (
        DeveloperDetectorInvocation(
            DetectorFamily.S1.value,
            hashes[DetectorFamily.S1.value],
            lambda: run_canonical_stock_signal_adapter(
                detector_id=DetectorFamily.S1.value,
                security_id=security.security_id,
                underlyer=ticker,
                bars=stock_bars,
                cohort_id=cohort_id,
                provider_tier=ProviderTier.MASSIVE_DEVELOPER,
                input_sha256=hashes[DetectorFamily.S1.value],
                market_cutoff=market_cutoff,
                decision_at=max(checked_at, market_cutoff),
                session_close=session_close,
            ),
        ),
        DeveloperDetectorInvocation(
            DetectorFamily.S2.value,
            hashes[DetectorFamily.S2.value],
            lambda: run_canonical_stock_signal_adapter(
                detector_id=DetectorFamily.S2.value,
                security_id=security.security_id,
                underlyer=ticker,
                bars=stock_bars,
                cohort_id=cohort_id,
                provider_tier=ProviderTier.MASSIVE_DEVELOPER,
                input_sha256=hashes[DetectorFamily.S2.value],
                market_cutoff=market_cutoff,
                decision_at=max(checked_at, market_cutoff),
                session_close=session_close,
            ),
        ),
        DeveloperDetectorInvocation(
            DetectorFamily.O1.value,
            hashes[DetectorFamily.O1.value],
            lambda: run_o1_adapter(
                activity,
                cohort_id=cohort_id,
                provider_tier=ProviderTier.MASSIVE_DEVELOPER,
                input_sha256=hashes[DetectorFamily.O1.value],
                market_cutoff=activity.market_time,
                decision_at=activity.received_at,
            ),
        ),
        DeveloperDetectorInvocation(
            DetectorFamily.O2.value,
            hashes[DetectorFamily.O2.value],
            lambda: run_o2_adapter(
                object(),
                None,
                cohort_id=cohort_id,
                provider_tier=ProviderTier.MASSIVE_DEVELOPER,
                input_sha256=hashes[DetectorFamily.O2.value],
                market_cutoff=activity.market_time,
                decision_at=activity.received_at,
            ),
        ),
        DeveloperDetectorInvocation(
            DetectorFamily.O3.value,
            hashes[DetectorFamily.O3.value],
            (
                lambda: run_o3_adapter(
                    o3_work[0],
                    o3_work[1],
                    cohort_id=cohort_id,
                    provider_tier=ProviderTier.MASSIVE_DEVELOPER,
                    input_sha256=hashes[DetectorFamily.O3.value],
                    activity_contract_id=o3_work[2],
                    snapshots=o3_work[3],
                    structural_invalidation=o3_work[4],
                    scheduled_cycle=o3_work[5],
                )
            )
            if o3_work is not None
            else _unavailable(
                DetectorFamily.O3.value,
                "options-o3-credit",
                cohort_id,
                hashes[DetectorFamily.O3.value],
                o3_reason or "O3_INPUT_UNAVAILABLE",
            ),
        ),
    )