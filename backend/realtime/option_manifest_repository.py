from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from uuid import NAMESPACE_URL, UUID, uuid5

from .option_manifest_cohort_repository import (
    DeveloperForwardCohortBinding,
    bind_ready_manifest_shadow_cohort_cursor,
    option_subscription_manifest_id,
    persist_option_universe_manifest_cursor,
)
from .option_session_manifest import (
    ContractSelectionStatus,
    ManifestSelectionStatus,
    SelectedContractManifestPlan,
)
from .option_slot_set import option_manifest_members, pre_session_slot_set
from .option_slot_set_repository import persist_and_activate_slot_set_cursor


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _call_put(contract_type) -> str:
    return "C" if contract_type.value == "CALL" else "P"


def option_manifest_selection_id(plan: SelectedContractManifestPlan) -> UUID:
    return uuid5(
        NAMESPACE_URL,
        f"realtime-option-manifest-selection:{plan.selection_sha256}",
    )


def persist_option_manifest_selection_cursor(
    cursor,
    plan: SelectedContractManifestPlan,
    *,
    effective_at: datetime,
    cohort_binding: DeveloperForwardCohortBinding | None = None,
    slot_policy_sha256: str | None = None,
) -> UUID:
    """Persist a selection audit and, for a READY plan, its V2 control plane.

    The manifest holds the equity universe only. When a cohort is bound (Developer SHADOW), the selected
    contracts become the PRE_SESSION slot set of that cohort and are activated, in the caller's transaction.
    A tier that binds no cohort here (Advanced) persists no slot set: its binder persists
    `pre_session_slot_set(...)` with `persist_and_activate_slot_set_cursor` once it has a cohort.
    `slot_policy_sha256` identifies the quote-slot policy; it defaults to the plan's allocation policy.
    """
    if cohort_binding is not None and plan.scope.universe.sha256 != cohort_binding.universe_sha256:
        raise ValueError("selection and forward cohort universe identities differ")
    effective = _utc(effective_at, "effective_at")
    if effective.date() > plan.session_date:
        raise ValueError("option manifest effective time cannot follow its session date")
    scope = plan.scope
    policy = plan.policy
    cursor.execute(
        "SELECT profile_sha256, provider_tier, universe_sha256 "
        "FROM rt.capability_profiles WHERE profile_id=%s",
        (str(scope.profile_id),),
    )
    expected_profile = (
        scope.profile_sha256,
        scope.provider_tier.value,
        scope.universe.sha256,
    )
    if cursor.fetchone() != expected_profile:
        raise ValueError("option manifest capability profile identity mismatch")

    cursor.execute(
        "SELECT revision.ticker, revision.security_id::text, instrument.instrument_id "
        "FROM ref.security_revisions AS revision "
        "JOIN rt.instruments AS instrument ON instrument.security_id=revision.security_id "
        "WHERE revision.ticker=ANY(%s) AND revision.effective_from <= %s "
        "AND (revision.effective_to IS NULL OR revision.effective_to > %s) "
        "ORDER BY revision.ticker",
        (list(scope.universe.equity_tickers), effective, effective),
    )
    equity_rows = tuple(
        (str(ticker), str(security_id), int(instrument_id))
        for ticker, security_id, instrument_id in cursor.fetchall()
    )
    equity_by_ticker = {
        ticker: (UUID(security_id), instrument_id)
        for ticker, security_id, instrument_id in equity_rows
    }
    if (
        set(equity_by_ticker) != set(scope.universe.equity_tickers)
        or len({row[1] for row in equity_by_ticker.values()}) != len(equity_by_ticker)
    ):
        raise ValueError("option manifest equity identities are incomplete or duplicated")

    dispositions_by_contract = {
        row.contract_id: row for row in plan.dispositions
    }
    contract_ids = tuple(sorted(dispositions_by_contract))
    version_ids = tuple(sorted(
        row.catalog_version_id for row in plan.dispositions
    ))
    if contract_ids:
        cursor.execute(
            "SELECT contract.contract_id, contract.contract_ticker, version.version_id, "
            "instrument.instrument_id FROM ref.option_contracts AS contract "
            "JOIN ref.option_contract_versions AS version "
            "ON version.contract_id=contract.contract_id "
            "LEFT JOIN rt.instruments AS instrument "
            "ON instrument.contract_id=contract.contract_id "
            "WHERE contract.contract_id=ANY(%s) AND version.version_id=ANY(%s) "
            "ORDER BY contract.contract_id",
            (list(contract_ids), list(version_ids)),
        )
        option_rows = tuple(
            (
                int(contract_id), str(contract_ticker), int(version_id),
                int(instrument_id) if instrument_id is not None else None,
            )
            for contract_id, contract_ticker, version_id, instrument_id
            in cursor.fetchall()
        )
    else:
        option_rows = ()
    option_by_contract = {
        contract_id: (ticker, version_id, instrument_id)
        for contract_id, ticker, version_id, instrument_id in option_rows
    }
    expected_option_identities = {
        row.contract_id: (row.contract_ticker, row.catalog_version_id)
        for row in plan.dispositions
    }
    if set(option_by_contract) != set(expected_option_identities):
        raise ValueError("option manifest catalog identities are incomplete")
    for contract_id, (expected_ticker, expected_version) in expected_option_identities.items():
        ticker, version_id, _ = option_by_contract[contract_id]
        if (ticker, version_id) != (expected_ticker, expected_version):
            raise ValueError("option manifest catalog identity conflict")

    manifest_id = option_subscription_manifest_id(plan)
    if plan.status is ManifestSelectionStatus.READY:
        if plan.manifest is None or manifest_id is None:
            raise ValueError("ready option selection requires a manifest")
        selected_tickers = {
            row.contract_ticker
            for row in plan.dispositions
            if row.status is ContractSelectionStatus.INCLUDED
        }
        selected_contract_ids = {
            row.contract_id
            for row in plan.dispositions
            if row.status is ContractSelectionStatus.INCLUDED
        }
        if any(
            option_by_contract[contract_id][2] is None
            for contract_id in selected_contract_ids
        ):
            raise ValueError("selected option manifest is missing instrument identities")
        manifest = plan.manifest
        contract_id_by_ticker = {
            row.contract_ticker: row.contract_id for row in plan.dispositions
        }
        slot_instrument_ids = {}
        for member in option_manifest_members(manifest):
            if member.provider_symbol not in selected_tickers:
                raise ValueError("option manifest member is not selected")
            _, catalog_version_id, instrument_id = option_by_contract[
                contract_id_by_ticker[member.provider_symbol]
            ]
            if catalog_version_id != member.catalog_version_id:
                raise ValueError("option manifest catalog identity conflict")
            slot_instrument_ids[member.provider_symbol] = instrument_id
        if set(slot_instrument_ids) != selected_tickers:
            raise ValueError("option manifest members do not match the selected contracts")
        persist_option_universe_manifest_cursor(
            cursor,
            manifest,
            manifest_id=manifest_id,
            profile_id=scope.profile_id,
            effective_at=effective,
            equity_instrument_ids={
                ticker: instrument_id
                for ticker, (_, instrument_id) in equity_by_ticker.items()
            },
        )
    elif plan.manifest is not None or manifest_id is not None:
        raise ValueError("blocked option selection cannot persist a manifest")

    selection_id = option_manifest_selection_id(plan)
    selected_count = plan.selected_contract_count
    excluded_count = len(plan.dispositions) - selected_count
    cursor.execute(
        "INSERT INTO rt.option_manifest_selection_audits ("
        "selection_id, selection_sha256, manifest_id, profile_id, provider_tier, "
        "universe_sha256, policy_sha256, source_evidence_sha256, session_date, status, "
        "reason_codes, selected_contract_count, excluded_contract_count, coverage_count, "
        "primary_option_quote_contracts, reserved_option_quote_contracts, "
        "maximum_option_quote_contracts) VALUES ("
        "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (selection_id) DO NOTHING",
        (
            str(selection_id), plan.selection_sha256,
            str(manifest_id) if manifest_id is not None else None,
            str(scope.profile_id), scope.provider_tier.value, scope.universe.sha256,
            policy.sha256, plan.source_evidence_sha256, plan.session_date,
            plan.status.value, list(plan.reason_codes), selected_count, excluded_count,
            len(plan.coverage), policy.primary_option_quote_contracts,
            policy.reserved_option_quote_contracts,
            policy.maximum_option_quote_contracts,
        ),
    )
    cursor.execute(
        "SELECT selection_sha256, manifest_id::text, profile_id::text, provider_tier, "
        "universe_sha256, policy_sha256, source_evidence_sha256, status, reason_codes, "
        "selected_contract_count, excluded_contract_count, coverage_count, "
        "primary_option_quote_contracts, reserved_option_quote_contracts, "
        "maximum_option_quote_contracts FROM rt.option_manifest_selection_audits "
        "WHERE selection_id=%s",
        (str(selection_id),),
    )
    expected_audit = (
        plan.selection_sha256,
        str(manifest_id) if manifest_id is not None else None,
        str(scope.profile_id),
        scope.provider_tier.value,
        scope.universe.sha256,
        policy.sha256,
        plan.source_evidence_sha256,
        plan.status.value,
        list(plan.reason_codes),
        selected_count,
        excluded_count,
        len(plan.coverage),
        policy.primary_option_quote_contracts,
        policy.reserved_option_quote_contracts,
        policy.maximum_option_quote_contracts,
    )
    stored_audit = cursor.fetchone()
    if stored_audit is None or tuple(stored_audit) != expected_audit:
        raise ValueError("persisted option manifest selection audit conflict")

    for row in plan.dispositions:
        cursor.execute(
            "INSERT INTO rt.option_manifest_contract_dispositions ("
            "selection_id, contract_id, catalog_version_id, contract_ticker, underlyer, "
            "expiration_date, calendar_dte, call_put, strike, day_volume, open_interest, "
            "disposition, reason_codes) VALUES ("
            "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (selection_id, contract_id) DO NOTHING",
            (
                str(selection_id), row.contract_id, row.catalog_version_id,
                row.contract_ticker, row.underlyer, row.expiration_date,
                row.calendar_dte, _call_put(row.contract_type), row.strike,
                row.day_volume, row.open_interest, row.status.value,
                list(row.reason_codes),
            ),
        )
    cursor.execute(
        "SELECT contract_id, catalog_version_id, contract_ticker, underlyer, "
        "expiration_date, calendar_dte, call_put, strike, day_volume, open_interest, "
        "disposition, reason_codes FROM rt.option_manifest_contract_dispositions "
        "WHERE selection_id=%s ORDER BY contract_id",
        (str(selection_id),),
    )
    stored_dispositions = tuple(cursor.fetchall())
    expected_dispositions = tuple(sorted((
        row.contract_id,
        row.catalog_version_id,
        row.contract_ticker,
        row.underlyer,
        row.expiration_date,
        row.calendar_dte,
        _call_put(row.contract_type),
        row.strike,
        row.day_volume,
        row.open_interest,
        row.status.value,
        list(row.reason_codes),
    ) for row in plan.dispositions))
    if stored_dispositions != expected_dispositions:
        raise ValueError("persisted option manifest dispositions are incomplete")

    for row in plan.coverage:
        security_id = equity_by_ticker[row.underlyer][0]
        cursor.execute(
            "INSERT INTO rt.option_manifest_coverage ("
            "selection_id, underlying_security_id, underlyer, target_dte, call_put, "
            "expiration_date, selected_count, required_count, status, reason_codes) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT DO NOTHING",
            (
                str(selection_id), str(security_id), row.underlyer, row.target_dte,
                _call_put(row.contract_type), row.expiration_date, row.selected_count,
                row.required_count, row.status, list(row.reason_codes),
            ),
        )
    cursor.execute(
        "SELECT underlying_security_id::text, underlyer, target_dte, call_put, "
        "expiration_date, selected_count, required_count, status, reason_codes "
        "FROM rt.option_manifest_coverage WHERE selection_id=%s "
        "ORDER BY underlyer, target_dte, call_put",
        (str(selection_id),),
    )
    stored_coverage = tuple(cursor.fetchall())
    expected_coverage = tuple(
        (
            str(equity_by_ticker[row.underlyer][0]),
            row.underlyer,
            row.target_dte,
            _call_put(row.contract_type),
            row.expiration_date,
            row.selected_count,
            row.required_count,
            row.status,
            list(row.reason_codes),
        )
        for row in sorted(
            plan.coverage,
            key=lambda item: (
                item.underlyer,
                item.target_dte,
                _call_put(item.contract_type),
            ),
        )
    )
    if stored_coverage != expected_coverage:
        raise ValueError("persisted option manifest coverage is incomplete")
    if manifest_id is not None:
        cohort_id = bind_ready_manifest_shadow_cohort_cursor(
            cursor,
            plan,
            manifest_id=manifest_id,
            effective_at=effective,
            cohort_binding=cohort_binding,
        )
        if cohort_id is not None:
            persist_and_activate_slot_set_cursor(
                cursor,
                pre_session_slot_set(
                    plan.manifest,
                    cohort_id=cohort_id,
                    instrument_ids=slot_instrument_ids,
                    effective_at=effective,
                    policy_sha256=slot_policy_sha256,
                ),
                activated_at=effective,
                allow_superseded=True,
            )
    return selection_id


def persist_option_manifest_selection(
    connection,
    plan: SelectedContractManifestPlan,
    *,
    effective_at: datetime,
    cohort_binding: DeveloperForwardCohortBinding | None = None,
    slot_policy_sha256: str | None = None,
) -> UUID:
    try:
        with connection.cursor() as cursor:
            selection_id = persist_option_manifest_selection_cursor(
                cursor,
                plan,
                effective_at=effective_at,
                cohort_binding=cohort_binding,
                slot_policy_sha256=slot_policy_sha256,
            )
        connection.commit()
        return selection_id
    except Exception:
        connection.rollback()
        raise
