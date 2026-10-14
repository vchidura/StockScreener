from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID
from zoneinfo import ZoneInfo

from options.analytics.eligible_chain_projection import eligible_chain_snapshot_row, query_stored_eligible_chain
from options.config import OptionRuntimeConfiguration
from options.discovery import CONTRACT_FILTERS, EligibleChainQuery

from .contracts import canonical_sha256
from .forward_ingestion import CompleteForwardMatrix, ForwardMatrixScope, OfflineForwardPublication, forward_detector_dataset_id
from .option_flow_projection import (
    DeveloperFlowChainEvidence, developer_policy_chain_input_sha256, validate_developer_policy_chain_bundle,
)
from .preflight import ProviderTier


SCREENER_INPUT_CONTRACT = "original_eligible_chain_inputs_v1"


def _json_row_default(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, UUID):
        return str(value)
    raise TypeError(f"Unsupported original Screener value: {type(value).__name__}")


@dataclass(frozen=True, slots=True)
class OfflineEligibleChainPublication(OfflineForwardPublication):
    input_sha256: str

    def query(self, request: EligibleChainQuery, *, cohort_id: UUID, provider_tier: ProviderTier, scope_sha256: str) -> dict:
        stored = self.read(cohort_id=cohort_id, provider_tier=provider_tier, scope_sha256=scope_sha256)
        if stored["contract"] != SCREENER_INPUT_CONTRACT:
            raise ValueError("stored Screener contract mismatch")
        result = query_stored_eligible_chain(request, rows=tuple(stored["rows"]), coverage=tuple(stored["coverage"]),
            underlyers=tuple(stored["underlyers"]), filter_values=stored["filter_values"])
        if not result["available"] and result["reason"] != "UNDERLYING_NOT_CONFIGURED" and stored["blocked"]:
            requested = {request.underlyer} if request.underlyer else set(stored["underlyers"])
            session_matches = request.session_date is None or request.session_date == datetime.fromisoformat(
                stored["cutoff"]).astimezone(ZoneInfo("America/New_York")).date()
            if session_matches and any(symbol in requested and "POLICY_ELIGIBLE_CHAIN_COVERAGE_UNAVAILABLE" in reasons
                    for symbol, reasons in stored["blocked"]):
                result["reason"] = "POLICY_ELIGIBLE_CHAIN_COVERAGE_UNAVAILABLE"
        return dict(result, data_tier="15-MINUTE DELAYED RESEARCH DATA", generated_at=stored["published_at"],
            policy_sha256=stored["policy_sha256"], model_version=None)


def seal_offline_eligible_chain_publication(
    scope: ForwardMatrixScope, matrices: tuple[CompleteForwardMatrix, ...], configuration: OptionRuntimeConfiguration,
    *, selected_at: datetime, chain_evidence: tuple[DeveloperFlowChainEvidence, ...] = (),
    unavailable_underlyers: tuple[tuple[str, tuple[str, ...]], ...] = (),
) -> OfflineEligibleChainPublication:
    members, blocked = validate_developer_policy_chain_bundle(scope, matrices, configuration,
        selected_at=selected_at, chain_evidence=chain_evidence, unavailable_underlyers=unavailable_underlyers)
    rows, coverage, filter_values = [], [], {}
    for matrix, evidence, received_count in members:
        market_time, observed_time = matrix.analysis.context.market_time, matrix.analysis.context.observed_time
        catalogs = {item.catalog.contract_id: item.catalog for item in evidence.inputs}
        eligible = []
        for snapshot in matrix.snapshots:
            row = eligible_chain_snapshot_row(snapshot, matrix_id=matrix.matrix_id, market_time=market_time,
                observed_time=observed_time, asset_type=catalogs[snapshot.contract_id].asset_type.value,
                policy_version=configuration.policy.policy_version, policy_sha256=configuration.policy_sha256)
            if row is not None:
                eligible.append(row)
                filter_values[str(snapshot.snapshot_id)] = {field: str(row[field]) for field in CONTRACT_FILTERS
                    if isinstance(row[field], Decimal)}
        rows.extend(eligible)
        coverage.append(dict(underlying=matrix.analysis.underlying.underlyer, batch_id=matrix.ingress.batch_id,
            matrix_id=matrix.matrix_id, market_time=market_time, observed_time=observed_time,
            received=received_count, retained=len(matrix.snapshots), eligible=len(eligible)))
    payload = dict(contract=SCREENER_INPUT_CONTRACT, underlyers=scope.underlyings, rows=rows, coverage=coverage,
        filter_values=filter_values, blocked=blocked, published_at=selected_at, cutoff=scope.cutoff, deadline=scope.deadline,
        policy_sha256=configuration.policy_sha256, configuration_sha256=configuration.configuration_sha256,
        reference_sha256=scope.reference_sha256, action_sha256=scope.action_sha256)
    input_sha256 = canonical_sha256(dict(contract=SCREENER_INPUT_CONTRACT,
        chain_input_sha256=developer_policy_chain_input_sha256(scope, matrices, chain_evidence)))
    return OfflineEligibleChainPublication(scope.cohort_id, scope.selection.scope.provider_tier, scope.sha256,
        forward_detector_dataset_id(scope, configuration),
        json.dumps(payload, default=_json_row_default, sort_keys=True, separators=(",", ":"), allow_nan=False), input_sha256)