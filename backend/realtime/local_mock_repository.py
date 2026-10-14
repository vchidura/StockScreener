from __future__ import annotations

from uuid import NAMESPACE_URL, UUID, uuid5

from psycopg2.extras import Json, execute_batch

from .local_mock_execution import LocalMockExecutionResult


LOCAL_MOCK_BATCH_PAGE_SIZE = 8


def _database_identity(row) -> tuple[object, ...] | None:
    if row is None:
        return None
    return tuple(str(value) if isinstance(value, UUID) else value for value in row)


def _policy_payload(result: LocalMockExecutionResult) -> dict[str, object]:
    policy = result.policy
    return {
        "version": policy.version,
        "maximum_quote_age_ms": policy.maximum_quote_age_ms,
        "maximum_quote_skew_ms": policy.maximum_quote_skew_ms,
        "slippage_per_share": str(policy.slippage_per_share),
        "fee_per_contract": str(policy.fee_per_contract),
        "maximum_capital_at_risk": str(policy.maximum_capital_at_risk),
        "maximum_total_contracts": policy.maximum_total_contracts,
    }


def persist_local_mock_execution(
    connection,
    result: LocalMockExecutionResult,
    *,
    batch_executor=execute_batch,
) -> UUID:
    if not isinstance(result, LocalMockExecutionResult):
        raise TypeError("local mock persistence result is invalid")
    disposition_id = uuid5(result.request_id, "terminal-disposition")
    package = result.package
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO rt.local_mock_requests ("
                "request_id, alert_id, candidate_id, cohort_id, "
                "candidate_identity_sha256, requested_at, request_origin, "
                "policy_version, policy_sha256, policy, earnings_status, "
                "earnings_reason_codes, evidence_batch_id, evidence_count"
                ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (request_id) DO NOTHING",
                (
                    str(result.request_id),
                    str(result.alert_id),
                    str(package.candidate_id),
                    str(package.cohort_id),
                    package.candidate_identity_sha256,
                    result.requested_at,
                    "PORTAL_MANUAL",
                    result.policy_version,
                    result.policy_sha256,
                    Json(_policy_payload(result)),
                    result.earnings.status.value,
                    list(result.earnings.reason_codes),
                    str(result.evidence_batch_id),
                    len(result.source_evidence_ids),
                ),
            )
            cursor.execute(
                "SELECT alert_id, candidate_id, cohort_id, candidate_identity_sha256, "
                "requested_at, request_origin, policy_version, policy_sha256, "
                "earnings_status, earnings_reason_codes, evidence_batch_id, evidence_count "
                "FROM rt.local_mock_requests WHERE request_id=%s",
                (str(result.request_id),),
            )
            expected_request = (
                str(result.alert_id),
                str(package.candidate_id),
                str(package.cohort_id),
                package.candidate_identity_sha256,
                result.requested_at,
                "PORTAL_MANUAL",
                result.policy_version,
                result.policy_sha256,
                result.earnings.status.value,
                list(result.earnings.reason_codes),
                str(result.evidence_batch_id),
                len(result.source_evidence_ids),
            )
            if _database_identity(cursor.fetchone()) != expected_request:
                raise ValueError("persisted local mock request identity conflict")
            input_rows = [
                (str(result.request_id), index, str(evidence_id))
                for index, evidence_id in enumerate(result.source_evidence_ids)
            ]
            batch_executor(
                cursor,
                "INSERT INTO rt.local_mock_request_inputs ("
                "request_id, input_index, evidence_id) VALUES (%s, %s, %s) "
                "ON CONFLICT (request_id, input_index) DO NOTHING",
                input_rows,
                page_size=LOCAL_MOCK_BATCH_PAGE_SIZE,
            )
            cursor.execute(
                "SELECT input_index, evidence_id FROM rt.local_mock_request_inputs "
                "WHERE request_id=%s ORDER BY input_index",
                (str(result.request_id),),
            )
            expected_inputs = tuple((row[1], row[2]) for row in input_rows)
            stored_inputs = tuple(_database_identity(row) for row in cursor.fetchall())
            if stored_inputs != expected_inputs:
                raise ValueError("persisted local mock input identity conflict")
            cursor.execute(
                "INSERT INTO rt.local_mock_dispositions ("
                "disposition_id, request_id, status, reason_codes, gross_cash_flow, "
                "estimated_fees, net_cash_flow, execution_permission, decided_at"
                ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (request_id) DO NOTHING",
                (
                    str(disposition_id),
                    str(result.request_id),
                    result.status.value,
                    list(result.reason_codes),
                    result.gross_cash_flow,
                    result.estimated_fees,
                    result.net_cash_flow,
                    result.execution_permission,
                    result.requested_at,
                ),
            )
            cursor.execute(
                "SELECT request_id, status, reason_codes, gross_cash_flow, "
                "estimated_fees, net_cash_flow, execution_permission, decided_at "
                "FROM rt.local_mock_dispositions WHERE disposition_id=%s",
                (str(disposition_id),),
            )
            expected_disposition = (
                str(result.request_id),
                result.status.value,
                list(result.reason_codes),
                result.gross_cash_flow,
                result.estimated_fees,
                result.net_cash_flow,
                result.execution_permission,
                result.requested_at,
            )
            if _database_identity(cursor.fetchone()) != expected_disposition:
                raise ValueError("persisted local mock disposition identity conflict")
            fill_rows = [
                (
                    str(fill.fill_id),
                    str(result.request_id),
                    fill.leg_index,
                    fill.instrument_id,
                    fill.contract_id,
                    fill.contract_ticker,
                    fill.side.value,
                    fill.quantity,
                    fill.multiplier,
                    fill.bid,
                    fill.ask,
                    fill.fill_price,
                    fill.fee,
                    str(fill.source_evidence_id),
                )
                for fill in result.fills
            ]
            if fill_rows:
                batch_executor(
                    cursor,
                    "INSERT INTO rt.local_mock_fills ("
                    "fill_id, request_id, leg_index, instrument_id, contract_id, "
                    "contract_ticker, side, quantity, multiplier, bid, ask, fill_price, "
                    "fee, source_evidence_id) VALUES ("
                    "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (fill_id) DO NOTHING",
                    fill_rows,
                    page_size=LOCAL_MOCK_BATCH_PAGE_SIZE,
                )
            cursor.execute(
                "SELECT fill_id, request_id, leg_index, instrument_id, contract_id, "
                "contract_ticker, side, quantity, multiplier, bid, ask, fill_price, fee, "
                "source_evidence_id FROM rt.local_mock_fills WHERE request_id=%s "
                "ORDER BY leg_index",
                (str(result.request_id),),
            )
            stored_fills = tuple(_database_identity(row) for row in cursor.fetchall())
            if stored_fills != tuple(fill_rows):
                raise ValueError("persisted local mock fill identity conflict")
        connection.commit()
        return disposition_id
    except Exception:
        connection.rollback()
        raise