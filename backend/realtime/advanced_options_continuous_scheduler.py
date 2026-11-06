"""Bind materialized live Equity intervals to the frozen Options session."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import NAMESPACE_URL, UUID, uuid5

from requests import RequestException, Timeout

from options.data.polygon_http import PolygonHttpResponse, SingleAttemptPolygonHttpTransport
from .acquisition_ledger import (
    ProviderRequestCompletion, ProviderRequestOutcome, ProviderRequestReservation,
    ProviderRequestState, RequestCompleteness, finish_provider_request_cursor,
    load_recorded_provider_request_cursor,
    load_raw_payload_cursor, persist_raw_payload_cursor, PriceBasis, ProviderRequestLane,
)
from .advanced_options_control_plane import ADVANCED_OPTIONS_REST_LANES
from .advanced_options_p8_production import (
    _P8ProductionCapture, p8_options_cycle_cutoff_from_capture,
)
from .advanced_options_p8_runtime import build_p8_downstream_binding
from .advanced_options_p8_selection_source import (
    MassiveAdvancedOptionsExactContractSource, combine_exact_contract_captures,
)
from .advanced_options_session_state import PostgresContinuousSessionStateRepository
from .advanced_options_control_plane import load_advanced_options_equity_input_binding_cursor
from .contracts import canonical_sha256
from .database_access_v2 import APP_ROLE
from .provider_budget import (
    MASSIVE_ADVANCED_ACCOUNT_DECODED_BYTE_CEILING,
    MASSIVE_ADVANCED_ACCOUNT_REQUEST_CEILING,
    ProviderBudgetScope, reserve_coordinated_provider_request_cursor,
)


UTC = timezone.utc
RESPONSE_BYTE_CEILING = 2_000_000
ACCOUNTED_RESPONSE_BYTE_CEILING = RESPONSE_BYTE_CEILING + 1
CONTRACTS_PER_REQUEST = 250
MAXIMUM_FROZEN_CONTRACTS = 1000
ATTEMPTS_PER_INTERVAL = 3
SESSION_REQUEST_CEILING = 78 * ATTEMPTS_PER_INTERVAL


class _RecordedTransport:
    def __init__(self, delegate=None, response=None):
        self.delegate = delegate
        self.response = response

    def get(self, url, params):
        if self.delegate is not None:
            self.response = self.delegate.get(url, params)
        if self.response is None:
            raise RuntimeError("Options snapshot response is unavailable")
        return self.response

    @property
    def body(self):
        return (
            self.response.body if self.response is not None
            else getattr(self.delegate, "response_body", b"")
        )

    @property
    def http_status(self):
        return (
            self.response.status_code if self.response is not None
            else getattr(self.delegate, "http_status", None)
        )

    @property
    def response_bytes(self):
        return (
            len(self.response.body) if self.response is not None
            else getattr(self.delegate, "response_bytes", 0)
        )


def _contract_batches(tickers):
    if (
        not 1 <= len(tickers) <= MAXIMUM_FROZEN_CONTRACTS
        or len(set(tickers)) != len(tickers)
        or tuple(sorted(tickers)) != tuple(tickers)
    ):
        raise ValueError("Options scheduler frozen membership is invalid")
    return tuple(
        tickers[offset:offset + CONTRACTS_PER_REQUEST]
        for offset in range(0, len(tickers), CONTRACTS_PER_REQUEST)
    )


def _scheduler_budget(acquisition, authorization_reference):
    request_ceiling = SESSION_REQUEST_CEILING * len(
        _contract_batches(acquisition.contract_tickers)
    )
    return ProviderBudgetScope(
        account_alias="MASSIVE_ADVANCED",
        session_date=acquisition.authority.session_date,
        authorization_reference=authorization_reference,
        provider_authorization_reference=authorization_reference,
        expected_cost="bounded frozen-contract snapshots for live E intervals",
        request_ceiling=request_ceiling,
        decoded_byte_ceiling=request_ceiling * ACCOUNTED_RESPONSE_BYTE_CEILING,
        account_request_ceiling=MASSIVE_ADVANCED_ACCOUNT_REQUEST_CEILING,
        account_decoded_byte_ceiling=MASSIVE_ADVANCED_ACCOUNT_DECODED_BYTE_CEILING,
        lanes=ADVANCED_OPTIONS_REST_LANES,
        authorized_until=acquisition.authority.acquisition_stop,
    )


def _finish_capture_request(
    provider, *, request_id, transport, started, completed, receipt,
    capture=None, error=None,
):
    status = transport.http_status
    outcome = (
        ProviderRequestOutcome.OK if error is None
        else ProviderRequestOutcome.HTTP_429 if status == 429
        else ProviderRequestOutcome.HTTP_ERROR if status is not None and status != 200
        else ProviderRequestOutcome.TIMEOUT if isinstance(error, Timeout)
        else ProviderRequestOutcome.HTTP_ERROR if isinstance(error, RequestException)
        else ProviderRequestOutcome.INVALID
    )
    with provider.transaction(APP_ROLE) as unit:
        payload_id = (
            persist_raw_payload_cursor(
                unit.cursor, transport.body, arrived_at=completed, request_id=request_id,
            ) if transport.body else None
        )
        finish_provider_request_cursor(
            unit.cursor, request_id,
            ProviderRequestCompletion(
                state=ProviderRequestState.COMPLETE if error is None else ProviderRequestState.FAILED,
                started_at=started, completed_at=completed, outcome=outcome,
                http_status=status, response_bytes=transport.response_bytes,
                row_count=None if capture is None else len(capture.selection.contracts),
                pages_fetched=int(status is not None),
                requested_from=receipt[1],
                requested_to=None if capture is None else capture.selection.reviewed_at,
                payload_id=payload_id, price_basis=PriceBasis.NOT_APPLICABLE,
                completeness=(
                    RequestCompleteness.COMPLETE if error is None
                    else RequestCompleteness.INCOMPLETE
                ),
                failure_code=None if error is None else type(error).__name__[:96],
            ),
        )


def _capture(provider, *, state, receipt, budget, api_key, clock, transport_factory, deadline):
    acquisition = state.load_acquisition_binding()
    batches = _contract_batches(acquisition.contract_tickers)
    for attempt in range(ATTEMPTS_PER_INTERVAL):
        captures = []
        for batch_index, tickers in enumerate(batches):
            started = clock()
            if started >= deadline:
                raise Timeout("Options snapshot interval deadline expired")
            suffix = "" if len(batches) == 1 else f":batch:{batch_index}"
            request_id = uuid5(
                NAMESPACE_URL,
                f"continuous-options:{acquisition.authority.authorization_id}:"
                f"{receipt[0]}:{attempt}{suffix}",
            )
            reservation = ProviderRequestReservation(
                request_id=request_id, account_alias=budget.account_alias,
                lane=ProviderRequestLane.OPTIONS_ALIGNMENT,
                endpoint_class="OPTIONS_UNIFIED_SNAPSHOT",
                request_sha256=canonical_sha256({
                    "receipt_id": receipt[0], "contracts": tickers, "attempt": attempt,
                }),
                reserved_at=started, cohort_id=acquisition.authority.cohort_id,
            )
            with provider.transaction(APP_ROLE) as unit:
                admission = reserve_coordinated_provider_request_cursor(
                    unit.cursor, budget, reservation,
                    reserved_response_bytes=ACCOUNTED_RESPONSE_BYTE_CEILING,
                    account_wide=True,
                )
                recorded = (
                    None if admission.newly_reserved
                    else load_recorded_provider_request_cursor(unit.cursor, request_id)
                )
                body = (
                    load_raw_payload_cursor(unit.cursor, recorded.completion.payload_id)
                    if recorded is not None
                    and recorded.completion.state is ProviderRequestState.COMPLETE
                    and recorded.completion.completeness is RequestCompleteness.COMPLETE
                    and recorded.completion.payload_id is not None
                    else None
                )
            if not admission.newly_reserved and body is None:
                if recorded is not None and recorded.completion.state is ProviderRequestState.FAILED:
                    break
                raise RuntimeError("Options snapshot has an ambiguous prior reservation")
            observed = started if recorded is None else recorded.completion.requested_to
            if observed is None:
                raise ValueError("Options snapshot replay lacks its original selection clock")
            transport = _RecordedTransport(
                delegate=(
                    transport_factory(
                        api_key, maximum_response_bytes=RESPONSE_BYTE_CEILING,
                        deadline=deadline, clock=clock,
                    ) if body is None else None
                ),
                response=None if body is None else PolygonHttpResponse(200, body),
            )
            try:
                if clock() >= deadline:
                    raise Timeout("Options snapshot interval deadline expired")
                capture = MassiveAdvancedOptionsExactContractSource(
                    transport, clock=clock if recorded is None else lambda: observed,
                    maximum_decoded_bytes=RESPONSE_BYTE_CEILING,
                ).capture(
                    contract_tickers=tickers, reviewed_at=observed,
                    session_date=acquisition.authority.session_date,
                )
                if clock() >= deadline:
                    raise Timeout("Options snapshot interval deadline expired")
            except (RequestException, RuntimeError, TypeError, ValueError) as error:
                if recorded is None:
                    _finish_capture_request(
                        provider, request_id=request_id, transport=transport,
                        started=started, completed=clock(), receipt=receipt, error=error,
                    )
                raise
            if recorded is None:
                _finish_capture_request(
                    provider, request_id=request_id, transport=transport,
                    started=started, completed=clock(), receipt=receipt, capture=capture,
                )
            captures.append(capture)
        else:
            if min(item.selection.valid_until for item in captures) > max(
                clock(), *(item.selection.reviewed_at for item in captures)
            ):
                return combine_exact_contract_captures(
                    tuple(captures), contract_tickers=acquisition.contract_tickers,
                )
    raise OptionsSnapshotStale("Options snapshot interval retry budget exhausted")


class OptionsSnapshotStale(RuntimeError):
    """G74: every attempt for this Equity window failed, or returned a snapshot already past its validity (a quiet
    contract). The request ids are fixed per window, so retrying (or restarting) replays the same recorded answers: the
    outcome cannot change until the next Equity window. The cycle reports a labelled skip instead of crashing."""


STALE_SNAPSHOT_SKIPPED = "SNAPSHOT_STALE_SKIPPED"
WAITING_AFTER_STALE_SNAPSHOT = "WAITING_AFTER_STALE_SNAPSHOT"


def run_continuous_options_schedule_cycle(
    provider, *, session_date, api_key,
    clock=lambda: datetime.now(UTC),
    transport_factory=SingleAttemptPolygonHttpTransport,
    skipped_receipts=frozenset(),
):
    """`skipped_receipts`: Equity INPUT_READY receipt ids this process already skipped as stale (G74); the cycle
    waits for the next Equity window instead of replaying them."""
    with provider.transaction(APP_ROLE) as unit:
        unit.cursor.execute(
            "SELECT pg_try_advisory_xact_lock(hashtextextended(%s,0))",
            (f"advanced-options-continuous-scheduler:{session_date}",),
        )
        if not unit.cursor.fetchone()[0]:
            return {"status": "SCHEDULER_BUSY"}
        return _run_schedule_cycle(
            provider, session_date=session_date, api_key=api_key,
            clock=clock, transport_factory=transport_factory,
            skipped_receipts=frozenset(skipped_receipts),
        )


def _stale_skip(receipt, reason):
    return {
        "status": STALE_SNAPSHOT_SKIPPED, "equity_receipt_id": receipt[0],
        "equity_cutoff": receipt[1].isoformat(), "reason": reason,
    }


def _run_schedule_cycle(
    provider, *, session_date, api_key, clock, transport_factory, skipped_receipts=frozenset(),
):
    from scripts.run_advanced_options_v3_production import _authorize_window

    checked = clock()
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        unit.cursor.execute(
            "SELECT desired_cohort_id::text FROM rt.advanced_options_continuous_sessions "
            "WHERE session_date=%s AND prepared_at<=%s AND decision_stop>%s",
            (session_date, checked, checked),
        )
        session_row = unit.cursor.fetchone()
        if session_row is None:
            return {"status": "WAITING_FOR_SESSION"}
        unit.cursor.execute(
            "SELECT receipt.receipt_id::text,receipt.cutoff,receipt.created_at,"
            "receipt.input_sha256 FROM rt.cycle_receipts AS receipt "
            "JOIN rt.window_runs AS window_run USING(window_id) "
            "WHERE receipt.cohort_id=%s AND receipt.receipt_kind='INPUT_READY' "
            "AND window_run.state='COMPLETE' AND receipt.cutoff<=%s "
            "AND receipt.cutoff>%s "
            "AND EXISTS (SELECT 1 FROM rt.stage_runs AS stage "
            "WHERE stage.window_id=receipt.window_id "
            "AND stage.stage_id='EQUITY_MATERIALIZE' AND stage.state='COMPLETE') "
            "ORDER BY receipt.cutoff DESC,receipt.created_at DESC LIMIT 1",
            (session_row[0], checked, checked - timedelta(seconds=300)),
        )
        receipt = unit.cursor.fetchone()
    if receipt is None:
        return {"status": "WAITING_FOR_MATERIALIZED_E"}
    state = PostgresContinuousSessionStateRepository(provider, session_date=session_date)
    package = state.load_package()
    bundle = state.load_bundle_document()
    session = state.load_preopen_session_binding(bundle)
    bindings = state.load_downstream_bindings(session)
    if any(item.finalized_cutoff == receipt[1] for item in bindings):
        return {"status": "ALREADY_ATTACHED", "equity_cutoff": receipt[1].isoformat()}
    if bindings and bindings[-1].finalized_cutoff > receipt[1]:
        raise ValueError("Options scheduler cannot regress its Equity cutoff")
    if receipt[0] in skipped_receipts:
        return {"status": WAITING_AFTER_STALE_SNAPSHOT, "equity_cutoff": receipt[1].isoformat()}
    acquisition = state.load_acquisition_binding()
    deadline = min(
        receipt[1] + timedelta(seconds=300), acquisition.authority.acquisition_stop,
    )
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        unit.cursor.execute(
            "SELECT options_cutoff FROM rt.advanced_options_window_cycle_authorizations "
            "WHERE session_authorization_id=%s AND equity_receipt_id=%s",
            (str(acquisition.authority.authorization_id), receipt[0]),
        )
        authorized_cutoffs = tuple(row[0] for row in unit.cursor.fetchall())
    if authorized_cutoffs:
        deadline = min(deadline, *authorized_cutoffs)
    budget = _scheduler_budget(acquisition, package["authorization_reference"])
    try:
        capture = _capture(
            provider, state=state, receipt=receipt, budget=budget, api_key=api_key,
            clock=clock, transport_factory=transport_factory, deadline=deadline,
        )
    except OptionsSnapshotStale:
        return _stale_skip(receipt, "OPTIONS_SNAPSHOT_RETRY_BUDGET_EXHAUSTED")
    observed = capture.selection.reviewed_at
    options_cutoff = min(
        p8_options_cycle_cutoff_from_capture(
            capture, checked_at=observed, stop=acquisition.authority.acquisition_stop,
        ),
        receipt[1] + timedelta(seconds=300),
    )
    if options_cutoff <= clock():
        # Deterministic for this Equity window as well: the next one gets a fresh capture.
        return _stale_skip(receipt, "OPTIONS_E_O_DEADLINE_MISSED")
    reference = f"{package['authorization_reference']}:E:{receipt[1].isoformat()}"
    authorized = _authorize_window(
        SimpleNamespace(
            window_key=f"E:{receipt[1].isoformat()}",
            options_cutoff=options_cutoff,
            window_deadline=min(
                options_cutoff + timedelta(seconds=300),
                acquisition.authority.acquisition_stop,
            ),
            equity_receipt_id=UUID(receipt[0]),
            authorization_reference=reference,
            publication_authorization_reference=f"{reference}:PUBLICATION",
            activity_evidence_sha256=capture.evidence["selection_sha256"],
        ),
        state=state, provider=provider, prepared_at=observed,
    )
    target_window_id = UUID(authorized["window_id"])
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        equity_input = load_advanced_options_equity_input_binding_cursor(
            unit.cursor, target_window_id,
        )
    factory = _P8ProductionCapture(provider, package, None, False)
    binding = factory.bind_continuous_selection(
        session_state=state, capture=capture, equity_input=equity_input,
        budget=budget, checked_at=observed,
    )
    # Discovery is current evidence only; frozen prior OI comes from activation.
    with provider.transaction(APP_ROLE) as unit:
        factory.persist_selection_cursor(unit.cursor, binding, persist_open_interest=False)
    previous = None if not bindings else bindings[-1]
    downstream = build_p8_downstream_binding(
        session_binding=session,
        sequence=1 if previous is None else previous.sequence + 1,
        previous_binding_sha256=None if previous is None else previous.binding_sha256,
        target_window_id=target_window_id, equity_receipt_id=UUID(receipt[0]),
        equity_input_sha256=receipt[3], finalized_cutoff=receipt[1],
        receipt_created_at=receipt[2], options_cutoff=options_cutoff,
        history_binding_sha256=binding.history.binding_sha256,
        equity_binding_sha256=equity_input.binding_sha256,
        options_activity_evidence_sha256=capture.evidence["selection_sha256"],
        window_cycle_authorization_sha256=authorized["window_cycle_authorization_sha256"],
        publication_authorization_reference=authorized["publication_authorization_reference"],
        production_binding=factory.production_binding_payload,
    )
    state.attach_downstream_binding(session, downstream)
    return {
        "status": "ATTACHED", "window_id": str(target_window_id),
        "equity_cutoff": receipt[1].isoformat(), "options_cutoff": options_cutoff.isoformat(),
        "contracts": len(acquisition.contract_tickers), "sequence": downstream.sequence,
    }
