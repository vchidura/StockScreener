from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

from .database_access import RealtimeDatabaseRole
from .option_manifest_repository import persist_option_manifest_selection_cursor
from .option_pre_session_producer import (
    OptionPreSessionProducer, OptionPreSessionProducerResult,
)
from .option_pre_session_staging import (
    StagedOptionPackageReceipt, claim_staged_package_cursor,
    complete_staged_package_cursor, fail_staged_package_cursor,
    load_staged_package_receipts_cursor, load_staged_usage_cursor,
    option_pre_session_run_id, persist_staged_finalization_cursor,
    persist_staged_run_cursor, assemble_staged_collection,
)
from .option_pre_session_target import (
    OptionPreSessionTargetPlan, load_option_pre_session_request_cursor,
    persist_option_pre_session_package_cursor,
)
from .option_session_manifest import build_bounded_option_session_manifest
from .worker_database import RealtimeProcessDatabase


def _utc(value, name):
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class PreparedStagedOptionEnrollment:
    process_plan_sha256: str
    configuration_sha256: str
    run_id: UUID
    target_plan: OptionPreSessionTargetPlan
    collection_deadline: datetime
    prepared_at: datetime

    def __post_init__(self):
        for digest in (self.process_plan_sha256, self.configuration_sha256):
            if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
                raise ValueError("prepared staged enrollment requires reviewed hashes")
        deadline = _utc(self.collection_deadline, "collection_deadline")
        prepared = _utc(self.prepared_at, "prepared_at")
        if not self.target_plan.scheduled_cycle <= prepared < deadline:
            raise ValueError("prepared staged enrollment clocks are invalid")
        object.__setattr__(self, "collection_deadline", deadline)
        object.__setattr__(self, "prepared_at", prepared)


class StagedTargetOptionPreSessionRunner:
    def __init__(self, references_database, control_database, source, *,
                 loader=load_option_pre_session_request_cursor,
                 run_repository=persist_staged_run_cursor,
                 claim_repository=claim_staged_package_cursor,
                 package_repository=persist_option_pre_session_package_cursor,
                 completion_repository=complete_staged_package_cursor,
                 failure_repository=fail_staged_package_cursor,
                 receipt_loader=load_staged_package_receipts_cursor,
                 usage_loader=load_staged_usage_cursor,
                 selection_repository=persist_option_manifest_selection_cursor,
                 finalization_repository=persist_staged_finalization_cursor,
                 clock=lambda: datetime.now(timezone.utc)):
        if (not isinstance(references_database, RealtimeProcessDatabase)
                or references_database.process_plan.role is not RealtimeDatabaseRole.REFERENCES):
            raise ValueError("staged option acquisition requires the REFERENCES role")
        if (not isinstance(control_database, RealtimeProcessDatabase)
                or control_database.process_plan.role is not RealtimeDatabaseRole.CONTROL):
            raise ValueError("staged option finalization requires the CONTROL role")
        if not callable(getattr(source, "collect_underlyer", None)):
            raise TypeError("staged option source must collect one underlying")
        self.references_database, self.control_database, self.source = references_database, control_database, source
        self.loader, self.run_repository, self.claim_repository = loader, run_repository, claim_repository
        self.package_repository, self.completion_repository = package_repository, completion_repository
        self.failure_repository, self.receipt_loader, self.usage_loader = failure_repository, receipt_loader, usage_loader
        self.selection_repository, self.finalization_repository = selection_repository, finalization_repository
        self.clock = clock

    def _request(self, plan):
        with self.references_database.transaction(read_only=True, isolation_level="REPEATABLE READ") as cursor:
            return self.loader(cursor, plan)

    def prepare(self, plan: OptionPreSessionTargetPlan, *, collection_deadline):
        request = self._request(plan)
        with self.references_database.transaction() as cursor:
            return self.run_repository(cursor, request, collection_deadline=collection_deadline)

    def acquire_one(self, plan: OptionPreSessionTargetPlan, owner_id: UUID, *, underlyer=None):
        request = self._request(plan)
        run_id = option_pre_session_run_id(request)
        with self.references_database.transaction() as cursor:
            claim = self.claim_repository(cursor, run_id, owner_id, underlyer=underlyer)
        if claim is None:
            return None
        before = self.source.account_counters() if callable(getattr(self.source, "account_counters", None)) else (0, 0)
        try:
            package, package_requests, package_bytes = self.source.collect_underlyer(request, claim.underlyer)
            after = self.source.account_counters() if callable(getattr(self.source, "account_counters", None)) else (
                before[0] + package_requests, before[1] + package_bytes)
            consumed_requests, consumed_bytes = after[0] - before[0], after[1] - before[1]
            if (consumed_requests, consumed_bytes) != (package_requests, package_bytes):
                raise ValueError("staged option source counters changed outside its package")
            receipt = StagedOptionPackageReceipt(run_id, claim.underlyer,
                request.security_id_by_underlyer[claim.underlyer], package,
                package_requests, package_bytes, self.clock())
            with self.references_database.transaction() as cursor:
                self.package_repository(cursor, package)
                self.completion_repository(cursor, claim, receipt)
            return receipt
        except Exception as error:
            after = self.source.account_counters() if callable(getattr(self.source, "account_counters", None)) else before
            requests, byte_count = max(0, after[0] - before[0]), max(0, after[1] - before[1])
            with self.references_database.transaction() as cursor:
                reason = (str(error)[:240] if isinstance(error, ValueError) else type(error).__name__)
                self.failure_repository(cursor, claim, reason=reason,
                    provider_requests=requests, response_bytes=byte_count)
            raise

    def finalize(self, plan: OptionPreSessionTargetPlan, *, cohort_binding=None):
        request = self._request(plan)
        with self.references_database.transaction(read_only=True, isolation_level="REPEATABLE READ") as cursor:
            receipts = self.receipt_loader(cursor, request)
            usage = self.usage_loader(cursor, option_pre_session_run_id(request))
        collection = assemble_staged_collection(request, receipts,
            provider_requests=usage[0], response_bytes=usage[1])
        packages = OptionPreSessionProducer._validate_collection(request, collection)
        candidates = tuple(candidate for package in packages for candidate in package.candidates)
        selection = build_bounded_option_session_manifest(request.scope, request.policy,
            candidates, session_date=request.session_date)
        if len(selection.policy_admitted_contract_ids) > 10_000:
            raise ValueError("staged policy-admitted matrix exceeds the retained global row ceiling")
        with self.control_database.transaction() as cursor:
            selection_id = self.selection_repository(cursor, selection,
                effective_at=request.effective_at, cohort_binding=cohort_binding)
            self.finalization_repository(cursor, request, selection_id, finalized_at=_utc(self.clock(), "clock"))
        return OptionPreSessionProducerResult(tuple(row.package.evidence.result.batch.batch_id for row in receipts),
            selection_id, selection, collection.provider_requests,
            collection.response_bytes,
            catalog=tuple(sorted((row.catalog for row in candidates), key=lambda row: row.contract_id)),
            snapshots=tuple(sorted((row.snapshot for row in candidates), key=lambda row: row.contract_id)))