from __future__ import annotations

import json
import os
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID

from psycopg2 import Error as PsycopgError, errors
from psycopg2.extras import Json

from options.calendar import OptionExchangeCalendar

from .advanced_options_acquisition_runtime import (
    AdvancedOptionsAcquisitionBinding,
    acquisition_binding_from_payload,
    acquisition_binding_payload,
)
from .advanced_options_activity_momentum_c6 import (
    activity_momentum_allocation_from_payload,
    activity_momentum_allocation_payload,
    c6_coverage_from_activity_allocation,
    c6_members_from_activity_allocation,
    load_activity_momentum_profile,
)
from .advanced_options_p8_runtime import (
    LegacyArtifactSessionStateAdapter,
    P8DownstreamBinding,
    P8PreopenSessionBinding,
    SessionRuntimeStatePort,
    build_p8_preopen_session_binding,
    build_p8_waiting_execution_bundle,
    p8_downstream_binding_from_payload,
    p8_downstream_binding_payload,
    p8_preopen_session_binding_from_payload,
    p8_preopen_session_binding_payload,
    validate_p8_execution_bundle_document,
)
from .advanced_options_p8_selection_source import (
    p8_activity_selection_capture_from_sealed,
)
from .advanced_options_reference_repository import (
    PostgresAdvancedOptionsReferenceStateRepository,
)
from .advanced_options_selection_c6 import (
    C6ConnectionCapacity,
    C6ReadinessState,
    C6SelectionMember,
    C6SelectionPlan,
    C6UnderlyingReadiness,
    OptionSide,
    build_c6_selection_plan,
    c6_selection_health_cursor,
    load_c6_selection_cursor,
    load_advanced_options_selection_c6_policy,
    load_strategy_policy_identity,
    persist_c6_selection_cursor,
)
from .advanced_options_service import AdvancedOptionsProcessKind
from .advanced_options_v3_authority import (
    AdvancedOptionsAuthorityBundle,
    OpeningReferenceEvidence,
    advanced_options_authority_bundle_payload,
    build_opening_subscription_readiness,
    persist_session_acquisition_authorization_cursor,
    prospective_opening_admission_from_payload,
    session_acquisition_authorization_from_payload,
)
from .advanced_options_v3_history import (
    advanced_options_v3_history_binding_from_payload,
)
from .advanced_realtime_desired_state import (
    DesiredMemberKind,
)
from .advanced_realtime_desired_state_repository import (
    load_effective_desired_state_reconciliation_cursor,
)
from .advanced_options_flow_policy import (
    EXPECTED_UNDERLYINGS,
    load_advanced_options_flow_policy,
)
from .contracts import (
    MarketChannel,
    canonical_sha256,
)
from .database_access_v2 import APP_ROLE, V2ConnectionProvider
from .option_slot_set import (
    SlotSetKind,
    SlotSetMember,
    SlotSetPurpose,
    SlotSetVersion,
)
from .stage_graph import STAGE_CATALOG_VERSION_V5


UTC = timezone.utc
POSTGRES_STATE_SOURCE = "postgres"
LEGACY_DIAGNOSTIC_STATE_SOURCE = "legacy-diagnostic"
OPTIONS_STATE_SOURCE_ENV = "REALTIME_V3_OPTIONS_STATE_SOURCE"
ACTIVATION_SERIALIZATION_ATTEMPTS = 3
_DEFAULT_PACKAGE_PATH_REFERENCE = (
    "/run/advanced-options-p8/package.json"
)
_DEFAULT_C6_STRATEGY_POLICY_PATH = (
    Path(__file__).resolve().parents[1]
    / "options"
    / "policies"
    / "strategy_o3_credit_v2.json"
)


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _digest(value: object, name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


def _write_json(path: Path, value: object) -> None:
    encoded = (
        json.dumps(value, indent=2, sort_keys=True, default=str) + "\n"
    ).encode("utf-8")
    with path.open("xb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())


@dataclass(frozen=True, slots=True)
class _StoredContinuousSession:
    session_binding_id: UUID
    session_date: date
    package: dict[str, object]
    authority_bundle: dict[str, object]
    acquisition_binding: AdvancedOptionsAcquisitionBinding
    session_binding: P8PreopenSessionBinding
    execution_bundle: dict[str, object]


class PostgresContinuousSessionStateRepository(SessionRuntimeStatePort):
    def __init__(
        self,
        provider: V2ConnectionProvider,
        *,
        session_date: date | None = None,
        checked_at: datetime | None = None,
        head_poll_interval_seconds: float = 1.0,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if not callable(getattr(provider, "transaction", None)):
            raise TypeError("Options session-state provider is invalid")
        if session_date is not None and not isinstance(session_date, date):
            raise TypeError("Options session-state date is invalid")
        if (
            not isinstance(head_poll_interval_seconds, (int, float))
            or head_poll_interval_seconds <= 0
            or not callable(monotonic)
        ):
            raise ValueError("Options session-state poll cadence is invalid")
        self._provider = provider
        self._session_date = session_date
        self._checked_at = _utc(
            checked_at or datetime.now(UTC),
            "checked_at",
        )
        self._head_poll_interval_seconds = float(
            head_poll_interval_seconds
        )
        self._monotonic = monotonic
        self._stored: _StoredContinuousSession | None = None
        self._head_checked_at: float | None = None
        self._head_sequence = 0
        self._head_binding: P8DownstreamBinding | None = None
        self._c6_selection_loaded = False
        self._c6_selection: C6SelectionPlan | None = None

    def _load_stored(self) -> _StoredContinuousSession:
        if self._stored is not None:
            return self._stored
        with self._provider.transaction(
            APP_ROLE,
            read_only=True,
        ) as unit:
            if self._session_date is None:
                unit.cursor.execute(
                    "SELECT session_binding_id::text,session_date,"
                    "package_document,authority_bundle_document,"
                    "acquisition_binding_document,"
                    "session_binding_document,execution_bundle_document "
                    "FROM rt.advanced_options_continuous_sessions "
                    "WHERE prepared_at<=%s AND decision_stop>%s "
                    "ORDER BY session_date DESC LIMIT 2",
                    (self._checked_at, self._checked_at),
                )
            else:
                unit.cursor.execute(
                    "SELECT session_binding_id::text,session_date,"
                    "package_document,authority_bundle_document,"
                    "acquisition_binding_document,"
                    "session_binding_document,execution_bundle_document "
                    "FROM rt.advanced_options_continuous_sessions "
                    "WHERE session_date=%s",
                    (self._session_date,),
                )
            rows = unit.cursor.fetchall()
        if len(rows) != 1:
            raise RuntimeError(
                "current PostgreSQL Options session state is unavailable"
            )
        row = rows[0]
        package = dict(row[2])
        acquisition = acquisition_binding_from_payload(row[4])
        session_binding = p8_preopen_session_binding_from_payload(
            row[5]
        )
        execution_bundle = validate_p8_execution_bundle_document(
            package,
            row[6],
        )
        if (
            session_binding.binding_id != UUID(str(row[0]))
            or acquisition.authority.session_date != row[1]
            or session_binding.acquisition_binding_id
            != acquisition.binding_id
        ):
            raise RuntimeError(
                "stored PostgreSQL Options session identity diverged"
            )
        self._stored = _StoredContinuousSession(
            session_binding_id=session_binding.binding_id,
            session_date=row[1],
            package=package,
            authority_bundle=dict(row[3]),
            acquisition_binding=acquisition,
            session_binding=session_binding,
            execution_bundle=execution_bundle,
        )
        return self._stored

    @staticmethod
    def _validate_package(
        package: Mapping[str, object],
        authority_bundle: AdvancedOptionsAuthorityBundle,
        acquisition_binding: AdvancedOptionsAcquisitionBinding,
    ) -> dict[str, object]:
        value = dict(package)
        package_digest = _digest(
            value.get("package_sha256"),
            "package_sha256",
        )
        unsigned = dict(value)
        unsigned.pop("package_sha256", None)
        pre_session = value.get("v3_pre_session_authority")
        history = value.get("v3_history_binding")
        equity = value.get("equity_input_binding")
        authorities = value.get("v3_authorities")
        equity_symbols = (
            ()
            if not isinstance(equity, Mapping)
            or not isinstance(equity.get("symbols"), list)
            else tuple(
                str(ticker) for ticker in equity["symbols"]
            )
        )
        if (
            canonical_sha256(unsigned) != package_digest
            or not isinstance(pre_session, Mapping)
            or not isinstance(history, Mapping)
            or not isinstance(equity, Mapping)
            or not isinstance(authorities, Mapping)
            or authorities.get("session_acquisition")
            != advanced_options_authority_bundle_payload(
                authority_bundle
            )["session_acquisition"]
            or tuple(sorted(
                str(ticker)
                for ticker in value.get("v3_admitted_contracts", ())
            ))
            != acquisition_binding.contract_tickers
            or not equity_symbols
            or len(equity_symbols) != len(set(equity_symbols))
            or any(
                ticker != ticker.strip().upper()
                for ticker in equity_symbols
            )
        ):
            raise ValueError(
                "continuous Options package identity is invalid"
            )
        return value

    @staticmethod
    def _validate_selection(
        sealed_selection: Mapping[str, object],
        *,
        package: Mapping[str, object],
        authority_bundle: AdvancedOptionsAuthorityBundle,
    ) -> str:
        document = dict(sealed_selection)
        semantic_sha256 = _digest(
            document.get("semantic_sha256"),
            "sealed selection semantic_sha256",
        )
        unsigned = dict(document)
        unsigned.pop("semantic_sha256", None)
        if canonical_sha256(unsigned) != semantic_sha256:
            raise ValueError("sealed selection semantic identity changed")
        capture = p8_activity_selection_capture_from_sealed(document)
        pre_session = package["v3_pre_session_authority"]
        admission = authority_bundle.admission
        if (
            capture.selection.reviewed_at.date()
            != authority_bundle.session.session_date
            or capture.selection.reviewed_at > admission.prepared_at
            or tuple(
                row.contract_ticker
                for row in capture.selection.selected
            )
            != tuple(
                row.contract_ticker for row in admission.contracts
            )
            or pre_session.get(
                "source_capture_semantic_sha256"
            )
            != semantic_sha256
        ):
            raise ValueError(
                "sealed selection belongs to another Options session"
            )
        return semantic_sha256

    @staticmethod
    def _validate_history(
        cursor,
        *,
        package: Mapping[str, object],
        options_cohort_id: UUID,
        session_date: date,
        prepared_at: datetime,
    ) -> None:
        history = package["v3_history_binding"]
        binding_payload = history.get("binding")
        evidence = history.get("evidence")
        if not isinstance(evidence, Mapping):
            raise ValueError("current-session Options history is absent")
        binding = advanced_options_v3_history_binding_from_payload(
            binding_payload
        )
        unsigned = dict(evidence)
        evidence_sha256 = unsigned.pop("evidence_sha256", None)
        readiness = evidence.get("readiness")
        previous_session = OptionExchangeCalendar().previous_session_date(
            session_date
        )
        known_at = datetime.fromisoformat(
            str(evidence.get("known_at_utc"))
        )
        if (
            binding.target_cohort_id != options_cohort_id
            or evidence.get("session_date") != session_date.isoformat()
            or evidence_sha256 != canonical_sha256(unsigned)
            or binding.evidence_sha256 != evidence_sha256
            or not isinstance(readiness, Mapping)
            or readiness.get("status") != "READY"
            or readiness.get("required_through_session")
            != previous_session.isoformat()
            or readiness.get("latest_covered_session")
            != previous_session.isoformat()
            or _utc(known_at, "history known_at") > prepared_at
        ):
            raise ValueError(
                "current-session Options history is stale or divergent"
            )
        cursor.execute(
            "SELECT binding_sha256 FROM rt.cohort_history_bindings "
            "WHERE target_cohort_id=%s",
            (str(options_cohort_id),),
        )
        row = cursor.fetchone()
        if row is None or str(row[0]) != binding.binding_sha256:
            raise ValueError(
                "stored current-session Options history changed"
            )

    @staticmethod
    def _validate_database_facts(
        cursor,
        *,
        package: Mapping[str, object],
        authority_bundle: AdvancedOptionsAuthorityBundle,
        reference: OpeningReferenceEvidence,
        selection_plan: C6SelectionPlan | None = None,
    ):
        authority = authority_bundle.session
        admission = authority_bundle.admission
        readiness = authority_bundle.readiness
        reconciliation = (
            load_effective_desired_state_reconciliation_cursor(
                cursor,
                session_date=authority.session_date,
            )
        )
        if (
            reconciliation is None
            or reconciliation.state != "READY"
            or reconciliation.snapshot is None
        ):
            raise ValueError(
                "effective current-session desired snapshot is unavailable"
            )
        option_underlyings = {
            row.ticker
            for row in reconciliation.revision.members
            if row.enabled
            and row.kind is DesiredMemberKind.OPTION_UNDERLYING
        }
        configured_option_underlyings = (
            set(selection_plan.broad_underlyings)
            if selection_plan is not None
            else {
                row.underlyer for row in admission.contracts
            }
        )
        package_equity_symbols = set(
            package["equity_input_binding"]["symbols"]
        )
        snapshot = reconciliation.snapshot
        cursor.execute(
            "SELECT DISTINCT instrument.security_id::text "
            "FROM rt.subscription_members AS member "
            "JOIN rt.instruments AS instrument "
            "ON instrument.instrument_id=member.instrument_id "
            "WHERE member.manifest_id=%s "
            "AND member.channel='AM' "
            "AND member.option_underlying=false",
            (str(snapshot.manifest_id),),
        )
        ticker_by_security = {
            str(row.security_id): row.ticker
            for row in reconciliation.preparations
            if row.security_id is not None
        }
        effective_equity_members = {
            ticker_by_security[str(row[0])]
            for row in cursor.fetchall()
            if str(row[0]) in ticker_by_security
        }
        if (
            option_underlyings != configured_option_underlyings
            or not configured_option_underlyings.issubset(
                effective_equity_members
            )
            or not configured_option_underlyings.issubset(
                package_equity_symbols
            )
            or not package_equity_symbols.issubset(
                effective_equity_members
            )
        ):
            raise ValueError(
                "continuous Options requires configured underlyings "
                "to have READY desired and package Equity context"
            )
        if selection_plan is not None and (
            selection_plan.session_date != authority.session_date
            or selection_plan.desired_revision_id
            != snapshot.revision_id
            or selection_plan.desired_snapshot_sha256
            != snapshot.snapshot_sha256
            or {
                row.contract_ticker
                for row in selection_plan.members
            }
            != {
                row.contract_ticker
                for row in admission.contracts
            }
        ):
            raise ValueError(
                "C6 selection plan diverges from current session"
            )
        cursor.execute(
            "SELECT manifest.session_date,manifest.manifest_id::text,"
            "snapshot.snapshot_sha256 "
            "FROM rt.realtime_desired_session_snapshots AS snapshot "
            "JOIN rt.subscription_manifests AS manifest "
            "ON manifest.manifest_id=snapshot.manifest_id "
            "WHERE snapshot.revision_id=%s",
            (str(snapshot.revision_id),),
        )
        desired_row = cursor.fetchone()
        if desired_row != (
            authority.session_date,
            str(snapshot.manifest_id),
            snapshot.snapshot_sha256,
        ):
            raise ValueError(
                "effective desired snapshot session identity changed"
            )

        cursor.execute(
            "SELECT plan.session_date,plan.stage_catalog_version,"
            "plan.cohort_id::text,manifest.session_date "
            "FROM rt.service_plans AS plan "
            "JOIN rt.cohorts AS cohort USING(cohort_id) "
            "JOIN rt.subscription_manifests AS manifest "
            "ON manifest.manifest_id=cohort.manifest_id "
            "WHERE plan.plan_id=%s",
            (str(package["orchestration"]["plan_id"]),),
        )
        plan_row = cursor.fetchone()
        if plan_row != (
            authority.session_date,
            STAGE_CATALOG_VERSION_V5,
            str(authority.cohort_id),
            authority.session_date,
        ):
            raise ValueError(
                "Options plan or cohort belongs to another session"
            )

        cursor.execute(
            "SELECT state_id::text,state_sha256,session_date,known_at,"
            "valid_until,reference_policy_sha256,"
            "contract_scope_sha256 "
            "FROM rt.advanced_options_reference_states "
            "WHERE state_id=%s",
            (str(reference.state_id),),
        )
        reference_row = cursor.fetchone()
        if (
            reference_row is None
            or reference_row != (
                str(reference.state_id),
                reference.state_sha256,
                reference.session_date,
                reference.known_at,
                reference.valid_until,
                reference.reference_policy_sha256,
                reference.contract_scope_sha256,
            )
            or reference.session_date != authority.session_date
            or reference.known_at > admission.prepared_at
            or reference.valid_until < admission.capture_boundary
            or admission.reference_evidence_sha256 != reference.sha256
        ):
            raise ValueError(
                "current-session Options reference/OI state is stale"
            )
        cursor.execute(
            "SELECT member_key,member_sha256,availability,"
            "member_document "
            "FROM rt.advanced_options_reference_state_members "
            "WHERE state_id=%s AND member_kind='OPEN_INTEREST' "
            "ORDER BY member_key",
            (str(reference.state_id),),
        )
        stored_oi = {
            int(row[0]): (
                str(row[1]),
                str(row[2]),
                dict(row[3]),
            )
            for row in cursor.fetchall()
        }
        if set(stored_oi) != {
            row.contract_id for row in reference.open_interest
        } or any(
            stored_oi[row.contract_id][0] != row.member_sha256
            or stored_oi[row.contract_id][1] != "AVAILABLE"
            or stored_oi[row.contract_id][2].get(
                "contract_ticker"
            )
            != row.contract_ticker
            or stored_oi[row.contract_id][2].get("open_interest")
            != row.open_interest
            for row in reference.open_interest
        ):
            raise ValueError(
                "stored current-session prior OI members changed"
            )

        cursor.execute(
            "SELECT version.kind,version.effective_at,"
            "version.policy_sha256,version.member_sha256,"
            "current.version "
            "FROM rt.slot_set_versions AS version "
            "JOIN rt.slot_set_current AS current USING(cohort_id) "
            "WHERE version.cohort_id=%s AND version.version=%s",
            (
                str(readiness.cohort_id),
                readiness.slot_set_version,
            ),
        )
        slot_row = cursor.fetchone()
        if (
            slot_row is None
            or str(slot_row[0]) != "PRE_SESSION"
            or slot_row[1] > readiness.ready_at
            or str(slot_row[2]) != readiness.slot_set_policy_sha256
            or str(slot_row[3]) != readiness.slot_set_member_sha256
            or int(slot_row[4]) != readiness.slot_set_version
        ):
            raise ValueError(
                "current-session Options slot set is stale"
            )
        cursor.execute(
            "SELECT slot.instrument_id,instrument.contract_id,"
            "contract.contract_ticker,slot.catalog_version_id,"
            "slot.channel,version.payload_sha256,version.effective_to "
            "FROM rt.slot_set_members AS slot "
            "JOIN rt.instruments AS instrument "
            "ON instrument.instrument_id=slot.instrument_id "
            "JOIN ref.option_contracts AS contract "
            "ON contract.contract_id=instrument.contract_id "
            "JOIN ref.option_contract_versions AS version "
            "ON version.version_id=slot.catalog_version_id "
            "WHERE slot.cohort_id=%s AND slot.version=%s "
            "ORDER BY contract.contract_ticker,slot.channel",
            (
                str(readiness.cohort_id),
                readiness.slot_set_version,
            ),
        )
        rows = cursor.fetchall()
        expected_slots = {
            (instrument_id, contract_id, ticker, catalog_id, channel)
            for (
                instrument_id,
                contract_id,
                ticker,
                catalog_id,
                channels,
            ) in readiness.slots
            for channel in channels
        }
        if {
            (
                int(row[0]),
                int(row[1]),
                str(row[2]),
                int(row[3]),
                str(row[4]),
            )
            for row in rows
        } != expected_slots:
            raise ValueError(
                "current-session Options contract membership changed"
            )
        contracts = {
            row.contract_ticker: row for row in admission.contracts
        }
        if any(
            row[6] is not None
            or str(row[5])
            != contracts[str(row[2])].catalog_payload_sha256
            for row in rows
        ):
            raise ValueError(
                "current-session Options catalog version is stale"
            )
        reference_by_ticker = {
            row.contract_ticker: row for row in reference.open_interest
        }
        if (
            not set(reference_by_ticker).issubset(contracts)
            or any(
                (
                    contract.open_interest is None
                    and ticker in reference_by_ticker
                )
                or (
                    contract.open_interest is not None
                    and (
                        ticker not in reference_by_ticker
                        or reference_by_ticker[ticker].open_interest
                        != contract.open_interest
                        or reference_by_ticker[ticker].member_sha256
                        != contract.open_interest_member_sha256
                    )
                )
                for ticker, contract in contracts.items()
            )
        ):
            raise ValueError(
                "current-session Options prior OI identity changed"
            )
        PostgresContinuousSessionStateRepository._validate_history(
            cursor,
            package=package,
            options_cohort_id=authority.cohort_id,
            session_date=authority.session_date,
            prepared_at=admission.prepared_at,
        )
        return reconciliation

    def _persist_activation_cursor(
        self,
        cursor,
        *,
        package_document: Mapping[str, object],
        authority_bundle: AdvancedOptionsAuthorityBundle,
        acquisition_binding: AdvancedOptionsAcquisitionBinding,
        reference: OpeningReferenceEvidence,
        source_selection_sha256: str,
        session_binding: P8PreopenSessionBinding,
        bundle: Mapping[str, object],
        authority_document: Mapping[str, object],
        acquisition_document: Mapping[str, object],
        session_document: Mapping[str, object],
        selection_plan: C6SelectionPlan | None,
    ) -> None:
        cursor.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
            (
                "advanced-options-continuous-session:"
                f"{authority_bundle.session.session_date.isoformat()}",
            ),
        )
        reconciliation = self._validate_database_facts(
            cursor,
            package=package_document,
            authority_bundle=authority_bundle,
            reference=reference,
            selection_plan=selection_plan,
        )
        persist_session_acquisition_authorization_cursor(
            cursor,
            authority_bundle.session,
        )
        snapshot = reconciliation.snapshot
        cursor.execute(
            "INSERT INTO rt.advanced_options_continuous_sessions ("
            "session_binding_id,session_binding_sha256,session_date,"
            "desired_revision_id,desired_snapshot_sha256,"
            "desired_manifest_id,desired_cohort_id,options_cohort_id,"
            "plan_id,session_authorization_id,reference_state_id,"
            "reference_state_sha256,source_selection_sha256,"
            "package_sha256,acquisition_binding_id,"
            "acquisition_binding_sha256,execution_bundle_sha256,"
            "prepared_at,acquisition_start,decision_stop,"
            "package_document,authority_bundle_document,"
            "acquisition_binding_document,session_binding_document,"
            "execution_bundle_document"
            ") VALUES ("
            "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
            "%s,%s,%s,%s,%s,%s,%s,%s,%s"
            ") ON CONFLICT (session_date) DO NOTHING",
            (
                str(session_binding.binding_id),
                session_binding.binding_sha256,
                authority_bundle.session.session_date,
                str(snapshot.revision_id),
                snapshot.snapshot_sha256,
                str(snapshot.manifest_id),
                str(snapshot.cohort_id),
                str(authority_bundle.session.cohort_id),
                str(session_binding.plan_id),
                str(authority_bundle.session.authorization_id),
                str(reference.state_id),
                reference.state_sha256,
                source_selection_sha256,
                package_document["package_sha256"],
                str(acquisition_binding.binding_id),
                acquisition_binding.binding_sha256,
                bundle["bundle_sha256"],
                authority_bundle.session.prepared_at,
                authority_bundle.session.acquisition_start,
                authority_bundle.session.acquisition_stop,
                Json(package_document),
                Json(authority_document),
                Json(acquisition_document),
                Json(session_document),
                Json(bundle),
            ),
        )
        inserted_session = cursor.rowcount == 1
        cursor.execute(
            "SELECT session_binding_id::text,"
            "session_binding_sha256,package_document,"
            "authority_bundle_document,acquisition_binding_document,"
            "session_binding_document,execution_bundle_document "
            "FROM rt.advanced_options_continuous_sessions "
            "WHERE session_date=%s",
            (authority_bundle.session.session_date,),
        )
        stored = cursor.fetchone()
        if stored is None or (
            str(stored[0]),
            str(stored[1]),
            canonical_sha256(stored[2]),
            canonical_sha256(stored[3]),
            canonical_sha256(stored[4]),
            canonical_sha256(stored[5]),
            canonical_sha256(stored[6]),
        ) != (
            str(session_binding.binding_id),
            session_binding.binding_sha256,
            canonical_sha256(package_document),
            canonical_sha256(authority_document),
            canonical_sha256(acquisition_document),
            canonical_sha256(session_document),
            canonical_sha256(bundle),
        ):
            raise ValueError(
                "continuous Options session replay diverged"
            )
        cursor.execute(
            "INSERT INTO rt.advanced_options_downstream_heads ("
            "session_binding_id"
            ") VALUES (%s) ON CONFLICT DO NOTHING",
            (str(session_binding.binding_id),),
        )
        cursor.execute(
            "SELECT sequence,binding_sha256 "
            "FROM rt.advanced_options_downstream_heads "
            "WHERE session_binding_id=%s",
            (str(session_binding.binding_id),),
        )
        head = cursor.fetchone()
        if (
            head is None
            or (
                inserted_session
                and head != (0, None)
            )
        ):
            raise ValueError(
                "continuous Options downstream head is invalid"
            )
        if selection_plan is not None:
            persist_c6_selection_cursor(
                cursor,
                session_binding_id=session_binding.binding_id,
                plan=selection_plan,
            )

    def activate_session(
        self,
        *,
        package: Mapping[str, object],
        authority_bundle: AdvancedOptionsAuthorityBundle,
        acquisition_binding: AdvancedOptionsAcquisitionBinding,
        reference: OpeningReferenceEvidence,
        sealed_selection: Mapping[str, object],
        written_at: datetime,
        selection_plan: C6SelectionPlan | None = None,
        package_path_reference: str = (
            _DEFAULT_PACKAGE_PATH_REFERENCE
        ),
    ) -> P8PreopenSessionBinding:
        _utc(written_at, "activation attempted_at")
        if (
            not isinstance(authority_bundle, AdvancedOptionsAuthorityBundle)
            or authority_bundle.cycle is not None
            or not isinstance(
                acquisition_binding,
                AdvancedOptionsAcquisitionBinding,
            )
            or not isinstance(reference, OpeningReferenceEvidence)
            or acquisition_binding.authority
            != authority_bundle.session
            or acquisition_binding.contract_tickers
            != tuple(
                row.contract_ticker
                for row in authority_bundle.admission.contracts
            )
            or selection_plan is not None
            and not isinstance(selection_plan, C6SelectionPlan)
            or selection_plan is not None
            and (
                selection_plan.physical_subscription.owner_id
                != acquisition_binding.owner_id
                or selection_plan.physical_subscription.journal_generation
                != authority_bundle.session.journal_generation
            )
        ):
            raise ValueError(
                "continuous Options activation inputs diverged"
            )
        package_document = self._validate_package(
            package,
            authority_bundle,
            acquisition_binding,
        )
        source_selection_sha256 = self._validate_selection(
            sealed_selection,
            package=package_document,
            authority_bundle=authority_bundle,
        )
        session_binding = build_p8_preopen_session_binding(
            package=package_document,
            acquisition_binding=acquisition_binding,
        )
        bundle = build_p8_waiting_execution_bundle(
            package=package_document,
            acquisition_binding=acquisition_binding,
            written_at=authority_bundle.session.prepared_at,
            package_path_reference=package_path_reference,
        )
        validate_p8_execution_bundle_document(
            package_document,
            bundle,
        )
        authority_document = advanced_options_authority_bundle_payload(
            authority_bundle
        )
        acquisition_document = acquisition_binding_payload(
            acquisition_binding
        )
        session_document = p8_preopen_session_binding_payload(
            session_binding
        )
        for attempt in range(ACTIVATION_SERIALIZATION_ATTEMPTS):
            try:
                with self._provider.transaction(
                    APP_ROLE,
                    isolation_level="SERIALIZABLE",
                ) as unit:
                    self._persist_activation_cursor(
                        unit.cursor,
                        package_document=package_document,
                        authority_bundle=authority_bundle,
                        acquisition_binding=acquisition_binding,
                        reference=reference,
                        source_selection_sha256=(
                            source_selection_sha256
                        ),
                        session_binding=session_binding,
                        bundle=bundle,
                        authority_document=authority_document,
                        acquisition_document=acquisition_document,
                        session_document=session_document,
                        selection_plan=selection_plan,
                    )
                break
            except PsycopgError as error:
                serialization_failure = (
                    isinstance(error, errors.SerializationFailure)
                    or error.pgcode == "40001"
                )
                if (
                    not serialization_failure
                    or attempt + 1
                    >= ACTIVATION_SERIALIZATION_ATTEMPTS
                ):
                    raise
        self._session_date = authority_bundle.session.session_date
        self._checked_at = authority_bundle.session.prepared_at
        self._stored = None
        self._head_checked_at = None
        self._c6_selection_loaded = False
        self._c6_selection = None
        self._load_stored()
        return session_binding

    def activate_session_from_documents(
        self,
        *,
        package: Mapping[str, object],
        pre_session_authority: Mapping[str, object],
        acquisition_binding: AdvancedOptionsAcquisitionBinding,
        sealed_selection: Mapping[str, object],
        written_at: datetime,
        selection_plan: C6SelectionPlan | None = None,
        selection_capacity: C6ConnectionCapacity | None = None,
        package_path_reference: str = (
            _DEFAULT_PACKAGE_PATH_REFERENCE
        ),
    ) -> P8PreopenSessionBinding:
        admission = prospective_opening_admission_from_payload(
            pre_session_authority.get("admission")
        )
        session = session_acquisition_authorization_from_payload(
            pre_session_authority.get("session_acquisition")
        )
        if (
            acquisition_binding.authority != session
            or pre_session_authority.get("session_date")
            != session.session_date.isoformat()
            or pre_session_authority.get("target_cohort_id")
            != str(session.cohort_id)
        ):
            raise ValueError(
                "pre-session activation authority diverged"
            )
        with self._provider.transaction(
            APP_ROLE,
            read_only=True,
        ) as unit:
            unit.cursor.execute(
                "SELECT kind,effective_at,policy_sha256,member_sha256 "
                "FROM rt.slot_set_versions "
                "WHERE cohort_id=%s AND version=1",
                (str(session.cohort_id),),
            )
            version = unit.cursor.fetchone()
            unit.cursor.execute(
                "SELECT slot.instrument_id,slot.channel,slot.purpose,"
                "slot.priority,slot.catalog_version_id,"
                "contract.contract_ticker "
                "FROM rt.slot_set_members AS slot "
                "JOIN rt.instruments AS instrument "
                "ON instrument.instrument_id=slot.instrument_id "
                "JOIN ref.option_contracts AS contract "
                "ON contract.contract_id=instrument.contract_id "
                "WHERE slot.cohort_id=%s AND slot.version=1 "
                "ORDER BY slot.instrument_id,slot.channel",
                (str(session.cohort_id),),
            )
            slot_rows = unit.cursor.fetchall()
            reconciliation = (
                load_effective_desired_state_reconciliation_cursor(
                    unit.cursor,
                    session_date=session.session_date,
                )
            )
        if version is None or str(version[0]) != "PRE_SESSION":
            raise ValueError(
                "current-session pre-open slot set is unavailable"
            )
        slot_set = SlotSetVersion(
            cohort_id=session.cohort_id,
            version=1,
            kind=SlotSetKind.PRE_SESSION,
            effective_at=version[1],
            policy_sha256=str(version[2]),
            members=tuple(
                SlotSetMember(
                    instrument_id=int(row[0]),
                    channel=MarketChannel(str(row[1])),
                    purpose=SlotSetPurpose(str(row[2])),
                    priority=int(row[3]),
                    catalog_version_id=int(row[4]),
                )
                for row in slot_rows
            ),
        )
        if slot_set.member_sha256 != str(version[3]):
            raise ValueError(
                "stored current-session slot membership changed"
            )
        instrument_ids = {
            str(row[5]): int(row[0]) for row in slot_rows
        }
        readiness = None
        for ready_at in dict.fromkeys((
            session.prepared_at,
            admission.prepared_at,
            slot_set.effective_at,
        )):
            try:
                candidate = build_opening_subscription_readiness(
                    admission,
                    slot_set,
                    instrument_id_by_contract_ticker=instrument_ids,
                    journal_generation=session.journal_generation,
                    journal_identity_sha256=(
                        session.journal_identity_sha256
                    ),
                    ready_at=ready_at,
                )
            except ValueError:
                continue
            if (
                candidate.readiness_sha256
                == session.readiness_sha256
            ):
                readiness = candidate
                break
        if (
            readiness is None
            or readiness.readiness_sha256
            != session.readiness_sha256
            or pre_session_authority.get("readiness_sha256")
            != readiness.readiness_sha256
        ):
            raise ValueError(
                "pre-session readiness reconstruction changed"
            )
        state = PostgresAdvancedOptionsReferenceStateRepository(
            self._provider
        ).load_for_session(
            session.session_date,
            known_at=admission.prepared_at,
        )
        if state is None:
            raise ValueError(
                "current-session Options reference state is unavailable"
            )
        reference = OpeningReferenceEvidence.from_reference_state(
            state
        )
        authority_bundle = AdvancedOptionsAuthorityBundle(
            session=session,
            admission=admission,
            readiness=readiness,
        )
        if selection_plan is None and selection_capacity is not None:
            if (
                reconciliation is None
                or reconciliation.snapshot is None
                or reconciliation.state != "READY"
            ):
                raise ValueError(
                    "C6 effective desired snapshot is unavailable"
                )
            flow_policy = load_advanced_options_flow_policy()
            selection_policy = (
                load_advanced_options_selection_c6_policy()
            )
            allocation_value = sealed_selection.get(
                "activity_momentum_allocation"
            )
            allocation = (
                None
                if allocation_value is None
                else activity_momentum_allocation_from_payload(
                    allocation_value
                )
            )
            if allocation is not None:
                profile = load_activity_momentum_profile(
                    selection_policy=selection_policy,
                    version=allocation.profile_version,
                    profile_sha256=allocation.profile_sha256,
                )
                if (
                    allocation.profile_sha256
                    != profile.profile_sha256
                    or allocation.session_date != session.session_date
                    or set(allocation.selected_contract_tickers)
                    != {
                        row.contract_ticker
                        for row in admission.contracts
                    }
                ):
                    raise ValueError(
                        "C6 activity/momentum allocation diverged"
                    )
                catalog_members = {
                    row.contract_ticker: C6SelectionMember(
                        contract_id=row.contract_id,
                        contract_ticker=row.contract_ticker,
                        underlying=row.underlyer,
                        expiration=row.expiration_date,
                        strike=row.strike,
                        option_side=(
                            OptionSide.CALL
                            if row.contract_type == "CALL"
                            else OptionSide.PUT
                        ),
                        multiplier=Decimal("100"),
                        deliverable="STANDARD",
                        catalog_version_id=row.catalog_version_id,
                        catalog_payload_sha256=(
                            row.catalog_payload_sha256
                        ),
                        matching_strategies=(),
                        contract_roles=("ALLOCATED_CONTRACT",),
                        selection_reasons=("CATALOG_IDENTITY",),
                        evidence_references=(
                            row.snapshot_payload_sha256,
                            row.open_interest_member_sha256,
                        ),
                        flow_eligible=False,
                    )
                    for row in admission.contracts
                }
                members = c6_members_from_activity_allocation(
                    allocation,
                    catalog_members=catalog_members,
                )
                admission_by_ticker = {
                    row.contract_ticker: row
                    for row in admission.contracts
                }
                if any(
                    member.contract_id
                    != admission_by_ticker[
                        member.contract_ticker
                    ].contract_id
                    or member.catalog_version_id
                    != admission_by_ticker[
                        member.contract_ticker
                    ].catalog_version_id
                    or member.catalog_payload_sha256
                    != admission_by_ticker[
                        member.contract_ticker
                    ].catalog_payload_sha256
                    for member in members
                ):
                    raise ValueError(
                        "C6 allocation catalog identity diverged"
                    )
                admission_profile_version = profile.version
                admission_profile_sha256 = profile.profile_sha256
                allocation_document = (
                    activity_momentum_allocation_payload(allocation)
                )
                allocation_coverage = (
                    c6_coverage_from_activity_allocation(allocation)
                )
            else:
                if len(admission.contracts) != 4:
                    raise ValueError(
                        "configured C6 admission requires persisted "
                        "activity/momentum allocation"
                    )
                members = tuple(
                    C6SelectionMember(
                        contract_id=row.contract_id,
                        contract_ticker=row.contract_ticker,
                        underlying=row.underlyer,
                        expiration=row.expiration_date,
                        strike=row.strike,
                        option_side=(
                            OptionSide.CALL
                            if row.contract_type == "CALL"
                            else OptionSide.PUT
                        ),
                        multiplier=Decimal("100"),
                        deliverable="STANDARD",
                        catalog_version_id=row.catalog_version_id,
                        catalog_payload_sha256=(
                            row.catalog_payload_sha256
                        ),
                        matching_strategies=(),
                        contract_roles=(
                            "CALL_ANCHOR"
                            if row.contract_type == "CALL"
                            else "PUT_ANCHOR",
                        ),
                        selection_reasons=(
                            "EXACT_FOUR_COMPATIBILITY",
                        ),
                        evidence_references=(
                            row.snapshot_payload_sha256,
                            row.open_interest_member_sha256,
                        ),
                        flow_eligible=(
                            row.underlyer in EXPECTED_UNDERLYINGS
                            and 0
                            <= (
                                row.expiration_date
                                - session.session_date
                            ).days
                            <= 60
                            and flow_policy.lane_for(
                                (
                                    row.expiration_date
                                    - session.session_date
                                ).days
                            )
                            is not None
                        ),
                    )
                    for row in admission.contracts
                )
                admission_profile_version = None
                admission_profile_sha256 = None
                allocation_document = None
                allocation_coverage = ()
            underlyings = tuple(sorted({
                row.underlyer for row in admission.contracts
            }))
            selection_plan = build_c6_selection_plan(
                session_date=session.session_date,
                desired_revision_id=(
                    reconciliation.snapshot.revision_id
                ),
                desired_snapshot_sha256=(
                    reconciliation.snapshot.snapshot_sha256
                ),
                selection_policy=selection_policy,
                members=members,
                readiness=tuple(
                    C6UnderlyingReadiness(
                        underlying=underlying,
                        catalog=C6ReadinessState.AVAILABLE,
                        open_interest=(
                            C6ReadinessState.AVAILABLE
                        ),
                        history=C6ReadinessState.AVAILABLE,
                        feed_states=(
                            (
                                "OPTION_QUOTE",
                                C6ReadinessState.AVAILABLE,
                            ),
                            (
                                "OPTION_TRADE",
                                C6ReadinessState.AVAILABLE,
                            ),
                        ),
                        reason_codes=(),
                        observed_at=admission.prepared_at,
                        source_as_of=reference.known_at,
                    )
                    for underlying in underlyings
                ),
                capacity=selection_capacity,
                strategy_policies=(
                    load_strategy_policy_identity(
                        _DEFAULT_C6_STRATEGY_POLICY_PATH
                    ),
                ),
                catalog_identity=readiness.slot_set_member_sha256,
                reference_identity=reference.state_sha256,
                source_as_of=admission.prepared_at,
                admission_profile_version=(
                    admission_profile_version
                ),
                admission_profile_sha256=(
                    admission_profile_sha256
                ),
                allocation_document=allocation_document,
                allocation_coverage=allocation_coverage,
                physical_owner_id=acquisition_binding.owner_id,
                journal_generation=session.journal_generation,
            )
        return self.activate_session(
            package=package,
            authority_bundle=authority_bundle,
            acquisition_binding=acquisition_binding,
            reference=reference,
            sealed_selection=sealed_selection,
            written_at=written_at,
            selection_plan=selection_plan,
            package_path_reference=package_path_reference,
        )

    def load_c6_selection_plan(self) -> C6SelectionPlan | None:
        if self._c6_selection_loaded:
            return self._c6_selection
        stored = self._load_stored()
        with self._provider.transaction(
            APP_ROLE,
            read_only=True,
        ) as unit:
            selection = load_c6_selection_cursor(
                unit.cursor,
                session_binding_id=stored.session_binding_id,
            )
        self._c6_selection = selection
        self._c6_selection_loaded = True
        return selection

    def load_c6_selection_health(
        self,
    ) -> Mapping[str, object] | None:
        stored = self._load_stored()
        with self._provider.transaction(
            APP_ROLE,
            read_only=True,
        ) as unit:
            return c6_selection_health_cursor(
                unit.cursor,
                session_binding_id=stored.session_binding_id,
            )

    def load_package(self) -> Mapping[str, object]:
        return dict(self._load_stored().package)

    def load_bundle_document(self) -> Mapping[str, object]:
        return dict(self._load_stored().execution_bundle)

    def load_execution_bundle(
        self,
        *,
        expected_kind: AdvancedOptionsProcessKind | None = None,
        checked_at: datetime | None = None,
    ) -> Mapping[str, object]:
        stored = self._load_stored()
        return validate_p8_execution_bundle_document(
            stored.package,
            stored.execution_bundle,
            expected_kind=expected_kind,
            checked_at=checked_at,
        )

    def load_acquisition_binding(
        self,
    ) -> AdvancedOptionsAcquisitionBinding:
        return self._load_stored().acquisition_binding

    def load_preopen_session_binding(
        self,
        bundle: Mapping[str, object],
    ) -> P8PreopenSessionBinding | None:
        stored = self._load_stored()
        if canonical_sha256(bundle) != canonical_sha256(
            stored.execution_bundle
        ):
            raise ValueError(
                "PostgreSQL Options execution bundle changed"
            )
        return stored.session_binding

    def _load_head(self, *, force: bool = False) -> None:
        stored = self._load_stored()
        now = self._monotonic()
        if (
            not force
            and self._head_checked_at is not None
            and now - self._head_checked_at
            < self._head_poll_interval_seconds
        ):
            return
        with self._provider.transaction(
            APP_ROLE,
            read_only=True,
        ) as unit:
            unit.cursor.execute(
                "SELECT sequence,binding_sha256 "
                "FROM rt.advanced_options_downstream_heads "
                "WHERE session_binding_id=%s",
                (str(stored.session_binding_id),),
            )
            row = unit.cursor.fetchone()
        if row is None:
            raise RuntimeError(
                "PostgreSQL Options downstream head is unavailable"
            )
        sequence = int(row[0])
        binding_sha256 = (
            None if row[1] is None else str(row[1])
        )
        if (
            (sequence == 0 and binding_sha256 is not None)
            or (sequence > 0 and binding_sha256 is None)
        ):
            raise RuntimeError(
                "PostgreSQL Options downstream head diverged"
            )
        cached_sha256 = (
            None
            if self._head_binding is None
            else self._head_binding.binding_sha256
        )
        if (
            sequence == self._head_sequence
            and binding_sha256 == cached_sha256
            and (
                sequence == 0
                or self._head_checked_at is not None
            )
        ):
            self._head_checked_at = now
            return
        binding = None
        if sequence > 0:
            with self._provider.transaction(
                APP_ROLE,
                read_only=True,
            ) as unit:
                unit.cursor.execute(
                    "SELECT binding_document "
                    "FROM rt.advanced_options_downstream_bindings "
                    "WHERE session_binding_id=%s AND sequence=%s "
                    "AND binding_sha256=%s",
                    (
                        str(stored.session_binding_id),
                        sequence,
                        binding_sha256,
                    ),
                )
                document = unit.cursor.fetchone()
            if document is None:
                raise RuntimeError(
                    "PostgreSQL Options downstream head document "
                    "is unavailable"
                )
            binding = p8_downstream_binding_from_payload(document[0])
            if (
                binding.sequence != sequence
                or binding.binding_sha256 != binding_sha256
                or binding.session_binding_sha256
                != stored.session_binding.binding_sha256
            ):
                raise RuntimeError(
                    "PostgreSQL Options downstream head diverged"
                )
        self._head_sequence = sequence
        self._head_binding = binding
        self._head_checked_at = now

    def load_latest_downstream_binding(
        self,
        session_binding: P8PreopenSessionBinding,
    ) -> P8DownstreamBinding | None:
        stored = self._load_stored()
        if session_binding != stored.session_binding:
            raise ValueError(
                "PostgreSQL downstream read targets another session"
            )
        self._load_head()
        return self._head_binding

    def load_downstream_bindings(
        self,
        session_binding: P8PreopenSessionBinding,
    ) -> tuple[P8DownstreamBinding, ...]:
        stored = self._load_stored()
        if session_binding != stored.session_binding:
            raise ValueError(
                "PostgreSQL downstream read targets another session"
            )
        with self._provider.transaction(
            APP_ROLE,
            read_only=True,
        ) as unit:
            unit.cursor.execute(
                "WITH captured_head AS MATERIALIZED ("
                "SELECT sequence,binding_sha256 "
                "FROM rt.advanced_options_downstream_heads "
                "WHERE session_binding_id=%s"
                ") SELECT head.sequence,head.binding_sha256,"
                "binding.sequence,binding.binding_sha256,"
                "binding.binding_document "
                "FROM captured_head AS head "
                "LEFT JOIN rt.advanced_options_downstream_bindings "
                "AS binding "
                "ON binding.session_binding_id=%s "
                "AND binding.sequence<=head.sequence "
                "ORDER BY binding.sequence",
                (
                    str(stored.session_binding_id),
                    str(stored.session_binding_id),
                ),
            )
            rows = unit.cursor.fetchall()
        if not rows:
            raise RuntimeError(
                "PostgreSQL Options downstream head is unavailable"
            )
        head_sequence = int(rows[0][0])
        head_sha256 = (
            None if rows[0][1] is None else str(rows[0][1])
        )
        if any(
            int(row[0]) != head_sequence
            or (
                None if row[1] is None else str(row[1])
            )
            != head_sha256
            for row in rows
        ):
            raise RuntimeError(
                "PostgreSQL Options captured head changed"
            )
        bindings = tuple(
            p8_downstream_binding_from_payload(row[4])
            for row in rows
            if row[2] is not None
        )
        previous = None
        for sequence, binding in enumerate(bindings, start=1):
            if (
                binding.sequence != sequence
                or binding.previous_binding_sha256 != previous
                or binding.session_binding_sha256
                != session_binding.binding_sha256
            ):
                raise RuntimeError(
                    "PostgreSQL Options downstream chain diverged"
                )
            previous = binding.binding_sha256
        if (
            head_sequence != len(bindings)
            or (
                head_sequence == 0
                and head_sha256 is not None
            )
            or (
                bindings
                and bindings[-1].binding_sha256 != head_sha256
            )
        ):
            raise RuntimeError(
                "PostgreSQL Options downstream head is not the chain tail"
            )
        self._head_sequence = head_sequence
        self._head_binding = None if not bindings else bindings[-1]
        self._head_checked_at = self._monotonic()
        return bindings

    @staticmethod
    def _validate_downstream_facts(
        cursor,
        *,
        session_binding: P8PreopenSessionBinding,
        binding: P8DownstreamBinding,
    ) -> None:
        cursor.execute(
            "SELECT authorization_sha256,session_authorization_id::text,"
            "equity_binding_sha256,equity_receipt_id::text,"
            "equity_cutoff,options_cutoff,"
            "publication_authorization_reference "
            "FROM rt.advanced_options_window_cycle_authorizations "
            "WHERE target_window_id=%s",
            (str(binding.target_window_id),),
        )
        authority = cursor.fetchone()
        if authority != (
            binding.window_cycle_authorization_sha256,
            str(session_binding.session_authorization_id),
            binding.equity_binding_sha256,
            str(binding.equity_receipt_id),
            binding.finalized_cutoff,
            binding.options_cutoff,
            binding.publication_authorization_reference,
        ):
            raise ValueError(
                "PostgreSQL downstream authority identity changed"
            )
        cursor.execute(
            "SELECT binding_sha256,equity_receipt_id::text,"
            "input_sha256,equity_cutoff "
            "FROM rt.advanced_options_equity_input_bindings "
            "WHERE target_options_window_id=%s",
            (str(binding.target_window_id),),
        )
        if cursor.fetchone() != (
            binding.equity_binding_sha256,
            str(binding.equity_receipt_id),
            binding.equity_input_sha256,
            binding.finalized_cutoff,
        ):
            raise ValueError(
                "PostgreSQL downstream Equity binding changed"
            )

    def attach_downstream_binding(
        self,
        session_binding: P8PreopenSessionBinding,
        binding: P8DownstreamBinding,
    ) -> P8DownstreamBinding:
        stored = self._load_stored()
        if (
            session_binding != stored.session_binding
            or binding.session_binding_sha256
            != session_binding.binding_sha256
        ):
            raise ValueError(
                "PostgreSQL downstream append targets another session"
            )
        document = p8_downstream_binding_payload(binding)
        with self._provider.transaction(
            APP_ROLE,
        ) as unit:
            unit.cursor.execute(
                "SELECT sequence,binding_sha256 "
                "FROM rt.advanced_options_downstream_heads "
                "WHERE session_binding_id=%s FOR UPDATE",
                (str(stored.session_binding_id),),
            )
            head = unit.cursor.fetchone()
            if head is None:
                raise RuntimeError(
                    "PostgreSQL Options downstream head is unavailable"
                )
            unit.cursor.execute(
                "SELECT binding_document "
                "FROM rt.advanced_options_downstream_bindings "
                "WHERE session_binding_id=%s AND sequence=%s",
                (
                    str(stored.session_binding_id),
                    binding.sequence,
                ),
            )
            replay = unit.cursor.fetchone()
            if replay is not None:
                stored_binding = p8_downstream_binding_from_payload(
                    replay[0]
                )
                if stored_binding != binding:
                    raise RuntimeError(
                        "PostgreSQL downstream replay diverged"
                    )
                if binding.sequence < int(head[0]):
                    return stored_binding
                if (
                    int(head[0]) == binding.sequence
                ):
                    if str(head[1]) != binding.binding_sha256:
                        raise RuntimeError(
                            "PostgreSQL downstream head diverged"
                        )
                    return stored_binding
                if (
                    binding.sequence != int(head[0]) + 1
                    or binding.previous_binding_sha256
                    != (
                        None
                        if int(head[0]) == 0
                        else str(head[1])
                    )
                ):
                    raise RuntimeError(
                        "PostgreSQL downstream replay is not the head"
                    )
                self._validate_downstream_facts(
                    unit.cursor,
                    session_binding=session_binding,
                    binding=binding,
                )
                unit.cursor.execute(
                    "UPDATE rt.advanced_options_downstream_heads "
                    "SET sequence=%s,binding_sha256=%s,"
                    "updated_at=clock_timestamp() "
                    "WHERE session_binding_id=%s AND sequence=%s "
                    "AND binding_sha256 IS NOT DISTINCT FROM %s",
                    (
                        binding.sequence,
                        binding.binding_sha256,
                        str(stored.session_binding_id),
                        int(head[0]),
                        head[1],
                    ),
                )
                if unit.cursor.rowcount != 1:
                    raise RuntimeError(
                        "PostgreSQL downstream tail recovery raced"
                    )
                return stored_binding
            expected_sequence = int(head[0]) + 1
            expected_previous = (
                None if int(head[0]) == 0 else str(head[1])
            )
            if (
                binding.sequence != expected_sequence
                or binding.previous_binding_sha256
                != expected_previous
            ):
                raise ValueError(
                    "PostgreSQL downstream binding order changed"
                )
            if int(head[0]) > 0:
                unit.cursor.execute(
                    "SELECT finalized_cutoff "
                    "FROM rt.advanced_options_downstream_bindings "
                    "WHERE session_binding_id=%s AND sequence=%s",
                    (
                        str(stored.session_binding_id),
                        int(head[0]),
                    ),
                )
                previous = unit.cursor.fetchone()
                if (
                    previous is None
                    or binding.finalized_cutoff <= previous[0]
                ):
                    raise ValueError(
                        "PostgreSQL downstream cutoff order changed"
                    )
            self._validate_downstream_facts(
                unit.cursor,
                session_binding=session_binding,
                binding=binding,
            )
            unit.cursor.execute(
                "INSERT INTO rt.advanced_options_downstream_bindings ("
                "session_binding_id,sequence,binding_id,binding_sha256,"
                "previous_binding_sha256,target_window_id,"
                "equity_receipt_id,finalized_cutoff,options_cutoff,"
                "binding_document"
                ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    str(stored.session_binding_id),
                    binding.sequence,
                    str(binding.binding_id),
                    binding.binding_sha256,
                    binding.previous_binding_sha256,
                    str(binding.target_window_id),
                    str(binding.equity_receipt_id),
                    binding.finalized_cutoff,
                    binding.options_cutoff,
                    Json(document),
                ),
            )
            unit.cursor.execute(
                "UPDATE rt.advanced_options_downstream_heads "
                "SET sequence=%s,binding_sha256=%s,"
                "updated_at=clock_timestamp() "
                "WHERE session_binding_id=%s AND sequence=%s "
                "AND binding_sha256 IS NOT DISTINCT FROM %s",
                (
                    binding.sequence,
                    binding.binding_sha256,
                    str(stored.session_binding_id),
                    int(head[0]),
                    head[1],
                ),
            )
            if unit.cursor.rowcount != 1:
                raise RuntimeError(
                    "PostgreSQL downstream head advance raced"
                )
        self._head_sequence = binding.sequence
        self._head_binding = binding
        self._head_checked_at = self._monotonic()
        return binding

    def export_artifacts(self, output_directory: Path) -> Mapping[str, object]:
        root = output_directory.resolve()
        root.mkdir(parents=True, exist_ok=False)
        stored = self._load_stored()
        package_path = root / "package.json"
        bundle_path = root / "execution-bundle.json"
        acquisition_path = root / "acquisition-binding.json"
        downstream_root = root / "downstream-bindings"
        downstream_root.mkdir()
        _write_json(package_path, stored.package)
        _write_json(bundle_path, stored.execution_bundle)
        _write_json(
            acquisition_path,
            acquisition_binding_payload(
                stored.acquisition_binding
            ),
        )
        bindings = self.load_downstream_bindings(
            stored.session_binding
        )
        for binding in bindings:
            _write_json(
                downstream_root / f"{binding.sequence:03d}.json",
                p8_downstream_binding_payload(binding),
            )
        if bindings:
            latest = bindings[-1]
            _write_json(
                downstream_root / "LATEST.json",
                {
                    "schema": (
                        "advanced-options-p8-downstream-binding-"
                        "latest-v1"
                    ),
                    "sequence": latest.sequence,
                    "binding_sha256": latest.binding_sha256,
                    "file": f"{latest.sequence:03d}.json",
                },
            )
        return {
            "status": "EXPORTED",
            "session_date": stored.session_date.isoformat(),
            "package_sha256": stored.package["package_sha256"],
            "bundle_sha256": stored.execution_bundle[
                "bundle_sha256"
            ],
            "acquisition_binding_sha256": (
                stored.acquisition_binding.binding_sha256
            ),
            "session_binding_sha256": (
                stored.session_binding.binding_sha256
            ),
            "downstream_binding_count": len(bindings),
            "output_directory": str(root),
        }


def select_options_session_state(
    *,
    environment: Mapping[str, str],
    provider: V2ConnectionProvider | None = None,
    session_date: date | None = None,
    checked_at: datetime | None = None,
    package_path: Path | None = None,
    bundle_path: Path | None = None,
    acquisition_binding_path: Path | None = None,
    downstream_binding_directory: Path | None = None,
    state_source: str | None = None,
) -> SessionRuntimeStatePort:
    source = str(
        state_source
        or environment.get(OPTIONS_STATE_SOURCE_ENV, "")
    ).strip().casefold()
    artifact_paths = (
        package_path,
        bundle_path,
        acquisition_binding_path,
        downstream_binding_directory,
    )
    if source == POSTGRES_STATE_SOURCE:
        if provider is None:
            raise ValueError(
                "PostgreSQL Options state requires a database provider"
            )
        if any(path is not None for path in artifact_paths):
            raise ValueError(
                "PostgreSQL Options state cannot mix artifact paths"
            )
        return PostgresContinuousSessionStateRepository(
            provider,
            session_date=session_date,
            checked_at=checked_at,
        )
    if source == LEGACY_DIAGNOSTIC_STATE_SOURCE:
        if provider is not None:
            raise ValueError(
                "legacy diagnostic Options state cannot mix PostgreSQL"
            )
        if not any(path is not None for path in artifact_paths):
            raise ValueError(
                "legacy diagnostic Options state requires artifacts"
            )
        return LegacyArtifactSessionStateAdapter(
            package_path=package_path,
            bundle_path=bundle_path,
            acquisition_binding_path=acquisition_binding_path,
            downstream_binding_directory=downstream_binding_directory,
        )
    raise ValueError(
        f"{OPTIONS_STATE_SOURCE_ENV} must select postgres or "
        "legacy-diagnostic"
    )
