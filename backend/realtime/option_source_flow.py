"""Canonical source-flow identity shared by Options flow and detector cohorts."""
from __future__ import annotations

import json
from dataclasses import dataclass
from uuid import NAMESPACE_URL, UUID, uuid5

from psycopg2.extras import Json

from .advanced_options_flow_policy import (
    AdvancedOptionsContractScope,
    AdvancedOptionsFlowPolicy,
)
from .advanced_options_provider_contract import (
    MASSIVE_OPTIONS_PROVIDER_CONTRACT,
)
from .contracts import canonical_json, canonical_sha256


CANONICAL_OPTION_FLOW_OWNER = "option_flow_stream_v1"
CANONICAL_FLOW_POLICY_SOURCE = (
    "backend/realtime/policies/advanced_options_flow_v1.json"
)


def _sha256(value: str, name: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


@dataclass(frozen=True, slots=True)
class OptionSourceFlowIdentity:
    source_flow_id: UUID
    source_flow_sha256: str
    journal_generation: UUID
    stream_session_ids: tuple[UUID, ...]
    owner_name: str
    condition_semantics_sha256: str
    flow_policy_sha256: str
    identity_version: int = 2
    advanced_flow_policy_sha256: str | None = None
    contract_scope_sha256: str | None = None
    provider_contract_sha256: str | None = None
    condition_reference_sha256: str | None = None
    exchange_reference_sha256: str | None = None

    def __post_init__(self) -> None:
        sessions = tuple(sorted(self.stream_session_ids, key=str))
        if (
            not isinstance(self.journal_generation, UUID)
            or not sessions
            or any(not isinstance(value, UUID) for value in sessions)
            or len(sessions) != len(set(sessions))
        ):
            raise ValueError(
                "source flow requires one journal generation and exact stream sessions"
            )
        if self.owner_name != CANONICAL_OPTION_FLOW_OWNER:
            raise ValueError("source-flow owner is not canonical")
        condition = _sha256(
            self.condition_semantics_sha256,
            "condition_semantics_sha256",
        )
        policy = _sha256(self.flow_policy_sha256, "flow_policy_sha256")
        if self.identity_version == 2:
            if (
                self.advanced_flow_policy_sha256 is not None
                or self.contract_scope_sha256 is not None
                or self.provider_contract_sha256 is not None
                or self.condition_reference_sha256 is not None
                or self.exchange_reference_sha256 is not None
            ):
                raise ValueError(
                    "source-flow v2 cannot carry Advanced policy scope"
                )
            identity = {
                "contract": "advanced-options-source-flow-v1",
                "journal_generation": self.journal_generation,
                "stream_session_ids": sessions,
                "owner_name": self.owner_name,
                "condition_semantics_sha256": condition,
                "flow_policy_sha256": policy,
            }
        elif self.identity_version == 3:
            if any((
                self.provider_contract_sha256,
                self.condition_reference_sha256,
                self.exchange_reference_sha256,
            )):
                raise ValueError(
                    "source-flow v3 cannot carry provider references"
                )
            advanced = _sha256(
                self.advanced_flow_policy_sha256,
                "advanced_flow_policy_sha256",
            )
            scope = _sha256(
                self.contract_scope_sha256,
                "contract_scope_sha256",
            )
            identity = {
                "contract": "advanced-options-source-flow-v2",
                "identity_version": 3,
                "journal_generation": self.journal_generation,
                "stream_session_ids": sessions,
                "owner_name": self.owner_name,
                "condition_semantics_sha256": condition,
                "flow_policy_sha256": policy,
                "advanced_flow_policy_sha256": advanced,
                "contract_scope_sha256": scope,
            }
            object.__setattr__(
                self,
                "advanced_flow_policy_sha256",
                advanced,
            )
            object.__setattr__(self, "contract_scope_sha256", scope)
        elif self.identity_version == 4:
            advanced = _sha256(
                self.advanced_flow_policy_sha256,
                "advanced_flow_policy_sha256",
            )
            scope = _sha256(
                self.contract_scope_sha256,
                "contract_scope_sha256",
            )
            provider = _sha256(
                self.provider_contract_sha256,
                "provider_contract_sha256",
            )
            condition_reference = _sha256(
                self.condition_reference_sha256,
                "condition_reference_sha256",
            )
            exchange_reference = _sha256(
                self.exchange_reference_sha256,
                "exchange_reference_sha256",
            )
            identity = {
                "contract": "advanced-options-source-flow-v3",
                "identity_version": 4,
                "journal_generation": self.journal_generation,
                "stream_session_ids": sessions,
                "owner_name": self.owner_name,
                "condition_semantics_sha256": condition,
                "flow_policy_sha256": policy,
                "advanced_flow_policy_sha256": advanced,
                "contract_scope_sha256": scope,
                "provider_contract_sha256": provider,
                "condition_reference_sha256": condition_reference,
                "exchange_reference_sha256": exchange_reference,
            }
            for name, value in (
                ("advanced_flow_policy_sha256", advanced),
                ("contract_scope_sha256", scope),
                ("provider_contract_sha256", provider),
                ("condition_reference_sha256", condition_reference),
                ("exchange_reference_sha256", exchange_reference),
            ):
                object.__setattr__(self, name, value)
        else:
            raise ValueError("source-flow identity version is invalid")
        expected_sha256 = canonical_sha256(identity)
        expected_id = uuid5(
            NAMESPACE_URL,
            f"stockscreener:advanced-options-source-flow:{expected_sha256}",
        )
        if self.source_flow_sha256 != expected_sha256:
            raise ValueError("source-flow SHA does not match its lineage")
        if self.source_flow_id != expected_id:
            raise ValueError("source-flow ID does not match its lineage")
        object.__setattr__(self, "stream_session_ids", sessions)
        object.__setattr__(
            self,
            "condition_semantics_sha256",
            condition,
        )
        object.__setattr__(self, "flow_policy_sha256", policy)

    @property
    def lineage_identity(self) -> dict[str, object]:
        if self.identity_version == 2:
            return {
                "contract": "advanced-options-source-flow-v1",
                "journal_generation": self.journal_generation,
                "stream_session_ids": self.stream_session_ids,
                "owner_name": self.owner_name,
                "condition_semantics_sha256": self.condition_semantics_sha256,
                "flow_policy_sha256": self.flow_policy_sha256,
            }
        identity = {
            "contract": (
                "advanced-options-source-flow-v2"
                if self.identity_version == 3
                else "advanced-options-source-flow-v3"
            ),
            "identity_version": self.identity_version,
            "journal_generation": self.journal_generation,
            "stream_session_ids": self.stream_session_ids,
            "owner_name": self.owner_name,
            "condition_semantics_sha256": self.condition_semantics_sha256,
            "flow_policy_sha256": self.flow_policy_sha256,
            "advanced_flow_policy_sha256": (
                self.advanced_flow_policy_sha256
            ),
            "contract_scope_sha256": self.contract_scope_sha256,
        }
        if self.identity_version == 4:
            identity.update({
                "provider_contract_sha256": self.provider_contract_sha256,
                "condition_reference_sha256": (
                    self.condition_reference_sha256
                ),
                "exchange_reference_sha256": (
                    self.exchange_reference_sha256
                ),
            })
        return identity


def build_option_source_flow_identity(
    *,
    journal_generation: UUID,
    stream_session_ids: tuple[UUID, ...],
    condition_semantics_sha256: str,
    flow_policy_sha256: str,
    advanced_flow_policy_sha256: str | None = None,
    contract_scope_sha256: str | None = None,
    provider_contract_sha256: str | None = None,
    condition_reference_sha256: str | None = None,
    exchange_reference_sha256: str | None = None,
) -> OptionSourceFlowIdentity:
    sessions = tuple(sorted(stream_session_ids, key=str))
    if (advanced_flow_policy_sha256 is None) != (
        contract_scope_sha256 is None
    ):
        raise ValueError(
            "Advanced source-flow policy and scope must be bound together"
        )
    references = (
        provider_contract_sha256,
        condition_reference_sha256,
        exchange_reference_sha256,
    )
    if any(value is not None for value in references) and not all(
        value is not None for value in references
    ):
        raise ValueError("provider reference hashes must be bound together")
    identity_version = (
        2
        if advanced_flow_policy_sha256 is None
        else 4
        if provider_contract_sha256 is not None
        else 3
    )
    identity = {
        "contract": (
            "advanced-options-source-flow-v1"
            if identity_version == 2
            else "advanced-options-source-flow-v3"
            if identity_version == 4
            else "advanced-options-source-flow-v2"
        ),
        **(
            {}
            if identity_version == 2
            else {"identity_version": identity_version}
        ),
        "journal_generation": journal_generation,
        "stream_session_ids": sessions,
        "owner_name": CANONICAL_OPTION_FLOW_OWNER,
        "condition_semantics_sha256": condition_semantics_sha256,
        "flow_policy_sha256": flow_policy_sha256,
        **(
            {}
            if identity_version == 2
            else {
                "advanced_flow_policy_sha256": (
                    advanced_flow_policy_sha256
                ),
                "contract_scope_sha256": contract_scope_sha256,
                **(
                    {}
                    if identity_version == 3
                    else {
                        "provider_contract_sha256": (
                            provider_contract_sha256
                        ),
                        "condition_reference_sha256": (
                            condition_reference_sha256
                        ),
                        "exchange_reference_sha256": (
                            exchange_reference_sha256
                        ),
                    }
                ),
            }
        ),
    }
    source_flow_sha256 = canonical_sha256(identity)
    return OptionSourceFlowIdentity(
        source_flow_id=uuid5(
            NAMESPACE_URL,
            f"stockscreener:advanced-options-source-flow:{source_flow_sha256}",
        ),
        source_flow_sha256=source_flow_sha256,
        journal_generation=journal_generation,
        stream_session_ids=sessions,
        owner_name=CANONICAL_OPTION_FLOW_OWNER,
        condition_semantics_sha256=condition_semantics_sha256,
        flow_policy_sha256=flow_policy_sha256,
        identity_version=identity_version,
        advanced_flow_policy_sha256=advanced_flow_policy_sha256,
        contract_scope_sha256=contract_scope_sha256,
        provider_contract_sha256=provider_contract_sha256,
        condition_reference_sha256=condition_reference_sha256,
        exchange_reference_sha256=exchange_reference_sha256,
    )


def persist_option_flow_policy_cursor(
    cursor,
    policy: AdvancedOptionsFlowPolicy,
    *,
    source_path: str = CANONICAL_FLOW_POLICY_SOURCE,
) -> None:
    if not isinstance(policy, AdvancedOptionsFlowPolicy):
        raise TypeError("policy must be an AdvancedOptionsFlowPolicy")
    if not isinstance(source_path, str) or not source_path.strip():
        raise ValueError("policy source path cannot be blank")
    document = json.loads(canonical_json(policy))
    values = (
        policy.sha256,
        policy.version,
        Json(document),
        MASSIVE_OPTIONS_PROVIDER_CONTRACT.sha256,
        source_path,
    )
    cursor.execute(
        "INSERT INTO rt.option_flow_policies ("
        "policy_sha256,policy_version,policy_document,"
        "provider_contract_sha256,source_path"
        ") VALUES (%s,%s,%s,%s,%s) "
        "ON CONFLICT (policy_sha256) DO NOTHING",
        values,
    )
    cursor.execute(
        "SELECT policy_sha256,policy_version,policy_document,"
        "provider_contract_sha256,source_path "
        "FROM rt.option_flow_policies WHERE policy_sha256=%s",
        (policy.sha256,),
    )
    stored = cursor.fetchone()
    if stored is None or (
        str(stored[0]),
        str(stored[1]),
        stored[2],
        str(stored[3]),
        str(stored[4]),
    ) != (
        policy.sha256,
        policy.version,
        document,
        MASSIVE_OPTIONS_PROVIDER_CONTRACT.sha256,
        source_path,
    ):
        raise ValueError("persisted flow policy conflicts")


def persist_option_contract_scope_cursor(
    cursor,
    contract_scope: AdvancedOptionsContractScope,
) -> None:
    if not isinstance(contract_scope, AdvancedOptionsContractScope):
        raise TypeError(
            "contract_scope must be an AdvancedOptionsContractScope"
        )
    values = (
        contract_scope.sha256,
        contract_scope.version,
        contract_scope.session_date,
        contract_scope.manifest_sha256,
        contract_scope.slot_sha256,
        contract_scope.flow_policy_sha256,
    )
    cursor.execute(
        "INSERT INTO rt.option_flow_contract_scopes ("
        "contract_scope_sha256,scope_version,session_date,"
        "manifest_sha256,slot_sha256,advanced_flow_policy_sha256"
        ") VALUES (%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (contract_scope_sha256) DO NOTHING",
        values,
    )
    cursor.execute(
        "SELECT contract_scope_sha256,scope_version,session_date,"
        "manifest_sha256,slot_sha256,advanced_flow_policy_sha256 "
        "FROM rt.option_flow_contract_scopes "
        "WHERE contract_scope_sha256=%s",
        (contract_scope.sha256,),
    )
    stored = cursor.fetchone()
    if stored is None or tuple(stored) != values:
        raise ValueError("persisted contract scope conflicts")
    member_values = [
        (
            contract_scope.sha256,
            row.contract_ticker,
            row.underlyer,
            row.expiration,
            row.universe_tier.value,
            row.dte_lane.value,
        )
        for row in contract_scope.contracts
    ]
    cursor.executemany(
        "INSERT INTO rt.option_flow_contract_scope_members ("
        "contract_scope_sha256,contract_ticker,underlyer,expiration,"
        "universe_tier,dte_lane) VALUES (%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (contract_scope_sha256,contract_ticker) DO NOTHING",
        member_values,
    )
    cursor.execute(
        "SELECT contract_scope_sha256,contract_ticker,underlyer,"
        "expiration,universe_tier,dte_lane "
        "FROM rt.option_flow_contract_scope_members "
        "WHERE contract_scope_sha256=%s ORDER BY contract_ticker",
        (contract_scope.sha256,),
    )
    if [tuple(row) for row in cursor.fetchall()] != member_values:
        raise ValueError("persisted contract scope members conflict")


def persist_option_source_flow_cursor(
    cursor,
    source_flow: OptionSourceFlowIdentity,
    *,
    source_cohort_id: UUID,
    contract_scope: AdvancedOptionsContractScope | None = None,
) -> None:
    if not isinstance(source_flow, OptionSourceFlowIdentity):
        raise TypeError("source_flow must be an OptionSourceFlowIdentity")
    if not isinstance(source_cohort_id, UUID):
        raise TypeError("source_cohort_id must be a UUID")
    if source_flow.identity_version in (3, 4):
        if (
            not isinstance(contract_scope, AdvancedOptionsContractScope)
            or source_flow.advanced_flow_policy_sha256
            != contract_scope.flow_policy_sha256
            or source_flow.contract_scope_sha256
            != contract_scope.sha256
        ):
            raise ValueError(
                "Advanced source flow does not match its contract scope"
            )
        persist_option_contract_scope_cursor(cursor, contract_scope)
    elif contract_scope is not None:
        raise ValueError("source-flow v2 cannot persist a contract scope")
    values = (
        str(source_flow.source_flow_id),
        source_flow.source_flow_sha256,
        str(source_cohort_id),
        str(source_flow.journal_generation),
        [str(value) for value in source_flow.stream_session_ids],
        source_flow.owner_name,
        source_flow.condition_semantics_sha256,
        source_flow.flow_policy_sha256,
        source_flow.identity_version,
        source_flow.advanced_flow_policy_sha256,
        source_flow.contract_scope_sha256,
    )
    reference_columns = ""
    reference_placeholders = ""
    if source_flow.identity_version == 4:
        values = (
            *values,
            source_flow.provider_contract_sha256,
            source_flow.condition_reference_sha256,
            source_flow.exchange_reference_sha256,
        )
        reference_columns = (
            ",provider_contract_sha256,condition_reference_sha256,"
            "exchange_reference_sha256"
        )
        reference_placeholders = ",%s,%s,%s"
    cursor.execute(
        "INSERT INTO rt.option_source_flows ("
        "source_flow_id,source_flow_sha256,source_cohort_id,"
        "journal_generation,stream_session_ids,owner_name,"
        "condition_semantics_sha256,flow_policy_sha256,identity_version,"
        "advanced_flow_policy_sha256,contract_scope_sha256"
        f"{reference_columns}"
        ") VALUES (%s,%s,%s,%s,%s::uuid[],%s,%s,%s,%s,%s,%s"
        f"{reference_placeholders}) "
        "ON CONFLICT (source_flow_id) DO NOTHING",
        values,
    )
    cursor.execute(
        "SELECT source_flow_id,source_flow_sha256,source_cohort_id,"
        "journal_generation,stream_session_ids,owner_name,"
        "condition_semantics_sha256,flow_policy_sha256,identity_version,"
        "advanced_flow_policy_sha256,contract_scope_sha256"
        f"{reference_columns} "
        "FROM rt.option_source_flows WHERE source_flow_id=%s",
        (str(source_flow.source_flow_id),),
    )
    stored = cursor.fetchone()
    if stored is None:
        raise ValueError("source-flow persistence did not retain an identity")
    stored_sessions = stored[4]
    if isinstance(stored_sessions, str):
        session_values = [
            value
            for value in stored_sessions.strip("{}").split(",")
            if value
        ]
    else:
        session_values = [str(value) for value in stored_sessions]
    actual = (
        str(stored[0]),
        str(stored[1]),
        str(stored[2]),
        str(stored[3]),
        session_values,
        str(stored[5]),
        str(stored[6]),
        str(stored[7]),
        int(stored[8]),
        None if stored[9] is None else str(stored[9]),
        None if stored[10] is None else str(stored[10]),
    )
    if source_flow.identity_version == 4:
        actual = (
            *actual,
            str(stored[11]),
            str(stored[12]),
            str(stored[13]),
        )
    if actual != values:
        names = (
            "source_flow_id",
            "source_flow_sha256",
            "source_cohort_id",
            "journal_generation",
            "stream_session_ids",
            "owner_name",
            "condition_semantics_sha256",
            "flow_policy_sha256",
            "identity_version",
            "advanced_flow_policy_sha256",
            "contract_scope_sha256",
            "provider_contract_sha256",
            "condition_reference_sha256",
            "exchange_reference_sha256",
        )
        changed = tuple(
            name
            for name, expected, found in zip(names, values, actual)
            if expected != found
        )
        raise ValueError(
            "persisted source-flow identity conflicts: "
            + ",".join(changed)
        )
