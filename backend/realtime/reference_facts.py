from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Mapping


class ReferenceFactRelation(str, Enum):
    SECURITY = "SECURITY"
    FUNDAMENTAL_REPORT = "FUNDAMENTAL_REPORT"
    FUNDAMENTAL_COVERAGE = "FUNDAMENTAL_COVERAGE"
    OPTION_CONTRACT = "OPTION_CONTRACT"
    CORPORATE_ACTION = "CORPORATE_ACTION"
    CORPORATE_ACTION_COVERAGE = "CORPORATE_ACTION_COVERAGE"
    OI_BASELINE = "OI_BASELINE"
    MARKET_EVENT = "MARKET_EVENT"
    MARKET_EVENT_COVERAGE = "MARKET_EVENT_COVERAGE"
    RATE_OBSERVATION = "RATE_OBSERVATION"


REFERENCE_FACT_FIELDS: Mapping[ReferenceFactRelation, tuple[str, ...]] = {
    ReferenceFactRelation.SECURITY: (
        "security_id", "revision_id", "ticker", "asset_type", "source_origin",
        "exchange", "calendar_id", "effective_from", "effective_to",
        "observed_at", "payload_sha256", "company_name", "security_type",
        "active", "cik", "composite_figi", "share_class_figi", "sic_code",
        "sic_description", "sector", "industry", "list_date", "delisted_date",
        "weighted_shares", "free_float", "free_float_percent", "market_cap",
    ),
    ReferenceFactRelation.FUNDAMENTAL_REPORT: (
        "report_id", "security_id", "security_revision_id", "cik",
        "accession_number", "form_type", "timeframe", "fiscal_year",
        "fiscal_quarter", "period_end", "filing_date", "availability_time",
        "observed_at", "source", "source_key", "metrics", "raw_payload",
        "payload_sha256", "quality_codes",
    ),
    ReferenceFactRelation.FUNDAMENTAL_COVERAGE: (
        "coverage_id", "security_id", "window_start", "window_end", "source",
        "report_count", "response_sha256", "checked_at", "valid_until",
    ),
    ReferenceFactRelation.OPTION_CONTRACT: (
        "contract_id", "version_id", "underlying_security_id", "provider",
        "contract_ticker", "first_observed_at", "source_origin", "call_put",
        "expiration", "strike", "exercise_style", "multiplier", "deliverables",
        "eligibility", "effective_from", "effective_to", "observed_at",
        "payload_sha256",
    ),
    ReferenceFactRelation.CORPORATE_ACTION: (
        "action_id", "security_id", "ticker", "action_type", "effective_date",
        "declaration_date", "ex_date", "record_date", "pay_date", "cash_amount",
        "split_from", "split_to", "new_ticker", "source", "source_key",
        "observed_at", "received_at", "payload_sha256", "raw_payload",
        "supersedes_action_id",
    ),
    ReferenceFactRelation.CORPORATE_ACTION_COVERAGE: (
        "coverage_id", "security_id", "action_type", "window_start", "window_end",
        "source", "source_key", "response_action_count", "response_sha256",
        "observed_at", "received_at", "payload_sha256",
    ),
    ReferenceFactRelation.OI_BASELINE: (
        "baseline_id", "contract_id", "settlement_session", "open_interest",
        "source", "date_basis", "observed_session", "observed_at", "received_at",
        "payload_sha256", "supersedes_baseline_id",
    ),
    ReferenceFactRelation.MARKET_EVENT: (
        "revision_id", "security_id", "event_key", "event_type", "event_date",
        "announced_at", "event_timezone", "time_precision", "status", "source",
        "observed_at", "received_at", "valid_until", "payload_sha256",
        "supersedes_revision_id",
    ),
    ReferenceFactRelation.MARKET_EVENT_COVERAGE: (
        "coverage_id", "security_id", "event_type", "window_start", "window_end",
        "source", "verdict", "checked_at", "valid_until", "evidence_sha256",
    ),
    ReferenceFactRelation.RATE_OBSERVATION: (
        "observation_id", "rate_date", "tenor_days", "annual_rate", "source",
        "source_key", "source_observed_at", "received_at", "payload_sha256",
    ),
}


@dataclass(frozen=True, slots=True)
class ReferenceFact:
    relation: ReferenceFactRelation
    member_kind: str
    member_key: str
    values: tuple[tuple[str, object], ...]

    @classmethod
    def build(
        cls,
        relation: ReferenceFactRelation,
        member_kind: str,
        member_key: str,
        **values: object,
    ) -> ReferenceFact:
        if relation is ReferenceFactRelation.SECURITY:
            values.setdefault("company_name", None)
            values.setdefault("security_type", None)
            for field in REFERENCE_FACT_FIELDS[relation][13:]:
                values.setdefault(field, None)
        expected = REFERENCE_FACT_FIELDS[relation]
        if set(values) != set(expected):
            missing = sorted(set(expected) - set(values))
            extra = sorted(set(values) - set(expected))
            raise ValueError(
                f"invalid {relation.value} fact fields: missing={missing}, extra={extra}"
            )
        kind = member_kind.strip().upper()
        key = member_key.strip().upper()
        if not kind or not key:
            raise ValueError("reference fact member identity cannot be blank")
        payload_sha256 = values.get("payload_sha256")
        if payload_sha256 is not None and (
            not isinstance(payload_sha256, str)
            or len(payload_sha256) != 64
            or any(character not in "0123456789abcdef" for character in payload_sha256)
        ):
            raise ValueError("reference fact payload hash must be SHA-256")
        return cls(
            relation,
            kind,
            key,
            tuple((name, values[name]) for name in expected),
        )

    @property
    def identity(self) -> tuple[ReferenceFactRelation, object]:
        return self.relation, self.values[0][1]

    def ordered_values(self) -> tuple[object, ...]:
        return tuple(value for _, value in self.values)


def validate_reference_facts(facts: tuple[ReferenceFact, ...]) -> None:
    identities = tuple(fact.identity for fact in facts)
    if len(identities) != len(set(identities)):
        raise ValueError("reference fact identities must be unique")