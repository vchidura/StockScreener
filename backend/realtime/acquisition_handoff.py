from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from uuid import NAMESPACE_URL, UUID, uuid5

from psycopg2.extras import Json, execute_values

from equity.domain import (
    BarAvailabilityMode,
    BarSessionScope,
    BarSourceKind,
    EquityBarRevision,
)
from options.domain import (
    ContractType,
    DataQualityFlag,
    ExerciseStyle,
    MarkSource,
    OptionContractSnapshot,
)

from .contracts import canonical_sha256
from .equity_bar_adapter import RealtimeEquityBarFact, adapt_developer_equity_bar
from .equity_bar_repository import persist_realtime_equity_bars
from .preflight import ProviderTier


MAX_DEVELOPER_ACQUISITION_FACTS = 1_000
ACQUISITION_FACT_PAGE_SIZE = 100
DEVELOPER_EQUITY_BAR_SCHEMA = "developer_equity_bar_v1"
DEVELOPER_OPTION_SNAPSHOT_SCHEMA = "developer_option_snapshot_v1"


class DeveloperAcquisitionFactKind(str, Enum):
    EQUITY_BAR = "EQUITY_BAR"
    OPTION_SNAPSHOT = "OPTION_SNAPSHOT"

    @property
    def schema_version(self) -> str:
        if self is DeveloperAcquisitionFactKind.EQUITY_BAR:
            return DEVELOPER_EQUITY_BAR_SCHEMA
        return DEVELOPER_OPTION_SNAPSHOT_SCHEMA


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _sha256(value: str, name: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


def _datetime_value(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _decimal_value(value: Decimal | None) -> str | None:
    return str(value) if value is not None else None


def _parse_datetime(value: object, name: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be an ISO datetime")
    return _utc(datetime.fromisoformat(value), name)


def _parse_optional_datetime(value: object, name: str) -> datetime | None:
    return None if value is None else _parse_datetime(value, name)


def _parse_decimal(value: object, name: str) -> Decimal:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be an exact decimal string")
    return Decimal(value)


def _parse_optional_decimal(value: object, name: str) -> Decimal | None:
    return None if value is None else _parse_decimal(value, name)


def _require_contract_fields(
    payload: Mapping[str, object],
    expected: tuple[str, ...],
    schema_version: str,
) -> None:
    actual = frozenset(payload)
    required = frozenset(expected)
    if actual != required:
        missing = sorted(required - actual)
        extra = sorted(actual - required)
        raise ValueError(
            f"{schema_version} field mismatch: missing={missing} extra={extra}"
        )


_BAR_FIELDS = (
    "bar_revision_id",
    "security_id",
    "ticker",
    "interval",
    "session_date",
    "bar_start",
    "bar_end",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "vwap",
    "transaction_count",
    "source_kind",
    "availability_mode",
    "is_final",
    "system_observed_at",
    "replay_available_at",
    "adjusted",
    "payload_sha256",
    "quality_codes",
    "provider_published_at",
    "ingestion_segment_id",
    "source_bar_revision_ids",
    "supersedes_bar_revision_id",
    "reconciliation_status",
    "session_scope",
)


def _bar_payload(revision: EquityBarRevision) -> dict[str, object]:
    return {
        "bar_revision_id": str(revision.bar_revision_id),
        "security_id": str(revision.security_id),
        "ticker": revision.ticker,
        "interval": revision.interval,
        "session_date": revision.session_date.isoformat(),
        "bar_start": revision.bar_start.isoformat(),
        "bar_end": revision.bar_end.isoformat(),
        "open_price": str(revision.open_price),
        "high_price": str(revision.high_price),
        "low_price": str(revision.low_price),
        "close_price": str(revision.close_price),
        "volume": str(revision.volume),
        "vwap": _decimal_value(revision.vwap),
        "transaction_count": revision.transaction_count,
        "source_kind": revision.source_kind.value,
        "availability_mode": revision.availability_mode.value,
        "is_final": revision.is_final,
        "system_observed_at": revision.system_observed_at.isoformat(),
        "replay_available_at": _datetime_value(revision.replay_available_at),
        "adjusted": revision.adjusted,
        "payload_sha256": revision.payload_sha256,
        "quality_codes": list(revision.quality_codes),
        "provider_published_at": _datetime_value(revision.provider_published_at),
        "ingestion_segment_id": (
            str(revision.ingestion_segment_id)
            if revision.ingestion_segment_id is not None
            else None
        ),
        "source_bar_revision_ids": [
            str(value) for value in revision.source_bar_revision_ids
        ],
        "supersedes_bar_revision_id": (
            str(revision.supersedes_bar_revision_id)
            if revision.supersedes_bar_revision_id is not None
            else None
        ),
        "reconciliation_status": revision.reconciliation_status,
        "session_scope": revision.session_scope.value,
    }


def _bar_from_payload(payload: Mapping[str, object]) -> EquityBarRevision:
    _require_contract_fields(payload, _BAR_FIELDS, DEVELOPER_EQUITY_BAR_SCHEMA)
    session_date = payload["session_date"]
    if not isinstance(session_date, str):
        raise ValueError("bar session_date must be an ISO date")
    return EquityBarRevision(
        bar_revision_id=UUID(str(payload["bar_revision_id"])),
        security_id=UUID(str(payload["security_id"])),
        ticker=str(payload["ticker"]),
        interval=str(payload["interval"]),
        session_date=date.fromisoformat(session_date),
        bar_start=_parse_datetime(payload["bar_start"], "bar_start"),
        bar_end=_parse_datetime(payload["bar_end"], "bar_end"),
        open_price=_parse_decimal(payload["open_price"], "open_price"),
        high_price=_parse_decimal(payload["high_price"], "high_price"),
        low_price=_parse_decimal(payload["low_price"], "low_price"),
        close_price=_parse_decimal(payload["close_price"], "close_price"),
        volume=_parse_decimal(payload["volume"], "volume"),
        vwap=_parse_optional_decimal(payload["vwap"], "vwap"),
        transaction_count=(
            int(payload["transaction_count"])
            if payload["transaction_count"] is not None
            else None
        ),
        source_kind=BarSourceKind(str(payload["source_kind"])),
        availability_mode=BarAvailabilityMode(str(payload["availability_mode"])),
        is_final=bool(payload["is_final"]),
        system_observed_at=_parse_datetime(
            payload["system_observed_at"], "system_observed_at"
        ),
        replay_available_at=_parse_optional_datetime(
            payload["replay_available_at"], "replay_available_at"
        ),
        adjusted=bool(payload["adjusted"]),
        payload_sha256=str(payload["payload_sha256"]),
        quality_codes=tuple(str(value) for value in payload["quality_codes"]),
        provider_published_at=_parse_optional_datetime(
            payload["provider_published_at"], "provider_published_at"
        ),
        ingestion_segment_id=(
            UUID(str(payload["ingestion_segment_id"]))
            if payload["ingestion_segment_id"] is not None
            else None
        ),
        source_bar_revision_ids=tuple(
            UUID(str(value)) for value in payload["source_bar_revision_ids"]
        ),
        supersedes_bar_revision_id=(
            UUID(str(payload["supersedes_bar_revision_id"]))
            if payload["supersedes_bar_revision_id"] is not None
            else None
        ),
        reconciliation_status=(
            str(payload["reconciliation_status"])
            if payload["reconciliation_status"] is not None
            else None
        ),
        session_scope=BarSessionScope(str(payload["session_scope"])),
    )


_OPTION_FIELDS = (
    "snapshot_id",
    "contract_id",
    "contract_ticker",
    "underlyer",
    "provider",
    "contract_type",
    "expiration_date",
    "expiration_cutoff",
    "calendar_dte",
    "time_to_expiration_years",
    "strike",
    "shares_per_contract",
    "exercise_style",
    "spot",
    "spot_market_data_time",
    "bid",
    "ask",
    "midpoint",
    "display_mark",
    "model_mark",
    "mark_market_data_time",
    "mark_source",
    "day_volume",
    "open_interest",
    "market_data_time",
    "first_observed_at",
    "revised_observed_at",
    "local_iv",
    "local_gamma",
    "local_delta",
    "local_theta_per_day",
    "local_vega_per_vol_point",
    "local_rho_per_rate_point",
    "intrinsic_value",
    "extrinsic_value",
    "single_contract_breakeven",
    "provider_iv",
    "provider_gamma",
    "risk_free_rate",
    "dividend_yield",
    "iv_converged",
    "iv_solver",
    "iv_iteration_count",
    "iv_price_error",
    "iv_failure_reason",
    "model_version",
    "quality_flags",
    "batch_id",
    "raw_payload_sha256",
    "normalized_payload_sha256",
    "revision",
    "valuation_policy_version",
    "valuation_policy_sha256",
)


def _option_payload(snapshot: OptionContractSnapshot) -> dict[str, object]:
    return {
        "snapshot_id": str(snapshot.snapshot_id),
        "contract_id": snapshot.contract_id,
        "contract_ticker": snapshot.contract_ticker,
        "underlyer": snapshot.underlyer,
        "provider": snapshot.provider,
        "contract_type": snapshot.contract_type.value,
        "expiration_date": snapshot.expiration_date.isoformat(),
        "expiration_cutoff": snapshot.expiration_cutoff.isoformat(),
        "calendar_dte": snapshot.calendar_dte,
        "time_to_expiration_years": snapshot.time_to_expiration_years,
        "strike": str(snapshot.strike),
        "shares_per_contract": snapshot.shares_per_contract,
        "exercise_style": snapshot.exercise_style.value,
        "spot": str(snapshot.spot),
        "spot_market_data_time": snapshot.spot_market_data_time.isoformat(),
        "bid": _decimal_value(snapshot.bid),
        "ask": _decimal_value(snapshot.ask),
        "midpoint": _decimal_value(snapshot.midpoint),
        "display_mark": _decimal_value(snapshot.display_mark),
        "model_mark": _decimal_value(snapshot.model_mark),
        "mark_market_data_time": snapshot.mark_market_data_time.isoformat(),
        "mark_source": snapshot.mark_source.value,
        "day_volume": snapshot.day_volume,
        "open_interest": snapshot.open_interest,
        "market_data_time": snapshot.market_data_time.isoformat(),
        "first_observed_at": snapshot.first_observed_at.isoformat(),
        "revised_observed_at": _datetime_value(snapshot.revised_observed_at),
        "local_iv": snapshot.local_iv,
        "local_gamma": snapshot.local_gamma,
        "local_delta": snapshot.local_delta,
        "local_theta_per_day": snapshot.local_theta_per_day,
        "local_vega_per_vol_point": snapshot.local_vega_per_vol_point,
        "local_rho_per_rate_point": snapshot.local_rho_per_rate_point,
        "intrinsic_value": _decimal_value(snapshot.intrinsic_value),
        "extrinsic_value": _decimal_value(snapshot.extrinsic_value),
        "single_contract_breakeven": _decimal_value(
            snapshot.single_contract_breakeven
        ),
        "provider_iv": snapshot.provider_iv,
        "provider_gamma": snapshot.provider_gamma,
        "risk_free_rate": snapshot.risk_free_rate,
        "dividend_yield": snapshot.dividend_yield,
        "iv_converged": snapshot.iv_converged,
        "iv_solver": snapshot.iv_solver,
        "iv_iteration_count": snapshot.iv_iteration_count,
        "iv_price_error": snapshot.iv_price_error,
        "iv_failure_reason": snapshot.iv_failure_reason,
        "model_version": snapshot.model_version,
        "quality_flags": [flag.value for flag in snapshot.quality_flags],
        "batch_id": str(snapshot.batch_id),
        "raw_payload_sha256": snapshot.raw_payload_sha256,
        "normalized_payload_sha256": snapshot.normalized_payload_sha256,
        "revision": snapshot.revision,
        "valuation_policy_version": snapshot.valuation_policy_version,
        "valuation_policy_sha256": snapshot.valuation_policy_sha256,
    }


def _optional_float(value: object) -> float | None:
    return None if value is None else float(value)


def _option_from_payload(payload: Mapping[str, object]) -> OptionContractSnapshot:
    _require_contract_fields(payload, _OPTION_FIELDS, DEVELOPER_OPTION_SNAPSHOT_SCHEMA)
    expiration_date = payload["expiration_date"]
    if not isinstance(expiration_date, str):
        raise ValueError("option expiration_date must be an ISO date")
    return OptionContractSnapshot(
        snapshot_id=UUID(str(payload["snapshot_id"])),
        contract_id=int(payload["contract_id"]),
        contract_ticker=str(payload["contract_ticker"]),
        underlyer=str(payload["underlyer"]),
        provider=str(payload["provider"]),
        contract_type=ContractType(str(payload["contract_type"])),
        expiration_date=date.fromisoformat(expiration_date),
        expiration_cutoff=_parse_datetime(
            payload["expiration_cutoff"], "expiration_cutoff"
        ),
        calendar_dte=int(payload["calendar_dte"]),
        time_to_expiration_years=float(payload["time_to_expiration_years"]),
        strike=_parse_decimal(payload["strike"], "strike"),
        shares_per_contract=int(payload["shares_per_contract"]),
        exercise_style=ExerciseStyle(str(payload["exercise_style"])),
        spot=_parse_decimal(payload["spot"], "spot"),
        spot_market_data_time=_parse_datetime(
            payload["spot_market_data_time"], "spot_market_data_time"
        ),
        bid=_parse_optional_decimal(payload["bid"], "bid"),
        ask=_parse_optional_decimal(payload["ask"], "ask"),
        midpoint=_parse_optional_decimal(payload["midpoint"], "midpoint"),
        display_mark=_parse_optional_decimal(payload["display_mark"], "display_mark"),
        model_mark=_parse_optional_decimal(payload["model_mark"], "model_mark"),
        mark_market_data_time=_parse_datetime(
            payload["mark_market_data_time"], "mark_market_data_time"
        ),
        mark_source=MarkSource(str(payload["mark_source"])),
        day_volume=(
            int(payload["day_volume"]) if payload["day_volume"] is not None else None
        ),
        open_interest=(
            int(payload["open_interest"])
            if payload["open_interest"] is not None
            else None
        ),
        market_data_time=_parse_datetime(
            payload["market_data_time"], "market_data_time"
        ),
        first_observed_at=_parse_datetime(
            payload["first_observed_at"], "first_observed_at"
        ),
        revised_observed_at=_parse_optional_datetime(
            payload["revised_observed_at"], "revised_observed_at"
        ),
        local_iv=_optional_float(payload["local_iv"]),
        local_gamma=_optional_float(payload["local_gamma"]),
        local_delta=_optional_float(payload["local_delta"]),
        local_theta_per_day=_optional_float(payload["local_theta_per_day"]),
        local_vega_per_vol_point=_optional_float(
            payload["local_vega_per_vol_point"]
        ),
        local_rho_per_rate_point=_optional_float(
            payload["local_rho_per_rate_point"]
        ),
        intrinsic_value=_parse_optional_decimal(
            payload["intrinsic_value"], "intrinsic_value"
        ),
        extrinsic_value=_parse_optional_decimal(
            payload["extrinsic_value"], "extrinsic_value"
        ),
        single_contract_breakeven=_parse_optional_decimal(
            payload["single_contract_breakeven"], "single_contract_breakeven"
        ),
        provider_iv=_optional_float(payload["provider_iv"]),
        provider_gamma=_optional_float(payload["provider_gamma"]),
        risk_free_rate=float(payload["risk_free_rate"]),
        dividend_yield=float(payload["dividend_yield"]),
        iv_converged=bool(payload["iv_converged"]),
        iv_solver=(str(payload["iv_solver"]) if payload["iv_solver"] is not None else None),
        iv_iteration_count=int(payload["iv_iteration_count"]),
        iv_price_error=_optional_float(payload["iv_price_error"]),
        iv_failure_reason=(
            str(payload["iv_failure_reason"])
            if payload["iv_failure_reason"] is not None
            else None
        ),
        model_version=str(payload["model_version"]),
        quality_flags=tuple(
            DataQualityFlag(str(value)) for value in payload["quality_flags"]
        ),
        batch_id=UUID(str(payload["batch_id"])),
        raw_payload_sha256=str(payload["raw_payload_sha256"]),
        normalized_payload_sha256=str(payload["normalized_payload_sha256"]),
        revision=int(payload["revision"]),
        valuation_policy_version=(
            str(payload["valuation_policy_version"])
            if payload["valuation_policy_version"] is not None
            else None
        ),
        valuation_policy_sha256=(
            str(payload["valuation_policy_sha256"])
            if payload["valuation_policy_sha256"] is not None
            else None
        ),
    )


@dataclass(frozen=True, slots=True)
class DeveloperEquityBarAcquisitionFact:
    instrument_id: int
    revision: EquityBarRevision

    def __post_init__(self) -> None:
        if type(self.instrument_id) is not int or self.instrument_id <= 0:
            raise ValueError("Developer bar acquisition instrument ID must be positive")
        if (
            not self.revision.is_final
            or self.revision.source_kind not in {
                BarSourceKind.NATIVE_REST,
                BarSourceKind.DERIVED,
            }
            or self.revision.ingestion_segment_id is None
            or (
                self.revision.source_kind is BarSourceKind.DERIVED
                and not self.revision.source_bar_revision_ids
            )
        ):
            raise ValueError(
                "Developer acquisition requires a final native or lineage-bound derived bar"
            )

    @property
    def provider_batch_id(self) -> UUID:
        assert self.revision.ingestion_segment_id is not None
        return self.revision.ingestion_segment_id

    @property
    def provider_revision_id(self) -> UUID:
        return self.revision.bar_revision_id

    @property
    def market_time(self) -> datetime:
        return self.revision.bar_end

    @property
    def observed_at(self) -> datetime:
        return self.revision.system_observed_at

    @property
    def raw_payload_sha256(self) -> str:
        return self.revision.payload_sha256

    @property
    def normalized_payload_sha256(self) -> str:
        return self.revision.payload_sha256

    def structured_fact(self) -> dict[str, object]:
        return _bar_payload(self.revision)


@dataclass(frozen=True, slots=True)
class DeveloperOptionSnapshotAcquisitionFact:
    instrument_id: int
    snapshot: OptionContractSnapshot

    def __post_init__(self) -> None:
        if type(self.instrument_id) is not int or self.instrument_id <= 0:
            raise ValueError("Developer option acquisition instrument ID must be positive")

    @property
    def provider_batch_id(self) -> UUID:
        return self.snapshot.batch_id

    @property
    def provider_revision_id(self) -> UUID:
        return self.snapshot.snapshot_id

    @property
    def market_time(self) -> datetime:
        return self.snapshot.market_data_time

    @property
    def observed_at(self) -> datetime:
        return self.snapshot.revised_observed_at or self.snapshot.first_observed_at

    @property
    def raw_payload_sha256(self) -> str:
        return self.snapshot.raw_payload_sha256

    @property
    def normalized_payload_sha256(self) -> str:
        return self.snapshot.normalized_payload_sha256

    def structured_fact(self) -> dict[str, object]:
        return _option_payload(self.snapshot)


DeveloperAcquisitionFact = (
    DeveloperEquityBarAcquisitionFact | DeveloperOptionSnapshotAcquisitionFact
)


def _batch_contract(
    *,
    cohort_id: UUID,
    provider_tier: ProviderTier,
    fact_kind: DeveloperAcquisitionFactKind,
    provider: str,
    provider_batch_id: UUID,
    started_at: datetime,
    completed_at: datetime,
    facts: tuple[DeveloperAcquisitionFact, ...],
) -> dict[str, object]:
    return {
        "cohort_id": str(cohort_id),
        "provider_tier": provider_tier.value,
        "fact_kind": fact_kind.value,
        "fact_schema_version": fact_kind.schema_version,
        "provider": provider,
        "provider_batch_id": str(provider_batch_id),
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "facts": [
            {
                "fact_index": index,
                "instrument_id": fact.instrument_id,
                "provider_revision_id": str(fact.provider_revision_id),
                "market_time": fact.market_time.isoformat(),
                "observed_at": fact.observed_at.isoformat(),
                "raw_payload_sha256": fact.raw_payload_sha256,
                "normalized_payload_sha256": fact.normalized_payload_sha256,
                "structured_fact": fact.structured_fact(),
            }
            for index, fact in enumerate(facts)
        ],
    }


@dataclass(frozen=True, slots=True)
class DeveloperAcquisitionBatch:
    acquisition_batch_id: UUID
    cohort_id: UUID
    provider_tier: ProviderTier
    fact_kind: DeveloperAcquisitionFactKind
    provider: str
    provider_batch_id: UUID
    started_at: datetime
    completed_at: datetime
    facts: tuple[DeveloperAcquisitionFact, ...]
    batch_payload_sha256: str
    request_id: UUID | None = None

    def __post_init__(self) -> None:
        provider = self.provider.strip()
        if not provider:
            raise ValueError("Developer acquisition provider cannot be blank")
        started = _utc(self.started_at, "started_at")
        completed = _utc(self.completed_at, "completed_at")
        if completed < started:
            raise ValueError("Developer acquisition completion precedes its start")
        if not 1 <= len(self.facts) <= MAX_DEVELOPER_ACQUISITION_FACTS:
            raise ValueError("Developer acquisition requires 1-1000 facts")
        if self.provider_tier is not ProviderTier.MASSIVE_DEVELOPER:
            raise ValueError("Developer acquisition cannot be relabelled to another tier")
        expected_type = (
            DeveloperEquityBarAcquisitionFact
            if self.fact_kind is DeveloperAcquisitionFactKind.EQUITY_BAR
            else DeveloperOptionSnapshotAcquisitionFact
        )
        if any(not isinstance(fact, expected_type) for fact in self.facts):
            raise ValueError("Developer acquisition fact kind is inconsistent")
        if any(fact.provider_batch_id != self.provider_batch_id for fact in self.facts):
            raise ValueError("Developer acquisition facts cross provider batches")
        if self.fact_kind is DeveloperAcquisitionFactKind.OPTION_SNAPSHOT and any(
            fact.snapshot.provider != provider
            for fact in self.facts
            if isinstance(fact, DeveloperOptionSnapshotAcquisitionFact)
        ):
            raise ValueError("Developer option acquisition provider is inconsistent")
        if max(fact.observed_at for fact in self.facts) > completed:
            raise ValueError("Developer acquisition completed before a fact was observed")
        identities = tuple(
            (fact.instrument_id, fact.provider_revision_id) for fact in self.facts
        )
        if len(identities) != len(set(identities)):
            raise ValueError("Developer acquisition contains duplicate fact identities")
        contract = _batch_contract(
            cohort_id=self.cohort_id,
            provider_tier=self.provider_tier,
            fact_kind=self.fact_kind,
            provider=provider,
            provider_batch_id=self.provider_batch_id,
            started_at=started,
            completed_at=completed,
            facts=self.facts,
        )
        expected_hash = canonical_sha256(contract)
        if _sha256(self.batch_payload_sha256, "batch_payload_sha256") != expected_hash:
            raise ValueError("Developer acquisition batch payload hash mismatch")
        expected_id = uuid5(
            NAMESPACE_URL,
            f"developer-acquisition:{self.cohort_id}:{self.fact_kind.value}:"
            f"{provider}:{self.provider_batch_id}:{expected_hash}",
        )
        if self.acquisition_batch_id != expected_id:
            raise ValueError("Developer acquisition batch identity mismatch")
        object.__setattr__(self, "provider", provider)
        object.__setattr__(self, "started_at", started)
        object.__setattr__(self, "completed_at", completed)


def build_developer_acquisition_batch(
    *,
    cohort_id: UUID,
    provider: str,
    provider_batch_id: UUID,
    started_at: datetime,
    completed_at: datetime,
    facts: Sequence[DeveloperAcquisitionFact],
    request_id: UUID | None = None,
) -> DeveloperAcquisitionBatch:
    rows = tuple(facts)
    if not rows:
        raise ValueError("Developer acquisition requires 1-1000 facts")
    fact_kind = (
        DeveloperAcquisitionFactKind.EQUITY_BAR
        if isinstance(rows[0], DeveloperEquityBarAcquisitionFact)
        else DeveloperAcquisitionFactKind.OPTION_SNAPSHOT
    )
    normalized_provider = provider.strip()
    started = _utc(started_at, "started_at")
    completed = _utc(completed_at, "completed_at")
    contract = _batch_contract(
        cohort_id=cohort_id,
        provider_tier=ProviderTier.MASSIVE_DEVELOPER,
        fact_kind=fact_kind,
        provider=normalized_provider,
        provider_batch_id=provider_batch_id,
        started_at=started,
        completed_at=completed,
        facts=rows,
    )
    payload_hash = canonical_sha256(contract)
    acquisition_batch_id = uuid5(
        NAMESPACE_URL,
        f"developer-acquisition:{cohort_id}:{fact_kind.value}:"
        f"{normalized_provider}:{provider_batch_id}:{payload_hash}",
    )
    return DeveloperAcquisitionBatch(
        acquisition_batch_id=acquisition_batch_id,
        cohort_id=cohort_id,
        provider_tier=ProviderTier.MASSIVE_DEVELOPER,
        fact_kind=fact_kind,
        provider=normalized_provider,
        provider_batch_id=provider_batch_id,
        started_at=started,
        completed_at=completed,
        facts=rows,
        batch_payload_sha256=payload_hash,
        request_id=request_id,
    )


def _database_identity(row) -> tuple[object, ...] | None:
    if row is None:
        return None
    return tuple(str(value) if isinstance(value, UUID) else value for value in row)


def _fact_rows(batch: DeveloperAcquisitionBatch) -> tuple[tuple[object, ...], ...]:
    return tuple(
        (
            str(batch.acquisition_batch_id),
            index,
            fact.instrument_id,
            str(fact.provider_revision_id),
            fact.market_time,
            fact.observed_at,
            fact.raw_payload_sha256,
            fact.normalized_payload_sha256,
            fact.structured_fact(),
        )
        for index, fact in enumerate(batch.facts)
    )


def persist_developer_acquisition_batch_cursor(
    cursor,
    batch: DeveloperAcquisitionBatch,
    *,
    values_executor=execute_values,
) -> None:
    rows = _fact_rows(batch)
    cursor.execute(
        "INSERT INTO rt.acquisition_batches ("
        "acquisition_batch_id, cohort_id, provider_tier, fact_kind, "
        "fact_schema_version, provider, provider_batch_id, request_id, started_at, "
        "completed_at, fact_count, batch_payload_sha256) VALUES ("
        "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
        "ON CONFLICT DO NOTHING",
        (
            str(batch.acquisition_batch_id),
            str(batch.cohort_id),
            batch.provider_tier.value,
            batch.fact_kind.value,
            batch.fact_kind.schema_version,
            batch.provider,
            str(batch.provider_batch_id),
            str(batch.request_id) if batch.request_id is not None else None,
            batch.started_at,
            batch.completed_at,
            len(batch.facts),
            batch.batch_payload_sha256,
        ),
    )
    # The batch row is the idempotency catalog: facts are written only for a new batch.
    if cursor.rowcount == 1:
        values_executor(
            cursor,
            "INSERT INTO rt.acquisition_facts ("
            "acquisition_batch_id, fact_index, instrument_id, provider_revision_id, "
            "market_time, observed_at, raw_payload_sha256, normalized_payload_sha256, "
            "structured_fact) VALUES %s ON CONFLICT DO NOTHING",
            [(*row[:-1], Json(row[-1])) for row in rows],
            page_size=ACQUISITION_FACT_PAGE_SIZE,
        )
    cursor.execute(
        "SELECT cohort_id, provider_tier, fact_kind, fact_schema_version, "
        "provider, provider_batch_id, started_at, completed_at, fact_count, "
        "batch_payload_sha256 FROM rt.acquisition_batches "
        "WHERE acquisition_batch_id=%s",
        (str(batch.acquisition_batch_id),),
    )
    expected_batch = (
        str(batch.cohort_id),
        batch.provider_tier.value,
        batch.fact_kind.value,
        batch.fact_kind.schema_version,
        batch.provider,
        str(batch.provider_batch_id),
        batch.started_at,
        batch.completed_at,
        len(batch.facts),
        batch.batch_payload_sha256,
    )
    if _database_identity(cursor.fetchone()) != expected_batch:
        raise ValueError("persisted Developer acquisition batch identity conflict")
    observed = [fact.observed_at for fact in batch.facts]
    cursor.execute(
        "SELECT acquisition_batch_id, fact_index, instrument_id, "
        "provider_revision_id, market_time, observed_at, raw_payload_sha256, "
        "normalized_payload_sha256, structured_fact "
        "FROM rt.acquisition_facts WHERE acquisition_batch_id=%s "
        "AND observed_at BETWEEN %s AND %s ORDER BY fact_index",
        (str(batch.acquisition_batch_id), min(observed), max(observed)),
    )
    stored_rows = tuple(_database_identity(row) for row in cursor.fetchall())
    if stored_rows != rows:
        raise ValueError("persisted Developer acquisition fact identity conflict")


def persist_developer_acquisition_batch(
    connection,
    batch: DeveloperAcquisitionBatch,
    *,
    values_executor=execute_values,
) -> None:
    try:
        with connection.cursor() as cursor:
            persist_developer_acquisition_batch_cursor(
                cursor,
                batch,
                values_executor=values_executor,
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def _decode_fact(
    *,
    fact_kind: DeveloperAcquisitionFactKind,
    provider: str,
    provider_batch_id: UUID,
    row: tuple[object, ...],
) -> DeveloperAcquisitionFact:
    (
        _fact_index,
        instrument_id,
        provider_revision_id,
        market_time,
        observed_at,
        raw_payload_sha256,
        normalized_payload_sha256,
        structured_fact,
    ) = row
    if not isinstance(structured_fact, Mapping):
        raise ValueError("Developer acquisition structured fact must be an object")
    if fact_kind is DeveloperAcquisitionFactKind.EQUITY_BAR:
        fact: DeveloperAcquisitionFact = DeveloperEquityBarAcquisitionFact(
            int(instrument_id),
            _bar_from_payload(structured_fact),
        )
    else:
        fact = DeveloperOptionSnapshotAcquisitionFact(
            int(instrument_id),
            _option_from_payload(structured_fact),
        )
        if fact.snapshot.provider != provider:
            raise ValueError("Developer option acquisition provider is inconsistent")
    expected = (
        fact.provider_batch_id,
        fact.provider_revision_id,
        fact.market_time,
        fact.observed_at,
        fact.raw_payload_sha256,
        fact.normalized_payload_sha256,
    )
    actual = (
        provider_batch_id,
        UUID(str(provider_revision_id)),
        market_time,
        observed_at,
        str(raw_payload_sha256),
        str(normalized_payload_sha256),
    )
    if actual != expected:
        raise ValueError("Developer acquisition explicit lineage conflicts with its fact")
    return fact


def _restore_developer_acquisition_batch(
    batch_row: tuple[object, ...],
    fact_rows: tuple[tuple[object, ...], ...],
) -> DeveloperAcquisitionBatch:
    (
        stored_batch_id,
        cohort_id,
        provider_tier,
        fact_kind_value,
        fact_schema_version,
        provider,
        provider_batch_id,
        started_at,
        completed_at,
        fact_count,
        batch_payload_sha256,
        request_id,
    ) = batch_row
    fact_kind = DeveloperAcquisitionFactKind(str(fact_kind_value))
    if str(fact_schema_version) != fact_kind.schema_version:
        raise ValueError("Developer acquisition fact schema is unsupported")
    if int(fact_count) != len(fact_rows) or tuple(
        int(row[0]) for row in fact_rows
    ) != tuple(range(len(fact_rows))):
        raise ValueError("Developer acquisition fact set is incomplete")
    source_batch_id = UUID(str(provider_batch_id))
    facts = tuple(
        _decode_fact(
            fact_kind=fact_kind,
            provider=str(provider),
            provider_batch_id=source_batch_id,
            row=row,
        )
        for row in fact_rows
    )
    return DeveloperAcquisitionBatch(
        acquisition_batch_id=UUID(str(stored_batch_id)),
        cohort_id=UUID(str(cohort_id)),
        provider_tier=ProviderTier(str(provider_tier)),
        fact_kind=fact_kind,
        provider=str(provider),
        provider_batch_id=source_batch_id,
        started_at=started_at,
        completed_at=completed_at,
        facts=facts,
        batch_payload_sha256=str(batch_payload_sha256),
        request_id=UUID(str(request_id)) if request_id is not None else None,
    )


def load_developer_acquisition_batch_cursor(
    cursor,
    acquisition_batch_id: UUID,
) -> DeveloperAcquisitionBatch:
    cursor.execute(
        "SELECT acquisition_batch_id, cohort_id, provider_tier, fact_kind, "
        "fact_schema_version, provider, provider_batch_id, started_at, completed_at, "
        "fact_count, batch_payload_sha256, request_id FROM rt.acquisition_batches "
        "WHERE acquisition_batch_id=%s",
        (str(acquisition_batch_id),),
    )
    batch_row = cursor.fetchone()
    if batch_row is None:
        raise ValueError("Developer acquisition batch does not exist")
    # Facts are never observed after their batch completed, so later partitions are skipped.
    cursor.execute(
        "SELECT fact_index, instrument_id, provider_revision_id, market_time, "
        "observed_at, raw_payload_sha256, normalized_payload_sha256, structured_fact "
        "FROM rt.acquisition_facts WHERE acquisition_batch_id=%s "
        "AND observed_at <= %s ORDER BY fact_index",
        (str(acquisition_batch_id), batch_row[8]),
    )
    return _restore_developer_acquisition_batch(batch_row, tuple(cursor.fetchall()))


def load_developer_acquisition_batch(
    connection,
    acquisition_batch_id: UUID,
) -> DeveloperAcquisitionBatch:
    with connection.cursor() as cursor:
        return load_developer_acquisition_batch_cursor(cursor, acquisition_batch_id)


def developer_equity_bar_facts(
    batch: DeveloperAcquisitionBatch,
) -> tuple[RealtimeEquityBarFact, ...]:
    if batch.fact_kind is not DeveloperAcquisitionFactKind.EQUITY_BAR:
        raise ValueError("Developer acquisition batch does not contain equity bars")
    return tuple(
        adapt_developer_equity_bar(
            fact.revision,
            cohort_id=batch.cohort_id,
            instrument_id=fact.instrument_id,
        )
        for fact in batch.facts
        if isinstance(fact, DeveloperEquityBarAcquisitionFact)
    )


def persist_developer_equity_acquisition(
    connection,
    batch: DeveloperAcquisitionBatch,
    *,
    repository=persist_realtime_equity_bars,
) -> None:
    repository(connection, developer_equity_bar_facts(batch))


def developer_option_snapshots(
    batch: DeveloperAcquisitionBatch,
) -> dict[int, OptionContractSnapshot]:
    if batch.fact_kind is not DeveloperAcquisitionFactKind.OPTION_SNAPSHOT:
        raise ValueError("Developer acquisition batch does not contain option snapshots")
    return {
        fact.instrument_id: fact.snapshot
        for fact in batch.facts
        if isinstance(fact, DeveloperOptionSnapshotAcquisitionFact)
    }