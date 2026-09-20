"""Causal adapter for retained stock-idea setup evidence, without detection or I/O."""
from __future__ import annotations

import hashlib
import json
import math
from collections import Counter
from datetime import datetime
from typing import ClassVar, Literal
from uuid import UUID

from pydantic import AwareDatetime, Field, model_validator

from .behavior import BehaviorSource, Contract, Name, Sha256
from research.stock_idea_engine import candidate_record, digest, read_candidate


class StockSetupEvidence(Contract):
    schema_version: Literal["stock_setup_evidence_v1"] = "stock_setup_evidence_v1"
    source: BehaviorSource
    lifecycle_payload_text: str
    candidate_payload_sha256: Sha256
    atr_formula: Literal["stock_ideas_ewm14_first_seed_activation_v1"] = (
        "stock_ideas_ewm14_first_seed_activation_v1"
    )
    execution_permission: Literal[False] = False

    @property
    def candidate(self):
        payload = json.loads(self.lifecycle_payload_text)["candidate"]
        return read_candidate({key: value for key, value in payload.items() if key != "episode_id"})

    @model_validator(mode="after")
    def validate_setup(self):
        source = self.source
        if len(self.lifecycle_payload_text.encode("ascii")) > 262144:
            raise ValueError("setup payload exceeds contract size")
        if hashlib.sha256(self.lifecycle_payload_text.encode("ascii")).hexdigest() != source.payload_sha256:
            raise ValueError("setup source payload hash mismatch")
        lifecycle = json.loads(self.lifecycle_payload_text)
        canonical = json.dumps(lifecycle, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
        if canonical != self.lifecycle_payload_text:
            raise ValueError("setup lifecycle must be canonical")
        if lifecycle.get("setup") != "TRIGGERED" or lifecycle.get("health") != "READY":
            raise ValueError("setup requires a retained healthy trigger")
        candidate = self.candidate
        if candidate.health != "READY" or candidate.selection_block is not None:
            raise ValueError("blocked setup cannot be bound")
        if candidate.model != "acceptance" or candidate.interval != "1h" or candidate.horizon != "INTRADAY":
            raise ValueError("only hourly acceptance setups are supported")
        if candidate.policy_version != "range_breakout_acceptance_intraday_v1":
            raise ValueError("unsupported setup detector version")
        if lifecycle["candidate"].get("episode_id") != candidate.episode_id:
            raise ValueError("setup episode identity mismatch")
        if digest(candidate_record(candidate)) != self.candidate_payload_sha256:
            raise ValueError("setup candidate hash mismatch")
        if not candidate.revision_ids or len(set(candidate.revision_ids)) != len(candidate.revision_ids):
            raise ValueError("setup requires exact distinct source revisions")
        if tuple(lifecycle.get("source_revision_ids", ())) != candidate.revision_ids:
            raise ValueError("setup source revisions do not match candidate")
        if str(source.security_id) != candidate.security_id or source.interval != candidate.interval:
            raise ValueError("setup source security/interval mismatch")
        if source.price_basis != "RAW_ACTION_GATED" or source.availability_mode != "PROSPECTIVE_RECEIPT":
            raise ValueError("setup requires prospective raw action-gated source")
        if not candidate.trigger_at <= candidate.available_at <= source.received_at < candidate.expires_at:
            raise ValueError("setup receipt or expiry is not causal")
        if source.market_time != candidate.trigger_at or source.valid_until > candidate.expires_at:
            raise ValueError("setup source must preserve trigger and expiry")
        if source.observed_at < candidate.available_at:
            raise ValueError("setup source cannot precede candidate availability")
        values = (candidate.reference, candidate.stop, candidate.target, candidate.price, candidate.activation_atr)
        if any(value <= 0 for value in values):
            raise ValueError("setup price and ATR anchors must be positive")
        return self

    def available_at(self, decision_at: datetime, market_cutoff: datetime) -> bool:
        if decision_at.tzinfo is None or market_cutoff.tzinfo is None:
            raise ValueError("setup cutoffs must be timezone-aware")
        return (
            self.source.market_time <= market_cutoff <= decision_at
            and self.source.received_at <= decision_at < self.source.valid_until
        )


def bind_stock_setup(lifecycle: dict, source: BehaviorSource) -> StockSetupEvidence:
    payload_text = json.dumps(lifecycle, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
    candidate_payload = lifecycle.get("candidate", {})
    candidate = read_candidate({key: value for key, value in candidate_payload.items() if key != "episode_id"})
    return StockSetupEvidence(
        source=source, lifecycle_payload_text=payload_text,
        candidate_payload_sha256=digest(candidate_record(candidate)),
    )


class PublishedSetupSourcePolicy(Contract):
    version: Literal["stock_setup_publication_source_v1"] = "stock_setup_publication_source_v1"
    instance_id: Sha256
    instance_policy_sha256: Sha256
    publication_policy_sha256: Sha256
    runtime_sources: tuple[tuple[Name, Sha256], ...] = Field(min_length=1)
    detector_version: Literal["range_breakout_acceptance_intraday_v2"] = "range_breakout_acceptance_intraday_v2"
    price_basis: Literal["RAW_ACTION_GATED"] = "RAW_ACTION_GATED"

    @model_validator(mode="after")
    def validate_runtime_sources(self):
        names = tuple(name for name, _ in self.runtime_sources)
        if names != tuple(sorted(set(names))):
            raise ValueError("setup runtime sources must be distinct and sorted")
        required = {"research/stock_idea_models.py", "research/stock_idea_engine.py",
                    "equity/stock_idea_forward_source.py", "research/stock_idea_forward.py",
                    "research/stock_idea_replay.py", "scripts/run_stock_idea_worker.py"}
        if not required <= set(names):
            raise ValueError("setup policy must bind all runtime source owners")
        return self


class PublishedSetupReceipt(Contract):
    instance_id: Sha256
    record_id: Name
    payload_sha256: Sha256
    policy_sha256: Sha256
    security_id: UUID
    interval: Literal["1h"] = "1h"
    market_time: AwareDatetime
    observed_at: AwareDatetime
    recorded_at: AwareDatetime
    received_at: AwareDatetime
    valid_until: AwareDatetime

    @model_validator(mode="after")
    def validate_clocks(self):
        if not self.market_time <= self.observed_at <= self.recorded_at <= self.received_at < self.valid_until:
            raise ValueError("published setup requires causal receipt before original expiry")
        return self


class PublishedStockSetupEvidence(Contract):
    expected_model: ClassVar[str] = "acceptance"
    schema_version: Literal["stock_setup_publication_evidence_v1"] = "stock_setup_publication_evidence_v1"
    source: PublishedSetupReceipt
    candidate_payload_text: str
    candidate_payload_sha256: Sha256
    episode_id: Sha256
    source_policy: PublishedSetupSourcePolicy
    atr_formula: Literal["stock_ideas_ewm14_first_seed_activation_v1"] = "stock_ideas_ewm14_first_seed_activation_v1"
    execution_permission: Literal[False] = False

    @property
    def candidate(self):
        return read_candidate(json.loads(self.candidate_payload_text))

    @model_validator(mode="after")
    def validate_candidate(self):
        if len(self.candidate_payload_text.encode("ascii")) > 262144:
            raise ValueError("published setup candidate exceeds bound")
        candidate = self.candidate
        canonical = json.dumps(candidate_record(candidate), sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
        if canonical != self.candidate_payload_text or digest(candidate_record(candidate)) != self.candidate_payload_sha256:
            raise ValueError("published setup candidate checksum mismatch")
        if self.source.policy_sha256 != self.source_policy.sha256 or self.source.instance_id != self.source_policy.instance_id:
            raise ValueError("published setup source policy mismatch")
        if (candidate.episode_id != self.episode_id or str(self.source.security_id) != candidate.security_id
                or candidate.interval != "1h" or candidate.model != self.expected_model or candidate.horizon != "INTRADAY"
                or candidate.policy_version != self.source_policy.detector_version):
            raise ValueError("published setup detector/episode/identity mismatch")
        if candidate.health != "READY" or candidate.selection_block is not None:
            raise ValueError("published setup is not healthy")
        if not candidate.trigger_at <= candidate.available_at <= self.source.observed_at:
            raise ValueError("publication predates setup availability")
        if self.source.market_time != candidate.trigger_at or self.source.valid_until != candidate.expires_at:
            raise ValueError("published setup trigger/expiry mismatch")
        if not candidate.revision_ids or len(set(candidate.revision_ids)) != len(candidate.revision_ids):
            raise ValueError("published setup requires exact distinct revisions")
        for revision in candidate.revision_ids:
            UUID(revision)
        if min(candidate.reference, candidate.stop, candidate.target, candidate.price, candidate.activation_atr) <= 0:
            raise ValueError("published setup anchors must be positive")
        return self

    def available_at(self, decision_at: datetime, market_cutoff: datetime) -> bool:
        if decision_at.tzinfo is None or market_cutoff.tzinfo is None:
            raise ValueError("setup cutoffs must be timezone-aware")
        return self.source.market_time <= market_cutoff <= decision_at and self.source.received_at <= decision_at < self.source.valid_until


class DirectSetupSourcePolicy(PublishedSetupSourcePolicy):
    version: Literal["stock_setup_direct_source_v1"] = "stock_setup_direct_source_v1"
    availability_contract: Literal["DIRECT_COMMITTED_LEDGER_RECEIPT"] = "DIRECT_COMMITTED_LEDGER_RECEIPT"


class DirectSetupReceipt(Contract):
    instance_id: Sha256
    record_id: Name
    payload_sha256: Sha256
    policy_sha256: Sha256
    security_id: UUID
    interval: Literal["1h"] = "1h"
    market_time: AwareDatetime
    observed_at: AwareDatetime
    received_at: AwareDatetime
    valid_until: AwareDatetime

    @model_validator(mode="after")
    def validate_clocks(self):
        if not self.market_time <= self.observed_at <= self.received_at < self.valid_until:
            raise ValueError("direct setup requires committed read receipt before original expiry")
        return self


class DirectStockSetupEvidence(PublishedStockSetupEvidence):
    schema_version: Literal["stock_setup_direct_evidence_v1"] = "stock_setup_direct_evidence_v1"
    source: DirectSetupReceipt
    source_policy: DirectSetupSourcePolicy


class DirectResumptionSourcePolicy(DirectSetupSourcePolicy):
    version: Literal["stock_resumption_direct_source_v1"] = "stock_resumption_direct_source_v1"
    detector_version: Literal["relative_trend_resumption_intraday_v1"] = "relative_trend_resumption_intraday_v1"


class DirectStockResumptionEvidence(DirectStockSetupEvidence):
    expected_model: ClassVar[str] = "resumption"
    schema_version: Literal["stock_resumption_direct_evidence_v1"] = "stock_resumption_direct_evidence_v1"
    source_policy: DirectResumptionSourcePolicy


def resolve_direct_setup_policy(value):
    payload = value.model_dump(mode="json") if isinstance(value, Contract) else value
    if not isinstance(payload, dict):
        raise ValueError("direct setup reader requires a versioned source policy")
    contracts = {"stock_setup_direct_source_v1": DirectSetupSourcePolicy,
        "stock_resumption_direct_source_v1": DirectResumptionSourcePolicy}
    contract = contracts.get(payload.get("version"))
    if contract is None:
        raise ValueError("unsupported direct setup source policy version")
    return contract.model_validate(payload)


def _validated_publication_candidate(*, instance, publication, record_id, payload_sha256,
                                     episode_id, security_id, ticker, policy):
    identity = {key: instance[key] for key in ("policy_hash", "enrolled_at", "enrollment_sha256")}
    if (instance["instance_id"] != policy.instance_id
            or digest(["stock_alert_results_v1", identity]) != policy.instance_id
            or instance["policy_hash"] != policy.instance_policy_sha256
            or digest(instance["policy"]) != policy.instance_policy_sha256
            or digest(instance["enrollment"]) != instance["enrollment_sha256"]
            or instance["stream"] != "intraday"
            or instance["policy_version"] != "stock_ideas_forward_quality_v2"):
        raise ValueError("retained setup instance does not match trusted policy")
    if (digest(publication) != payload_sha256 or publication["window_key"] != record_id
            or publication["policy_hash"] != policy.publication_policy_sha256
            or publication["runtime_sources"] != dict(policy.runtime_sources)
            or publication["policy_version"] != instance["policy_version"]
            or publication.get("source") != "SHADOW" or publication.get("coverage") != "PUBLISHED"):
        raise ValueError("retained setup publication does not match trusted policy")
    candidate = read_candidate(publication["candidates"][episode_id])
    if candidate.security_id != str(security_id) or candidate.ticker != ticker:
        raise ValueError("requested setup security/ticker mismatch")
    if (str(security_id) not in publication["expected_members"]
            or str(security_id) in publication["missing_members"]):
        raise ValueError("setup member unavailable at publication")
    dispositions = [row for row in publication["dispositions"] if row["episode_id"] == episode_id]
    if len(dispositions) != 1:
        raise ValueError("setup requires one retained disposition")
    disposition = dispositions[0]
    allowed_suppression = {"MODEL_QUOTA", "MODEL_STOCK_DUPLICATE", "ACTIVE_POSITION_CAP"}
    if not (disposition["selection"] in ("SELECTED", "ELIGIBLE") and disposition.get("reason") is None
            or disposition["selection"] == "SUPPRESSED" and disposition.get("reason") in allowed_suppression):
        raise ValueError("setup disposition is unavailable or structurally blocked")
    if any(disposition[key] != getattr(candidate, key) for key in ("security_id", "model", "interval", "direction")):
        raise ValueError("setup disposition identity mismatch")
    publication_at = datetime.fromisoformat(publication["actual_publication_at"])
    boundary = datetime.fromisoformat(publication.get("market_time", record_id))
    if not (candidate.trigger_at <= boundary <= publication_at
            and datetime.fromisoformat(instance["enrolled_at"]) < candidate.trigger_at
            and datetime.fromisoformat(publication["input_deadline"]) <= publication_at
            <= datetime.fromisoformat(publication["latest_dispatch_at"])):
        raise ValueError("setup publication clocks are not causal")
    return candidate, publication_at


def bind_published_stock_setup(*, instance: dict, publication: dict, record_id: str,
                             payload_sha256: str, imported_at: datetime, received_at: datetime,
                             episode_id: str, security_id: UUID, ticker: str,
                             policy: PublishedSetupSourcePolicy) -> PublishedStockSetupEvidence:
    if type(policy) is not PublishedSetupSourcePolicy:
        raise ValueError("shared publication reader requires its exact source policy")
    candidate, publication_at = _validated_publication_candidate(instance=instance, publication=publication,
        record_id=record_id, payload_sha256=payload_sha256, episode_id=episode_id,
        security_id=security_id, ticker=ticker, policy=policy)
    return PublishedStockSetupEvidence(
        source=PublishedSetupReceipt(instance_id=policy.instance_id, record_id=record_id,
            payload_sha256=payload_sha256, policy_sha256=policy.sha256, security_id=security_id,
            market_time=candidate.trigger_at, observed_at=publication_at, recorded_at=imported_at,
            received_at=received_at, valid_until=candidate.expires_at),
        candidate_payload_text=json.dumps(candidate_record(candidate), sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False),
        candidate_payload_sha256=digest(candidate_record(candidate)), episode_id=episode_id, source_policy=policy,
    )


def bind_direct_stock_setup(*, instance: dict, publication: dict, record_id: str,
                           payload_sha256: str, received_at: datetime, episode_id: str,
                           security_id: UUID, ticker: str, policy: DirectSetupSourcePolicy) -> DirectStockSetupEvidence:
    evidence_types = {DirectSetupSourcePolicy: DirectStockSetupEvidence,
        DirectResumptionSourcePolicy: DirectStockResumptionEvidence}
    if type(policy) not in evidence_types:
        raise ValueError("direct reader requires its exact source policy")
    evidence_type = evidence_types[type(policy)]
    policy = resolve_direct_setup_policy(policy)
    candidate, publication_at = _validated_publication_candidate(instance=instance, publication=publication,
        record_id=record_id, payload_sha256=payload_sha256, episode_id=episode_id,
        security_id=security_id, ticker=ticker, policy=policy)
    return evidence_type(
        source=DirectSetupReceipt(instance_id=policy.instance_id, record_id=record_id,
            payload_sha256=payload_sha256, policy_sha256=policy.sha256, security_id=security_id,
            market_time=candidate.trigger_at, observed_at=publication_at,
            received_at=received_at, valid_until=candidate.expires_at),
        candidate_payload_text=json.dumps(candidate_record(candidate), sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False),
        candidate_payload_sha256=digest(candidate_record(candidate)), episode_id=episode_id, source_policy=policy,
    )


def summarize_setup_publications(rows, tickers, received_at):
    counts, versions, reasons, by_ticker = Counter(), Counter(), Counter(), Counter()
    identities = {}
    samples = []
    episodes, excluded = set(), []
    latest = None
    for row in rows:
        publication = row["publication"]
        if publication is None:
            reasons["PUBLICATION_PAYLOAD_BOUND_EXCEEDED"] += 1
            continue
        if digest(publication) != row["payload_sha256"]:
            reasons["PUBLICATION_HASH_MISMATCH"] += 1
            if len(excluded) < 5:
                excluded.append(dict(record_id=row["record_id"], reason="PUBLICATION_HASH_MISMATCH",
                    stored_sha256=row["payload_sha256"], resolved_sha256=digest(publication)))
            continue
        counts["verified_publications"] += 1
        instance = row["instance"]
        group = (instance["instance_id"], instance["policy_hash"], publication["policy_hash"], digest(publication.get("runtime_sources", {})))
        identities[group] = dict(instance_id=group[0], instance_policy_sha256=group[1],
            publication_policy_sha256=group[2], runtime_sources_sha256=group[3])
        published_at = datetime.fromisoformat(publication["actual_publication_at"])
        latest = max(latest, published_at) if latest else published_at
        candidates = publication.get("candidates", {})
        if len(candidates) > 10000:
            reasons["PUBLICATION_CANDIDATE_BOUND_EXCEEDED"] += 1
            continue
        for episode_id, payload in candidates.items():
            if payload.get("ticker") not in tickers:
                continue
            counts["configured_candidate_occurrences"] += 1
            if payload.get("model") != "acceptance" or payload.get("interval") != "1h":
                continue
            counts["hourly_acceptance_occurrences"] += 1
            episodes.add((instance["instance_id"], episode_id))
            versions[payload["policy_version"]] += 1
            by_ticker[payload["ticker"]] += 1
            try:
                candidate = read_candidate(payload)
                expiry = candidate.expires_at
                counts["published_before_expiry"] += published_at < expiry
                counts["imported_before_expiry"] += row["imported_at"] < expiry
                counts["unexpired_at_read_receipt"] += received_at < expiry
                if candidate.episode_id != episode_id:
                    reasons["EPISODE_MISMATCH"] += 1
                if candidate.policy_version != "range_breakout_acceptance_intraday_v2":
                    reasons["DETECTOR_VERSION_UNSUPPORTED"] += 1
                if received_at >= expiry:
                    reasons["EXPIRED_AT_READ_RECEIPT"] += 1
                if len(samples) < 5:
                    samples.append(dict(ticker=candidate.ticker, episode_id=episode_id, record_id=row["record_id"],
                        publication_sha256=row["payload_sha256"], publication_at=published_at.isoformat(),
                        imported_at=row["imported_at"].isoformat(), expires_at=expiry.isoformat(),
                        publication_slack_seconds=(expiry - published_at).total_seconds(),
                        projection_lag_seconds=(row["imported_at"] - published_at).total_seconds(),
                        import_slack_seconds=(expiry - row["imported_at"]).total_seconds()))
            except (ValueError, TypeError, KeyError):
                reasons["CANDIDATE_CONTRACT_INVALID"] += 1
    return dict(schema_version="stock_setup_source_inventory_v1", status="DIAGNOSTIC_NOT_APPROVED",
        received_at=received_at.isoformat(), publication_rows=len(rows), counts=dict(counts),
        detector_versions=dict(versions), occurrences_by_ticker=dict(by_ticker), reasons=dict(reasons),
        latest_publication_at=latest.isoformat() if latest else None,
        configured_tickers=list(tickers), missing_hourly_acceptance_tickers=sorted(set(tickers) - set(by_ticker)),
        source_identity_groups=list(identities.values()), samples=samples,
        distinct_hourly_acceptance_episodes=len(episodes), excluded_records=excluded,
        source_policy_approved=False, prospective_readiness="NOT_ESTABLISHED", execution_permission=False,
        limitations=["Occurrence counts are not unique setups or outcome samples.",
            "Imported timestamps are not proof of historical observer receipt.",
            "Source policy approval and full binding validation remain required."])


def compare_original_setup_publication(row, original, original_policy_sha256):
    common = dict(record_id=row["record_id"], stored_sha256=row["payload_sha256"],
        source_sha256=digest(original) if original is not None else None,
        shared_resolved_sha256=digest(row["publication"]) if row["publication"] is not None else None,
        writes_performed=False, historical_receipt_verified=False)
    if original is None:
        return dict(common, status="ORIGINAL_RECORD_UNAVAILABLE")
    if original_policy_sha256 != row["instance"]["policy_hash"]:
        return dict(common, status="ORIGINAL_INSTANCE_POLICY_MISMATCH")
    if common["source_sha256"] != common["stored_sha256"]:
        return dict(common, status="ORIGINAL_CHECKSUM_MISMATCH")
    if common["shared_resolved_sha256"] == common["stored_sha256"]:
        return dict(common, status="EXACT_MATCH", differences=[])
    pending = [((), original, row["publication"])]
    differences, visited, signed_zero_only = [], 0, True
    difference_count = 0
    while pending:
        path, left, right = pending.pop()
        visited += 1
        if visited > 250000:
            return dict(common, status="COMPARISON_BOUND_EXCEEDED")
        if type(left) is dict and type(right) is dict and left.keys() == right.keys():
            pending.extend((path + (key,), left[key], right[key]) for key in sorted(left))
        elif type(left) is list and type(right) is list and len(left) == len(right):
            pending.extend((path + (str(index),), first, second) for index, (first, second) in enumerate(zip(left, right)))
        elif json.dumps(left, sort_keys=True, allow_nan=False) != json.dumps(right, sort_keys=True, allow_nan=False):
            signed_zero = (type(left) is float and type(right) is float and left == right == 0.
                           and math.copysign(1., left) != math.copysign(1., right))
            signed_zero_only = signed_zero_only and signed_zero
            difference_count += 1
            if len(differences) < 8:
                differences.append(dict(path="/" + "/".join(path),
                    kind="SIGNED_ZERO" if signed_zero else "CONTENT_OR_REPRESENTATION",
                    original_type=type(left).__name__, shared_type=type(right).__name__,
                    original_scalar=left if not isinstance(left, (dict, list, str)) else None,
                    shared_scalar=right if not isinstance(right, (dict, list, str)) else None))
    return dict(common, status="JSONB_SIGNED_ZERO_NORMALIZATION" if signed_zero_only and difference_count else "SHARED_PAYLOAD_DIFFERS",
        difference_count=difference_count, differences=differences,
        reader_eligibility="UNCHANGED_FAIL_CLOSED")