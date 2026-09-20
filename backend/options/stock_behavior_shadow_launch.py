"""Frozen launch contract for bounded or continuous stock-behavior shadow collection."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from equity.behavior import resolve_behavior_profile
from options.stock_behavior_gates import resolve_stock_behavior_gate_policy


class OptionStockBehaviorShadowLaunch(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: Literal[
        "option_stock_behavior_shadow_launch_v1",
        "option_stock_behavior_shadow_launch_v2",
    ]
    mode: Literal["BOUNDED", "CONTINUOUS_DEVELOPMENT"] = "BOUNDED"
    launch_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{7,63}$")
    starts_at: AwareDatetime
    ends_at: AwareDatetime | None = None
    usage_window_seconds: int = Field(default=86400, ge=3600, le=604800)
    underlyers: tuple[str, ...]
    maximum_assessments: int = Field(ge=1, le=10_000)
    maximum_payload_bytes: int = Field(ge=1_000_000, le=100_000_000)
    maximum_candidates_per_matrix: int = Field(ge=1, le=100)
    minimum_assessments_before_rate_stops: int = Field(ge=1, le=1_000)
    maximum_unavailable_fraction: float = Field(ge=0, le=1)
    maximum_p95_decision_lag_seconds: int = Field(ge=1, le=3_600)
    artifact_destination: str
    detector_policy_sha256: str
    behavior_definition_sha256: str
    behavior_policy_sha256: str
    assessment_only: Literal[True]
    execution_permission: Literal[False]

    @model_validator(mode="after")
    def validate_launch(self):
        if self.mode == "BOUNDED":
            if self.ends_at is None or not self.starts_at < self.ends_at:
                raise ValueError("bounded shadow launch requires ends_at after starts_at")
            if self.ends_at - self.starts_at > timedelta(days=7):
                raise ValueError("bounded shadow launch duration cannot exceed seven days")
        elif self.ends_at is not None:
            raise ValueError("continuous development launch cannot define ends_at")
        if self.mode == "CONTINUOUS_DEVELOPMENT" and self.schema_version != "option_stock_behavior_shadow_launch_v2":
            raise ValueError("continuous development requires launch schema v2")
        normalized = tuple(ticker.strip().upper() for ticker in self.underlyers)
        if normalized != self.underlyers or len(normalized) != len(set(normalized)):
            raise ValueError("shadow launch underlyers must be unique uppercase symbols")
        if self.minimum_assessments_before_rate_stops > self.maximum_assessments:
            raise ValueError("rate-stop minimum cannot exceed maximum assessments")
        resolve_stock_behavior_gate_policy("option_stock_behavior_gate_v1", self.detector_policy_sha256)
        resolve_behavior_profile("OPTIONS_SWING_V1", self.behavior_definition_sha256, self.behavior_policy_sha256)
        destination = Path(self.artifact_destination)
        if destination.is_absolute() or destination.suffix != ".json":
            raise ValueError("shadow launch artifact destination must be a relative JSON path")
        if not destination.parts or destination.parts[0] != "backups" or ".." in destination.parts:
            raise ValueError("shadow launch artifacts must remain under backend/backups")
        return self

    def accepts(self, market_time: datetime) -> bool:
        return self.starts_at <= market_time and (
            self.ends_at is None or market_time < self.ends_at
        )

    def usage_bounds(self, as_of: datetime) -> tuple[datetime, datetime]:
        upper = min(as_of, self.ends_at) if self.ends_at is not None else as_of
        if self.mode == "CONTINUOUS_DEVELOPMENT":
            return max(self.starts_at, upper - timedelta(seconds=self.usage_window_seconds)), upper
        return self.starts_at, upper

    @property
    def sha256(self) -> str:
        payload = json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":"),
            ensure_ascii=True,
        )
        return hashlib.sha256(payload.encode("ascii")).hexdigest()


def load_option_stock_behavior_shadow_launch(
    path: Path,
) -> OptionStockBehaviorShadowLaunch:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"unable to load option stock-behavior shadow launch from {path}") from exc
    return OptionStockBehaviorShadowLaunch.model_validate(payload)