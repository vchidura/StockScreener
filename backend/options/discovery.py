from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


CONTRACT_FILTERS = {
    "calendar_dte": {"label": "DTE", "unit": "days", "minimum": 0, "maximum": 365, "integer": True},
    "model_mark": {"label": "Option mark", "unit": "USD/share", "minimum": 0},
    "spot": {"label": "Underlying price", "unit": "USD", "minimum": 0},
    "absolute_delta": {"label": "Absolute delta", "unit": "ratio", "minimum": 0, "maximum": 1},
    "local_iv": {"label": "Implied volatility", "unit": "fraction", "minimum": 0},
    "local_gamma": {"label": "Gamma", "unit": "delta/USD", "minimum": 0},
    "local_theta_per_day": {"label": "Theta", "unit": "USD/share/day"},
    "local_vega_per_vol_point": {"label": "Vega", "unit": "USD/share/IV point", "minimum": 0},
    "otm_fraction": {"label": "OTM distance", "unit": "fraction"},
    "day_volume": {"label": "Volume", "unit": "contracts", "minimum": 0, "integer": True},
    "open_interest": {"label": "Open interest", "unit": "contracts", "minimum": 0, "integer": True},
    "volume_open_interest_ratio": {"label": "Volume / OI", "unit": "ratio", "minimum": 0},
}

ContractField = Literal[
    "calendar_dte", "model_mark", "spot", "absolute_delta", "local_iv", "local_gamma",
    "local_theta_per_day", "local_vega_per_vol_point", "otm_fraction", "day_volume",
    "open_interest", "volume_open_interest_ratio",
]


class ContractRange(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    field: ContractField
    minimum: float | None = None
    maximum: float | None = None

    @model_validator(mode="after")
    def validate_bounds(self):
        if self.minimum is None and self.maximum is None:
            raise ValueError("a filter requires at least one bound")
        if self.minimum is not None and self.maximum is not None and self.minimum > self.maximum:
            raise ValueError("minimum exceeds maximum")
        specification = CONTRACT_FILTERS[self.field]
        for value in (self.minimum, self.maximum):
            if value is None:
                continue
            if value < specification.get("minimum", float("-inf")) or value > specification.get("maximum", float("inf")):
                raise ValueError("bound outside field limits")
            if specification.get("integer") and not value.is_integer():
                raise ValueError("whole number required")
        return self


class EligibleChainQuery(BaseModel):
    model_config = ConfigDict(extra="forbid")
    session_date: date | None = None
    underlyer: str | None = Field(default=None, pattern=r"^[A-Z][A-Z0-9.]{0,14}$")
    contract_type: Literal["CALL", "PUT"] | None = None
    filters: tuple[ContractRange, ...] = Field(default=(), max_length=len(CONTRACT_FILTERS))
    sort: ContractField = "day_volume"
    descending: bool = True
    limit: int = Field(default=100, ge=1, le=500)
    offset: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def unique_filters(self):
        if len({item.field for item in self.filters}) != len(self.filters):
            raise ValueError("duplicate filter field")
        return self


def contract_filter_sql(request: EligibleChainQuery) -> tuple[str, tuple[object, ...]]:
    clauses = []
    parameters: list[object] = []
    if request.contract_type:
        clauses.append("contract_type = %s")
        parameters.append(request.contract_type)
    for condition in request.filters:
        for operator, value in ((">=", condition.minimum), ("<=", condition.maximum)):
            if value is not None:
                clauses.append(f"{condition.field} {operator} %s")
                parameters.append(value)
    return " AND ".join(clauses) or "TRUE", tuple(parameters)