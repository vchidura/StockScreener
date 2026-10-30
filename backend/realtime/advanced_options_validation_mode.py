from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum

from .advanced_options_service import (
    OPTIONS_PROJECTION_ONLY_STAGE_ORDER,
    OPTIONS_V3_STAGE_ORDER,
)


VALIDATION_MODE_ENV = "REALTIME_V3_VALIDATION_MODE"
PROVIDER_FREE_SENTINEL_ENV = (
    "REALTIME_V3_PROVIDER_FREE_QUALIFICATION"
)


class AdvancedOptionsValidationMode(str, Enum):
    PRODUCTION_STRICT = "PRODUCTION_STRICT"
    DEVELOPMENT_DIAGNOSTIC = "DEVELOPMENT_DIAGNOSTIC"


@dataclass(frozen=True, slots=True)
class AdvancedOptionsProjectionPolicy:
    validation_mode: AdvancedOptionsValidationMode
    equity_stage_order: tuple[str, ...]
    equity_terminal_receipt: str
    options_stage_order: tuple[str, ...]
    options_terminal_receipt: str
    release_allowed: bool
    pointer_allowed: bool
    read_pin_allowed: bool
    normal_portal_serving_allowed: bool


def advanced_options_validation_mode(
    environment: Mapping[str, str] | None,
) -> AdvancedOptionsValidationMode:
    values = {} if environment is None else environment
    raw = str(values.get(VALIDATION_MODE_ENV) or "").strip().upper()
    if not raw:
        mode = AdvancedOptionsValidationMode.PRODUCTION_STRICT
    else:
        try:
            mode = AdvancedOptionsValidationMode(raw)
        except ValueError as error:
            raise ValueError(
                f"{VALIDATION_MODE_ENV} is invalid"
            ) from error

    sentinel = str(
        values.get(PROVIDER_FREE_SENTINEL_ENV) or ""
    ).strip()
    if sentinel not in {"", "ENABLED"}:
        raise ValueError(
            f"{PROVIDER_FREE_SENTINEL_ENV} is invalid"
        )
    if (
        mode is AdvancedOptionsValidationMode.DEVELOPMENT_DIAGNOSTIC
    ) != (sentinel == "ENABLED"):
        raise ValueError(
            "development diagnostic mode requires the provider-free "
            "qualification sentinel"
        )
    return mode


def advanced_options_projection_policy(
    environment: Mapping[str, str] | None,
) -> AdvancedOptionsProjectionPolicy:
    mode = advanced_options_validation_mode(environment)
    diagnostic = (
        mode is AdvancedOptionsValidationMode.DEVELOPMENT_DIAGNOSTIC
    )
    return AdvancedOptionsProjectionPolicy(
        validation_mode=mode,
        equity_stage_order=(
            "EQUITY_INPUT_GATE",
            "EQUITY_MATERIALIZE",
            "EQUITY_PROJECTION",
        ),
        equity_terminal_receipt="PORTAL_READY",
        options_stage_order=(
            OPTIONS_PROJECTION_ONLY_STAGE_ORDER
            if diagnostic
            else OPTIONS_V3_STAGE_ORDER
        ),
        options_terminal_receipt="OPTIONS_PORTAL_READY",
        release_allowed=not diagnostic,
        pointer_allowed=not diagnostic,
        read_pin_allowed=not diagnostic,
        normal_portal_serving_allowed=not diagnostic,
    )
