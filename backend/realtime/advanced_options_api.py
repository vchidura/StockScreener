"""Disabled stored-read capability contract for Advanced Options V2."""
from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from .advanced_options_projection import OPTIONS_PROJECTION_TYPES
from .advanced_options_projection import (
    read_advanced_options_projection_cursor,
)
from .advanced_options_read_pin import (
    load_enabled_advanced_options_release_cursor,
)


def advanced_options_v2_disabled_payload(
    snapshot_type: str | None = None,
) -> dict[str, object]:
    if (
        snapshot_type is not None
        and snapshot_type not in OPTIONS_PROJECTION_TYPES
    ):
        raise ValueError("Advanced Options V2 projection type is invalid")
    return {
        "version": "advanced-options-v2-read-seam-v1",
        "enabled": False,
        "stage_catalog": "stages_v4",
        "snapshot_type": snapshot_type,
        "provider_fetch_performed": False,
        "detector_work_performed": False,
        "publication_performed": False,
        "pointer_advanced": False,
    }


def advanced_options_v2_status_payload(
    *,
    enabled: bool,
) -> dict[str, object]:
    return {
        **advanced_options_v2_disabled_payload(),
        "enabled": enabled,
        "reason": None if enabled else "ADVANCED_OPTIONS_V2_DISABLED",
    }


@dataclass(frozen=True, slots=True)
class AdvancedOptionsStoredProjectionRepository:
    cursor_factory: Callable[[], AbstractContextManager]
    require_read_pin: bool = False

    def __post_init__(self) -> None:
        if not callable(self.cursor_factory):
            raise TypeError(
                "Advanced Options projection cursor factory is invalid"
            )
        if type(self.require_read_pin) is not bool:
            raise TypeError("Advanced Options read-pin policy is invalid")

    def read(
        self,
        *,
        cohort_id: UUID,
        snapshot_type: str,
        available_by: datetime,
    ) -> dict[str, object]:
        with self.cursor_factory() as cursor:
            release_pin = (
                load_enabled_advanced_options_release_cursor(
                    cursor,
                    cohort_id=cohort_id,
                )
                if self.require_read_pin
                else None
            )
            if self.require_read_pin and release_pin is None:
                return {
                    "available": False,
                    "reason": "ADVANCED_OPTIONS_READ_PIN_UNAVAILABLE",
                    "cohort_id": str(cohort_id),
                    "snapshot_type": snapshot_type,
                    "advanced_options_v2_enabled": False,
                    "provider_fetch_performed": False,
                    "detector_work_performed": False,
                    "publication_performed": False,
                    "data": None,
                }
            release_id = None if release_pin is None else release_pin[0]
            release_cutoff = None if release_pin is None else release_pin[1]
            result = read_advanced_options_projection_cursor(
                cursor,
                cohort_id=cohort_id,
                snapshot_type=snapshot_type,
                available_by=available_by,
                release_id=release_id,
                release_cutoff=release_cutoff,
            )
        return {
            **result,
            "cohort_id": str(cohort_id),
            "snapshot_type": snapshot_type,
            "advanced_options_v2_enabled": True,
        }
