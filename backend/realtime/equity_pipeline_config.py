from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

class EquityPipeline(str, Enum):
    MASSIVE_DEVELOPER = "MASSIVE_DEVELOPER"
    MASSIVE_ADVANCED = "MASSIVE_ADVANCED"


class OptionsPipeline(str, Enum):
    DISABLED = "DISABLED"
    MASSIVE_DEVELOPER = "MASSIVE_DEVELOPER"
    MASSIVE_ADVANCED = "MASSIVE_ADVANCED"


class AdvancedScaleControl(str, Enum):
    MANUAL_GATED = "MANUAL_GATED"
    AUTOMATIC_DOWN_MANUAL_UP = "AUTOMATIC_DOWN_MANUAL_UP"
    AUTOMATIC_BOTH = "AUTOMATIC_BOTH"


class RealtimeV2Process(str, Enum):
    FULL = "FULL"
    CONTROLLER = "CONTROLLER"
    INGRESS = "INGRESS"
    BRIDGE = "BRIDGE"
    SCHEDULER = "SCHEDULER"
    APP_WORKER = "APP_WORKER"
    API = "API"


_PLACEHOLDERS = frozenset(
    {
        "...",
        "replace_with_generated_value",
        "replace_with_provider_value",
        "your_polygon_api_key_here",
    }
)


def _required(settings: Mapping[str, str | None], name: str) -> str:
    value = str(settings.get(name) or "").strip()
    if not value or value.lower() in _PLACEHOLDERS:
        raise ValueError(f"{name} is missing or still contains a placeholder")
    return value


def _enum(settings: Mapping[str, str | None], name: str, enum_type):
    value = _required(settings, name).upper()
    try:
        return enum_type(value)
    except ValueError as error:
        choices = ", ".join(row.value for row in enum_type)
        raise ValueError(f"{name} must be one of: {choices}") from error


@dataclass(frozen=True, slots=True)
class RealtimeV2PipelineConfig:
    equity_pipeline: EquityPipeline
    options_pipeline: OptionsPipeline
    process: RealtimeV2Process
    app_dsn: str | None = field(repr=False)
    api_dsn: str | None = field(repr=False)
    massive_api_key: str | None = field(repr=False)
    advanced_journal_dir: Path | None = None
    advanced_ingress_policy_path: Path | None = None
    advanced_ingress_policy_version: str | None = None

    def __post_init__(self) -> None:
        if (
            self.app_dsn is not None
            and self.api_dsn is not None
            and self.app_dsn == self.api_dsn
        ):
            raise ValueError("V2 app and API DSNs must use separate principals")
        if (
            self.process
            in {
                RealtimeV2Process.FULL,
                RealtimeV2Process.CONTROLLER,
                RealtimeV2Process.INGRESS,
                RealtimeV2Process.BRIDGE,
                RealtimeV2Process.SCHEDULER,
                RealtimeV2Process.APP_WORKER,
            }
            and self.app_dsn is None
        ):
            raise ValueError("REALTIME_V2_APP_DSN is required for this process")
        if (
            self.process in {RealtimeV2Process.FULL, RealtimeV2Process.API}
            and self.api_dsn is None
        ):
            raise ValueError("REALTIME_V2_API_DSN is required for this process")
        if (
            self.process
            in {
                RealtimeV2Process.FULL,
                RealtimeV2Process.CONTROLLER,
                RealtimeV2Process.INGRESS,
            }
            and self.massive_api_key is None
        ):
            raise ValueError("POLYGON_API_KEY is required for this process")
        if self.equity_pipeline is EquityPipeline.MASSIVE_ADVANCED:
            if self.process in {
                RealtimeV2Process.FULL,
                RealtimeV2Process.CONTROLLER,
                RealtimeV2Process.INGRESS,
                RealtimeV2Process.BRIDGE,
            } and (
                self.advanced_journal_dir is None
                or not self.advanced_journal_dir.is_absolute()
            ):
                raise ValueError(
                    "Advanced equity requires an absolute local journal directory"
                )
            if self.process in {
                RealtimeV2Process.FULL,
                RealtimeV2Process.CONTROLLER,
                RealtimeV2Process.INGRESS,
                RealtimeV2Process.SCHEDULER,
            } and (
                self.advanced_ingress_policy_path is None
                or not self.advanced_ingress_policy_path.is_absolute()
                or not (self.advanced_ingress_policy_version or "").strip()
            ):
                raise ValueError(
                    "Advanced equity requires an absolute ingress policy "
                    "path and exact version"
                )
        elif (
            self.advanced_ingress_policy_path is not None
            or self.advanced_ingress_policy_version is not None
        ):
            raise ValueError(
                "Developer equity cannot activate Advanced runtime settings"
            )

    @property
    def redacted_identity(self) -> dict[str, object]:
        return {
            "equity_pipeline": self.equity_pipeline.value,
            "options_pipeline": self.options_pipeline.value,
            "process": self.process.value,
            "app_dsn_configured": self.app_dsn is not None,
            "api_dsn_configured": self.api_dsn is not None,
            "massive_api_key_configured": self.massive_api_key is not None,
            "advanced_journal_dir": (
                str(self.advanced_journal_dir)
                if self.advanced_journal_dir is not None
                else None
            ),
            "advanced_ingress_policy_path": (
                str(self.advanced_ingress_policy_path)
                if self.advanced_ingress_policy_path is not None
                else None
            ),
            "advanced_ingress_policy_version": (
                self.advanced_ingress_policy_version
            ),
        }

    @classmethod
    def from_mapping(
        cls,
        settings: Mapping[str, str | None],
        *,
        process: RealtimeV2Process = RealtimeV2Process.FULL,
    ) -> RealtimeV2PipelineConfig:
        equity = _enum(
            settings,
            "REALTIME_V2_EQUITY_PIPELINE",
            EquityPipeline,
        )
        options = _enum(
            settings,
            "REALTIME_V2_OPTIONS_PIPELINE",
            OptionsPipeline,
        )
        app_dsn = (
            _required(settings, "REALTIME_V2_APP_DSN")
            if process
            in {
                RealtimeV2Process.FULL,
                RealtimeV2Process.CONTROLLER,
                RealtimeV2Process.INGRESS,
                RealtimeV2Process.BRIDGE,
                RealtimeV2Process.SCHEDULER,
                RealtimeV2Process.APP_WORKER,
            }
            else None
        )
        api_dsn = (
            _required(settings, "REALTIME_V2_API_DSN")
            if process in {RealtimeV2Process.FULL, RealtimeV2Process.API}
            else None
        )
        api_key = (
            _required(settings, "POLYGON_API_KEY")
            if process
            in {
                RealtimeV2Process.FULL,
                RealtimeV2Process.CONTROLLER,
                RealtimeV2Process.INGRESS,
            }
            else None
        )
        if equity is EquityPipeline.MASSIVE_ADVANCED:
            journal_dir = (
                Path(
                    _required(
                        settings,
                        "REALTIME_V2_ADVANCED_JOURNAL_DIR",
                    )
                )
                if process
                in {
                    RealtimeV2Process.FULL,
                    RealtimeV2Process.CONTROLLER,
                    RealtimeV2Process.INGRESS,
                    RealtimeV2Process.BRIDGE,
                }
                else None
            )
            ingress_policy_path = (
                Path(
                    _required(
                        settings,
                        "REALTIME_V2_ADVANCED_INGRESS_POLICY_PATH",
                    )
                )
                if process
                in {
                    RealtimeV2Process.FULL,
                    RealtimeV2Process.CONTROLLER,
                    RealtimeV2Process.INGRESS,
                    RealtimeV2Process.SCHEDULER,
                }
                else None
            )
            ingress_policy_version = (
                _required(
                    settings,
                    "REALTIME_V2_ADVANCED_INGRESS_POLICY_VERSION",
                )
                if ingress_policy_path is not None
                else None
            )
        else:
            journal_dir = None
            ingress_policy_path = None
            ingress_policy_version = None
        return cls(
            equity_pipeline=equity,
            options_pipeline=options,
            process=process,
            app_dsn=app_dsn,
            api_dsn=api_dsn,
            massive_api_key=api_key,
            advanced_journal_dir=journal_dir,
            advanced_ingress_policy_path=ingress_policy_path,
            advanced_ingress_policy_version=ingress_policy_version,
        )
