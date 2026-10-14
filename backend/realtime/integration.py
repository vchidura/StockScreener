from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path

from .contracts import canonical_sha256


STABLE_ADAPTER_MANIFEST_VERSION = "realtime_stable_adapter_manifest_v1"


@dataclass(frozen=True, slots=True)
class StableSourceRequirement:
    relative_path: str
    required_symbols: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class StableAdapterSpec:
    adapter_id: str
    phase: str
    sources: tuple[StableSourceRequirement, ...]
    focused_tests: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class StableSourcePin:
    relative_path: str
    sha256: str
    required_symbols: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class StableAdapterRegistration:
    adapter_id: str
    phase: str
    sources: tuple[StableSourcePin, ...]
    focused_tests: tuple[str, ...]
    registration_sha256: str


@dataclass(frozen=True, slots=True)
class StableAdapterManifest:
    version: str
    registrations: tuple[StableAdapterRegistration, ...]
    execution_permission: bool = False

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


STABLE_ADAPTER_SPECS = (
    StableAdapterSpec(
        "stock-intraday-quality-v2",
        "P7",
        (
            StableSourceRequirement(
                "research/stock_idea_models.py",
                ("feature_frames",),
            ),
            StableSourceRequirement("research/strategy_v2.py", ("detect",)),
            StableSourceRequirement(
                "research/stock_idea_forward.py",
                ("decision_candidate", "alert_selection_cohort"),
            ),
        ),
        ("backend/tests/test_stock_idea_forward.py",),
    ),
    StableAdapterSpec(
        "stock-swing-v1",
        "P7",
        (
            StableSourceRequirement(
                "research/stock_idea_swing.py",
                ("bind_swing_candidate", "preconfirmation_path"),
            ),
        ),
        ("backend/tests/test_stock_idea_forward.py",),
    ),
    StableAdapterSpec(
        "options-o1-participation",
        "P7",
        (
            StableSourceRequirement(
                "options/dual_origin.py",
                ("detect_option_participation",),
            ),
        ),
        ("backend/tests/options/test_dual_origin.py",),
    ),
    StableAdapterSpec(
        "options-o2-surface",
        "P7",
        (
            StableSourceRequirement(
                "options/surface_detection.py",
                ("assess_surface_first",),
            ),
        ),
        ("backend/tests/options/test_smile_analytics.py",),
    ),
    StableAdapterSpec(
        "options-o3-credit",
        "P7",
        (
            StableSourceRequirement(
                "options/credit_detection.py",
                ("assess_o3_credit",),
            ),
        ),
        ("backend/tests/options/test_credit_detection.py",),
    ),
    StableAdapterSpec(
        "options-s1-s2-stock-first",
        "P7",
        (
            StableSourceRequirement(
                "options/dual_origin.py",
                ("StockFirstPolicy", "StockResumptionPolicy", "assess_stock_first"),
            ),
        ),
        ("backend/tests/options/test_dual_origin.py",),
    ),
    StableAdapterSpec(
        "options-alert-qualification",
        "P7",
        (
            StableSourceRequirement(
                "options/alert_qualification.py",
                ("alert_requirements", "qualify_option_alert"),
            ),
        ),
        ("backend/tests/options/test_options_api.py",),
    ),
    StableAdapterSpec(
        "stock-alert-portal-contract",
        "P8",
        (
            StableSourceRequirement(
                "equity/stock_alert_api.py",
                ("alert_view", "alert_eod_review"),
            ),
        ),
        (
            "backend/tests/test_stock_alerts.py",
            "frontend/tests/stockAlertNavigation.test.mjs",
        ),
    ),
    StableAdapterSpec(
        "options-alert-portal-contract",
        "P8",
        (
            StableSourceRequirement(
                "options/api.py",
                ("option_alert_evaluations", "option_detector_alerts"),
            ),
        ),
        (
            "backend/tests/options/test_options_api.py",
            "frontend/tests/optionsAlertPresentation.test.mjs",
        ),
    ),
    StableAdapterSpec(
        "screening-portal-contract",
        "P8",
        (
            StableSourceRequirement(
                "equity/screening_projection.py",
                ("build_latest",),
            ),
            StableSourceRequirement(
                "equity/portal_snapshots.py",
                ("publish",),
            ),
        ),
        (
            "backend/tests/test_screening.py",
            "backend/tests/test_equity_portal_snapshot_worker.py",
            "backend/tests/test_equity_portal_snapshot_reads.py",
        ),
    ),
)


def _has_symbol(source: str, symbol: str) -> bool:
    return re.search(
        rf"^(?:async\s+def|def|class)\s+{re.escape(symbol)}\b",
        source,
        re.MULTILINE,
    ) is not None


def build_stable_adapter_manifest(
    backend_dir: str | Path,
    *,
    repository_root: str | Path | None = None,
) -> StableAdapterManifest:
    backend = Path(backend_dir)
    root = Path(repository_root) if repository_root is not None else backend.parent
    registrations: list[StableAdapterRegistration] = []
    for spec in STABLE_ADAPTER_SPECS:
        pins: list[StableSourcePin] = []
        for requirement in spec.sources:
            path = backend / requirement.relative_path
            if not path.is_file():
                raise ValueError(
                    f"stable adapter source is missing: {requirement.relative_path}"
                )
            source = path.read_text(encoding="utf-8")
            missing = tuple(
                symbol
                for symbol in requirement.required_symbols
                if not _has_symbol(source, symbol)
            )
            if missing:
                raise ValueError(
                    f"stable adapter source {requirement.relative_path} is missing "
                    f"symbols: {', '.join(missing)}"
                )
            pins.append(
                StableSourcePin(
                    relative_path=requirement.relative_path,
                    sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                    required_symbols=requirement.required_symbols,
                )
            )
        missing_tests = tuple(
            path for path in spec.focused_tests if not (root / path).is_file()
        )
        if missing_tests:
            raise ValueError(
                "stable adapter focused tests are missing: "
                + ", ".join(missing_tests)
            )
        identity = {
            "adapter_id": spec.adapter_id,
            "phase": spec.phase,
            "sources": tuple(pins),
            "focused_tests": spec.focused_tests,
        }
        registrations.append(
            StableAdapterRegistration(
                adapter_id=spec.adapter_id,
                phase=spec.phase,
                sources=tuple(pins),
                focused_tests=spec.focused_tests,
                registration_sha256=canonical_sha256(identity),
            )
        )
    return StableAdapterManifest(
        version=STABLE_ADAPTER_MANIFEST_VERSION,
        registrations=tuple(registrations),
    )