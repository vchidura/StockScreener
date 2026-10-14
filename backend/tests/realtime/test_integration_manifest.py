import shutil
from pathlib import Path

import pytest

from realtime.integration import (
    STABLE_ADAPTER_SPECS,
    build_stable_adapter_manifest,
)


ROOT = Path(__file__).resolve().parents[3]
BACKEND = ROOT / "backend"


def test_stable_adapter_manifest_pins_every_reuse_surface_and_test():
    manifest = build_stable_adapter_manifest(BACKEND, repository_root=ROOT)

    assert manifest.execution_permission is False
    assert [row.adapter_id for row in manifest.registrations] == [
        row.adapter_id for row in STABLE_ADAPTER_SPECS
    ]
    assert len(manifest.sha256) == 64
    assert all(len(row.registration_sha256) == 64 for row in manifest.registrations)
    assert all(row.phase in {"P7", "P8"} for row in manifest.registrations)
    assert all(row.sources and row.focused_tests for row in manifest.registrations)


def _copy_manifest_inputs(tmp_path):
    backend = tmp_path / "backend"
    for spec in STABLE_ADAPTER_SPECS:
        for requirement in spec.sources:
            source = BACKEND / requirement.relative_path
            target = backend / requirement.relative_path
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
        for relative_test in spec.focused_tests:
            source = ROOT / relative_test
            target = tmp_path / relative_test
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
    return backend


def test_stable_adapter_manifest_detects_source_drift(tmp_path):
    backend = _copy_manifest_inputs(tmp_path)
    before = build_stable_adapter_manifest(backend, repository_root=tmp_path)
    path = backend / "research/strategy_v2.py"
    path.write_text(path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    after = build_stable_adapter_manifest(backend, repository_root=tmp_path)

    assert before.sha256 != after.sha256


def test_stable_adapter_manifest_fails_when_required_symbol_disappears(tmp_path):
    backend = _copy_manifest_inputs(tmp_path)
    path = backend / "research/strategy_v2.py"
    path.write_text(
        path.read_text(encoding="utf-8").replace("def detect(", "def removed("),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="missing symbols: detect"):
        build_stable_adapter_manifest(backend, repository_root=tmp_path)