from pathlib import Path
from types import SimpleNamespace

from scripts import run_v2_test_sets


def test_fast_set_places_the_io_guard_after_the_pytest_module(monkeypatch):
    seen = {}
    monkeypatch.setattr(run_v2_test_sets, "collect", lambda *args, **kwargs: [Path("focused.py")])

    def run(command, **options):
        seen["command"] = command
        seen["options"] = options
        return SimpleNamespace(returncode=0, stdout="passed\n", stderr="")

    monkeypatch.setattr(run_v2_test_sets.subprocess, "run", run)

    assert run_v2_test_sets.main(["fast"]) == 0
    assert seen["command"][1:7] == [
        "-B",
        "-m",
        "pytest",
        "-p",
        "backend.tests.realtime.offline_io_guard",
        "-q",
    ]
    assert seen["command"][-1] == "focused.py"
    assert seen["options"]["cwd"] == run_v2_test_sets.BACKEND_DIR.parent
