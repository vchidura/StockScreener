import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from scripts import refresh_equity_portal_snapshots as worker


def test_one_shot_skips_refresh_when_current_snapshot_is_fresh(monkeypatch):
    monkeypatch.setattr(worker, "current", lambda _: {"is_fresh": True})
    with patch.object(worker, "refresh_once") as refresh:
        with patch.object(sys, "argv", ["worker"]):
            assert worker.main() == 0
    refresh.assert_not_called()


def test_one_shot_refreshes_missing_snapshot(monkeypatch):
    monkeypatch.setattr(worker, "current", lambda _: None)
    monkeypatch.setattr(worker, "refresh_once", lambda: {"status": "published"})
    with patch.object(sys, "argv", ["worker"]):
        assert worker.main() == 0


def test_continuous_worker_retries_source_generation_change(monkeypatch):
    monkeypatch.setattr(worker, "current", lambda _: None)
    refresh = MagicMock(side_effect=[
        worker.SourceGenerationChanged("source changed"),
        KeyboardInterrupt(),
    ])
    monkeypatch.setattr(worker, "refresh_once", refresh)
    monkeypatch.setattr(worker.time, "sleep", lambda _: None)

    with patch.object(sys, "argv", ["worker", "--continuous"]):
        with pytest.raises(KeyboardInterrupt):
            worker.main()

    assert refresh.call_count == 2


def test_one_shot_surfaces_source_generation_change(monkeypatch):
    monkeypatch.setattr(worker, "current", lambda _: None)
    monkeypatch.setattr(
        worker,
        "refresh_once",
        lambda: (_ for _ in ()).throw(worker.SourceGenerationChanged("source changed")),
    )

    with patch.object(sys, "argv", ["worker"]):
        with pytest.raises(worker.SourceGenerationChanged):
            worker.main()