"""Small helpers shared by the realtime operator scripts that survive the V1 retirement."""
from __future__ import annotations

import json
import os
import time
from pathlib import Path

MINIMUM_REQUEST_INTERVAL_SECONDS = 12.0


def write_report(path: Path, report: dict[str, object]) -> None:
    """Write a JSON report atomically (temporary file, fsync, replace)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (json.dumps(report, indent=2, sort_keys=True) + "\n").encode("utf-8")
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("wb") as output:
        output.write(payload)
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary, path)


class PacedPolygonTransport:
    """Spaces provider requests by a minimum interval around any transport with a `get(url, params)`."""

    def __init__(
        self,
        delegate,
        *,
        minimum_interval_seconds: float = MINIMUM_REQUEST_INTERVAL_SECONDS,
        monotonic=time.monotonic,
        sleep=time.sleep,
    ) -> None:
        if minimum_interval_seconds <= 0:
            raise ValueError("Polygon request interval must be positive")
        self.delegate = delegate
        self.minimum_interval_seconds = minimum_interval_seconds
        self.monotonic = monotonic
        self.sleep = sleep
        self.last_completed_at = None

    def get(self, url, params):
        if self.last_completed_at is not None:
            remaining = (
                self.last_completed_at
                + self.minimum_interval_seconds
                - self.monotonic()
            )
            if remaining > 0:
                self.sleep(remaining)
        response = self.delegate.get(url, params)
        self.last_completed_at = self.monotonic()
        return response
