#!/usr/bin/env python3
"""Report a sleeping role healthy, otherwise run its real health probe.

Standard library only: this runs on every health interval, and a sleeping role
should cost almost nothing while it waits for the next session.
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path


SLEEP_STATE_PATH = Path("/tmp/realtime-v2-role-sleep.json")
# The wrapper refreshes this every minute, so an old file means it has died.
MAXIMUM_STATE_AGE_SECONDS = 300


def role_is_sleeping(path: Path = SLEEP_STATE_PATH, *, now=time.time) -> bool:
    try:
        age = now() - path.stat().st_mtime
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    return state.get("sleeping") is True and age <= MAXIMUM_STATE_AGE_SECONDS


def main(argv=None) -> int:
    command = list(sys.argv[1:] if argv is None else argv)
    if command[:1] == ["--"]:
        command = command[1:]
    if not command:
        raise SystemExit("a health probe command is required after --")
    if role_is_sleeping():
        return 0
    os.execvp(command[0], command)
    raise RuntimeError("health probe replacement unexpectedly returned")


if __name__ == "__main__":
    raise SystemExit(main())
