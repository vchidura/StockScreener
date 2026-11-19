#!/usr/bin/env python3
r"""Grant more provider connection attempts to the CURRENT Options acquisition session (G102 R100c, Nov 19 2026).

Why: the acquisition stops connecting at the session's sealed ceiling (12). Attempt N streams under the stream session
id uuid5(binding stream session, "connection-attempt:N"), and the journal refuses a (stream session id, message number)
that already holds a different payload. On Nov 18 the ceiling was spent by a local crash loop and the recovery LOWERED
`connection_attempts` by hand (12 -> 9 -> 7 -> 4): the re-used attempt numbers collided in the journal ("raw message
identity conflicts with committed payload") and only the never-used attempt 5 worked. This tool instead adds
`--add N` to the status field `operator_extra_connection_attempts`; the runtime then allows binding ceiling + extra
attempts (capped at 64; the Options ingress accepts derived ids up to 256), so numbering continues 13, 14, ... and no
session id repeats. The extension belongs to the status file's journal generation only: the next session starts fresh.

Usage (dry run by default; --apply writes):
    python -B scripts/extend_options_acquisition_attempts.py --status-file PATH --add N [--apply]
        [--journal-root /var/lib/realtime-v2-advanced] [--binding-ceiling 12]

Refuses (exit 2, one REFUSED json document, nothing written) when:
- the status file is missing, not the acquisition status schema, or its fields are malformed;
- it is not the current session's: state COMPLETE (the session ended), heartbeat not from today (UTC), or its journal
  generation is not the newest one under <journal-root>/options-acquisition/;
- acquisition still looks alive (status heartbeat younger than 10 s): its status writer would overwrite the file;
- N <= 0, or binding ceiling + extension would exceed 64.
It never changes `connection_attempts` (or any other field): it writes the one field atomically (temp file + replace,
same JSON format as the runtime), re-reads the file and checks that nothing else changed. Prints before/after.

OPERATOR RUNBOOK (live host; containers: stock-screener-realtime-v3-options-acquisition; volumes: reports
alpha-research-realtime-v2-advanced-reports -> /var/lib/realtime-v2-reports, journals
alpha-research-realtime-v2-advanced-journals -> /var/lib/realtime-v2-advanced):
 1. Stop acquisition (it rewrites the status file every second while it runs):
      docker stop stock-screener-realtime-v3-options-acquisition        (stop grace 60 s)
 2. Back up the status file and dry-run (IMAGE = the acquisition's image id, e.g. from docker inspect):
      docker run --rm --network none \
        -v alpha-research-realtime-v2-advanced-reports:/var/lib/realtime-v2-reports \
        -v alpha-research-realtime-v2-advanced-journals:/var/lib/realtime-v2-advanced:ro \
        -w /backend --entrypoint python IMAGE -B scripts/extend_options_acquisition_attempts.py \
        --status-file /var/lib/realtime-v2-reports/options-acquisition-status.json --add 4
    Check before/after: connection_attempts unchanged, effective ceiling +4, next attempts 13..16.
 3. Same command with --apply. Keep the printed APPLIED document.
 4. Start acquisition: docker start stock-screener-realtime-v3-options-acquisition
 5. Check the logs: one `ACQUISITION_ATTEMPT_EXTENSION {...}` line (effective ceiling, next attempt), then state
    CONNECTING -> CAPTURING with connection_attempts 13 or more. With REALTIME_V3_ACQUISITION_LOCAL_BACKOFF=ON and a
    previous local failure, one `ACQUISITION_LOCAL_FAILURE_BACKOFF {...}` wait (<= 300 s) comes first; stop + start again
    skips it (the wait clears its pending flag when it starts).
 NEVER edit `connection_attempts` by hand, and never lower it: a re-used attempt number re-uses its stream session id and
 collides in the journal. Fix the local cause first (new image), or the extra attempts burn like the first 12.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_options_acquisition_runtime import (  # noqa: E402
    ACQUISITION_STATUS_SCHEMA,
    MAXIMUM_EFFECTIVE_CONNECTION_ATTEMPTS,
    OPERATOR_EXTRA_CONNECTION_ATTEMPTS,
)


UTC = timezone.utc
DEFAULT_JOURNAL_ROOT = Path("/var/lib/realtime-v2-advanced")
DEFAULT_BINDING_CEILING = 12  # advanced_options_automatic_preparation connection_attempt_ceiling (sealed per session)
RUNNING_HEARTBEAT_SECONDS = 10.0


class Refused(Exception):
    pass


def _encode(value: object) -> bytes:
    # Same format as advanced_options_acquisition_runtime._write_json.
    return (
        json.dumps(value, indent=2, sort_keys=True, default=str) + "\n"
    ).encode("utf-8")


def _write_atomically(path: Path, value: object) -> None:
    # Own temp name: the runtime's status writer uses "<file>.tmp".
    temporary = path.with_name(path.name + ".extend.tmp")
    with temporary.open("wb") as stream:
        stream.write(_encode(value))
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def _heartbeat(status: dict[str, object]) -> datetime:
    raw = status.get("runtime_heartbeat_at_utc") or status.get("observed_at_utc")
    try:
        value = datetime.fromisoformat(str(raw))
    except (TypeError, ValueError) as error:
        raise Refused("status heartbeat is missing or unreadable") from error
    if value.tzinfo is None:
        raise Refused("status heartbeat has no time zone")
    return value.astimezone(UTC)


def _newest_generation(journal_root: Path) -> tuple[str | None, list[str]]:
    base = journal_root / "options-acquisition"
    if not base.is_dir():
        raise Refused(f"journal folder {base} is missing (mount the journals volume)")
    newest, newest_mtime, names = None, None, []
    for folder in sorted(base.iterdir()):
        if not folder.is_dir():
            continue
        names.append(folder.name)
        mtimes = [folder.stat().st_mtime] + [
            item.stat().st_mtime for item in folder.iterdir() if item.is_file()
        ]
        mtime = max(mtimes)
        if newest_mtime is None or mtime > newest_mtime:
            newest, newest_mtime = folder.name, mtime
    return newest, names


def plan_extension(
    status: object,
    *,
    add: int,
    binding_ceiling: int,
    journal_root: Path,
    now: datetime,
) -> dict[str, object]:
    """The checked before/after of an extension, or Refused."""
    if not isinstance(status, dict) or status.get("schema") != ACQUISITION_STATUS_SCHEMA:
        raise Refused("not an Options acquisition status file")
    try:
        generation = str(UUID(str(status.get("journal_generation"))))
        UUID(str(status.get("owner_id")))
    except ValueError as error:
        raise Refused("status journal_generation/owner_id is malformed") from error
    binding_sha256 = status.get("binding_sha256")
    if not isinstance(binding_sha256, str) or len(binding_sha256) != 64:
        raise Refused("status binding_sha256 is malformed")
    attempts = status.get("connection_attempts")
    if type(attempts) is not int or attempts < 0:
        raise Refused("status connection_attempts is malformed")
    existing = status.get(OPERATOR_EXTRA_CONNECTION_ATTEMPTS, 0)
    if type(existing) is not int or existing < 0:
        raise Refused(f"status {OPERATOR_EXTRA_CONNECTION_ATTEMPTS} is malformed")
    if type(binding_ceiling) is not int or not 0 < binding_ceiling <= MAXIMUM_EFFECTIVE_CONNECTION_ATTEMPTS:
        raise Refused("binding ceiling is invalid")
    # Current session only.
    if status.get("state") == "COMPLETE":
        raise Refused("status is COMPLETE: that session's acquisition authority has ended")
    heartbeat = _heartbeat(status)
    now = now.astimezone(UTC)
    if heartbeat.date() != now.date() or heartbeat > now:
        raise Refused(
            f"status heartbeat {heartbeat.isoformat()} is not from today's session ({now.date()})"
        )
    age = (now - heartbeat).total_seconds()
    if age < RUNNING_HEARTBEAT_SECONDS:
        raise Refused(
            f"acquisition looks alive (status heartbeat {age:.1f} s old): stop it first, it rewrites this file"
        )
    newest, generations = _newest_generation(journal_root)
    if generation not in generations:
        raise Refused(f"no journal for generation {generation} under {journal_root}/options-acquisition")
    if newest != generation:
        raise Refused(
            f"generation {generation} is not the newest journal ({newest}): not the current session"
        )
    # The extension itself.
    if type(add) is not int or add <= 0:
        raise Refused("--add must be a whole number > 0 (an extension never lowers anything)")
    before_ceiling = min(binding_ceiling + existing, MAXIMUM_EFFECTIVE_CONNECTION_ATTEMPTS) if existing else binding_ceiling
    after_extra = existing + add
    after_ceiling = binding_ceiling + after_extra
    if after_ceiling > MAXIMUM_EFFECTIVE_CONNECTION_ATTEMPTS:
        raise Refused(
            f"binding ceiling {binding_ceiling} + extension {after_extra} = {after_ceiling} exceeds the cap "
            f"{MAXIMUM_EFFECTIVE_CONNECTION_ATTEMPTS}"
        )
    first_new = max(attempts, before_ceiling) + 1
    return {
        "journal_generation": generation,
        "binding_sha256": binding_sha256,
        "state": status.get("state"),
        "heartbeat_at_utc": heartbeat.isoformat(),
        "heartbeat_age_seconds": round(age, 1),
        "before": {
            "connection_attempts": attempts,
            OPERATOR_EXTRA_CONNECTION_ATTEMPTS: existing,
            "effective_connection_attempt_ceiling": before_ceiling,
            "remaining_attempts": max(0, before_ceiling - attempts),
        },
        "after": {
            "connection_attempts": attempts,
            OPERATOR_EXTRA_CONNECTION_ATTEMPTS: after_extra,
            "effective_connection_attempt_ceiling": after_ceiling,
            "remaining_attempts": max(0, after_ceiling - attempts),
        },
        "new_attempt_numbers": list(range(first_new, after_ceiling + 1)),
        "binding_connection_attempt_ceiling": binding_ceiling,
        "cap": MAXIMUM_EFFECTIVE_CONNECTION_ATTEMPTS,
    }


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Grant more connection attempts to the current Options acquisition session (R100c)."
    )
    parser.add_argument("--status-file", type=Path, required=True)
    parser.add_argument("--add", type=int, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--journal-root", type=Path, default=DEFAULT_JOURNAL_ROOT)
    parser.add_argument("--binding-ceiling", type=int, default=DEFAULT_BINDING_CEILING)
    return parser.parse_args(argv)


def main(argv=None, *, now: datetime | None = None) -> int:
    args = parse_args(argv)
    current = datetime.now(UTC) if now is None else now
    path = args.status_file
    document: dict[str, object] = {"status_file": str(path), "apply": bool(args.apply)}
    try:
        if not path.is_file():
            raise Refused("status file is missing")
        raw = path.read_bytes()
        try:
            status = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise Refused("status file is not JSON") from error
        plan = plan_extension(
            status,
            add=args.add,
            binding_ceiling=args.binding_ceiling,
            journal_root=args.journal_root,
            now=current,
        )
        document.update(plan)
        if not args.apply:
            document["result"] = "DRY_RUN"
            print(json.dumps(document, indent=2, sort_keys=True))
            return 0
        if path.read_bytes() != raw:
            raise Refused("status file changed while planning: is acquisition running?")
        updated = dict(status)
        updated[OPERATOR_EXTRA_CONNECTION_ATTEMPTS] = plan["after"][OPERATOR_EXTRA_CONNECTION_ATTEMPTS]
        _write_atomically(path, updated)
        written = json.loads(path.read_text(encoding="utf-8"))
        if (
            written.get(OPERATOR_EXTRA_CONNECTION_ATTEMPTS)
            != plan["after"][OPERATOR_EXTRA_CONNECTION_ATTEMPTS]
            or {key: value for key, value in written.items() if key != OPERATOR_EXTRA_CONNECTION_ATTEMPTS}
            != {key: value for key, value in status.items() if key != OPERATOR_EXTRA_CONNECTION_ATTEMPTS}
            or written.get("connection_attempts") != status.get("connection_attempts")
        ):
            document["result"] = "WRITE_CHECK_FAILED"
            print(json.dumps(document, indent=2, sort_keys=True))
            return 1
        document["result"] = "APPLIED"
        print(json.dumps(document, indent=2, sort_keys=True))
        return 0
    except Refused as error:
        document["result"] = "REFUSED"
        document["reason"] = str(error)
        print(json.dumps(document, indent=2, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
