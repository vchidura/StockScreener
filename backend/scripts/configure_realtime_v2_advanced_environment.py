#!/usr/bin/env python3
"""Write only non-secret Advanced V2 selectors to an existing env file."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from dotenv import set_key


BACKEND_DIR = Path(__file__).resolve().parent.parent
SAFE_SETTINGS = {
    "REALTIME_V2_EQUITY_PIPELINE": "MASSIVE_ADVANCED",
    "REALTIME_V2_OPTIONS_PIPELINE": "DISABLED",
    "REALTIME_V2_ADVANCED_STOCK_CHANNELS": "AM",
    "REALTIME_V2_ADVANCED_CONNECTION_CEILING": "1",
    "REALTIME_V2_ADVANCED_SCALE_CONTROL": "MANUAL_GATED",
    "REALTIME_V2_ADVANCED_UNIVERSE_LADDER": "1,9,25,50,100,200,400",
    "REALTIME_V2_ADVANCED_UNIVERSE_TARGET": "9",
    "REALTIME_V2_ADVANCED_STABLE_SESSIONS_BEFORE_UP": "5",
}
SECRET_NAMES = {
    "POLYGON_API_KEY",
    "REALTIME_V2_APP_DSN",
    "REALTIME_V2_API_DSN",
}
PLACEHOLDERS = {
    "...",
    "replace_with_generated_value",
    "replace_with_provider_value",
    "your_polygon_api_key_here",
}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env-file",
        type=Path,
        default=BACKEND_DIR / ".env",
    )
    parser.add_argument("--journal-directory", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument(
        "--scale-control",
        choices=(
            "MANUAL_GATED",
            "AUTOMATIC_DOWN_MANUAL_UP",
            "AUTOMATIC_BOTH",
        ),
        default="MANUAL_GATED",
    )
    parser.add_argument(
        "--universe-ladder",
        default="1,9,25,50,100,200,400",
    )
    parser.add_argument("--universe-target", type=int, default=9)
    parser.add_argument(
        "--stable-sessions-before-up",
        type=int,
        default=5,
    )
    parser.add_argument(
        "--remove-placeholder-secret-lines",
        action="store_true",
    )
    args = parser.parse_args(argv)
    args.env_file = args.env_file.resolve()
    args.journal_directory = args.journal_directory.resolve()
    if not args.env_file.is_file():
        parser.error("--env-file must exist")
    if not args.journal_directory.is_dir():
        parser.error("--journal-directory must exist")
    try:
        ladder = tuple(
            int(value.strip())
            for value in args.universe_ladder.split(",")
            if value.strip()
        )
    except ValueError:
        parser.error("--universe-ladder must contain integers")
    if (
        not ladder
        or ladder[0] != 1
        or ladder != tuple(sorted(set(ladder)))
        or ladder[-1] > 400
        or args.universe_target not in ladder
        or args.stable_sessions_before_up <= 0
    ):
        parser.error("universe scaling settings are invalid")
    args.safe_settings = {
        **SAFE_SETTINGS,
        "REALTIME_V2_ADVANCED_SCALE_CONTROL": args.scale_control,
        "REALTIME_V2_ADVANCED_UNIVERSE_LADDER": ",".join(
            str(value) for value in ladder
        ),
        "REALTIME_V2_ADVANCED_UNIVERSE_TARGET": str(
            args.universe_target
        ),
        "REALTIME_V2_ADVANCED_STABLE_SESSIONS_BEFORE_UP": str(
            args.stable_sessions_before_up
        ),
    }
    return args


def main(argv=None):
    args = parse_args(argv)
    names = (
        *args.safe_settings,
        "REALTIME_V2_ADVANCED_JOURNAL_DIR",
    )
    if args.execute:
        removed_placeholders = []
        if args.remove_placeholder_secret_lines:
            retained = []
            for line in args.env_file.read_text(
                encoding="utf-8"
            ).splitlines(keepends=True):
                key, separator, raw_value = line.partition("=")
                value = raw_value.strip().strip("'\"").lower()
                if (
                    separator
                    and key.strip() in SECRET_NAMES
                    and value in PLACEHOLDERS
                ):
                    removed_placeholders.append(key.strip())
                    continue
                retained.append(line)
            args.env_file.write_text(
                "".join(retained),
                encoding="utf-8",
            )
        for name, value in args.safe_settings.items():
            set_key(
                str(args.env_file),
                name,
                value,
                quote_mode="never",
            )
        set_key(
            str(args.env_file),
            "REALTIME_V2_ADVANCED_JOURNAL_DIR",
            str(args.journal_directory),
            quote_mode="always",
        )
    print(
        json.dumps(
            {
                "mode": "EXECUTE" if args.execute else "PLAN",
                "updated_names": names if args.execute else (),
                "secret_names_touched": (),
                "placeholder_secret_lines_removed": (
                    tuple(removed_placeholders)
                    if args.execute
                    else ()
                ),
                "values_exposed": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
