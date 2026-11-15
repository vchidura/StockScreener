#!/usr/bin/env python3
"""Write only non-secret Advanced V2 selectors to an existing env file."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from dotenv import set_key, unset_key


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

# The universe limits (realtime/universe_limits.py) are operator settings: this writer never sets them, so a raised
# limit in the env file is kept (they are listed in backend/.env.realtime.example).
SAFE_SETTINGS = {
    "REALTIME_V2_EQUITY_PIPELINE": "MASSIVE_ADVANCED",
    "REALTIME_V2_OPTIONS_PIPELINE": "DISABLED",
}
# Universe sizing (Nov 15): settings of the October canary's scale ladder that no code reads (the size comes from the
# universe file, the cap from REALTIME_UNIVERSE_MAX_NAMES, the stream settings from the runtime policy file). --execute
# removes them from the env file so they cannot be mistaken for live configuration.
RETIRED_SETTINGS = (
    "REALTIME_V2_ADVANCED_STOCK_CHANNELS",
    "REALTIME_V2_ADVANCED_CONNECTION_CEILING",
    "REALTIME_V2_ADVANCED_SCALE_CONTROL",
    "REALTIME_V2_ADVANCED_UNIVERSE_LADDER",
    "REALTIME_V2_ADVANCED_UNIVERSE_TARGET",
    "REALTIME_V2_ADVANCED_STABLE_SESSIONS_BEFORE_UP",
)
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
    parser.add_argument(
        "--ingress-policy-path",
        type=Path,
        default=(
            BACKEND_DIR
            / "realtime"
            / "policies"
            / "advanced_equity_ingress_v1.json"
        ),
    )
    parser.add_argument(
        "--ingress-policy-version",
        default="advanced-equity-ingress-v1",
    )
    parser.add_argument("--execute", action="store_true")
    parser.add_argument(
        "--remove-placeholder-secret-lines",
        action="store_true",
    )
    args = parser.parse_args(argv)
    args.env_file = args.env_file.resolve()
    args.journal_directory = args.journal_directory.resolve()
    args.ingress_policy_path = args.ingress_policy_path.resolve()
    if not args.env_file.is_file():
        parser.error("--env-file must exist")
    if not args.journal_directory.is_dir():
        parser.error("--journal-directory must exist")
    if (
        not args.ingress_policy_path.is_file()
        or not args.ingress_policy_version.strip()
    ):
        parser.error("Advanced ingress policy identity is invalid")
    args.safe_settings = {
        **SAFE_SETTINGS,
        "REALTIME_V2_ADVANCED_INGRESS_POLICY_PATH": str(
            args.ingress_policy_path
        ),
        "REALTIME_V2_ADVANCED_INGRESS_POLICY_VERSION": (
            args.ingress_policy_version.strip()
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
        retired_removed = []
        for name in RETIRED_SETTINGS:
            removed, _key = unset_key(str(args.env_file), name, quote_mode="never")
            if removed:
                retired_removed.append(name)
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
                "retired_settings_removed": (
                    tuple(retired_removed) if args.execute else ()
                ),
                "values_exposed": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
