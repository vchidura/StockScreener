#!/usr/bin/env python3
"""Render Advanced Realtime V3 freeze inputs from explicit named bindings."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import date
from pathlib import Path


TOKEN = re.compile(r"^\$\{([a-z][a-z0-9_]*)\}$")


def _read(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid freeze input template: {path}") from exc
    if not isinstance(value, dict):
        raise ValueError("freeze input template must be an object")
    return value


def _bindings(values: list[str]) -> dict[str, str]:
    result = {}
    for value in values:
        name, separator, item = value.partition("=")
        if (
            not separator
            or not re.fullmatch(r"[a-z][a-z0-9_]*", name)
            or not item
            or name in result
        ):
            raise ValueError(f"invalid or duplicate freeze binding: {name}")
        result[name] = item
    return result


def _render(value, bindings, used):
    if isinstance(value, dict):
        return {
            name: _render(item, bindings, used)
            for name, item in value.items()
        }
    if isinstance(value, list):
        return [_render(item, bindings, used) for item in value]
    if isinstance(value, str):
        match = TOKEN.fullmatch(value)
        if match:
            name = match.group(1)
            if name not in bindings:
                raise ValueError(f"freeze binding is unavailable: {name}")
            used.add(name)
            return bindings[name]
    return value


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--template", type=Path, required=True)
    parser.add_argument("--binding", action="append", default=[])
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    args.template = args.template.resolve()
    args.output = args.output.resolve()
    if (
        not args.template.is_file()
        or args.output.exists()
        or not args.output.parent.is_dir()
        or not args.execute
    ):
        parser.error(
            "freeze rendering requires an existing template, new output and --execute"
        )
    return args


def execute(args) -> dict:
    template = _read(args.template)
    if template.get("session_date") != args.session_date.isoformat():
        raise ValueError("freeze input template session changed")
    bindings = _bindings(args.binding)
    used = set()
    rendered = _render(template, bindings, used)
    unused = set(bindings) - used
    if unused:
        raise ValueError(f"unused freeze bindings: {sorted(unused)}")
    encoded = (
        json.dumps(rendered, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    with args.output.open("xb") as stream:
        stream.write(encoded)
    return {
        "status": "FREEZE_INPUTS_RENDERED",
        "output": str(args.output),
        "file_sha256": hashlib.sha256(encoded).hexdigest(),
        "binding_count": len(bindings),
        "provider_effects": 0,
        "production_effects": 0,
    }


def main(argv=None) -> int:
    result = execute(parse_args(argv))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
