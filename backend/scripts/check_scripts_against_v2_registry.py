#!/usr/bin/env python3
"""Offline check of backend scripts against the V2 relation registry (no database, no provider).

For each script it scans the script and the local realtime/scripts modules it imports (transitively)
for schema-qualified `rt.` / `ref.` relation names in string literals and compares them with
backend/realtime/relation_registry.py. It also reports imports of local modules that no longer exist
and, unless --no-import-check is given, imports every module the script depends on in a subprocess
that carries the offline guard (psycopg2/requests I/O rejected, canonical `database`/`main` blocked).
The script under test is never imported or executed.

  backend\\.venv\\Scripts\\python.exe -B backend/scripts/check_scripts_against_v2_registry.py [--all] [paths...]

Exit code 0: no mismatch; 1: at least one script has a missing relation or a broken import.
"""
from __future__ import annotations

import argparse
import ast
import importlib
import importlib.abc
import json
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parent.parent
LOCAL_PACKAGES = ("realtime", "scripts")
NAMED_SCRIPTS = (
    "run_realtime_developer_acquisition", "run_realtime_daily_strategies", "run_realtime_ticker_history",
    "run_realtime_equity_validation", "audit_realtime_environment", "audit_realtime_equity_validation",
    "audit_realtime_historical_coverage", "audit_realtime_ticker_history", "report_realtime_target_status",
    "validate_realtime_database",
)
_PLACEHOLDER = "\x00"
_RELATION = re.compile(r"(?<![\w.])(rt|ref)\.([a-z_][a-z0-9_]*)(\x00?)")
IMPORT_TIMEOUT_SECONDS = 180


@dataclass(frozen=True)
class ImportRef:
    module: str
    names: tuple[str, ...]
    line: int


@dataclass
class ScriptReport:
    script: str
    status: str = "OK"
    relations: tuple[str, ...] = ()
    missing_relations: dict[str, list[str]] = field(default_factory=dict)
    broken_imports: list[dict[str, object]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    dependencies: tuple[str, ...] = ()


def v2_relations() -> frozenset[str]:
    if str(BACKEND_DIR) not in sys.path:
        sys.path.insert(0, str(BACKEND_DIR))
    from realtime.relation_registry import RELATIONS
    return frozenset(RELATIONS)


def _docstring_nodes(tree: ast.AST) -> set[int]:
    nodes = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            body = node.body
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) \
                    and isinstance(body[0].value.value, str):
                nodes.add(id(body[0].value))
    return nodes


def _literal_texts(tree: ast.AST):
    docstrings = _docstring_nodes(tree)
    inside_fstring = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.JoinedStr):
            inside_fstring.update(id(value) for value in node.values)
            parts = [part.value if isinstance(part, ast.Constant) else _PLACEHOLDER for part in node.values]
            yield "".join(str(part) for part in parts)
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str) \
                and id(node) not in docstrings and id(node) not in inside_fstring:
            yield node.value


def extract_relations(source: str) -> frozenset[str]:
    """Schema-qualified relation names found in string literals. A name built with an f-string suffix
    is returned as a prefix pattern ending in `*`; function calls (`rt.fn(`) are not relations."""
    found = set()
    for text in _literal_texts(ast.parse(source)):
        for match in _RELATION.finditer(text):
            schema, name, dynamic = match.groups()
            if not dynamic and text[match.end():match.end() + 1] == "(":
                continue
            found.add(f"{schema}.{name}{'*' if dynamic else ''}")
    return frozenset(found)


def missing_from_registry(relations, known: frozenset[str]) -> list[str]:
    missing = []
    for name in sorted(relations):
        if name.endswith("*"):
            if not any(item.startswith(name[:-1]) for item in known):
                missing.append(name)
        elif name not in known and not _is_sequence_of_known(name, known):
            missing.append(name)
    return missing


def _is_sequence_of_known(name: str, known: frozenset[str]) -> bool:
    """`<relation>_<column>_seq` is the implicit sequence of a registry relation, not a relation."""
    return name.endswith("_seq") and any(name.startswith(f"{relation}_") for relation in known)


def _package_of(path: Path, backend_dir: Path) -> str:
    try:
        return ".".join(path.resolve().relative_to(backend_dir.resolve()).parts[:-1])
    except ValueError:
        return ""


def extract_imports(source: str, package: str) -> list[ImportRef]:
    refs = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            refs.extend(ImportRef(alias.name, (), node.lineno) for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if node.level:
                base = package.split(".") if package else []
                base = base[:len(base) - (node.level - 1)] if node.level > 1 else base
                module = ".".join([*base, *([module] if module else [])])
            if module:
                refs.append(ImportRef(module, tuple(alias.name for alias in node.names), node.lineno))
    return refs


def _strip_backend(module: str) -> str:
    return module[len("backend."):] if module.startswith("backend.") else module


def resolve_local(module: str, backend_dir: Path) -> Path | None:
    parts = _strip_backend(module).split(".")
    base = backend_dir.joinpath(*parts)
    for candidate in (base.with_suffix(".py"), base / "__init__.py"):
        if candidate.is_file():
            return candidate
    return None


def _is_local(module: str) -> bool:
    return _strip_backend(module).split(".")[0] in LOCAL_PACKAGES


def _module_exists(module: str, backend_dir: Path) -> bool:
    """A module file or package; a directory without __init__.py is a namespace package (backend/scripts)."""
    return resolve_local(module, backend_dir) is not None \
        or backend_dir.joinpath(*_strip_backend(module).split(".")).is_dir()


def _import_closure(script: Path, backend_dir: Path, missing_modules: list[dict[str, object]]):
    """Return ({path: source}, {path: importing path}) for the script and every local module it reaches;
    unresolved local imports are appended to missing_modules."""
    sources: dict[Path, str] = {}
    parents: dict[Path, Path] = {}
    pending = [(script.resolve(), None)]
    while pending:
        path, parent = pending.pop()
        if path in sources:
            continue
        sources[path] = path.read_text(encoding="utf-8")
        if parent is not None:
            parents[path] = parent
        for ref in extract_imports(sources[path], _package_of(path, backend_dir)):
            if not _is_local(ref.module):
                continue
            if not _module_exists(ref.module, backend_dir):
                missing_modules.append({"file": path.name, "line": ref.line, "module": ref.module,
                                        "reason": "MODULE_NOT_FOUND"})
                continue
            target = resolve_local(ref.module, backend_dir)
            if target is not None:
                pending.append((target.resolve(), path))
            for name in ref.names:
                submodule = resolve_local(f"{ref.module}.{name}", backend_dir)
                if submodule is not None:
                    pending.append((submodule.resolve(), path))
    return sources, parents


def _chain(path: Path, parents: dict[Path, Path]) -> str:
    names = [path.name]
    while path in parents:
        path = parents[path]
        names.append(path.name)
    return " < ".join(names)


class _CanonicalImportGuard(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname in {"database", "main"}:
            raise RuntimeError(f"Canonical import prohibited in offline realtime checks: {fullname}")


def _reject_io(*args, **kwargs):
    raise RuntimeError("Database/provider I/O prohibited in offline realtime checks")


def import_worker() -> int:
    """Subprocess entry: read [{module, names}] from stdin, import each under the offline guard, print results."""
    requests_in = json.load(sys.stdin)
    sys.path.insert(0, str(BACKEND_DIR))
    sys.meta_path.insert(0, _CanonicalImportGuard())
    for module_name, attribute in (("psycopg2", "connect"),):
        try:
            setattr(importlib.import_module(module_name), attribute, _reject_io)
        except ImportError:
            pass
    try:
        importlib.import_module("requests").sessions.Session.request = _reject_io
    except ImportError:
        pass
    results = []
    for item in requests_in:
        outcome = {"module": item["module"], "error": None, "blocked_canonical": False}
        try:
            module = importlib.import_module(item["module"])
            for name in item["names"]:
                if name != "*" and not hasattr(module, name):
                    importlib.import_module(f"{item['module']}.{name}")
        except Exception as error:  # noqa: BLE001 - any import-time failure is the finding
            outcome["error"] = f"{type(error).__name__}: {error}"
            outcome["blocked_canonical"] = "Canonical import prohibited" in str(error)
        results.append(outcome)
    print(json.dumps(results))
    return 0


def check_imports(refs: list[ImportRef], *, timeout: int = IMPORT_TIMEOUT_SECONDS) -> list[dict[str, object]]:
    merged: dict[str, set[str]] = {}
    for ref in refs:
        merged.setdefault(_strip_backend(ref.module), set()).update(ref.names)
    if not merged:
        return []
    payload = json.dumps([{"module": module, "names": sorted(names)} for module, names in sorted(merged.items())])
    try:
        completed = subprocess.run([sys.executable, "-B", str(Path(__file__).resolve()), "--import-worker"],
                                   input=payload, capture_output=True, text=True, timeout=timeout, check=False)
    except subprocess.TimeoutExpired:
        return [{"module": "*", "error": "import worker timed out", "blocked_canonical": False}]
    try:
        return json.loads(completed.stdout.strip().splitlines()[-1])
    except (IndexError, ValueError):
        return [{"module": "*", "error": f"import worker failed (exit {completed.returncode})",
                 "blocked_canonical": False}]


def check_script(script: Path, known: frozenset[str], *, backend_dir: Path = BACKEND_DIR,
                 import_check: bool = True) -> ScriptReport:
    display = script.name
    report = ScriptReport(script=display)
    if not script.is_file():
        report.status = "ABSENT"
        return report
    unresolved: list[dict[str, object]] = []
    sources, parents = _import_closure(script, backend_dir, unresolved)
    relations: set[str] = set()
    for path, source in sources.items():
        found = extract_relations(source)
        relations.update(found)
        for name in missing_from_registry(found, known):
            report.missing_relations.setdefault(name, []).append(_chain(path, parents))
    report.relations = tuple(sorted(relations))
    report.dependencies = tuple(sorted(path.name for path in sources if path != script.resolve()))
    report.broken_imports.extend(unresolved)
    if import_check:
        package = _package_of(script, backend_dir)
        for outcome in check_imports(extract_imports(sources[script.resolve()], package)):
            if outcome["error"] is None:
                continue
            if outcome["blocked_canonical"]:
                report.notes.append(f"{outcome['module']}: imports the canonical database module (not a mismatch)")
            else:
                report.broken_imports.append({"module": outcome["module"], "reason": outcome["error"]})
    if report.missing_relations or report.broken_imports:
        report.status = "MISMATCH"
    return report


def _targets(args: argparse.Namespace) -> list[Path]:
    if args.paths:
        return [Path(path) for path in args.paths]
    if args.all:
        return sorted(path for path in (BACKEND_DIR / "scripts").glob("*.py") if path.name != Path(__file__).name)
    return [BACKEND_DIR / "scripts" / f"{name}.py" for name in NAMED_SCRIPTS]


def realtime_module_importers(backend_dir: Path = BACKEND_DIR) -> dict[str, dict[str, set[str]]]:
    """{realtime module: {"code": importing non-test files, "tests": importing test files}}."""
    importers = {path.stem: {"code": set(), "tests": set()}
                 for path in (backend_dir / "realtime").glob("*.py") if path.stem != "__init__"}
    roots = [backend_dir / name for name in ("realtime", "scripts", "tests", "equity", "options")]
    files = [path for root in roots if root.is_dir() for path in root.rglob("*.py")]
    files += [path for path in backend_dir.glob("*.py")]
    for path in files:
        kind = "tests" if "tests" in path.relative_to(backend_dir).parts else "code"
        try:
            refs = extract_imports(path.read_text(encoding="utf-8"), _package_of(path, backend_dir))
        except SyntaxError:
            continue
        for ref in refs:
            parts = _strip_backend(ref.module).split(".")
            if parts[0] != "realtime":
                continue
            imported = [parts[1]] if len(parts) > 1 else list(ref.names)
            for name in imported:
                if name in importers and path.stem != name:
                    importers[name][kind].add(path.name)
    return importers


def main(argv=None) -> int:
    if argv is None and "--import-worker" in sys.argv[1:]:
        return import_worker()
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("paths", nargs="*", help="script files; default is the named realtime script set")
    parser.add_argument("--all", action="store_true", help="scan every backend/scripts/*.py")
    parser.add_argument("--no-import-check", action="store_true", help="skip the offline subprocess import check")
    parser.add_argument("--only-mismatch", action="store_true", help="print only scripts with a mismatch")
    parser.add_argument("--summary", action="store_true", help="one line per script instead of JSON")
    parser.add_argument("--verbose", action="store_true", help="list every relation and dependency in the JSON")
    parser.add_argument("--dead-realtime-modules", action="store_true",
                        help="list backend/realtime modules no code imports (and those only tests import)")
    args = parser.parse_args(argv)
    if args.dead_realtime_modules:
        importers = realtime_module_importers()
        print(json.dumps({
            "unreferenced": sorted(name for name, row in importers.items() if not row["code"] and not row["tests"]),
            "tests_only": {name: sorted(row["tests"]) for name, row in sorted(importers.items())
                           if not row["code"] and row["tests"]},
        }, indent=2))
        return 0
    known = v2_relations()
    reports = [check_script(path, known, import_check=not args.no_import_check) for path in _targets(args)]
    shown = [item for item in reports if item.status == "MISMATCH"] if args.only_mismatch else reports
    if args.summary:
        for item in shown:
            print(f"{item.status:9} {item.script}: {len(item.missing_relations)} missing relation(s), "
                  f"{len(item.broken_imports)} broken import(s)")
    else:
        verbose = args.verbose
        print(json.dumps({"v2_relation_count": len(known),
                          "summary": {status: sum(item.status == status for item in reports)
                                      for status in ("OK", "MISMATCH", "ABSENT")},
                          "scripts": [{**item.__dict__, "relations": item.relations if verbose else len(item.relations),
                                       "dependencies": item.dependencies if verbose else len(item.dependencies)}
                                      for item in shown]}, indent=2, default=str))
    return 1 if any(item.status == "MISMATCH" for item in reports) else 0


if __name__ == "__main__":
    raise SystemExit(main())
