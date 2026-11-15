"""G102 R62 (Nov 15): every database write the live services can reach must be one the app role is granted.

On Nov 13 the controller crash-looped at the 45-name activation: it INSERTed an exact-size runtime profile into
`rt.advanced_ingress_runtime_profiles`, where the app role has SELECT only, and the only test of that path ran as the
database owner. This static check walks every module the live services import (from the compose entry points and the
scripts they launch), finds each SQL write literal (INSERT INTO / UPDATE / DELETE FROM an `rt.` or `ref.` relation) and
checks it against `realtime.relation_registry` (the source of the app role's grants). A write that only an admin tool
performs must live in a function on the allowlist below, and no live module may reference that function.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

from realtime.relation_registry import RELATIONS

BACKEND_DIR = Path(__file__).resolve().parents[2]

# Every script docker-compose.realtime-v2.yml runs (directly or through scripts/run_realtime_v2_container.py), the
# portal API (uvicorn scripts.run_realtime_v2_portal_api:create_app) and the scripts those services launch as
# subprocesses (the controller's ingress and hybrid gap repair, the scheduler's daily products job).
LIVE_ENTRY_POINTS = (
    "scripts/run_realtime_v2_advanced_equity_ingress.py",
    "scripts/run_realtime_v2_advanced_gap_repair.py",
    "scripts/run_realtime_v2_daily_products.py",
    "scripts/run_realtime_v2_container.py",
    "scripts/run_advanced_options_v3_production.py",
    "scripts/run_advanced_options_v3_acquisition.py",
    "scripts/run_advanced_options_v3_scheduler.py",
    "scripts/run_advanced_options_v3_lifecycle.py",
    "scripts/run_realtime_v2_retention_service.py",
    "scripts/run_realtime_v2_canonical_stock_alerts.py",
    "scripts/run_realtime_v2_portal_publication_retry.py",
    "scripts/run_realtime_v2_equity_interval_finalizer.py",
    "scripts/run_realtime_v2_advanced_options_os_detectors.py",
    "scripts/run_realtime_v2_advanced_journal_replay.py",
    "scripts/run_realtime_v2_advanced_ingress_service.py",
    "scripts/run_realtime_v2_advanced_equity_scheduler.py",
    "scripts/run_realtime_v2_advanced_equity_preparation.py",
    "scripts/run_realtime_v2_advanced_controller.py",
    "scripts/run_realtime_v2_advanced_capability_refresh.py",
    "scripts/run_realtime_v2_portal_api.py",
    "scripts/report_realtime_v2_advanced_health.py",
    "scripts/check_advanced_realtime_v3_service.py",
    "scripts/check_realtime_v2_role_sleep.py",
)

# Functions that write relations the app role cannot, called only by admin tools (they connect as the admin role).
ADMIN_ONLY_FUNCTIONS = {
    ("realtime/advanced_ingress_repository.py", "register_advanced_runtime_profile_cursor"):
        "runtime profiles are registered by migration 053 and the admin tools "
        "(prepare_realtime_v2_advanced_ingress sync-profiles, register_realtime_v2_advanced_runtime_profile)",
    ("realtime/advanced_ingress_repository.py", "activate_advanced_runtime_profile_cursor"):
        "runtime profiles are activated by migration 053 and the admin tools",
    ("realtime/security_v2.py", "_record_phase"):
        "security phases are recorded by the admin security apply/audit tools",
}

# The v1 developer-forward storage writes `rt.developer_forward_*` tables of the v1 developer pipeline; they are not v2
# relations (a v2 service calling it would fail on the missing table, not on a grant). Shared option modules import
# its encoders, so it is reachable, but it is outside this check.
LEGACY_V1_MODULES = {
    "realtime/forward_storage.py": "v1 developer-forward storage; its tables do not exist in stocks_realtime_v2",
}

_WRITE = re.compile(
    r"\b(INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+((?:rt|ref)\.[a-z_0-9]+)",
    re.IGNORECASE,
)
_SCRIPT_LITERAL = re.compile(r"(?:^|[/\\])([a-z0-9_]+\.py)$")


def _module_file(dotted: str) -> Path | None:
    parts = dotted.split(".")
    for candidate in (
        BACKEND_DIR.joinpath(*parts).with_suffix(".py"),
        BACKEND_DIR.joinpath(*parts, "__init__.py"),
    ):
        if candidate.is_file():
            return candidate
    return None


def _imports(path: Path, tree: ast.AST) -> set[Path]:
    found: set[Path] = set()
    package = ".".join(path.relative_to(BACKEND_DIR).with_suffix("").parts[:-1])
    for node in ast.walk(tree):
        names: list[str] = []
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                base = package.split(".")
                base = base[: len(base) - (node.level - 1)] if node.level > 1 else base
                prefix = ".".join(part for part in base if part)
                module = f"{prefix}.{node.module}" if node.module else prefix
            else:
                module = node.module or ""
            names = [module] + [f"{module}.{alias.name}" for alias in node.names]
        for name in names:
            target = _module_file(name) if name else None
            if target is not None:
                found.add(target)
    return found


def _reachable() -> dict[Path, ast.AST]:
    pending = [BACKEND_DIR / entry for entry in LIVE_ENTRY_POINTS]
    trees: dict[Path, ast.AST] = {}
    while pending:
        path = pending.pop()
        if path in trees or not path.is_file():
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        trees[path] = tree
        pending.extend(_imports(path, tree) - trees.keys())
    return trees


def _literals_by_function(tree: ast.AST):
    found = []

    class Visitor(ast.NodeVisitor):
        def __init__(self) -> None:
            self.stack: list[str] = []

        def visit_FunctionDef(self, node):  # noqa: N802 - ast API
            self.stack.append(node.name)
            self.generic_visit(node)
            self.stack.pop()

        visit_AsyncFunctionDef = visit_FunctionDef

        def visit_JoinedStr(self, node):  # noqa: N802 - ast API
            text = "".join(
                value.value if isinstance(value, ast.Constant) and isinstance(value.value, str) else "{}"
                for value in node.values
            )
            found.append((node.lineno, tuple(self.stack), text))

        def visit_Constant(self, node):  # noqa: N802 - ast API
            if isinstance(node.value, str):
                found.append((node.lineno, tuple(self.stack), node.value))

    Visitor().visit(tree)
    return found


def _violations(trees: dict[Path, ast.AST]):
    violations = []
    for path, tree in sorted(trees.items()):
        relative = path.relative_to(BACKEND_DIR).as_posix()
        if relative in LEGACY_V1_MODULES:
            continue
        for line, stack, text in _literals_by_function(tree):
            for verb, relation in _WRITE.findall(text):
                verb = verb.split()[0].upper()
                relation = relation.lower()
                spec = RELATIONS.get(relation)
                if spec is None:
                    problem = "UNREGISTERED_RELATION"
                elif verb in {"INSERT", "UPDATE"} and spec.writes == "NONE":
                    problem = "APP_ROLE_HAS_NO_WRITE"
                elif verb == "DELETE" and not spec.delete:
                    problem = "APP_ROLE_HAS_NO_DELETE"
                else:
                    continue
                if any((relative, name) in ADMIN_ONLY_FUNCTIONS for name in stack):
                    continue
                violations.append(f"{relative}:{line} {'.'.join(stack) or '<module>'} {verb} {relation} {problem}")
    return violations


def test_live_entry_points_exist():
    assert [entry for entry in LIVE_ENTRY_POINTS if not (BACKEND_DIR / entry).is_file()] == []


def test_every_write_the_live_services_reach_is_granted_to_the_app_role():
    trees = _reachable()

    assert len(trees) > 50  # the walk reached the service code, not just the entry scripts
    assert BACKEND_DIR / "realtime" / "advanced_ingress_repository.py" in trees
    assert _violations(trees) == []


def _module_functions(tree: ast.AST) -> dict[str, set[str]]:
    """Module-level functions (and class methods by name) -> the names their bodies reference."""
    functions: dict[str, set[str]] = {}
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            names = functions.setdefault(node.name, set())
            for inner in ast.walk(node):
                if isinstance(inner, ast.Name):
                    names.add(inner.id)
                elif isinstance(inner, ast.Attribute):
                    names.add(inner.attr)
    return functions


def _admin_closure(relative: str, tree: ast.AST) -> set[str]:
    """The allowlisted writers of a module plus every function of that module that leads to one."""
    closure = {name for module, name in ADMIN_ONLY_FUNCTIONS if module == relative}
    if not closure:
        return closure
    functions = _module_functions(tree)
    changed = True
    while changed:
        changed = False
        for name, referenced in functions.items():
            if name not in closure and referenced & closure:
                closure.add(name)
                changed = True
    return closure


def test_admin_only_writers_are_not_referenced_by_live_code():
    """No other live module references an allowlisted writer, or a function of its module that leads to one."""
    trees = _reachable()
    closures = {
        path.relative_to(BACKEND_DIR).as_posix(): _admin_closure(path.relative_to(BACKEND_DIR).as_posix(), tree)
        for path, tree in trees.items()
    }
    references = []
    for path, tree in sorted(trees.items()):
        relative = path.relative_to(BACKEND_DIR).as_posix()
        foreign = {
            name: module
            for module, names in closures.items()
            if module != relative
            for name in names
        }
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    if alias.name in foreign:
                        references.append(f"{relative}:{node.lineno} imports {alias.name} ({foreign[alias.name]})")
            elif isinstance(node, ast.Attribute) and node.attr in foreign:
                references.append(f"{relative}:{node.lineno} uses .{node.attr} ({foreign[node.attr]})")
    assert references == []


def test_the_r62_path_would_have_been_caught():
    """The pre-R62 controller called an exact-size registration that INSERTs a runtime profile: a violation."""
    source = (
        "def ensure_exact(cursor):\n"
        "    cursor.execute(\"INSERT INTO rt.advanced_ingress_runtime_profiles (a) VALUES (%s)\", (1,))\n"
    )
    path = BACKEND_DIR / "realtime" / "r62_example.py"
    assert _violations({path: ast.parse(source)}) == [
        "realtime/r62_example.py:2 ensure_exact INSERT rt.advanced_ingress_runtime_profiles APP_ROLE_HAS_NO_WRITE"
    ]
