from __future__ import annotations

import sys
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from scripts import check_scripts_against_v2_registry as checker  # noqa: E402

KNOWN = frozenset({"rt.cohorts", "rt.bar_current", "ref.securities", "rt.instruments"})


def _backend(tmp_path: Path, files: dict[str, str]) -> Path:
    for name, text in files.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return tmp_path


def test_extract_relations_reads_sql_literals_only():
    source = (
        '"""Mentions rt.docstring_only in prose."""\n'
        "import os\n"
        "QUERY = ('SELECT * FROM rt.cohorts AS c JOIN ref.securities AS s USING (security_id) '\n"
        "         'WHERE rt.ensure_partitions(%s) AND x.ref.value = 1')\n"
        "OLD = f\"SELECT 1 FROM rt.developer_forward_{name}\"\n"
        "# rt.comment_only is not a literal\n"
        "OTHER = 'reference.security_id and art.thing'\n"
    )
    assert checker.extract_relations(source) == {
        "rt.cohorts", "ref.securities", "rt.developer_forward_*"}


def test_missing_from_registry_checks_names_prefixes_and_sequences():
    found = {"rt.cohorts", "rt.bar_revisions", "rt.developer_forward_*", "rt.instruments_instrument_id_seq",
             "rt.bar_*"}
    assert checker.missing_from_registry(found, KNOWN) == ["rt.bar_revisions", "rt.developer_forward_*"]


def test_check_script_reports_missing_relation_and_dead_import_through_dependency(tmp_path):
    backend = _backend(tmp_path, {
        "scripts/audit_demo.py": (
            "from realtime.legacy_reader import read\n"
            "SQL = 'SELECT 1 FROM rt.cohorts'\n"),
        "realtime/__init__.py": "",
        "realtime/legacy_reader.py": "READ = 'SELECT 1 FROM rt.portal_snapshots JOIN rt.bar_current USING (x)'\n",
    })
    report = checker.check_script(backend / "scripts" / "audit_demo.py", KNOWN, backend_dir=backend,
                                  import_check=False)
    assert report.status == "MISMATCH"
    assert report.missing_relations == {"rt.portal_snapshots": ["legacy_reader.py < audit_demo.py"]}
    assert report.broken_imports == []
    assert set(report.relations) == {"rt.cohorts", "rt.portal_snapshots", "rt.bar_current"}


def test_check_script_flags_import_of_a_removed_local_module(tmp_path):
    backend = _backend(tmp_path, {
        "scripts/audit_demo.py": "import realtime.removed_runner\nfrom scripts import helper\n",
        "scripts/helper.py": "VALUE = 'rt.instruments'\n",
        "realtime/__init__.py": "",
    })
    report = checker.check_script(backend / "scripts" / "audit_demo.py", KNOWN, backend_dir=backend,
                                  import_check=False)
    assert report.status == "MISMATCH"
    assert [item["module"] for item in report.broken_imports] == ["realtime.removed_runner"]
    assert report.missing_relations == {}


def test_check_script_ok_and_absent(tmp_path):
    backend = _backend(tmp_path, {"scripts/audit_demo.py": "SQL = 'SELECT 1 FROM rt.cohorts'\n"})
    ok = checker.check_script(backend / "scripts" / "audit_demo.py", KNOWN, backend_dir=backend,
                              import_check=False)
    absent = checker.check_script(backend / "scripts" / "retired.py", KNOWN, backend_dir=backend)
    assert (ok.status, absent.status) == ("OK", "ABSENT")


def test_relative_imports_resolve_inside_the_package(tmp_path):
    backend = _backend(tmp_path, {
        "scripts/audit_demo.py": "from realtime.reader import read\n",
        "realtime/__init__.py": "",
        "realtime/reader.py": "from . import helper\nfrom .missing import x\n",
        "realtime/helper.py": "SQL = 'SELECT 1 FROM rt.legacy_table'\n",
    })
    report = checker.check_script(backend / "scripts" / "audit_demo.py", KNOWN, backend_dir=backend,
                                  import_check=False)
    assert report.missing_relations == {"rt.legacy_table": ["helper.py < reader.py < audit_demo.py"]}
    assert [item["module"] for item in report.broken_imports] == ["realtime.missing"]


def test_registry_is_loaded_from_relation_registry():
    known = checker.v2_relations()
    assert {"rt.bar_current", "rt.window_runs", "ref.securities"} <= known
    assert "rt.developer_forward_work" not in known


def test_realtime_module_importers_separate_code_tests_and_unreferenced_modules(tmp_path):
    backend = _backend(tmp_path, {
        "realtime/__init__.py": "",
        "realtime/a.py": "from .b import value\n",
        "realtime/b.py": "value = 1\n",
        "realtime/c.py": "value = 2\n",
        "realtime/d.py": "value = 3\n",
        "realtime/sub/__init__.py": "",
        "realtime/sub/e.py": "from ..a import value\n",
        "scripts/s.py": "from realtime import a\n",
        "tests/realtime/t.py": "from realtime.d import value\nimport realtime.c\n",
    })
    importers = checker.realtime_module_importers(backend)
    assert importers["a"] == {"code": {"s.py", "e.py"}, "tests": set()}
    assert importers["b"] == {"code": {"a.py"}, "tests": set()}
    assert importers["c"] == {"code": set(), "tests": {"t.py"}}
    assert importers["d"] == {"code": set(), "tests": {"t.py"}}
