"""Explicit pytest plugin for source-only checks; no database or provider I/O."""
import importlib.abc
import sys

import psycopg2
import requests


class CanonicalImportGuard(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname in {"database", "main"}:
            raise RuntimeError(f"Canonical import prohibited in offline realtime checks: {fullname}")


def reject_io(*args, **kwargs):
    raise RuntimeError("Database/provider I/O prohibited in offline realtime checks")


def pytest_configure(config):
    sys.meta_path.insert(0, CanonicalImportGuard())
    psycopg2.connect = reject_io
    requests.sessions.Session.request = reject_io
