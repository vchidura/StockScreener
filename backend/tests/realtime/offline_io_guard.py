"""Explicit pytest plugin for source-only checks; no database or provider I/O."""
import importlib.abc
import socket
import sys
import threading

import psycopg2
import pytest
import requests


class CanonicalImportGuard(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname in {"database", "main"}:
            raise RuntimeError(f"Canonical import prohibited in offline realtime checks: {fullname}")


def reject_io(*args, **kwargs):
    raise RuntimeError("Database/provider I/O prohibited in offline realtime checks")


def pytest_addoption(parser):
    parser.addoption(
        "--realtime-no-network",
        action="store_true",
        help="Also prohibit socket connections and DNS in offline rehearsals.",
    )


def pytest_configure(config):
    sys.meta_path.insert(0, CanonicalImportGuard())
    psycopg2.connect = reject_io
    requests.sessions.Session.request = reject_io
    if config.getoption("--realtime-no-network"):
        patch = pytest.MonkeyPatch()
        original_pair = socket.socketpair
        original_connect = socket.socket.connect
        pair_context = threading.local()

        def guarded_connect(connection, address):
            if getattr(pair_context, "constructing_pair", False):
                return original_connect(connection, address)
            return reject_io()

        def internal_socketpair(*args, **kwargs):
            # Windows asyncio uses a loopback connection for its private wakeup pair.
            pair_context.constructing_pair = True
            try:
                return original_pair(*args, **kwargs)
            finally:
                pair_context.constructing_pair = False

        patch.setattr(socket, "socketpair", internal_socketpair)
        patch.setattr(socket.socket, "connect", guarded_connect)
        patch.setattr(socket.socket, "connect_ex", reject_io)
        for name in ("create_connection", "getaddrinfo"):
            patch.setattr(socket, name, reject_io)
        config.add_cleanup(patch.undo)
