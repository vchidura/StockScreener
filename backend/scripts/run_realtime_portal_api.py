from __future__ import annotations

import argparse
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from dotenv import load_dotenv
from psycopg2 import ProgrammingError, pool
from psycopg2.extensions import parse_dsn


BACKEND_DIR = Path(__file__).resolve().parents[1]
WORKSPACE_DIR = BACKEND_DIR.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.portal_api import (  # noqa: E402
    build_realtime_portal_repositories,
    create_realtime_portal_app,
)
from realtime.security import build_role_names  # noqa: E402
from realtime.worker_database import RealtimeProcessDatabase  # noqa: E402
from realtime.worker_topology import build_api_reader_database_plan  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the isolated realtime portal API")
    parser.add_argument("--env-file", type=Path, default=BACKEND_DIR / ".env")
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--role-prefix", default="stocks_realtime")
    return parser.parse_args()


def _build_app(role_prefix):
    dsn = os.getenv("REALTIME_API_DSN", "").strip()
    if not dsn:
        raise RuntimeError("realtime portal API requires REALTIME_API_DSN")

    roles = build_role_names(role_prefix)
    try:
        parameters = parse_dsn(dsn)
    except ProgrammingError:
        raise ValueError("realtime API DSN is invalid") from None
    if (parameters.get("dbname") != "stocks_realtime_v1"
            or parameters.get("user") != roles.api
            or any(name in parameters for name in ("service", "servicefile", "options"))):
        raise ValueError("realtime API DSN must identify stocks_realtime_v1 and its API principal without role overrides")
    process_plan = build_api_reader_database_plan("portal-api:standalone")
    principal_pool = pool.ThreadedConnectionPool(
        process_plan.pool_budget.minimum_connections,
        process_plan.pool_budget.maximum_connections,
        dsn=dsn,
    )
    try:
        database = RealtimeProcessDatabase.wire(
            process_plan,
            principal_pool=principal_pool,
            role_name=roles.reader,
        )
        app = create_realtime_portal_app(
            build_realtime_portal_repositories(database)
        )
    except Exception:
        principal_pool.closeall()
        raise
    return app, principal_pool


def create_app():
    load_dotenv(BACKEND_DIR / ".env", override=False)
    app, principal_pool = _build_app("stocks_realtime")
    original_lifespan = app.router.lifespan_context

    @asynccontextmanager
    async def lifespan(application):
        try:
            async with original_lifespan(application) as state:
                yield state
        finally:
            principal_pool.closeall()

    app.router.lifespan_context = lifespan
    return app


def main() -> int:
    args = parse_args()
    environment_file = args.env_file if args.env_file.is_absolute() else WORKSPACE_DIR / args.env_file
    if not environment_file.is_file():
        raise FileNotFoundError(f"realtime API environment file is missing: {environment_file}")
    load_dotenv(environment_file, override=True)
    if args.host is None:
        args.host = os.getenv("REALTIME_API_HOST", "127.0.0.1")
    if args.port is None:
        args.port = int(os.getenv("REALTIME_API_PORT", "8001"))
    if args.host != "127.0.0.1":
        raise ValueError("realtime portal API must bind to loopback")
    if not 1 <= args.port <= 65535:
        raise ValueError("realtime portal API port is invalid")
    app, principal_pool = _build_app(args.role_prefix)
    try:
        uvicorn.run(app, host=args.host, port=args.port)
    finally:
        principal_pool.closeall()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())