"""Command-line entry point for the canonical Stock Screener API."""
from __future__ import annotations

import argparse
import os


def main() -> None:
    parser = argparse.ArgumentParser(description="Stock Screener API Server")
    parser.add_argument("--host", default=os.getenv("API_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.getenv("API_PORT", "8001")))
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--reload", action="store_true")
    parser.add_argument("--env-file", default=".env")
    args = parser.parse_args()

    if os.path.exists(args.env_file):
        from dotenv import load_dotenv

        load_dotenv(args.env_file)

    import uvicorn

    uvicorn.run(
        "main:app",
        host=args.host,
        port=args.port,
        workers=args.workers if not args.reload else 1,
        reload=args.reload,
    )


if __name__ == "__main__":
    main()