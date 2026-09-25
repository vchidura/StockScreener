"""Launch the local API in a separate native Windows console."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys


BACKEND_DIR = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = BACKEND_DIR.parent


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8002)
    args = parser.parse_args()
    if os.name != "nt" or not 1 <= args.port <= 65535:
        raise ValueError("native API launcher requires Windows and a valid port")
    process = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "main:app", "--app-dir", "backend",
         "--env-file", str(BACKEND_DIR / ".env"), "--host", "127.0.0.1",
         "--port", str(args.port)],
        cwd=REPOSITORY_ROOT,
        creationflags=subprocess.CREATE_NEW_CONSOLE,
    )
    print(f"EXTERNAL_API_STARTED pid={process.pid} port={args.port}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())