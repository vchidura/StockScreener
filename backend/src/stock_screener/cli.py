"""CLI entry point for Stock Screener API."""

import argparse
import os
import sys


def main():
    """Run the Stock Screener API server."""
    parser = argparse.ArgumentParser(
        description="Stock Screener API Server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  stock-screener                    # Run on default port 8001
  stock-screener --port 8080        # Run on port 8080
  stock-screener --workers 4        # Run with 4 workers
  stock-screener --reload           # Development mode with auto-reload
        """,
    )
    
    parser.add_argument(
        "--host",
        type=str,
        default=os.getenv("API_HOST", "0.0.0.0"),
        help="Host to bind to (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("API_PORT", "8001")),
        help="Port to bind to (default: 8001)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of worker processes (default: 1)",
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Enable auto-reload for development",
    )
    parser.add_argument(
        "--env-file",
        type=str,
        default=".env",
        help="Path to .env file (default: .env)",
    )
    
    args = parser.parse_args()
    
    # Load environment variables
    if os.path.exists(args.env_file):
        from dotenv import load_dotenv
        load_dotenv(args.env_file)
        print(f"Loaded environment from: {args.env_file}")
    
    # Import uvicorn here to allow env vars to load first
    import uvicorn
    
    print(f"Starting Stock Screener API on {args.host}:{args.port}")
    
    uvicorn.run(
        "stock_screener.app:app",
        host=args.host,
        port=args.port,
        workers=args.workers if not args.reload else 1,
        reload=args.reload,
    )


if __name__ == "__main__":
    main()
