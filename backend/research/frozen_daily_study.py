"""Read-only contracts for inspecting retained daily study inputs from V2 research."""
import hashlib
import json
import os
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CURRENT_ADAPTERS = (
    "composite-scanners-1d-v1", "gap-formation-v2", "gap-breakaway-confirmation-v2",
    "gap-entry-fill-v2", "ma-crossover-9-21-v1", "momentum-pullback-v2",
    "bearish-bounce-v2", "pattern-boundary-break-1d-v1",
)
SAMPLE_FILES = {
    "1": "equity_price_diagnostic_results.sample.json",
    "2": "equity_price_replication_results.sample.json",
}
RETIRED_SOURCES = {
    "backend/scripts/run_daily_strategy_backtests.py": "legacy-daily-harness",
    "backend/scripts/run_historical_signal_research.py": "legacy-daily-harness",
    "backend/scripts/merge_historical_signal_research.py": "legacy-daily-harness",
    "backend/equity/scanner_research.py": "stock-research-retirement",
}


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_samples(docs_dir: Path) -> tuple[dict, dict]:
    samples, hashes = {}, {}
    for sample, filename in SAMPLE_FILES.items():
        path = docs_dir / filename
        tickers = json.loads(path.read_text(encoding="utf-8"))["sampled_tickers"]
        if len(tickers) != 300 or len(set(tickers)) != 300:
            raise ValueError(f"sample {sample} must contain 300 distinct tickers")
        if any(not isinstance(ticker, str) or not ticker or ticker != ticker.strip().upper() for ticker in tickers):
            raise ValueError(f"invalid ticker in sample {sample}")
        samples[sample] = tickers
        hashes[filename] = file_sha256(path)
    if set(samples["1"]) & set(samples["2"]):
        raise ValueError("the original samples must not overlap")
    return samples, hashes


def completed(command: list[str], report: Path, artifacts: tuple[Path, ...]) -> bool:
    marker = report.with_suffix(".complete.json")
    if not marker.exists():
        return False
    record = json.loads(marker.read_text(encoding="utf-8"))
    if record["command"] != command:
        raise ValueError(f"completed command differs: {report}")
    if any(not path.is_file() or record["sha256"].get(str(path)) != file_sha256(path) for path in artifacts):
        raise ValueError(f"completed artifact changed or missing: {report}")
    return True


def verify_replays(plan: dict) -> None:
    selections = set()
    for group in plan["groups"]:
        report, events = Path(group["report"]), Path(group["events"])
        if not completed(group["merge_command"], report, (report, events, Path(group["coverage"]))):
            raise ValueError(f"finish replay before outcomes: {group['name']}")
        document = json.loads(report.read_text(encoding="utf-8"))
        if document.get("replay_contract") != "DAILY_IDENTITY_REPLAY_V3" or document.get("source_cutoff") != plan["source_cutoff"]:
            raise ValueError("merged replay does not match the frozen strict contract")
        selections.add(document["universe_selection"]["sha256"])
    if len(selections) != 1:
        raise ValueError("replays disagree on historical universe revisions")


def source_path(filename: str, *, root: Path = ROOT, archive_root: Path | None = None) -> Path:
    relative = filename.replace("\\", "/")
    path = root / relative
    if path.is_file() or relative not in RETIRED_SOURCES:
        return path
    configured = archive_root if archive_root is not None else os.getenv("STOCK_SCREENER_LEGACY_ARCHIVE_ROOT")
    location = Path(configured) if configured is not None else root / "legacy"
    if configured is not None and not location.is_absolute():
        raise ValueError("STOCK_SCREENER_LEGACY_ARCHIVE_ROOT must be an absolute path to the archive containing all batches")
    archive = location / RETIRED_SOURCES[relative]
    manifest_path = archive / "manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(
            f"Historical source verification requires {manifest_path}; set "
            "STOCK_SCREENER_LEGACY_ARCHIVE_ROOT to the external archive directory. "
            "Verification cannot skip missing source evidence."
        )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    rows = [row for row in manifest["files"] if row["original"] == relative]
    if len(rows) != 1 or rows[0]["archived"] != "files/" + relative:
        raise ValueError("retired source is missing from the reviewed archive")
    archived = archive / rows[0]["archived"]
    if file_sha256(archived) != rows[0]["sha256"]:
        raise ValueError("retired source archive checksum differs")
    return archived