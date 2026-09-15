"""Build an isolated read-only Alerts page projection from frozen replay facts."""
import argparse
import hashlib
import json
from pathlib import Path
import sys

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from research.stock_alerts import enrich_replay, replay_snapshot
from research.stock_idea_engine import digest
from research.stock_idea_replay import write_once


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replay", type=Path, required=True)
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists() or args.output.resolve().parent == args.replay.resolve():
        parser.error("choose a new output outside the frozen replay directory")
    manifest = json.loads((args.replay / "manifest.json").read_text(encoding="utf-8"))
    bundle = json.loads(args.inputs.read_text(encoding="utf-8"))
    if digest(bundle) != manifest["inputs_sha256"]:
        raise ValueError("input checksum differs from completed replay")
    publications_path = args.replay / "publications.json"
    publications = json.loads(publications_path.read_text(encoding="utf-8"))
    print("Preparing read-only retained alert rows and pinned indicator snapshots", flush=True)
    snapshot = replay_snapshot(publications, manifest["config"], bundle["source_cutoff"], manifest["study_id"])
    snapshot = enrich_replay(snapshot, bundle, manifest["config"])
    snapshot["lineage"] = dict(inputs_sha256=manifest["inputs_sha256"],
        publications_sha256=hashlib.sha256(publications_path.read_bytes()).hexdigest(),
        builder_sha256=hashlib.sha256((BACKEND / "research/stock_alerts.py").read_bytes()).hexdigest())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    write_once(args.output, snapshot)
    print(json.dumps(dict(alerts=len(snapshot["alerts"]), observations=len(snapshot["observations"]),
        sessions=len(snapshot["sessions"]), publications=len(snapshot["publications"]), output=str(args.output))))


if __name__ == "__main__":
    main()