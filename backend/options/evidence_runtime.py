"""Bounded process observations for auxiliary evidence work; not admission gates."""
from collections import deque
from datetime import datetime, timezone
from math import ceil
from time import perf_counter


def resident_bytes():
    try:
        import psutil
        return psutil.Process().memory_info().rss
    except Exception:
        return None


class EvidenceRuntimeMonitor:
    def __init__(self, *, memory=resident_bytes, timer=perf_counter):
        self.samples = deque(maxlen=100)
        self.memory = memory
        self.timer = timer

    def _memory(self):
        try:
            value = self.memory()
            return value if isinstance(value, int) and value >= 0 else None
        except Exception:
            return None

    def run(self, stage, callback):
        before = self._memory()
        started = self.timer()
        failed = False
        try:
            return callback()
        except Exception:
            failed = True
            raise
        finally:
            after = self._memory()
            self.samples.append({
                "stage": stage, "elapsed_seconds": max(0., self.timer() - started),
                "rss_before_bytes": before, "rss_after_bytes": after,
                "rss_delta_bytes": after - before if before is not None and after is not None else None,
                "failed": failed, "observed_at": datetime.now(timezone.utc).isoformat(),
            })

    def summary(self):
        samples = tuple(self.samples)
        measured = [row for row in samples if row["rss_delta_bytes"] is not None]
        stages = {}
        for stage in sorted({row["stage"] for row in samples}):
            selected = [row for row in samples if row["stage"] == stage]
            elapsed = sorted(row["elapsed_seconds"] for row in selected)
            stages[stage] = {
                "count": len(selected), "failures": sum(row["failed"] for row in selected),
                "p95_elapsed_seconds": elapsed[ceil(.95 * len(elapsed)) - 1],
                "maximum_rss_delta_bytes": max(
                    (row["rss_delta_bytes"] for row in selected if row["rss_delta_bytes"] is not None), default=None,
                ),
            }
        return {
            "version": "option_evidence_runtime_v1", "sample_count": len(samples),
            "maximum_samples": 100, "memory_sample_count": len(measured), "stages": stages,
            "latest_observed_at": samples[-1]["observed_at"] if samples else None,
            "state": "MEASURED" if measured else "PENDING_REALTIME_EVIDENCE",
            "acceptance": "BASELINE_AND_PEAK_MEASUREMENT_REQUIRED",
            "limitations": ["RSS deltas include allocator and concurrent process work.",
                            "Before/after samples are not peak memory or a disabled-control comparison."],
        }