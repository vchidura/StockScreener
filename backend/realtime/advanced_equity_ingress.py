from __future__ import annotations

import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from uuid import UUID

from .advanced_stream_control import AdvancedStreamUsage
from .bridge import drain_raw_buffer_once
from .buffer import RawMessageBuffer
from .contracts import MarketChannel
from .journal import RawJournalReader, RawJournalWriter
from .normalizer import InstrumentAdmission, normalize_raw_message


class AdvancedJournalDecodeError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class AdvancedJournalProgress:
    admitted_market_messages: int = 0
    admitted_market_bytes: int = 0
    last_raw_offset: int = 0
    last_event_offset: int = 0
    event_time_watermark: datetime | None = None


class AdvancedEquityJournalPump:
    def __init__(
        self,
        buffer: RawMessageBuffer,
        journal_path: Path,
        journal_generation: UUID,
        admissions: Mapping[str, InstrumentAdmission],
        *,
        maximum_batch_messages: int,
        maximum_batch_bytes: int,
        on_progress: Callable[[AdvancedJournalProgress], None] | None = None,
    ) -> None:
        if (
            not admissions
            or type(maximum_batch_messages) is not int
            or maximum_batch_messages <= 0
            or type(maximum_batch_bytes) is not int
            or maximum_batch_bytes <= 0
        ):
            raise ValueError("Advanced journal pump bounds are invalid")
        self.buffer = buffer
        self.journal_path = journal_path
        self.journal_generation = journal_generation
        self.admissions = dict(admissions)
        self.maximum_batch_messages = maximum_batch_messages
        self.maximum_batch_bytes = maximum_batch_bytes
        self.on_progress = on_progress
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._error: BaseException | None = None
        self._progress = AdvancedJournalProgress()
        self._progress_lock = threading.Lock()

    @property
    def progress(self) -> AdvancedJournalProgress:
        with self._progress_lock:
            return self._progress

    def usage(
        self,
        *,
        received_frame_count: int,
        received_byte_count: int,
    ) -> AdvancedStreamUsage:
        progress = self.progress
        return AdvancedStreamUsage(
            received_frame_count=received_frame_count,
            received_byte_count=received_byte_count,
            admitted_market_message_count=(
                progress.admitted_market_messages
            ),
            admitted_market_byte_count=progress.admitted_market_bytes,
            last_raw_offset=progress.last_raw_offset,
            last_event_offset=progress.last_event_offset,
            event_time_watermark=progress.event_time_watermark,
        )

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("Advanced journal pump is already started")
        self._thread = threading.Thread(
            target=self._run,
            name="advanced-equity-journal-pump",
            daemon=False,
        )
        self._thread.start()

    def request_stop(self) -> None:
        self._stop.set()

    def join(self, timeout: float | None = None) -> AdvancedJournalProgress:
        if self._thread is None:
            raise RuntimeError("Advanced journal pump was not started")
        self._thread.join(timeout)
        if self._thread.is_alive():
            raise TimeoutError("Advanced journal pump did not stop")
        if self._error is not None:
            if isinstance(self._error, AdvancedJournalDecodeError):
                raise self._error
            raise AdvancedJournalDecodeError(
                "ADVANCED_JOURNAL_PUMP_FAILED"
            ) from self._error
        return self.progress

    def _publish_progress(
        self,
        progress: AdvancedJournalProgress,
    ) -> None:
        with self._progress_lock:
            self._progress = progress
        if self.on_progress is not None:
            self.on_progress(progress)

    def _run(self) -> None:
        try:
            self.journal_path.parent.mkdir(parents=True, exist_ok=True)
            with RawJournalWriter(
                self.journal_path,
                generation=self.journal_generation,
            ) as writer:
                last_raw_offset = 0
                progress = self._progress
                while (
                    not self._stop.is_set()
                    or self.buffer.snapshot().admitted_events > 0
                ):
                    commit = drain_raw_buffer_once(
                        self.buffer,
                        writer,
                        maximum_events=self.maximum_batch_messages,
                        maximum_bytes=self.maximum_batch_bytes,
                    )
                    if commit is None:
                        self._stop.wait(0.01)
                        continue
                    reader = RawJournalReader(self.journal_path)
                    raw_records = reader.read_after(
                        last_raw_offset,
                        limit=self.maximum_batch_messages,
                    )
                    if (
                        not raw_records
                        or raw_records[-1].raw_offset != commit.last_offset
                    ):
                        raise RuntimeError(
                            "Advanced journal commit is not readable"
                        )
                    event_watermark = progress.event_time_watermark
                    last_event_offset = progress.last_event_offset
                    for raw in raw_records:
                        batch = normalize_raw_message(
                            raw,
                            self.admissions,
                        )
                        if batch.quarantines:
                            raise AdvancedJournalDecodeError(
                                "ADVANCED_AM_QUARANTINED"
                            )
                        if any(
                            event.event_kind
                            is not MarketChannel.MINUTE_AGGREGATE
                            for event in batch.events
                        ):
                            raise AdvancedJournalDecodeError(
                                "ADVANCED_UNEXPECTED_CHANNEL"
                            )
                        normalized = writer.commit_normalized(batch)
                        last_event_offset = (
                            normalized.committed_event_offset
                        )
                        if batch.events:
                            own_watermark = max(
                                event.event_at for event in batch.events
                            )
                            event_watermark = (
                                own_watermark
                                if event_watermark is None
                                else max(event_watermark, own_watermark)
                            )
                        last_raw_offset = raw.raw_offset
                    progress = AdvancedJournalProgress(
                        admitted_market_messages=(
                            progress.admitted_market_messages
                            + commit.inserted_count
                            + commit.duplicate_count
                        ),
                        admitted_market_bytes=(
                            progress.admitted_market_bytes
                            + commit.payload_bytes
                        ),
                        last_raw_offset=commit.last_offset,
                        last_event_offset=last_event_offset,
                        event_time_watermark=event_watermark,
                    )
                    self._publish_progress(progress)
        except BaseException as error:
            self._error = error
            self._stop.set()
