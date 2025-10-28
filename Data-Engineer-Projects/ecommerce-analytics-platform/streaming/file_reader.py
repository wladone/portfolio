"""File event reader implementation."""

import hashlib
import json
import os
from datetime import UTC, datetime

from .event_reader import Event


class FileTailReader:
    """Read events from file in tail -f fashion."""

    def __init__(self, filename: str):
        """Initialize file reader.

        Args:
            filename: Path to file to read
        """
        self.filename = filename
        self._pos = 0
        self._file = None

    @property
    def source(self) -> str:
        """Get event source identifier."""
        return f"file:{os.path.basename(self.filename)}"

    @property
    def partition(self) -> str:
        """Get partition identifier."""
        return "0"

    async def open(self) -> None:
        """Open file for reading."""
        self._file = open(self.filename, "rb")
        self._pos = 0

    async def poll(self, max_events: int) -> list[Event]:
        """Read next batch of events from file."""
        if not self._file:
            raise RuntimeError("Reader not opened")

        events = []
        for _ in range(max_events):
            line = self._file.readline()
            if not line:
                break

            try:
                payload = json.loads(line)
                raw = line
                offset = self._pos
                self._pos += 1

                # Extract timestamp from payload or use current time
                ts = payload.get("transaction_ts")
                if ts:
                    try:
                        timestamp = datetime.fromisoformat(ts)
                    except ValueError:
                        timestamp = datetime.now(UTC)
                else:
                    timestamp = datetime.now(UTC)

                # Generate stable event ID
                event_id = hashlib.sha1(line).hexdigest()

                events.append(
                    Event(
                        source=self.source,
                        partition=self.partition,
                        offset=offset,
                        event_id=event_id,
                        timestamp=timestamp,
                        payload=payload,
                        raw=raw,
                        key=None,
                    )
                )

            except json.JSONDecodeError:
                continue  # Skip invalid JSON

        return events

    async def ack(self, last_offset: int) -> None:
        """Acknowledge processing up to given offset."""
        # File reader doesn't need acknowledgment
        pass

    async def close(self) -> None:
        """Close the file."""
        if self._file:
            self._file.close()
            self._file = None
