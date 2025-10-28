"""Event reader interface and related types."""

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol


@dataclass
class Event:
    """A normalized event from any source."""

    source: str
    partition: str
    offset: int
    event_id: str
    timestamp: datetime
    payload: dict
    raw: bytes
    key: str | None = None


class EventReader(Protocol):
    """Protocol for event readers from various sources."""

    source: str
    partition: str

    async def open(self) -> None:
        """Open the reader connection."""
        ...

    async def poll(self, max_events: int) -> list[Event]:
        """Poll next batch of events.

        Args:
            max_events: Maximum number of events to return.

        Returns:
            List of normalized events.
        """
        ...

    async def ack(self, last_offset: int) -> None:
        """Acknowledge processing up to given offset.

        Args:
            last_offset: Last successfully processed offset.
        """
        ...

    async def close(self) -> None:
        """Close the reader connection."""
        ...
