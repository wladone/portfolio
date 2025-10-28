import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any

from backend.app.streaming.settings import StreamingSettings

from .reader import KafkaReader

logger = logging.getLogger(__name__)


class StreamingWorker(ABC):
    def __init__(self, settings: StreamingSettings):
        self.settings = settings
        self.reader = KafkaReader(settings)
        self.running = False

    @abstractmethod
    async def process_batch(self, batch: list[dict[str, Any]]) -> None:
        pass

    async def start(self, topics: list[str]):
        self.reader.subscribe(topics)
        self.running = True
        logger.info(f"Starting streaming worker for topics: {topics}")

        while self.running:
            try:
                batch = self.reader.poll()
                if batch:
                    await self.process_batch(batch)
                else:
                    # Small delay to prevent busy waiting
                    await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"Error in streaming worker: {e}")
                await asyncio.sleep(1)  # Back off on error

    def stop(self):
        self.running = False
        self.reader.close()
        logger.info("Streaming worker stopped")
