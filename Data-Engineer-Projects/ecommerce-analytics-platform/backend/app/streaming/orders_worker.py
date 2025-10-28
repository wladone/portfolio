import logging
from typing import Any

from ..core.db import get_session
from ..repositories.sales_repo import SalesRepository
from .settings import StreamingSettings
from .worker import StreamingWorker

logger = logging.getLogger(__name__)


class OrdersWorker(StreamingWorker):
    def __init__(self, settings: StreamingSettings):
        super().__init__(settings)
        self.sales_repo = SalesRepository

    async def process_batch(self, batch: list[dict[str, Any]]) -> None:
        async with get_session() as session:
            try:
                # Create repo instance with session
                repo = self.sales_repo(session)
                await repo.bulk_insert_orders(session, batch)
                logger.info(f"Processed batch of {len(batch)} orders")
            except Exception as e:
                logger.error(f"Failed to process orders batch: {e}")
                # In a real implementation, you might want to implement retry logic or dead letter queue
